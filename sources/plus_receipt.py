"""
PLUS supermarket receipt parser (photos of paper kassabonnen).

Pipeline:
1. OCR with Apple's Vision framework (scripts/vision_ocr.swift) - runs locally, no cloud.
2. Rebuild receipt rows from OCR boxes. Photos are taken at an angle, so a row's amount
   sits higher/lower than its description; we estimate that shear and align descriptions
   with amounts in top-to-bottom order.
3. Interpret rows (items, "Korting", "Statiegeld", actions under the subtotal) and check
   the arithmetic against "Subtotaal" and "Totaal" printed on the receipt.

Product prices are stored net of discounts (like AH online invoices), so summing
products gives what was actually paid for groceries.
"""

import json
import os
import re
import shutil
import subprocess
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from sources.base import ParsedProduct, ParsedReceipt, ReceiptSource


IMAGE_EXTENSIONS = {".heic", ".heif", ".jpg", ".jpeg", ".png"}

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OCR_SCRIPT = PROJECT_ROOT / "scripts" / "vision_ocr.swift"
CACHE_DIR = PROJECT_ROOT / "data" / "cache"
OCR_BINARY = CACHE_DIR / "bin" / "vision_ocr"
OCR_CACHE_DIR = CACHE_DIR / "ocr"

# Bump when parsing logic changes in a way that should invalidate stored results.
PARSER_VERSION = "plus-1.0"

# Totals must match to the cent; allow float noise only.
EPSILON = 0.005


# ============================================================
# OCR
# ============================================================

def _ensure_ocr_binary() -> Path:
    """Compile the Vision OCR helper once (interpreting the script costs ~30s per call)."""
    if OCR_BINARY.exists() and OCR_BINARY.stat().st_mtime >= OCR_SCRIPT.stat().st_mtime:
        return OCR_BINARY
    if not shutil.which("swiftc"):
        raise RuntimeError(
            "PLUS-bonnen vereisen macOS met Xcode Command Line Tools (swiftc) voor lokale OCR"
        )
    OCR_BINARY.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["swiftc", "-O", str(OCR_SCRIPT), "-o", str(OCR_BINARY)],
        check=True, capture_output=True, text=True,
    )
    return OCR_BINARY


def run_ocr(image_path: str, file_hash: Optional[str] = None) -> Dict:
    """OCR an image, caching the result per file hash so re-imports are instant."""
    cache_file = OCR_CACHE_DIR / f"{file_hash}.json" if file_hash else None
    if cache_file and cache_file.exists():
        return json.loads(cache_file.read_text())

    binary = _ensure_ocr_binary()
    proc = subprocess.run(
        [str(binary), str(image_path)], check=True, capture_output=True, text=True, timeout=120,
    )
    result = json.loads(proc.stdout)

    if cache_file:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(json.dumps(result))
    return result


# ============================================================
# TOKENS
# ============================================================

# Amount at the end of a token: "4,38", "€-0,38", "Є-1,97", "- 1,30", "€27, 01"
AMOUNT_TAIL = re.compile(r"(-)?\s*[€ЄCc]?\s*(-)?\s*(\d{1,4})\s?[,.]\s?(\d{2})\s*$")
# OCR sometimes splits an amount into "2" and ",89"
INT_PART = re.compile(r"^(-)?\s*[€ЄC]?\s*(-)?\s*(\d{1,3})$")
DEC_PART = re.compile(r"^[,.]\s?(\d{2})$")
MARKER_ONLY = re.compile(r"^[\*\^•·'\"x]$")
LEADING_MARKER = re.compile(r"^[\*\^•·'\"]\s*")
QTY_PREFIX = re.compile(r"^([1-9I]\d?)\s*[Xx×]\s+")
UNIT_PRICE_SUFFIX = re.compile(r"\s*\d+[,.]\d{2,3}\s*[€ЄC]?\s*/\s*kg\s*$", re.IGNORECASE)
WEIGHT_LINE = re.compile(r"^(\d+[,.]\d{3})\s*kg\s*[xX]\s*(\d+[,.]\d{2})", re.IGNORECASE)
PER_KG_ONLY = re.compile(r"^[€ЄC]?\s*/\s*kg$", re.IGNORECASE)


@dataclass
class Token:
    text: str
    conf: float
    x: float
    y: float
    w: float
    h: float
    amount: Optional[float] = None  # set when the token is purely an amount
    negative: bool = False
    partial: bool = False  # only the cents were legible

    @property
    def cx(self) -> float:
        return self.x + self.w / 2

    @property
    def cy(self) -> float:
        return self.y + self.h / 2

    @property
    def right(self) -> float:
        return self.x + self.w


def _parse_amount_token(text: str) -> Optional[Tuple[float, bool]]:
    """Return (value, has_minus) when the token is an amount, allowing a little OCR junk in front."""
    m = AMOUNT_TAIL.search(text)
    if not m:
        return None
    prefix = text[:m.start()].strip()
    # Watermark/ink noise sometimes glues a few chars in front ("gr0-0,90"), but never a word
    if len(prefix) > 4 or re.search(r"[A-Za-z]{3,}", prefix):
        return None
    value = float(f"{m.group(3)}.{m.group(4)}")
    negative = bool(m.group(1) or m.group(2) or "-" in prefix)
    return value, negative


def _make_tokens(lines: List[Dict]) -> List[Token]:
    tokens = [Token(l["text"].strip(), l.get("conf", 1.0), l["x"], l["y"], l["w"], l["h"])
              for l in lines if l["text"].strip()]

    # Merge split amounts ("2" + ",89") before classifying, closest pairs first
    pairs = []
    for i, a in enumerate(tokens):
        if not INT_PART.match(a.text):
            continue
        for j, b in enumerate(tokens):
            if j != i and DEC_PART.match(b.text):
                gap = b.x - a.right
                dy = abs(a.cy - b.cy)
                if -0.01 <= gap <= 0.04 and dy < max(a.h, b.h):
                    pairs.append((dy, i, j))
    merged: List[Token] = []
    used = set()
    for _, i, j in sorted(pairs):
        if i in used or j in used:
            continue
        a, b = tokens[i], tokens[j]
        merged.append(Token(a.text + b.text.replace(" ", ""), min(a.conf, b.conf), a.x,
                            min(a.y, b.y), b.right - a.x, max(a.h, b.h)))
        used.update({i, j})
    tokens = [t for k, t in enumerate(tokens) if k not in used] + merged

    for t in tokens:
        parsed = _parse_amount_token(t.text)
        if parsed:
            t.amount, t.negative = parsed
        elif DEC_PART.match(t.text):
            # Integer digit not recognised at all (",59"); validation may recover it
            t.amount, t.partial = float("0." + DEC_PART.match(t.text).group(1)), True

    # "0,60" read as "0" + "1,60": a lone digit butting against a one-digit amount means the
    # comma was read as "1," - the lone digit is the real integer part
    leftovers = []
    for a in tokens:
        if a.amount is not None or not re.fullmatch(r"\d", a.text):
            continue
        for b in tokens:
            if (b.amount is not None and re.fullmatch(r"\d[,.]\d{2}", b.text)
                    and -0.012 <= b.x - a.right <= 0.01 and abs(a.cy - b.cy) < max(a.h, b.h)):
                b.text = f"{a.text},{b.text[-2:]}"
                b.amount = float(f"{a.text}.{b.text[-2:]}")
                b.x, b.w = a.x, b.right - a.x
                leftovers.append(id(a))
                break
    return [t for t in tokens if id(t) not in leftovers]


# ============================================================
# TEXT CLASSIFIERS (fuzzy, because OCR misreads characters)
# ============================================================

def _compact(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", _ascii(text).lower())


def _ascii(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()


def _similar(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def is_subtotal(text: str) -> bool:
    c = _compact(text)
    return "ubtot" in c or (c[:1] in ("s", "5") and _similar(c[:9], "subtotaal") >= 0.8)


def is_total(text: str) -> bool:
    c = _compact(text)
    if is_subtotal(text) or "totale" in c:
        return False
    return bool(re.match(r"^\d{0,2}[tli1]?[o0]taa[l1i]{1,2}$", c))


def is_korting(text: str) -> bool:
    words = LEADING_MARKER.sub("", text).split()
    if not words:
        return False
    raw = words[0] + (words[1] if len(_compact(words[0])) < 5 and len(words) > 1 else "")  # "Kor ting"
    first = _compact(raw)
    if not first.startswith("k") or _similar(first[:7], "korting") < 0.7:
        return False
    # Receipts print "Korting" in mixed case; uppercase look-alikes are products (KONINGSZUIVEL)
    return any(ch.islower() for ch in raw) or first == "korting"


def is_statiegeld(text: str) -> bool:
    c = _compact(QTY_PREFIX.sub("", LEADING_MARKER.sub("", text)))
    return _similar(c[:10], "statiegeld") >= 0.75


def is_inclusive_discount(text: str) -> bool:
    """'Inclusief korting: €0,35' - discount already included in the line amount above."""
    return "inclusief" in _compact(text) or "inclkorting" in _compact(text)


def is_header(text: str) -> bool:
    c = _compact(text)
    return any(k in c for k in ("schrij", "chrijv", "pstkg", "bedrag", "klant", "kiant",
                                "spaarkaart", "geregistreerd"))


def is_points(text: str) -> bool:
    c = _compact(text)
    return "pluspunt" in c or "pluspont" in c or "punten" in c


def is_action(text: str) -> bool:
    return "actie" in _compact(text)


def _strip_units(text: str) -> str:
    """Drop '2,25 €/kg' and 'kg' - the only lowercase text inside item lines."""
    text = UNIT_PRICE_SUFFIX.sub("", text)
    text = re.sub(r"[€ЄC]?\s*/\s*kg|\bkg\b", "", text, flags=re.IGNORECASE)
    return _drop_lowercase_words(text)


def _drop_lowercase_words(text: str) -> str:
    """Item names are uppercase; an all-lowercase word glued on is watermark noise ('eiq')."""
    return " ".join(w for w in text.split() if not (w.lower() == w and any(c.isalpha() for c in w)))


def _is_noise(token: "Token") -> bool:
    """Mirrored FSC watermark and ink smudges read as mixed-case gibberish ('10.02l.w')."""
    if token.amount is not None:
        return False
    t = token.text
    if t.count("=") >= 2:  # separator lines
        return True
    if _is_product_like(t):
        return False
    units_stripped = re.sub(r"[€ЄC]?\s*/\s*kg|\bkg\b", "", t, flags=re.IGNORECASE)
    if sum(1 for ch in units_stripped if ch.islower()) < 2:
        return False
    return not (is_korting(t) or is_statiegeld(t) or is_subtotal(t) or is_total(t) or is_action(t)
                or is_points(t) or is_inclusive_discount(t) or is_header(t))


def _is_product_like(text: str) -> bool:
    """Receipt item names are uppercase; watermark noise ('www.fsc.org' mirrored) is not."""
    body = _strip_units(LEADING_MARKER.sub("", text))
    upper = sum(1 for ch in body if ch.isupper())
    lower = sum(1 for ch in body if ch.islower())
    return upper >= 3 and lower <= 1


# ============================================================
# ROW RECONSTRUCTION
# ============================================================

@dataclass
class Row:
    tokens: List[Token]
    yp: float  # shear-corrected vertical position
    amount: Optional[Token] = None
    marked: bool = False
    below_subtotal: bool = False

    @property
    def text(self) -> str:
        parts = [t.text for t in sorted(self.tokens, key=lambda t: t.x)
                 if t.amount is None and not PER_KG_ONLY.match(t.text) and not _is_noise(t)]
        return re.sub(r"\s+", " ", " ".join(parts)).strip()

    @property
    def inline_amounts(self) -> List[Token]:
        return [t for t in self.tokens if t.amount is not None]


def _bedrag_column(amounts: List[Token]) -> List[Token]:
    """Pick the right-aligned 'Bedrag' column; the rest are unit prices (P.st/kg)."""
    if not amounts:
        return []
    rights = sorted(t.right for t in amounts)
    ref = rights[int(len(rights) * 0.8)]
    core = [t for t in amounts if t.right >= ref - 0.06]
    # The column drifts sideways on tilted photos; follow it with a linear fit right = a + b*y
    if len(core) >= 3:
        n = len(core)
        my = sum(t.cy for t in core) / n
        mr = sum(t.right for t in core) / n
        var = sum((t.cy - my) ** 2 for t in core)
        b = sum((t.cy - my) * (t.right - mr) for t in core) / var if var else 0.0
        a = mr - b * my
    else:
        a, b = ref, 0.0
    return [t for t in amounts if t.right >= a + b * t.cy - 0.045]


def _shear_candidates(labels: List[Token], amounts: List[Token], tol: float,
                      limit: int = 6) -> List[float]:
    """
    Slopes s for which amounts line up with labels (y' = cy - s*cx), best first.

    Several slopes can fit equally well: shifting every amount exactly one row up or down
    also lines up. We return the local minima and let the receipt arithmetic decide.
    """
    if not labels or not amounts:
        return [0.0]
    steps = [k * 0.0025 for k in range(-100, 101)]
    costs = []
    for s in steps:
        label_y = [l.cy - s * l.cx for l in labels]
        costs.append(sum(min(tol, min(abs(a.cy - s * a.cx - ly) for ly in label_y))
                         for a in amounts))
    minima = [i for i in range(len(steps))
              if (i == 0 or costs[i] <= costs[i - 1]) and (i == len(steps) - 1 or costs[i] < costs[i + 1])]
    minima.sort(key=lambda i: (costs[i], abs(steps[i])))
    return [steps[i] for i in minima[:limit]]


def _cluster_rows(tokens: List[Token], shear: float, tol: float) -> List[Row]:
    ordered = sorted(tokens, key=lambda t: t.cy - shear * t.cx)
    rows: List[Row] = []
    for t in ordered:
        yp = t.cy - shear * t.cx
        # Tokens on one receipt line never overlap horizontally
        overlaps = rows and any(t.x < u.right - 0.005 and u.x < t.right - 0.005
                                for u in rows[-1].tokens)
        if rows and yp - rows[-1].yp < tol * 0.55 and not overlaps:
            row = rows[-1]
            row.tokens.append(t)
            row.yp = sum(x.cy - shear * x.cx for x in row.tokens) / len(row.tokens)
        else:
            rows.append(Row(tokens=[t], yp=yp))
    return rows


def _mismatch(row: Row, amount: Token, tol: float) -> float:
    """Extra alignment cost when an amount's form doesn't suit the row's meaning."""
    txt = row.text
    euro = bool(re.match(r"^\W{0,2}[€ЄC]", amount.text))
    total_row = is_subtotal(txt) or is_total(txt)
    discount_row = is_korting(txt) or is_action(txt) or (row.below_subtotal and not total_row)
    cost = 0.0
    if amount.negative and not discount_row:
        cost += 3 * tol
    elif amount.negative and not (is_korting(txt) or is_action(txt)):
        cost += 0.5 * tol  # e.g. a coupon line; an explicit "ACTIE" line is the likelier owner
    if is_korting(txt) and not amount.negative:
        cost += tol  # minus sign is sometimes lost, so not forbidden
    if euro and not (total_row or row.below_subtotal):
        cost += tol  # only lines under the subtotal print a € sign
    if total_row and not euro:
        cost += tol
    return cost


def _align(rows: List[Row], amounts: List[Token], shear: float, tol: float):
    """Order-preserving assignment of amounts to rows (both run top to bottom)."""
    for r in rows:
        r.amount = None
    n, m = len(rows), len(amounts)
    amt_y = [a.cy - shear * a.cx for a in amounts]
    skip_row, skip_amt, max_d = 0.35 * tol, 2.0 * tol, 1.6 * tol
    INF = float("inf")
    dp = [[INF] * (m + 1) for _ in range(n + 1)]
    back = [[None] * (m + 1) for _ in range(n + 1)]
    dp[0][0] = 0.0
    for i in range(n + 1):
        for j in range(m + 1):
            cur = dp[i][j]
            if cur == INF:
                continue
            if i < n and cur + skip_row < dp[i + 1][j]:
                dp[i + 1][j], back[i + 1][j] = cur + skip_row, (i, j, False)
            if j < m and cur + skip_amt < dp[i][j + 1]:
                dp[i][j + 1], back[i][j + 1] = cur + skip_amt, (i, j, False)
            if i < n and j < m:
                d = abs(rows[i].yp - amt_y[j])
                if d <= max_d:
                    d += _mismatch(rows[i], amounts[j], tol)
                    if cur + d < dp[i + 1][j + 1]:
                        dp[i + 1][j + 1], back[i + 1][j + 1] = cur + d, (i, j, True)
    i, j = n, m
    while (i, j) != (0, 0):
        pi, pj, matched = back[i][j]
        if matched:
            rows[pi].amount = amounts[pj]
        i, j = pi, pj


# ============================================================
# RECEIPT INTERPRETATION
# ============================================================

@dataclass
class PlusItem:
    raw_name: str
    quantity: int
    amount: float
    unit_price: Optional[float] = None
    discount: float = 0.0  # positive number, subtracted from amount
    marked: bool = False
    is_fee: bool = False  # PLUSPunten etc. - not a product discount target
    partial: bool = False  # euro digit unreadable; amount holds only the cents

    @property
    def net(self) -> float:
        return round(self.amount - self.discount, 2)


@dataclass
class PlusParseResult:
    items: List[PlusItem] = field(default_factory=list)
    deposits: float = 0.0
    inline_savings: float = 0.0
    item_discounts: float = 0.0
    actions: List[Tuple[str, float]] = field(default_factory=list)  # bottom lines with amounts
    subtotal: Optional[float] = None
    total: Optional[float] = None
    stated_savings: Optional[float] = None
    store: Optional[str] = None
    notes: List[str] = field(default_factory=list)
    valid: bool = False
    error: float = float("inf")  # how far the arithmetic is off; used to pick the best reading
    rows_text: List[str] = field(default_factory=list)


def _clean_name(text: str) -> str:
    text = LEADING_MARKER.sub("", text)
    text = QTY_PREFIX.sub("", text)
    text = _drop_lowercase_words(UNIT_PRICE_SUFFIX.sub("", text))
    text = re.sub(r"\s+\d+[,.]\d{2,3}\s*[€ЄC]?$", "", text)  # stray unit price
    text = _ascii(text).upper()
    text = re.sub(r"[^A-Z0-9 .,%&'/+()\-]", "", text)
    return re.sub(r"\s+", " ", text).strip(" .-")


def _qty(text: str) -> int:
    m = QTY_PREFIX.match(LEADING_MARKER.sub("", text))
    if not m:
        return 1
    return int(m.group(1).replace("I", "1"))


def _unit_price(row: Row) -> Optional[float]:
    inline = [t.amount for t in row.inline_amounts if t is not row.amount]
    if inline:
        return inline[0]
    m = re.search(r"(\d+)[,.](\d{2})\s*[€ЄC]?\s*/\s*kg", row.text, re.IGNORECASE)
    return float(f"{m.group(1)}.{m.group(2)}") if m else None


def _parse_stated_savings(lines: List[Dict]) -> Optional[float]:
    """'DE TOTALE KORTING IS €2,48' (OCR often reads € as C/6 and 0 as O)."""
    for l in lines:
        c = _ascii(l["text"]).upper()
        if "KORT" not in c or " IS" not in c:
            continue
        tail = c.split(" IS", 1)[1].strip().replace("O", "0")
        m = re.match(r"^[^\d]*(\d+)[,.]\s?(\d{2})$", tail)
        if m:
            whole = m.group(1)
            if len(whole) > 1 and whole[0] == "6":  # '€' misread as '6'
                whole = whole[1:]
            return float(f"{whole}.{m.group(2)}")
    return None


def parse_ocr(ocr: Dict) -> PlusParseResult:
    """Turn Vision OCR output of a PLUS receipt photo into items and totals."""
    lines = ocr.get("lines", [])
    tokens = _make_tokens(lines)
    if not tokens:
        result = PlusParseResult()
        result.notes.append("Geen tekst herkend")
        return result

    labels = [t for t in tokens if t.amount is None and re.search(r"[A-Za-z]{2,}", t.text)]
    tol = sorted(t.h for t in labels)[len(labels) // 2] if labels else 0.015

    amounts = [t for t in tokens if t.amount is not None]
    bedrag = _bedrag_column(amounts)
    left_labels = [l for l in labels if l.right < max((a.x for a in bedrag), default=1.0)]

    stated_savings = _parse_stated_savings(lines)
    best: Optional[PlusParseResult] = None
    for shear in _shear_candidates(left_labels, bedrag, tol):
        result = _interpret(tokens, bedrag, shear, tol, stated_savings)
        if best is None or result.error < best.error - EPSILON:
            best = result
        if result.valid:
            break

    best.store = next((t.text for t in tokens
                       if t.text.upper().startswith("PLUS ") and len(t.text) > 6), None)
    if best.valid:
        _apply_actions(best)
    return best


def _interpret(tokens: List[Token], bedrag: List[Token], shear: float, tol: float,
               stated_savings: Optional[float]) -> PlusParseResult:
    """Rebuild rows for one shear hypothesis and read items/totals from them."""
    result = PlusParseResult(stated_savings=stated_savings)
    bedrag_ids = {id(t) for t in bedrag}
    others = [t for t in tokens if id(t) not in bedrag_ids]
    rows = _cluster_rows(others, shear, tol)

    # Region: after the column header / loyalty card lines, up to and including the total row.
    # (Search the header only above the total: the payment slip below has "KLANTTICKET".)
    total_idx = next((i for i, r in enumerate(rows) if is_total(r.text)), None)
    if total_idx is None:
        result.notes.append("Regel 'Totaal' niet gevonden")
        total_idx = len(rows) - 1
    header_idx = max((i for i, r in enumerate(rows[:total_idx]) if is_header(r.text)), default=-1)
    region = rows[header_idx + 1: total_idx + 1]

    # Lone '*' markers sit just left of an item that takes part in an action
    markers = [r for r in region if MARKER_ONLY.match(r.text or "")]
    candidates = []
    for r in region:
        txt = r.text
        if not txt or r in markers:
            continue
        if (_is_product_like(txt) or is_korting(txt) or is_statiegeld(txt) or is_subtotal(txt)
                or is_total(txt) or is_action(txt) or is_points(txt)
                or is_inclusive_discount(txt) or WEIGHT_LINE.match(txt)):
            candidates.append(r)
    for mk in markers:
        near = min(candidates, key=lambda r: abs(r.yp - mk.yp), default=None)
        if near is not None and abs(near.yp - mk.yp) < tol * 1.6:
            near.marked = True
    seen_subtotal = False
    for r in candidates:
        r.below_subtotal = seen_subtotal
        seen_subtotal = seen_subtotal or is_subtotal(r.text)

    lo = candidates[0].yp - 2 * tol if candidates else 0
    hi = candidates[-1].yp + 2 * tol if candidates else 1
    region_amounts = sorted((a for a in bedrag if lo <= a.cy - shear * a.cx <= hi),
                            key=lambda a: a.cy - shear * a.cx)
    # Rows that never carry an amount in the Bedrag column must not steal one
    alignable = [r for r in candidates if not is_inclusive_discount(r.text)]
    _align(alignable, region_amounts, shear, tol)

    result.rows_text = [f"{r.text} => {r.amount.text if r.amount else '-'}" for r in candidates]

    in_bottom = False
    last_item: Optional[PlusItem] = None
    for r in candidates:
        txt = r.text
        amt = r.amount.amount if r.amount else None
        neg = r.amount.negative if r.amount else False

        if is_subtotal(txt):
            result.subtotal = amt
            in_bottom = True
            continue
        if is_total(txt):
            result.total = amt
            break
        if is_inclusive_discount(txt):
            m = re.search(r"(\d+)[,.](\d{2})", txt)
            if m:
                result.inline_savings += float(f"{m.group(1)}.{m.group(2)}")
            continue

        if in_bottom:
            if amt is None:
                continue  # stamps/coupons without a price ("2X VOETBALPLAATJES")
            if is_points(txt):
                result.items.append(PlusItem(raw_name="PLUSPUNTEN DIGITAAL", quantity=_qty(txt),
                                             amount=amt, is_fee=True))
            else:
                # Actions are discounts even when OCR lost the minus sign
                value = -amt if (neg or is_action(txt) or "korting" in _compact(txt)) else amt
                result.actions.append((txt, value))
            continue

        if is_korting(txt):
            if amt is None:
                result.notes.append("Korting zonder bedrag")
                continue
            result.item_discounts += amt
            if last_item:
                last_item.discount += amt
            continue
        if is_statiegeld(txt):
            if amt is not None:
                result.deposits += amt
            continue
        weight = WEIGHT_LINE.match(txt)
        if weight:
            # "0,830 kg X 2,25€/kg" continues the product above it
            if last_item and last_item.amount == 0:
                kg, per_kg = (float(g.replace(",", ".")) for g in weight.groups())
                last_item.amount = amt if amt is not None else round(kg * per_kg + 1e-9, 2)
                last_item.unit_price = per_kg
            continue
        if not _is_product_like(txt):
            continue

        name = _clean_name(txt)
        if not name:
            continue
        qty, unit = _qty(txt), _unit_price(r)
        euro_inline = [t for t in r.inline_amounts if re.match(r"^\W{0,2}[€ЄC]", t.text)]
        if amt is None and euro_inline:
            # Some tills print weighed items as "BANAAN  €1,87" in the middle column
            amt, unit = euro_inline[0].amount, None
        if qty == 1 and unit and amt and "kg" not in txt.lower():
            # The "3X" prefix is sometimes cut off the photo; amount / piece price recovers it
            ratio = amt / unit
            if ratio >= 1.5 and abs(ratio - round(ratio)) < 0.01:
                qty = round(ratio)
        last_item = PlusItem(raw_name=name, quantity=qty, amount=amt or 0.0,
                             unit_price=unit, marked=r.marked or txt.startswith(("*", "^")),
                             partial=bool(r.amount and r.amount.partial))
        result.items.append(last_item)

    _validate(result)
    return result


def _euro_misreads(value: Optional[float]) -> List[float]:
    """Large-font totals like '€15,47' are sometimes read as '115,47' or '615,47'."""
    if value is None:
        return []
    text = f"{value:.2f}"
    options = [value]
    if len(text) > 4 and text[0] in "16":
        options.append(float(text[1:]))
    return options


def _recover_missing_digit(result: PlusParseResult, goods: float, fees: float, actions: float) -> float:
    """
    When exactly one line has only its cents legible (",59"), the printed subtotal
    (or total) fixes the missing euro digit. Returns the corrected goods sum.
    """
    partial = [i for i in result.items if i.partial and not i.is_fee]
    if len(partial) != 1:
        return goods
    if result.subtotal is not None:
        targets = _euro_misreads(result.subtotal)
    elif result.total is not None:
        targets = [t - fees - actions for t in _euro_misreads(result.total)]
    else:
        return goods
    for target in targets:
        digit = round(target - goods)
        if 1 <= digit <= 9 and abs(target - goods - digit) <= EPSILON:
            item = partial[0]
            item.amount = round(item.amount + digit, 2)
            item.partial = False
            result.notes.append(f"Eurocijfer van '{item.raw_name}' onleesbaar; afgeleid uit totaal")
            return goods + digit
    return goods


def _validate(result: PlusParseResult):
    goods = sum(i.amount for i in result.items if not i.is_fee) - result.item_discounts + result.deposits
    fees = sum(i.amount for i in result.items if i.is_fee)
    actions = sum(v for _, v in result.actions)
    goods = _recover_missing_digit(result, goods, fees, actions)

    # Prefer the reading of each printed total that agrees with the lines above it
    if result.subtotal is not None:
        result.subtotal = min(_euro_misreads(result.subtotal), key=lambda v: abs(v - goods))
    if result.total is not None:
        base = result.subtotal if result.subtotal is not None else goods
        result.total = min(_euro_misreads(result.total), key=lambda v: abs(v - (base + fees + actions)))

    # A smudged total can still be pinned down: subtotal and actions must then also
    # reproduce the "DE TOTALE KORTING IS" line printed under it.
    if result.total is None and result.subtotal is not None and result.stated_savings is not None:
        savings = result.item_discounts + result.inline_savings - sum(v for _, v in result.actions if v < 0)
        if abs(goods - result.subtotal) <= EPSILON and abs(savings - result.stated_savings) <= EPSILON:
            result.total = round(result.subtotal + fees + actions, 2)
            result.notes.append("Totaalbedrag onleesbaar; afgeleid uit subtotaal en kortingen")

    checks = []
    if result.subtotal is not None:
        checks.append(("subtotaal", goods, result.subtotal))
        if result.total is not None:
            checks.append(("totaal", result.subtotal + fees + actions, result.total))
    elif result.total is not None:
        checks.append(("totaal", goods + fees + actions, result.total))

    missing = [i for i in result.items if i.amount == 0]
    for item in missing:
        result.notes.append(f"Geen bedrag gevonden voor '{item.raw_name}'")
    result.error = 10.0 * len(missing)
    if not checks:
        result.notes.append("Geen subtotaal/totaal om te controleren")
        result.error += 1000
        result.valid = False
        return
    ok = True
    for label, computed, printed in checks:
        diff = abs(computed - printed)
        result.error += diff
        if diff > EPSILON:
            ok = False
            result.notes.append(f"{label}: berekend €{computed:.2f}, bon zegt €{printed:.2f}")
    if result.total is None:
        result.notes.append("Totaalbedrag niet gelezen")
        result.error += 100
        ok = False
    if ok and missing:
        # Totals add up without them, so these were stray text lines rather than products
        result.items = [i for i in result.items if i.amount != 0]
        result.notes = [n for n in result.notes if not n.startswith("Geen bedrag")]
        result.error -= 10.0 * len(missing)
        missing = []
    result.valid = ok and not missing


def _action_targets(text: str, items: List[PlusItem]) -> List[PlusItem]:
    """Find items an action line refers to ('1X ACTIE AVOCADO' -> 'AVOCADO EETRIJP')."""
    words = [w for w in re.split(r"[^A-Z0-9]+", _ascii(text).upper())
             if len(w) >= 4 and w not in {"ACTIE", "EXTRA", "PLUS", "GEV", "ZELF"}]
    goods = [i for i in items if not i.is_fee]
    hits = []
    for item in goods:
        name_words = item.raw_name.split()
        compact = item.raw_name.replace(" ", "")
        for w in words:
            if w in compact or any(nw.startswith(w) or (len(nw) >= 5 and w.startswith(nw))
                                   for nw in name_words):
                hits.append(item)
                break
    if not hits:
        # "ACTIE RED BULL" on a receipt that prints the can as "ENERGY DRINK"
        from importers.common import DISCOUNT_SYNONYMS
        key = re.sub(r"[^a-z0-9]", "", _ascii(text).lower())
        for brand, terms in DISCOUNT_SYNONYMS.items():
            if brand in key:
                hits = [i for i in goods if any(t in i.raw_name.replace(" ", "").lower() for t in terms)]
                break
    return hits


def _apply_actions(result: PlusParseResult):
    """Spread receipt-level discounts over the items they belong to, to the cent."""
    goods = [i for i in result.items if not i.is_fee]
    for text, value in result.actions:
        if value >= 0:
            # Positive extras (rare) are kept as a separate line so totals still add up
            result.items.append(PlusItem(raw_name=_clean_name(text) or "TOESLAG", quantity=1,
                                         amount=value, is_fee=True))
            continue
        # Fold the discount into the product it belongs to (named, or starred */^ by the till).
        # If we can't tell, keep it as its own line so product prices stay as printed.
        targets = _action_targets(text, goods) or [i for i in goods if i.marked]
        leftover = _spread(-value, targets) if targets else round(-value, 2)
        if leftover > EPSILON:
            label = re.sub(r"^\s*[1I]?\d*\s*X\s+|\b(EXTRA\s+)?ACTIE\b:?", " ", _ascii(text).upper())
            result.items.append(PlusItem(raw_name=_clean_name("ACTIEKORTING " + label), quantity=1,
                                         amount=-leftover, is_fee=True))

    paid = sum(i.net for i in result.items) + result.deposits
    if result.total is not None and abs(paid - result.total) > EPSILON:
        result.notes.append(f"Productsom €{paid:.2f} wijkt af van totaal €{result.total:.2f}")
        result.valid = False


def _spread(discount: float, targets: List[PlusItem]) -> float:
    """Split a discount over items proportional to their net price; returns what didn't fit."""
    remaining = round(discount, 2)
    base = sum(i.net for i in targets)
    if base <= 0:
        return remaining
    shares = [round(discount * i.net / base, 2) for i in targets]
    shares[-1] = round(discount - sum(shares[:-1]), 2)  # absorb rounding
    for item, share in zip(targets, shares):
        share = max(0.0, min(share, item.net, remaining))  # never below zero
        item.discount = round(item.discount + share, 2)
        remaining = round(remaining - share, 2)
    return remaining


# ============================================================
# NAME NORMALISATION
# ============================================================

def build_vocabulary(product_names: List[str]) -> Dict[str, int]:
    """Word frequencies over known product names (AH invoices spell words out in full)."""
    vocab: Dict[str, int] = {}
    for name in product_names:
        for word in re.findall(r"[a-z]{4,}", _ascii(name).lower()):
            vocab[word] = vocab.get(word, 0) + 1
    return vocab


def correct_ocr_name(raw: str, vocab: Dict[str, int]) -> str:
    """
    Fix single-character OCR slips in item names ('NUMMUS PIKANT' -> 'HUMMUS PIKANT').
    Only a same-length word differing in exactly one letter is accepted: that is what an
    OCR misread looks like, whereas real spelling variants (DONKER/DONKERE) differ in length.
    """
    out = []
    for word in raw.split():
        w = word.lower()
        if len(w) < 5 or not w.isalpha() or vocab.get(w, 0) >= 2:
            out.append(word)
            continue
        candidates = [(freq, cand) for cand, freq in vocab.items()
                      if freq >= 3 and len(cand) == len(w)
                      and sum(a != b for a, b in zip(cand, w)) == 1]
        out.append(max(candidates)[1].upper() if candidates else word)
    return " ".join(out)


def display_name(raw: str) -> str:
    """'HALFVOLLE MELK BIO' -> 'Halfvolle melk bio' (brand 'PLUS' kept)."""
    words = raw.lower().split()
    if not words:
        return raw
    out = [w.upper() if w == "plus" else w for w in words]
    out[0] = out[0] if out[0] == "PLUS" else out[0].capitalize()
    return " ".join(out)


# ============================================================
# SOURCE
# ============================================================

class PlusReceiptSource(ReceiptSource):
    """Photos of PLUS supermarket receipts (HEIC/JPG/PNG)."""

    @property
    def name(self) -> str:
        return "plus_receipt"

    @property
    def display_name(self) -> str:
        return "PLUS Kassabon"

    @property
    def icon(self) -> str:
        return "📸"

    @property
    def color(self) -> str:
        return "#E3001B"  # PLUS red

    def can_parse(self, filepath: str) -> bool:
        return Path(filepath).suffix.lower() in IMAGE_EXTENSIONS

    def parse(self, filepath: str) -> ParsedReceipt:
        from database import calculate_file_hash

        file_hash = calculate_file_hash(filepath)
        ocr = run_ocr(filepath, file_hash)

        receipt = ParsedReceipt(
            file_hash=file_hash,
            filename=Path(filepath).name,
            source_type=self.name,
        )
        all_text = " ".join(l["text"] for l in ocr.get("lines", []))
        if "PLUS" not in all_text.upper():
            receipt.parse_notes = "Geen PLUS-bon herkend"
            receipt.parse_quality_score = 0.0
            return receipt

        parsed = parse_ocr(ocr)
        reader, llm_usage = "local_ocr", None
        if not parsed.valid and os.getenv("PLUS_LLM_FALLBACK", "1") != "0":
            # Local reading doesn't add up: ask OpenAI vision (sends the photo to the cloud)
            from sources.plus_llm import parse_with_llm
            llm_result, llm_usage = parse_with_llm(filepath)
            if llm_result is not None and llm_result.valid:
                _apply_actions(llm_result)
                llm_result.store = parsed.store
                parsed, reader = llm_result, llm_usage["models"][-1]
            elif llm_usage.get("error"):
                parsed.notes.append(f"AI-vangnet niet beschikbaar: {llm_usage['error']}")
            else:
                parsed.notes.append("AI-vangnet kwam ook niet sluitend uit")
        if not parsed.valid:
            parsed.notes.append("Controleer deze bon handmatig")

        # The date line is usually cut off the photo; the photo's capture time is the shopping moment
        if ocr.get("captured_at"):
            receipt.date = datetime.strptime(ocr["captured_at"], "%Y-%m-%d %H:%M:%S")
            receipt.invoice_number = "PLUS-" + receipt.date.strftime("%Y%m%d-%H%M")

        receipt.total_amount = parsed.total or 0.0
        receipt.total_savings = round(
            parsed.stated_savings if parsed.stated_savings is not None
            else parsed.item_discounts + parsed.inline_savings - sum(v for _, v in parsed.actions if v < 0),
            2,
        )
        receipt.raw_text = "\n".join(parsed.rows_text)
        receipt.parse_quality_score = 1.0 if parsed.valid else 0.5
        receipt.parse_notes = "; ".join(parsed.notes) or None
        receipt.source_metadata = {
            "store": parsed.store,
            "rotation": ocr.get("rotation"),
            "captured_at": ocr.get("captured_at"),
            "subtotal": parsed.subtotal,
            "deposits": round(parsed.deposits, 2),
            "actions": parsed.actions,
            "reader": reader,
            "llm_cost_usd": round(llm_usage["cost_usd"], 4) if llm_usage else 0.0,
            "parser_version": PARSER_VERSION,
        }

        for item in parsed.items:
            receipt.products.append(ParsedProduct(
                name=display_name(item.raw_name),
                original_name_raw=item.raw_name,
                price=item.net,
                quantity=item.quantity,
                unit_price=item.unit_price,
                is_discount=item.discount > 0,
                discount_amount=round(item.discount, 2) or None,
            ))
        return receipt
