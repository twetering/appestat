"""
Shared receipt arithmetic and classification for all importers.

Line model
----------
product  -> {'name', 'original_name', 'quantity', 'gross' (as printed), 'discount' (>= 0),
             'price' (= gross - discount, what was paid), 'unit_price', 'weight_kg', 'btw',
             'bonus' (till marked it as part of a promotion)}
extra    -> {'type', 'description', 'quantity', 'amount'}

Extra types and how they count:
    deposit      statiegeld, kratten, verpakking, tasjes      -> bijkomende kosten (spent)
    delivery     bezorgkosten                                 -> bijkomende kosten (spent)
    subscription bezorgbundel, Premium-abonnement             -> bijkomende kosten (spent)
    savings      koopzegels, spaarzegels, PLUSPunten          -> gespaard, NOT spent
"""

import re
from typing import Dict, List, Optional

EPSILON = 0.005

EXTRA_TYPES = {
    "deposit": "Statiegeld & verpakking",
    "delivery": "Bezorgkosten",
    "subscription": "Abonnementen",
    "savings": "Koopzegels & spaarpunten",
}
SPENT_EXTRA_TYPES = ("deposit", "delivery", "subscription")


def new_result(source_type: str, filename: str, file_hash: str) -> Dict:
    return {
        "file_hash": file_hash,
        "filename": filename,
        "source_type": source_type,
        "store": None,
        "invoice_number": None,
        "invoice_date": None,
        "total_amount": 0.0,
        "total_savings": 0.0,
        "products": [],
        "extras": [],
        "raw_text": "",
        "parse_quality_score": None,
        "parse_notes": None,
    }


def to_float(text: str) -> float:
    """'1.234,56' / '-0,35' / '2.38' -> float."""
    text = text.strip().replace(" ", "")
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    return float(text)


def classify_extra(description: str) -> Optional[str]:
    """Return the extra type for a non-product receipt line, or None for a product."""
    d = description.lower().strip()
    if any(k in d for k in ("koopzegel", "spaarzegel", "pluspunt", "to go sparen")):
        return "savings"
    if "bezorgbundel" in d or "abonnement" in d:
        return "subscription"
    if d.startswith("bezorgkosten") or d == "bezorging":
        return "delivery"
    if (d.startswith("+") or "statiegeld" in d or "klapkrat" in d or "verpakkingsmateriaal" in d
            or "plastic verpakking" in d or "tasje" in d or "draagtas" in d
            or re.match(r"^(bulk )?blik\b", d) or re.match(r"^petfles\b", d)
            # returned containers: "Glazen flesje 0.10", "Petfles klein 0.15", "Krat 0.25"
            or re.search(r"\b(fles|flesje|petfles|blik|krat|kratje)\b.*\b0[.,]\d{2}\b", d)):
        return "deposit"
    return None


def make_product(name: str, quantity: int, gross: float, btw: Optional[str] = None,
                 unit_price: Optional[float] = None, weight_kg: Optional[float] = None,
                 bonus: bool = False, original_name: Optional[str] = None) -> Dict:
    return {
        "name": name,
        "original_name": original_name or name,
        "quantity": quantity,
        "gross": round(gross, 2),
        "discount": 0.0,
        "price": round(gross, 2),
        "unit_price": unit_price if unit_price is not None else (round(gross / quantity, 4) if quantity else gross),
        "weight_kg": weight_kg,
        "btw": btw,
        "bonus": bonus,
    }


def make_extra(extra_type: str, description: str, amount: float, quantity: int = 1) -> Dict:
    return {"type": extra_type, "description": description.strip(), "quantity": quantity,
            "amount": round(amount, 2)}


# ------------------------------------------------------------------
# Discounts
# ------------------------------------------------------------------

def _compact(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", text.lower())


def _longest_common(a: str, b: str) -> int:
    best = 0
    for i in range(len(a)):
        for j in range(len(b)):
            k = 0
            while i + k < len(a) and j + k < len(b) and a[i + k] == b[j + k]:
                k += 1
            best = max(best, k)
    return best


STOPWORDS = {"bonus", "bbox", "actie", "extra", "alle", "ah", "mb", "plus", "gev", "premium"}


# Discount labels that name the brand while the till prints a generic name ("ACTIE RED BULL" vs
# "ENERGY DRINK 4PACK" on PLUS receipts; Cafe Nova is PLUS's coffee capsule brand).
DISCOUNT_SYNONYMS = {
    "redbull": ("energydrink",),
    "cafenova": ("espresso", "lungo", "ristretto", "capsule", "koffie"),
}


def discount_targets(label: str, products: List[Dict]) -> List[Dict]:
    """
    Which products does a discount line refer to? ('BONUS AHWITTEDRUIF' -> 'WITTE DRUIF ...').
    Matches on a shared run of >= 5 letters between the label and the product name.
    """
    words = [w for w in re.split(r"[^a-z0-9]+", label.lower()) if w and w not in STOPWORDS]
    key = _compact("".join(words))
    for prefix in ("alle", "ah", "mb"):
        if key.startswith(prefix):
            key = key[len(prefix):]
    if len(key) < 4:
        return []
    scored = []
    for p in products:
        name = _compact(re.sub(r"^ah\s+", "", p["name"].lower()))
        common = _longest_common(key, name)
        if common >= 5 or (common >= 4 and common == len(name)):
            scored.append((common, p))
    if not scored:
        for brand, terms in DISCOUNT_SYNONYMS.items():
            if brand in key:
                return [p for p in products if any(t in _compact(p["name"]) for t in terms)]
        return []
    top = max(c for c, _ in scored)
    return [p for c, p in scored if c == top]


def spread_discount(amount: float, targets: List[Dict]) -> float:
    """Split a discount over products proportional to their net price; returns what didn't fit."""
    remaining = round(amount, 2)
    base = sum(p["price"] for p in targets if p["price"] > 0)
    if base <= 0:
        return remaining
    eligible = [p for p in targets if p["price"] > 0]
    shares = [round(amount * p["price"] / base, 2) for p in eligible]
    shares[-1] = round(amount - sum(shares[:-1]), 2)
    for p, share in zip(eligible, shares):
        share = max(0.0, min(share, p["price"], remaining))
        p["discount"] = round(p["discount"] + share, 2)
        p["price"] = round(p["gross"] - p["discount"], 2)
        remaining = round(remaining - share, 2)
    return remaining


def apply_discount(label: str, amount: float, products: List[Dict], fallback_name: str) -> Optional[Dict]:
    """
    Fold a receipt-level discount (positive amount) into the products it belongs to:
    name match first, then till-marked bonus items. If neither applies, return a
    separate negative product line so product prices stay as printed.
    """
    targets = discount_targets(label, products) or [p for p in products if p.get("bonus")]
    if not targets:
        # A discount on a one-product receipt can only belong to that product
        items = [p for p in products if not p.get("is_discount_line")]
        if len(items) == 1:
            targets = items
    leftover = spread_discount(amount, targets) if targets else round(amount, 2)
    if leftover > EPSILON:
        line = make_product(fallback_name, 1, -leftover)
        line["is_discount_line"] = True
        return line
    return None


# ------------------------------------------------------------------
# Reconciliation
# ------------------------------------------------------------------

def reconcile(result: Dict, checks: List[tuple]) -> None:
    """
    checks: [(label, computed, printed), ...]. Sets parse_quality_score (1.0 when every
    check holds to the cent, else 0.5) and parse_notes describing differences.
    """
    notes = []
    for label, computed, printed in checks:
        if printed is None:
            notes.append(f"{label}: niet gevonden op de bon")
        elif abs(computed - printed) > EPSILON:
            notes.append(f"{label}: berekend €{computed:.2f}, bon zegt €{printed:.2f}")
    result["parse_quality_score"] = 0.5 if notes else 1.0
    existing = [result["parse_notes"]] if result.get("parse_notes") else []
    result["parse_notes"] = "; ".join(existing + notes) or None


def products_total(result: Dict) -> float:
    return round(sum(p["price"] for p in result["products"]), 2)


def extras_total(result: Dict, types=None) -> float:
    return round(sum(e["amount"] for e in result["extras"] if types is None or e["type"] in types), 2)
