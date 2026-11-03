"""
Product catalog: one product, many receipt spellings.

    receipt line ("Red Bull Energy drink 24-pack voordeelverpakking")
        -> alias (normalised key, pack_count=24)
        -> catalog product ("Red Bull Energy drink", brand, family "Energiedrank", category)

Pipeline (incremental, idempotent; see sync()):
    1. normalise unknown aliases with an LLM (brand, product, family, pack size)
    2. link aliases to catalog products (exact product name), merge near-duplicates per family
    3. label uncategorised catalog products: user's own choices first, then the
       OpenAI Decisions API (fixed choice among valid category › subcategory pairs),
       keyword rules when no AI is available
    4. materialise: every product line gets the catalog's category (auto_*) and catalog_id

The category of a catalog product is the source of truth; product lines only mirror it.
"""

import json
import re
import unicodedata
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from difflib import get_close_matches
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from database import get_connection
from services import ai

# Receipt lines that are money adjustments, not products ("(Algemene correctie)" = refund for an earlier order)
DISCOUNT_LINE = re.compile(r"^(actiekorting|bonuskorting|\(?algemene correctie)\b", re.IGNORECASE)
NON_PRODUCT_CATEGORIES = {"Bijkomende kosten", "Verpakking & Statiegeld", "Bezorgkosten"}
CATEGORIES_JSON = Path(__file__).parent.parent / "data" / "ah_categories.json"
REVIEW_BELOW = 0.6  # Decisions confidence under which a human should look


# ------------------------------------------------------------------
# Alias keys
# ------------------------------------------------------------------

def alias_key(name: str) -> str:
    """'AH Groene kiwi's BONUS' / 'ZAANLANDER (0.092kg)' -> stable lowercase key."""
    n = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    n = re.sub(r"\s*\(\d+[.,]\d+\s*kg\)", "", n)
    n = re.sub(r"\bbonus\b", " ", n)
    n = re.sub(r"[^a-z0-9%+&.,/ ]", " ", n)
    return re.sub(r"\s+", " ", n).strip()


def is_discount_line(name: str) -> bool:
    return bool(DISCOUNT_LINE.match(name or ""))


# ------------------------------------------------------------------
# Category tree
# ------------------------------------------------------------------

_pairs_cache: Optional[List[Tuple[str, Optional[str]]]] = None


def invalidate_category_cache() -> None:
    global _pairs_cache
    _pairs_cache = None


def category_pairs() -> List[Tuple[str, Optional[str]]]:
    """Valid (category, subcategory) labels for products, in display order (cached)."""
    global _pairs_cache
    if _pairs_cache is None:
        _pairs_cache = _load_category_pairs()
    return _pairs_cache


def _load_category_pairs() -> List[Tuple[str, Optional[str]]]:
    conn = get_connection()
    rows = conn.execute("""
        SELECT c.name category, s.name subcategory
        FROM categories c LEFT JOIN subcategories s ON s.category_id = c.id
        ORDER BY c.sort_order, c.name, s.sort_order, s.name
    """).fetchall()
    conn.close()
    pairs = []
    for r in rows:
        if r["category"] in NON_PRODUCT_CATEGORIES:
            continue
        if r["subcategory"] is None and r["category"] != "Overig":
            continue  # categories with subcategories are only labelled at subcategory level
        pairs.append((r["category"], r["subcategory"]))
    return pairs


def pair_label(category: str, subcategory: Optional[str]) -> str:
    return f"{category} › {subcategory}" if subcategory else category


def _pair_descriptions() -> Dict[str, str]:
    """Example keywords per subcategory from data/ah_categories.json, as hints for the model."""
    try:
        data = json.loads(CATEGORIES_JSON.read_text())["categories"]
    except Exception:
        return {}
    out = {}
    for cat, info in data.items():
        for sub in info.get("subcategories", []):
            out[pair_label(cat, sub["name"])] = ", ".join(sub.get("keywords", [])[:12])
    return out


def valid_pair(category: str, subcategory: Optional[str]) -> bool:
    """Subcategory must belong to its category; no subcategory is always allowed."""
    pairs = category_pairs()
    if subcategory is None:
        return any(c == category for c, _ in pairs)
    return (category, subcategory) in pairs


# ------------------------------------------------------------------
# 1. Normalisation (LLM)
# ------------------------------------------------------------------

NORMALISE_SCHEMA = {
    "name": "normalised_products",
    "strict": True,
    "schema": {
        "type": "object", "additionalProperties": False, "required": ["items"],
        "properties": {"items": {"type": "array", "items": {
            "type": "object", "additionalProperties": False,
            "required": ["key", "product", "brand", "private_label", "family", "pack_count",
                         "size_value", "size_unit"],
            "properties": {
                "key": {"type": "string"},
                "product": {"type": "string"},
                "brand": {"type": ["string", "null"]},
                "private_label": {"type": "boolean"},
                "family": {"type": "string"},
                "pack_count": {"type": "integer"},
                "size_value": {"type": ["number", "null"]},
                "size_unit": {"type": ["string", "null"], "enum": ["ml", "l", "g", "kg", "stuks", None]},
            }}}},
    },
}

NORMALISE_PROMPT = """Je normaliseert productnamen van Nederlandse supermarktbonnen.
Bronnen: Albert Heijn online-factuur (volledige namen), AH-kassabon (afgekorte hoofdletters) en
PLUS-kassabon (afgekorte hoofdletters).

Geef per regel (met de gegeven key ongewijzigd terug):
- product: duidelijke productnaam in zinsvorm, MET merk vooraan, ZONDER verpakkingsgrootte, aantal,
  gewicht, "voordeelverpakking", "voordeelpak", "BONUS", "x-pack" of andere actieteksten.
  Hetzelfde product in een andere verpakking krijgt EXACT dezelfde product-tekst.
  Smaak, variant en soort horen er wel bij (ongezoet, light, 0%, volkoren, bio, pitloos).
  Voorbeelden: "Red Bull Energy drink 24-pack voordeelverpakking" -> "Red Bull Energy drink";
  "AH HV MELK" (AH-kassabon) -> "AH Halfvolle melk"; "NOTENMIX ONGEBR ONGZ" (PLUS) ->
  "PLUS Notenmix ongebrand ongezouten".
- brand: het merk ("AH", "PLUS", "Red Bull", "Alpro", ...). Huismerklijnen (AH Terra, AH Biologisch,
  AH Excellent) hebben brand "AH". Verzin geen merk: onbekend = null en product zonder merk.
- private_label: true voor huismerk van de winkel (AH..., PLUS...).
- family: korte algemene productsoort in het Nederlands, enkelvoud, zonder merk
  (bv. "Energiedrank", "Halfvolle melk", "Notenmix", "Banaan", "Geraspte kaas").
- pack_count: aantal stuks in de verpakking ("24-pack" -> 24, "2ST" -> 2, "4PACK" -> 4), anders 1.
- size_value + size_unit: inhoud per stuk als die in de naam staat (250 ml, 1.5 l, 500 g), anders null.

Afkortingen uitschrijven. Bij PLUS-bonnen zonder merk is het een PLUS-huismerkproduct, tenzij de naam
duidelijk een A-merk is (LAYS, NIBBIT, CHEETOS, RED BULL, ...). Bij AH-kassabonnen zonder merk:
brand null tenzij de afkorting met AH begint.
{existing}
Namen:
{names}"""


def normalise(entries: List[Dict], existing_names: Iterable[str] = (), batch: int = 50,
              workers: int = 6) -> Dict[str, Dict]:
    """entries: [{'key', 'example', 'chain'}] -> {key: normalised dict}."""
    entries = sorted(entries, key=lambda e: e["key"])
    existing_names = list(existing_names)

    def run(chunk):
        hints = set()
        for e in chunk:
            hints.update(get_close_matches(e["example"], existing_names, n=3, cutoff=0.6))
        existing = ("\nBestaande productnamen (hergebruik EXACT als het hetzelfde product is):\n"
                    + "\n".join(f"- {h}" for h in sorted(hints)) + "\n") if hints else ""
        names = "\n".join(f'{e["key"]} | {e["example"]} | bon: {e["chain"]}' for e in chunk)
        data = ai.json_completion(NORMALISE_PROMPT.format(existing=existing, names=names), NORMALISE_SCHEMA)
        return {item["key"]: item for item in data.get("items", [])}

    chunks = [entries[i:i + batch] for i in range(0, len(entries), batch)]
    out: Dict[str, Dict] = {}
    with ThreadPoolExecutor(workers) as pool:
        for result in pool.map(run, chunks):
            out.update(result)
    return out


def rule_based_normalise(example: str) -> Dict:
    """Fallback without AI: strip pack/promo words, detect pack count."""
    name = re.sub(r"\s+BONUS$", "", example.strip(), flags=re.IGNORECASE)
    m = re.search(r"(\d+)\s*-?\s*(pack|st\b|stuks)", name, re.IGNORECASE)
    pack = int(m.group(1)) if m else 1
    product = re.sub(r"\s*\d+\s*-?\s*(pack|st\b|stuks)\b.*$", "", name, flags=re.IGNORECASE)
    product = re.sub(r"\s+(voordeelverpakking|voordeelpak|voordeelpakket)\b", "", product, flags=re.IGNORECASE)
    return {"product": product.strip() or name, "brand": None, "private_label": False, "family": "",
            "pack_count": pack, "size_value": None, "size_unit": None}


# ------------------------------------------------------------------
# 2. Linking and merging
# ------------------------------------------------------------------

def _catalog_by_name(cursor) -> Dict[str, int]:
    return {r["name"].lower(): r["id"] for r in cursor.execute("SELECT id, name FROM catalog_products")}


def _ensure_catalog(cursor, cache: Dict[str, int], info: Dict) -> int:
    key = info["product"].strip().lower()
    if key in cache:
        return cache[key]
    cursor.execute("""
        INSERT INTO catalog_products (name, brand, family, is_private_label) VALUES (?, ?, ?, ?)
    """, (info["product"].strip(), info.get("brand"), info.get("family") or None,
          1 if info.get("private_label") else 0))
    cache[key] = cursor.lastrowid
    return cache[key]


_GROUP = {
    "type": "object", "additionalProperties": False, "required": ["keep", "merge"],
    "properties": {"keep": {"type": "string"}, "merge": {"type": "array", "items": {"type": "string"}}},
}
MERGE_SCHEMA = {
    "name": "merge_groups", "strict": True,
    "schema": {"type": "object", "additionalProperties": False, "required": ["groups"],
               "properties": {"groups": {"type": "array", "items": _GROUP}}},
}

MERGE_PROMPT = """Hieronder staan productnamen uit een supermarkt-catalogus, gegroepeerd per productsoort.
Geef alleen groepen van namen die PRECIES hetzelfde product zijn en alleen verschillen in schrijfwijze,
hoofdletters, afkorting of verpakkingsgrootte. Andere smaken, varianten, merken of soorten zijn NIET
hetzelfde (AH Halfvolle melk ≠ AH Volle melk ≠ PLUS Halfvolle melk; Red Bull ≠ Red Bull Sugarfree).
"keep" is de beste naam, "merge" de namen die erin opgaan. Geen twijfelgevallen.

{families}"""


def merge_duplicates(workers: int = 4) -> int:
    """Merge catalog products that are the same product spelled differently (LLM, per family)."""
    conn = get_connection()
    rows = conn.execute("SELECT id, name, family FROM catalog_products WHERE family IS NOT NULL").fetchall()
    by_family = defaultdict(list)
    for r in rows:
        by_family[r["family"].lower()].append(r["name"])
    families = [(f, names) for f, names in sorted(by_family.items()) if len(names) > 1]
    if not families:
        conn.close()
        return 0

    def run(chunk):
        text = "\n".join(f"[{f}]\n" + "\n".join(f"- {n}" for n in names) for f, names in chunk)
        return ai.json_completion(MERGE_PROMPT.format(families=text), MERGE_SCHEMA).get("groups", [])

    chunks = [families[i:i + 25] for i in range(0, len(families), 25)]
    with ThreadPoolExecutor(workers) as pool:
        groups = [g for result in pool.map(run, chunks) for g in result]

    ids = {r["name"]: r["id"] for r in rows}
    merged = 0
    cursor = conn.cursor()
    for g in groups:
        keep = ids.get(g["keep"])
        for name in g["merge"]:
            gone = ids.get(name)
            if not keep or not gone or gone == keep:
                continue
            cursor.execute("UPDATE product_aliases SET catalog_id = ? WHERE catalog_id = ?", (keep, gone))
            cursor.execute("UPDATE products SET catalog_id = ? WHERE catalog_id = ?", (keep, gone))
            cursor.execute("DELETE FROM catalog_products WHERE id = ?", (gone,))
            ids.pop(name, None)
            merged += 1
    conn.commit()
    conn.close()
    return merged


def _fold(name: str) -> str:
    """Compare-friendly name: lowercase ascii, plural endings dropped per word."""
    n = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    words = re.findall(r"[a-z0-9%]+", n)
    return " ".join(re.sub(r"(s|en|'s)$", "", w) if len(w) > 4 else w for w in words)


def merge_candidates(threshold: float = 0.86) -> List[Tuple[Dict, Dict]]:
    """Pairs of catalog products of the same brand whose names are nearly identical."""
    from difflib import SequenceMatcher

    conn = get_connection()
    rows = [dict(r) for r in conn.execute("SELECT id, name, brand, category_source FROM catalog_products")]
    conn.close()
    by_brand = defaultdict(list)
    for r in rows:
        by_brand[(r["brand"] or r["name"].split()[0]).lower()].append(r)
    pairs = []
    for group in by_brand.values():
        for i, a in enumerate(group):
            fa = _fold(a["name"])
            for b in group[i + 1:]:
                fb = _fold(b["name"])
                if fa == fb or SequenceMatcher(None, fa, fb).ratio() >= threshold:
                    pairs.append((a, b))
    return pairs


def merge_near_duplicates(min_probability: float = 0.85, workers: int = 12) -> int:
    """Confirm near-identical names with the Decisions API and merge the confirmed pairs."""
    pairs = merge_candidates()
    if not pairs:
        return 0

    def same(pair):
        a, b = pair
        if _fold(a["name"]) == _fold(b["name"]):
            return pair, 1.0
        answer = ai.decide(
            f"Productnaam A: {a['name']}\nProductnaam B: {b['name']}",
            [{"type": "predicate", "name": "zelfde",
              "instructions": "Zijn A en B precies hetzelfde supermarktproduct, en verschillen ze alleen in "
                              "schrijfwijze, enkelvoud/meervoud, afkorting of verpakkingsgrootte? Een andere "
                              "smaak, variant, soort of vetpercentage is NIET hetzelfde."}])
        return pair, float(answer["zelfde"]["value"] or 0)

    with ThreadPoolExecutor(workers) as pool:
        verdicts = list(pool.map(same, pairs))

    merged, gone = 0, set()
    for (a, b), p in verdicts:
        if p < min_probability or a["id"] in gone or b["id"] in gone:
            continue
        # Keep the user's labelled product, else the longer (more descriptive) name
        keep, drop = (a, b) if (a["category_source"] == "user" or len(a["name"]) >= len(b["name"])) \
            and b["category_source"] != "user" else (b, a)
        merge_products(keep["id"], [drop["id"]])
        gone.add(drop["id"])
        merged += 1
    return merged


def link_lines(use_ai: bool = True) -> Dict[str, int]:
    """Create aliases for unknown receipt names and link every product line to the catalog."""
    conn = get_connection()
    cursor = conn.cursor()
    known = {r["alias_key"] for r in cursor.execute("SELECT alias_key FROM product_aliases")}

    lines = cursor.execute("""
        SELECT p.original_name, s.chain FROM products p
        JOIN invoices i ON i.id = p.invoice_id LEFT JOIN stores s ON s.id = i.store_id
    """).fetchall()
    unknown: Dict[str, Dict] = {}
    for r in lines:
        if is_discount_line(r["original_name"]):
            continue
        key = alias_key(r["original_name"])
        if key and key not in known and key not in unknown:
            unknown[key] = {"key": key, "example": r["original_name"], "chain": r["chain"] or "?"}

    stats = {"new_aliases": len(unknown), "ai_normalised": 0}
    if unknown:
        existing_names = [r["name"] for r in cursor.execute("SELECT name FROM catalog_products")]
        normalised = {}
        if use_ai and ai.available():
            try:
                normalised = normalise(list(unknown.values()), existing_names)
                stats["ai_normalised"] = len(normalised)
            except ai.AIUnavailable as e:
                print(f"AI normalisatie niet beschikbaar: {e}")
        cache = _catalog_by_name(cursor)
        for key, entry in unknown.items():
            info = normalised.get(key) or rule_based_normalise(entry["example"])
            source = "llm" if key in normalised else "rule"
            catalog_id = _ensure_catalog(cursor, cache, info)
            cursor.execute("""
                INSERT OR REPLACE INTO product_aliases
                    (alias_key, catalog_id, example_name, source, pack_count, size_value, size_unit, chain)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (key, catalog_id, entry["example"], source, max(1, info.get("pack_count") or 1),
                  info.get("size_value"), info.get("size_unit"), entry["chain"]))
        conn.commit()

    alias_to_catalog = {r["alias_key"]: r["catalog_id"]
                        for r in cursor.execute("SELECT alias_key, catalog_id FROM product_aliases")}
    updates = []
    for r in cursor.execute("SELECT id, original_name, catalog_id, alias_key FROM products").fetchall():
        key = None if is_discount_line(r["original_name"]) else alias_key(r["original_name"])
        target = alias_to_catalog.get(key) if key else None
        if target != r["catalog_id"] or key != r["alias_key"]:
            updates.append((target, key, r["id"]))
    cursor.executemany("UPDATE products SET catalog_id = ?, alias_key = ? WHERE id = ?", updates)
    conn.commit()
    conn.close()
    stats["lines_linked"] = len(updates)
    return stats





def estimate_packs() -> int:
    """
    Set products.pack_count per line: the variant's pack size, or for names without pack
    info ("RED BULL" on a kassabon) an estimate from the price: a line costing ~8x the
    product's typical price per piece is an 8-pack. Weighed products are left alone.
    """
    from statistics import median

    conn = get_connection()
    rows = conn.execute("""
        SELECT p.id, p.catalog_id, MAX(COALESCE(p.quantity, 1), 1) qty, p.price, p.weight_kg,
               COALESCE(a.pack_count, 1) alias_pack, p.pack_count, p.pack_estimated
        FROM products p LEFT JOIN product_aliases a ON a.alias_key = p.alias_key
        WHERE p.catalog_id IS NOT NULL
    """).fetchall()
    by_product = defaultdict(list)
    for r in rows:
        by_product[r["catalog_id"]].append(r)

    updates = []
    for lines in by_product.values():
        # Only for products sold in explicitly named packs elsewhere (e.g. Red Bull 6/8/12/24);
        # sizes and weights of fresh food vary too much to guess from the price.
        known_packs = {l["alias_pack"] for l in lines if l["alias_pack"] > 1}
        multi = [l["price"] / (l["qty"] * l["alias_pack"]) for l in lines
                 if l["price"] > 0 and l["alias_pack"] > 1 and not l["weight_kg"]]
        reference = median(multi) if multi else None
        for l in lines:
            pack, estimated = l["alias_pack"], 0
            if reference and l["alias_pack"] == 1 and l["price"] > 0 and not l["weight_kg"]:
                ratio = (l["price"] / l["qty"]) / reference
                if ratio >= 2.5:
                    best = min(known_packs, key=lambda pk: abs(ratio - pk) / pk)
                    if abs(ratio - best) / best <= 0.15:
                        pack, estimated = best, 1
            if (pack, estimated) != (l["pack_count"], l["pack_estimated"]):
                updates.append((pack, estimated, l["id"]))
    conn.executemany("UPDATE products SET pack_count = ?, pack_estimated = ? WHERE id = ?", updates)
    conn.commit()
    conn.close()
    return len(updates)


# ------------------------------------------------------------------
# 3. Labelling
# ------------------------------------------------------------------

def _user_choice(cursor, catalog_id: int) -> Optional[Tuple[str, Optional[str]]]:
    """The user's majority manual category for a catalog product's lines, if any."""
    rows = cursor.execute("""
        SELECT user_category, user_subcategory, COUNT(*) n FROM products
        WHERE catalog_id = ? AND user_category IS NOT NULL
        GROUP BY user_category, user_subcategory ORDER BY n DESC
    """, (catalog_id,)).fetchall()
    if not rows:
        return None
    return rows[0]["user_category"], rows[0]["user_subcategory"]


def _decisions_text(product: Dict, examples: List[str]) -> str:
    parts = [f"Supermarktproduct: {product['name']}"]
    if product.get("brand"):
        parts.append(f"Merk: {product['brand']}")
    if product.get("family"):
        parts.append(f"Soort: {product['family']}")
    parts.append("Zoals op de bon: " + "; ".join(examples[:4]))
    return "\n".join(parts) + "\nIn welke categorie van een Nederlandse supermarkt hoort dit product?"


def label_with_decisions(product: Dict, examples: List[str],
                         restrict_category: Optional[str] = None) -> Tuple[str, Optional[str], float]:
    """Pick the best category › subcategory with the Decisions API."""
    pairs = category_pairs()
    if restrict_category:
        pairs = [p for p in pairs if p[0] == restrict_category] or [(restrict_category, None)]
    if len(pairs) == 1:
        return pairs[0][0], pairs[0][1], 1.0
    desc = _pair_descriptions()
    choices = [{"value": pair_label(c, s), "description": desc.get(pair_label(c, s)) or pair_label(c, s)}
               for c, s in pairs]
    answer = ai.decide(_decisions_text(product, examples), [{
        "type": "choice", "name": "categorie", "choices": choices,
        "instructions": "Kies de categorie › subcategorie waar dit product in een supermarkt staat. "
                        "Kies 'Overig' alleen voor non-food dat nergens past.",
    }])["categorie"]
    value = answer["value"]
    by_label = {pair_label(c, s): (c, s) for c, s in pairs}
    category, subcategory = by_label.get(value, ("Overig", None))
    return category, subcategory, float(answer.get("confidence") or 0.0)


def _keyword_label(name: str) -> Tuple[str, Optional[str]]:
    from services.categorization import get_categorization_service
    result = get_categorization_service().categorize(name)
    category, subcategory = result["category"], result.get("subcategory")
    if not valid_pair(category, subcategory):
        subcategory = None
    return category, subcategory


def label_catalog(only_missing: bool = True, use_ai: bool = True, workers: int = 12) -> Dict[str, int]:
    """Give every catalog product a category: user choice > Decisions API > keyword rules."""
    conn = get_connection()
    cursor = conn.cursor()
    where = "WHERE category IS NULL" if only_missing else "WHERE COALESCE(category_source, '') != 'user'"
    products = [dict(r) for r in cursor.execute(f"SELECT * FROM catalog_products {where}").fetchall()]
    examples = defaultdict(list)
    for r in cursor.execute("SELECT catalog_id, example_name FROM product_aliases"):
        examples[r["catalog_id"]].append(r["example_name"])

    decisions_todo, results = [], {}
    for p in products:
        choice = _user_choice(cursor, p["id"])
        if choice:
            category, subcategory = choice
            if subcategory and not valid_pair(category, subcategory):
                subcategory = None  # repaired below by asking for a subcategory inside this category
            results[p["id"]] = (category, subcategory, 1.0, "user")
            if subcategory is None and any(c == category and s for c, s in category_pairs()):
                decisions_todo.append((p, category))
        else:
            decisions_todo.append((p, None))
    conn.close()

    def run(item):
        p, restrict = item
        try:
            c, s, conf = label_with_decisions(p, examples[p["id"]] or [p["name"]], restrict)
            return p["id"], c, s, conf, "decisions"
        except ai.AIUnavailable:
            c, s = _keyword_label(p["name"])
            if restrict and c != restrict:
                c, s = restrict, None
            return p["id"], c, s, 0.0, "keyword"

    if use_ai and ai.available():
        with ThreadPoolExecutor(workers) as pool:
            labelled = list(pool.map(run, decisions_todo))
    else:
        labelled = []
        for p, restrict in decisions_todo:
            c, s = _keyword_label(p["name"])
            labelled.append((p["id"], restrict or c, s if not restrict or c == restrict else None, 0.0, "keyword"))

    for pid, c, s, conf, source in labelled:
        if pid in results and results[pid][3] == "user":
            results[pid] = (results[pid][0], s, 1.0, "user")  # user category, model's subcategory
        else:
            results[pid] = (c, s, conf, source)

    conn = get_connection()
    conn.executemany("""
        UPDATE catalog_products SET category = ?, subcategory = ?, category_confidence = ?,
               category_source = ?, needs_review = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?
    """, [(c, s, conf, source, 1 if source != "user" and conf < REVIEW_BELOW else 0, pid)
          for pid, (c, s, conf, source) in results.items()])
    conn.commit()
    conn.close()
    return dict(Counter(v[3] for v in results.values()))


def refresh_model_subcategories(workers: int = 12) -> int:
    """
    For products whose category you chose but whose subcategory the model filled in
    (no subcategory in your own line-level history), ask again within your category.
    Useful after adding subcategories.
    """
    conn = get_connection()
    rows = [dict(r) for r in conn.execute("""
        SELECT cp.* FROM catalog_products cp
        WHERE cp.category_source = 'user' AND cp.category != 'Overig'
          AND NOT EXISTS (SELECT 1 FROM legacy_line_categories l JOIN products p ON p.id = l.product_id
                          WHERE p.catalog_id = cp.id AND l.user_subcategory IS NOT NULL)
    """)]
    examples = defaultdict(list)
    for r in conn.execute("SELECT catalog_id, example_name FROM product_aliases"):
        examples[r["catalog_id"]].append(r["example_name"])
    conn.close()

    def run(p):
        try:
            _, sub, _ = label_with_decisions(p, examples[p["id"]] or [p["name"]], restrict_category=p["category"])
            return p["id"], sub
        except ai.AIUnavailable:
            return p["id"], p["subcategory"]

    with ThreadPoolExecutor(workers) as pool:
        results = list(pool.map(run, rows))
    conn = get_connection()
    conn.executemany("UPDATE catalog_products SET subcategory = ? WHERE id = ?", [(s, i) for i, s in results])
    conn.commit()
    conn.close()
    return sum(1 for (i, s), p in zip(results, rows) if s != p["subcategory"])


def suggest_for_user_labels(min_confidence: float = 0.9, workers: int = 12) -> int:
    """
    Never override your own choices; where the model strongly prefers another category,
    store it as a suggestion for the review queue. Returns the number of suggestions.
    """
    conn = get_connection()
    rows = [dict(r) for r in conn.execute("SELECT * FROM catalog_products WHERE category_source = 'user'")]
    examples = defaultdict(list)
    for r in conn.execute("SELECT catalog_id, example_name FROM product_aliases"):
        examples[r["catalog_id"]].append(r["example_name"])
    conn.close()

    def run(p):
        try:
            return p, label_with_decisions(p, examples[p["id"]] or [p["name"]])
        except ai.AIUnavailable:
            return p, None

    with ThreadPoolExecutor(workers) as pool:
        results = list(pool.map(run, rows))
    updates = []
    for p, label in results:
        if label and label[0] != p["category"] and label[2] >= min_confidence:
            updates.append((label[0], label[1], label[2], p["id"]))
        else:
            updates.append((None, None, None, p["id"]))
    conn = get_connection()
    conn.executemany("""UPDATE catalog_products SET suggested_category = ?, suggested_subcategory = ?,
                        suggested_confidence = ? WHERE id = ?""", updates)
    conn.commit()
    conn.close()
    return sum(1 for u in updates if u[0])


def accept_suggestion(catalog_id: int) -> Dict:
    conn = get_connection()
    row = conn.execute("SELECT suggested_category, suggested_subcategory FROM catalog_products WHERE id = ?",
                       (catalog_id,)).fetchone()
    conn.close()
    if not row or not row["suggested_category"]:
        raise ValueError("Geen suggestie voor dit product")
    return set_category(catalog_id, row["suggested_category"], row["suggested_subcategory"])


def confirm_category(catalog_id: int) -> None:
    """Keep the current category: mark it as yours and drop any suggestion."""
    conn = get_connection()
    conn.execute("""UPDATE catalog_products SET category_source = 'user', needs_review = 0, category_confidence = 1.0,
                    suggested_category = NULL, suggested_subcategory = NULL, suggested_confidence = NULL
                    WHERE id = ?""", (catalog_id,))
    conn.commit()
    conn.close()


# ------------------------------------------------------------------
# 4. Materialise + manual changes
# ------------------------------------------------------------------

def materialise() -> int:
    """Copy catalog categories onto product lines (auto_*). Manual line overrides stay."""
    conn = get_connection()
    cur = conn.execute("""
        UPDATE products SET
            auto_category = (SELECT category FROM catalog_products cp WHERE cp.id = products.catalog_id),
            auto_subcategory = (SELECT subcategory FROM catalog_products cp WHERE cp.id = products.catalog_id)
        WHERE catalog_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM catalog_products cp WHERE cp.id = products.catalog_id AND cp.category IS NOT NULL
              AND (cp.category IS NOT products.auto_category OR cp.subcategory IS NOT products.auto_subcategory))
    """)
    changed = cur.rowcount
    # Discount lines follow the product category of what they discount, set at import time
    conn.commit()
    conn.close()
    return changed


def adopt_line_overrides() -> int:
    """
    One-off move to product-level categories: back up line-level manual categories, then
    clear them on lines whose catalog product now carries that (user) category.
    """
    conn = get_connection()
    conn.execute("""
        INSERT OR IGNORE INTO legacy_line_categories (product_id, original_name, user_category, user_subcategory)
        SELECT id, original_name, user_category, user_subcategory FROM products
        WHERE user_category IS NOT NULL OR user_subcategory IS NOT NULL
    """)
    cur = conn.execute("""
        UPDATE products SET user_category = NULL, user_subcategory = NULL
        WHERE catalog_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM catalog_products cp WHERE cp.id = products.catalog_id
              AND cp.category = COALESCE(products.user_category, products.auto_category))
    """)
    cleared = cur.rowcount
    conn.commit()
    conn.close()
    return cleared


def set_category(catalog_id: int, category: str, subcategory: Optional[str]) -> Dict:
    """Manual category for a catalog product; applies to all its purchases, past and future."""
    if not valid_pair(category, subcategory):
        raise ValueError(f"'{subcategory}' hoort niet bij categorie '{category}'")
    conn = get_connection()
    conn.execute("""
        UPDATE catalog_products SET category = ?, subcategory = ?, category_source = 'user',
               category_confidence = 1.0, needs_review = 0, suggested_category = NULL,
               suggested_subcategory = NULL, suggested_confidence = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (category, subcategory, catalog_id))
    cur = conn.execute("""
        UPDATE products SET auto_category = ?, auto_subcategory = ?, user_category = NULL,
               user_subcategory = NULL WHERE catalog_id = ?
    """, (category, subcategory, catalog_id))
    conn.commit()
    conn.close()
    return {"catalog_id": catalog_id, "lines_updated": cur.rowcount}


def move_alias(alias: str, target_catalog_id: Optional[int] = None, new_name: Optional[str] = None) -> int:
    """Split/merge: point one receipt spelling at another (or a new) catalog product."""
    conn = get_connection()
    cursor = conn.cursor()
    if target_catalog_id is None:
        cursor.execute("INSERT INTO catalog_products (name) VALUES (?)", (new_name or alias,))
        target_catalog_id = cursor.lastrowid
    cursor.execute("UPDATE product_aliases SET catalog_id = ?, source = 'user' WHERE alias_key = ?",
                   (target_catalog_id, alias))
    cursor.execute("UPDATE products SET catalog_id = ? WHERE alias_key = ?", (target_catalog_id, alias))
    cursor.execute("DELETE FROM catalog_products WHERE id NOT IN (SELECT DISTINCT catalog_id FROM product_aliases)")
    conn.commit()
    conn.close()
    return target_catalog_id


def merge_products(keep_id: int, merge_ids: List[int]) -> None:
    conn = get_connection()
    for mid in merge_ids:
        if mid == keep_id:
            continue
        conn.execute("UPDATE product_aliases SET catalog_id = ?, source = 'user' WHERE catalog_id = ?", (keep_id, mid))
        conn.execute("UPDATE products SET catalog_id = ? WHERE catalog_id = ?", (keep_id, mid))
        conn.execute("DELETE FROM catalog_products WHERE id = ?", (mid,))
    conn.commit()
    conn.close()
    materialise()


def rename_product(catalog_id: int, name: str) -> None:
    conn = get_connection()
    conn.execute("UPDATE catalog_products SET name = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                 (name.strip(), catalog_id))
    conn.commit()
    conn.close()


# ------------------------------------------------------------------
# Read models
# ------------------------------------------------------------------

def list_products(search: str = "", category: str = "", subcategory: str = "", chain: str = "",
                  year=None, needs_review: bool = False, sort: str = "spent", limit: int = 500) -> List[Dict]:
    """Catalog products with purchase stats (optionally filtered by period/store)."""
    clauses, params = ["p.catalog_id IS NOT NULL"], []
    if year:
        clauses.append("strftime('%Y', i.invoice_date) = ?")
        params.append(str(year))
    if chain:
        clauses.append("s.chain = ?")
        params.append(chain)
    having, hparams = [], []
    if search:
        having.append("(lower(cp.name) LIKE ? OR lower(COALESCE(cp.family,'')) LIKE ? OR lower(COALESCE(cp.brand,'')) LIKE ?"
                      " OR EXISTS (SELECT 1 FROM product_aliases pa WHERE pa.catalog_id = cp.id AND pa.alias_key LIKE ?))")
        like = f"%{search.lower()}%"
        hparams += [like, like, like, like]
    if category:
        having.append("cp.category = ?")
        hparams.append(category)
    if subcategory:
        having.append("cp.subcategory = ?")
        hparams.append(subcategory)
    if needs_review:
        having.append("(cp.needs_review = 1 OR cp.category IS NULL OR cp.suggested_category IS NOT NULL"
                      " OR (cp.category = 'Overig' AND COALESCE(cp.category_source, '') != 'user'))")
    order = {"spent": "spent DESC", "name": "cp.name COLLATE NOCASE", "count": "purchases DESC",
             "recent": "last_bought DESC"}.get(sort, "spent DESC")
    conn = get_connection()
    rows = conn.execute(f"""
        SELECT cp.id, cp.name, cp.brand, cp.family, cp.category, cp.subcategory, cp.category_source,
               cp.category_confidence, cp.needs_review, cp.is_private_label,
               cp.suggested_category, cp.suggested_subcategory, cp.suggested_confidence,
               COUNT(p.id) purchases, SUM(MAX(COALESCE(p.quantity, 1), 1) * COALESCE(p.pack_count, a.pack_count, 1)) units,
               ROUND(SUM(p.price), 2) spent, MIN(i.invoice_date) first_bought, MAX(i.invoice_date) last_bought,
               COUNT(DISTINCT a.alias_key) variants, GROUP_CONCAT(DISTINCT s.chain) chains
        FROM catalog_products cp
        JOIN products p ON p.catalog_id = cp.id
        JOIN invoices i ON i.id = p.invoice_id
        LEFT JOIN stores s ON s.id = i.store_id
        LEFT JOIN product_aliases a ON a.alias_key = p.alias_key
        WHERE {' AND '.join(clauses)}
        GROUP BY cp.id
        {('HAVING ' + ' AND '.join(having)) if having else ''}
        ORDER BY {order} LIMIT ?
    """, params + hparams + [limit]).fetchall()
    conn.close()
    out = []
    for r in rows:
        d = dict(r)
        d["unit_price_avg"] = round(d["spent"] / d["units"], 2) if d.get("units") else None
        out.append(d)
    return out


def product_lines(catalog_id: int) -> List[Dict]:
    """Every purchase of a catalog product with variant info and price per piece."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT p.id, p.invoice_id, i.invoice_number, i.invoice_date date, s.name store, s.chain,
               p.original_name, p.quantity, p.price, p.gross_price, p.discount_amount, p.weight_kg,
               COALESCE(p.pack_count, a.pack_count, 1) pack_count, p.pack_estimated
        FROM products p JOIN invoices i ON i.id = p.invoice_id LEFT JOIN stores s ON s.id = i.store_id
        LEFT JOIN product_aliases a ON a.alias_key = p.alias_key
        WHERE p.catalog_id = ? ORDER BY i.invoice_date
    """, (catalog_id,)).fetchall()
    conn.close()
    out = []
    for r in rows:
        d = dict(r)
        pieces = max(d["quantity"] or 1, 1) * d["pack_count"]
        d["unit_price"] = round(d["price"] / pieces, 3) if d["price"] else None
        d["price_per_kg"] = round(d["price"] / d["weight_kg"], 2) if d.get("weight_kg") else None
        out.append(d)
    return out


def product_detail(catalog_id: int) -> Optional[Dict]:
    from services import metrics

    conn = get_connection()
    row = conn.execute("SELECT * FROM catalog_products WHERE id = ?", (catalog_id,)).fetchone()
    aliases = [dict(r) for r in conn.execute("""
        SELECT alias_key, example_name, pack_count, size_value, size_unit, chain, source
        FROM product_aliases WHERE catalog_id = ? ORDER BY pack_count, example_name
    """, (catalog_id,))]
    conn.close()
    if not row:
        return None
    lines = product_lines(catalog_id)
    spent = round(sum(l["price"] for l in lines), 2)
    units = sum(max(l["quantity"] or 1, 1) * l["pack_count"] for l in lines)

    variants = defaultdict(lambda: {"purchases": 0, "spent": 0.0, "units": 0})
    for l in lines:
        v = variants[(l["pack_count"], l["chain"])]
        v["purchases"] += 1
        v["spent"] += l["price"]
        v["units"] += max(l["quantity"] or 1, 1) * l["pack_count"]
    variant_list = [{"pack_count": pack, "chain": chain, "purchases": v["purchases"],
                     "spent": round(v["spent"], 2),
                     "unit_price": round(v["spent"] / v["units"], 3) if v["units"] else None}
                    for (pack, chain), v in sorted(variants.items(), key=lambda kv: (kv[0][0], kv[0][1] or ""))]
    priced = [v for v in variant_list if v["unit_price"] and v["unit_price"] > 0]
    best = min(priced, key=lambda v: v["unit_price"]) if priced else None

    # Unit price trend: first vs last quarter of purchases
    unit_prices = [l["unit_price"] for l in lines if l["unit_price"] and l["unit_price"] > 0]
    trend = None
    if len(unit_prices) >= 4:
        q = max(1, len(unit_prices) // 4)
        first, last = sum(unit_prices[:q]) / q, sum(unit_prices[-q:]) / q
        trend = round((last - first) / first * 100, 1) if first else None

    months = metrics.months_in_period()
    return {
        **dict(row),
        "aliases": aliases,
        "variants": variant_list,
        "best_variant": best,
        "purchases": len(lines),
        "units": units,
        "spent": spent,
        "unit_price_avg": round(spent / units, 3) if units else None,
        "unit_price_trend_pct": trend,
        "projection": metrics.projection(spent, months),
        "projection_months": months,
        "lines": lines,
    }


# ------------------------------------------------------------------
# Orchestration
# ------------------------------------------------------------------

def sync(use_ai: bool = True, merge: bool = False) -> Dict:
    """Bring the catalog up to date with all product lines. Safe to run after every import."""
    stats = {"link": link_lines(use_ai)}
    stats["packs"] = estimate_packs()
    if merge and use_ai and ai.available():
        stats["merged"] = merge_duplicates()
        stats["relink"] = link_lines(use_ai)
    stats["labelled"] = label_catalog(only_missing=True, use_ai=use_ai)
    stats["materialised"] = materialise()
    stats["ai_usage"] = ai.usage_report()
    return stats
