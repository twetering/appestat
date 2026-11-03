"""
Receipt import entry point.

Routes each file to the right importer (importers/), categorizes the product lines and
stores the result. Supported:
    - Albert Heijn online invoices   (PDF, 1069835-xxxxx.pdf)
    - Albert Heijn kassabonnen        (PDF, AH_kassabon_*.pdf)
    - PLUS receipt photos            (HEIC/JPG/PNG, e.g. data/invoices/plus/)
"""

import re
from pathlib import Path
from typing import Dict, List, Optional

from database import (
    calculate_file_hash, get_all_product_names, get_connection, get_invoice_by_hash, save_parsed_receipt,
)
from importers import ah_kassabon, ah_online
from importers.ah_kassabon import ABBREVIATION_MAP, expand_abbreviation  # noqa: F401 - re-exported
from importers.plus_photo import IMAGE_EXTENSIONS

SUPPORTED_EXTENSIONS = IMAGE_EXTENSIONS | {".pdf", ".json"}


# ============================================================
# NAME / CATEGORY HELPERS (also used by other modules)
# ============================================================

def normalize_product_name(name: str) -> str:
    """Strip whitespace and the ' BONUS' suffix AH appends to promoted products."""
    name = name.strip()
    if name.upper().endswith(" BONUS"):
        name = name[:-6].strip()
    return name


def categorize(product_name: str) -> Dict[str, Optional[str]]:
    """Category and subcategory for a product name via the categorization service."""
    try:
        from services.categorization import get_categorization_service
        return get_categorization_service().categorize(product_name)
    except Exception:
        return {"category": "Overig", "subcategory": None}


def categorize_product(product_name: str) -> str:
    return categorize(product_name)["category"]


def determine_subcategory(product_name: str, category: str) -> Optional[str]:
    return categorize(product_name).get("subcategory")


def guess_btw(product_name: str) -> str:
    """Guess VAT for receipts that don't print it: 21% for alcohol, soda and non-food, else 9%."""
    name_lower = product_name.lower()
    btw_21_keywords = [
        "leffe", "wijn", "bier", "whisky", "vodka", "rum", "gin",
        "red bull", "cola", "fanta", "sprite", "pepsi", "7up",
        "statiegeld", "plastic", "verpakking",
        "shampoo", "tandpasta", "zeep", "douche", "deo",
        "schoonmaak", "afwasmiddel",
    ]
    for keyword in btw_21_keywords:
        # Match at word start, so 'cola' doesn't hit 'rucola' and 'gin' not 'aubergine'
        if re.search(r"\b" + re.escape(keyword), name_lower):
            return "21%"
    return "9%"


# ============================================================
# PARSING
# ============================================================

def _pdf_text(pdf_path: str) -> str:
    import pdfplumber

    with pdfplumber.open(pdf_path) as pdf:
        return "".join((page.extract_text() or "") + "\n" for page in pdf.pages)


def parse_file(filepath: str, known_names: Optional[List[str]] = None) -> Optional[Dict]:
    """Parse any supported receipt file into an importer result (see importers.common)."""
    path = Path(filepath)
    file_hash = calculate_file_hash(filepath)
    if path.suffix.lower() == ".json":
        from importers import manual
        result = manual.parse_file(filepath, file_hash)
        if result is None:
            return None
    elif path.suffix.lower() in IMAGE_EXTENSIONS:
        from importers import plus_photo
        result = plus_photo.parse_file(filepath, file_hash, known_names)
    else:
        text = _pdf_text(filepath)
        if ah_online.is_online_invoice(text):
            result = ah_online.parse_text(text, path.name, file_hash)
        elif ah_kassabon.is_kassabon(text):
            result = ah_kassabon.parse_text(text, path.name, file_hash)
        else:
            return None
    for p in result["products"]:
        if not p.get("btw"):
            p["btw"] = guess_btw(p["name"])
    return result


def parse_pdf(pdf_path: str) -> Optional[Dict]:
    """Backwards compatible name for parse_file on PDFs."""
    return parse_file(pdf_path)


def categorize_lines(result: Dict) -> None:
    """Fill category/subcategory on each product line."""
    for p in result["products"]:
        # "Actiekorting franse wijn" / "Bonuskorting ..." -> categorize on what the action is about
        label = re.sub(r"^(actiekorting|bonuskorting)\s+", "", p["name"], flags=re.IGNORECASE)
        cat = categorize(label)
        p["category"], p["subcategory"] = cat["category"], cat.get("subcategory")


def _manual_work(invoice_id: int) -> Dict[str, Dict]:
    """User categories and validation flags per product name of an existing invoice."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT original_name, user_category, user_subcategory, is_validated
        FROM products WHERE invoice_id = ?
    """, (invoice_id,)).fetchall()
    conn.close()
    manual = {}
    for r in rows:
        if r["user_category"] or r["user_subcategory"] or r["is_validated"]:
            manual[r["original_name"]] = dict(r)
            # Older imports put the weight in the name: "ZAANLANDER (0.092kg)"
            manual.setdefault(_strip_weight(r["original_name"]), dict(r))
    return manual


def _same_receipt(result: Dict) -> Optional[str]:
    """
    The same receipt can arrive as a different file (a second photo, HEIC vs JPG, a
    re-downloaded PDF). Same source, same moment and same total means same receipt.
    """
    date = result.get("invoice_date")
    if not date or not result.get("total_amount"):
        return None
    when = date.strftime("%Y-%m-%d %H:%M") if hasattr(date, "strftime") else str(date)[:16]
    conn = get_connection()
    row = conn.execute("""
        SELECT filename FROM invoices
        WHERE source_type = ? AND substr(invoice_date, 1, 16) = ? AND ABS(total_amount - ?) < 0.005
          AND file_hash != ?
    """, (result["source_type"], when, result["total_amount"], result["file_hash"])).fetchone()
    if not row and result.get("invoice_number") and result["source_type"] == "ah_online":
        row = conn.execute("SELECT filename FROM invoices WHERE invoice_number = ? AND file_hash != ?",
                           (result["invoice_number"], result["file_hash"])).fetchone()
    conn.close()
    return row["filename"] if row else None


def _resolve_store_number(result: Dict) -> None:
    """A kassabon without a readable header gets 'Albert Heijn #1177'; reuse the name
    other receipts from the same store number (file name suffix) already have."""
    m = re.match(r"Albert Heijn #(\d{4})$", result.get("store") or "")
    if not m:
        return
    conn = get_connection()
    row = conn.execute("""
        SELECT s.name FROM invoices i JOIN stores s ON s.id = i.store_id
        WHERE i.filename LIKE ? ESCAPE '\\' AND s.name NOT LIKE 'Albert Heijn #%' AND i.file_hash != ?
        GROUP BY s.name ORDER BY COUNT(*) DESC LIMIT 1
    """, (f"%\\_{m.group(1)}.pdf", result["file_hash"])).fetchone()
    conn.close()
    if row:
        result["store"] = row["name"]


def _strip_weight(name: str) -> str:
    return re.sub(r"\s*\(\d+[.,]\d+\s*kg\)$", "", name, flags=re.IGNORECASE)


# ============================================================
# IMPORT
# ============================================================

def sync_catalog() -> Optional[Dict]:
    """Link new receipt lines to the product catalog and label new products."""
    try:
        from services import catalog
        return catalog.sync(use_ai=True)
    except Exception as e:  # never fail an import because labelling failed
        print(f"Catalogus bijwerken mislukt: {e}")
        return None


def import_invoice(filepath: str, force: bool = False, known_names: Optional[List[str]] = None,
                   sync: bool = True) -> Dict:
    """
    Import one receipt. With force=True an already imported receipt is re-parsed in place
    (same invoice id); manual categories and validations are kept per product name.
    sync=False skips the catalog update (bulk imports do it once at the end).
    """
    file_hash = calculate_file_hash(filepath)
    existing = get_invoice_by_hash(file_hash)
    if existing and not force:
        return {"status": "skipped", "reason": "Already exists"}

    try:
        result = parse_file(filepath, known_names)
    except Exception as e:
        return {"status": "error", "reason": f"Parse failed: {e}"}
    if result is None:
        return {"status": "error", "reason": "Onbekend bonformaat"}
    if not result.get("invoice_date"):
        return {"status": "error", "reason": "No date found"}

    if not existing:
        duplicate = _same_receipt(result)
        if duplicate:
            return {"status": "skipped", "reason": f"Dezelfde bon staat er al in ({duplicate})",
                    "invoice_id": None}

    categorize_lines(result)
    _resolve_store_number(result)
    if existing:
        manual = _manual_work(existing["id"])
        for p in result["products"]:
            keep = manual.get(p.get("original_name") or p["name"])
            if keep:
                p["user_category"], p["user_subcategory"] = keep["user_category"], keep["user_subcategory"]
                p["is_validated"] = keep["is_validated"]

    try:
        invoice_id = save_parsed_receipt(result, existing["id"] if existing else None)
    except Exception as e:
        return {"status": "error", "reason": str(e)}
    if sync:
        sync_catalog()

    return {
        "status": "success",
        "invoice_id": invoice_id,
        "products_count": len(result["products"]),
        "total_amount": result["total_amount"],
        "source_type": result["source_type"],
        "parse_quality_score": result.get("parse_quality_score"),
        "parse_notes": result.get("parse_notes"),
    }


def reparse_invoice(invoice_id: int, directory: str = "data/invoices") -> Dict:
    """Re-parse one stored invoice from its original file with the right importer."""
    conn = get_connection()
    row = conn.execute("SELECT filename, file_hash FROM invoices WHERE id = ?", (invoice_id,)).fetchone()
    conn.close()
    if not row:
        return {"status": "error", "reason": "Invoice not found"}
    for path in find_receipt_files(directory):
        if path.name == row["filename"] and calculate_file_hash(str(path)) == row["file_hash"]:
            return import_invoice(str(path), force=True, known_names=get_all_product_names())
    return {"status": "error", "reason": f"Origineel bestand {row['filename']} niet gevonden in {directory}"}


def find_receipt_files(directory: str = "data/invoices") -> List[Path]:
    dir_path = Path(directory)
    if not dir_path.exists():
        return []
    return sorted(p for p in dir_path.rglob("*") if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS)


def import_all_invoices(directory: str = "data/invoices", force: bool = False) -> Dict:
    """
    Import all PDF invoices and receipt photos from a directory and its subfolders
    (e.g. data/invoices/plus/). Returns a summary of import results.
    """
    results = {"success": 0, "skipped": 0, "errors": 0, "needs_review": 0, "details": [], "total_amount": 0.0}

    files = find_receipt_files(directory)
    if not files:
        print(f"No receipts found in: {directory}")
        return results
    known_names = get_all_product_names()  # for repairing OCR'd names of photos

    for i, path in enumerate(files, 1):
        result = import_invoice(str(path), force=force, known_names=known_names, sync=False)
        if result["status"] == "success":
            results["success"] += 1
            results["total_amount"] += result.get("total_amount", 0)
            print(f"✓ [{i}/{len(files)}] {path.name}: {result['products_count']} products, "
                  f"€{result.get('total_amount', 0):.2f}")
            if result.get("parse_quality_score") is not None and result["parse_quality_score"] < 1:
                # Imported, but the totals didn't add up - surface it instead of hiding it
                results["needs_review"] += 1
                results["details"].append({"file": path.name, "warning": result.get("parse_notes")})
                print(f"  ⚠ {result.get('parse_notes')}")
        elif result["status"] == "skipped":
            results["skipped"] += 1
        else:
            results["errors"] += 1
            results["details"].append({"file": path.name, "error": result.get("reason", "Unknown error")})
            print(f"✗ [{i}/{len(files)}] {path.name}: {result.get('reason', 'Error')}")

    if results["success"]:
        results["catalog"] = sync_catalog()

    print(f"\n=== Import Summary ===\nSuccess: {results['success']}\nSkipped: {results['skipped']}\n"
          f"Errors: {results['errors']}\nNeeds review: {results['needs_review']}\n"
          f"Total imported: €{results['total_amount']:.2f}")
    return results


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        parsed = parse_file(sys.argv[1])
        if not parsed:
            print("Failed to parse")
        else:
            print(f"{parsed['filename']} | {parsed['source_type']} | {parsed['invoice_date']} | "
                  f"€{parsed['total_amount']:.2f} | quality {parsed['parse_quality_score']} {parsed['parse_notes'] or ''}")
            for p in parsed["products"]:
                print(f"  {p['quantity']}x {p['name']}: €{p['price']:.2f}")
            for e in parsed["extras"]:
                print(f"  [{e['type']}] {e['description']}: €{e['amount']:.2f}")
    else:
        import_all_invoices("data/invoices")
