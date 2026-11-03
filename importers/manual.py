"""
Manual purchases: food bought outside the supermarket receipts (e.g. Jimmy Joy webshop orders)
stored as one small JSON file per purchase under data/invoices/handmatig/. Keeping them as files
means re-imports, resets and backups treat them exactly like PDFs and photos.

    {"chain": "Jimmy Joy", "date": "2026-10-05", "total": 153.80,
     "product": "Jimmy Joy bestelling", "note": "1 x Plenny Shake Wild Berries + 7 more",
     "lines": [{"name": "Plenny Shake Wild Berries", "quantity": 1, "price": 23.47}]}   # lines optional
"""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from importers.common import make_product, new_result, products_total, reconcile

MANUAL_DIR = Path("data/invoices/handmatig")


def parse_file(filepath: str, file_hash: str) -> Optional[Dict]:
    try:
        data = json.loads(Path(filepath).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or not data.get("chain") or not data.get("date") or "total" not in data:
        return None

    result = new_result("manual", Path(filepath).name, file_hash)
    result["chain"] = str(data["chain"]).strip()
    result["store"] = (data.get("store") or result["chain"]).strip()
    result["invoice_date"] = datetime.strptime(str(data["date"])[:10], "%Y-%m-%d")
    result["invoice_number"] = data.get("order_number")
    result["total_amount"] = round(float(data["total"]), 2)
    result["raw_text"] = json.dumps(data, ensure_ascii=False, indent=2)

    lines = data.get("lines") or [{"name": data.get("product") or f"{result['chain']} bestelling",
                                   "quantity": 1, "price": result["total_amount"]}]
    for line in lines:
        result["products"].append(make_product(str(line["name"]).strip(), int(line.get("quantity") or 1),
                                               float(line["price"])))
    reconcile(result, [("totaal", products_total(result), result["total_amount"])])
    return result


def write_purchase(chain: str, date: str, total: float, product: Optional[str] = None,
                   note: Optional[str] = None, lines: Optional[List[Dict]] = None,
                   order_number: Optional[str] = None, directory: Path = MANUAL_DIR) -> Path:
    """Write one manual purchase file and return its path (import it with parser.import_invoice).
    The same chain + date + total is treated as the same purchase (FileExistsError)."""
    datetime.strptime(date[:10], "%Y-%m-%d")  # validate early
    slug = re.sub(r"[^a-z0-9]+", "-", chain.lower()).strip("-") or "aankoop"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{date[:10]}_{slug}_{round(float(total) * 100)}.json"
    if path.exists():
        raise FileExistsError(f"{path.name} bestaat al")
    data = {"chain": chain, "date": date[:10], "total": round(float(total), 2)}
    for key, value in (("product", product), ("note", note), ("lines", lines), ("order_number", order_number)):
        if value:
            data[key] = value
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
