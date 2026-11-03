"""
PLUS receipt photos (HEIC/JPG/PNG) -> importer result.

OCR, row reconstruction and the OpenAI fallback live in sources/plus_receipt.py; this
module maps that onto the shared importer shape: PLUSPunten become savings, statiegeld
becomes a deposit extra, and OCR'd names are repaired against known product names.
"""

import re
from typing import Dict, List, Optional

from importers.common import make_extra, make_product, new_result, products_total, extras_total, reconcile
from sources.plus_receipt import (
    IMAGE_EXTENSIONS, PlusReceiptSource, build_vocabulary, correct_ocr_name, display_name,
)

__all__ = ["IMAGE_EXTENSIONS", "parse_file"]


def _repair_names(raw: str, vocab: Dict[str, int], by_compact: Dict[str, str]) -> str:
    raw = correct_ocr_name(raw, vocab)
    known = by_compact.get(re.sub(r"[^A-Z0-9]", "", raw))
    return known if known and known.isupper() else raw


def parse_file(image_path: str, file_hash: str, known_names: Optional[List[str]] = None) -> Dict:
    receipt = PlusReceiptSource().parse(image_path)
    meta = receipt.source_metadata or {}
    result = new_result("plus_receipt", receipt.filename, receipt.file_hash or file_hash)
    result["store"] = meta.get("store") or "PLUS"
    result["invoice_number"] = receipt.invoice_number
    result["invoice_date"] = receipt.date
    result["total_amount"] = receipt.total_amount
    result["raw_text"] = receipt.raw_text
    result["source_metadata"] = meta
    result["parse_notes"] = receipt.parse_notes

    known_names = known_names or []
    vocab = build_vocabulary(known_names)
    by_compact: Dict[str, str] = {}
    for name in known_names:
        by_compact.setdefault(re.sub(r"[^A-Z0-9]", "", name.upper()), name)

    for p in receipt.products:
        raw = p.original_name_raw or p.name
        if raw.upper().startswith("PLUSPUNTEN"):
            result["extras"].append(make_extra("savings", "PLUSPunten digitaal", p.price, p.quantity))
            continue
        raw = _repair_names(raw, vocab, by_compact)
        gross = round(p.price + (p.discount_amount or 0), 2)
        line = make_product(display_name(raw), p.quantity, gross, unit_price=p.unit_price,
                            original_name=raw)
        line["discount"] = round(p.discount_amount or 0, 2)
        line["price"] = round(p.price, 2)
        if raw.startswith("ACTIEKORTING"):
            line["is_discount_line"] = True
        result["products"].append(line)

    if meta.get("deposits"):
        result["extras"].append(make_extra("deposit", "Statiegeld", meta["deposits"]))
    result["total_savings"] = receipt.total_savings

    computed = round(products_total(result) + extras_total(result), 2)
    reconcile(result, [("totaal", computed, receipt.total_amount or None)])
    if receipt.parse_quality_score is not None and receipt.parse_quality_score < 1:
        result["parse_quality_score"] = min(result["parse_quality_score"], receipt.parse_quality_score)
    return result
