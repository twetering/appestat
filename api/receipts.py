"""
Receipts (bonnen), upload and export.

    GET  /api/receipts?year=&chain=&q=&status=review
    GET  /api/receipts/<id>                       lines, extras, reconciliation
    GET  /api/receipts/<id>/text                  original text
    POST /api/receipts/<id>/validate              {validated: bool}
    POST /api/receipts/<id>/reparse               re-read from the original file
    POST /api/upload                              multipart files[] -> saved under data/invoices, imported
    GET  /api/export/<bonnen|regels|producten|bijkomend>.csv
"""

import re
from pathlib import Path

from flask import Blueprint, Response, jsonify, request

from services import receipts

bp = Blueprint("receipts_api", __name__, url_prefix="/api")

INVOICES_DIR = Path(__file__).resolve().parent.parent / "data" / "invoices"
UPLOAD_EXTENSIONS = {".pdf", ".heic", ".heif", ".jpg", ".jpeg", ".png"}


@bp.get("/receipts")
def list_receipts():
    a = request.args
    return jsonify(receipts.list_receipts(a.get("year") or None, a.get("chain") or None,
                                          a.get("q", "").strip(), a.get("status", "")))


@bp.get("/receipts/<int:invoice_id>")
def receipt_detail(invoice_id):
    d = receipts.receipt_detail(invoice_id)
    return (jsonify(d), 200) if d else (jsonify({"error": "Bon niet gevonden"}), 404)


@bp.get("/receipts/<int:invoice_id>/text")
def receipt_text(invoice_id):
    text = receipts.raw_text(invoice_id)
    return (jsonify({"text": text}), 200) if text is not None else (jsonify({"error": "Bon niet gevonden"}), 404)


@bp.post("/receipts/<int:invoice_id>/validate")
def validate(invoice_id):
    data = request.get_json(silent=True) or {}
    return jsonify({"updated": receipts.set_validated(invoice_id, data.get("validated", True))})


@bp.post("/receipts/<int:invoice_id>/reparse")
def reparse(invoice_id):
    from parser import reparse_invoice
    result = reparse_invoice(invoice_id)
    code = 200 if result["status"] == "success" else 400
    return jsonify(result), code


def _safe_name(name: str) -> str:
    name = Path(name).name
    return re.sub(r"[^A-Za-z0-9 ._()-]", "_", name).strip() or "bon"


@bp.post("/upload")
def upload():
    """Save uploaded receipts into data/invoices (photos in fotos/) and import them."""
    from parser import import_invoice, sync_catalog
    from database import get_all_product_names

    files = request.files.getlist("files")
    if not files:
        return jsonify({"error": "Geen bestanden ontvangen"}), 400
    known = get_all_product_names()
    results = []
    for f in files:
        name = _safe_name(f.filename or "")
        ext = Path(name).suffix.lower()
        if ext not in UPLOAD_EXTENSIONS:
            results.append({"file": name, "status": "error", "reason": "Alleen PDF of foto (HEIC/JPG/PNG)"})
            continue
        target_dir = INVOICES_DIR / ("fotos" if ext != ".pdf" else "")
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / name
        n = 1
        while target.exists():
            target = target_dir / f"{Path(name).stem}-{n}{ext}"
            n += 1
        f.save(target)
        result = import_invoice(str(target), sync=False, known_names=known)
        if result["status"] == "skipped":
            target.unlink(missing_ok=True)  # duplicate of a receipt we already have
        results.append({"file": name, **result})
    if any(r["status"] == "success" for r in results):
        sync_catalog()
    return jsonify({"results": results})


@bp.post("/receipts/manual")
def add_manual():
    """Purchase without a receipt file (webshop order): {chain, date, total, product?, note?, lines?}."""
    from importers.manual import write_purchase
    from parser import import_invoice

    data = request.get_json(silent=True) or {}
    try:
        chain = str(data.get("chain") or "").strip()
        if not chain:
            raise ValueError("winkel ontbreekt")
        total = float(data["total"])
        path = write_purchase(chain, str(data.get("date") or ""), total, data.get("product"),
                              data.get("note"), data.get("lines"), data.get("order_number"),
                              directory=INVOICES_DIR / "handmatig")
    except FileExistsError:
        return jsonify({"status": "skipped", "reason": "Deze aankoop staat er al in"})
    except (KeyError, TypeError, ValueError) as e:
        return jsonify({"error": f"Ongeldige aankoop: {e}"}), 400
    result = import_invoice(str(path))
    if result["status"] == "skipped":
        path.unlink(missing_ok=True)
    return jsonify(result), 200 if result["status"] != "error" else 400


@bp.get("/export/<kind>.csv")
def export(kind):
    data = receipts.export_csv(kind)
    if data is None:
        return jsonify({"error": "Onbekend export-type"}), 404
    return Response("﻿" + data, mimetype="text/csv; charset=utf-8",
                    headers={"Content-Disposition": f"attachment; filename=appestat-{kind}.csv"})
