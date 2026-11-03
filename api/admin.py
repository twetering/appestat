"""Maintenance (backup, reset, reimport, migrations) and the optional Albert Heijn app link."""

import time
import urllib.parse

from flask import Blueprint, jsonify, request

from database import get_connection, save_parsed_receipt
from importers.common import make_product, new_result, products_total, reconcile
from parser import import_all_invoices, sync_catalog

bp = Blueprint("admin_api", __name__)


@bp.route("/api/admin/backup")
def api_download_backup():
    """Download database backup"""
    from flask import send_file
    from database import DATABASE_PATH
    return send_file(DATABASE_PATH, as_attachment=True, download_name="appestat_backup.db")


@bp.route("/api/admin/reset", methods=["POST"])
def api_reset_data():
    """
    Reset all invoice and product data.
    Keeps categories, keywords, and settings.
    """
    from database import get_connection
    
    data = request.json or {}
    confirm = data.get("confirm", False)
    
    if not confirm:
        return jsonify({
            "error": "Please set 'confirm': true to reset data",
            "warning": "This will delete ALL invoices and products!"
        }), 400
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Delete all invoice-related data
    cursor.execute("DELETE FROM products")
    products_deleted = cursor.rowcount
    
    cursor.execute("DELETE FROM budget_goals")
    cursor.execute("DELETE FROM budget_alerts")
    
    cursor.execute("DELETE FROM invoices")
    invoices_deleted = cursor.rowcount
    
    conn.commit()
    conn.close()
    
    return jsonify({
        "status": "reset_complete",
        "invoices_deleted": invoices_deleted,
        "products_deleted": products_deleted,
        "message": "All data has been reset. You can now reimport invoices."
    })


@bp.route("/api/admin/reimport", methods=["POST"])
def api_reimport_all():
    """
    Reimport all invoices from data/invoices directory.
    Handles both online invoices and kassabonnen automatically.
    """
    
    results = import_all_invoices("data/invoices", force=False)
    
    return jsonify({
        "status": "success",
        "imported": results.get("success", 0),
        "skipped": results.get("skipped", 0),
        "errors": results.get("errors", 0),
        "needs_review": results.get("needs_review", 0),
        "total_amount": results.get("total_amount", 0),
    })


@bp.route("/api/admin/run-migrations", methods=["POST"])
def api_run_migrations():
    """Run pending database migrations"""
    try:
        from migrations.migration_manager import run_all_migrations
        count = run_all_migrations()
        return jsonify({"status": "success", "migrations_applied": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/ah/auth/url")
def ah_auth_url():
    """Return the AH OAuth URL the user should open to authenticate."""
    from services.ah_api_client import AHApiClient
    return jsonify({"url": AHApiClient.get_auth_url()})


@bp.route("/api/ah/auth/exchange", methods=["POST"])
def ah_auth_exchange():
    """Exchange an authorization code (or full redirect URL) for tokens."""
    from services.ah_api_client import AHApiClient
    data = request.get_json() or {}
    code = (data.get("code") or "").strip()

    # Accept full redirect URL: appie://login-exit?code=XXXX
    if "?" in code:
        params = urllib.parse.parse_qs(urllib.parse.urlparse(code).query)
        code = params.get("code", [code])[0]

    if not code:
        return jsonify({"error": "Code is vereist"}), 400

    try:
        client = AHApiClient()
        client.exchange_code(code)
        return jsonify({"status": "ok", "message": "Succesvol gekoppeld met Albert Heijn"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@bp.route("/api/ah/auth/status")
def ah_auth_status():
    """Check whether the user is authenticated with AH."""
    from services.ah_api_client import AHApiClient
    client = AHApiClient()
    return jsonify({
        "authenticated": client.is_authenticated,
        "token_expired": client._is_token_expired() if client.is_authenticated else None,
    })


@bp.route("/api/ah/auth", methods=["DELETE"])
def ah_auth_logout():
    """Disconnect from Albert Heijn (clear stored tokens)."""
    from services.ah_api_client import AHApiClient
    AHApiClient().clear_tokens()
    return jsonify({"status": "ok", "message": "Ontkoppeld van Albert Heijn"})


@bp.route("/api/ah/sync", methods=["POST"])
def ah_sync():
    """
    Sync receipts from the AH API into Appestat.

    Body (optional JSON):
      full_sync   bool  – if true, keep going even after 3 consecutive known receipts
      batch_size  int   – receipts per API page (default 50)
    """
    from services.ah_api_client import AHApiClient
    from sources.ah_api import AHApiSource

    body = request.get_json() or {}
    full_sync = bool(body.get("full_sync", False))
    batch_size = int(body.get("batch_size", 50))

    client = AHApiClient()
    if not client.is_authenticated:
        return jsonify({"error": "Niet gekoppeld met Albert Heijn. Koppel eerst je account."}), 401

    source = AHApiSource()

    # Pre-fetch existing API receipt file_hashes for fast dedup
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT file_hash FROM invoices WHERE source_type = 'ah_api'")
    existing_hashes = {row["file_hash"] for row in cursor.fetchall()}
    conn.close()

    synced = []
    skipped = 0
    errors = []
    consecutive_existing = 0
    offset = 0
    caught_up = False

    while True:
        try:
            page = client.get_receipts(offset=offset, limit=batch_size)
        except Exception as e:
            errors.append({"error": f"Ophalen mislukt bij offset {offset}: {e}"})
            break

        receipts = page.get("posReceipts", [])
        if not receipts:
            break

        for summary in receipts:
            receipt_id = summary.get("id")
            if not receipt_id:
                continue

            import hashlib as _hashlib
            fh = _hashlib.md5(f"ah_api:{receipt_id}".encode()).hexdigest()

            if fh in existing_hashes:
                skipped += 1
                consecutive_existing += 1
                if not full_sync and consecutive_existing >= 3:
                    caught_up = True  # incremental sync: the rest is already imported
                    break
                continue

            consecutive_existing = 0
            time.sleep(0.3)  # Rate limiting

            try:
                receipt_details = client.get_receipt(receipt_id)
                if not receipt_details:
                    errors.append({"receipt_id": receipt_id, "error": "Lege bongegevens ontvangen"})
                    continue

                parsed = source.receipt_to_parsed(receipt_details)

                # Same storage path as file imports: store, product lines, reconciliation
                result = new_result("ah_api", parsed.filename, parsed.file_hash)
                result.update({
                    "store": "Albert Heijn", "invoice_number": parsed.invoice_number, "invoice_date": parsed.date,
                    "total_amount": parsed.total_amount, "total_savings": parsed.total_savings,
                    "external_id": parsed.external_id,
                })
                for p in parsed.products:
                    line = make_product(p.name, p.quantity or 1, p.price, btw=p.btw_percentage)
                    line["category"], line["subcategory"] = p.category, p.subcategory
                    result["products"].append(line)
                reconcile(result, [("totaal", products_total(result), parsed.total_amount or None)])
                save_parsed_receipt(result)

                existing_hashes.add(fh)
                synced.append({
                    "receipt_id": receipt_id,
                    "date": parsed.date.isoformat() if parsed.date else None,
                    "total": parsed.total_amount,
                    "products": len(parsed.products),
                })

            except Exception as e:
                errors.append({"receipt_id": receipt_id, "error": str(e)})

        if caught_up:
            break
        pagination = page.get("pagination", {})
        total_elements = pagination.get("totalElements", 0)
        offset += batch_size
        if offset >= total_elements:
            break

        time.sleep(0.3)

    if synced:
        sync_catalog()
    return jsonify({
        "status": "ok",
        "synced": len(synced),
        "skipped": skipped,
        "errors": errors,
        "message": f"{len(synced)} nieuwe bonnen gesynchroniseerd, {skipped} overgeslagen",
        "details": synced,
    })

