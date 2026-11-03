"""
Read models for pages (one call per page, same definitions everywhere).

    GET /api/overview?year=&chain=     dashboard: totals, categories, months, stores, top products, budget
    GET /api/years?chain=              year comparison
    GET /api/insights/prices?chain=    unit price trends and pack savings
"""

from flask import Blueprint, jsonify, request

from services import insights

bp = Blueprint("insights_api", __name__, url_prefix="/api")


def _filters():
    year = request.args.get("year") or None
    chain = request.args.get("chain") or None
    return year, chain


@bp.get("/status")
def status():
    """Small counters for the navigation (review badge)."""
    from database import get_connection
    from services import catalog
    conn = get_connection()
    receipts_review = conn.execute(
        "SELECT COUNT(*) FROM invoices WHERE COALESCE(parse_quality_score, 1) < 1").fetchone()[0]
    conn.close()
    return jsonify({"review_products": len(catalog.list_products(needs_review=True, limit=5000)),
                    "review_receipts": receipts_review})


@bp.get("/meta")
def meta():
    """Years and store chains that have data (for filter bars)."""
    from database import get_connection
    conn = get_connection()
    years = [r[0] for r in conn.execute(
        "SELECT DISTINCT strftime('%Y', invoice_date) y FROM invoices WHERE invoice_date IS NOT NULL ORDER BY y DESC")]
    chains = [r[0] for r in conn.execute("SELECT DISTINCT chain FROM stores ORDER BY chain")]
    conn.close()
    return jsonify({"available_years": years, "chains": chains})


@bp.get("/overview")
def overview():
    year, chain = _filters()
    return jsonify(insights.overview(year, chain))


@bp.get("/years")
def years():
    return jsonify(insights.years(request.args.get("chain") or None))


@bp.get("/insights/prices")
def prices():
    return jsonify(insights.price_insights(request.args.get("chain") or None))
