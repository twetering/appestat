"""
Metrics: the single definition of every number Appestat shows.

Per receipt:   paid = groceries (product lines) + extra costs + savings stamps
               spent = paid - savings stamps
    groceries    product lines, net of discounts (incl. negative discount lines)
    extra costs  statiegeld/verpakking, bezorgkosten, abonnementen  (invoice_extras)
    savings      koopzegels, PLUSPunten, Miles ingewisseld         (not spending)
    bonus        discounts received (invoices.total_savings)

Category totals = groceries per category + one "Bijkomende kosten" bucket, so the
categories always add up to "spent". Averages per month divide by calendar months in
the period (not "months that happen to have data"). All functions take the same
filters: year (str/int or None for everything) and chain ('Albert Heijn', 'PLUS' or None).
"""

from datetime import date, datetime
from typing import Dict, List, Optional, Tuple

from database import get_connection

EXTRA_COSTS_CATEGORY = "Bijkomende kosten"
EFFECTIVE_CATEGORY = "COALESCE(p.user_category, p.auto_category, 'Overig')"
EFFECTIVE_SUBCATEGORY = "COALESCE(p.user_subcategory, p.auto_subcategory)"
PRODUCT_NAME = "COALESCE(cp.name, p.display_name, p.original_name)"


# ------------------------------------------------------------------
# Filters
# ------------------------------------------------------------------

def _filters(year=None, chain: Optional[str] = None, alias: str = "i") -> Tuple[str, list]:
    clauses, params = [], []
    if year:
        clauses.append(f"strftime('%Y', {alias}.invoice_date) = ?")
        params.append(str(year))
    if chain:
        clauses.append(f"{alias}.store_id IN (SELECT id FROM stores WHERE chain = ?)")
        params.append(chain)
    return (" AND " + " AND ".join(clauses)) if clauses else "", params


def _rows(sql: str, params: list) -> List[Dict]:
    conn = get_connection()
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def _one(sql: str, params: list) -> Dict:
    rows = _rows(sql, params)
    return rows[0] if rows else {}


# ------------------------------------------------------------------
# Period
# ------------------------------------------------------------------

def _month_index(d: date) -> int:
    return d.year * 12 + d.month - 1


def data_range(chain: Optional[str] = None) -> Tuple[Optional[date], Optional[date]]:
    where, params = _filters(None, chain)
    row = _one(f"SELECT MIN(i.invoice_date) first, MAX(i.invoice_date) last FROM invoices i WHERE 1=1 {where}",
               params)
    to_date = lambda v: datetime.fromisoformat(str(v).replace("T", " ")[:19]).date() if v else None
    return to_date(row.get("first")), to_date(row.get("last"))


def months_in_period(year=None, chain: Optional[str] = None, today: Optional[date] = None) -> int:
    """
    Calendar months the period covers: from the first month with data (or January) to the
    month of the latest receipt (or December). Receipts are imported in batches, so the
    latest receipt marks how far the data is complete. A year with data only in December
    counts as 1 month; a month without shopping inside the period still counts.
    """
    today = today or date.today()
    first, last = data_range(chain)
    if not first:
        return 1
    up_to = min(today, last)
    if year:
        year = int(year)
        start = max(_month_index(date(year, 1, 1)), _month_index(first))
        end = min(_month_index(date(year, 12, 1)), _month_index(up_to))
    else:
        start, end = _month_index(first), _month_index(up_to)
    return max(1, end - start + 1)


def period_is_complete(year, today: Optional[date] = None) -> bool:
    today = today or date.today()
    first, _ = data_range()
    return bool(first) and int(year) < today.year and first <= date(int(year), 1, 31)


# ------------------------------------------------------------------
# Core numbers
# ------------------------------------------------------------------

def summary(year=None, chain: Optional[str] = None) -> Dict:
    where, params = _filters(year, chain)
    inv = _one(f"""
        SELECT COUNT(*) receipts, COALESCE(SUM(i.total_amount), 0) paid,
               COALESCE(SUM(i.total_savings), 0) bonus, MIN(i.invoice_date) first, MAX(i.invoice_date) last
        FROM invoices i WHERE 1=1 {where}
    """, params)
    groceries = _one(f"""
        SELECT COALESCE(SUM(p.price), 0) total FROM products p JOIN invoices i ON i.id = p.invoice_id
        WHERE 1=1 {where}
    """, params)["total"]
    extras = {r["extra_type"]: r["total"] for r in _rows(f"""
        SELECT e.extra_type, SUM(e.amount) total FROM invoice_extras e JOIN invoices i ON i.id = e.invoice_id
        WHERE 1=1 {where} GROUP BY e.extra_type
    """, params)}
    savings_stamps = extras.get("savings", 0.0)
    extra_costs = sum(v for k, v in extras.items() if k != "savings")
    spent = inv["paid"] - savings_stamps
    months = months_in_period(year, chain)
    receipts = inv["receipts"] or 0
    return {
        "receipts": receipts,
        "paid": round(inv["paid"], 2),
        "spent": round(spent, 2),
        "groceries": round(groceries, 2),
        "extra_costs": round(extra_costs, 2),
        "extra_costs_by_type": {k: round(v, 2) for k, v in extras.items() if k != "savings"},
        "savings_stamps": round(savings_stamps, 2),
        "bonus_savings": round(inv["bonus"], 2),
        "unexplained": round(spent - groceries - extra_costs, 2),  # 0 when every receipt reconciles
        "months": months,
        "avg_per_month": round(spent / months, 2),
        "avg_per_receipt": round(spent / receipts, 2) if receipts else 0.0,
        "first_date": inv["first"],
        "last_date": inv["last"],
    }


def category_totals(year=None, chain: Optional[str] = None) -> Dict[str, Dict]:
    """{category: {'total', 'count'}} incl. the 'Bijkomende kosten' bucket; sums to spent."""
    where, params = _filters(year, chain)
    totals = {r["category"]: {"total": round(r["total"], 2), "count": r["count"]} for r in _rows(f"""
        SELECT {EFFECTIVE_CATEGORY} category, SUM(p.price) total, COUNT(*) count
        FROM products p JOIN invoices i ON i.id = p.invoice_id
        WHERE 1=1 {where} GROUP BY category
    """, params)}
    extra = _one(f"""
        SELECT COALESCE(SUM(e.amount), 0) total, COUNT(*) count
        FROM invoice_extras e JOIN invoices i ON i.id = e.invoice_id
        WHERE e.extra_type != 'savings' {where}
    """, params)
    if extra.get("count"):
        totals[EXTRA_COSTS_CATEGORY] = {"total": round(extra["total"], 2), "count": extra["count"]}
    return dict(sorted(totals.items(), key=lambda kv: -kv[1]["total"]))


def subcategory_totals(year=None, chain: Optional[str] = None) -> Dict[str, Dict[str, Dict]]:
    where, params = _filters(year, chain)
    out: Dict[str, Dict[str, Dict]] = {}
    for r in _rows(f"""
        SELECT {EFFECTIVE_CATEGORY} category, {EFFECTIVE_SUBCATEGORY} subcategory,
               SUM(p.price) total, COUNT(*) count
        FROM products p JOIN invoices i ON i.id = p.invoice_id
        WHERE {EFFECTIVE_SUBCATEGORY} IS NOT NULL {where}
        GROUP BY category, subcategory ORDER BY category, total DESC
    """, params):
        out.setdefault(r["category"], {})[r["subcategory"]] = {"total": round(r["total"], 2), "count": r["count"]}
    return out


def _spent_per(group_sql: str, year=None, chain: Optional[str] = None) -> Dict[str, float]:
    where, params = _filters(year, chain)
    return {r["k"]: round(r["spent"], 2) for r in _rows(f"""
        SELECT {group_sql} k, SUM(i.total_amount - COALESCE(sv.amount, 0)) spent
        FROM invoices i
        LEFT JOIN (SELECT invoice_id, SUM(amount) amount FROM invoice_extras
                   WHERE extra_type = 'savings' GROUP BY invoice_id) sv ON sv.invoice_id = i.id
        WHERE 1=1 {where} GROUP BY k ORDER BY k
    """, params)}


def monthly_spent(year=None, chain: Optional[str] = None) -> Dict[str, float]:
    return _spent_per("strftime('%Y-%m', i.invoice_date)", year, chain)


def yearly_spent(chain: Optional[str] = None) -> Dict[str, float]:
    return _spent_per("strftime('%Y', i.invoice_date)", None, chain)


def spent_by_store(year=None) -> List[Dict]:
    where, params = _filters(year, None)
    return _rows(f"""
        SELECT s.chain, s.name store, COUNT(i.id) receipts,
               ROUND(SUM(i.total_amount - COALESCE(sv.amount, 0)), 2) spent
        FROM invoices i JOIN stores s ON s.id = i.store_id
        LEFT JOIN (SELECT invoice_id, SUM(amount) amount FROM invoice_extras
                   WHERE extra_type = 'savings' GROUP BY invoice_id) sv ON sv.invoice_id = i.id
        WHERE 1=1 {where} GROUP BY s.id ORDER BY spent DESC
    """, params)


INVEST_RATE = 0.05  # yearly return for "what if you had invested it instead"


def projection(total: float, months: int) -> Dict[str, float]:
    """
    The one projection formula: average per calendar month in the period, times 12 and 360.
    thirty_years_invested: the same monthly amount deposited for 30 years at INVEST_RATE a year,
    compounded monthly (future value of an annuity).
    """
    monthly = total / max(months, 1)
    r, n = INVEST_RATE / 12, 360
    return {"monthly": round(monthly, 2), "yearly": round(monthly * 12, 2), "thirty_years": round(monthly * 360, 2),
            "thirty_years_invested": round(monthly * ((1 + r) ** n - 1) / r, 2)}


def top_products(year=None, chain: Optional[str] = None, limit: int = 20) -> List[Dict]:
    """Products ranked by spend; grouped per catalog product when linked, else per name."""
    where, params = _filters(year, chain)
    return _rows(f"""
        SELECT {PRODUCT_NAME} name, p.catalog_id catalog_id,
               {EFFECTIVE_CATEGORY} category, {EFFECTIVE_SUBCATEGORY} subcategory,
               SUM(p.quantity) count, COUNT(*) purchases, ROUND(SUM(p.price), 2) total
        FROM products p JOIN invoices i ON i.id = p.invoice_id
        LEFT JOIN catalog_products cp ON cp.id = p.catalog_id
        WHERE p.price > 0 {where}
        GROUP BY COALESCE('c' || p.catalog_id, 'n' || p.original_name)
        ORDER BY total DESC LIMIT ?
    """, params + [limit])


def bonus(year=None, chain: Optional[str] = None) -> Dict:
    where, params = _filters(year, chain)
    row = _one(f"""
        SELECT COALESCE(SUM(i.total_savings), 0) total, SUM(i.total_savings > 0) receipts,
               (SELECT COUNT(*) FROM products p JOIN invoices i ON i.id = p.invoice_id
                WHERE p.discount_amount > 0 {where}) items
        FROM invoices i WHERE 1=1 {where}
    """, params + params)
    return {"total_bonus_savings": round(row["total"], 2), "invoices_with_bonus": row["receipts"] or 0,
            "bonus_items_count": row["items"] or 0}


def day_of_week(year=None, chain: Optional[str] = None) -> Dict:
    names = ["Zondag", "Maandag", "Dinsdag", "Woensdag", "Donderdag", "Vrijdag", "Zaterdag"]
    spent = _spent_per("cast(strftime('%w', i.invoice_date) as integer)", year, chain)
    where, params = _filters(year, chain)
    counts = {r["d"]: r["n"] for r in _rows(f"""
        SELECT cast(strftime('%w', i.invoice_date) as integer) d, COUNT(*) n FROM invoices i
        WHERE 1=1 {where} GROUP BY d
    """, params)}
    by_day = [{"day_name": names[d], "day_num": d, "invoice_count": counts.get(d, 0),
               "total_spent": spent.get(d, 0.0),
               "avg_per_visit": round(spent.get(d, 0.0) / counts[d], 2) if counts.get(d) else 0.0}
              for d in range(7) if counts.get(d)]
    if not by_day:
        return {"by_day": [], "insights": {}}
    hi, lo = max(by_day, key=lambda r: r["total_spent"]), min(by_day, key=lambda r: r["total_spent"])
    return {"by_day": by_day, "insights": {
        "max_spending_day": hi["day_name"], "max_spending_amount": hi["total_spent"],
        "min_spending_day": lo["day_name"], "min_spending_amount": lo["total_spent"]}}


def prediction(year=None, chain: Optional[str] = None) -> Dict:
    year = int(year or date.today().year)
    s = summary(year, chain)
    return {"predicted_monthly": s["avg_per_month"], "predicted_yearly": round(s["avg_per_month"] * 12, 2),
            "months_with_data": s["months"], "year": str(year)}
