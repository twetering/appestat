"""
Page-level read models built on services.metrics and services.catalog: one call returns
everything a page needs, computed with the same definitions everywhere.
"""

from collections import defaultdict
from datetime import date
from statistics import median
from typing import Dict, List, Optional

from database import get_connection
from services import catalog, metrics


def _categories_meta() -> Dict[str, Dict]:
    conn = get_connection()
    rows = conn.execute("SELECT name, icon, color, sort_order FROM categories").fetchall()
    conn.close()
    return {r["name"]: dict(r) for r in rows}


def category_breakdown(year=None, chain: Optional[str] = None) -> List[Dict]:
    meta = _categories_meta()
    totals = metrics.category_totals(year, chain)
    subs = metrics.subcategory_totals(year, chain)
    spent = sum(v["total"] for v in totals.values()) or 1
    out = []
    for name, v in totals.items():
        m = meta.get(name, {})
        out.append({
            "name": name, "icon": m.get("icon") or "📋", "color": m.get("color") or "#9E9E9E",
            "total": v["total"], "count": v["count"], "share": round(v["total"] / spent * 100, 1),
            "subcategories": [{"name": s, "total": d["total"], "count": d["count"]}
                              for s, d in sorted(subs.get(name, {}).items(), key=lambda kv: -kv[1]["total"])],
        })
    return out


def budget_status(year: int) -> Optional[Dict]:
    from services.budget import get_budget_service
    d = get_budget_service().get_budget_dashboard(int(year))
    if not d.get("total_budget"):
        return None
    return {k: d.get(k) for k in ("total_budget", "total_spent", "expected_by_now", "on_track",
                                   "months_elapsed", "projected_annual")}


def overview(year=None, chain: Optional[str] = None) -> Dict:
    s = metrics.summary(year, chain)
    top = metrics.top_products(year, chain, limit=15)
    for p in top:
        p["projection"] = metrics.projection(p["total"], s["months"])
    yearly = metrics.yearly_spent(chain)
    current = date.today().year
    return {
        "filters": {"year": year, "chain": chain},
        "summary": s,
        "categories": category_breakdown(year, chain),
        "monthly": metrics.monthly_spent(year, chain),
        "yearly": [{"year": y, "spent": v, "complete": metrics.period_is_complete(y)} for y, v in yearly.items()],
        "stores": metrics.spent_by_store(year),
        "top_products": top,
        "day_of_week": metrics.day_of_week(year, chain),
        "prediction": metrics.prediction(year or current, chain) if (not year or int(year) == current) else None,
        "budget": budget_status(year or current) if (not year or int(year) == current) and not chain else None,
        "available_years": sorted(yearly.keys(), reverse=True),
        "chains": [r["chain"] for r in get_connection().execute("SELECT DISTINCT chain FROM stores ORDER BY chain")],
        "review_count": len(catalog.list_products(needs_review=True, limit=2000)),
    }


def years(chain: Optional[str] = None) -> Dict:
    yearly = metrics.yearly_spent(chain)
    out = []
    for y in sorted(yearly):
        s = metrics.summary(y, chain)
        out.append({"year": y, "spent": s["spent"], "groceries": s["groceries"], "extra_costs": s["extra_costs"],
                    "savings_stamps": s["savings_stamps"], "bonus": s["bonus_savings"], "receipts": s["receipts"],
                    "months": s["months"], "avg_per_month": s["avg_per_month"],
                    "complete": metrics.period_is_complete(y)})
    by_cat: Dict[str, Dict[str, float]] = defaultdict(dict)
    for y in yearly:
        for name, v in metrics.category_totals(y, chain).items():
            by_cat[name][y] = v["total"]
    monthly: Dict[str, Dict[str, float]] = defaultdict(dict)
    for key, v in metrics.monthly_spent(None, chain).items():
        monthly[key[:4]][key[5:]] = v
    meta = _categories_meta()
    return {
        "years": out,
        "categories": [{"name": n, "icon": meta.get(n, {}).get("icon") or "📋",
                        "color": meta.get(n, {}).get("color") or "#9E9E9E", "by_year": v}
                       for n, v in sorted(by_cat.items(), key=lambda kv: -sum(kv[1].values()))],
        "monthly": monthly,
    }


def price_insights(chain: Optional[str] = None, min_purchases: int = 5) -> Dict:
    """
    Price per piece over time per catalog product, and what choosing the cheapest pack
    of products you buy in several packs would have saved.
    """
    clauses, params = ["p.catalog_id IS NOT NULL", "p.price > 0", "p.weight_kg IS NULL"], []
    if chain:
        clauses.append("s.chain = ?")
        params.append(chain)
    conn = get_connection()
    rows = conn.execute(f"""
        SELECT p.catalog_id, p.alias_key, cp.name, cp.category, i.invoice_date d, s.chain,
               p.price / (MAX(COALESCE(p.quantity, 1), 1) * COALESCE(p.pack_count, a.pack_count, 1)) unit,
               MAX(COALESCE(p.quantity, 1), 1) * COALESCE(p.pack_count, a.pack_count, 1) pieces,
               COALESCE(p.pack_count, a.pack_count, 1) pack, p.price
        FROM products p JOIN invoices i ON i.id = p.invoice_id JOIN catalog_products cp ON cp.id = p.catalog_id
        LEFT JOIN stores s ON s.id = i.store_id LEFT JOIN product_aliases a ON a.alias_key = p.alias_key
        WHERE {' AND '.join(clauses)} ORDER BY i.invoice_date
    """, params).fetchall()
    conn.close()

    by_product, by_sku = defaultdict(list), defaultdict(list)
    for r in rows:
        by_product[r["catalog_id"]].append(r)
        by_sku[(r["catalog_id"], r["alias_key"])].append(r)

    # Price trend per exact receipt name (one SKU); a >100% jump is almost always a different
    # pack size sold under the same short name, not inflation.
    changes = []
    for (pid, _), lines in by_sku.items():
        if len(lines) < min_purchases:
            continue
        k = max(2, len(lines) // 4)
        first, last = median(l["unit"] for l in lines[:k]), median(l["unit"] for l in lines[-k:])
        change = (last - first) / first * 100 if first > 0 else 0
        if first > 0 and abs(change) <= 100:
            changes.append({"id": pid, "name": lines[0]["name"], "category": lines[0]["category"],
                            "purchases": len(lines), "first_unit_price": round(first, 3),
                            "last_unit_price": round(last, 3), "change_pct": round(change, 1),
                            "first_date": lines[0]["d"], "last_date": lines[-1]["d"]})
    best_change = {}
    for c in changes:  # one row per product: its most-bought SKU
        if c["id"] not in best_change or c["purchases"] > best_change[c["id"]]["purchases"]:
            best_change[c["id"]] = c
    changes = list(best_change.values())

    # Pack choice: only real multipacks (cans, bottles, rolls), compared per piece
    packs = []
    for pid, lines in by_product.items():
        by_pack = defaultdict(lambda: [0.0, 0])
        for l in lines:
            by_pack[l["pack"]][0] += l["price"]
            by_pack[l["pack"]][1] += l["pieces"]
        if len(by_pack) < 2 or max(by_pack) < 4:
            continue
        unit_prices = {k: v[0] / v[1] for k, v in by_pack.items() if v[1]}
        best = min(unit_prices, key=unit_prices.get)
        pieces = sum(v[1] for v in by_pack.values())
        spent = sum(v[0] for v in by_pack.values())
        saving = spent - pieces * unit_prices[best]
        if saving >= 1:
            packs.append({"id": pid, "name": lines[0]["name"], "pieces": pieces, "spent": round(spent, 2),
                          "best_pack": best, "best_unit_price": round(unit_prices[best], 3),
                          "avg_unit_price": round(spent / pieces, 3), "could_save": round(saving, 2),
                          "packs": {str(k): round(v, 3) for k, v in sorted(unit_prices.items())}})

    changes.sort(key=lambda c: -c["change_pct"])
    packs.sort(key=lambda p: -p["could_save"])
    return {"rising": [c for c in changes if c["change_pct"] >= 5][:15],
            "falling": sorted([c for c in changes if c["change_pct"] <= -5], key=lambda c: c["change_pct"])[:10],
            "pack_savings": packs[:15],
            "pack_savings_total": round(sum(p["could_save"] for p in packs), 2)}
