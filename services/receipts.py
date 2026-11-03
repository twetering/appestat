"""Receipt (bon) read models and actions for the v3 UI."""

import csv
import io
from typing import Dict, List, Optional

from database import get_connection
from importers.common import EXTRA_TYPES


def list_receipts(year=None, chain: Optional[str] = None, q: str = "", status: str = "") -> List[Dict]:
    clauses, params = [], []
    if year:
        clauses.append("strftime('%Y', i.invoice_date) = ?")
        params.append(str(year))
    if chain:
        clauses.append("s.chain = ?")
        params.append(chain)
    if q:
        clauses.append("(i.invoice_number LIKE ? OR i.filename LIKE ? OR s.name LIKE ? OR EXISTS "
                       "(SELECT 1 FROM products p WHERE p.invoice_id = i.id AND p.original_name LIKE ?))")
        params += [f"%{q}%"] * 4
    if status == "review":
        clauses.append("COALESCE(i.parse_quality_score, 1) < 1")
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    conn = get_connection()
    rows = conn.execute(f"""
        SELECT i.id, i.invoice_number, i.filename, i.invoice_date, i.total_amount paid, i.total_savings bonus,
               i.source_type, i.parse_quality_score quality, i.parse_notes, s.name store, s.chain,
               COALESCE(p.lines, 0) lines, COALESCE(p.groceries, 0) groceries, COALESCE(p.validated, 0) validated,
               COALESCE(e.savings, 0) savings_stamps
        FROM invoices i LEFT JOIN stores s ON s.id = i.store_id
        LEFT JOIN (SELECT invoice_id, COUNT(*) lines, SUM(price) groceries, SUM(is_validated = 1) validated
                   FROM products GROUP BY invoice_id) p ON p.invoice_id = i.id
        LEFT JOIN (SELECT invoice_id, SUM(amount) savings FROM invoice_extras
                   WHERE extra_type = 'savings' GROUP BY invoice_id) e ON e.invoice_id = i.id
        {where} ORDER BY i.invoice_date DESC
    """, params).fetchall()
    conn.close()
    out = []
    for r in rows:
        d = dict(r)
        d["spent"] = round(d["paid"] - d["savings_stamps"], 2)
        out.append(d)
    return out


def receipt_detail(invoice_id: int) -> Optional[Dict]:
    conn = get_connection()
    inv = conn.execute("""
        SELECT i.*, s.name store, s.chain FROM invoices i LEFT JOIN stores s ON s.id = i.store_id WHERE i.id = ?
    """, (invoice_id,)).fetchone()
    if not inv:
        conn.close()
        return None
    lines = [dict(r) for r in conn.execute("""
        SELECT p.id, p.original_name, p.display_name, p.quantity, p.price, p.gross_price, p.discount_amount,
               p.unit_price, p.weight_kg, p.btw_percentage, p.is_validated, p.catalog_id,
               COALESCE(p.user_category, p.auto_category) category,
               COALESCE(p.user_subcategory, p.auto_subcategory) subcategory,
               p.user_category IS NOT NULL line_override,
               cp.name catalog_name, cp.category_source, cp.needs_review, cp.suggested_category,
               COALESCE(p.pack_count, a.pack_count, 1) pack_count
        FROM products p LEFT JOIN catalog_products cp ON cp.id = p.catalog_id
        LEFT JOIN product_aliases a ON a.alias_key = p.alias_key
        WHERE p.invoice_id = ? ORDER BY p.id
    """, (invoice_id,))]
    extras = [dict(r) for r in conn.execute("""
        SELECT extra_type, description, quantity, amount FROM invoice_extras WHERE invoice_id = ? ORDER BY id
    """, (invoice_id,))]
    neighbours = conn.execute("""
        SELECT (SELECT id FROM invoices WHERE invoice_date < ? ORDER BY invoice_date DESC LIMIT 1) prev,
               (SELECT id FROM invoices WHERE invoice_date > ? ORDER BY invoice_date ASC LIMIT 1) next
    """, (inv["invoice_date"], inv["invoice_date"])).fetchone()
    conn.close()

    d = dict(inv)
    groceries = round(sum(l["price"] for l in lines), 2)
    by_type = {}
    for e in extras:
        by_type[e["extra_type"]] = round(by_type.get(e["extra_type"], 0) + e["amount"], 2)
    computed = round(groceries + sum(by_type.values()), 2)
    d.update({
        "lines": lines, "extras": extras, "extra_labels": EXTRA_TYPES,
        "reconciliation": {
            "groceries": groceries,
            "extras": by_type,
            "computed_total": computed,
            "paid": d["total_amount"],
            "difference": round(d["total_amount"] - computed, 2),
            "spent": round(d["total_amount"] - by_type.get("savings", 0), 2),
        },
        "prev_id": neighbours["prev"], "next_id": neighbours["next"],
    })
    d.pop("raw_text", None)
    return d


def raw_text(invoice_id: int) -> Optional[str]:
    conn = get_connection()
    row = conn.execute("SELECT raw_text FROM invoices WHERE id = ?", (invoice_id,)).fetchone()
    conn.close()
    return row["raw_text"] if row else None


def set_validated(invoice_id: int, validated: bool = True) -> int:
    conn = get_connection()
    cur = conn.execute("UPDATE products SET is_validated = ? WHERE invoice_id = ?", (1 if validated else 0, invoice_id))
    conn.commit()
    conn.close()
    return cur.rowcount


EXPORTS = {
    "bonnen": ("""
        SELECT i.id, i.invoice_date datum, s.name winkel, s.chain keten, i.invoice_number bonnummer,
               i.total_amount betaald, i.total_savings bonus_korting,
               (SELECT COALESCE(SUM(amount),0) FROM invoice_extras e WHERE e.invoice_id=i.id AND e.extra_type='savings') gespaard,
               i.parse_quality_score kwaliteit, i.filename bestand
        FROM invoices i LEFT JOIN stores s ON s.id = i.store_id ORDER BY i.invoice_date"""),
    "regels": ("""
        SELECT p.id, i.invoice_date datum, s.name winkel, p.original_name bonnaam, cp.name product,
               p.quantity aantal, COALESCE(p.pack_count, 1) per_verpakking, p.gross_price bruto, p.discount_amount korting,
               p.price betaald, COALESCE(p.user_category, p.auto_category) categorie,
               COALESCE(p.user_subcategory, p.auto_subcategory) subcategorie
        FROM products p JOIN invoices i ON i.id = p.invoice_id LEFT JOIN stores s ON s.id = i.store_id
        LEFT JOIN catalog_products cp ON cp.id = p.catalog_id ORDER BY i.invoice_date, p.id"""),
    "producten": ("""
        SELECT cp.id, cp.name product, cp.brand merk, cp.family soort, cp.category categorie,
               cp.subcategory subcategorie, cp.category_source bron, COUNT(p.id) aankopen, ROUND(SUM(p.price),2) uitgegeven
        FROM catalog_products cp LEFT JOIN products p ON p.catalog_id = cp.id GROUP BY cp.id ORDER BY uitgegeven DESC"""),
    "bijkomend": ("""
        SELECT e.id, i.invoice_date datum, s.name winkel, e.extra_type soort, e.description omschrijving,
               e.quantity aantal, e.amount bedrag
        FROM invoice_extras e JOIN invoices i ON i.id = e.invoice_id LEFT JOIN stores s ON s.id = i.store_id
        ORDER BY i.invoice_date"""),
}


def export_csv(kind: str) -> Optional[str]:
    sql = EXPORTS.get(kind)
    if not sql:
        return None
    conn = get_connection()
    cur = conn.execute(sql)
    out = io.StringIO()
    writer = csv.writer(out, delimiter=";")  # Dutch Excel expects ';'
    writer.writerow([c[0] for c in cur.description])
    for row in cur.fetchall():
        writer.writerow(["" if v is None else (str(v).replace(".", ",") if isinstance(v, float) else v) for v in row])
    conn.close()
    return out.getvalue()
