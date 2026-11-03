"""
Product catalog API: products (with variants), categories at product level, review queue.

    GET  /api/catalog                      list products (search, category, chain, year, review, sort)
    GET  /api/catalog/<id>                 detail: variants, unit prices, purchases, projection
    PUT  /api/catalog/<id>                 rename
    PUT  /api/catalog/<id>/category        set category › subcategory for all purchases
    POST /api/catalog/merge                {keep_id, merge_ids}
    POST /api/catalog/aliases/move         {alias, target_id | new_name}
    GET  /api/catalog/review               products that need a human look, by € impact
    POST /api/catalog/sync                 link new receipt lines, label new products
    GET  /api/category-tree                valid category › subcategory pairs for pickers
"""

from flask import Blueprint, jsonify, request

from services import ai, catalog

bp = Blueprint("catalog_api", __name__, url_prefix="/api")


def _json() -> dict:
    return request.get_json(silent=True) or {}


@bp.get("/catalog")
def list_products():
    args = request.args
    return jsonify(catalog.list_products(
        search=args.get("q", "").strip(), category=args.get("category", ""),
        subcategory=args.get("subcategory", ""), chain=args.get("chain", ""), year=args.get("year"),
        needs_review=args.get("review") in ("1", "true"), sort=args.get("sort", "spent"),
        limit=min(args.get("limit", 500, type=int), 2000),
    ))


@bp.get("/catalog/<int:catalog_id>")
def product_detail(catalog_id):
    detail = catalog.product_detail(catalog_id)
    if not detail:
        return jsonify({"error": "Product niet gevonden"}), 404
    return jsonify(detail)


@bp.put("/catalog/<int:catalog_id>")
def rename_product(catalog_id):
    name = (_json().get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is verplicht"}), 400
    catalog.rename_product(catalog_id, name)
    return jsonify({"status": "ok"})


@bp.put("/catalog/<int:catalog_id>/category")
def set_category(catalog_id):
    data = _json()
    category = (data.get("category") or "").strip()
    if not category:
        return jsonify({"error": "category is verplicht"}), 400
    try:
        return jsonify(catalog.set_category(catalog_id, category, data.get("subcategory") or None))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@bp.post("/catalog/<int:catalog_id>/suggestion/accept")
def accept_suggestion(catalog_id):
    try:
        return jsonify(catalog.accept_suggestion(catalog_id))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@bp.post("/catalog/<int:catalog_id>/confirm")
def confirm(catalog_id):
    catalog.confirm_category(catalog_id)
    return jsonify({"status": "ok"})


@bp.post("/catalog/suggestions/accept-all")
def accept_all_suggestions():
    min_conf = float((_json().get("min_confidence") or 0.95))
    accepted = 0
    for p in catalog.list_products(needs_review=True, limit=5000):
        if p.get("suggested_category") and (p.get("suggested_confidence") or 0) >= min_conf:
            catalog.accept_suggestion(p["id"])
            accepted += 1
    return jsonify({"accepted": accepted})


@bp.post("/catalog/merge")
def merge_products():
    data = _json()
    keep, merge = data.get("keep_id"), [int(i) for i in data.get("merge_ids", [])]
    if not keep or not merge:
        return jsonify({"error": "keep_id en merge_ids zijn verplicht"}), 400
    catalog.merge_products(int(keep), merge)
    return jsonify({"status": "merged", "keep_id": keep})


@bp.post("/catalog/aliases/move")
def move_alias():
    data = _json()
    alias = data.get("alias")
    if not alias or not (data.get("target_id") or data.get("new_name")):
        return jsonify({"error": "alias en target_id of new_name zijn verplicht"}), 400
    target = catalog.move_alias(alias, data.get("target_id"), data.get("new_name"))
    catalog.label_catalog(only_missing=True)
    catalog.materialise()
    return jsonify({"status": "moved", "catalog_id": target})


@bp.get("/catalog/review")
def review_queue():
    return jsonify(catalog.list_products(needs_review=True, sort="spent",
                                         limit=request.args.get("limit", 200, type=int)))


@bp.post("/catalog/sync")
def sync():
    data = _json()
    return jsonify(catalog.sync(use_ai=data.get("use_ai", True), merge=data.get("merge", False)))


@bp.delete("/subcategories/<int:subcategory_id>")
def delete_subcategory(subcategory_id):
    """Remove a subcategory; its products keep their main category without subcategory."""
    from database import get_connection
    conn = get_connection()
    row = conn.execute("""SELECT s.name, c.name category FROM subcategories s JOIN categories c ON c.id = s.category_id
                          WHERE s.id = ?""", (subcategory_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Subcategorie niet gevonden"}), 404
    conn.execute("UPDATE catalog_products SET subcategory = NULL WHERE category = ? AND subcategory = ?",
                 (row["category"], row["name"]))
    conn.execute("UPDATE products SET auto_subcategory = NULL WHERE auto_category = ? AND auto_subcategory = ?",
                 (row["category"], row["name"]))
    conn.execute("UPDATE products SET user_subcategory = NULL WHERE user_subcategory = ?", (row["name"],))
    conn.execute("DELETE FROM category_keywords WHERE subcategory_id = ?", (subcategory_id,))
    conn.execute("DELETE FROM subcategories WHERE id = ?", (subcategory_id,))
    conn.commit()
    conn.close()
    catalog.invalidate_category_cache()
    return jsonify({"status": "deleted", "name": row["name"]})


@bp.get("/category-tree")
def category_tree():
    tree = {}
    for category, subcategory in catalog.category_pairs():
        tree.setdefault(category, [])
        if subcategory:
            tree[category].append(subcategory)
    return jsonify({"tree": tree, "ai_available": ai.available()})
