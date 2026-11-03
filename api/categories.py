"""Categories, subcategories and keyword rules."""


from flask import Blueprint, jsonify, request

from database import (
    get_all_categories,
    add_category, delete_category, migrate_normalize_product_names, get_all_subcategories, get_subcategories_for_category, add_subcategory,
    get_subcategory_totals, get_connection,
)

bp = Blueprint("categories_api", __name__)


@bp.route("/api/categories")
def api_categories():
    """Get all categories"""
    return jsonify(get_all_categories())


@bp.route("/api/categories", methods=["POST"])
def api_add_category():
    """Add a new category"""
    data = request.json
    name = data.get("name")
    color = data.get("color", "#9E9E9E")
    icon = data.get("icon", "📁")
    
    if not name:
        return jsonify({"error": "Name is required"}), 400
    
    category_id = add_category(name, color, icon)
    from services import catalog
    catalog.invalidate_category_cache()
    return jsonify({"id": category_id, "name": name})


@bp.route("/api/categories/<int:category_id>", methods=["DELETE"])
def api_delete_category(category_id):
    """Delete a category"""
    result = delete_category(category_id)
    
    if not result["success"]:
        return jsonify({"error": result["error"]}), 404
    
    return jsonify({
        "status": "deleted", 
        "name": result["name"],
        "products_affected": result["products_affected"]
    })


@bp.route("/api/migrate", methods=["POST"])
def api_run_migration():
    """Run migration to normalize product names and recategorize"""
    try:
        result = migrate_normalize_product_names()
        return jsonify({
            "status": "completed",
            "updated": result.get("updated", 0),
            "recategorized": result.get("recategorized", 0)
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@bp.route("/api/subcategories")
def api_subcategories():
    """Get all subcategories"""
    return jsonify(get_all_subcategories())


@bp.route("/api/subcategories/<category_name>")
def api_subcategories_for_category(category_name):
    """Get subcategories for a specific category"""
    return jsonify(get_subcategories_for_category(category_name))


@bp.route("/api/subcategories", methods=["POST"])
def api_add_subcategory():
    """Add a new subcategory"""
    data = request.json
    category = data.get("category")
    name = data.get("name")
    icon = data.get("icon", "📁")
    
    if not category or not name:
        return jsonify({"error": "category and name are required"}), 400
    
    try:
        subcat_id = add_subcategory(category, name, icon)
        from services import catalog
        catalog.invalidate_category_cache()
        return jsonify({"id": subcat_id, "name": name})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@bp.route("/api/subcategories/totals")
def api_subcategory_totals():
    """Get spending totals per subcategory"""
    return jsonify(get_subcategory_totals())


@bp.route("/api/categorization/categorize", methods=["POST"])
def api_categorize_products():
    """Categorize products using the 3-tier system"""
    from services.categorization import get_categorization_service
    service = get_categorization_service()
    
    data = request.json
    products = data.get("products", [])
    
    if not products:
        return jsonify({"error": "No products provided"}), 400
    
    result = service.categorize_batch(products)
    return jsonify(result)


@bp.route("/api/categorization/learn", methods=["POST"])
def api_learn_categorization():
    """Learn from a user correction"""
    from services.categorization import get_categorization_service
    service = get_categorization_service()
    
    data = request.json
    product_name = data.get("product_name")
    category = data.get("category")
    subcategory = data.get("subcategory")
    apply_retroactively = data.get("apply_retroactively", True)
    
    if not product_name or not category:
        return jsonify({"error": "product_name and category are required"}), 400
    
    result = service.learn_from_correction(
        product_name, category, subcategory,
        apply_retroactively=apply_retroactively
    )
    return jsonify(result)


@bp.route("/api/categorization/keywords")
def api_get_keywords():
    """Get all category keywords from database"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            ck.id,
            ck.keyword,
            ck.match_type,
            ck.priority,
            ck.is_active,
            ck.created_by,
            c.name as category_name,
            s.name as subcategory_name
        FROM category_keywords ck
        JOIN categories c ON ck.category_id = c.id
        LEFT JOIN subcategories s ON ck.subcategory_id = s.id
        ORDER BY c.sort_order, ck.priority DESC
    """)
    
    keywords = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return jsonify(keywords)


@bp.route("/api/categorization/keywords", methods=["POST"])
def api_add_keyword():
    """Add a new keyword to the database"""
    
    data = request.json
    keyword = data.get("keyword", "").lower().strip()
    category_name = data.get("category")
    subcategory_name = data.get("subcategory")
    match_type = data.get("match_type", "contains")
    priority = data.get("priority", 0)
    
    if not keyword or not category_name:
        return jsonify({"error": "keyword and category are required"}), 400
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get category ID
    cursor.execute("SELECT id FROM categories WHERE name = ?", (category_name,))
    cat_row = cursor.fetchone()
    if not cat_row:
        conn.close()
        return jsonify({"error": f"Category '{category_name}' not found"}), 404
    
    cat_id = cat_row['id']
    subcat_id = None
    
    if subcategory_name:
        cursor.execute("SELECT id FROM subcategories WHERE name = ? AND category_id = ?", 
                      (subcategory_name, cat_id))
        subcat_row = cursor.fetchone()
        if subcat_row:
            subcat_id = subcat_row['id']
    
    try:
        cursor.execute("""
            INSERT INTO category_keywords (category_id, subcategory_id, keyword, match_type, priority, created_by)
            VALUES (?, ?, ?, ?, ?, 'user')
        """, (cat_id, subcat_id, keyword, match_type, priority))
        
        keyword_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return jsonify({"status": "added", "id": keyword_id})
    except Exception as e:
        conn.close()
        return jsonify({"error": str(e)}), 400


@bp.route("/api/categorization/keywords/<int:keyword_id>", methods=["DELETE"])
def api_delete_keyword(keyword_id):
    """Delete a keyword"""
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM category_keywords WHERE id = ?", (keyword_id,))
    affected = cursor.rowcount
    
    conn.commit()
    conn.close()
    
    if affected == 0:
        return jsonify({"error": "Keyword not found"}), 404
    
    return jsonify({"status": "deleted"})

