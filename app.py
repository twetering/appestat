#!/usr/bin/env python3
"""
Appestat - Eetlust & Uitgaven Regulatie
Complete invoice analysis for grocery spending
"""

import os
from pathlib import Path

from flask import Flask, render_template, jsonify, request

from database import (
    init_database, get_analysis_data, get_all_invoices, 
    get_invoice_with_products, get_all_products, get_all_categories,
    update_product_category, validate_product, add_validation_feedback,
    add_category, get_products_by_category, get_validation_stats,
    migrate_normalize_product_names, get_overig_products_summary,
    bulk_update_category_by_name, get_product_details, get_all_unique_products,
    get_smart_savings_insights, get_all_subcategories, get_subcategories_for_category,
    add_subcategory, update_product_subcategory, get_subcategory_totals,
    migrate_add_subcategories
)
from parser import import_all_invoices, import_invoice
from receipt_parser import import_all_receipts, import_receipt

app = Flask(__name__)

# Initialize database on startup
init_database()


# ============ Pages ============

@app.route("/")
def index():
    """Main dashboard page"""
    return render_template("index.html")


@app.route("/invoices")
def invoices_page():
    """Invoices management page"""
    return render_template("invoices.html")


@app.route("/invoice/<int:invoice_id>")
def invoice_detail_page(invoice_id):
    """Single invoice detail page"""
    return render_template("invoice_detail.html", invoice_id=invoice_id)


@app.route("/categories")
def categories_page():
    """Category management page"""
    return render_template("categories.html")


@app.route("/search")
def search_page():
    """Product search page"""
    return render_template("search.html")


@app.route("/category-editor")
def category_editor_page():
    """Category and keyword editor page"""
    return render_template("category_editor.html")


@app.route("/years")
def years_page():
    """Year overviews page"""
    return render_template("years.html")


# ============ API Endpoints ============

@app.route("/api/analysis")
def api_analysis():
    """Get complete analysis data, optionally filtered by year"""
    year = request.args.get("year")
    data = get_analysis_data(year=year)
    return jsonify(data)


@app.route("/api/invoices")
def api_invoices():
    """Get all invoices"""
    return jsonify(get_all_invoices())


@app.route("/api/invoices/<int:invoice_id>")
def api_invoice_detail(invoice_id):
    """Get single invoice with products"""
    invoice = get_invoice_with_products(invoice_id)
    if not invoice:
        return jsonify({"error": "Invoice not found"}), 404
    return jsonify(invoice)


@app.route("/api/invoices/import", methods=["POST"])
def api_import_invoices():
    """Import all invoices and receipts from both directories"""
    force = request.json.get("force", False) if request.json else False
    
    # Import regular invoices
    invoice_results = import_all_invoices(force=force)
    
    # Import kassabonnen
    from receipt_parser import import_all_receipts
    receipt_results = import_all_receipts(force=force)
    
    # Combine results
    combined_results = {
        "status": "success",
        "invoices": {
            "imported": invoice_results.get("success", 0),
            "skipped": invoice_results.get("skipped", 0),
            "errors": invoice_results.get("errors", 0)
        },
        "receipts": {
            "imported": receipt_results.get("success", 0),
            "skipped": receipt_results.get("skipped", 0),
            "errors": receipt_results.get("errors", 0)
        },
        "total_imported": invoice_results.get("success", 0) + receipt_results.get("success", 0),
        "total_skipped": invoice_results.get("skipped", 0) + receipt_results.get("skipped", 0),
        "total_errors": invoice_results.get("errors", 0) + receipt_results.get("errors", 0)
    }
    
    return jsonify(combined_results)


@app.route("/api/products")
def api_products():
    """Get all products"""
    category = request.args.get("category")
    if category:
        return jsonify(get_products_by_category(category))
    return jsonify(get_all_products())


@app.route("/api/products/<int:product_id>/category", methods=["PUT"])
def api_update_category(product_id):
    """Update a product's category"""
    data = request.json
    category = data.get("category")
    apply_to_similar = data.get("apply_to_similar", False)
    
    if not category:
        return jsonify({"error": "Category is required"}), 400
    
    result = update_product_category(product_id, category, apply_to_similar)
    if result.get("success"):
        return jsonify({
            "status": "updated",
            "updated_count": result.get("updated_count", 1),
            "product_name": result.get("product_name")
        })
    return jsonify({"error": result.get("error", "Product not found")}), 404


@app.route("/api/products/<int:product_id>/validate", methods=["PUT"])
def api_validate_product(product_id):
    """Validate or invalidate a product"""
    data = request.json
    is_valid = data.get("is_valid", True)
    notes = data.get("notes", "")
    
    validate_product(product_id, is_valid, notes)
    return jsonify({"status": "validated"})


@app.route("/api/categories")
def api_categories():
    """Get all categories"""
    return jsonify(get_all_categories())


@app.route("/api/categories", methods=["POST"])
def api_add_category():
    """Add a new category"""
    data = request.json
    name = data.get("name")
    color = data.get("color", "#9E9E9E")
    icon = data.get("icon", "📁")
    
    if not name:
        return jsonify({"error": "Name is required"}), 400
    
    category_id = add_category(name, color, icon)
    return jsonify({"id": category_id, "name": name})


@app.route("/api/categories/<int:category_id>", methods=["DELETE"])
def api_delete_category(category_id):
    """Delete a category"""
    from database import get_connection
    conn = get_connection()
    cursor = conn.cursor()
    
    # First check if category exists
    cursor.execute("SELECT name FROM categories WHERE id = ?", (category_id,))
    category = cursor.fetchone()
    
    if not category:
        conn.close()
        return jsonify({"error": "Category not found"}), 404
    
    # Check if there are products using this category
    cursor.execute("""
        SELECT COUNT(*) as count 
        FROM products 
        WHERE user_category = ? OR auto_category = ?
    """, (category['name'], category['name']))
    
    product_count = cursor.fetchone()['count']
    
    # Delete the category (products will be reassigned to 'Overig' via trigger or we handle it)
    cursor.execute("DELETE FROM categories WHERE id = ?", (category_id,))
    conn.commit()
    conn.close()
    
    return jsonify({
        "status": "deleted", 
        "name": category['name'],
        "products_affected": product_count
    })


@app.route("/api/feedback", methods=["POST"])
def api_add_feedback():
    """Add validation feedback"""
    data = request.json
    add_validation_feedback(
        invoice_id=data.get("invoice_id"),
        product_id=data.get("product_id"),
        feedback_type=data.get("feedback_type"),
        original_value=data.get("original_value"),
        corrected_value=data.get("corrected_value"),
        notes=data.get("notes")
    )
    return jsonify({"status": "feedback_added"})


@app.route("/api/stats/validation")
def api_validation_stats():
    """Get validation statistics"""
    return jsonify(get_validation_stats())


@app.route("/api/migrate", methods=["POST"])
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


@app.route("/api/products/overig")
def api_overig_products():
    """Get summary of products in Overig category"""
    return jsonify(get_overig_products_summary())


@app.route("/api/products/bulk-update", methods=["POST"])
def api_bulk_update_category():
    """Update category for all products with a specific name"""
    data = request.json
    product_name = data.get("product_name")
    category = data.get("category")
    
    if not product_name or not category:
        return jsonify({"error": "product_name and category are required"}), 400
    
    affected = bulk_update_category_by_name(product_name, category)
    return jsonify({"status": "updated", "affected": affected})


@app.route("/api/products/bulk-update-subcategory", methods=["POST"])
def api_bulk_update_subcategory():
    """Update subcategory for all products with a specific name"""
    data = request.json
    product_name = data.get("product_name")
    subcategory = data.get("subcategory")
    
    if not product_name or not subcategory:
        return jsonify({"error": "product_name and subcategory are required"}), 400
    
    # Get connection and update all products with this name
    from database import get_connection
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE products 
        SET user_subcategory = ? 
        WHERE original_name = ?
    """, (subcategory, product_name))
    
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    
    return jsonify({"status": "updated", "affected": affected})


@app.route("/api/products/unique")
def api_unique_products():
    """Get all unique products for search/browse"""
    return jsonify(get_all_unique_products())


@app.route("/api/products/details/<path:product_name>")
def api_product_details(product_name):
    """Get detailed information about a specific product"""
    details = get_product_details(product_name)
    if not details:
        return jsonify({"error": "Product not found"}), 404
    return jsonify(details)


@app.route("/api/insights/savings")
def api_smart_savings():
    """Get smart savings insights"""
    return jsonify(get_smart_savings_insights())


@app.route("/api/categories/keywords")
def api_get_keywords():
    """Get all category keywords from parser"""
    from parser import CATEGORY_KEYWORDS
    return jsonify(CATEGORY_KEYWORDS)


@app.route("/api/categories/keywords", methods=["POST"])
def api_add_keyword():
    """Add a keyword to a category"""
    from parser import add_keyword_to_category
    data = request.json
    category = data.get("category")
    keyword = data.get("keyword")
    
    if not category or not keyword:
        return jsonify({"error": "category and keyword are required"}), 400
    
    result = add_keyword_to_category(category, keyword)
    if result.get("success"):
        return jsonify({"status": "added"})
    return jsonify({"error": result.get("error", "Failed to add keyword")}), 400


@app.route("/api/categories/keywords", methods=["DELETE"])
def api_remove_keyword():
    """Remove a keyword from a category"""
    from parser import remove_keyword_from_category
    data = request.json
    category = data.get("category")
    keyword = data.get("keyword")
    
    if not category or not keyword:
        return jsonify({"error": "category and keyword are required"}), 400
    
    result = remove_keyword_from_category(category, keyword)
    if result.get("success"):
        return jsonify({"status": "removed"})
    return jsonify({"error": result.get("error", "Failed to remove keyword")}), 400


@app.route("/api/search/products")
def api_search_products():
    """Search products by name"""
    query = request.args.get("q", "").lower()
    category = request.args.get("category", "")
    
    products = get_all_unique_products()
    
    if query:
        products = [p for p in products if query in p['name'].lower()]
    if category:
        products = [p for p in products if p['category'] == category]
    
    return jsonify(products)


@app.route("/api/receipts/import", methods=["POST"])
def api_import_receipts():
    """Import all receipts from the data/bonnen directory"""
    force = request.json.get("force", False) if request.json else False
    results = import_all_receipts(force=force)
    return jsonify(results)


@app.route("/api/receipts/import-single", methods=["POST"])
def api_import_single_receipt():
    """Import a single receipt by path"""
    data = request.json
    path = data.get("path")
    force = data.get("force", False)
    
    if not path:
        return jsonify({"error": "path is required"}), 400
    
    result = import_receipt(path, force=force)
    return jsonify(result)


# ============ Subcategories API ============

@app.route("/api/subcategories")
def api_subcategories():
    """Get all subcategories"""
    return jsonify(get_all_subcategories())


@app.route("/api/subcategories/<category_name>")
def api_subcategories_for_category(category_name):
    """Get subcategories for a specific category"""
    return jsonify(get_subcategories_for_category(category_name))


@app.route("/api/subcategories", methods=["POST"])
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
        return jsonify({"id": subcat_id, "name": name})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/subcategories/totals")
def api_subcategory_totals():
    """Get spending totals per subcategory"""
    return jsonify(get_subcategory_totals())


@app.route("/api/products/<int:product_id>/subcategory", methods=["PUT"])
def api_update_subcategory(product_id):
    """Update a product's subcategory"""
    data = request.json
    subcategory = data.get("subcategory")
    apply_to_similar = data.get("apply_to_similar", False)
    
    if not subcategory:
        return jsonify({"error": "subcategory is required"}), 400
    
    result = update_product_subcategory(product_id, subcategory, apply_to_similar)
    if result.get("success"):
        return jsonify({
            "status": "updated",
            "updated_count": result.get("updated_count", 1)
        })
    return jsonify({"error": result.get("error", "Failed to update")}), 400


@app.route("/api/migrate/subcategories", methods=["POST"])
def api_migrate_subcategories():
    """Run migration to add subcategories"""
    try:
        result = migrate_add_subcategories()
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Import invoices on first run if database is empty
    print("Checking database...")
    invoices = get_all_invoices()
    
    if not invoices:
        print("No invoices found. Importing from data/invoices...")
        results = import_all_invoices()
        print(f"Import completed: {results['success']} success, {results['skipped']} skipped, {results['errors']} errors")
    else:
        print(f"Found {len(invoices)} invoices in database")
    
    # Get analysis summary
    analysis = get_analysis_data()
    print(f"Total spent: €{analysis['summary']['total_spent']:.2f}")
    
    app.run(debug=True, port=5050)
