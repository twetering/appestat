#!/usr/bin/env python3
"""
Appestat - Eetlust & Uitgaven Regulatie
Complete invoice analysis for grocery spending
"""

import os
from pathlib import Path

from flask import Flask, render_template, jsonify, request
from werkzeug.exceptions import HTTPException

import time
import urllib.parse

from database import (
    init_database, get_analysis_data, get_all_invoices,
    get_invoice_with_products, get_all_products, get_all_categories,
    update_product_category, validate_product, add_validation_feedback,
    add_category, delete_category, get_products_by_category, get_validation_stats,
    migrate_normalize_product_names, get_overig_products_summary,
    bulk_update_category_by_name, bulk_update_category_by_names,
    bulk_update_subcategory_by_names, get_product_details, get_all_unique_products,
    get_smart_savings_insights, get_all_subcategories, get_subcategories_for_category,
    add_subcategory, update_product_subcategory, get_subcategory_totals,
    migrate_add_subcategories, get_budget_candidates, save_budget_goal,
    get_budget_goals, get_budget_tradeoff, get_budget_summary,
    get_day_of_week_analysis, get_bonus_analysis, get_spending_prediction,
    get_llm_suggestions, apply_llm_suggestions, reject_llm_suggestions,
    update_llm_suggestions_bulk,
    get_llm_suggestions_count,
    get_connection, save_invoice, save_products,
)
from parser import import_all_invoices, import_invoice

app = Flask(__name__)

# Initialize database on startup
init_database()

# Run migrations on startup to keep schema in sync
try:
    from migrations.migration_manager import run_all_migrations
    run_all_migrations()
except Exception as e:
    print(f"Migration error on startup: {e}")


@app.errorhandler(HTTPException)
def handle_http_exception(e):
    """Return JSON for API errors; default HTML elsewhere."""
    if request.path.startswith("/api/"):
        return jsonify({"error": e.description}), e.code
    return e


@app.errorhandler(Exception)
def handle_exception(e):
    """Return JSON for API errors; re-raise for non-API routes."""
    if request.path.startswith("/api/"):
        return jsonify({"error": str(e)}), 500
    raise e


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


@app.route("/budget")
def budget_page():
    """Budget 2026 wizard page"""
    return render_template("budget.html")


@app.route("/admin")
def admin_page():
    """Admin/settings page"""
    return render_template("admin.html")


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
    """Import all invoices and receipts from the data/invoices directory"""
    force = request.json.get("force", False) if request.json else False
    
    # Import all PDFs (both online invoices and kassabonnen) from unified directory
    results = import_all_invoices("data/invoices", force=force)
    
    return jsonify({
        "status": "success",
        "imported": results.get("success", 0),
        "skipped": results.get("skipped", 0),
        "errors": results.get("errors", 0),
        "total_amount": results.get("total_amount", 0),
        "details": results.get("details", [])
    })


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
    result = delete_category(category_id)
    
    if not result["success"]:
        return jsonify({"error": result["error"]}), 404
    
    return jsonify({
        "status": "deleted", 
        "name": result["name"],
        "products_affected": result["products_affected"]
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
    product_names = data.get("product_names")
    category = data.get("category")
    
    if not category:
        return jsonify({"error": "category is required"}), 400
    
    if product_names:
        affected = bulk_update_category_by_names(product_names, category)
        return jsonify({"status": "updated", "affected": affected})
    
    if not product_name:
        return jsonify({"error": "product_name or product_names is required"}), 400
    
    affected = bulk_update_category_by_name(product_name, category)
    return jsonify({"status": "updated", "affected": affected})


@app.route("/api/products/bulk-update-subcategory", methods=["POST"])
def api_bulk_update_subcategory():
    """Update subcategory for all products with a specific name"""
    data = request.json
    product_name = data.get("product_name")
    product_names = data.get("product_names")
    subcategory = data.get("subcategory")
    category = data.get("category")
    
    if not subcategory:
        return jsonify({"error": "subcategory is required"}), 400
    
    try:
        if product_names:
            affected = bulk_update_subcategory_by_names(product_names, subcategory, category_name=category)
            return jsonify({"status": "updated", "affected": affected})
        
        if not product_name:
            return jsonify({"error": "product_name or product_names is required"}), 400
        
        # Fallback for single-name update
        affected = bulk_update_subcategory_by_names([product_name], subcategory, category_name=category)
        return jsonify({"status": "updated", "affected": affected})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


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


@app.route("/api/insights/day-analysis")
def api_day_analysis():
    """Get day of week spending analysis"""
    year = request.args.get("year")
    return jsonify(get_day_of_week_analysis(year=year))


@app.route("/api/insights/bonus")
def api_bonus_analysis():
    """Get bonus savings analysis"""
    year = request.args.get("year")
    return jsonify(get_bonus_analysis(year=year))


@app.route("/api/insights/prediction")
def api_spending_prediction():
    """Get spending prediction for current/selected year"""
    year = request.args.get("year")
    return jsonify(get_spending_prediction(year=year))


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


@app.route("/api/invoices/import-single", methods=["POST"])
def api_import_single_invoice():
    """Import a single invoice/receipt by path"""
    data = request.json
    path = data.get("path")
    force = data.get("force", False)
    
    if not path:
        return jsonify({"error": "path is required"}), 400
    
    result = import_invoice(path, force=force)
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


# ============ Budget 2026 API ============

@app.route("/api/budget/candidates")
def api_budget_candidates():
    """Get candidate products for budgeting"""
    year = request.args.get("year", "2025")
    try:
        candidates = get_budget_candidates(int(year))
        return jsonify(candidates)
    except ValueError:
        return jsonify({"error": "Invalid year"}), 400


@app.route("/api/budget/goals", methods=["GET", "POST"])
def api_budget_goals():
    """Get or save budget goals"""
    if request.method == "POST":
        data = request.json
        if not data.get("product_name") or not data.get("target_year"):
            return jsonify({"error": "Missing required fields"}), 400
            
        goal_id = save_budget_goal(data)
        return jsonify({"status": "saved", "id": goal_id})
    else:
        target_year = request.args.get("target_year", "2026")
        try:
            goals = get_budget_goals(int(target_year))
            return jsonify(goals)
        except ValueError:
            return jsonify({"error": "Invalid year"}), 400


@app.route("/api/budget/tradeoff")
def api_budget_tradeoff():
    """Get a trade-off scenario"""
    year = request.args.get("year", "2025")
    try:
        result = get_budget_tradeoff(int(year))
        return jsonify(result)
    except ValueError:
        return jsonify({"error": "Invalid year"}), 400


@app.route("/api/budget/summary")
def api_budget_summary():
    """Get budget summary and projections"""
    target_year = request.args.get("target_year", "2026")
    try:
        summary = get_budget_summary(int(target_year))
        return jsonify(summary)
    except ValueError:
        return jsonify({"error": "Invalid year"}), 400


@app.route("/api/budget/dashboard")
def api_budget_dashboard():
    """Get comprehensive budget dashboard data"""
    from services.budget import get_budget_service
    year = request.args.get("year", "2026", type=int)
    service = get_budget_service()
    return jsonify(service.get_budget_dashboard(year))


@app.route("/api/budget/categories")
def api_budget_categories():
    """Get category-level budgets"""
    from services.budget import get_budget_service
    year = request.args.get("year", "2026", type=int)
    service = get_budget_service()
    return jsonify(service.get_category_budgets(year))


@app.route("/api/budget/categories", methods=["POST"])
def api_set_category_budget():
    """Set a category budget"""
    from services.budget import get_budget_service
    service = get_budget_service()
    
    data = request.json
    year = data.get("year", 2026)
    category_id = data.get("category_id")
    target_amount = data.get("target_amount")
    warning_threshold = data.get("warning_threshold", 0.8)
    notes = data.get("notes")
    
    if not category_id or target_amount is None:
        return jsonify({"error": "category_id and target_amount are required"}), 400
    
    result = service.set_category_budget(year, category_id, target_amount, warning_threshold, notes)
    return jsonify(result)


@app.route("/api/budget/categories/generate", methods=["POST"])
def api_generate_category_budgets():
    """Auto-generate category budgets from historical spending"""
    from services.budget import get_budget_service
    service = get_budget_service()
    
    data = request.json or {}
    target_year = data.get("target_year", 2026)
    reduction = data.get("reduction_percentage", 0)
    
    results = service.generate_category_budgets_from_history(target_year, reduction)
    return jsonify({
        "status": "generated",
        "target_year": target_year,
        "reduction_percentage": reduction,
        "categories": results
    })


@app.route("/api/budget/alerts")
def api_budget_alerts():
    """Get budget alerts"""
    from services.budget import get_budget_service
    service = get_budget_service()
    
    year = request.args.get("year", "2026", type=int)
    
    # Check for new alerts
    service.check_and_create_alerts(year)
    
    # Get dashboard which includes alerts
    dashboard = service.get_budget_dashboard(year)
    return jsonify(dashboard.get("alerts", []))


@app.route("/api/budget/alerts/<int:alert_id>/dismiss", methods=["POST"])
def api_dismiss_alert(alert_id):
    """Dismiss a budget alert"""
    from services.budget import get_budget_service
    service = get_budget_service()
    
    success = service.dismiss_alert(alert_id)
    if success:
        return jsonify({"status": "dismissed"})
    return jsonify({"error": "Alert not found"}), 404


# ============ Categorization API ============

@app.route("/api/categorization/uncategorized")
def api_uncategorized_products():
    """Get products currently in 'Overig' category"""
    from services.categorization import get_categorization_service
    service = get_categorization_service()
    limit = request.args.get("limit", 100, type=int)
    products = service.get_uncategorized_products(limit)
    return jsonify(products)


@app.route("/api/categorization/categorize", methods=["POST"])
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


@app.route("/api/categorization/learn", methods=["POST"])
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


@app.route("/api/categorization/llm", methods=["POST"])
def api_llm_categorize():
    """
    Use LLM to suggest categories for uncategorized products.
    
    This stores suggestions for review (no auto-apply).
    
    Request body:
        limit (int): Maximum products to process (default: 50)
        min_count (int): Minimum purchase count (default: 1)
        save_as_suggestions (bool): Save results for review (default: true)
        
    Returns:
        results: Dict mapping product names to categorization results
        stats: Processing statistics
        cost: Token usage and cost estimates
    """
    import asyncio
    from services.llm_categorizer import categorize_all_uncategorized_products
    
    data = request.json or {}
    limit = data.get("limit", 50)
    min_count = data.get("min_count", 1)
    save_as_suggestions = data.get("save_as_suggestions", True)
    
    try:
        result = asyncio.run(categorize_all_uncategorized_products(limit, min_count=min_count, save_as_suggestions=save_as_suggestions))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/categorization/llm/batch", methods=["POST"])
def api_llm_categorize_batch():
    """
    Suggest categories for a specific list of products using LLM.
    
    Request body:
        products (list): List of product names to categorize
        save_as_suggestions (bool): Save results for review (default: true)
        
    Returns:
        results: Dict mapping product names to categorization results
        stats: Processing statistics
        cost: Token usage and cost estimates
    """
    import asyncio
    from services.llm_categorizer import categorize_products_with_llm
    
    data = request.json or {}
    products = data.get("products", [])
    save_as_suggestions = data.get("save_as_suggestions", True)
    
    if not products:
        return jsonify({"error": "No products provided"}), 400
    
    try:
        result = asyncio.run(categorize_products_with_llm(products, save_as_suggestions))
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/categorization/llm/estimate")
def api_llm_cost_estimate():
    """
    Get cost estimate for LLM categorization.
    
    Query params:
        count (int): Number of products to estimate for
        model (str): Model to use (default: gpt-4o-mini)
        
    Returns:
        Cost estimates and batch information
    """
    from services.llm_categorizer import get_categorization_cost_estimate
    
    count = request.args.get("count", 100, type=int)
    model = request.args.get("model", "gpt-4o-mini")
    
    return jsonify(get_categorization_cost_estimate(count, model))


@app.route("/api/categorization/llm/status")
def api_llm_categorization_status():
    """
    Get current LLM categorization status.
    
    Returns:
        uncategorized_products: Count of products needing categorization
        cumulative_costs: Total costs incurred this session
        model: Current model being used
        available_models: List of available models
    """
    from services.llm_categorizer import get_llm_categorization_status
    
    return jsonify(get_llm_categorization_status())


@app.route("/api/categorization/keywords")
def api_get_keywords():
    """Get all category keywords from database"""
    from database import get_connection
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


@app.route("/api/categorization/keywords", methods=["POST"])
def api_add_keyword():
    """Add a new keyword to the database"""
    from database import get_connection
    
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


@app.route("/api/categorization/keywords/<int:keyword_id>", methods=["DELETE"])
def api_delete_keyword(keyword_id):
    """Delete a keyword"""
    from database import get_connection
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("DELETE FROM category_keywords WHERE id = ?", (keyword_id,))
    affected = cursor.rowcount
    
    conn.commit()
    conn.close()
    
    if affected == 0:
        return jsonify({"error": "Keyword not found"}), 404
    
    return jsonify({"status": "deleted"})


# ============ LLM Suggestions API ============

@app.route("/api/categorization/suggestions")
def api_get_llm_suggestions():
    """Get LLM suggestions for review."""
    status = request.args.get("status", "pending")
    limit = request.args.get("limit", 100, type=int)
    return jsonify(get_llm_suggestions(status=status, limit=limit))


@app.route("/api/categorization/suggestions/accept", methods=["POST"])
def api_accept_llm_suggestions():
    """Accept LLM suggestions and apply to products."""
    data = request.json or {}
    suggestion_ids = data.get("suggestion_ids", [])
    overrides = data.get("overrides", [])
    
    if not suggestion_ids:
        return jsonify({"error": "suggestion_ids is required"}), 400
    
    if overrides:
        update_llm_suggestions_bulk(overrides)
    
    result = apply_llm_suggestions(suggestion_ids)
    return jsonify(result)


@app.route("/api/categorization/suggestions/reject", methods=["POST"])
def api_reject_llm_suggestions():
    """Reject LLM suggestions."""
    data = request.json or {}
    suggestion_ids = data.get("suggestion_ids", [])
    
    if not suggestion_ids:
        return jsonify({"error": "suggestion_ids is required"}), 400
    
    result = reject_llm_suggestions(suggestion_ids)
    return jsonify(result)


@app.route("/api/invoices/<int:invoice_id>/reparse", methods=["POST"])
def api_reparse_invoice(invoice_id):
    """Reparse a specific invoice with the latest parser"""
    from database import get_connection
    from parser import extract_products_from_text
    from services.categorization import get_categorization_service
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get the invoice
    cursor.execute("SELECT id, raw_text, filename FROM invoices WHERE id = ?", (invoice_id,))
    invoice = cursor.fetchone()
    
    if not invoice:
        conn.close()
        return jsonify({"error": "Invoice not found"}), 404
    
    # Count old products
    cursor.execute("SELECT COUNT(*) as count FROM products WHERE invoice_id = ?", (invoice_id,))
    old_count = cursor.fetchone()['count']
    
    # Delete old products
    cursor.execute("DELETE FROM products WHERE invoice_id = ?", (invoice_id,))
    
    # Reparse
    products = extract_products_from_text(invoice['raw_text'])
    
    # Get categorization service
    service = get_categorization_service()
    
    # Insert new products with fresh categorization
    for product in products:
        cat_result = service.categorize(product['name'])
        
        cursor.execute("""
            INSERT INTO products (invoice_id, original_name, display_name, quantity, 
                                 price, btw_percentage, auto_category, auto_subcategory)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            invoice_id,
            product['name'],
            product['name'],
            product.get('quantity', 1),
            product.get('price', 0),
            product.get('btw', ''),
            cat_result['category'],
            cat_result.get('subcategory')
        ))
    
    # Update invoice parsing metadata
    cursor.execute("""
        UPDATE invoices SET 
            parser_version = '2.0',
            last_parsed_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (invoice_id,))
    
    conn.commit()
    conn.close()
    
    return jsonify({
        "status": "reparsed",
        "invoice_id": invoice_id,
        "old_product_count": old_count,
        "new_product_count": len(products)
    })


@app.route("/api/admin/backup")
def api_download_backup():
    """Download database backup"""
    from flask import send_file
    from database import DATABASE_PATH
    return send_file(DATABASE_PATH, as_attachment=True, download_name="appestat_backup.db")


@app.route("/api/admin/reset", methods=["POST"])
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


@app.route("/api/admin/reimport", methods=["POST"])
def api_reimport_all():
    """
    Reimport all invoices from data/invoices directory.
    Handles both online invoices and kassabonnen automatically.
    """
    from parser import import_all_invoices
    from pathlib import Path
    
    results = import_all_invoices("data/invoices", force=False)
    
    return jsonify({
        "status": "success",
        "imported": results.get("success", 0),
        "skipped": results.get("skipped", 0),
        "errors": results.get("errors", 0),
        "total_amount": results.get("total_amount", 0),
    })


@app.route("/api/admin/run-migrations", methods=["POST"])
def api_run_migrations():
    """Run pending database migrations"""
    try:
        from migrations.migration_manager import run_all_migrations
        count = run_all_migrations()
        return jsonify({"status": "success", "migrations_applied": count})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ============ Albert Heijn API integratie ============

@app.route("/api/ah/auth/url")
def ah_auth_url():
    """Return the AH OAuth URL the user should open to authenticate."""
    from services.ah_api_client import AHApiClient
    return jsonify({"url": AHApiClient.get_auth_url()})


@app.route("/api/ah/auth/exchange", methods=["POST"])
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


@app.route("/api/ah/auth/status")
def ah_auth_status():
    """Check whether the user is authenticated with AH."""
    from services.ah_api_client import AHApiClient
    client = AHApiClient()
    return jsonify({
        "authenticated": client.is_authenticated,
        "token_expired": client._is_token_expired() if client.is_authenticated else None,
    })


@app.route("/api/ah/auth", methods=["DELETE"])
def ah_auth_logout():
    """Disconnect from Albert Heijn (clear stored tokens)."""
    from services.ah_api_client import AHApiClient
    AHApiClient().clear_tokens()
    return jsonify({"status": "ok", "message": "Ontkoppeld van Albert Heijn"})


@app.route("/api/ah/sync", methods=["POST"])
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
                    return jsonify({
                        "status": "ok",
                        "synced": len(synced),
                        "skipped": skipped,
                        "errors": errors,
                        "message": f"{len(synced)} nieuwe bonnen gesynchroniseerd, {skipped} overgeslagen",
                        "details": synced,
                    })
                continue

            consecutive_existing = 0
            time.sleep(0.3)  # Rate limiting

            try:
                receipt_details = client.get_receipt(receipt_id)
                if not receipt_details:
                    errors.append({"receipt_id": receipt_id, "error": "Lege bongegevens ontvangen"})
                    continue

                parsed = source.receipt_to_parsed(receipt_details)

                invoice_id = save_invoice({
                    "file_hash": parsed.file_hash,
                    "filename": parsed.filename,
                    "invoice_number": parsed.invoice_number,
                    "invoice_date": parsed.date.isoformat() if parsed.date else None,
                    "total_amount": parsed.total_amount,
                    "total_savings": parsed.total_savings,
                    "raw_text": "",
                    "source_type": "ah_api",
                    "external_id": parsed.external_id,
                })

                save_products(invoice_id, [
                    {
                        "name": p.name,
                        "display_name": p.name,
                        "quantity": p.quantity,
                        "price": p.price,
                        "btw_percentage": p.btw_percentage,
                        "category": p.category,
                        "subcategory": p.subcategory,
                    }
                    for p in parsed.products
                ])

                existing_hashes.add(fh)
                synced.append({
                    "receipt_id": receipt_id,
                    "date": parsed.date.isoformat() if parsed.date else None,
                    "total": parsed.total_amount,
                    "products": len(parsed.products),
                })

            except Exception as e:
                errors.append({"receipt_id": receipt_id, "error": str(e)})

        pagination = page.get("pagination", {})
        total_elements = pagination.get("totalElements", 0)
        offset += batch_size
        if offset >= total_elements:
            break

        time.sleep(0.3)

    return jsonify({
        "status": "ok",
        "synced": len(synced),
        "skipped": skipped,
        "errors": errors,
        "message": f"{len(synced)} nieuwe bonnen gesynchroniseerd",
        "details": synced,
    })


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
