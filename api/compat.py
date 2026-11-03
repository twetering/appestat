"""v2 API kept for agents and scripts (see API.md): analysis, invoices, product lines, imports."""

from pathlib import Path

from flask import Blueprint, jsonify, request

from database import (
    get_analysis_data, get_all_invoices, get_invoice_with_products, get_all_products, update_product_category, validate_product, get_products_by_category,
    bulk_update_category_by_name, bulk_update_category_by_names,
    bulk_update_subcategory_by_names, update_product_subcategory, get_day_of_week_analysis, get_bonus_analysis,
    get_spending_prediction,
)
from parser import import_all_invoices, import_invoice

bp = Blueprint("compat_api", __name__)


@bp.route("/api/analysis")
def api_analysis():
    """Get complete analysis data, optionally filtered by year"""
    year = request.args.get("year")
    data = get_analysis_data(year=year)
    return jsonify(data)


@bp.route("/api/invoices")
def api_invoices():
    """Get all invoices"""
    return jsonify(get_all_invoices())


@bp.route("/api/invoices/<int:invoice_id>")
def api_invoice_detail(invoice_id):
    """Get single invoice with products"""
    invoice = get_invoice_with_products(invoice_id)
    if not invoice:
        return jsonify({"error": "Invoice not found"}), 404
    return jsonify(invoice)


@bp.route("/api/invoices/import", methods=["POST"])
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
        "needs_review": results.get("needs_review", 0),
        "total_amount": results.get("total_amount", 0),
        "details": results.get("details", [])
    })


@bp.route("/api/products")
def api_products():
    """Get all products"""
    category = request.args.get("category")
    if category:
        return jsonify(get_products_by_category(category))
    return jsonify(get_all_products())


@bp.route("/api/products/<int:product_id>/category", methods=["PUT"])
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


@bp.route("/api/products/<int:product_id>/validate", methods=["PUT"])
def api_validate_product(product_id):
    """Validate or invalidate a product"""
    data = request.json
    is_valid = data.get("is_valid", True)
    notes = data.get("notes", "")
    
    validate_product(product_id, is_valid, notes)
    return jsonify({"status": "validated"})


@bp.route("/api/products/bulk-update", methods=["POST"])
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


@bp.route("/api/products/bulk-update-subcategory", methods=["POST"])
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


@bp.route("/api/insights/day-analysis")
def api_day_analysis():
    """Get day of week spending analysis"""
    year = request.args.get("year")
    return jsonify(get_day_of_week_analysis(year=year))


@bp.route("/api/insights/bonus")
def api_bonus_analysis():
    """Get bonus savings analysis"""
    year = request.args.get("year")
    return jsonify(get_bonus_analysis(year=year))


@bp.route("/api/insights/prediction")
def api_spending_prediction():
    """Get spending prediction for current/selected year"""
    year = request.args.get("year")
    return jsonify(get_spending_prediction(year=year))


@bp.route("/api/invoices/import-single", methods=["POST"])
def api_import_single_invoice():
    """Import a single invoice/receipt by path"""
    data = request.json
    path = data.get("path")
    force = data.get("force", False)
    
    if not path:
        return jsonify({"error": "path is required"}), 400

    # Only files inside the receipts folder; never an arbitrary path on disk
    invoices_dir = (Path(__file__).parent / "data" / "invoices").resolve()
    target = (invoices_dir / path).resolve() if not Path(path).is_absolute() else Path(path).resolve()
    if invoices_dir not in target.parents or not target.is_file():
        return jsonify({"error": "path moet een bestaand bestand in data/invoices zijn"}), 400

    result = import_invoice(str(target), force=force)
    return jsonify(result)


@bp.route("/api/products/<int:product_id>/subcategory", methods=["PUT"])
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


@bp.route("/api/invoices/<int:invoice_id>/reparse", methods=["POST"])
def api_reparse_invoice(invoice_id):
    """
    Re-parse one invoice from its original file with the matching importer
    (AH online, AH kassabon or PLUS photo). Manual categories are kept.
    """
    from parser import reparse_invoice

    result = reparse_invoice(invoice_id)
    if result["status"] != "success":
        code = 404 if "not found" in result.get("reason", "").lower() else 400
        return jsonify({"error": result.get("reason")}), code
    return jsonify({
        "status": "reparsed",
        "invoice_id": invoice_id,
        "new_product_count": result["products_count"],
        "parse_quality_score": result.get("parse_quality_score"),
        "parse_notes": result.get("parse_notes"),
    })

