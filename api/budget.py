"""Category budgets per year, alerts."""

from datetime import datetime

from flask import Blueprint, jsonify, request


bp = Blueprint("budget_api", __name__)


def _this_year() -> int:
    """Budget and reference years always follow the calendar (see CLAUDE.md)."""
    return datetime.now().year


@bp.route("/api/budget/dashboard")
def api_budget_dashboard():
    """Get comprehensive budget dashboard data"""
    from services.budget import get_budget_service
    year = request.args.get("year", _this_year(), type=int)
    service = get_budget_service()
    return jsonify(service.get_budget_dashboard(year))


@bp.route("/api/budget/categories")
def api_budget_categories():
    """Get category-level budgets"""
    from services.budget import get_budget_service
    year = request.args.get("year", _this_year(), type=int)
    service = get_budget_service()
    return jsonify(service.get_category_budgets(year))


@bp.route("/api/budget/categories", methods=["POST"])
def api_set_category_budget():
    """Set a category budget"""
    from services.budget import get_budget_service
    service = get_budget_service()
    
    data = request.json
    year = data.get("year", _this_year())
    category_id = data.get("category_id")
    target_amount = data.get("target_amount")
    warning_threshold = data.get("warning_threshold", 0.8)
    notes = data.get("notes")
    
    if not category_id or target_amount is None:
        return jsonify({"error": "category_id and target_amount are required"}), 400
    
    result = service.set_category_budget(year, category_id, target_amount, warning_threshold, notes)
    return jsonify(result)


@bp.route("/api/budget/categories/generate", methods=["POST"])
def api_generate_category_budgets():
    """Auto-generate category budgets from historical spending"""
    from services.budget import get_budget_service
    service = get_budget_service()
    
    data = request.json or {}
    target_year = data.get("target_year", _this_year())
    reduction = data.get("reduction_percentage", 0)
    
    results = service.generate_category_budgets_from_history(target_year, reduction)
    return jsonify({
        "status": "generated",
        "target_year": target_year,
        "reduction_percentage": reduction,
        "categories": results
    })


@bp.route("/api/budget/alerts")
def api_budget_alerts():
    """Get budget alerts"""
    from services.budget import get_budget_service
    service = get_budget_service()
    
    year = request.args.get("year", _this_year(), type=int)
    
    # Check for new alerts
    service.check_and_create_alerts(year)
    
    # Get dashboard which includes alerts
    dashboard = service.get_budget_dashboard(year)
    return jsonify(dashboard.get("alerts", []))


@bp.route("/api/budget/alerts/<int:alert_id>/dismiss", methods=["POST"])
def api_dismiss_alert(alert_id):
    """Dismiss a budget alert"""
    from services.budget import get_budget_service
    service = get_budget_service()
    
    success = service.dismiss_alert(alert_id)
    if success:
        return jsonify({"status": "dismissed"})
    return jsonify({"error": "Alert not found"}), 404

