#!/usr/bin/env python3
"""
Appestat — your groceries, analysed locally.

This module only builds the Flask app; routes live in blueprints:
    web/pages.py       HTML pages
    api/insights.py    dashboard, years, price insights
    api/catalog.py     products (catalog), categories per product, review queue
    api/receipts.py    receipts, upload, export
    api/categories.py  categories, subcategories, keyword rules
    api/budget.py      budgets
    api/admin.py       backup, reset, reimport, AH app link
    api/compat.py      v2 API kept for agents and scripts
"""

import os

from flask import Flask, jsonify, render_template, request
from werkzeug.exceptions import HTTPException

from database import init_database

app = Flask(__name__)

# Schema: base tables + all versioned migrations
init_database()

from api import admin, budget, catalog, categories, compat, insights, receipts  # noqa: E402 - needs the schema
from web.pages import bp as pages  # noqa: E402

for module in (insights, catalog, receipts, categories, budget, admin, compat):
    app.register_blueprint(module.bp)
app.register_blueprint(pages)


@app.errorhandler(HTTPException)
def handle_http_exception(e):
    """JSON for API errors, a friendly page elsewhere."""
    if request.path.startswith("/api/"):
        return jsonify({"error": e.description}), e.code
    if e.code == 404:
        return render_template("pages/not_found.html", active=None), 404
    return e


@app.errorhandler(Exception)
def handle_exception(e):
    """JSON for API errors; re-raise for non-API routes."""
    if request.path.startswith("/api/"):
        return jsonify({"error": str(e)}), 500
    raise e


if __name__ == "__main__":
    # Debug (auto-reload + Werkzeug debugger) only when asked for: the debugger allows code
    # execution, so it must never be on by default. Binds to localhost only.
    debug = os.getenv("APPESTAT_DEBUG", "0") == "1"
    app.run(host="127.0.0.1", port=int(os.getenv("APPESTAT_PORT", "5050")), debug=debug)
