"""
Page routes. Every page extends templates/base.html and loads its data from /api in one
call. Old URLs redirect so bookmarks keep working.
"""

import time

from flask import Blueprint, redirect, render_template, request

bp = Blueprint("pages", __name__)
ASSET_VERSION = str(int(time.time()))


@bp.app_context_processor
def inject_globals():
    return {"asset_version": ASSET_VERSION}


def page(template: str, active: str, **ctx):
    return render_template(f"pages/{template}.html", active=active, **ctx)


@bp.get("/")
def dashboard():
    return page("dashboard", "dashboard")


@bp.get("/products")
def products():
    return page("products", "products")


@bp.get("/products/<int:catalog_id>")
def product(catalog_id):
    return page("product", "products", catalog_id=catalog_id)


@bp.get("/receipts")
def receipts():
    return page("receipts", "receipts")


@bp.get("/receipts/<int:invoice_id>")
def receipt(invoice_id):
    return page("receipt", "receipts", invoice_id=invoice_id)


@bp.get("/categories")
def categories():
    return page("categories", "categories")


@bp.get("/review")
def review():
    return page("review", "review")


@bp.get("/budget")
def budget():
    return page("budget", "budget")


@bp.get("/years")
def years():
    return page("years", "years")


@bp.get("/import")
def import_center():
    return page("import", "import")


# ---- Old URLs (v2) ----
@bp.get("/invoices")
def old_invoices():
    return redirect("/receipts", code=301)


@bp.get("/invoice/<int:invoice_id>")
def old_invoice(invoice_id):
    return redirect(f"/receipts/{invoice_id}", code=301)


@bp.get("/search")
def old_search():
    q = request.args.get("q")
    return redirect(f"/products?q={q}" if q else "/products", code=301)


@bp.get("/category-editor")
def old_category_editor():
    return redirect("/categories#regels", code=301)


@bp.get("/admin")
def old_admin():
    return redirect("/import", code=301)
