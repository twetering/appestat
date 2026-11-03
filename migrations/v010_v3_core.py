"""
Migration v010: Appestat 3.0 core schema (additive only).

- stores + invoices.store_id: which shop a receipt is from
- invoice_extras: non-product receipt lines (deposits, delivery, subscriptions, savings
  stamps) so that products + extras reconcile with the paid total
- products: gross/discount/unit price, weight, bonus flag, link to the product catalog
- catalog_products + product_aliases: one product with many receipt spellings/variants
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

VERSION = "010"
NAME = "v3_core"
DESCRIPTION = "Stores, invoice extras, product catalog and richer product lines"

NEW_PRODUCT_COLUMNS = {
    "gross_price": "REAL",
    "discount_amount": "REAL DEFAULT 0",
    "unit_price": "REAL",
    "weight_kg": "REAL",
    "is_bonus": "BOOLEAN DEFAULT 0",
    "catalog_id": "INTEGER REFERENCES catalog_products(id) ON DELETE SET NULL",
}


def migrate():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            chain TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS invoice_extras (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL REFERENCES invoices(id) ON DELETE CASCADE,
            extra_type TEXT NOT NULL CHECK (extra_type IN ('deposit','delivery','subscription','savings')),
            description TEXT NOT NULL,
            quantity INTEGER DEFAULT 1,
            amount REAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_invoice_extras_invoice ON invoice_extras(invoice_id)")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS catalog_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            brand TEXT,
            family TEXT,
            category TEXT,
            subcategory TEXT,
            category_source TEXT,
            category_confidence REAL,
            pack_count INTEGER,
            size_value REAL,
            size_unit TEXT,
            is_private_label BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_aliases (
            alias_key TEXT PRIMARY KEY,
            catalog_id INTEGER NOT NULL REFERENCES catalog_products(id) ON DELETE CASCADE,
            example_name TEXT,
            source TEXT,
            confidence REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_aliases_catalog ON product_aliases(catalog_id)")

    cursor.execute("PRAGMA table_info(invoices)")
    if "store_id" not in {r["name"] for r in cursor.fetchall()}:
        cursor.execute("ALTER TABLE invoices ADD COLUMN store_id INTEGER REFERENCES stores(id)")

    cursor.execute("PRAGMA table_info(products)")
    existing = {r["name"] for r in cursor.fetchall()}
    for column, ddl in NEW_PRODUCT_COLUMNS.items():
        if column not in existing:
            cursor.execute(f"ALTER TABLE products ADD COLUMN {column} {ddl}")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_catalog ON products(catalog_id)")

    conn.commit()
    conn.close()
    return True
