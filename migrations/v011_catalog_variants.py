"""
Migration v011: product variants on aliases, review flags, and a backup of line-level
manual categories before they move to the catalog.

A catalog product ("Red Bull Energy drink") has many receipt spellings (aliases), each
possibly a different pack ("…24-pack" -> pack_count 24). Unit prices divide by pack_count.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

VERSION = "011"
NAME = "catalog_variants"
DESCRIPTION = "Variant fields on product_aliases, review flag on catalog, legacy category backup"


def _add_columns(cursor, table, columns):
    cursor.execute(f"PRAGMA table_info({table})")
    existing = {r["name"] for r in cursor.fetchall()}
    for column, ddl in columns.items():
        if column not in existing:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")


def migrate():
    conn = get_connection()
    cursor = conn.cursor()
    _add_columns(cursor, "product_aliases", {
        "pack_count": "INTEGER DEFAULT 1",
        "size_value": "REAL",
        "size_unit": "TEXT",
        "chain": "TEXT",
    })
    _add_columns(cursor, "catalog_products", {
        "needs_review": "BOOLEAN DEFAULT 0",
    })
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS legacy_line_categories (
            product_id INTEGER PRIMARY KEY,
            original_name TEXT,
            user_category TEXT,
            user_subcategory TEXT,
            saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()
    return True
