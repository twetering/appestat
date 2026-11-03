"""
Migration v012: store the normalised alias key on each product line, so lines join to
their variant (product_aliases) in plain SQL. Filled by services.catalog.link_lines().
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

VERSION = "012"
NAME = "product_alias_key"
DESCRIPTION = "products.alias_key for joining lines to catalog variants"


def migrate():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(products)")
    if "alias_key" not in {r["name"] for r in cursor.fetchall()}:
        cursor.execute("ALTER TABLE products ADD COLUMN alias_key TEXT")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_alias_key ON products(alias_key)")
    conn.commit()
    conn.close()
    return True
