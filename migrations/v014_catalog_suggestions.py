"""
Migration v014: AI suggestions on catalog products. When the model strongly disagrees
with a category you chose yourself, it never overrides it; it leaves a suggestion for the
review queue instead.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

VERSION = "014"
NAME = "catalog_suggestions"
DESCRIPTION = "suggested_category/subcategory/confidence on catalog_products"


def migrate():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(catalog_products)")
    existing = {r["name"] for r in cursor.fetchall()}
    for column, ddl in {"suggested_category": "TEXT", "suggested_subcategory": "TEXT",
                        "suggested_confidence": "REAL"}.items():
        if column not in existing:
            cursor.execute(f"ALTER TABLE catalog_products ADD COLUMN {column} {ddl}")
    conn.commit()
    conn.close()
    return True
