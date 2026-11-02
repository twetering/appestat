"""
Migration v007: Add updated_at column to products (if missing).
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

VERSION = "007"
NAME = "add_products_updated_at"
DESCRIPTION = "Add updated_at column to products for auditability"


def migrate():
    """Run the migration."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("PRAGMA table_info(products)")
    columns = {row['name'] for row in cursor.fetchall()}
    
    if 'updated_at' not in columns:
        cursor.execute("ALTER TABLE products ADD COLUMN updated_at TIMESTAMP")
        # Backfill with created_at where available
        try:
            cursor.execute("UPDATE products SET updated_at = COALESCE(updated_at, created_at)")
        except Exception:
            pass
    
    conn.commit()
    conn.close()
    return True


if __name__ == "__main__":
    migrate()
