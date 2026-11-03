"""
Migration v013: per-line pack count. Usually taken from the variant (alias), but store
receipts print "RED BULL" for a single can and for an 8-pack alike; services.catalog
estimates the pack from the price for such lines and stores it here.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

VERSION = "013"
NAME = "line_pack_count"
DESCRIPTION = "products.pack_count (+ pack_estimated) for unit prices per piece"


def migrate():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(products)")
    existing = {r["name"] for r in cursor.fetchall()}
    if "pack_count" not in existing:
        cursor.execute("ALTER TABLE products ADD COLUMN pack_count INTEGER")
    if "pack_estimated" not in existing:
        cursor.execute("ALTER TABLE products ADD COLUMN pack_estimated BOOLEAN DEFAULT 0")
    conn.commit()
    conn.close()
    return True
