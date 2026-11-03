"""
Migration v015: seed subcategories and keyword rules from data/ah_categories.json on a
fresh database. Existing databases (non-empty tables) are left untouched.
"""

import contextlib
import io
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

VERSION = "015"
NAME = "seed_taxonomy"
DESCRIPTION = "Seed subcategories/keywords from ah_categories.json when empty"


def migrate():
    from scripts import full_reset

    conn = get_connection()
    subcategories = conn.execute("SELECT COUNT(*) FROM subcategories").fetchone()[0]
    keywords = conn.execute("SELECT COUNT(*) FROM category_keywords").fetchone()[0]
    conn.close()
    with contextlib.redirect_stdout(io.StringIO()):
        if not subcategories:
            full_reset.seed_subcategories()
        if not keywords:
            full_reset.seed_keywords()
    return True
