"""
Migration v017: "Sport- & dieetvoeding" category (protein shakes/powder, protein bars, meal
replacements such as Jimmy Joy). Before this, whey ended up under yoghurt and vegan protein
under meat substitutes. Adds whatever of it is missing, with its keyword rules.
"""

import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

VERSION = "017"
NAME = "sport_nutrition"
DESCRIPTION = "Category Sport- & dieetvoeding with subcategories and keywords"

CATEGORY = "Sport- & dieetvoeding"
SORT_AFTER = "Tussendoortjes"


def migrate():
    spec = json.loads((Path(__file__).parent.parent / "data" / "ah_categories.json").read_text())
    info = spec["categories"][CATEGORY]
    conn = get_connection()
    cur = conn.cursor()
    row = cur.execute("SELECT id FROM categories WHERE name = ?", (CATEGORY,)).fetchone()
    if row:
        cat_id = row[0]
    else:
        after = cur.execute("SELECT sort_order FROM categories WHERE name = ?", (SORT_AFTER,)).fetchone()
        order = (after[0] if after else 16) + 1
        cur.execute("UPDATE categories SET sort_order = sort_order + 1 WHERE sort_order >= ? AND sort_order < 999",
                    (order,))
        cur.execute("INSERT INTO categories (name, color, icon, sort_order, is_system) VALUES (?, ?, ?, ?, 1)",
                    (CATEGORY, info["color"], info["icon"], order))
        cat_id = cur.lastrowid
    for i, sub in enumerate(info["subcategories"], 1):
        cur.execute("INSERT OR IGNORE INTO subcategories (name, category_id, icon, sort_order) VALUES (?, ?, ?, ?)",
                    (sub["name"], cat_id, sub["icon"], i))
        sub_id = cur.execute("SELECT id FROM subcategories WHERE name = ? AND category_id = ?",
                             (sub["name"], cat_id)).fetchone()[0]
        for kw in sub["keywords"]:
            cur.execute("""INSERT OR IGNORE INTO category_keywords
                           (category_id, subcategory_id, keyword, match_type, priority, created_by)
                           VALUES (?, ?, ?, 'contains', 10, 'system')""", (cat_id, sub_id, kw.lower()))
    for kw in info["keywords"]:
        cur.execute("""INSERT OR IGNORE INTO category_keywords (category_id, keyword, match_type, priority, created_by)
                       VALUES (?, ?, 'contains', 0, 'system')""", (cat_id, kw.lower()))
    conn.commit()
    conn.close()
    return True
