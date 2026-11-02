"""
Migration v009: Deduplicate product_overrides and enforce unique index.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

VERSION = "009"
NAME = "dedupe_product_overrides"
DESCRIPTION = "Remove duplicate product_overrides and create unique index"


def migrate():
    conn = get_connection()
    cursor = conn.cursor()
    
    # Determine available columns
    cursor.execute("PRAGMA table_info(product_overrides)")
    columns = [row['name'] for row in cursor.fetchall()]
    if 'product_name_pattern' not in columns:
        conn.close()
        return True
    
    select_cols = [c for c in [
        'product_name_pattern', 'category_id', 'subcategory_id',
        'match_type', 'source', 'confidence', 'times_applied',
        'last_applied_at', 'created_at'
    ] if c in columns]
    
    cursor.execute(f"""
        SELECT {", ".join(select_cols)}
        FROM product_overrides
        ORDER BY product_name_pattern,
                 COALESCE(times_applied, 0) DESC,
                 COALESCE(last_applied_at, created_at) DESC
    """)
    
    rows = cursor.fetchall()
    deduped = {}
    for row in rows:
        pattern = row['product_name_pattern']
        if pattern not in deduped:
            deduped[pattern] = row
    
    # Rebuild table content
    cursor.execute("DELETE FROM product_overrides")
    
    if deduped:
        placeholders = ",".join(["?"] * len(select_cols))
        insert_sql = f"INSERT INTO product_overrides ({', '.join(select_cols)}) VALUES ({placeholders})"
        cursor.executemany(insert_sql, [
            tuple(row[col] for col in select_cols) for row in deduped.values()
        ])
    
    # Drop existing index (non-unique) and create unique index
    cursor.execute("DROP INDEX IF EXISTS idx_product_overrides_pattern")
    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_product_overrides_pattern
        ON product_overrides(product_name_pattern)
    """)
    
    conn.commit()
    conn.close()
    return True


if __name__ == "__main__":
    migrate()
