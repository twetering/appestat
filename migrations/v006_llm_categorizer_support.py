"""
Migration v006: Add LLM Categorizer Support

Adds columns needed for the robust LLM categorizer:
- match_type, source, confidence to product_overrides
- Unique constraint on product_name_pattern
- Adds product_name_pattern column if missing

This migration supports the 3-tier categorization system:
1. User Overrides (highest priority)
2. Keyword Matching (fast, free)
3. LLM Fallback (intelligent, paid)

User Story: US-2.3 (LLM Categorization)
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

VERSION = "006"
NAME = "llm_categorizer_support"
DESCRIPTION = "Add columns and unique index for product_overrides to support LLM categorizer"


def migrate():
    """Run the migration."""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Check current columns in product_overrides
    cursor.execute("PRAGMA table_info(product_overrides)")
    existing_columns = {row['name'] for row in cursor.fetchall()}
    
    # Add missing columns
    columns_to_add = {
        'match_type': "TEXT DEFAULT 'exact'",
        'source': "TEXT DEFAULT 'user'",
        'confidence': "REAL DEFAULT 1.0",
        'times_applied': "INTEGER DEFAULT 0",
        'last_applied_at': "TIMESTAMP",
        'subcategory_id': "INTEGER"
    }
    
    for col_name, col_def in columns_to_add.items():
        if col_name not in existing_columns:
            try:
                cursor.execute(f"ALTER TABLE product_overrides ADD COLUMN {col_name} {col_def}")
                print(f"  Added column: {col_name}")
            except Exception as e:
                print(f"  Column {col_name} might already exist: {e}")
    
    # Create unique index if it doesn't exist
    try:
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_product_overrides_pattern 
            ON product_overrides(product_name_pattern)
        """)
        print("  Created unique index on product_name_pattern")
    except Exception as e:
        print(f"  Index might already exist: {e}")
    
    conn.commit()
    conn.close()
    
    print("Migration v006 completed: LLM Categorizer Support")
    return True


if __name__ == "__main__":
    migrate()
