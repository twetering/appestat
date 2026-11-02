"""
Migration v008: Add llm_suggestions table.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection

VERSION = "008"
NAME = "add_llm_suggestions"
DESCRIPTION = "Create llm_suggestions table for reviewable LLM output"


def migrate():
    """Run the migration."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS llm_suggestions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            subcategory_id INTEGER,
            confidence REAL DEFAULT 0.0,
            model TEXT,
            prompt_version TEXT,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            decided_at TIMESTAMP,
            decided_by TEXT,
            FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE,
            FOREIGN KEY (subcategory_id) REFERENCES subcategories(id) ON DELETE SET NULL,
            UNIQUE(product_name)
        )
    """)
    
    conn.commit()
    conn.close()
    return True


if __name__ == "__main__":
    migrate()
