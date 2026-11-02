"""
Migration: Create category_keywords table.
Moves hardcoded keywords from Python to database for persistence and UI management.
"""

VERSION = "002"
NAME = "add_category_keywords_table"
DESCRIPTION = "Create category_keywords table for persistent keyword storage"

UP_SQL = """
-- Create category_keywords table (replaces in-code CATEGORY_KEYWORDS dict)
CREATE TABLE IF NOT EXISTS category_keywords (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category_id INTEGER NOT NULL,
    subcategory_id INTEGER,
    keyword TEXT NOT NULL,
    match_type TEXT DEFAULT 'contains',
    priority INTEGER DEFAULT 0,
    is_active BOOLEAN DEFAULT 1,
    created_by TEXT DEFAULT 'system',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE,
    FOREIGN KEY (subcategory_id) REFERENCES subcategories(id) ON DELETE SET NULL,
    UNIQUE(category_id, keyword)
);

-- Create index for fast keyword lookups
CREATE INDEX IF NOT EXISTS idx_category_keywords_keyword ON category_keywords(keyword);
CREATE INDEX IF NOT EXISTS idx_category_keywords_active ON category_keywords(is_active, priority DESC)
"""

DOWN_SQL = """
DROP INDEX IF EXISTS idx_category_keywords_active;
DROP INDEX IF EXISTS idx_category_keywords_keyword;
DROP TABLE IF EXISTS category_keywords
"""


