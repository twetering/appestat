"""
Migration: Enhance product_overrides table for learning system.
Adds subcategory support, match types, and confidence tracking.
"""

VERSION = "003"
NAME = "enhance_product_overrides"
DESCRIPTION = "Add learning-related columns to product_overrides"

UP_SQL = """
-- Enhance product_overrides for learning system
ALTER TABLE product_overrides ADD COLUMN subcategory_id INTEGER REFERENCES subcategories(id);
ALTER TABLE product_overrides ADD COLUMN match_type TEXT DEFAULT 'exact';
ALTER TABLE product_overrides ADD COLUMN confidence REAL DEFAULT 1.0;
ALTER TABLE product_overrides ADD COLUMN source TEXT DEFAULT 'user';
ALTER TABLE product_overrides ADD COLUMN times_applied INTEGER DEFAULT 0;
ALTER TABLE product_overrides ADD COLUMN last_applied_at TIMESTAMP;

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_product_overrides_pattern ON product_overrides(product_name_pattern)
"""

DOWN_SQL = """
-- SQLite doesn't support DROP COLUMN, would need table recreation
"""


