"""
Migration: Add reparse tracking columns to invoices.
Enables version-controlled reparsing and tracking of parsing quality.
"""

VERSION = "004"
NAME = "add_reparse_tracking"
DESCRIPTION = "Add parser version and reparse tracking to invoices"

UP_SQL = """
-- Add parser version tracking
ALTER TABLE invoices ADD COLUMN parser_version TEXT DEFAULT '1.0';
ALTER TABLE invoices ADD COLUMN last_parsed_at TIMESTAMP;
ALTER TABLE invoices ADD COLUMN parse_quality_score REAL;
ALTER TABLE invoices ADD COLUMN parse_notes TEXT;

-- Update existing invoices
UPDATE invoices SET parser_version = '1.0', last_parsed_at = created_at WHERE parser_version IS NULL
"""

DOWN_SQL = """
-- SQLite doesn't support DROP COLUMN
"""


