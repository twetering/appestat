"""
Migration: Add source tracking columns to invoices table.
Enables multi-source support (AH online, AH kassabon, Picnic, etc.)
"""

VERSION = "001"
NAME = "add_source_columns"
DESCRIPTION = "Add source_type and metadata columns to invoices for multi-source support"

UP_SQL = """
-- Add source tracking columns
ALTER TABLE invoices ADD COLUMN source_type TEXT DEFAULT 'ah_online';
ALTER TABLE invoices ADD COLUMN source_metadata TEXT;
ALTER TABLE invoices ADD COLUMN external_id TEXT;

-- Add duplicate tracking
ALTER TABLE invoices ADD COLUMN is_duplicate BOOLEAN DEFAULT 0;
ALTER TABLE invoices ADD COLUMN duplicate_of_id INTEGER REFERENCES invoices(id);

-- Update existing invoices based on filename patterns
UPDATE invoices SET source_type = 'ah_kassabon' WHERE filename LIKE 'AH_kassabon%';
UPDATE invoices SET source_type = 'ah_online' WHERE source_type IS NULL OR source_type = ''
"""

DOWN_SQL = """
-- SQLite doesn't support DROP COLUMN directly, would need table recreation
-- This is a placeholder for documentation purposes
"""


