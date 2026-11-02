"""
Migration Manager for Appestat Database
Handles versioned schema migrations with rollback support.
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Callable, Optional
import importlib
import sys

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import DATABASE_PATH, get_connection


def ensure_migration_table():
    """Create the migrations tracking table if it doesn't exist."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            version TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            checksum TEXT
        )
    """)
    
    conn.commit()
    conn.close()


def get_applied_migrations() -> List[str]:
    """Get list of already applied migration versions."""
    ensure_migration_table()
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT version FROM schema_migrations ORDER BY version")
    versions = [row['version'] for row in cursor.fetchall()]
    
    conn.close()
    return versions


def record_migration(version: str, name: str, checksum: str = None):
    """Record a migration as applied."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO schema_migrations (version, name, checksum)
        VALUES (?, ?, ?)
    """, (version, name, checksum))
    
    conn.commit()
    conn.close()


def run_migration(version: str, name: str, up_sql: str, description: str = ""):
    """Run a single migration if not already applied."""
    applied = get_applied_migrations()
    
    if version in applied:
        print(f"  ⏭️  {version} - {name} (already applied)")
        return False
    
    print(f"  ▶️  {version} - {name}")
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Execute each statement separately
        for statement in up_sql.split(';'):
            statement = statement.strip()
            if statement:
                cursor.execute(statement)
        
        conn.commit()
        record_migration(version, name)
        print(f"     ✅ Applied successfully")
        return True
        
    except Exception as e:
        conn.rollback()
        print(f"     ❌ Error: {e}")
        raise
    finally:
        conn.close()


def _run_migration_module(module) -> bool:
    """Run a migration module that may expose UP_SQL or a migrate() function."""
    version = module.VERSION
    name = module.NAME
    description = getattr(module, "DESCRIPTION", "")
    
    applied = get_applied_migrations()
    if version in applied:
        print(f"  ⏭️  {version} - {name} (already applied)")
        return False
    
    print(f"  ▶️  {version} - {name}")
    try:
        if hasattr(module, "migrate"):
            module.migrate()
            record_migration(version, name)
            print(f"     ✅ Applied successfully")
        else:
            run_migration(version, name, module.UP_SQL, description)
        return True
    except Exception as e:
        print(f"     ❌ Error: {e}")
        raise


def run_all_migrations():
    """Run all pending migrations in order."""
    print("\n🔄 Running database migrations...")
    print("=" * 50)
    
    migrations_applied = 0
    
    # Import and run each migration
    from migrations import v001_add_source_columns
    from migrations import v002_add_category_keywords_table
    from migrations import v003_enhance_product_overrides
    from migrations import v004_add_reparse_tracking
    from migrations import v005_budget_categories
    from migrations import v006_llm_categorizer_support
    from migrations import v007_add_products_updated_at
    from migrations import v008_add_llm_suggestions
    from migrations import v009_dedupe_product_overrides
    
    migration_modules = [
        v001_add_source_columns,
        v002_add_category_keywords_table,
        v003_enhance_product_overrides,
        v004_add_reparse_tracking,
        v005_budget_categories,
        v006_llm_categorizer_support,
        v007_add_products_updated_at,
        v008_add_llm_suggestions,
        v009_dedupe_product_overrides,
    ]
    
    for module in migration_modules:
        if _run_migration_module(module):
            migrations_applied += 1
    
    print("=" * 50)
    if migrations_applied > 0:
        print(f"✅ Applied {migrations_applied} migration(s)")
    else:
        print("✅ Database is up to date")
    print()
    
    return migrations_applied


if __name__ == "__main__":
    run_all_migrations()
