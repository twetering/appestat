#!/usr/bin/env python3
"""
Complete database reset and reseed script.
Removes all data and reimports everything fresh from ah_categories.json and PDF files.
"""

import sys
import json
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection, DATABASE_PATH


def load_categories_json():
    """Load category definitions from JSON file."""
    json_path = Path(__file__).parent.parent / "data" / "ah_categories.json"
    with open(json_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def reset_database():
    """Delete all invoice, product, keyword, and category data."""
    print("\n" + "=" * 60)
    print("🗑️  FASE 1: DATABASE RESET")
    print("=" * 60)
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Delete in correct order (foreign keys)
    tables_to_clear = [
        ("products", "producten"),
        ("invoices", "facturen"),
        ("category_keywords", "keywords"),
        ("subcategories", "subcategorieën"),
        ("budget_categories", "budget categorieën"),
        ("budget_goals", "budget goals"),
        ("budget_alerts", "budget alerts"),
        ("product_overrides", "product overrides"),
        ("validation_feedback", "validatie feedback"),
    ]
    
    for table, label in tables_to_clear:
        try:
            cursor.execute(f"DELETE FROM {table}")
            print(f"   ✓ {cursor.rowcount} {label} verwijderd")
        except Exception as e:
            print(f"   - {label}: {e}")
    
    # Delete all categories
    cursor.execute("DELETE FROM categories")
    print(f"   ✓ {cursor.rowcount} categorieën verwijderd")
    
    conn.commit()
    conn.close()
    
    print("\n   ✅ Database is leeg")


def seed_categories():
    """Create fresh categories from JSON."""
    print("\n" + "=" * 60)
    print("📁 FASE 2: CATEGORIEËN AANMAKEN")
    print("=" * 60)
    
    data = load_categories_json()
    categories = data.get("categories", {})
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cat_count = 0
    for i, (name, cat_data) in enumerate(categories.items()):
        cursor.execute("""
            INSERT INTO categories (name, icon, color, is_system, sort_order)
            VALUES (?, ?, ?, 1, ?)
        """, (name, cat_data.get("icon", "📋"), cat_data.get("color", "#9E9E9E"), i + 1))
        cat_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"   ✓ {cat_count} categorieën aangemaakt")


def seed_subcategories():
    """Create subcategories from JSON."""
    print("\n" + "=" * 60)
    print("📂 FASE 3: SUBCATEGORIEËN AANMAKEN")
    print("=" * 60)
    
    data = load_categories_json()
    categories = data.get("categories", {})
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get category IDs
    cursor.execute("SELECT id, name FROM categories")
    cat_ids = {row['name']: row['id'] for row in cursor.fetchall()}
    
    subcat_count = 0
    for cat_name, cat_data in categories.items():
        cat_id = cat_ids.get(cat_name)
        if not cat_id:
            continue
        
        subcats = cat_data.get("subcategories", [])
        for i, subcat in enumerate(subcats):
            cursor.execute("""
                INSERT INTO subcategories (name, category_id, icon, sort_order)
                VALUES (?, ?, ?, ?)
            """, (subcat["name"], cat_id, subcat.get("icon", "📋"), i + 1))
            subcat_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"   ✓ {subcat_count} subcategorieën aangemaakt")


def seed_keywords():
    """Create keywords from JSON (both category and subcategory level)."""
    print("\n" + "=" * 60)
    print("🏷️  FASE 4: KEYWORDS AANMAKEN")
    print("=" * 60)
    
    data = load_categories_json()
    categories = data.get("categories", {})
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get category and subcategory IDs
    cursor.execute("SELECT id, name FROM categories")
    cat_ids = {row['name']: row['id'] for row in cursor.fetchall()}
    
    cursor.execute("SELECT id, name, category_id FROM subcategories")
    subcat_rows = cursor.fetchall()
    # Create lookup: (category_id, subcat_name) -> subcat_id
    subcat_ids = {(row['category_id'], row['name']): row['id'] for row in subcat_rows}
    
    keyword_count = 0
    
    for cat_name, cat_data in categories.items():
        cat_id = cat_ids.get(cat_name)
        if not cat_id:
            continue
        
        # Category-level keywords
        cat_keywords = cat_data.get("keywords", [])
        for keyword in cat_keywords:
            try:
                cursor.execute("""
                    INSERT INTO category_keywords (category_id, keyword, match_type, priority, created_by)
                    VALUES (?, ?, 'contains', 0, 'system')
                """, (cat_id, keyword.lower()))
                keyword_count += 1
            except Exception:
                pass  # Ignore duplicates
        
        # Subcategory-level keywords
        subcats = cat_data.get("subcategories", [])
        for subcat in subcats:
            subcat_id = subcat_ids.get((cat_id, subcat["name"]))
            subcat_keywords = subcat.get("keywords", [])
            
            for keyword in subcat_keywords:
                try:
                    cursor.execute("""
                        INSERT INTO category_keywords (category_id, subcategory_id, keyword, match_type, priority, created_by)
                        VALUES (?, ?, ?, 'contains', 10, 'system')
                    """, (cat_id, subcat_id, keyword.lower()))
                    keyword_count += 1
                except Exception:
                    pass  # Ignore duplicates
    
    conn.commit()
    conn.close()
    
    print(f"   ✓ {keyword_count} keywords aangemaakt")


def reimport_invoices():
    """Reimport all PDF files from data/invoices directory."""
    print("\n" + "=" * 60)
    print("📥 FASE 5: FACTUREN IMPORTEREN")
    print("=" * 60)
    
    from parser import import_all_invoices
    
    base_path = Path(__file__).parent.parent / "data"
    
    # Import all PDFs from unified invoices directory
    invoices_dir = base_path / "invoices"
    if invoices_dir.exists():
        result = import_all_invoices(str(invoices_dir), force=False)
        print(f"\n   ✓ Totaal: {result.get('success', 0)} geïmporteerd, {result.get('skipped', 0)} overgeslagen")
        print(f"   ✓ Totaalbedrag: €{result.get('total_amount', 0):.2f}")
    else:
        print(f"   - Map {invoices_dir} niet gevonden")


def verify_results():
    """Verify the reset results."""
    print("\n" + "=" * 60)
    print("✅ VERIFICATIE")
    print("=" * 60)
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) as cnt FROM categories")
    print(f"   Categorieën: {cursor.fetchone()['cnt']}")
    
    cursor.execute("SELECT COUNT(*) as cnt FROM subcategories")
    print(f"   Subcategorieën: {cursor.fetchone()['cnt']}")
    
    cursor.execute("SELECT COUNT(*) as cnt FROM category_keywords")
    print(f"   Keywords: {cursor.fetchone()['cnt']}")
    
    cursor.execute("SELECT COUNT(*) as cnt FROM invoices")
    print(f"   Facturen: {cursor.fetchone()['cnt']}")
    
    cursor.execute("SELECT COUNT(*) as cnt FROM products")
    print(f"   Producten: {cursor.fetchone()['cnt']}")
    
    # Check product distribution
    cursor.execute("""
        SELECT COALESCE(user_category, auto_category) as cat, COUNT(*) as cnt
        FROM products
        GROUP BY cat
        ORDER BY cnt DESC
        LIMIT 10
    """)
    print("\n   Top 10 categorieën met producten:")
    for row in cursor.fetchall():
        print(f"      {row['cat']}: {row['cnt']}")
    
    conn.close()


def run_full_reset(skip_reimport=False):
    """Run the complete reset process."""
    print("\n" + "=" * 60)
    print("🔄 APPESTAT COMPLETE DATABASE RESET")
    print("=" * 60)
    print(f"\n   Database: {DATABASE_PATH}")
    
    reset_database()
    seed_categories()
    seed_subcategories()
    seed_keywords()
    
    if not skip_reimport:
        reimport_invoices()
    
    verify_results()
    
    print("\n" + "=" * 60)
    print("🎉 RESET VOLTOOID")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Complete Appestat database reset")
    parser.add_argument("--skip-reimport", action="store_true", 
                        help="Skip reimporting PDF files (only reset and seed)")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Skip confirmation prompt")
    
    args = parser.parse_args()
    
    if not args.yes:
        print("\n⚠️  WAARSCHUWING: Dit verwijdert ALLE data uit de database!")
        print("   - Alle facturen")
        print("   - Alle producten")
        print("   - Alle categorieën en keywords")
        print("\n   De data wordt opnieuw geïmporteerd vanuit de PDF bestanden.\n")
        
        confirm = input("Weet je het zeker? (type 'ja' om door te gaan): ")
        if confirm.lower() != 'ja':
            print("Geannuleerd.")
            sys.exit(0)
    
    run_full_reset(skip_reimport=args.skip_reimport)

