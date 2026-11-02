"""
Database module for AH Invoice Analyzer
Uses SQLite for data persistence and management
"""

import sqlite3
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

DATABASE_PATH = Path(__file__).parent / "data" / "appestat.db"


def get_connection():
    """Get database connection with row factory"""
    conn = sqlite3.connect(str(DATABASE_PATH))
    conn.row_factory = sqlite3.Row
    # Ensure foreign key constraints are enforced in SQLite
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def upsert_product_override(
    product_name_pattern: str,
    category_id: int,
    subcategory_id: Optional[int] = None,
    match_type: str = "exact",
    source: str = "user",
    confidence: float = 1.0,
    increment_times: bool = True,
    conn: Optional[sqlite3.Connection] = None
):
    """Upsert a product override without relying on UNIQUE constraints."""
    owns_conn = False
    if conn is None:
        conn = get_connection()
        owns_conn = True
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT id, times_applied
        FROM product_overrides
        WHERE product_name_pattern = ?
    """, (product_name_pattern,))
    existing = cursor.fetchone()
    
    if existing:
        new_times = (existing['times_applied'] or 0) + (1 if increment_times else 0)
        cursor.execute("""
            UPDATE product_overrides
            SET category_id = ?,
                subcategory_id = ?,
                match_type = ?,
                source = ?,
                confidence = ?,
                times_applied = ?,
                last_applied_at = CURRENT_TIMESTAMP
            WHERE product_name_pattern = ?
        """, (
            category_id, subcategory_id, match_type, source, confidence, new_times, product_name_pattern
        ))
    else:
        times = 1 if increment_times else 0
        cursor.execute("""
            INSERT INTO product_overrides (
                product_name_pattern, category_id, subcategory_id,
                match_type, source, confidence, times_applied, last_applied_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            product_name_pattern, category_id, subcategory_id,
            match_type, source, confidence, times
        ))
    
    if owns_conn:
        conn.commit()
        conn.close()


def init_database():
    """Initialize the database schema"""
    DATABASE_PATH.parent.mkdir(parents=True, exist_ok=True)
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # Invoices table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_hash TEXT UNIQUE NOT NULL,
            filename TEXT NOT NULL,
            invoice_number TEXT,
            invoice_date DATE,
            total_amount REAL,
            total_savings REAL,
            raw_text TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Products table (extracted from invoices)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER NOT NULL,
            original_name TEXT NOT NULL,
            display_name TEXT,
            quantity INTEGER DEFAULT 1,
            price REAL NOT NULL,
            btw_percentage TEXT,
            auto_category TEXT,
            user_category TEXT,
            auto_subcategory TEXT,
            user_subcategory TEXT,
            is_validated BOOLEAN DEFAULT 0,
            validation_notes TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (invoice_id) REFERENCES invoices(id) ON DELETE CASCADE
        )
    """)
    
    # Custom categories table (met parent_id voor subcategorieën)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            parent_id INTEGER,
            color TEXT,
            icon TEXT,
            is_system BOOLEAN DEFAULT 0,
            sort_order INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (parent_id) REFERENCES categories(id) ON DELETE SET NULL
        )
    """)
    
    # Subcategories table (voor meer gedetailleerde categorisatie)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS subcategories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            color TEXT,
            icon TEXT,
            sort_order INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE,
            UNIQUE(name, category_id)
        )
    """)
    
    # Category rules table (for automatic categorization)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS category_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id INTEGER NOT NULL,
            keyword TEXT NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
        )
    """)

    # Category keywords table (preferred over category_rules)
    cursor.execute("""
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
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_category_keywords_keyword ON category_keywords(keyword)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_category_keywords_active ON category_keywords(is_active, priority DESC)
    """)
    
    # Product category overrides (user corrections)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name_pattern TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            subcategory_id INTEGER,
            match_type TEXT DEFAULT 'exact',
            source TEXT DEFAULT 'user',
            confidence REAL DEFAULT 1.0,
            times_applied INTEGER DEFAULT 0,
            last_applied_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE,
            FOREIGN KEY (subcategory_id) REFERENCES subcategories(id) ON DELETE SET NULL
        )
    """)

    cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_product_overrides_pattern
        ON product_overrides(product_name_pattern)
    """)

    # LLM suggestions queue (review before applying)
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
    
    # Validation feedback table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS validation_feedback (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER,
            product_id INTEGER,
            feedback_type TEXT NOT NULL,
            original_value TEXT,
            corrected_value TEXT,
            notes TEXT,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (invoice_id) REFERENCES invoices(id) ON DELETE CASCADE,
            FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
        )
    """)

    # Budget goals table (Budget 2026)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS budget_goals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name TEXT NOT NULL,
            reference_year INTEGER NOT NULL,
            target_year INTEGER NOT NULL,
            reference_quantity REAL DEFAULT 0,
            reference_total_cost REAL DEFAULT 0,
            target_quantity REAL,
            moscow_label TEXT,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(product_name, target_year)
        )
    """)

    conn.commit()
    
    # Insert default categories (AH.nl gebaseerde structuur)
    default_categories = [
        ("Groente & Aardappelen", "#7CB518", "🥦", 1, 1),
        ("Fruit & Verse sappen", "#E63946", "🍎", 1, 2),
        ("Maaltijden & Salades", "#2A9D8F", "🥗", 1, 3),
        ("Vlees", "#C1121F", "🥩", 1, 4),
        ("Vis", "#0077B6", "🐟", 1, 5),
        ("Vegetarisch & Vegan", "#52B788", "🌱", 1, 6),
        ("Vleeswaren", "#BC6C25", "🥓", 1, 7),
        ("Kaas", "#FFBA08", "🧀", 1, 8),
        ("Zuivel & Eieren", "#00ADE6", "🥛", 1, 9),
        ("Bakkerij", "#D4A574", "🍞", 1, 10),
        ("Borrel, Chips & Snacks", "#F4A261", "🥜", 1, 11),
        ("Pasta, Rijst & Wereldkeuken", "#FFB347", "🍝", 1, 12),
        ("Soepen, Sauzen & Kruiden", "#FF7043", "🫙", 1, 13),
        ("Koek, Snoep & Chocolade", "#AB47BC", "🍫", 1, 14),
        ("Ontbijtgranen & Beleg", "#FFC300", "🥣", 1, 15),
        ("Tussendoortjes", "#E9C46A", "🍪", 1, 16),
        ("Diepvries", "#48CAE4", "❄️", 1, 17),
        ("Koffie & Thee", "#6F4E37", "☕", 1, 18),
        ("Frisdrank & Water", "#4FC3F7", "🥤", 1, 19),
        ("Bier, Wijn & Aperitieven", "#722F37", "🍷", 1, 20),
        ("Drogisterij", "#F48FB1", "🧴", 1, 21),
        ("Huishouden", "#78909C", "🧹", 1, 22),
        ("Baby & Kind", "#90CAF9", "👶", 1, 23),
        ("Huisdier", "#A1887F", "🐕", 1, 24),
        ("Verpakking & Statiegeld", "#90A4AE", "📦", 1, 25),
        ("Bezorgkosten", "#607D8B", "🚚", 1, 26),
        ("Overig", "#9E9E9E", "📋", 1, 99),
    ]
    
    for name, color, icon, is_system, sort_order in default_categories:
        cursor.execute("""
            INSERT OR IGNORE INTO categories (name, color, icon, is_system, sort_order)
            VALUES (?, ?, ?, ?, ?)
        """, (name, color, icon, is_system, sort_order))
    
    conn.commit()
    conn.close()


def calculate_file_hash(filepath: str) -> str:
    """Calculate MD5 hash of a file for duplicate detection"""
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        buf = f.read(65536)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(65536)
    return hasher.hexdigest()


def invoice_exists(file_hash: str) -> bool:
    """Check if an invoice with this hash already exists"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM invoices WHERE file_hash = ?", (file_hash,))
    result = cursor.fetchone()
    conn.close()
    return result is not None


def get_invoice_by_hash(file_hash: str) -> Optional[Dict]:
    """Get invoice by file hash"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM invoices WHERE file_hash = ?", (file_hash,))
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def save_invoice(invoice_data: Dict) -> int:
    """Save an invoice to the database, returns invoice_id"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Support both old and new field names for backwards compatibility
    invoice_date = invoice_data.get('invoice_date') or invoice_data.get('date')
    total_amount = invoice_data.get('total_amount') or invoice_data.get('total', 0)
    total_savings = invoice_data.get('total_savings') or invoice_data.get('savings', 0)
    
    cursor.execute("""
        INSERT INTO invoices (file_hash, filename, invoice_number, invoice_date,
                             total_amount, total_savings, raw_text, source_type, external_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        invoice_data['file_hash'],
        invoice_data['filename'],
        invoice_data.get('invoice_number'),
        invoice_date,
        total_amount,
        total_savings,
        invoice_data.get('raw_text', ''),
        invoice_data.get('source_type', 'ah_online'),
        invoice_data.get('external_id'),
    ))
    
    invoice_id = cursor.lastrowid

    # Optional parse-quality columns come from migrations; only fill them when present
    columns = {row['name'] for row in cursor.execute("PRAGMA table_info(invoices)").fetchall()}
    extras = {
        'source_metadata': json.dumps(invoice_data['source_metadata'], default=str)
        if invoice_data.get('source_metadata') else None,
        'parse_quality_score': invoice_data.get('parse_quality_score'),
        'parse_notes': invoice_data.get('parse_notes'),
    }
    extras = {k: v for k, v in extras.items() if k in columns and v is not None}
    if extras:
        assignments = ", ".join(f"{k} = ?" for k in extras)
        cursor.execute(f"UPDATE invoices SET {assignments} WHERE id = ?", (*extras.values(), invoice_id))

    conn.commit()
    conn.close()
    return invoice_id


def save_products(invoice_id: int, products: List[Dict]):
    """Save products for an invoice"""
    conn = get_connection()
    cursor = conn.cursor()
    
    for product in products:
        # Support both field name formats
        name = product.get('name') or product.get('original_name') or product.get('display_name')
        display = product.get('display_name') or name
        category = product.get('category') or product.get('auto_category') or 'Overig'
        subcategory = product.get('subcategory') or product.get('auto_subcategory')
        btw = product.get('btw') or product.get('btw_percentage') or ''
        
        cursor.execute("""
            INSERT INTO products (invoice_id, original_name, display_name, quantity, 
                                 price, btw_percentage, auto_category, auto_subcategory)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            invoice_id,
            name,
            display,
            product.get('quantity', 1),
            product.get('price', 0),
            btw,
            category,
            subcategory
        ))
    
    conn.commit()
    conn.close()


def get_all_product_names() -> List[str]:
    """All original product names (one entry per purchase), e.g. to correct OCR'd names."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT original_name FROM products")
    names = [row['original_name'] for row in cursor.fetchall()]
    conn.close()
    return names


def get_all_invoices() -> List[Dict]:
    """Get all invoices with product counts"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT i.*, 
               COUNT(p.id) as product_count,
               SUM(CASE WHEN p.is_validated = 1 THEN 1 ELSE 0 END) as validated_count
        FROM invoices i
        LEFT JOIN products p ON i.id = p.invoice_id
        GROUP BY i.id
        ORDER BY i.invoice_date DESC
    """)
    
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_invoice_with_products(invoice_id: int) -> Optional[Dict]:
    """Get invoice with all its products"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT * FROM invoices WHERE id = ?", (invoice_id,))
    invoice = cursor.fetchone()
    
    if not invoice:
        conn.close()
        return None
    
    result = dict(invoice)
    
    cursor.execute("""
        SELECT p.*, 
               COALESCE(p.user_category, p.auto_category) as effective_category,
               COALESCE(p.user_subcategory, p.auto_subcategory) as subcategory
        FROM products p
        WHERE p.invoice_id = ?
        ORDER BY p.id
    """, (invoice_id,))
    
    result['products'] = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return result


def get_all_products() -> List[Dict]:
    """Get all products with their invoices"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT p.*, i.invoice_date, i.invoice_number,
               COALESCE(p.user_category, p.auto_category) as effective_category,
               COALESCE(p.user_subcategory, p.auto_subcategory) as subcategory
        FROM products p
        JOIN invoices i ON p.invoice_id = i.id
        ORDER BY i.invoice_date DESC, p.id
    """)
    
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def update_product_category(product_id: int, category: str, apply_to_similar: bool = False):
    """Update a product's category"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get the product name first
    cursor.execute("SELECT original_name, display_name FROM products WHERE id = ?", (product_id,))
    product = cursor.fetchone()
    
    if not product:
        conn.close()
        return {"success": False, "error": "Product not found", "updated_count": 0}
    
    product_name = product['original_name']
    updated_count = 0
    
    # If apply to similar, update all products with the same name
    if apply_to_similar:
        # Update alle producten met dezelfde naam
        cursor.execute("""
            UPDATE products SET user_category = ?, user_subcategory = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE original_name = ?
        """, (category, product_name))
        updated_count = cursor.rowcount
        
        # Also save as an override rule for future imports
        cursor.execute("SELECT id FROM categories WHERE name = ?", (category,))
        cat_row = cursor.fetchone()
        if cat_row:
            upsert_product_override(
                product_name_pattern=product_name,
                category_id=cat_row['id'],
                subcategory_id=None,
                match_type="exact",
                source="user",
                confidence=1.0,
                conn=conn
            )
    else:
        # Update alleen dit specifieke product
        cursor.execute("""
            UPDATE products SET user_category = ?, user_subcategory = NULL, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (category, product_id))
        updated_count = cursor.rowcount
    
    conn.commit()
    conn.close()
    return {"success": True, "updated_count": updated_count, "product_name": product_name}


def validate_product(product_id: int, is_valid: bool, notes: str = None):
    """Mark a product as validated or add validation notes"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE products 
        SET is_validated = ?, validation_notes = ?
        WHERE id = ?
    """, (1 if is_valid else 0, notes, product_id))
    
    conn.commit()
    conn.close()


def add_validation_feedback(invoice_id: int = None, product_id: int = None, 
                           feedback_type: str = None, original_value: str = None,
                           corrected_value: str = None, notes: str = None):
    """Add validation feedback for review"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO validation_feedback 
        (invoice_id, product_id, feedback_type, original_value, corrected_value, notes)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (invoice_id, product_id, feedback_type, original_value, corrected_value, notes))
    
    conn.commit()
    conn.close()


def get_all_categories() -> List[Dict]:
    """Get all categories"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT c.*, 
               (SELECT COUNT(*) FROM products p 
                WHERE COALESCE(p.user_category, p.auto_category) = c.name) as product_count
        FROM categories c
        ORDER BY c.sort_order, c.name
    """)
    
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def add_category(name: str, color: str = "#9E9E9E", icon: str = "📁") -> int:
    """Add a new category"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO categories (name, color, icon, is_system, sort_order)
        VALUES (?, ?, ?, 0, (SELECT COALESCE(MAX(sort_order), 0) + 1 FROM categories))
    """, (name, color, icon))
    
    category_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return category_id


def delete_category(category_id: int) -> Dict:
    """Delete a category by ID. Returns info about the deleted category."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM categories WHERE id = ?", (category_id,))
    category = cursor.fetchone()

    if not category:
        conn.close()
        return {"success": False, "error": "Category not found"}

    cursor.execute("""
        SELECT COUNT(*) as count 
        FROM products 
        WHERE user_category = ? OR auto_category = ?
    """, (category['name'], category['name']))

    product_count = cursor.fetchone()['count']

    cursor.execute("DELETE FROM categories WHERE id = ?", (category_id,))
    conn.commit()
    conn.close()

    return {"success": True, "name": category['name'], "products_affected": product_count}


def get_products_by_category(category: str) -> List[Dict]:
    """Get all products in a specific category"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT p.*, i.invoice_date, i.invoice_number,
               COALESCE(p.user_category, p.auto_category) as effective_category
        FROM products p
        JOIN invoices i ON p.invoice_id = i.id
        WHERE COALESCE(p.user_category, p.auto_category) = ?
        ORDER BY p.original_name, i.invoice_date DESC
    """, (category,))
    
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_analysis_data(year: str = None) -> Dict:
    """Get aggregated analysis data, optionally filtered by year"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Build year filter
    year_filter = ""
    year_params = []
    if year:
        year_filter = "WHERE strftime('%Y', i.invoice_date) = ?"
        year_params = [year]
    
    # Summary
    cursor.execute(f"""
        SELECT 
            COUNT(DISTINCT i.id) as total_invoices,
            SUM(i.total_amount) as total_spent,
            SUM(i.total_savings) as total_savings,
            MIN(i.invoice_date) as first_date,
            MAX(i.invoice_date) as last_date
        FROM invoices i
        {year_filter}
    """, year_params)
    summary = dict(cursor.fetchone())
    
    # Category totals
    if year:
        cursor.execute("""
            SELECT COALESCE(p.user_category, p.auto_category) as category,
                   SUM(p.price) as total,
                   COUNT(*) as count
            FROM products p
            JOIN invoices i ON p.invoice_id = i.id
            WHERE strftime('%Y', i.invoice_date) = ?
            GROUP BY category
            ORDER BY total DESC
        """, [year])
    else:
        cursor.execute("""
            SELECT COALESCE(p.user_category, p.auto_category) as category,
                   SUM(p.price) as total,
                   COUNT(*) as count
            FROM products p
            GROUP BY category
            ORDER BY total DESC
        """)
    category_totals = {row['category']: row['total'] for row in cursor.fetchall()}
    
    # Monthly totals (filtered by year if specified)
    if year:
        cursor.execute("""
            SELECT strftime('%Y-%m', i.invoice_date) as month,
                   SUM(p.price) as total
            FROM products p
            JOIN invoices i ON p.invoice_id = i.id
            WHERE strftime('%Y', i.invoice_date) = ?
            GROUP BY month
            ORDER BY month
        """, [year])
    else:
        cursor.execute("""
            SELECT strftime('%Y-%m', i.invoice_date) as month,
                   SUM(p.price) as total
            FROM products p
            JOIN invoices i ON p.invoice_id = i.id
            GROUP BY month
            ORDER BY month
        """)
    monthly_totals = {row['month']: row['total'] for row in cursor.fetchall()}
    
    # Yearly totals (always show all years for navigation)
    cursor.execute("""
        SELECT strftime('%Y', i.invoice_date) as year,
               SUM(p.price) as total
        FROM products p
        JOIN invoices i ON p.invoice_id = i.id
        GROUP BY year
        ORDER BY year
    """)
    yearly_totals = {row['year']: row['total'] for row in cursor.fetchall()}
    
    # Yearly category breakdown
    cursor.execute("""
        SELECT strftime('%Y', i.invoice_date) as year,
               COALESCE(p.user_category, p.auto_category) as category,
               SUM(p.price) as total
        FROM products p
        JOIN invoices i ON p.invoice_id = i.id
        GROUP BY year, category
        ORDER BY year, total DESC
    """)
    yearly_category = {}
    for row in cursor.fetchall():
        if row['year'] not in yearly_category:
            yearly_category[row['year']] = {}
        yearly_category[row['year']][row['category']] = row['total']
    
    # Top products
    if year:
        cursor.execute("""
            SELECT p.original_name as name,
                   COALESCE(p.user_category, p.auto_category) as category,
                   COALESCE(p.user_subcategory, p.auto_subcategory) as subcategory,
                   SUM(p.quantity) as count,
                   SUM(p.price) as total
            FROM products p
            JOIN invoices i ON p.invoice_id = i.id
            WHERE strftime('%Y', i.invoice_date) = ?
            GROUP BY p.original_name
            ORDER BY total DESC
            LIMIT 20
        """, [year])
    else:
        cursor.execute("""
            SELECT p.original_name as name,
                   COALESCE(p.user_category, p.auto_category) as category,
                   COALESCE(p.user_subcategory, p.auto_subcategory) as subcategory,
                   SUM(p.quantity) as count,
                   SUM(p.price) as total
            FROM products p
            GROUP BY p.original_name
            ORDER BY total DESC
            LIMIT 20
        """)
    top_products = [dict(row) for row in cursor.fetchall()]
    
    # Subcategory totals per category
    if year:
        cursor.execute("""
            SELECT 
                COALESCE(p.user_category, p.auto_category) as category,
                COALESCE(p.user_subcategory, p.auto_subcategory) as subcategory,
                SUM(p.price) as total,
                COUNT(*) as count
            FROM products p
            JOIN invoices i ON p.invoice_id = i.id
            WHERE COALESCE(p.user_subcategory, p.auto_subcategory) IS NOT NULL
              AND strftime('%Y', i.invoice_date) = ?
            GROUP BY category, subcategory
            ORDER BY category, total DESC
        """, [year])
    else:
        cursor.execute("""
            SELECT 
                COALESCE(p.user_category, p.auto_category) as category,
                COALESCE(p.user_subcategory, p.auto_subcategory) as subcategory,
                SUM(p.price) as total,
                COUNT(*) as count
            FROM products p
            WHERE COALESCE(p.user_subcategory, p.auto_subcategory) IS NOT NULL
            GROUP BY category, subcategory
            ORDER BY category, total DESC
        """)
    subcategory_totals = {}
    for row in cursor.fetchall():
        cat = row['category']
        if cat not in subcategory_totals:
            subcategory_totals[cat] = {}
        subcategory_totals[cat][row['subcategory']] = {
            'total': round(row['total'], 2),
            'count': row['count']
        }
    
    conn.close()
    
    # Calculate derived metrics
    num_months = len(monthly_totals) if monthly_totals else 1
    avg_monthly = sum(monthly_totals.values()) / num_months if monthly_totals else 0
    
    # 30-year projections
    thirty_year = {}
    for product in top_products[:10]:
        monthly_avg = product['total'] / num_months
        thirty_year[product['name']] = {
            'monthly': round(monthly_avg, 2),
            'yearly': round(monthly_avg * 12, 2),
            'thirty_years': round(monthly_avg * 12 * 30, 2)
        }
    
    # Savings options
    savings_options = []
    if category_totals.get("Snacks & Zoetwaren", 0) > 0:
        snack_total = category_totals["Snacks & Zoetwaren"]
        savings_options.append({
            "category": "Snacks & Zoetwaren",
            "current_spend": round(snack_total, 2),
            "potential_savings": round(snack_total * 0.5, 2),
            "tip": "Halveer snacks en snoep voor gezondere keuzes én besparing",
            "thirty_year_impact": round(snack_total * 0.5 / num_months * 12 * 30, 2)
        })
    
    if category_totals.get("Vlees & Vis", 0) > 0:
        meat_total = category_totals["Vlees & Vis"]
        savings_options.append({
            "category": "Vlees & Vis",
            "current_spend": round(meat_total, 2),
            "potential_savings": round(meat_total * 0.25, 2),
            "tip": "1 dag per week vegetarisch eten bespaart ~25% op vlees",
            "thirty_year_impact": round(meat_total * 0.25 / num_months * 12 * 30, 2)
        })
    
    if category_totals.get("Dranken", 0) > 0:
        drink_total = category_totals["Dranken"]
        savings_options.append({
            "category": "Dranken",
            "current_spend": round(drink_total, 2),
            "potential_savings": round(drink_total * 0.3, 2),
            "tip": "Drink meer kraanwater in plaats van fleswater/frisdrank",
            "thirty_year_impact": round(drink_total * 0.3 / num_months * 12 * 30, 2)
        })
    
    if category_totals.get("Persoonlijke Verzorging", 0) > 0:
        care_total = category_totals["Persoonlijke Verzorging"]
        savings_options.append({
            "category": "Persoonlijke Verzorging",
            "current_spend": round(care_total, 2),
            "potential_savings": round(care_total * 0.4, 2),
            "tip": "Kies huismerk producten of wacht op aanbiedingen",
            "thirty_year_impact": round(care_total * 0.4 / num_months * 12 * 30, 2)
        })
    
    return {
        "summary": {
            "total_invoices": summary['total_invoices'] or 0,
            "total_spent": round(summary['total_spent'] or 0, 2),
            "total_savings": round(summary['total_savings'] or 0, 2),
            "avg_per_invoice": round((summary['total_spent'] or 0) / max(summary['total_invoices'] or 1, 1), 2),
            "avg_monthly": round(avg_monthly, 2),
            "date_range": {
                "start": summary['first_date'],
                "end": summary['last_date']
            }
        },
        "category_totals": category_totals,
        "subcategory_totals": subcategory_totals,
        "monthly_totals": monthly_totals,
        "yearly_totals": yearly_totals,
        "yearly_category": yearly_category,
        "top_products": top_products,
        "thirty_year_projection": thirty_year,
        "savings_options": savings_options,
        "selected_year": year,
        "available_years": sorted(yearly_totals.keys(), reverse=True) if yearly_totals else []
    }


def get_validation_stats() -> Dict:
    """Get validation statistics"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total_products,
            SUM(CASE WHEN is_validated = 1 THEN 1 ELSE 0 END) as validated,
            SUM(CASE WHEN is_validated = 0 THEN 1 ELSE 0 END) as unvalidated,
            SUM(CASE WHEN user_category IS NOT NULL THEN 1 ELSE 0 END) as recategorized
        FROM products
    """)
    
    result = dict(cursor.fetchone())
    
    cursor.execute("SELECT COUNT(*) as pending FROM validation_feedback WHERE status = 'pending'")
    result['pending_feedback'] = cursor.fetchone()['pending']
    
    conn.close()
    return result


def migrate_normalize_product_names():
    """
    Migratie: Normaliseer productnamen (BONUS strippen) en update auto-categorieën.
    Dit fixt de dubbele producten probleem.
    """
    from parser import normalize_product_name, categorize_product
    
    conn = get_connection()
    cursor = conn.cursor()
    
    print("Starting migration: normalizing product names...")
    
    # Haal alle producten op
    cursor.execute("SELECT id, original_name, display_name, auto_category FROM products")
    products = cursor.fetchall()
    
    updated_count = 0
    recategorized_count = 0
    
    for product in products:
        product_id = product['id']
        original_name = product['original_name']
        current_display = product['display_name']
        current_auto_cat = product['auto_category']
        
        # Normaliseer de naam
        normalized_name = normalize_product_name(original_name)
        
        # Herbereken categorie met nieuwe keywords
        new_category = categorize_product(original_name)
        
        # Update als nodig
        needs_update = False
        updates = {}
        
        if normalized_name != current_display:
            updates['display_name'] = normalized_name
            needs_update = True
            
        if new_category != current_auto_cat:
            updates['auto_category'] = new_category
            recategorized_count += 1
            needs_update = True
        
        if needs_update:
            # Update original_name to normalized version for consistency
            cursor.execute("""
                UPDATE products 
                SET original_name = ?, display_name = ?, auto_category = ?
                WHERE id = ?
            """, (normalized_name, normalized_name, new_category, product_id))
            updated_count += 1
    
    conn.commit()
    conn.close()
    
    print(f"Migration complete:")
    print(f"  - {updated_count} products updated (names normalized)")
    print(f"  - {recategorized_count} products recategorized")
    
    return {
        "updated": updated_count,
        "recategorized": recategorized_count
    }


def get_overig_products_summary():
    """Haal een overzicht van alle Overig producten voor analyse"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT original_name, 
               COUNT(*) as count, 
               SUM(price) as total,
               SUM(quantity) as total_quantity
        FROM products 
        WHERE COALESCE(user_category, auto_category) = 'Overig'
        GROUP BY original_name 
        ORDER BY total DESC
    """)
    
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_product_details(product_name: str) -> Dict:
    """Haal gedetailleerde informatie over een product op"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Basis info
    cursor.execute("""
        SELECT
            original_name,
            COALESCE(user_category, auto_category) as category,
            COUNT(*) as purchase_count,
            SUM(quantity) as total_quantity,
            SUM(price) as total_spent,
            AVG(CASE WHEN price > 0 AND quantity > 0 THEN price / quantity ELSE NULL END) as avg_unit_price,
            MIN(CASE WHEN price > 0 AND quantity > 0 THEN price / quantity ELSE NULL END) as min_unit_price,
            MAX(CASE WHEN price > 0 AND quantity > 0 THEN price / quantity ELSE NULL END) as max_unit_price
        FROM products
        WHERE original_name = ?
        GROUP BY original_name
    """, (product_name,))
    
    row = cursor.fetchone()
    if not row:
        conn.close()
        return None
    
    result = dict(row)
    
    # Prijshistorie per aankoop
    cursor.execute("""
        SELECT 
            p.price,
            p.quantity,
            p.price / NULLIF(p.quantity, 0) as unit_price,
            i.invoice_date,
            i.invoice_number,
            i.id as invoice_id
        FROM products p
        JOIN invoices i ON p.invoice_id = i.id
        WHERE p.original_name = ?
        ORDER BY i.invoice_date ASC
    """, (product_name,))
    
    result['price_history'] = [dict(r) for r in cursor.fetchall()]
    
    # Maandelijkse totalen
    cursor.execute("""
        SELECT 
            strftime('%Y-%m', i.invoice_date) as month,
            SUM(p.price) as total,
            SUM(p.quantity) as quantity
        FROM products p
        JOIN invoices i ON p.invoice_id = i.id
        WHERE p.original_name = ?
        GROUP BY month
        ORDER BY month
    """, (product_name,))
    
    result['monthly_totals'] = {r['month']: {'total': r['total'], 'quantity': r['quantity']} for r in cursor.fetchall()}
    
    # 30-jaar projectie
    num_months = len(result['monthly_totals']) if result['monthly_totals'] else 1
    monthly_avg = result['total_spent'] / num_months
    result['projection'] = {
        'monthly': round(monthly_avg, 2),
        'yearly': round(monthly_avg * 12, 2),
        'thirty_years': round(monthly_avg * 12 * 30, 2)
    }
    
    # Prijstrend
    if result['price_history']:
        prices = [h['unit_price'] for h in result['price_history'] if h['unit_price']]
        if len(prices) >= 2:
            first_half = sum(prices[:len(prices)//2]) / (len(prices)//2)
            second_half = sum(prices[len(prices)//2:]) / (len(prices) - len(prices)//2)
            trend_pct = ((second_half - first_half) / first_half * 100) if first_half > 0 else 0
            result['price_trend'] = {
                'direction': 'up' if trend_pct > 2 else ('down' if trend_pct < -2 else 'stable'),
                'percentage': round(trend_pct, 1)
            }
        else:
            result['price_trend'] = {'direction': 'stable', 'percentage': 0}
    
    conn.close()
    return result


def get_all_unique_products() -> List[Dict]:
    """Haal alle unieke producten op voor zoeken"""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            original_name as name,
            COALESCE(user_category, auto_category) as category,
            COALESCE(user_subcategory, auto_subcategory) as subcategory,
            COUNT(*) as purchase_count,
            SUM(quantity) as total_quantity,
            SUM(price) as total_spent,
            AVG(CASE WHEN price > 0 AND quantity > 0 THEN price / quantity ELSE NULL END) as avg_unit_price
        FROM products
        GROUP BY original_name
        ORDER BY total_spent DESC
    """)
    
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_smart_savings_insights() -> Dict:
    """Genereer slimme besparingsinzichten"""
    conn = get_connection()
    cursor = conn.cursor()
    
    insights = {
        "price_increases": [],
        "high_frequency_expensive": [],
        "impulse_buys": [],
        "premium_vs_huismerk": [],
        "bulk_opportunities": []
    }
    
    # 1. Producten met prijsstijging (vergelijk eerste en laatste aankoop)
    cursor.execute("""
        WITH first_last AS (
            SELECT 
                p.original_name,
                FIRST_VALUE(p.price / NULLIF(p.quantity, 0)) OVER (
                    PARTITION BY p.original_name ORDER BY i.invoice_date ASC
                ) as first_price,
                FIRST_VALUE(p.price / NULLIF(p.quantity, 0)) OVER (
                    PARTITION BY p.original_name ORDER BY i.invoice_date DESC
                ) as last_price,
                COUNT(*) OVER (PARTITION BY p.original_name) as purchase_count
            FROM products p
            JOIN invoices i ON p.invoice_id = i.id
        )
        SELECT DISTINCT
            original_name,
            first_price,
            last_price,
            purchase_count,
            ROUND((last_price - first_price) / NULLIF(first_price, 0) * 100, 1) as price_change_pct
        FROM first_last
        WHERE purchase_count >= 5
          AND first_price > 0
          AND last_price > first_price * 1.05
        ORDER BY price_change_pct DESC
        LIMIT 10
    """)
    
    for row in cursor.fetchall():
        insights["price_increases"].append({
            "name": row['original_name'],
            "first_price": round(row['first_price'], 2),
            "last_price": round(row['last_price'], 2),
            "change_pct": row['price_change_pct'],
            "purchase_count": row['purchase_count']
        })
    
    # 2. Dure producten die je vaak koopt (potentie voor alternatieven)
    cursor.execute("""
        SELECT 
            original_name,
            COALESCE(user_category, auto_category) as category,
            SUM(quantity) as total_quantity,
            SUM(price) as total_spent,
            AVG(price / NULLIF(quantity, 0)) as avg_price
        FROM products
        GROUP BY original_name
        HAVING total_quantity >= 10 AND avg_price > 3
        ORDER BY total_spent DESC
        LIMIT 10
    """)
    
    for row in cursor.fetchall():
        insights["high_frequency_expensive"].append({
            "name": row['original_name'],
            "category": row['category'],
            "total_quantity": row['total_quantity'],
            "total_spent": round(row['total_spent'], 2),
            "avg_price": round(row['avg_price'], 2)
        })
    
    # 3. Eenmalige aankopen (mogelijke impulsaankopen)
    cursor.execute("""
        SELECT 
            original_name,
            COALESCE(user_category, auto_category) as category,
            SUM(price) as total_spent
        FROM products
        GROUP BY original_name
        HAVING COUNT(*) = 1 AND total_spent > 5
        ORDER BY total_spent DESC
        LIMIT 10
    """)
    
    for row in cursor.fetchall():
        insights["impulse_buys"].append({
            "name": row['original_name'],
            "category": row['category'],
            "price": round(row['total_spent'], 2)
        })
    
    # 4. A-merk vs huismerk analyse per categorie
    cursor.execute("""
        SELECT 
            COALESCE(user_category, auto_category) as category,
            SUM(CASE WHEN original_name LIKE 'AH %' OR original_name LIKE 'AH Terra%' THEN price ELSE 0 END) as huismerk_spent,
            SUM(CASE WHEN NOT (original_name LIKE 'AH %' OR original_name LIKE 'AH Terra%') THEN price ELSE 0 END) as amerk_spent,
            SUM(price) as total_spent
        FROM products
        GROUP BY category
        HAVING total_spent > 50
        ORDER BY amerk_spent DESC
    """)
    
    for row in cursor.fetchall():
        if row['amerk_spent'] > 0:
            insights["premium_vs_huismerk"].append({
                "category": row['category'],
                "huismerk_spent": round(row['huismerk_spent'], 2),
                "amerk_spent": round(row['amerk_spent'], 2),
                "amerk_percentage": round(row['amerk_spent'] / row['total_spent'] * 100, 1) if row['total_spent'] > 0 else 0
            })
    
    # 5. Bulk-aankoopkansen (producten die je vaak koopt maar in kleine hoeveelheden)
    cursor.execute("""
        SELECT 
            original_name,
            COUNT(*) as purchase_count,
            SUM(quantity) as total_quantity,
            SUM(price) as total_spent,
            AVG(quantity) as avg_quantity_per_purchase
        FROM products
        GROUP BY original_name
        HAVING purchase_count >= 8 AND avg_quantity_per_purchase < 2
        ORDER BY purchase_count DESC
        LIMIT 10
    """)
    
    for row in cursor.fetchall():
        insights["bulk_opportunities"].append({
            "name": row['original_name'],
            "purchase_count": row['purchase_count'],
            "total_spent": round(row['total_spent'], 2),
            "avg_quantity": round(row['avg_quantity_per_purchase'], 1)
        })
    
    conn.close()
    return insights


def bulk_update_category_by_name(product_name: str, new_category: str):
    """Update categorie voor alle producten met deze naam"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE products 
        SET user_category = ?, user_subcategory = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE original_name = ?
    """, (new_category, product_name))
    
    # Persist override for future imports
    cursor.execute("SELECT id FROM categories WHERE name = ?", (new_category,))
    cat_row = cursor.fetchone()
    if cat_row:
        upsert_product_override(
            product_name_pattern=product_name,
            category_id=cat_row['id'],
            subcategory_id=None,
            match_type="exact",
            source="user",
            confidence=1.0,
            conn=conn
        )
    
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    
    return affected


def bulk_update_category_by_names(product_names: List[str], new_category: str) -> int:
    """Update categorie voor alle producten met deze namen (bulk)."""
    if not product_names:
        return 0
    
    conn = get_connection()
    cursor = conn.cursor()
    
    placeholders = ",".join(["?"] * len(product_names))
    cursor.execute(f"""
        UPDATE products
        SET user_category = ?, user_subcategory = NULL, updated_at = CURRENT_TIMESTAMP
        WHERE original_name IN ({placeholders})
    """, (new_category, *product_names))
    affected = cursor.rowcount
    
    # Persist overrides for future imports
    cursor.execute("SELECT id FROM categories WHERE name = ?", (new_category,))
    cat_row = cursor.fetchone()
    if cat_row:
        for name in product_names:
            upsert_product_override(
                product_name_pattern=name,
                category_id=cat_row['id'],
                subcategory_id=None,
                match_type="exact",
                source="user",
                confidence=1.0,
                conn=conn
            )
    
    conn.commit()
    conn.close()
    return affected


def bulk_update_subcategory_by_names(
    product_names: List[str],
    subcategory: str,
    category_name: Optional[str] = None
) -> int:
    """Update subcategorie voor alle producten met deze namen (bulk)."""
    if not product_names:
        return 0
    
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT s.id as subcategory_id, c.id as category_id, c.name as category_name
        FROM subcategories s
        JOIN categories c ON s.category_id = c.id
        WHERE s.name = ?
    """, (subcategory,))
    subcat_row = cursor.fetchone()
    
    if not subcat_row:
        conn.close()
        raise ValueError("Subcategorie niet gevonden")
    
    if category_name and subcat_row['category_name'] != category_name:
        conn.close()
        raise ValueError("Subcategorie hoort niet bij de gekozen categorie")
    
    placeholders = ",".join(["?"] * len(product_names))
    cursor.execute(f"""
        UPDATE products
        SET user_subcategory = ?, updated_at = CURRENT_TIMESTAMP
        WHERE original_name IN ({placeholders})
          AND COALESCE(user_category, auto_category) = ?
    """, (subcategory, *product_names, subcat_row['category_name']))
    affected = cursor.rowcount
    
    # Persist overrides for future imports
    for name in product_names:
        upsert_product_override(
            product_name_pattern=name,
            category_id=subcat_row['category_id'],
            subcategory_id=subcat_row['subcategory_id'],
            match_type="exact",
            source="user",
            confidence=1.0,
            conn=conn
        )
    
    conn.commit()
    conn.close()
    return affected


def save_llm_suggestions(suggestions: List[Dict]) -> int:
    """Upsert LLM suggestions into the review queue."""
    if not suggestions:
        return 0
    
    conn = get_connection()
    cursor = conn.cursor()
    
    rows = []
    for s in suggestions:
        rows.append((
            s['product_name'],
            s['category_id'],
            s.get('subcategory_id'),
            s.get('confidence', 0.0),
            s.get('model'),
            s.get('prompt_version')
        ))
    
    cursor.executemany("""
        INSERT INTO llm_suggestions (
            product_name, category_id, subcategory_id, confidence, model, prompt_version
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(product_name) DO UPDATE SET
            category_id = excluded.category_id,
            subcategory_id = excluded.subcategory_id,
            confidence = excluded.confidence,
            model = excluded.model,
            prompt_version = excluded.prompt_version,
            status = 'pending',
            updated_at = CURRENT_TIMESTAMP
    """, rows)
    
    conn.commit()
    conn.close()
    return len(suggestions)


def get_llm_suggestions(status: str = "pending", limit: int = 100) -> List[Dict]:
    """Get LLM suggestions with product stats for review."""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT
            ls.id,
            ls.product_name,
            ls.confidence,
            ls.model,
            ls.prompt_version,
            ls.status,
            ls.created_at,
            c.name as category_name,
            s.name as subcategory_name,
            COUNT(p.id) as purchase_count,
            SUM(p.price) as total_spent
        FROM llm_suggestions ls
        JOIN categories c ON ls.category_id = c.id
        LEFT JOIN subcategories s ON ls.subcategory_id = s.id
        LEFT JOIN products p ON p.original_name = ls.product_name
        WHERE ls.status = ?
        GROUP BY ls.id
        ORDER BY COALESCE(total_spent, 0) DESC, purchase_count DESC
        LIMIT ?
    """, (status, limit))
    
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def update_llm_suggestions_bulk(overrides: List[Dict]) -> Dict:
    """Update suggestion categories/subcategories by name before acceptance."""
    if not overrides:
        return {"updated": 0}
    
    conn = get_connection()
    cursor = conn.cursor()
    updated = 0
    
    for item in overrides:
        suggestion_id = item.get("id")
        category_name = item.get("category")
        subcategory_name = item.get("subcategory")
        
        if not suggestion_id or not category_name:
            continue
        
        cursor.execute("SELECT id FROM categories WHERE name = ?", (category_name,))
        cat_row = cursor.fetchone()
        if not cat_row:
            continue
        
        cat_id = cat_row['id']
        subcat_id = None
        
        if subcategory_name:
            cursor.execute("""
                SELECT id FROM subcategories WHERE name = ? AND category_id = ?
            """, (subcategory_name, cat_id))
            subcat_row = cursor.fetchone()
            if subcat_row:
                subcat_id = subcat_row['id']
        
        cursor.execute("""
            UPDATE llm_suggestions
            SET category_id = ?, subcategory_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (cat_id, subcat_id, suggestion_id))
        
        if cursor.rowcount > 0:
            updated += 1
    
    conn.commit()
    conn.close()
    return {"updated": updated}


def apply_llm_suggestions(suggestion_ids: List[int], decided_by: str = "user") -> Dict:
    """Apply accepted LLM suggestions to products and overrides."""
    if not suggestion_ids:
        return {"applied": 0}
    
    conn = get_connection()
    cursor = conn.cursor()
    
    placeholders = ",".join(["?"] * len(suggestion_ids))
    cursor.execute(f"""
        SELECT
            ls.id,
            ls.product_name,
            ls.category_id,
            ls.subcategory_id,
            ls.confidence,
            c.name as category_name,
            s.name as subcategory_name
        FROM llm_suggestions ls
        JOIN categories c ON ls.category_id = c.id
        LEFT JOIN subcategories s ON ls.subcategory_id = s.id
        WHERE ls.id IN ({placeholders}) AND ls.status = 'pending'
    """, suggestion_ids)
    
    suggestions = cursor.fetchall()
    if not suggestions:
        conn.close()
        return {"applied": 0}
    
    # Update products
    product_rows = [
        (row['category_name'], row['subcategory_name'], row['product_name'])
        for row in suggestions
    ]
    cursor.executemany("""
        UPDATE products
        SET user_category = ?, user_subcategory = ?, updated_at = CURRENT_TIMESTAMP
        WHERE original_name = ?
    """, product_rows)
    
    # Update overrides
    for row in suggestions:
        upsert_product_override(
            product_name_pattern=row['product_name'],
            category_id=row['category_id'],
            subcategory_id=row['subcategory_id'],
            match_type="exact",
            source="user",
            confidence=row['confidence'],
            conn=conn
        )
    
    # Mark suggestions as accepted
    cursor.execute(f"""
        UPDATE llm_suggestions
        SET status = 'accepted',
            decided_at = CURRENT_TIMESTAMP,
            decided_by = ?
        WHERE id IN ({placeholders})
    """, (decided_by, *suggestion_ids))
    
    conn.commit()
    conn.close()
    return {"applied": len(suggestions)}


def reject_llm_suggestions(suggestion_ids: List[int], decided_by: str = "user") -> Dict:
    """Reject LLM suggestions."""
    if not suggestion_ids:
        return {"rejected": 0}
    
    conn = get_connection()
    cursor = conn.cursor()
    
    placeholders = ",".join(["?"] * len(suggestion_ids))
    cursor.execute(f"""
        UPDATE llm_suggestions
        SET status = 'rejected',
            decided_at = CURRENT_TIMESTAMP,
            decided_by = ?
        WHERE id IN ({placeholders}) AND status = 'pending'
    """, (decided_by, *suggestion_ids))
    
    rejected = cursor.rowcount
    conn.commit()
    conn.close()
    return {"rejected": rejected}


def get_llm_suggestions_count(status: str = "pending") -> int:
    """Get count of LLM suggestions by status."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) as count FROM llm_suggestions WHERE status = ?", (status,))
    count = cursor.fetchone()['count']
    conn.close()
    return count


def reparse_all_invoices():
    """
    Herparse alle facturen met de verbeterde parser.
    Verwijdert bestaande producten en importeert ze opnieuw.
    """
    from parser import extract_products_from_text, categorize_product, normalize_product_name
    
    conn = get_connection()
    cursor = conn.cursor()
    
    print("Herparsing all invoices with improved parser...")
    
    # Alleen AH online facturen: raw_text van andere bronnen (PLUS-foto's, API) past niet bij deze parser
    cursor.execute("SELECT id, raw_text, filename FROM invoices WHERE COALESCE(source_type, 'ah_online') = 'ah_online'")
    invoices = cursor.fetchall()
    
    total_old = 0
    total_new = 0
    
    for invoice in invoices:
        invoice_id = invoice['id']
        raw_text = invoice['raw_text']
        filename = invoice['filename']
        
        # Tel oude producten
        cursor.execute("SELECT COUNT(*) as count FROM products WHERE invoice_id = ?", (invoice_id,))
        old_count = cursor.fetchone()['count']
        total_old += old_count
        
        # Verwijder oude producten
        cursor.execute("DELETE FROM products WHERE invoice_id = ?", (invoice_id,))
        
        # Parse opnieuw met nieuwe parser
        products = extract_products_from_text(raw_text)
        
        # Voeg nieuwe producten toe
        for product in products:
            cursor.execute("""
                INSERT INTO products (invoice_id, original_name, display_name, quantity, 
                                     price, btw_percentage, auto_category)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                invoice_id,
                product['name'],
                product['name'],
                product.get('quantity', 1),
                product.get('price', 0),
                product.get('btw', ''),
                product.get('category', 'Overig')
            ))
        
        total_new += len(products)
        
        if old_count != len(products):
            print(f"  {filename}: {old_count} -> {len(products)} producten")
    
    conn.commit()
    conn.close()
    
    print(f"\nReparse complete:")
    print(f"  - Oude producten: {total_old}")
    print(f"  - Nieuwe producten: {total_new}")
    print(f"  - Verschil: +{total_new - total_old}")
    
    return {"old_count": total_old, "new_count": total_new}


def get_all_subcategories() -> List[Dict]:
    """Haal alle subcategorieën op met hun hoofdcategorie"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT s.*, c.name as category_name, c.color as category_color,
               (SELECT COUNT(*) FROM products p 
                WHERE COALESCE(p.user_subcategory, p.auto_subcategory) = s.name
                  AND COALESCE(p.user_category, p.auto_category) = c.name) as product_count
        FROM subcategories s
        JOIN categories c ON s.category_id = c.id
        ORDER BY c.sort_order, s.sort_order
    """)
    
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def get_subcategories_for_category(category_name: str) -> List[Dict]:
    """Haal subcategorieën op voor een specifieke categorie"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT s.*, 
               (SELECT COUNT(*) FROM products p 
                WHERE COALESCE(p.user_subcategory, p.auto_subcategory) = s.name
                  AND COALESCE(p.user_category, p.auto_category) = ?) as product_count,
               (SELECT SUM(p.price) FROM products p 
                WHERE COALESCE(p.user_subcategory, p.auto_subcategory) = s.name
                  AND COALESCE(p.user_category, p.auto_category) = ?) as total_spent
        FROM subcategories s
        JOIN categories c ON s.category_id = c.id
        WHERE c.name = ?
        ORDER BY s.sort_order
    """, (category_name, category_name, category_name))
    
    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def add_subcategory(category_name: str, subcategory_name: str, icon: str = "📁") -> int:
    """Voeg een nieuwe subcategorie toe"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Haal category id op
    cursor.execute("SELECT id FROM categories WHERE name = ?", (category_name,))
    cat_row = cursor.fetchone()
    
    if not cat_row:
        conn.close()
        raise ValueError(f"Categorie '{category_name}' niet gevonden")
    
    cursor.execute("""
        INSERT INTO subcategories (name, category_id, icon, sort_order)
        VALUES (?, ?, ?, (SELECT COALESCE(MAX(sort_order), 0) + 1 FROM subcategories WHERE category_id = ?))
    """, (subcategory_name, cat_row['id'], icon, cat_row['id']))
    
    subcat_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return subcat_id


def update_product_subcategory(product_id: int, subcategory: str, apply_to_similar: bool = False):
    """Update de subcategorie van een product"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Haal het product op
    cursor.execute("""
        SELECT original_name, COALESCE(user_category, auto_category) as effective_category
        FROM products WHERE id = ?
    """, (product_id,))
    product = cursor.fetchone()
    
    if not product:
        conn.close()
        return {"success": False, "error": "Product niet gevonden", "updated_count": 0}
    
    product_name = product['original_name']
    updated_count = 0
    
    # Validate subcategory exists and belongs to the product's category
    cursor.execute("""
        SELECT s.id as subcategory_id, c.id as category_id, c.name as category_name
        FROM subcategories s
        JOIN categories c ON s.category_id = c.id
        WHERE s.name = ?
    """, (subcategory,))
    subcat_row = cursor.fetchone()
    
    if not subcat_row:
        conn.close()
        return {"success": False, "error": "Subcategorie niet gevonden", "updated_count": 0}
    
    if subcat_row['category_name'] != product['effective_category']:
        conn.close()
        return {
            "success": False,
            "error": "Subcategorie hoort niet bij de gekozen categorie",
            "updated_count": 0
        }
    
    if apply_to_similar:
        cursor.execute("""
            UPDATE products 
            SET user_subcategory = ?, updated_at = CURRENT_TIMESTAMP
            WHERE original_name = ? AND COALESCE(user_category, auto_category) = ?
        """, (subcategory, product_name, subcat_row['category_name']))
        updated_count = cursor.rowcount
        
        # Persist as override for future imports
        upsert_product_override(
            product_name_pattern=product_name,
            category_id=subcat_row['category_id'],
            subcategory_id=subcat_row['subcategory_id'],
            match_type="exact",
            source="user",
            confidence=1.0,
            conn=conn
        )
    else:
        cursor.execute("""
            UPDATE products SET user_subcategory = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (subcategory, product_id))
        updated_count = cursor.rowcount
    
    conn.commit()
    conn.close()
    return {"success": True, "updated_count": updated_count, "product_name": product_name}


def get_subcategory_totals() -> Dict:
    """Haal totalen per subcategorie op"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            COALESCE(p.user_category, p.auto_category) as category,
            COALESCE(p.user_subcategory, p.auto_subcategory) as subcategory,
            SUM(p.price) as total,
            COUNT(*) as count
        FROM products p
        GROUP BY category, subcategory
        ORDER BY category, total DESC
    """)
    
    result = {}
    for row in cursor.fetchall():
        cat = row['category']
        subcat = row['subcategory'] or 'Geen subcategorie'
        if cat not in result:
            result[cat] = {}
        result[cat][subcat] = {
            'total': row['total'],
            'count': row['count']
        }
    
    conn.close()
    return result


def migrate_add_subcategories():
    """
    Migratie: Voeg subcategorie kolommen toe aan bestaande products tabel
    en initialiseer subcategorieën
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    print("Migratie: Subcategorieën toevoegen...")
    
    # Check of kolommen bestaan
    cursor.execute("PRAGMA table_info(products)")
    columns = [col['name'] for col in cursor.fetchall()]
    
    if 'auto_subcategory' not in columns:
        print("  - Kolom auto_subcategory toevoegen...")
        cursor.execute("ALTER TABLE products ADD COLUMN auto_subcategory TEXT")
    
    if 'user_subcategory' not in columns:
        print("  - Kolom user_subcategory toevoegen...")
        cursor.execute("ALTER TABLE products ADD COLUMN user_subcategory TEXT")
    
    # Check of subcategories tabel bestaat
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='subcategories'")
    if not cursor.fetchone():
        print("  - Subcategories tabel aanmaken...")
        cursor.execute("""
            CREATE TABLE subcategories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                category_id INTEGER NOT NULL,
                color TEXT,
                icon TEXT,
                sort_order INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE,
                UNIQUE(name, category_id)
            )
        """)
    
    conn.commit()
    conn.close()
    
    # Herinitialiseer database om subcategorieën toe te voegen
    print("  - Default subcategorieën toevoegen...")
    init_database()
    
    print("Migratie voltooid!")
    return {"success": True}


if __name__ == "__main__":
    init_database()
    print(f"Database initialized at {DATABASE_PATH}")
    
    # Run reparse to use improved parser
    print("\nReparsing all invoices with improved parser...")
    reparse_all_invoices()


def get_budget_candidates(reference_year: int, limit: int = 50) -> List[Dict]:
    """Get candidate products for budgeting based on reference year spending"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            p.original_name as product_name,
            COALESCE(p.user_category, p.auto_category) as category,
            SUM(p.quantity) as annual_quantity,
            SUM(p.price) as annual_cost,
            COUNT(DISTINCT i.id) as frequency
        FROM products p
        JOIN invoices i ON p.invoice_id = i.id
        WHERE strftime('%Y', i.invoice_date) = ?
        GROUP BY p.original_name
        ORDER BY annual_cost DESC
        LIMIT ?
    """, (str(reference_year), limit))
    
    candidates = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return candidates


def save_budget_goal(goal_data: Dict) -> int:
    """Save or update a budget goal"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        INSERT INTO budget_goals (
            product_name, reference_year, target_year,
            reference_quantity, reference_total_cost,
            target_quantity, moscow_label, status, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(product_name, target_year) DO UPDATE SET
            target_quantity = excluded.target_quantity,
            moscow_label = excluded.moscow_label,
            status = excluded.status,
            updated_at = CURRENT_TIMESTAMP
    """, (
        goal_data['product_name'],
        goal_data['reference_year'],
        goal_data['target_year'],
        goal_data.get('reference_quantity', 0),
        goal_data.get('reference_total_cost', 0),
        goal_data.get('target_quantity'),
        goal_data.get('moscow_label'),
        goal_data.get('status', 'active')
    ))
    
    goal_id = cursor.lastrowid
    if not goal_id:  # On update, lastrowid might be 0/None depending on driver
        cursor.execute("SELECT id FROM budget_goals WHERE product_name = ? AND target_year = ?", 
                      (goal_data['product_name'], goal_data['target_year']))
        row = cursor.fetchone()
        if row:
            goal_id = row['id']
            
    conn.commit()
    conn.close()
    return goal_id


def get_budget_goals(target_year: int) -> List[Dict]:
    """Get all budget goals for a target year"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT * FROM budget_goals 
        WHERE target_year = ? AND status = 'active'
        ORDER BY reference_total_cost DESC
    """, (target_year,))
    
    goals = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return goals

def get_budget_tradeoff(reference_year: int) -> Dict:
    """Get a pair of products for 'Sophie's Choice' trade-off"""
    import random
    
    candidates = get_budget_candidates(reference_year, limit=50)
    if len(candidates) < 2:
        return {"error": "Not enough products for tradeoff"}
        
    # Pick two distinct products, preferably weighted by cost
    # For now, simple random choice from top 50
    p1, p2 = random.sample(candidates, 2)
    
    return {
        "product_a": p1,
        "product_b": p2,
        "scenario": "Als je moest kiezen..."
    }

def get_budget_summary(target_year: int) -> Dict:
    """Calculate total budget impact including 30-year projection"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Get all active goals
    cursor.execute("""
        SELECT 
            product_name,
            reference_quantity,
            reference_total_cost,
            target_quantity,
            moscow_label
        FROM budget_goals
        WHERE target_year = ? AND status = 'active'
    """, (target_year,))
    
    goals = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    total_ref_cost = 0
    total_target_cost = 0
    total_items = len(goals)
    
    # Calculate totals
    for g in goals:
        if g['reference_quantity'] and g['reference_quantity'] > 0:
            unit_price = g['reference_total_cost'] / g['reference_quantity']
            target_quantity = g['target_quantity'] if g['target_quantity'] is not None else g['reference_quantity']
            
            projected_cost = target_quantity * unit_price
            
            total_ref_cost += g['reference_total_cost']
            total_target_cost += projected_cost
            
    annual_savings = total_ref_cost - total_target_cost
    
    # Wealth Projection: Future Value of an Annuity
    # Formula: PMT * ((1 + r)^n - 1) / r
    # r = 0.07 (7% return), n = 30 years
    r = 0.07
    n = 30
    compound_factor = ((1 + r) ** n - 1) / r  # approx 94.46
    wealth_30y = annual_savings * compound_factor
    
    return {
        "target_year": target_year,
        "total_items_budgeted": total_items,
        "annual_reference_cost": round(total_ref_cost, 2),
        "annual_projected_cost": round(total_target_cost, 2),
        "annual_savings": round(annual_savings, 2),
        "wealth_30y": round(wealth_30y, 2),
        "compound_factor": round(compound_factor, 2)
    }


def get_day_of_week_analysis(year: str = None) -> Dict:
    """Analyseer uitgaven per dag van de week"""
    conn = get_connection()
    cursor = conn.cursor()

    year_filter = f"AND strftime('%Y', i.invoice_date) = '{year}'" if year else ""

    cursor.execute(f"""
        SELECT
            CASE cast(strftime('%w', i.invoice_date) as integer)
                WHEN 0 THEN 'Zondag'
                WHEN 1 THEN 'Maandag'
                WHEN 2 THEN 'Dinsdag'
                WHEN 3 THEN 'Woensdag'
                WHEN 4 THEN 'Donderdag'
                WHEN 5 THEN 'Vrijdag'
                WHEN 6 THEN 'Zaterdag'
            END as day_name,
            cast(strftime('%w', i.invoice_date) as integer) as day_num,
            COUNT(DISTINCT i.id) as invoice_count,
            SUM(i.total_amount) as total_spent,
            AVG(i.total_amount) as avg_per_visit,
            COUNT(DISTINCT i.invoice_date) as unique_dates
        FROM invoices i
        WHERE i.total_amount > 0 {year_filter}
        GROUP BY day_name, day_num
        ORDER BY day_num
    """)

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()

    # Calculate insights
    if results:
        max_spending_day = max(results, key=lambda x: x['total_spent'])
        min_spending_day = min(results, key=lambda x: x['total_spent'])

        return {
            "by_day": results,
            "insights": {
                "max_spending_day": max_spending_day['day_name'],
                "max_spending_amount": round(max_spending_day['total_spent'], 2),
                "min_spending_day": min_spending_day['day_name'],
                "min_spending_amount": round(min_spending_day['total_spent'], 2)
            }
        }

    return {"by_day": [], "insights": {}}


def get_bonus_analysis(year: str = None) -> Dict:
    """Analyseer totale Bonus-korting"""
    conn = get_connection()
    cursor = conn.cursor()

    year_filter = f"AND strftime('%Y', i.invoice_date) = '{year}'" if year else ""

    cursor.execute(f"""
        SELECT
            SUM(CASE WHEN p.original_name LIKE '%Bonus%' OR p.original_name LIKE '%bonus%'
                THEN ABS(p.price) ELSE 0 END) as total_bonus,
            COUNT(DISTINCT CASE WHEN p.original_name LIKE '%Bonus%' OR p.original_name LIKE '%bonus%'
                THEN i.id END) as invoices_with_bonus,
            COUNT(CASE WHEN p.original_name LIKE '%Bonus%' OR p.original_name LIKE '%bonus%'
                THEN 1 END) as bonus_items
        FROM products p
        JOIN invoices i ON p.invoice_id = i.id
        WHERE 1=1 {year_filter}
    """)

    row = cursor.fetchone()
    conn.close()

    if row:
        return {
            "total_bonus_savings": round(abs(row['total_bonus'] or 0), 2),
            "invoices_with_bonus": row['invoices_with_bonus'] or 0,
            "bonus_items_count": row['bonus_items'] or 0
        }

    return {"total_bonus_savings": 0, "invoices_with_bonus": 0, "bonus_items_count": 0}


def get_spending_prediction(year: str = None) -> Dict:
    """Voorspel totale maanduitgaven op basis van historisch gedrag"""
    conn = get_connection()
    cursor = conn.cursor()

    # Get current year if not specified
    if not year:
        cursor.execute("SELECT strftime('%Y', MAX(invoice_date)) FROM invoices")
        year = cursor.fetchone()[0]

    # Get spending per month for the year
    cursor.execute("""
        SELECT
            strftime('%m', invoice_date) as month,
            SUM(total_amount) as month_total
        FROM invoices
        WHERE strftime('%Y', invoice_date) = ?
        GROUP BY month
        ORDER BY month
    """, (year,))

    monthly_data = [dict(row) for row in cursor.fetchall()]
    conn.close()

    if not monthly_data:
        return {"predicted_monthly": 0, "predicted_yearly": 0, "months_with_data": 0}

    # Calculate average from available months
    total = sum(m['month_total'] for m in monthly_data)
    months_count = len(monthly_data)
    avg_monthly = total / months_count if months_count > 0 else 0
    predicted_yearly = avg_monthly * 12

    return {
        "predicted_monthly": round(avg_monthly, 2),
        "predicted_yearly": round(predicted_yearly, 2),
        "months_with_data": months_count,
        "year": year
    }
