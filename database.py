"""
Database module for AH Invoice Analyzer
Uses SQLite for data persistence and management
"""

import sqlite3
import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict

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

    # The base schema above is v1; everything since lives in versioned migrations.
    # Running them here means scripts and tests get the same schema as the app.
    import contextlib
    import io
    from migrations.migration_manager import run_all_migrations
    with contextlib.redirect_stdout(io.StringIO()):
        run_all_migrations()


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


CHAIN_BY_SOURCE = {"ah_online": "Albert Heijn", "ah_kassabon": "Albert Heijn", "ah_api": "Albert Heijn",
                   "plus_receipt": "PLUS"}


def _get_or_create_store(cursor, name: str, chain: str) -> int:
    cursor.execute("SELECT id FROM stores WHERE name = ?", (name,))
    row = cursor.fetchone()
    if row:
        return row["id"]
    cursor.execute("INSERT INTO stores (name, chain) VALUES (?, ?)", (name, chain))
    return cursor.lastrowid


def save_parsed_receipt(result: Dict, invoice_id: Optional[int] = None) -> int:
    """
    Store an importer result (see importers.common) in one transaction: store, invoice,
    product lines and extras. With invoice_id the existing invoice is rewritten in place
    (same id, so links and bookmarks keep working).

    Product dicts may carry 'category'/'subcategory' (automatic) and 'user_category',
    'user_subcategory', 'is_validated' (preserved manual work).
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        chain = result.get("chain") or CHAIN_BY_SOURCE.get(result.get("source_type"), "Onbekend")
        store_name = result.get("store") or chain
        if chain == "Albert Heijn" and result.get("source_type") == "ah_online":
            store_name = "Albert Heijn online"
        store_id = _get_or_create_store(cursor, store_name, chain)

        metadata = result.get("source_metadata")
        invoice_date = result.get("invoice_date")
        if isinstance(invoice_date, datetime):
            invoice_date = invoice_date.strftime("%Y-%m-%d %H:%M:%S")
        values = (
            result["file_hash"], result["filename"], result.get("invoice_number"),
            invoice_date, result.get("total_amount") or 0.0, result.get("total_savings") or 0.0,
            result.get("raw_text", ""), result.get("source_type"), result.get("external_id"),
            json.dumps(metadata, default=str) if metadata else None,
            result.get("parse_quality_score"), result.get("parse_notes"), store_id,
        )
        columns = ("file_hash, filename, invoice_number, invoice_date, total_amount, total_savings, raw_text, "
                   "source_type, external_id, source_metadata, parse_quality_score, parse_notes, store_id")
        if invoice_id is None:
            cursor.execute(f"INSERT INTO invoices ({columns}) VALUES ({', '.join('?' * len(values))})", values)
            invoice_id = cursor.lastrowid
        else:
            assignments = ", ".join(f"{c.strip()} = ?" for c in columns.split(","))
            cursor.execute(f"UPDATE invoices SET {assignments}, last_parsed_at = CURRENT_TIMESTAMP, "
                           f"updated_at = CURRENT_TIMESTAMP WHERE id = ?", (*values, invoice_id))
            cursor.execute("DELETE FROM products WHERE invoice_id = ?", (invoice_id,))
            cursor.execute("DELETE FROM invoice_extras WHERE invoice_id = ?", (invoice_id,))

        for p in result.get("products", []):
            cursor.execute("""
                INSERT INTO products (invoice_id, original_name, display_name, quantity, price,
                                      btw_percentage, auto_category, auto_subcategory,
                                      user_category, user_subcategory, is_validated,
                                      gross_price, discount_amount, unit_price, weight_kg, is_bonus, catalog_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                invoice_id, p.get("original_name") or p["name"], p["name"], p.get("quantity", 1), p["price"],
                p.get("btw") or "", p.get("category") or "Overig", p.get("subcategory"),
                p.get("user_category"), p.get("user_subcategory"), 1 if p.get("is_validated") else 0,
                p.get("gross", p["price"]), p.get("discount", 0.0), p.get("unit_price"), p.get("weight_kg"),
                1 if p.get("bonus") else 0, p.get("catalog_id"),
            ))
        for e in result.get("extras", []):
            cursor.execute("""
                INSERT INTO invoice_extras (invoice_id, extra_type, description, quantity, amount)
                VALUES (?, ?, ?, ?, ?)
            """, (invoice_id, e["type"], e["description"], e.get("quantity", 1), e["amount"]))
        conn.commit()
        return invoice_id
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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


def _apply_category_to_catalog(product_names: List[str], category: Optional[str],
                               subcategory: Optional[str]) -> int:
    """
    Promote a category choice made on receipt names to their catalog products, so it covers
    every spelling/pack of the product and future receipts. category=None keeps the
    product's current category (subcategory-only change). Returns products updated.
    """
    if not product_names:
        return 0
    from services import catalog

    conn = get_connection()
    placeholders = ",".join("?" * len(product_names))
    rows = conn.execute(f"""
        SELECT DISTINCT cp.id, cp.category FROM products p JOIN catalog_products cp ON cp.id = p.catalog_id
        WHERE p.original_name IN ({placeholders})
    """, product_names).fetchall()
    conn.close()
    updated = 0
    for row in rows:
        target = category or row["category"]
        if not target:
            continue
        sub = subcategory if subcategory and catalog.valid_pair(target, subcategory) else None
        catalog.set_category(row["id"], target, sub)
        updated += 1
    return updated


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
    if apply_to_similar:
        _apply_category_to_catalog([product_name], category, None)
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

    name = category['name']
    if name in ("Overig", "Bijkomende kosten"):
        conn.close()
        return {"success": False, "error": f"'{name}' is een vaste categorie"}

    # Products in the deleted category move to Overig (never a dangling category name)
    cursor.execute("""
        UPDATE catalog_products SET category = 'Overig', subcategory = NULL, needs_review = 1
        WHERE category = ?
    """, (name,))
    cursor.execute("""
        UPDATE products SET auto_category = 'Overig', auto_subcategory = NULL WHERE auto_category = ?
    """, (name,))
    cursor.execute("""
        UPDATE products SET user_category = NULL, user_subcategory = NULL WHERE user_category = ?
    """, (name,))
    product_count = cursor.rowcount

    cursor.execute("DELETE FROM subcategories WHERE category_id = ?", (category_id,))
    cursor.execute("DELETE FROM categories WHERE id = ?", (category_id,))
    conn.commit()
    conn.close()

    from services import catalog
    catalog.invalidate_category_cache()
    return {"success": True, "name": name, "products_affected": product_count}


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


def get_analysis_data(year: str = None, chain: str = None) -> Dict:
    """
    Dashboard data for a year (or everything), computed by services.metrics so every
    page shows the same numbers. Keys kept compatible with the existing frontend.
    """
    from services import metrics

    s = metrics.summary(year, chain)
    categories = metrics.category_totals(year, chain)
    top = metrics.top_products(year, chain, limit=20)
    yearly = metrics.yearly_spent(chain)
    return {
        "summary": {
            "total_invoices": s["receipts"],
            "total_spent": s["spent"],
            "total_paid": s["paid"],
            "total_groceries": s["groceries"],
            "total_extra_costs": s["extra_costs"],
            "extra_costs_by_type": s["extra_costs_by_type"],
            "total_savings_stamps": s["savings_stamps"],
            "total_savings": s["bonus_savings"],
            "avg_per_invoice": s["avg_per_receipt"],
            "avg_monthly": s["avg_per_month"],
            "months": s["months"],
            "date_range": {"start": s["first_date"], "end": s["last_date"]},
        },
        "category_totals": {k: v["total"] for k, v in categories.items()},
        "category_counts": {k: v["count"] for k, v in categories.items()},
        "subcategory_totals": metrics.subcategory_totals(year, chain),
        "monthly_totals": metrics.monthly_spent(year, chain),
        "yearly_totals": yearly,
        "yearly_complete": {y: metrics.period_is_complete(y) for y in yearly},
        "top_products": top,
        "thirty_year_projection": {p["name"]: metrics.projection(p["total"], s["months"]) for p in top[:10]},
        "savings_options": [],
        "selected_year": year,
        "available_years": sorted(yearly.keys(), reverse=True),
    }


def migrate_normalize_product_names():
    """
    "Keywords toepassen": re-apply keyword/override rules to catalog products that were
    categorised by keywords (or not at all). Products you labelled yourself or that the
    Decisions API labelled keep their category. Then lines are brought in line.
    """
    from parser import categorize
    from services import catalog

    conn = get_connection()
    cursor = conn.cursor()
    rows = cursor.execute("""
        SELECT id, name, category, subcategory FROM catalog_products
        WHERE category IS NULL OR COALESCE(category_source, 'keyword') IN ('keyword', 'rule')
    """).fetchall()
    recategorized = 0
    for row in rows:
        new = categorize(row['name'])
        sub = new.get('subcategory') if catalog.valid_pair(new['category'], new.get('subcategory')) else None
        if (new['category'], sub) != (row['category'], row['subcategory']):
            cursor.execute("""UPDATE catalog_products SET category = ?, subcategory = ?, category_source = 'keyword'
                              WHERE id = ?""", (new['category'], sub, row['id']))
            recategorized += 1
    # Lines without a catalog product (e.g. discount lines) follow the rules directly
    for row in cursor.execute("SELECT id, original_name, auto_category FROM products WHERE catalog_id IS NULL").fetchall():
        new = categorize(row['original_name'])
        if new['category'] != row['auto_category']:
            cursor.execute("UPDATE products SET auto_category = ?, auto_subcategory = ? WHERE id = ?",
                           (new['category'], new.get('subcategory'), row['id']))
            recategorized += 1
    conn.commit()
    conn.close()
    catalog.materialise()
    return {"updated": 0, "recategorized": recategorized}


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
    _apply_category_to_catalog([product_name], new_category, None)
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
    _apply_category_to_catalog(product_names, new_category, None)
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
    _apply_category_to_catalog(product_names, subcat_row['category_name'], subcategory)
    return affected


def reparse_all_invoices():
    """
    Re-parse every invoice from its original file with the matching importer, in place
    (same invoice ids, manual categories and validations kept).
    """
    from parser import import_all_invoices
    return import_all_invoices("data/invoices", force=True)


def get_all_subcategories() -> List[Dict]:
    """Haal alle subcategorieën op met hun hoofdcategorie"""
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT s.*, c.name as category_name, c.color as category_color,
               COALESCE(counts.n, 0) as product_count
        FROM subcategories s
        JOIN categories c ON s.category_id = c.id
        LEFT JOIN (
            SELECT COALESCE(user_category, auto_category) cat, COALESCE(user_subcategory, auto_subcategory) sub,
                   COUNT(*) n
            FROM products GROUP BY cat, sub
        ) counts ON counts.cat = c.name AND counts.sub = s.name
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
    if apply_to_similar:
        _apply_category_to_catalog([product_name], subcat_row['category_name'], subcategory)
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


def get_day_of_week_analysis(year: str = None) -> Dict:
    """Spending per weekday (see services.metrics.day_of_week)."""
    from services import metrics
    return metrics.day_of_week(year)


def get_bonus_analysis(year: str = None) -> Dict:
    """Bonus/discounts received, from the receipts' own 'voordeel' totals."""
    from services import metrics
    return metrics.bonus(year)


def get_spending_prediction(year: str = None) -> Dict:
    """Projected yearly spending from the average per calendar month so far."""
    from services import metrics
    return metrics.prediction(year)


if __name__ == "__main__":
    init_database()
    print(f"Database initialized at {DATABASE_PATH}")
