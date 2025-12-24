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
    return conn


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
    
    # Product category overrides (user corrections)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_overrides (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            product_name_pattern TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
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
    
    conn.commit()
    
    # Insert default categories
    default_categories = [
        ("Zuivel & Eieren", "#00ADE6", "🥛", 1, 1),
        ("Groente & Fruit", "#7CB518", "🥦", 1, 2),
        ("Vlees & Vis", "#E63946", "🥩", 1, 3),
        ("Brood & Bakkerij", "#D4A574", "🍞", 1, 4),
        ("Pasta, Rijst & Granen", "#FFB347", "🍝", 1, 5),
        ("Dranken", "#4FC3F7", "🥤", 1, 6),
        ("Sauzen & Specerijen", "#FF7043", "🧂", 1, 7),
        ("Snacks & Zoetwaren", "#AB47BC", "🍫", 1, 8),
        ("Huishouden", "#78909C", "🧹", 1, 9),
        ("Persoonlijke Verzorging", "#F48FB1", "🧴", 1, 10),
        ("Verpakking & Statiegeld", "#90A4AE", "📦", 1, 11),
        ("Bezorgkosten", "#607D8B", "🚚", 1, 12),
        ("Abonnementen", "#9C27B0", "⭐", 1, 13),
        ("Overig", "#9E9E9E", "📋", 1, 99),
    ]
    
    for name, color, icon, is_system, sort_order in default_categories:
        cursor.execute("""
            INSERT OR IGNORE INTO categories (name, color, icon, is_system, sort_order)
            VALUES (?, ?, ?, ?, ?)
        """, (name, color, icon, is_system, sort_order))
    
    conn.commit()
    
    # Insert default subcategories
    default_subcategories = {
        "Zuivel & Eieren": [
            ("Melk", "🥛"),
            ("Kaas", "🧀"),
            ("Yoghurt & Kwark", "🥄"),
            ("Boter & Margarine", "🧈"),
            ("Eieren", "🥚"),
            ("Room & Vla", "🍮"),
            ("Plantaardig", "🌱"),
        ],
        "Groente & Fruit": [
            ("Groente", "🥬"),
            ("Fruit", "🍎"),
            ("Kruiden", "🌿"),
            ("Peulvruchten", "🫘"),
            ("Salades", "🥗"),
            ("Aardappelen", "🥔"),
        ],
        "Vlees & Vis": [
            ("Kip", "🍗"),
            ("Rund & Varken", "🥩"),
            ("Vis", "🐟"),
            ("Vleeswaren", "🥓"),
            ("Vegetarisch & Vegan", "🌱"),
            ("Wild & Gevogelte", "🦆"),
        ],
        "Brood & Bakkerij": [
            ("Brood", "🍞"),
            ("Gebak & Taart", "🍰"),
            ("Ontbijtproducten", "🥐"),
            ("Crackers & Toast", "🥪"),
        ],
        "Pasta, Rijst & Granen": [
            ("Pasta", "🍝"),
            ("Rijst", "🍚"),
            ("Granen & Muesli", "🌾"),
            ("Peulvruchten Droog", "🫘"),
        ],
        "Dranken": [
            ("Koffie", "☕"),
            ("Thee", "🍵"),
            ("Frisdrank", "🥤"),
            ("Sap & Smoothies", "🧃"),
            ("Bier", "🍺"),
            ("Wijn", "🍷"),
            ("Sterke Drank", "🥃"),
            ("Water", "💧"),
            ("Sportdranken", "⚡"),
            ("Plantaardige Dranken", "🥛"),
        ],
        "Sauzen & Specerijen": [
            ("Sauzen", "🫙"),
            ("Kruiden & Specerijen", "🧂"),
            ("Olie & Azijn", "🫒"),
            ("Pasta Sauzen", "🍅"),
        ],
        "Snacks & Zoetwaren": [
            ("Chips & Noten", "🥜"),
            ("Snoep", "🍬"),
            ("Chocolade", "🍫"),
            ("Koekjes", "🍪"),
            ("Ijs", "🍨"),
        ],
        "Huishouden": [
            ("Schoonmaak", "🧹"),
            ("Wasmiddel", "🧺"),
            ("Keukenbenodigdheden", "🍽️"),
            ("Vuilniszakken & Folie", "🗑️"),
        ],
        "Persoonlijke Verzorging": [
            ("Haar", "💇"),
            ("Huid", "🧴"),
            ("Mondverzorging", "🦷"),
            ("Hygiëne", "🚿"),
        ],
    }
    
    for category_name, subcats in default_subcategories.items():
        # Haal category id op
        cursor.execute("SELECT id FROM categories WHERE name = ?", (category_name,))
        cat_row = cursor.fetchone()
        if cat_row:
            cat_id = cat_row['id']
            for i, (subcat_name, icon) in enumerate(subcats):
                cursor.execute("""
                    INSERT OR IGNORE INTO subcategories (name, category_id, icon, sort_order)
                    VALUES (?, ?, ?, ?)
                """, (subcat_name, cat_id, icon, i + 1))
    
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
    
    cursor.execute("""
        INSERT INTO invoices (file_hash, filename, invoice_number, invoice_date, 
                             total_amount, total_savings, raw_text)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        invoice_data['file_hash'],
        invoice_data['filename'],
        invoice_data.get('invoice_number'),
        invoice_data.get('date'),
        invoice_data.get('total', 0),
        invoice_data.get('savings', 0),
        invoice_data.get('raw_text', '')
    ))
    
    invoice_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return invoice_id


def save_products(invoice_id: int, products: List[Dict]):
    """Save products for an invoice"""
    conn = get_connection()
    cursor = conn.cursor()
    
    for product in products:
        cursor.execute("""
            INSERT INTO products (invoice_id, original_name, display_name, quantity, 
                                 price, btw_percentage, auto_category, auto_subcategory)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            invoice_id,
            product['name'],
            product['name'],
            product.get('quantity', 1),
            product.get('price', 0),
            product.get('btw', ''),
            product.get('category', 'Overig'),
            product.get('subcategory')
        ))
    
    conn.commit()
    conn.close()


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
            UPDATE products SET user_category = ?
            WHERE original_name = ?
        """, (category, product_name))
        updated_count = cursor.rowcount
        
        # Also save as an override rule for future imports
        cursor.execute("SELECT id FROM categories WHERE name = ?", (category,))
        cat_row = cursor.fetchone()
        if cat_row:
            cursor.execute("""
                INSERT OR REPLACE INTO product_overrides (product_name_pattern, category_id)
                VALUES (?, ?)
            """, (product_name, cat_row['id']))
    else:
        # Update alleen dit specifieke product
        cursor.execute("""
            UPDATE products SET user_category = ?
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
        SET is_validated = ?, validation_notes = ?, updated_at = CURRENT_TIMESTAMP
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
            AVG(price / NULLIF(quantity, 0)) as avg_unit_price,
            MIN(price / NULLIF(quantity, 0)) as min_unit_price,
            MAX(price / NULLIF(quantity, 0)) as max_unit_price
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
            AVG(price / NULLIF(quantity, 0)) as avg_unit_price
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
        SET user_category = ?
        WHERE original_name = ?
    """, (new_category, product_name))
    
    affected = cursor.rowcount
    conn.commit()
    conn.close()
    
    return affected


def reparse_all_invoices():
    """
    Herparse alle facturen met de verbeterde parser.
    Verwijdert bestaande producten en importeert ze opnieuw.
    """
    from parser import extract_products_from_text, categorize_product, normalize_product_name
    
    conn = get_connection()
    cursor = conn.cursor()
    
    print("Herparsing all invoices with improved parser...")
    
    # Haal alle facturen op
    cursor.execute("SELECT id, raw_text, filename FROM invoices")
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
    cursor.execute("SELECT original_name FROM products WHERE id = ?", (product_id,))
    product = cursor.fetchone()
    
    if not product:
        conn.close()
        return {"success": False, "error": "Product niet gevonden", "updated_count": 0}
    
    product_name = product['original_name']
    updated_count = 0
    
    if apply_to_similar:
        cursor.execute("""
            UPDATE products SET user_subcategory = ?
            WHERE original_name = ?
        """, (subcategory, product_name))
        updated_count = cursor.rowcount
    else:
        cursor.execute("""
            UPDATE products SET user_subcategory = ?
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

