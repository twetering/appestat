import tempfile
import unittest
from pathlib import Path

import database
from database import (
    init_database,
    get_connection,
    add_subcategory,
    upsert_product_override,
    bulk_update_category_by_names,
    bulk_update_subcategory_by_names,
    update_product_category,
    update_product_subcategory,
)


class TestDatabaseOverrides(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self._old_db_path = database.DATABASE_PATH
        database.DATABASE_PATH = Path(self._tmpdir.name) / "test_appestat.db"
        init_database()
        
        # Seed one invoice + products
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO invoices (file_hash, filename) VALUES ('hash1', 'f1.pdf')")
        invoice_id = cursor.lastrowid
        cursor.executemany("""
            INSERT INTO products (invoice_id, original_name, display_name, quantity, price, auto_category)
            VALUES (?, ?, ?, 1, 1.00, 'Overig')
        """, [
            (invoice_id, "Prod A", "Prod A"),
            (invoice_id, "Prod A", "Prod A"),
            (invoice_id, "Prod B", "Prod B"),
        ])
        conn.commit()
        conn.close()

    def tearDown(self):
        database.DATABASE_PATH = self._old_db_path
        self._tmpdir.cleanup()

    def test_upsert_product_override_insert_and_update(self):
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM categories WHERE name = 'Overig'")
        cat_id = cursor.fetchone()['id']
        conn.close()
        
        upsert_product_override("Prod A", cat_id, source="user")
        upsert_product_override("Prod A", cat_id, source="user")
        
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT times_applied FROM product_overrides WHERE product_name_pattern = 'Prod A'
        """)
        row = cursor.fetchone()
        self.assertEqual(row['times_applied'], 2)
        conn.close()

    def test_bulk_update_category_by_names_updates_products_and_overrides(self):
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM categories WHERE name = 'Zuivel & Eieren'")
        cat_id = cursor.fetchone()['id']
        conn.close()
        
        affected = bulk_update_category_by_names(["Prod A", "Prod B"], "Zuivel & Eieren")
        self.assertEqual(affected, 3)
        
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT user_category FROM products WHERE original_name IN ('Prod A', 'Prod B')
        """)
        rows = [r['user_category'] for r in cursor.fetchall()]
        self.assertEqual(rows, ["Zuivel & Eieren"])
        
        cursor.execute("""
            SELECT COUNT(*) as cnt FROM product_overrides
            WHERE product_name_pattern IN ('Prod A', 'Prod B')
        """)
        cnt = cursor.fetchone()['cnt']
        self.assertEqual(cnt, 2)
        conn.close()

    def test_bulk_update_subcategory_by_names_validates_category(self):
        subcat_id = add_subcategory("Zuivel & Eieren", "TestSub")
        self.assertIsNotNone(subcat_id)
        
        # Set product category to match
        update_product_category(1, "Zuivel & Eieren", apply_to_similar=True)
        
        affected = bulk_update_subcategory_by_names(["Prod A"], "TestSub", category_name="Zuivel & Eieren")
        self.assertEqual(affected, 2)
        
        with self.assertRaises(ValueError):
            bulk_update_subcategory_by_names(["Prod A"], "TestSub", category_name="Groente & Aardappelen")

    def test_update_product_subcategory_apply_to_similar(self):
        add_subcategory("Zuivel & Eieren", "TestSub")
        update_product_category(1, "Zuivel & Eieren", apply_to_similar=True)
        # Apply subcategory to similar products
        result = update_product_subcategory(1, "TestSub", apply_to_similar=True)
        self.assertTrue(result["success"])
        self.assertEqual(result["updated_count"], 2)

if __name__ == "__main__":
    unittest.main()
