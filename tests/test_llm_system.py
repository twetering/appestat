import os
import tempfile
import unittest
from pathlib import Path

import database
from database import init_database, get_connection, add_subcategory, save_llm_suggestions, apply_llm_suggestions
from services.llm_categorizer import LLMCategorizer


class TestLLMSystem(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self._old_db_path = database.DATABASE_PATH
        database.DATABASE_PATH = Path(self._tmpdir.name) / "test_appestat.db"
        init_database()

    def tearDown(self):
        database.DATABASE_PATH = self._old_db_path
        self._tmpdir.cleanup()

    def test_prompt_uses_ids(self):
        categorizer = LLMCategorizer()
        categories = {
            "Groente": {"id": 1, "subcategories": [{"id": 10, "name": "Bladgroente"}]},
            "Overig": {"id": 99, "subcategories": []},
        }
        prompt = categorizer._build_system_prompt(categories)
        self.assertIn("C1 Groente", prompt)
        self.assertIn("S10 Bladgroente", prompt)

    def test_resolve_category_and_subcategory(self):
        categorizer = LLMCategorizer()
        categories = {
            "Groente": {"id": 1, "subcategories": [{"id": 10, "name": "Bladgroente"}]},
            "Overig": {"id": 99, "subcategories": []},
        }
        indexes = categorizer._build_category_indexes(categories)
        self.assertEqual(categorizer._resolve_category("C1", indexes), "Groente")
        self.assertEqual(categorizer._resolve_category("1", indexes), "Groente")
        self.assertEqual(categorizer._resolve_category(1, indexes), "Groente")
        self.assertEqual(categorizer._resolve_category("Onbekend", indexes), "Overig")
        self.assertEqual(categorizer._resolve_subcategory("S10", "Groente", indexes), "Bladgroente")
        self.assertIsNone(categorizer._resolve_subcategory("S10", "Overig", indexes))

    def test_apply_llm_suggestions_updates_products_and_overrides(self):
        # Prepare category + subcategory
        category_name = "Groente & Aardappelen"
        subcategory_id = add_subcategory(category_name, "TestSub")

        # Insert product
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO invoices (file_hash, filename)
            VALUES ('testhash', 'test.pdf')
        """)
        invoice_id = cursor.lastrowid
        cursor.execute("""
            INSERT INTO products (invoice_id, original_name, display_name, quantity, price, auto_category)
            VALUES (?, 'Test Product', 'Test Product', 1, 1.23, 'Overig')
        """, (invoice_id,))
        conn.commit()

        # Lookup category ID
        cursor.execute("SELECT id FROM categories WHERE name = ?", (category_name,))
        cat_id = cursor.fetchone()['id']
        conn.close()

        # Save suggestion
        save_llm_suggestions([{
            "product_name": "Test Product",
            "category_id": cat_id,
            "subcategory_id": subcategory_id,
            "confidence": 0.8,
            "model": "gpt-4o-mini",
            "prompt_version": "test"
        }])

        # Fetch suggestion id and apply
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM llm_suggestions WHERE product_name = 'Test Product'")
        suggestion_id = cursor.fetchone()['id']
        conn.close()

        result = apply_llm_suggestions([suggestion_id])
        self.assertEqual(result["applied"], 1)

        # Verify product updated
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT user_category, user_subcategory FROM products WHERE original_name = 'Test Product'
        """)
        row = cursor.fetchone()
        self.assertEqual(row['user_category'], category_name)
        self.assertEqual(row['user_subcategory'], "TestSub")

        # Verify override created
        cursor.execute("""
            SELECT category_id, subcategory_id FROM product_overrides WHERE product_name_pattern = 'Test Product'
        """)
        override = cursor.fetchone()
        self.assertIsNotNone(override)
        self.assertEqual(override['category_id'], cat_id)
        self.assertEqual(override['subcategory_id'], subcategory_id)
        conn.close()


if __name__ == "__main__":
    unittest.main()
