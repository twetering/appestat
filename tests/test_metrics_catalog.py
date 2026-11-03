import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

import database
from importers.common import make_extra, make_product, new_result


def receipt(file_hash, date, source, store, products, extras=(), savings=0.0):
    r = new_result(source, f"{file_hash}.pdf", file_hash)
    r["invoice_date"] = date
    r["store"] = store
    r["products"] = [make_product(n, q, a) for n, q, a in products]
    for p, cat in zip(r["products"], [c for *_, c in [(None, "Overig")] * len(products)]):
        p["category"] = cat
    r["extras"] = [make_extra(t, d, a) for t, d, a in extras]
    r["total_amount"] = round(sum(p["price"] for p in r["products"]) + sum(e["amount"] for e in r["extras"]), 2)
    r["total_savings"] = savings
    return r


class MetricsCatalogTest(unittest.TestCase):
    def setUp(self):
        os.environ.pop("OPENAI_API_KEY", None)  # never call the API from tests
        self._tmp = tempfile.TemporaryDirectory()
        self._old = database.DATABASE_PATH
        database.DATABASE_PATH = Path(self._tmp.name) / "test.db"
        database.init_database()
        from services import catalog
        catalog.invalidate_category_cache()
        database.save_parsed_receipt(receipt(
            "a1", datetime(2025, 3, 1), "ah_online", "Albert Heijn",
            [("Red Bull Energy drink 24-pack", 1, 30.36), ("Red Bull Energy drink 8-pack voordeelverpakking", 1, 12.16),
             ("AH Halfvolle melk", 2, 2.38)],
            [("deposit", "Statiegeld", 1.80), ("savings", "Koopzegels Premium", 10.00)], savings=2.10))
        database.save_parsed_receipt(receipt(
            "p1", datetime(2025, 6, 15), "plus_receipt", "PLUS Van Schaick",
            [("HALFVOLLE MELK", 1, 1.15), ("ACTIEKORTING FRANSE WIJN", 1, -1.97), ("SAUVIGNON BLANC", 1, 3.59)],
            [("savings", "PLUSPunten digitaal", 0.34)]))

    def tearDown(self):
        database.DATABASE_PATH = self._old
        self._tmp.cleanup()

    def test_spent_excludes_savings_and_categories_add_up(self):
        from services import metrics
        s = metrics.summary(2025)
        self.assertEqual(s["paid"], 59.81)
        self.assertEqual(s["savings_stamps"], 10.34)
        self.assertEqual(s["spent"], 49.47)
        self.assertEqual(s["unexplained"], 0.0)
        self.assertAlmostEqual(sum(v["total"] for v in metrics.category_totals(2025).values()), s["spent"], places=2)
        self.assertAlmostEqual(sum(metrics.monthly_spent(2025).values()), s["spent"], places=2)

    def test_store_filter(self):
        from services import metrics
        self.assertEqual(metrics.summary(2025, chain="PLUS")["receipts"], 1)
        self.assertEqual(metrics.summary(2025, chain="Albert Heijn")["spent"], 46.70)

    def test_months_in_period_counts_calendar_months(self):
        from services import metrics
        self.assertEqual(metrics.months_in_period(2025), 4)  # March up to June (latest receipt)

    def test_catalog_groups_pack_variants_without_ai(self):
        from services import catalog
        catalog.sync(use_ai=False)
        products = {p["name"]: p for p in catalog.list_products()}
        self.assertIn("Red Bull Energy drink", products)
        red_bull = catalog.product_detail(products["Red Bull Energy drink"]["id"])
        self.assertEqual(red_bull["units"], 32)
        self.assertEqual(red_bull["best_variant"]["pack_count"], 24)
        self.assertAlmostEqual(red_bull["unit_price_avg"], 1.329, places=3)

    def test_discount_lines_are_not_catalog_products(self):
        from services import catalog
        catalog.sync(use_ai=False)
        names = {p["name"] for p in catalog.list_products()}
        self.assertFalse(any(n.upper().startswith("ACTIEKORTING") for n in names))

    def test_set_category_validates_and_applies_to_all_lines(self):
        from services import catalog
        catalog.sync(use_ai=False)
        pid = next(p["id"] for p in catalog.list_products() if p["name"] == "Red Bull Energy drink")
        category, subcategory = next((c, s) for c, s in catalog.category_pairs() if s)
        wrong = next(s for c, s in catalog.category_pairs() if s and c != category)
        with self.assertRaises(ValueError):
            catalog.set_category(pid, category, wrong)
        result = catalog.set_category(pid, category, subcategory)
        self.assertEqual(result["lines_updated"], 2)
        conn = database.get_connection()
        cats = {r[0] for r in conn.execute("SELECT auto_category FROM products WHERE catalog_id = ?", (pid,))}
        conn.close()
        self.assertEqual(cats, {category})


if __name__ == "__main__":
    unittest.main()
