import unittest

from importers import ah_kassabon, ah_online
from importers.common import classify_extra, products_total, extras_total

AH_ONLINE = """Factuur
T. Test Datum 25 mei 2026
Teststraat 1 Factuurnummer 1069835-00999
1234 AB TESTSTAD Debiteurnummer 1234567
Uw bestelling 315786041
Afleverdatum 25 mei 2026
Totaal inclusief btw 70,34
Uw voordeel 2,10 is in het totaalbedrag verwerkt
Omschrijving Btw Exclusief btw Btw-bedrag Inclusief btw
Boodschappen, zie specificatie 9% 30,60 2,75 33,35
Boodschappen, zie specificatie 21% 4,12 0,87 4,99
Statiegeld geleverd Geen 1,80 0,00 1,80
Koopzegels Premium Geen 29,40 0,00 29,40
Bezorgkosten 21% 2,47 0,52 2,99
Alle bedragen zijn in euro's Totaal 63,12 3,67 66,79
Specificatie
Datum 25 mei 2026
Factuurnummer 1069835-00999
Omschrijving Aantal Btw Exclusief btw Btw-bedrag Inclusief btw
AH Bananen tros 1 9% 1,33 0,12 1,45
AH Groene kiwi¿s BONUS 1 9% 2,74 0,25 2,99
o.b. ProComfort tampons normaal BONUS 1 9% 3,89 0,35 4,24
AH Kookzuivel 7,2% 1 9% 0,72 0,07 0,79
Red Bull Energy drink 12-pack voordeelverpakking 1 9% 17,15 1,54 18,69
+ Statiegeld 1 Geen 1,80 0,00 1,80
Pagina 2 van 3
AH Volkoren havermout fijne vlokken 1 9% 1,33 0,12 1,45
Old Amsterdam Original 48+ plakken voordeelverpakking
1 9% 3,43 0,31 3,74
Cono Sur Bicicleta sauvignon blanc 1 21% 4,12 0,87 4,99
Koopzegels Premium 294 Geen 29,40 0,00 29,40
Bezorgkosten 21% 2,47 0,52 2,99
AH Halfvolle melk 9% -1,55 -0,14 -1,69
Glazen flesje 0.10 2 Geen -0,20 0,00 -0,20
Petfles klein 0.15 2 Geen -0,30 0,00 -0,30
Totaal 63,12 3,67 66,79
"""

KASSABON = """ALBERT HEIJN TESTDORP
AANTAL OMSCHRIJVING PRIJS BEDRAG
BONUSKAART xx1234
1 AH HV MELK 1,79
1 SABLE DRUIF 3,29 B
2 DRAAGTAS 0,79 1,58
0.962KG TROSTOMAAT 2,38 2,29 B
1 HERTOG JAN 4,79 B
+STATIEGELD 0,60
5 SUBTOTAAL 14,34
BONUS AHSABLEDRUIF -1,10
BONUS HERTOGJAN -0,43
UW VOORDEEL 11,53
Waarvan
BONUS BOX PREMIUM 0,18
SUBTOTAAL 12,81
40 KOOPZEGELS PREMIUM 4,00
10 MIJN AH MILES -10,00
TOTAAL 6,81
15:24 27-2-2025
"""


class TestAHOnline(unittest.TestCase):
    def setUp(self):
        self.r = ah_online.parse_text(AH_ONLINE, "1069835-00999.pdf")
        self.names = [p["name"] for p in self.r["products"]]

    def test_first_product_on_each_page_is_kept(self):
        self.assertIn("AH Bananen tros", self.names)
        self.assertIn("AH Volkoren havermout fijne vlokken", self.names)
        self.assertFalse(any("\n" in n for n in self.names))

    def test_unusual_names(self):
        self.assertIn("AH Groene kiwi's", self.names)
        self.assertIn("o.b. ProComfort tampons normaal", self.names)
        self.assertIn("AH Kookzuivel 7,2%", self.names)
        self.assertIn("Old Amsterdam Original 48+ plakken voordeelverpakking", self.names)

    def test_extras_are_split_from_products(self):
        types = {(e["type"], e["description"]) for e in self.r["extras"]}
        self.assertIn(("savings", "Koopzegels Premium"), types)
        self.assertIn(("delivery", "Bezorgkosten"), types)
        self.assertIn(("deposit", "Glazen flesje 0.10"), types)
        self.assertNotIn("Glazen flesje 0.10", self.names)

    def test_credit_line_is_negative_product(self):
        credit = [p for p in self.r["products"] if p["name"] == "AH Halfvolle melk"][0]
        self.assertEqual(credit["price"], -1.69)

    def test_reconciles_with_printed_totals(self):
        self.assertEqual(self.r["parse_quality_score"], 1.0, self.r["parse_notes"])
        self.assertAlmostEqual(products_total(self.r) + extras_total(self.r), self.r["total_amount"], places=2)
        self.assertEqual(self.r["invoice_date"].isoformat()[:10], "2026-05-25")


class TestKassabon(unittest.TestCase):
    def setUp(self):
        self.r = ah_kassabon.parse_text(KASSABON, "AH_kassabon_2025-02-27 152400_8719.pdf")
        self.by_name = {p["name"]: p for p in self.r["products"]}

    def test_reconciles(self):
        self.assertEqual(self.r["parse_quality_score"], 1.0, self.r["parse_notes"])
        self.assertEqual(self.r["total_amount"], 6.81)

    def test_weighed_product_keeps_name_without_weight(self):
        tomaat = self.by_name["AH Trostomaten"]
        self.assertEqual(tomaat["weight_kg"], 0.962)
        self.assertEqual(tomaat["unit_price"], 2.38)

    def test_bonus_discounts_land_on_their_products(self):
        self.assertEqual(self.by_name["SABLE DRUIF"]["price"], 2.19)
        self.assertEqual(self.by_name["HERTOG JAN"]["price"], 4.36)
        self.assertEqual(self.by_name["AH Halfvolle melk"]["discount"], 0.0)

    def test_extras(self):
        extras = {(e["type"], e["description"]): e["amount"] for e in self.r["extras"]}
        self.assertEqual(extras[("deposit", "Statiegeld")], 0.60)
        self.assertEqual(extras[("deposit", "Draagtas")], 1.58)
        self.assertEqual(extras[("savings", "Koopzegels Premium")], 4.00)
        self.assertEqual(extras[("savings", "AH Miles ingewisseld")], -10.00)

    def test_store_and_date(self):
        self.assertEqual(self.r["store"], "Albert Heijn Testdorp")
        self.assertEqual(self.r["invoice_date"].isoformat(), "2025-02-27T15:24:00")


TOGO_KASSABON = """5865
AANTAL OMSCHRIJVING PRIJS BEDRAG
1 MU MILKSHAKE 2,50 A
1 SUBTOTAAL 2,50
ACTIE TOGO-MELKUNI -0,63
UW VOORDEEL 0,63
Waarvan
to go sparen 0,00
TOTAAL 1,87
BETAALD MET:
PINNEN 1,87
16:20 11-5-2026
"""


class TestKassabonToGo(unittest.TestCase):
    """AH to go tills print a VAT letter after the price and 'ACTIE' discounts."""

    def test_item_with_vat_letter_and_actie_discount(self):
        r = ah_kassabon.parse_text(TOGO_KASSABON, "AH_kassabon_2026-05-11 162000_5865.pdf", "togo")
        self.assertEqual(r["parse_quality_score"], 1.0, r["parse_notes"])
        self.assertEqual([(p["name"], p["price"]) for p in r["products"]], [("Melkunie Milkshake", 1.87)])
        self.assertEqual(r["extras"], [])  # "to go sparen 0,00" is not a saving


class TestManual(unittest.TestCase):
    def test_round_trip(self):
        import tempfile
        from pathlib import Path
        from importers import manual
        with tempfile.TemporaryDirectory() as tmp:
            path = manual.write_purchase("Jimmy Joy", "2026-10-05", 153.8, note="Plenny", directory=Path(tmp))
            r = manual.parse_file(str(path), "hash")
            self.assertEqual((r["chain"], r["total_amount"], r["parse_quality_score"]), ("Jimmy Joy", 153.8, 1.0))
            self.assertEqual(r["products"][0]["name"], "Jimmy Joy bestelling")
            with self.assertRaises(FileExistsError):
                manual.write_purchase("Jimmy Joy", "2026-10-05", 153.8, directory=Path(tmp))


class TestDiscountSynonyms(unittest.TestCase):
    def test_brand_discount_finds_generic_till_name(self):
        from importers.common import apply_discount, make_product
        ps = [make_product("LUNGO PROFUNDO 8", 1, 5.79, bonus=True), make_product("ENERGY DRINK", 2, 3.50, bonus=True)]
        self.assertIsNone(apply_discount("ACTIE RED BULL", 0.71, ps, "x"))
        self.assertEqual([p["price"] for p in ps], [5.79, 2.79])


class TestClassifyExtra(unittest.TestCase):
    def test_products_are_not_extras(self):
        for name in ["Hertog Jan krat 24 flesjes", "AH Bananen", "Spa blauw fles 1,5L", "Red Bull 4-pack"]:
            self.assertIsNone(classify_extra(name), name)

    def test_extras(self):
        self.assertEqual(classify_extra("AH Bezorgbundel onbeperkt* 12 maanden BONUS"), "subscription")
        self.assertEqual(classify_extra("AH Premium Abonnement"), "subscription")
        self.assertEqual(classify_extra("AH Klapkrat"), "deposit")
        self.assertEqual(classify_extra("Bulk blik (25 stuks)"), "deposit")
        self.assertEqual(classify_extra("Koopzegels"), "savings")


if __name__ == "__main__":
    unittest.main()
