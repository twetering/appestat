import unittest

from sources.plus_llm import _to_result
from sources.plus_receipt import (
    _apply_actions,
    build_vocabulary,
    correct_ocr_name,
    is_korting,
    is_subtotal,
    is_total,
    parse_ocr,
)

ROW = 0.025  # vertical distance between receipt lines (normalized)


def receipt_ocr(rows, skew=0.0):
    """
    Build Vision-like OCR output. Each row is (label, amount_text or None).
    `skew` shifts amounts up relative to their label, like a photo taken at an angle.
    """
    lines = [
        {"text": "PLUS Van Schaick", "conf": 1, "x": 0.35, "y": 0.05, "w": 0.3, "h": 0.02},
        {"text": "Omschrijving", "conf": 1, "x": 0.2, "y": 0.10, "w": 0.2, "h": 0.018},
        {"text": "P.st/kg Bedrag", "conf": 1, "x": 0.55, "y": 0.10, "w": 0.25, "h": 0.018},
    ]
    y = 0.15
    for label, amount in rows:
        lines.append({"text": label, "conf": 1, "x": 0.2, "y": y, "w": 0.008 * len(label), "h": 0.018})
        if amount is not None:
            w = 0.012 * len(amount)
            lines.append({"text": amount, "conf": 1, "x": 0.8 - w, "y": y - skew, "w": w, "h": 0.018})
        y += ROW
    return {"captured_at": "2026-03-13 11:27:19", "rotation": "up", "lines": lines}


BASIC = [
    ("2X FLAMMKUCHEN DEEG", "4,38"),
    ("TROSTOMATEN LOS 2,30 €/kg", "1,81"),
    ("VEGA FILET AMERICAIN", "2,49"),
    ("Korting", "-1,00"),
    ("ENERGY DRINK 4PACK", "6,95"),
    ("Statiegeld", "0,60"),
    ("2X TRIANGEL MEERGRANEN", "1,38"),
    ("Subtotaal", "€16,61"),
    ("18X PLUSPunten DIGITAAL", "€0,36"),
    ("1X extra actie: triange", "€-0,38"),
    ("1X ACTIE FRANSE WIJN 1", "€-1,97"),
    ("Totaal", "€14,62"),
]


class TestPlusReceiptParser(unittest.TestCase):
    def test_basic_receipt_adds_up(self):
        r = parse_ocr(receipt_ocr(BASIC))
        self.assertTrue(r.valid, r.notes)
        self.assertEqual(r.subtotal, 16.61)
        self.assertEqual(r.total, 14.62)
        self.assertAlmostEqual(r.deposits, 0.60)
        names = {i.raw_name: i for i in r.items}
        self.assertEqual(names["FLAMMKUCHEN DEEG"].quantity, 2)
        self.assertEqual(names["TROSTOMATEN LOS"].net, 1.81)  # unit price stripped from name
        self.assertEqual(names["VEGA FILET AMERICAIN"].net, 1.49)  # Korting applied to item above
        self.assertEqual(names["TRIANGEL MEERGRANEN"].net, 1.00)  # action matched by name
        self.assertEqual(names["ACTIEKORTING FRANSE WIJN 1"].net, -1.97)  # unmatched -> own line
        self.assertEqual(names["PLUSPUNTEN DIGITAAL"].quantity, 18)

    def test_products_plus_deposits_equal_paid_total(self):
        r = parse_ocr(receipt_ocr(BASIC))
        self.assertAlmostEqual(sum(i.net for i in r.items) + r.deposits, r.total, places=2)

    def test_skewed_photo(self):
        # Amounts printed almost a full line higher than their label
        r = parse_ocr(receipt_ocr(BASIC, skew=ROW * 0.9))
        self.assertTrue(r.valid, r.notes)

    def test_receipt_without_subtotal(self):
        rows = [("HUMMUS PIKANT", "2,75"), ("KOMKOMMER", "0,89"), ("Totaal", "€3,64")]
        r = parse_ocr(receipt_ocr(rows))
        self.assertTrue(r.valid, r.notes)
        self.assertIsNone(r.subtotal)

    def test_euro_sign_misread_in_total(self):
        rows = [("HUMMUS PIKANT", "2,75"), ("KOMKOMMER", "0,89"), ("Totaal", "13,64")]  # '€3,64'
        r = parse_ocr(receipt_ocr(rows))
        self.assertTrue(r.valid, r.notes)
        self.assertEqual(r.total, 3.64)

    def test_missing_euro_digit_recovered_from_total(self):
        rows = [("CITROEN VOORDEEL", ",59"), ("KOMKOMMER", "0,89"), ("Totaal", "€2,48")]
        r = parse_ocr(receipt_ocr(rows))
        self.assertTrue(r.valid, r.notes)
        self.assertEqual(r.items[0].net, 1.59)

    def test_wrong_reading_is_not_valid(self):
        rows = [("HUMMUS PIKANT", "2,75"), ("KOMKOMMER", "0,89"), ("Totaal", "€9,99")]
        self.assertFalse(parse_ocr(receipt_ocr(rows)).valid)


class TestClassifiers(unittest.TestCase):
    def test_total_vs_subtotal(self):
        self.assertTrue(is_total("Totaal"))
        self.assertTrue(is_total("lotaal"))
        self.assertFalse(is_total("Subtotaal"))
        self.assertFalse(is_total("DE TOTALE KORTING IS €2,48"))
        self.assertTrue(is_subtotal("subtotaal"))
        self.assertFalse(is_subtotal("Totaal"))

    def test_korting_is_mixed_case_only(self):
        self.assertTrue(is_korting("Korting"))
        self.assertTrue(is_korting("Kor ting"))
        self.assertTrue(is_korting("Konting bno 1"))
        self.assertFalse(is_korting("KONINGSZUIVEL KWARK"))
        self.assertFalse(is_korting("KOMKOMMER"))


class TestNameCorrection(unittest.TestCase):
    def setUp(self):
        self.vocab = build_vocabulary(["AH Hummus naturel"] * 3 + ["AH Komkommer"] * 5
                                      + ["AH Donkere chocolade"] * 4)

    def test_single_letter_misread_is_fixed(self):
        self.assertEqual(correct_ocr_name("NUMMUS PIKANT", self.vocab), "HUMMUS PIKANT")
        self.assertEqual(correct_ocr_name("KOMKUMMER", self.vocab), "KOMKOMMER")

    def test_spelling_variants_are_left_alone(self):
        self.assertEqual(correct_ocr_name("WALDKORN DONKER", self.vocab), "WALDKORN DONKER")


class TestLlmFallbackMapping(unittest.TestCase):
    def test_model_reading_is_validated_like_ocr(self):
        data = {"totale_korting": 2.48, "lines": [
            {"type": "item", "text": "MAALTIJDSALADE MANGO", "quantity": 1, "amount": 5.99},
            {"type": "item", "text": "EDAMAME", "quantity": 1, "amount": 2.45},
            {"type": "subtotaal", "text": "Subtotaal", "quantity": 1, "amount": 8.44},
            {"type": "punten", "text": "33X PLUSPunten DIGITAAL", "quantity": 33, "amount": 0.66},
            {"type": "actie", "text": "1X ACTIE MAALTIJDSALADE", "quantity": 1, "amount": -2.48},
            {"type": "totaal", "text": "Totaal", "quantity": 1, "amount": 6.62},
        ]}
        r = _to_result(data)
        self.assertTrue(r.valid, r.notes)
        _apply_actions(r)
        self.assertEqual(r.items[0].net, 3.51)

        data["lines"][1]["amount"] = 3.45  # a hallucinated amount must be rejected
        self.assertFalse(_to_result(data).valid)


if __name__ == "__main__":
    unittest.main()
