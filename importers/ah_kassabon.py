"""
Albert Heijn store receipts (kassabon PDFs, 'AH_kassabon_2025-02-27 152400_8719.pdf').

    1 AH HAVERDR 1,39 B                 qty, name, amount, B = bonus item
    2 DRAAGTAS 0,79 1,58                qty, name, piece price, amount
    0.962KG TROSTOMAAT 2,38 2,29        weight, name, price per kg, amount
    +STATIEGELD 0,60
    34 SUBTOTAAL 93,62
    BONUS AHWITTEDRUIF -0,82            discounts (BONUS / BBOX)
    UW VOORDEEL 5,58
    Waarvan / BONUS BOX PREMIUM 0,18    breakdown of the discount above, informational
    SUBTOTAAL 88,04
    174 KOOPZEGELS PREMIUM 17,40        savings stamps
    TOTAAL 105,44

Checks: items + deposits == first SUBTOTAAL; discounts == UW VOORDEEL;
net products + extras == TOTAAL.
"""

import os
import re
from datetime import datetime
from typing import Dict

from importers.common import (
    apply_discount, classify_extra, make_extra, make_product, new_result, products_total,
    extras_total, reconcile, to_float,
)

ABBREVIATION_MAP = {
    # Groente & Fruit
    "SNOEP PAPRIK": "AH Snoeppaprika",
    "TROSTOMAAT": "AH Trostomaten",
    "BIO POMPOEN": "AH Biologische pompoen",
    "AH BANANEN": "AH Bananen",
    "AH RAUWKOST": "AH Rauwkostsalade",
    "PAPRIKA GEEL": "AH Paprika geel",
    "SNOEPGROENTE": "AH Snoepgroente",
    "MANDARIJNEN": "AH Mandarijnen",
    "DRUIVEN": "AH Druiven",
    "MU MILKSHAKE": "Melkunie Milkshake",
    "PETERSELIE": "AH Peterselie",
    "KNOFLOOK": "AH Knoflook",
    "KOMKOMMER": "AH Komkommer",
    # Zuivel
    "AH HV MELK": "AH Halfvolle melk",
    "ALPRO MILD&C": "Alpro Mild & Creamy yoghurt",
    "AH KR MUESLI": "AH Krokante muesli",
    "AH HAVERDRIN": "AH Haverdrink",
    "BECEL LIGHT": "Becel Light margarine",
    "AH KLEINTJE": "AH Kleintje vla",
    # Dranken
    "AH EXC WIJN": "AH Excellent wijn",
    "AH SAUV BL": "AH Sauvignon Blanc wijn",
    "LEFFE BLOND": "Leffe Blond bier",
    "LEFFE": "Leffe bier",
    "DE LUNGO": "Nespresso De Lungo koffie",
    "RED BULL": "Red Bull energy drink",
    # Brood & Bakkerij
    "AH PISTOLETS": "AH Pistoletbroodjes",
    "AH HAVERMOUT": "AH Havermout",
    "AH TAARTDEEG": "AH Taartdeeg",
    "MINI CRACKER": "AH Mini crackers",
    # Kaas
    "AH GOUDSE": "AH Goudse kaas",
    "GRUYERE": "AH Gruyère kaas",
    "PECORINO": "Pecorino kaas",
    # Snacks & Zoetwaren
    "AH RIJSTWAF": "AH Rijstwafels",
    "CHEESE BITES": "AH Cheese bites",
    "AH NOTEN": "AH Notenmix",
    "PECANNOTEN": "AH Pecannoten",
    "MUSKETBOMEN": "Musketbomen gebak",
    "AH HAGELSLAG": "AH Hagelslag chocolade",
    # Vlees
    "KIPGEHAKT": "AH Kipgehakt",
    "AH PANCETTA": "AH Pancetta spek",
    # Sauzen & Specerijen
    "AH CHUTNEY": "AH Chutney saus",
    "AH BOEMBOE": "AH Boemboe kruidenpasta",
    # Diepvries
    "PICARD": "Picard diepvriesmaaltijd",
}

AMOUNT = r"-?\d+,\d{2}"
# Trailing marker: "B" = bonus item; AH to go tills print a VAT letter ("2,50 A") instead
ITEM = re.compile(rf"^(?P<qty>\d+)\s+(?P<name>.+?)\s+(?P<a>{AMOUNT})(?:\s+(?P<b>{AMOUNT}))?\s*(?P<bonus>B*)(?:\s*[ACD])?$")
WEIGHED = re.compile(
    rf"^(?P<kg>\d+[.,]\d+)\s*KG\s+(?P<name>.+?)\s+(?P<perkg>{AMOUNT})\s+(?P<amount>{AMOUNT})\s*(?P<bonus>B*)$",
    re.IGNORECASE,
)
DEPOSIT = re.compile(rf"^\+\s*(?P<name>STATIEGELD.*?)\s+(?P<amount>{AMOUNT})$", re.IGNORECASE)
SUBTOTAL = re.compile(rf"^(?:\d+\s+)?SUBTOTAAL\s+(?P<amount>{AMOUNT})$")
DISCOUNT = re.compile(r"^(?P<label>(?:BONUS|BBOX|ACTIE)\b.*?)\s+(?P<amount>-\d+,\d{2})$")
SAVINGS = re.compile(rf"^(?:(?P<qty>\d+)\s+)?(?P<label>.*(?:KOOPZEGELS|SPAARZEGELS|to go sparen).*?)\s+(?P<amount>{AMOUNT})$",
                     re.IGNORECASE)
MILES = re.compile(r"^(?:\d+\s+)?(?P<label>MIJN AH MILES.*?)\s+(?P<amount>-\d+,\d{2})$")
TOTAL = re.compile(rf"^TOTAAL\s+(?P<amount>{AMOUNT})$")


def is_kassabon(text: str) -> bool:
    return "BONUSKAART" in text or "OMSCHRIJVING" in text


def expand_abbreviation(name: str) -> str:
    name = name.strip()
    if name.upper() in ABBREVIATION_MAP:
        return ABBREVIATION_MAP[name.upper()]
    for abbrev, full in ABBREVIATION_MAP.items():
        if name.upper().startswith(abbrev):
            return full + name[len(abbrev):]
    return name


def _store_name(lines, filename: str) -> str:
    """
    The header is free text per store ("Albert Heijn Doorn", "Openingstijden AH Neude:",
    "1054 JW Amsterdam"). Prefer "AH <place>", then the city after a postcode, else the
    store number from the file name.
    """
    header = []
    for line in lines:
        if "OMSCHRIJVING" in line:
            break
        header.append(line)
    for line in header:
        m = re.search(r"\b(?:albert heijn|ah)\s+((?:to go\s+)?[a-z][a-z' .-]+?):?$", line, re.IGNORECASE)
        if m and not re.search(r"\d", m.group(1)) and not re.search(r"\bapp\b", line, re.IGNORECASE):
            return f"Albert Heijn {m.group(1).strip().title()}"
    for line in header:
        m = re.search(r"\b\d{4}\s?[A-Z]{2}\b\s+([A-Za-z' .-]{3,})$", line)
        if m:
            return f"Albert Heijn {m.group(1).strip().title()}"
    m = re.search(r"_(\d{4})\.pdf$", filename)
    return f"Albert Heijn #{m.group(1)}" if m else "Albert Heijn"


def _parse_date(raw_text: str, filename: str):
    """'15:24 27-2-2025' at the bottom; else 'Datum 26/02/2024 16:06'; else the file name."""
    m = re.search(r"(\d{1,2}):(\d{2})\s+(\d{1,2})-(\d{1,2})-(\d{4})", raw_text)
    if m:
        return datetime(int(m.group(5)), int(m.group(4)), int(m.group(3)), int(m.group(1)), int(m.group(2)))
    m = re.search(r"Datum\s+(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})", raw_text)
    if m:
        return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)), int(m.group(4)), int(m.group(5)))
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})[ _](\d{2})(\d{2})", filename)
    if m:
        return datetime(*(int(g) for g in m.groups()))
    return None


def parse_text(raw_text: str, filename: str = "", file_hash: str = "") -> Dict:
    result = new_result("ah_kassabon", filename, file_hash)
    result["raw_text"] = raw_text
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    result["store"] = _store_name(lines, filename)

    result["invoice_date"] = _parse_date(raw_text, filename)
    m = re.search(r"Transaction\s+(\d+)", raw_text)
    result["invoice_number"] = f"KASSA-{m.group(1)}" if m else None

    subtotals, discounts, stated_savings, total = [], [], None, None
    miles_redeemed = 0.0
    section = "header"
    for line in lines:
        if section == "header":
            if "OMSCHRIJVING" in line:
                section = "items"
            continue

        sub = SUBTOTAL.match(line)
        if sub:
            subtotals.append(to_float(sub.group("amount")))
            section = "after"
            continue
        if line.upper().startswith("TOTAAL") and TOTAL.match(line):
            total = to_float(TOTAL.match(line).group("amount"))
            break

        if section == "items":
            if line.startswith(("BONUSKAART", "AIRMILES")):
                continue
            d = DEPOSIT.match(line)
            if d:
                result["extras"].append(make_extra("deposit", d.group("name").title(), to_float(d.group("amount"))))
                continue
            w = WEIGHED.match(line)
            if w:
                kg = to_float(w.group("kg"))
                name = expand_abbreviation(w.group("name"))
                result["products"].append(make_product(
                    name, 1, to_float(w.group("amount")), unit_price=to_float(w.group("perkg")),
                    weight_kg=kg, bonus=bool(w.group("bonus"))))
                continue
            it = ITEM.match(line)
            if it:
                qty = int(it.group("qty"))
                amount = to_float(it.group("b") or it.group("a"))
                unit = to_float(it.group("a")) if it.group("b") else round(amount / qty, 4)
                name = expand_abbreviation(it.group("name"))
                extra_type = classify_extra(name)
                if extra_type:
                    result["extras"].append(make_extra(extra_type, name.title(), amount, qty))
                else:
                    result["products"].append(make_product(name, qty, amount, unit_price=unit,
                                                           bonus=bool(it.group("bonus"))))
            continue

        # After the first subtotal: discounts, savings stamps, Airmiles
        if line.startswith("Waarvan"):
            section = "breakdown"  # "BONUS BOX PREMIUM 0,18" repeats the discount, not a new one
            continue
        if line.startswith("UW VOORDEEL"):
            stated_savings = to_float(line.split()[-1])
            continue
        dsc = DISCOUNT.match(line)
        if dsc and section == "after":
            discounts.append((dsc.group("label"), -to_float(dsc.group("amount"))))
            continue
        mi = MILES.match(line)
        if mi:
            # Redeemed Miles are saved-up credit being spent, not a product discount
            result["extras"].append(make_extra("savings", "AH Miles ingewisseld", to_float(mi.group("amount"))))
            miles_redeemed += -to_float(mi.group("amount"))
            continue
        sv = SAVINGS.match(line)
        if sv and to_float(sv.group("amount")):
            result["extras"].append(make_extra("savings", sv.group("label").strip().title(),
                                               to_float(sv.group("amount")), int(sv.group("qty") or 1)))

    gross_items = round(sum(p["gross"] for p in result["products"]), 2)
    item_extras = extras_total(result, ("deposit",))

    for label, amount in discounts:
        line = apply_discount(label, amount, result["products"], f"BONUSKORTING {label}")
        if line:
            result["products"].append(line)

    bonus_total = round(sum(a for _, a in discounts), 2)
    result["total_amount"] = total or 0.0
    result["total_savings"] = bonus_total
    if stated_savings is not None:
        stated_savings = round(stated_savings - miles_redeemed, 2)  # AH counts redeemed Miles as "voordeel"

    checks = []
    if subtotals:
        checks.append(("subtotaal", round(gross_items + item_extras, 2), subtotals[0]))
    if stated_savings is not None:
        checks.append(("uw voordeel", bonus_total, stated_savings))
    checks.append(("totaal", round(products_total(result) + extras_total(result), 2), total))
    reconcile(result, checks)
    return result


def parse_file(pdf_path: str, file_hash: str) -> Dict:
    import pdfplumber

    with pdfplumber.open(pdf_path) as pdf:
        raw_text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    return parse_text(raw_text, os.path.basename(pdf_path), file_hash)
