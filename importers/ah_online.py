"""
Albert Heijn online order invoices (PDF, '1069835-00414.pdf').

Page 1 holds a summary ("Boodschappen, zie specificatie" per VAT rate plus deposits,
koopzegels, bezorgkosten); the following pages list every line with quantity and VAT.
We read every specification line, split products from extras, and check:
    products == sum of "Boodschappen, zie specificatie"
    products + extras == "Totaal inclusief btw"
"""

import os
import re
from datetime import datetime
from typing import Dict, Optional

from importers.common import (
    classify_extra, make_extra, make_product, new_result, products_total, extras_total,
    reconcile, to_float,
)

AMOUNT = r"-?[\d.]*\d,\d{2}"
SPEC_LINE = re.compile(
    rf"^(?P<name>\S.*?)\s+(?P<qty>\d+)\s+(?P<btw>9%|21%|Geen)\s+(?P<ex>{AMOUNT})\s+(?P<vat>{AMOUNT})\s+(?P<inc>{AMOUNT})$"
)
SUMMARY_LINE = re.compile(
    rf"^(?P<desc>\S.*?)\s+(?P<btw>9%|21%|Geen)\s+(?P<ex>{AMOUNT})\s+(?P<vat>{AMOUNT})\s+(?P<inc>{AMOUNT})$"
)
WRAPPED_TAIL = re.compile(
    rf"^(?P<qty>\d+)\s+(?P<btw>9%|21%|Geen)\s+(?P<ex>{AMOUNT})\s+(?P<vat>{AMOUNT})\s+(?P<inc>{AMOUNT})$"
)
# Before 1 Aug 2022 invoices listed: name, quantity, price per piece, total
OLD_SPEC_LINE = re.compile(rf"^(?P<name>\S.*?)\s+(?P<qty>\d+)\s+(?P<unit>{AMOUNT})\s+(?P<inc>{AMOUNT})$")

DUTCH_MONTHS = {
    "januari": 1, "februari": 2, "maart": 3, "april": 4, "mei": 5, "juni": 6, "juli": 7,
    "augustus": 8, "september": 9, "oktober": 10, "november": 11, "december": 12,
}

NOT_A_LINE = ("totaal", "pagina ", "datum ", "factuurnummer", "debiteurnummer", "uw bestelling",
              "afleverdatum", "bezorgadres", "omschrijving", "alle bedragen", "vragen over")


def is_online_invoice(text: str) -> bool:
    return "Factuurnummer" in text and "Debiteurnummer" in text


def _clean_name(name: str) -> (str, bool):
    """'AH Groene kiwi¿s BONUS' -> ("AH Groene kiwi's", True)."""
    name = name.replace("¿", "'").replace("’", "'").strip()
    bonus = False
    if name.upper().endswith(" BONUS"):
        name, bonus = name[:-6].strip(), True
    return name, bonus


def _parse_date(text: str) -> Optional[datetime]:
    m = re.search(r"Datum\s+(\d{1,2})\s+(\w+)\s+(\d{4})", text)
    if not m or m.group(2).lower() not in DUTCH_MONTHS:
        return None
    return datetime(int(m.group(3)), DUTCH_MONTHS[m.group(2).lower()], int(m.group(1)))


def parse_text(raw_text: str, filename: str = "", file_hash: str = "") -> Dict:
    result = new_result("ah_online", filename, file_hash)
    result["store"] = "Albert Heijn"
    result["raw_text"] = raw_text

    result["invoice_date"] = _parse_date(raw_text)
    m = re.search(r"Factuurnummer\s+(\S+)", raw_text)
    result["invoice_number"] = m.group(1) if m else None
    m = re.search(rf"Totaal inclusief btw\s+({AMOUNT})", raw_text)
    total = to_float(m.group(1)) if m else None
    result["total_amount"] = total or 0.0
    m = re.search(rf"Uw voordeel\s+({AMOUNT})", raw_text)
    result["total_savings"] = to_float(m.group(1)) if m else 0.0

    lines = [l.strip() for l in raw_text.splitlines()]

    # Summary block on page 1: sum of groceries per VAT rate (the check value)
    expected_products = None
    in_summary = False
    for line in lines:
        if line.startswith("Omschrijving Btw"):
            in_summary = True
            continue
        if in_summary and line.startswith("Alle bedragen"):
            break
        if in_summary:
            s = SUMMARY_LINE.match(line)
            if s and s.group("desc").startswith("Boodschappen"):
                expected_products = round((expected_products or 0) + to_float(s.group("inc")), 2)

    old_format = bool(result["invoice_date"] and result["invoice_date"] < datetime(2022, 8, 1))
    spec_started = False
    pending_name = None  # long names wrap: the amounts follow on the next line
    for line in lines:
        if line.startswith("Specificatie"):
            spec_started = True
            continue
        if not spec_started or line.lower().startswith(NOT_A_LINE):
            pending_name = None
            continue
        if old_format:
            o = OLD_SPEC_LINE.match(line)
            if not o:
                continue
            name, qty, amount, btw = o.group("name"), int(o.group("qty")), to_float(o.group("inc")), None
        else:
            s = SPEC_LINE.match(line)
            wrapped = WRAPPED_TAIL.match(line)
            if wrapped and pending_name:
                name, qty, amount, btw = (pending_name, int(wrapped.group("qty")),
                                          to_float(wrapped.group("inc")), wrapped.group("btw"))
            elif s:
                name, qty, amount, btw = s.group("name"), int(s.group("qty")), to_float(s.group("inc")), s.group("btw")
            else:
                n = SUMMARY_LINE.match(line)
                if n and classify_extra(n.group("desc")):
                    # "Bezorgkosten 21% 2,99 0,63 3,62" has no quantity column
                    result["extras"].append(make_extra(classify_extra(n.group("desc")), n.group("desc"),
                                                       to_float(n.group("inc"))))
                elif n:
                    # Credited product without quantity: "AH Halfvolle melk 9% -1,55 -0,14 -1,69"
                    clean, _ = _clean_name(n.group("desc"))
                    credit = make_product(clean, 1, to_float(n.group("inc")), btw=n.group("btw"),
                                          original_name=clean)
                    credit["is_credit"] = True
                    result["products"].append(credit)
                pending_name = line if not re.search(r"\d,\d{2}$", line) else None
                continue
            pending_name = None

        extra_type = classify_extra(name)
        if extra_type:
            result["extras"].append(make_extra(extra_type, name.lstrip("+ "), amount, qty))
            continue
        clean, bonus = _clean_name(name)
        result["products"].append(make_product(clean, qty, amount, btw=btw, bonus=bonus,
                                               original_name=clean))

    checks = []
    if expected_products is not None:
        # AH counts subscriptions (bezorgbundel) as "Boodschappen" in its summary, but not
        # credited products (they only show up in the grand total)
        credits = sum(p["price"] for p in result["products"] if p.get("is_credit"))
        groceries = products_total(result) - credits + extras_total(result, ("subscription",))
        checks.append(("boodschappen", round(groceries, 2), expected_products))
    checks.append(("totaal", round(products_total(result) + extras_total(result), 2), total))
    reconcile(result, checks)
    return result


def parse_file(pdf_path: str, file_hash: str) -> Dict:
    import pdfplumber

    with pdfplumber.open(pdf_path) as pdf:
        raw_text = "".join((page.extract_text() or "") + "\n" for page in pdf.pages)
    return parse_text(raw_text, os.path.basename(pdf_path), file_hash)
