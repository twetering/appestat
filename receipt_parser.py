"""
Receipt Parser for Albert Heijn kassabonnen (in-store receipts)
Handles the different format compared to online invoices
"""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pdfplumber

from database import (
    calculate_file_hash, invoice_exists, save_invoice, save_products,
    get_connection
)
from parser import normalize_product_name, categorize_product, determine_subcategory, CATEGORY_KEYWORDS


# Mapping van afkortingen naar volledige productnamen
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


def expand_abbreviation(name: str) -> str:
    """
    Probeer afgekorte productnamen uit te breiden.
    Gebruikt de ABBREVIATION_MAP en probeert slimme uitbreiding.
    """
    name = name.strip()
    
    # Direct match in map
    if name.upper() in ABBREVIATION_MAP:
        return ABBREVIATION_MAP[name.upper()]
    
    # Probeer gedeeltelijke match
    for abbrev, full in ABBREVIATION_MAP.items():
        if name.upper().startswith(abbrev):
            return full + name[len(abbrev):]
    
    return name


def parse_receipt(pdf_path: str) -> Tuple[Dict, List[Dict], str]:
    """
    Parse een AH kassabon PDF en extract producten.
    Returns: (receipt_data, products, raw_text)
    """
    receipt_data = {
        "file_hash": calculate_file_hash(pdf_path),
        "filename": os.path.basename(pdf_path),
        "invoice_number": None,
        "date": None,
        "total": 0,
        "savings": 0,
        "receipt_type": "kassabon"  # Markeer als kassabon
    }
    
    products = []
    all_text = []
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                all_text.append(text)
    
    raw_text = "\n".join(all_text)
    
    # Extract bonnummer (staat vaak bovenaan)
    bon_match = re.search(r'^(\d{4})\s*$', raw_text, re.MULTILINE)
    if bon_match:
        receipt_data["invoice_number"] = f"BON-{bon_match.group(1)}"
    
    # Extract datum (formaat: 14:26 20-12-2025 of dd-mm-yyyy)
    date_match = re.search(r'(\d{1,2}:\d{2}\s+)?(\d{1,2})-(\d{1,2})-(\d{4})', raw_text)
    if date_match:
        day, month, year = date_match.group(2), date_match.group(3), date_match.group(4)
        receipt_data["date"] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    
    # Extract totaalbedrag
    total_match = re.search(r'TOTAAL\s+(\d+[,\.]\d{2})', raw_text)
    if total_match:
        receipt_data["total"] = float(total_match.group(1).replace(',', '.'))
    
    # Extract besparing/voordeel
    voordeel_match = re.search(r'UW VOORDEEL\s+(\d+[,\.]\d{2})', raw_text)
    if voordeel_match:
        receipt_data["savings"] = float(voordeel_match.group(1).replace(',', '.'))
    
    # Parse producten
    products = extract_products_from_receipt(raw_text)
    
    return receipt_data, products, raw_text


def extract_products_from_receipt(text: str) -> List[Dict]:
    """
    Extract producten uit kassabon tekst.
    
    Formaten:
    1. Normaal:     1 PAPRIKA GEEL 1,29
    2. Met bedrag:  2 LEFFE BLOND 1,11 2,22 B
    3. Gewicht:     0.962KG TROSTOMAAT 2,38 2,29
    4. Statiegeld:  +STATIEGELD 0,30
    """
    products = []
    lines = text.split('\n')
    
    # Stop bij deze markers (checken op 'in' niet 'startswith' voor meer flexibiliteit)
    stop_markers = ['SUBTOTAAL', 'BONUS ', 'UW VOORDEEL', 'TOTAAL', 'BETAALD', 'SPAARACTIES', 'KOOPZEGELS']
    
    for i, line in enumerate(lines):
        line = line.strip()
        
        # Skip lege regels en headers
        if not line or 'OMSCHRIJVING' in line or 'BONUSKAART' in line or 'AIRMILES' in line:
            continue
        
        # Stop bij markers - check of de regel eindigt met een marker-woord
        should_stop = False
        for marker in stop_markers:
            if marker in line.upper():
                should_stop = True
                break
        if should_stop:
            break
        
        # Skip statiegeld regels (worden apart verwerkt)
        if line.startswith('+STATIEGELD') or 'STATIEGELD' in line.upper():
            continue
        
        product = parse_product_line(line)
        if product:
            products.append(product)
    
    return products


def parse_product_line(line: str) -> Optional[Dict]:
    """
    Parse een enkele productregel van de kassabon.
    """
    # Patroon 1: Gewicht product (bijv. "0.962KG TROSTOMAAT 2,38 2,29")
    weight_match = re.match(
        r'^(\d+[.,]\d+)KG\s+(.+?)\s+(\d+[,\.]\d{2})\s+(\d+[,\.]\d{2})(?:\s+B)?$',
        line, re.IGNORECASE
    )
    if weight_match:
        weight = float(weight_match.group(1).replace(',', '.'))
        name = weight_match.group(2).strip()
        price_per_kg = float(weight_match.group(3).replace(',', '.'))
        total_price = float(weight_match.group(4).replace(',', '.'))
        
        name = expand_abbreviation(name)
        name = normalize_product_name(name)
        
        return {
            "name": f"{name} ({weight:.3f}kg)",
            "quantity": 1,
            "price": total_price,
            "btw": "9%",  # Vers is meestal 9%
            "category": categorize_product(name),
            "subcategory": determine_subcategory(name, categorize_product(name)),
            "weight_kg": weight
        }
    
    # Patroon 2: Normaal product met meerdere stuks (bijv. "2 LEFFE BLOND 1,11 2,22 B")
    multi_match = re.match(
        r'^(\d+)\s+(.+?)\s+(\d+[,\.]\d{2})\s+(\d+[,\.]\d{2})(?:\s+B)?$',
        line
    )
    if multi_match:
        quantity = int(multi_match.group(1))
        name = multi_match.group(2).strip()
        unit_price = float(multi_match.group(3).replace(',', '.'))
        total_price = float(multi_match.group(4).replace(',', '.'))
        
        # Verwijder eventuele trailing markers
        name = re.sub(r'\s+B$', '', name)
        name = expand_abbreviation(name)
        name = normalize_product_name(name)
        
        # Bepaal BTW (dranken meestal 21%, voedsel 9%)
        btw = guess_btw(name)
        
        return {
            "name": name,
            "quantity": quantity,
            "price": total_price,
            "btw": btw,
            "category": categorize_product(name),
            "subcategory": determine_subcategory(name, categorize_product(name))
        }
    
    # Patroon 3: Enkel product (bijv. "1 PAPRIKA GEEL 1,29")
    single_match = re.match(
        r'^(\d+)\s+(.+?)\s+(\d+[,\.]\d{2})(?:\s+B)?$',
        line
    )
    if single_match:
        quantity = int(single_match.group(1))
        name = single_match.group(2).strip()
        price = float(single_match.group(3).replace(',', '.'))
        
        # Verwijder eventuele trailing markers
        name = re.sub(r'\s+B$', '', name)
        name = expand_abbreviation(name)
        name = normalize_product_name(name)
        
        btw = guess_btw(name)
        
        return {
            "name": name,
            "quantity": quantity,
            "price": price,
            "btw": btw,
            "category": categorize_product(name),
            "subcategory": determine_subcategory(name, categorize_product(name))
        }

    return None


def guess_btw(product_name: str) -> str:
    """
    Raad het BTW-percentage op basis van productnaam.
    - 21%: Alcohol, frisdrank, non-food
    - 9%: Voedsel, vers
    """
    name_lower = product_name.lower()
    
    # 21% BTW producten
    btw_21_keywords = [
        'leffe', 'wijn', 'bier', 'whisky', 'vodka', 'rum', 'gin',
        'red bull', 'cola', 'fanta', 'sprite', 'pepsi', '7up',
        'statiegeld', 'plastic', 'verpakking',
        'shampoo', 'tandpasta', 'zeep', 'douche', 'deo',
        'schoonmaak', 'afwasmiddel'
    ]
    
    for keyword in btw_21_keywords:
        if keyword in name_lower:
            return "21%"
    
    # Default naar 9% voor voedsel
    return "9%"


def import_receipt(pdf_path: str, force: bool = False) -> Dict:
    """
    Import een kassabon in de database.
    """
    result = {
        "filename": os.path.basename(pdf_path),
        "status": "unknown",
        "message": "",
        "invoice_id": None,
        "products_count": 0
    }
    
    try:
        # Check voor duplicaat
        file_hash = calculate_file_hash(pdf_path)
        
        if invoice_exists(file_hash) and not force:
            result["status"] = "skipped"
            result["message"] = "Kassabon bestaat al in database"
            return result
        
        # Parse de kassabon
        receipt_data, products, raw_text = parse_receipt(pdf_path)
        receipt_data["raw_text"] = raw_text
        
        if not receipt_data["date"]:
            result["status"] = "error"
            result["message"] = "Kon geen datum uit kassabon halen"
            return result
        
        # Sla op in database (hergebruik invoice tabel)
        invoice_id = save_invoice(receipt_data)
        save_products(invoice_id, products)
        
        result["status"] = "success"
        result["message"] = f"Geïmporteerd: {len(products)} producten"
        result["invoice_id"] = invoice_id
        result["products_count"] = len(products)
        result["date"] = receipt_data["date"]
        result["total"] = receipt_data["total"]
        
    except Exception as e:
        result["status"] = "error"
        result["message"] = str(e)
    
    return result


def import_all_receipts(directory: str = "data/bonnen", force: bool = False) -> Dict:
    """
    Import alle kassabonnen uit een directory.
    """
    receipt_dir = Path(directory)
    
    if not receipt_dir.exists():
        return {
            "status": "error",
            "message": f"Directory {directory} bestaat niet",
            "results": []
        }
    
    results = []
    success_count = 0
    skip_count = 0
    error_count = 0
    
    for pdf_file in sorted(receipt_dir.glob("*.pdf")):
        result = import_receipt(str(pdf_file), force=force)
        results.append(result)
        
        if result["status"] == "success":
            success_count += 1
            print(f"✓ {result['filename']}: {result['products_count']} producten")
        elif result["status"] == "skipped":
            skip_count += 1
            print(f"○ {result['filename']}: Overgeslagen (bestaat al)")
        else:
            error_count += 1
            print(f"✗ {result['filename']}: {result['message']}")
    
    return {
        "status": "completed",
        "total": len(results),
        "success": success_count,
        "skipped": skip_count,
        "errors": error_count,
        "results": results
    }


def learn_abbreviation(short_name: str, full_name: str):
    """
    Voeg een nieuwe afkorting-mapping toe.
    Dit helpt bij het herkennen van afgekorte productnamen.
    """
    ABBREVIATION_MAP[short_name.upper()] = full_name


def test_receipt_parser(pdf_path: str):
    """
    Test de parser op een specifieke kassabon en toon resultaten.
    """
    print(f"=== Test kassabon parser: {os.path.basename(pdf_path)} ===")
    print()
    
    receipt_data, products, raw_text = parse_receipt(pdf_path)
    
    print(f"Bonnummer: {receipt_data['invoice_number']}")
    print(f"Datum: {receipt_data['date']}")
    print(f"Totaal: €{receipt_data['total']:.2f}")
    print(f"Bespaard: €{receipt_data['savings']:.2f}")
    print()
    
    print(f"=== {len(products)} Producten ===")
    total_parsed = 0
    for p in products:
        print(f"  {p['quantity']}× {p['name']}: €{p['price']:.2f} [{p['category']}]")
        total_parsed += p['price']
    
    print()
    print(f"Totaal geparsed: €{total_parsed:.2f}")
    print(f"Verschil met bon: €{receipt_data['total'] - total_parsed:.2f}")
    
    # Toon categorieverdeling
    print()
    print("=== Categorieën ===")
    by_cat = {}
    for p in products:
        cat = p['category']
        if cat not in by_cat:
            by_cat[cat] = 0
        by_cat[cat] += p['price']
    
    for cat, total in sorted(by_cat.items(), key=lambda x: -x[1]):
        print(f"  {cat}: €{total:.2f}")


if __name__ == "__main__":
    # Test de parser
    test_receipt_parser("data/bonnen/AH_kassabon_2025-12-20 142600_1177.pdf")

