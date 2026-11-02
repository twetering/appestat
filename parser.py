"""
Unified PDF Parser for Albert Heijn invoices and kassabonnen
Handles both online invoices (1069835-xxx.pdf) and store receipts (AH_kassabon_xxx.pdf)
"""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pdfplumber

from database import (
    calculate_file_hash, get_all_product_names, invoice_exists, save_invoice, save_products
)


# ============================================================
# ABBREVIATION MAP for kassabon shortened names
# ============================================================

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


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def normalize_product_name(name: str) -> str:
    """
    Normaliseer productnaam:
    - Verwijder BONUS suffix
    - Trim whitespace
    """
    name = name.strip()
    
    if name.upper().endswith(' BONUS'):
        name = name[:-6].strip()
    
    return name


def expand_abbreviation(name: str) -> str:
    """
    Expand abbreviated product names from kassabonnen.
    Uses ABBREVIATION_MAP and tries partial matching.
    """
    name = name.strip()
    
    # Direct match in map
    if name.upper() in ABBREVIATION_MAP:
        return ABBREVIATION_MAP[name.upper()]
    
    # Try partial match
    for abbrev, full in ABBREVIATION_MAP.items():
        if name.upper().startswith(abbrev):
            return full + name[len(abbrev):]
    
    return name


def categorize_product(product_name: str) -> str:
    """Categorize a product using the categorization service."""
    try:
        from services.categorization import get_categorization_service
        service = get_categorization_service()
        result = service.categorize(product_name)
        return result['category']
    except Exception:
        return "Overig"


def determine_subcategory(product_name: str, category: str) -> Optional[str]:
    """Determine subcategory using the categorization service."""
    try:
        from services.categorization import get_categorization_service
        service = get_categorization_service()
        result = service.categorize(product_name)
        return result.get('subcategory')
    except Exception:
        return None


def guess_btw(product_name: str) -> str:
    """
    Guess BTW percentage based on product name.
    - 21%: Alcohol, sodas, non-food
    - 9%: Food, fresh produce
    """
    name_lower = product_name.lower()
    
    btw_21_keywords = [
        'leffe', 'wijn', 'bier', 'whisky', 'vodka', 'rum', 'gin',
        'red bull', 'cola', 'fanta', 'sprite', 'pepsi', '7up',
        'statiegeld', 'plastic', 'verpakking',
        'shampoo', 'tandpasta', 'zeep', 'douche', 'deo',
        'schoonmaak', 'afwasmiddel'
    ]
    
    for keyword in btw_21_keywords:
        # Match at word start, so 'cola' doesn't hit 'rucola' and 'gin' not 'aubergine'
        if re.search(r"\b" + re.escape(keyword), name_lower):
            return "21%"
    
    return "9%"


def parse_dutch_date(date_str: str) -> Optional[datetime]:
    """Parse Dutch date string (e.g., '15 januari 2024')"""
    dutch_months = {
        "januari": 1, "februari": 2, "maart": 3, "april": 4,
        "mei": 5, "juni": 6, "juli": 7, "augustus": 8,
        "september": 9, "oktober": 10, "november": 11, "december": 12
    }
    
    try:
        parts = date_str.split()
        if len(parts) == 3:
            day = int(parts[0])
            month = dutch_months.get(parts[1].lower(), 1)
            year = int(parts[2])
            return datetime(year, month, day)
    except:
        pass
    
    return None


# ============================================================
# PDF TYPE DETECTION
# ============================================================

def detect_pdf_type(text: str) -> str:
    """
    Detect the type of PDF based on content.
    Returns: 'online_invoice', 'kassabon', or 'unknown'
    """
    # Online invoices have these markers
    if 'Factuurnummer' in text and 'Debiteurnummer' in text:
        return 'online_invoice'
    
    # Kassabonnen have these markers
    if 'BONUSKAART' in text or 'OMSCHRIJVING' in text:
        return 'kassabon'
    
    # Additional check: kassabon format has 4-digit store number at top
    if re.match(r'^\d{4}\s*$', text.split('\n')[0].strip()):
        return 'kassabon'
    
    return 'unknown'


# ============================================================
# ONLINE INVOICE PARSER
# ============================================================

def parse_online_invoice(pdf_path: str) -> Optional[Dict]:
    """
    Parse an Albert Heijn online invoice PDF.
    Format: Full product names with BTW details.
    """
    file_hash = calculate_file_hash(pdf_path)
    
    result = {
        'file_hash': file_hash,
        'filename': os.path.basename(pdf_path),
        'invoice_number': None,
        'invoice_date': None,
        'total_amount': 0.0,
        'total_savings': 0.0,
        'products': [],
        'raw_text': '',
        'source_type': 'ah_online',
    }
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            raw_text = ""
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    raw_text += text + "\n"
            
            result['raw_text'] = raw_text
            
            # Extract invoice details
            date_match = re.search(r"Datum\s+(\d{1,2}\s+\w+\s+\d{4})", raw_text)
            if date_match:
                result['invoice_date'] = parse_dutch_date(date_match.group(1))
            
            inv_match = re.search(r"Factuurnummer\s+(\S+)", raw_text)
            if inv_match:
                result['invoice_number'] = inv_match.group(1)
            
            total_match = re.search(r"Totaal inclusief btw\s+([\d,]+)", raw_text)
            if total_match:
                result['total_amount'] = float(total_match.group(1).replace(",", "."))
            
            savings_match = re.search(r"Uw voordeel\s+([\d,]+)", raw_text)
            if savings_match:
                result['total_savings'] = float(savings_match.group(1).replace(",", "."))
            
            # Invoices before 1 August 2022 use a different column layout
            old_format = bool(result['invoice_date'] and result['invoice_date'] < datetime(2022, 8, 1))
            result['products'] = extract_products_from_online_invoice(raw_text, old_format=old_format)

    except Exception as e:
        print(f"Error parsing online invoice {pdf_path}: {e}")
        return None
    
    return result


def extract_products_from_online_invoice(text: str, old_format: bool = False) -> List[Dict]:
    """
    Extract products from online invoice text.
    Format: PRODUCT NAME    QTY  BTW%   EXCL    BTW    INCL
    Before 1 Aug 2022 (old_format): PRODUCT NAME    QTY  PER STUK  TOTAAL
    """
    products = []
    seen_products = set()

    name_part = r"^([A-Z][A-Za-z0-9àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞß'&\s\-\+\.%]+?)"
    if old_format:
        product_pattern = re.compile(name_part + r"\s+(\d+)\s+([\d,]+)\s+([\d,]+)$", re.MULTILINE)
        btw_group, price_group = None, 4
    else:
        product_pattern = re.compile(
            name_part + r"\s+(\d+)\s+(9%|21%|Geen)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)$", re.MULTILINE
        )
        btw_group, price_group = 3, 6

    skip_patterns = [
        "statiegeld", "retour", "klapkrat", "bezorgkosten", "verpakkingsmateriaal",
        "plastic verpakking", "koopzegels", "pagina ", "datum ", "factuurnummer",
        "debiteurnummer", "bestelling", "afleverdatum", "bezorgadres", "totaal",
        "boodschappen", "alle bedragen", "vragen over"
    ]
    
    for match in product_pattern.finditer(text):
        original_name = match.group(1).strip()
        quantity = int(match.group(2))
        btw = match.group(btw_group) if btw_group else None
        price_incl = float(match.group(price_group).replace(",", "."))
        
        name_lower = original_name.lower()
        if any(skip in name_lower for skip in skip_patterns):
            continue
        
        if len(original_name) < 3 or original_name.replace(" ", "").isdigit():
            continue
        
        display_name = normalize_product_name(original_name)
        
        product_key = f"{display_name}_{quantity}_{price_incl}"
        if product_key in seen_products:
            continue
        seen_products.add(product_key)
        
        category = categorize_product(display_name)
        subcategory = determine_subcategory(display_name, category)
        
        products.append({
            'name': display_name,
            'quantity': quantity,
            'btw': btw,
            'price': price_incl,
            'category': category,
            'subcategory': subcategory,
        })
    
    return products


# ============================================================
# KASSABON (STORE RECEIPT) PARSER
# ============================================================

def parse_kassabon(pdf_path: str) -> Optional[Dict]:
    """
    Parse an Albert Heijn kassabon (store receipt) PDF.
    Format: Abbreviated product names with simpler price structure.
    """
    file_hash = calculate_file_hash(pdf_path)
    
    result = {
        'file_hash': file_hash,
        'filename': os.path.basename(pdf_path),
        'invoice_number': None,
        'invoice_date': None,
        'total_amount': 0.0,
        'total_savings': 0.0,
        'products': [],
        'raw_text': '',
        'source_type': 'ah_kassabon',
    }
    
    all_text = []
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    all_text.append(text)
        
        raw_text = "\n".join(all_text)
        result['raw_text'] = raw_text
        
        # Extract store number (bonnummer - 4 digits at top)
        bon_match = re.search(r'^(\d{4})\s*$', raw_text, re.MULTILINE)
        if bon_match:
            result['invoice_number'] = f"BON-{bon_match.group(1)}"
        
        # Extract date (format: 14:26 20-12-2025 or dd-mm-yyyy)
        date_match = re.search(r'(\d{1,2}:\d{2}\s+)?(\d{1,2})-(\d{1,2})-(\d{4})', raw_text)
        if date_match:
            day, month, year = date_match.group(2), date_match.group(3), date_match.group(4)
            result['invoice_date'] = datetime(int(year), int(month), int(day))
        
        # Extract total amount
        total_match = re.search(r'TOTAAL\s+(\d+[,\.]\d{2})', raw_text)
        if total_match:
            result['total_amount'] = float(total_match.group(1).replace(',', '.'))
        
        # Extract savings
        voordeel_match = re.search(r'UW VOORDEEL\s+(\d+[,\.]\d{2})', raw_text)
        if voordeel_match:
            result['total_savings'] = float(voordeel_match.group(1).replace(',', '.'))
        
        result['products'] = extract_products_from_kassabon(raw_text)
        
    except Exception as e:
        print(f"Error parsing kassabon {pdf_path}: {e}")
        return None
    
    return result


def extract_products_from_kassabon(text: str) -> List[Dict]:
    """
    Extract products from kassabon text.
    
    Formats:
    1. Normal:     1 PAPRIKA GEEL 1,29
    2. Multi:      2 LEFFE BLOND 1,11 2,22 B
    3. Weight:     0.962KG TROSTOMAAT 2,38 2,29
    4. Deposit:    +STATIEGELD 0,30
    """
    products = []
    lines = text.split('\n')
    
    # Stop at these markers
    stop_markers = ['SUBTOTAAL', 'BONUS ', 'UW VOORDEEL', 'TOTAAL', 'BETAALD', 'SPAARACTIES', 'KOOPZEGELS']
    
    for line in lines:
        line = line.strip()
        
        # Skip empty lines and headers
        if not line or 'OMSCHRIJVING' in line or 'BONUSKAART' in line or 'AIRMILES' in line:
            continue
        
        # Stop at markers
        should_stop = False
        for marker in stop_markers:
            if marker in line.upper():
                should_stop = True
                break
        if should_stop:
            break
        
        # Skip deposit lines
        if line.startswith('+STATIEGELD') or 'STATIEGELD' in line.upper():
            continue
        
        product = parse_kassabon_line(line)
        if product:
            products.append(product)
    
    return products


def parse_kassabon_line(line: str) -> Optional[Dict]:
    """Parse a single product line from a kassabon."""
    
    # Pattern 1: Weight product (e.g., "0.962KG TROSTOMAAT 2,38 2,29")
    weight_match = re.match(
        r'^(\d+[.,]\d+)KG\s+(.+?)\s+(\d+[,\.]\d{2})\s+(\d+[,\.]\d{2})(?:\s+B)?$',
        line, re.IGNORECASE
    )
    if weight_match:
        weight = float(weight_match.group(1).replace(',', '.'))
        name = weight_match.group(2).strip()
        total_price = float(weight_match.group(4).replace(',', '.'))
        
        name = expand_abbreviation(name)
        name = normalize_product_name(name)
        
        category = categorize_product(name)
        subcategory = determine_subcategory(name, category)
        
        return {
            "name": f"{name} ({weight:.3f}kg)",
            "quantity": 1,
            "price": total_price,
            "btw": "9%",
            "category": category,
            "subcategory": subcategory,
            "weight_kg": weight
        }
    
    # Pattern 2: Multiple items (e.g., "2 LEFFE BLOND 1,11 2,22 B")
    multi_match = re.match(
        r'^(\d+)\s+(.+?)\s+(\d+[,\.]\d{2})\s+(\d+[,\.]\d{2})(?:\s+B)?$',
        line
    )
    if multi_match:
        quantity = int(multi_match.group(1))
        name = multi_match.group(2).strip()
        total_price = float(multi_match.group(4).replace(',', '.'))
        
        name = re.sub(r'\s+B$', '', name)
        name = expand_abbreviation(name)
        name = normalize_product_name(name)
        
        btw = guess_btw(name)
        category = categorize_product(name)
        subcategory = determine_subcategory(name, category)
        
        return {
            "name": name,
            "quantity": quantity,
            "price": total_price,
            "btw": btw,
            "category": category,
            "subcategory": subcategory
        }
    
    # Pattern 3: Single item (e.g., "1 PAPRIKA GEEL 1,29")
    single_match = re.match(
        r'^(\d+)\s+(.+?)\s+(\d+[,\.]\d{2})(?:\s+B)?$',
        line
    )
    if single_match:
        quantity = int(single_match.group(1))
        name = single_match.group(2).strip()
        price = float(single_match.group(3).replace(',', '.'))
        
        name = re.sub(r'\s+B$', '', name)
        name = expand_abbreviation(name)
        name = normalize_product_name(name)
        
        btw = guess_btw(name)
        category = categorize_product(name)
        subcategory = determine_subcategory(name, category)
        
        return {
            "name": name,
            "quantity": quantity,
            "price": price,
            "btw": btw,
            "category": category,
            "subcategory": subcategory
        }
    
    return None


# ============================================================
# UNIFIED IMPORT FUNCTIONS
# ============================================================

def parse_pdf(pdf_path: str) -> Optional[Dict]:
    """
    Parse any AH PDF (online invoice or kassabon).
    Auto-detects format and uses appropriate parser.
    """
    try:
        # Read first page to detect type
        with pdfplumber.open(pdf_path) as pdf:
            if not pdf.pages:
                return None
            text = pdf.pages[0].extract_text() or ""
        
        pdf_type = detect_pdf_type(text)
        
        if pdf_type == 'online_invoice':
            return parse_online_invoice(pdf_path)
        elif pdf_type == 'kassabon':
            return parse_kassabon(pdf_path)
        else:
            # Try both parsers, use the one that finds products
            online_result = parse_online_invoice(pdf_path)
            if online_result and len(online_result.get('products', [])) > 0:
                return online_result
            
            kassabon_result = parse_kassabon(pdf_path)
            if kassabon_result and len(kassabon_result.get('products', [])) > 0:
                return kassabon_result
            
            # Return whichever has a valid date
            if online_result and online_result.get('invoice_date'):
                return online_result
            if kassabon_result and kassabon_result.get('invoice_date'):
                return kassabon_result
            
            print(f"Unknown PDF format: {pdf_path}")
            return None
            
    except Exception as e:
        print(f"Error parsing {pdf_path}: {e}")
        return None


def parse_receipt_photo(image_path: str) -> Optional[Dict]:
    """
    Parse a photo of a PLUS kassabon (HEIC/JPG/PNG) into the same dict shape as parse_pdf.
    """
    from sources.plus_receipt import PlusReceiptSource, build_vocabulary, correct_ocr_name, display_name

    try:
        receipt = PlusReceiptSource().parse(image_path)
    except Exception as e:
        print(f"Error parsing photo {image_path}: {e}")
        return None

    # Repair OCR slips against names already in the database, and reuse an existing
    # spelling when a name only differs in spacing ("KAASVL INDERS" / "KAASVLINDERS")
    known_names = get_all_product_names()
    vocab = build_vocabulary(known_names)
    by_compact = {}
    for name in known_names:
        by_compact.setdefault(re.sub(r"[^A-Z0-9]", "", name.upper()), name)

    products = []
    for p in receipt.products:
        raw = correct_ocr_name(p.original_name_raw or p.name, vocab)
        known = by_compact.get(re.sub(r"[^A-Z0-9]", "", raw))
        if known and known.isupper():
            raw = known
        name = display_name(raw)
        category = categorize_product(name)
        products.append({
            'name': name,
            'original_name': raw,
            'quantity': p.quantity,
            'price': p.price,
            'btw': guess_btw(name),
            'category': category,
            'subcategory': determine_subcategory(name, category),
        })

    return {
        'file_hash': receipt.file_hash,
        'filename': receipt.filename,
        'invoice_number': receipt.invoice_number,
        'invoice_date': receipt.date,
        'total_amount': receipt.total_amount,
        'total_savings': receipt.total_savings,
        'products': products,
        'raw_text': receipt.raw_text,
        'source_type': receipt.source_type,
        'source_metadata': receipt.source_metadata,
        'parse_quality_score': receipt.parse_quality_score,
        'parse_notes': receipt.parse_notes,
    }


def import_invoice(filepath: str, force: bool = False) -> Dict:
    """
    Import a single invoice/receipt into the database.
    Returns status dict with success/error info.
    """
    from sources.plus_receipt import IMAGE_EXTENSIONS

    file_hash = calculate_file_hash(filepath)

    # Check if already exists (unless force)
    if not force and invoice_exists(file_hash):
        return {'status': 'skipped', 'reason': 'Already exists'}

    if Path(filepath).suffix.lower() in IMAGE_EXTENSIONS:
        result = parse_receipt_photo(filepath)
    else:
        result = parse_pdf(filepath)
    
    if result is None:
        return {'status': 'error', 'reason': 'Parse failed'}
    
    if not result.get('invoice_date'):
        return {'status': 'error', 'reason': 'No date found'}
    
    try:
        invoice_id = save_invoice(result)
        
        products_to_save = []
        for product in result['products']:
            products_to_save.append({
                'original_name': product.get('original_name', product['name']),
                'display_name': product['name'],
                'quantity': product['quantity'],
                'price': product['price'],
                'btw_percentage': product.get('btw', ''),
                'auto_category': product['category'],
                'auto_subcategory': product.get('subcategory'),
            })
        
        save_products(invoice_id, products_to_save)
        
        return {
            'status': 'success',
            'invoice_id': invoice_id,
            'products_count': len(products_to_save),
            'total_amount': result['total_amount'],
            'source_type': result.get('source_type', 'unknown'),
            'parse_quality_score': result.get('parse_quality_score'),
            'parse_notes': result.get('parse_notes'),
        }
        
    except Exception as e:
        return {'status': 'error', 'reason': str(e)}


def import_all_invoices(directory: str = "data/invoices", force: bool = False) -> Dict:
    """
    Import all PDF invoices and receipt photos from a directory and its subfolders
    (e.g. data/invoices/plus/ for photos of PLUS kassabonnen).
    Returns summary of import results.
    """
    from sources.plus_receipt import IMAGE_EXTENSIONS

    results = {
        'success': 0,
        'skipped': 0,
        'errors': 0,
        'needs_review': 0,
        'details': [],
        'total_amount': 0.0,
    }

    dir_path = Path(directory)
    if not dir_path.exists():
        print(f"Directory not found: {directory}")
        return results

    pdf_files = sorted(p for p in dir_path.rglob("*")
                       if p.is_file() and p.suffix.lower() in IMAGE_EXTENSIONS | {'.pdf'})
    total_files = len(pdf_files)

    for i, pdf_file in enumerate(pdf_files, 1):
        result = import_invoice(str(pdf_file), force=force)

        if result['status'] == 'success':
            results['success'] += 1
            results['total_amount'] += result.get('total_amount', 0)
            print(f"✓ [{i}/{total_files}] {pdf_file.name}: {result['products_count']} products, €{result.get('total_amount', 0):.2f}")
            if result.get('parse_quality_score') is not None and result['parse_quality_score'] < 1:
                # Imported, but the totals didn't add up - surface it instead of hiding it
                results['needs_review'] += 1
                results['details'].append({'file': pdf_file.name, 'warning': result.get('parse_notes')})
                print(f"  ⚠ {result.get('parse_notes')}")
        elif result['status'] == 'skipped':
            results['skipped'] += 1
            print(f"○ [{i}/{total_files}] {pdf_file.name}: Skipped (already exists)")
        else:
            results['errors'] += 1
            results['details'].append({
                'file': pdf_file.name,
                'error': result.get('reason', 'Unknown error')
            })
            print(f"✗ [{i}/{total_files}] {pdf_file.name}: {result.get('reason', 'Error')}")
    
    print(f"\n=== Import Summary ===")
    print(f"Success: {results['success']}")
    print(f"Skipped: {results['skipped']}")
    print(f"Errors: {results['errors']}")
    print(f"Needs review: {results['needs_review']}")
    print(f"Total imported: €{results['total_amount']:.2f}")
    
    return results


# For backwards compatibility with receipt_parser imports
def extract_products_from_text(text: str) -> List[Dict]:
    """Legacy function - use extract_products_from_online_invoice instead."""
    return extract_products_from_online_invoice(text)


if __name__ == "__main__":
    # Test the unified parser
    import sys
    
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
        result = parse_pdf(pdf_path)
        if result:
            print(f"\n=== {result['filename']} ===")
            print(f"Type: {result.get('source_type', 'unknown')}")
            print(f"Date: {result.get('invoice_date')}")
            print(f"Total: €{result.get('total_amount', 0):.2f}")
            print(f"Savings: €{result.get('total_savings', 0):.2f}")
            print(f"Products: {len(result.get('products', []))}")
            print("\nFirst 10 products:")
            for p in result.get('products', [])[:10]:
                print(f"  - {p['name']}: €{p['price']:.2f} [{p['category']}]")
        else:
            print("Failed to parse PDF")
    else:
        # Run import
        print("Running import from data/invoices...")
        results = import_all_invoices("data/invoices", force=False)
