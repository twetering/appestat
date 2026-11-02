"""
Albert Heijn Online Invoice Parser.
Parses PDF invoices from AH online orders.
"""

import re
from datetime import datetime
from pathlib import Path
from typing import List

import pdfplumber

from sources.base import ReceiptSource, ParsedReceipt, ParsedProduct
from database import calculate_file_hash


class AHOnlineSource(ReceiptSource):
    """Parser for Albert Heijn online order invoices (PDF)."""
    
    @property
    def name(self) -> str:
        return "ah_online"
    
    @property
    def display_name(self) -> str:
        return "Albert Heijn Online"
    
    @property
    def icon(self) -> str:
        return "🛒"
    
    @property
    def color(self) -> str:
        return "#00ADE6"  # AH Blue
    
    def can_parse(self, filepath: str) -> bool:
        """Check if this is an AH online invoice."""
        path = Path(filepath)
        
        if not path.suffix.lower() == '.pdf':
            return False
        
        # AH online invoices typically have numeric filenames like 1069835-00284.pdf
        if re.match(r'^\d{7}-\d{5}\.pdf$', path.name):
            return True
        
        # Or check content for AH markers
        try:
            with pdfplumber.open(filepath) as pdf:
                if pdf.pages:
                    text = pdf.pages[0].extract_text() or ""
                    if "Albert Heijn" in text and "Factuurnummer" in text:
                        return True
        except:
            pass
        
        return False
    
    def parse(self, filepath: str) -> ParsedReceipt:
        """Parse an AH online invoice PDF."""
        file_hash = calculate_file_hash(filepath)
        filename = Path(filepath).name
        
        receipt = ParsedReceipt(
            file_hash=file_hash,
            filename=filename,
            source_type=self.name
        )
        
        try:
            with pdfplumber.open(filepath) as pdf:
                raw_text = ""
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        raw_text += text + "\n"
                
                receipt.raw_text = raw_text
                
                # Extract date
                date_match = re.search(r"Datum\s+(\d{1,2}\s+\w+\s+\d{4})", raw_text)
                if date_match:
                    receipt.date = self._parse_dutch_date(date_match.group(1))
                
                # Extract invoice number
                inv_match = re.search(r"Factuurnummer\s+(\S+)", raw_text)
                if inv_match:
                    receipt.invoice_number = inv_match.group(1)
                
                # Extract total
                total_match = re.search(r"Totaal inclusief btw\s+([\d,]+)", raw_text)
                if total_match:
                    receipt.total_amount = float(total_match.group(1).replace(",", "."))
                
                # Extract savings
                savings_match = re.search(r"Uw voordeel\s+([\d,]+)", raw_text)
                if savings_match:
                    receipt.total_savings = float(savings_match.group(1).replace(",", "."))
                
                # Extract products
                receipt.products = self._extract_products(raw_text)
                
                # Calculate quality score
                if receipt.products:
                    receipt.parse_quality_score = min(1.0, len(receipt.products) / 20)
                
        except Exception as e:
            receipt.parse_notes = f"Parse error: {e}"
        
        return receipt
    
    def _parse_dutch_date(self, date_str: str) -> datetime:
        """Parse Dutch date string to datetime."""
        dutch_months = {
            "januari": 1, "februari": 2, "maart": 3, "april": 4,
            "mei": 5, "juni": 6, "juli": 7, "augustus": 8,
            "september": 9, "oktober": 10, "november": 11, "december": 12
        }
        
        parts = date_str.split()
        if len(parts) == 3:
            day = int(parts[0])
            month = dutch_months.get(parts[1].lower(), 1)
            year = int(parts[2])
            return datetime(year, month, day)
        
        return None
    
    def _extract_products(self, text: str) -> List[ParsedProduct]:
        """Extract products from invoice text."""
        products = []
        seen_products = set()
        
        # Pattern for product lines
        product_pattern = re.compile(
            r"^([A-Z][A-Za-z0-9àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿÀÁÂÃÄÅÆÇÈÉÊËÌÍÎÏÐÑÒÓÔÕÖØÙÚÛÜÝÞß'&\s\-\+\.%]+?)\s+(\d+)\s+(9%|21%|Geen)\s+([\d,]+)\s+([\d,]+)\s+([\d,]+)$",
            re.MULTILINE
        )
        
        skip_patterns = [
            "statiegeld", "retour", "klapkrat", "bezorgkosten", "verpakkingsmateriaal",
            "plastic verpakking", "koopzegels", "pagina ", "datum ", "factuurnummer",
            "debiteurnummer", "bestelling", "afleverdatum", "bezorgadres", "totaal",
            "boodschappen", "alle bedragen", "vragen over"
        ]
        
        for match in product_pattern.finditer(text):
            original_name = match.group(1).strip()
            quantity = int(match.group(2))
            btw = match.group(3)
            price_incl = float(match.group(6).replace(",", "."))
            
            name_lower = original_name.lower()
            if any(skip in name_lower for skip in skip_patterns):
                continue
            
            if len(original_name) < 3 or original_name.replace(" ", "").isdigit():
                continue
            
            # Normalize name
            display_name = self._normalize_name(original_name)
            
            product_key = f"{display_name}_{quantity}_{price_incl}"
            if product_key in seen_products:
                continue
            seen_products.add(product_key)
            
            # Categorize using service
            try:
                from services.categorization import get_categorization_service
                service = get_categorization_service()
                cat_result = service.categorize(original_name)
                category = cat_result['category']
                subcategory = cat_result.get('subcategory')
            except:
                category = "Overig"
                subcategory = None
            
            products.append(ParsedProduct(
                name=display_name,
                original_name_raw=original_name,
                quantity=quantity,
                btw_percentage=btw,
                price=price_incl,
                category=category,
                subcategory=subcategory
            ))
        
        return products
    
    def _normalize_name(self, name: str) -> str:
        """Normalize product name."""
        name = name.strip()
        if name.upper().endswith(' BONUS'):
            name = name[:-6].strip()
        return name


