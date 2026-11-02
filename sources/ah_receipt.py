"""
Albert Heijn Kassabon (In-store Receipt) Parser.
Parses PDF receipts from AH physical stores.
"""

import re
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pdfplumber

from sources.base import ReceiptSource, ParsedReceipt, ParsedProduct
from database import calculate_file_hash


class AHReceiptSource(ReceiptSource):
    """Parser for Albert Heijn in-store receipts (kassabonnen)."""
    
    @property
    def name(self) -> str:
        return "ah_kassabon"
    
    @property
    def display_name(self) -> str:
        return "Albert Heijn Kassabon"
    
    @property
    def icon(self) -> str:
        return "🧾"
    
    @property
    def color(self) -> str:
        return "#00ADE6"  # AH Blue
    
    def can_parse(self, filepath: str) -> bool:
        """Check if this is an AH kassabon."""
        path = Path(filepath)
        
        if not path.suffix.lower() == '.pdf':
            return False
        
        # Kassabonnen have a specific filename pattern
        if path.name.startswith('AH_kassabon'):
            return True
        
        return False
    
    def parse(self, filepath: str) -> ParsedReceipt:
        """Parse an AH kassabon PDF."""
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
                
                # Extract bon number
                bon_match = re.search(r'^(\d{4})\s*$', raw_text, re.MULTILINE)
                if bon_match:
                    receipt.invoice_number = f"BON-{bon_match.group(1)}"
                
                # Extract date (format: 14:26 20-12-2025)
                date_match = re.search(r'(\d{1,2}:\d{2}\s+)?(\d{1,2})-(\d{1,2})-(\d{4})', raw_text)
                if date_match:
                    day = int(date_match.group(2))
                    month = int(date_match.group(3))
                    year = int(date_match.group(4))
                    receipt.date = datetime(year, month, day)
                
                # Extract total
                total_match = re.search(r'TOTAAL\s+(\d+[,\.]\d{2})', raw_text)
                if total_match:
                    receipt.total_amount = float(total_match.group(1).replace(',', '.'))
                
                # Extract savings
                voordeel_match = re.search(r'UW VOORDEEL\s+(\d+[,\.]\d{2})', raw_text)
                if voordeel_match:
                    receipt.total_savings = float(voordeel_match.group(1).replace(',', '.'))
                
                # Extract products
                receipt.products = self._extract_products(raw_text)
                
        except Exception as e:
            receipt.parse_notes = f"Parse error: {e}"
        
        return receipt
    
    def _extract_products(self, text: str) -> List[ParsedProduct]:
        """Extract products from kassabon text."""
        products = []
        lines = text.split('\n')
        
        stop_markers = ['SUBTOTAAL', 'BONUS ', 'UW VOORDEEL', 'TOTAAL', 'BETAALD', 'SPAARACTIES', 'KOOPZEGELS']
        
        for line in lines:
            line = line.strip()
            
            if not line or 'OMSCHRIJVING' in line or 'BONUSKAART' in line:
                continue
            
            # Stop at markers
            if any(marker in line.upper() for marker in stop_markers):
                break
            
            # Skip statiegeld
            if 'STATIEGELD' in line.upper():
                continue
            
            product = self._parse_product_line(line)
            if product:
                products.append(product)
        
        return products
    
    def _parse_product_line(self, line: str) -> Optional[ParsedProduct]:
        """Parse a single product line."""
        
        # Pattern 1: Weight product (e.g., "0.962KG TROSTOMAAT 2,38 2,29")
        weight_match = re.match(
            r'^(\d+[.,]\d+)KG\s+(.+?)\s+(\d+[,\.]\d{2})\s+(\d+[,\.]\d{2})(?:\s+B)?$',
            line, re.IGNORECASE
        )
        if weight_match:
            weight = float(weight_match.group(1).replace(',', '.'))
            name = weight_match.group(2).strip()
            total_price = float(weight_match.group(4).replace(',', '.'))
            
            name = self._normalize_name(name)
            category, subcategory = self._categorize(name)
            
            return ParsedProduct(
                name=f"{name} ({weight:.3f}kg)",
                original_name_raw=line,
                quantity=1,
                price=total_price,
                btw_percentage="9%",
                category=category,
                subcategory=subcategory,
                weight_kg=weight
            )
        
        # Pattern 2: Multi-item (e.g., "2 LEFFE BLOND 1,11 2,22 B")
        multi_match = re.match(
            r'^(\d+)\s+(.+?)\s+(\d+[,\.]\d{2})\s+(\d+[,\.]\d{2})(?:\s+B)?$',
            line
        )
        if multi_match:
            quantity = int(multi_match.group(1))
            name = multi_match.group(2).strip()
            total_price = float(multi_match.group(4).replace(',', '.'))
            
            name = re.sub(r'\s+B$', '', name)
            name = self._normalize_name(name)
            category, subcategory = self._categorize(name)
            
            return ParsedProduct(
                name=name,
                original_name_raw=line,
                quantity=quantity,
                price=total_price,
                btw_percentage=self._guess_btw(name),
                category=category,
                subcategory=subcategory
            )
        
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
            name = self._normalize_name(name)
            category, subcategory = self._categorize(name)
            
            return ParsedProduct(
                name=name,
                original_name_raw=line,
                quantity=quantity,
                price=price,
                btw_percentage=self._guess_btw(name),
                category=category,
                subcategory=subcategory
            )
        
        return None
    
    def _normalize_name(self, name: str) -> str:
        """Normalize product name."""
        name = name.strip()
        if name.upper().endswith(' BONUS'):
            name = name[:-6].strip()
        return name
    
    def _categorize(self, name: str) -> tuple:
        """Categorize product using the service."""
        try:
            from services.categorization import get_categorization_service
            service = get_categorization_service()
            result = service.categorize(name)
            return result['category'], result.get('subcategory')
        except:
            return "Overig", None
    
    def _guess_btw(self, name: str) -> str:
        """Guess BTW percentage based on product name."""
        name_lower = name.lower()
        
        btw_21_keywords = [
            'leffe', 'wijn', 'bier', 'whisky', 'vodka', 'rum', 'gin',
            'red bull', 'cola', 'fanta', 'sprite', 'pepsi',
            'statiegeld', 'plastic', 'verpakking',
            'shampoo', 'tandpasta', 'zeep', 'douche', 'deo',
            'schoonmaak', 'afwasmiddel'
        ]
        
        for keyword in btw_21_keywords:
            if keyword in name_lower:
                return "21%"
        
        return "9%"


