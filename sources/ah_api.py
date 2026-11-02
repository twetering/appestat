"""
Albert Heijn API source for Appestat.
Fetches receipts directly from the AH API instead of parsing PDFs.
"""

import hashlib
from datetime import datetime
from typing import List, Optional

from sources.base import ParsedProduct, ParsedReceipt, ReceiptSource


class AHApiSource(ReceiptSource):
    """Source that fetches receipts from the Albert Heijn API."""

    @property
    def name(self) -> str:
        return "ah_api"

    @property
    def display_name(self) -> str:
        return "Albert Heijn App"

    @property
    def icon(self) -> str:
        return "🛒"

    @property
    def color(self) -> str:
        return "#00ADE6"

    def can_parse(self, filepath: str) -> bool:
        return False

    def parse(self, filepath: str) -> ParsedReceipt:
        raise NotImplementedError("AHApiSource is API-based; use receipt_to_parsed() instead")

    def supports_api(self) -> bool:
        return True

    def receipt_to_parsed(self, receipt_data: dict) -> ParsedReceipt:
        """Convert an AH API receipt dict to a ParsedReceipt."""
        receipt_id = receipt_data.get("id", "")

        # Deterministic hash so the same receipt is never imported twice
        file_hash = hashlib.md5(f"ah_api:{receipt_id}".encode()).hexdigest()

        # Parse timestamp
        transaction = receipt_data.get("transaction", {}) or {}
        dt_str = transaction.get("dateTime")
        if dt_str:
            date = datetime.fromisoformat(dt_str.replace("Z", "+00:00")).replace(tzinfo=None)
        else:
            date = datetime.utcnow()

        # Store info from address
        address = receipt_data.get("address", {}) or {}
        street = address.get("street", "")

        # Totals
        total_data = receipt_data.get("total", {}) or {}
        total_amount = total_data.get("amount", 0.0) if isinstance(total_data, dict) else 0.0

        discount_data = receipt_data.get("discountTotal", {}) or {}
        total_savings = abs(discount_data.get("amount", 0.0)) if isinstance(discount_data, dict) else 0.0

        # Payment method
        payments = receipt_data.get("payments", []) or []
        payment_method = payments[0].get("method") if payments else None

        products = self._parse_products(receipt_data.get("products", []) or [])

        return ParsedReceipt(
            file_hash=file_hash,
            filename=f"ah_api_{receipt_id}.json",
            source_type="ah_api",
            date=date,
            invoice_number=f"AH-{receipt_id[:8]}" if receipt_id else None,
            total_amount=total_amount,
            total_savings=total_savings,
            products=products,
            raw_text="",
            external_id=receipt_id,
            source_metadata={
                "api_receipt_id": receipt_id,
                "store_street": street,
                "store_city": address.get("city", ""),
                "store_postal_code": address.get("postalCode", ""),
                "payment_method": payment_method,
                "member_id": receipt_data.get("memberId"),
            },
            parse_quality_score=1.0,
        )

    def _parse_products(self, products_data: list) -> List[ParsedProduct]:
        from parser import categorize_product, determine_subcategory, guess_btw

        parsed = []
        for item in products_data:
            name = item.get("name", "Unknown")
            price_data = item.get("price", {}) or {}
            amount_data = item.get("amount", {}) or {}

            unit_price = price_data.get("amount", 0.0) if isinstance(price_data, dict) else 0.0
            line_total = amount_data.get("amount", 0.0) if isinstance(amount_data, dict) else 0.0
            raw_qty = item.get("quantity", 1)

            # AH returns quantity as float (e.g. 0.450 for weight items)
            try:
                qty_float = float(raw_qty)
                quantity = int(qty_float) if qty_float == int(qty_float) else 1
            except (TypeError, ValueError):
                quantity = 1

            price = line_total if line_total else unit_price
            category = categorize_product(name)
            subcategory = determine_subcategory(name, category)

            parsed.append(ParsedProduct(
                name=name,
                price=price,
                quantity=quantity,
                btw_percentage=guess_btw(name),
                category=category,
                subcategory=subcategory,
                unit_price=unit_price,
            ))
        return parsed
