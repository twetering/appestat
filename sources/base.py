"""
Base classes for receipt/invoice sources.
Defines the interface that all source parsers must implement.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from pathlib import Path


@dataclass
class ParsedProduct:
    """Represents a parsed product from a receipt."""
    name: str
    price: float
    quantity: int = 1
    btw_percentage: str = ""
    category: Optional[str] = None
    subcategory: Optional[str] = None
    original_name_raw: Optional[str] = None
    weight_kg: Optional[float] = None
    unit_price: Optional[float] = None
    is_discount: bool = False
    discount_amount: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ParsedReceipt:
    """Represents a parsed receipt/invoice."""
    # Required fields
    file_hash: str
    filename: str
    source_type: str
    
    # Receipt details
    date: Optional[datetime] = None
    invoice_number: Optional[str] = None
    total_amount: float = 0.0
    total_savings: float = 0.0
    
    # Products
    products: List[ParsedProduct] = field(default_factory=list)
    
    # Raw data
    raw_text: str = ""
    
    # Metadata
    external_id: Optional[str] = None  # For API-based sources
    source_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Quality metrics
    parse_quality_score: Optional[float] = None
    parse_notes: Optional[str] = None


class ReceiptSource(ABC):
    """
    Abstract base class for receipt/invoice sources.
    
    Each source (AH Online, AH Kassabon, Picnic, etc.) should implement this interface.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier for this source (e.g., 'ah_online', 'picnic')"""
        pass
    
    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable name for display in UI"""
        pass
    
    @property
    def icon(self) -> str:
        """Emoji or icon for this source"""
        return "📄"
    
    @property
    def color(self) -> str:
        """Brand color for this source"""
        return "#9E9E9E"
    
    @abstractmethod
    def can_parse(self, filepath: str) -> bool:
        """
        Check if this source can parse the given file.
        Used for auto-detection of source type.
        
        Args:
            filepath: Path to the file to check
            
        Returns:
            True if this source can parse the file
        """
        pass
    
    @abstractmethod
    def parse(self, filepath: str) -> ParsedReceipt:
        """
        Parse a file and return structured receipt data.
        
        Args:
            filepath: Path to the file to parse
            
        Returns:
            ParsedReceipt with all extracted data
        """
        pass
    
    def supports_api(self) -> bool:
        """Whether this source supports API-based import (vs file-based)"""
        return False
    
    async def fetch_from_api(self, **kwargs) -> List[ParsedReceipt]:
        """
        Fetch receipts from an API.
        Only applicable for API-based sources like Picnic.
        
        Returns:
            List of parsed receipts from API
        """
        raise NotImplementedError("This source doesn't support API fetching")


