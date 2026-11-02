"""
Source Registry for managing available receipt sources.
"""

from typing import Dict, List, Optional, Type
from pathlib import Path

from sources.base import ReceiptSource, ParsedReceipt


class SourceRegistry:
    """
    Registry for managing receipt sources.
    Supports auto-detection of source type based on file content.
    """
    
    def __init__(self):
        self._sources: Dict[str, ReceiptSource] = {}
    
    def register(self, source: ReceiptSource):
        """Register a new source."""
        self._sources[source.name] = source
    
    def get(self, name: str) -> Optional[ReceiptSource]:
        """Get a source by name."""
        return self._sources.get(name)
    
    def list_sources(self) -> List[ReceiptSource]:
        """Get all registered sources."""
        return list(self._sources.values())
    
    def detect_source(self, filepath: str) -> Optional[ReceiptSource]:
        """
        Auto-detect which source can parse the given file.
        Returns the first source that reports it can parse the file.
        """
        for source in self._sources.values():
            try:
                if source.can_parse(filepath):
                    return source
            except Exception:
                continue
        return None
    
    def parse_file(self, filepath: str, source_name: str = None) -> ParsedReceipt:
        """
        Parse a file using the specified or auto-detected source.
        
        Args:
            filepath: Path to file to parse
            source_name: Optional source name to use. If not provided, auto-detect.
            
        Returns:
            Parsed receipt data
            
        Raises:
            ValueError: If no suitable source found
        """
        if source_name:
            source = self.get(source_name)
            if not source:
                raise ValueError(f"Unknown source: {source_name}")
        else:
            source = self.detect_source(filepath)
            if not source:
                raise ValueError(f"No suitable source found for: {filepath}")
        
        return source.parse(filepath)


# Singleton registry instance
_registry = None


def get_registry() -> SourceRegistry:
    """Get the global source registry, initializing if needed."""
    global _registry
    
    if _registry is None:
        _registry = SourceRegistry()
        
        # Register built-in sources
        try:
            from sources.ah_online import AHOnlineSource
            _registry.register(AHOnlineSource())
        except ImportError:
            pass
        
        try:
            from sources.ah_receipt import AHReceiptSource
            _registry.register(AHReceiptSource())
        except ImportError:
            pass

        try:
            from sources.ah_api import AHApiSource
            _registry.register(AHApiSource())
        except ImportError:
            pass

        try:
            from sources.plus_receipt import PlusReceiptSource
            _registry.register(PlusReceiptSource())
        except ImportError:
            pass

    return _registry


