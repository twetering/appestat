"""
================================================================================
LLM Categorizer Service for Appestat
================================================================================

A robust, cost-efficient LLM-based product categorization system using OpenAI.

ARCHITECTURE OVERVIEW
---------------------
This service implements a 3-tier categorization approach:
1. User Overrides (highest priority) - learned from user corrections
2. Keyword Matching (fast, free) - database-driven keyword rules  
3. LLM Fallback (intelligent, paid) - OpenAI GPT for unknown products

COST EFFICIENCY
---------------
- Batches up to 100 products per API call
- Uses GPT-4o-mini by default (cheapest capable model)
- Tracks token usage and estimated costs
- Caches results to avoid re-categorizing known products

AGENT-NATIVE DESIGN
-------------------
This module follows agent-native architecture principles:
- Atomic tools: categorize_single, categorize_batch, categorize_all_uncategorized
- Composable: agents can combine these tools flexibly
- Observable: detailed logging and cost tracking for agent introspection
- Recoverable: retry logic and graceful degradation

USER STORIES
------------
US-001: As a user, I want products to be automatically categorized so I don't 
        have to manually assign categories to each item.
        
US-002: As a user, I want categorization to be accurate for Dutch supermarket 
        products including regional brands and specialty items.
        
US-003: As a power user, I want to see how much the AI categorization costs
        so I can decide whether to use it.
        
US-004: As an AI agent, I want to be able to trigger batch categorization 
        efficiently without making redundant API calls.

REQUIREMENTS
------------
REQ-001: Batch size must be configurable, default 100 products per API call
REQ-002: Must support both category and subcategory assignment
REQ-003: Must track token usage and provide cost estimates
REQ-004: Must have retry logic with exponential backoff
REQ-005: Must validate category names against database before assignment
REQ-006: Must learn from successful categorizations (save as keywords)
REQ-007: Must gracefully degrade to "Overig" if categorization fails
"""

import os
import json
import time
import asyncio
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
from pathlib import Path
from dataclasses import dataclass, field
from enum import Enum

# Load environment variables
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection


class CostTier(Enum):
    """Model cost tiers for budget awareness."""
    CHEAP = "cheap"      # gpt-4o-mini
    STANDARD = "standard"  # gpt-4o
    PREMIUM = "premium"   # gpt-4-turbo


@dataclass
class CategorizationResult:
    """Result of a single product categorization."""
    product_name: str
    category: str
    subcategory: Optional[str] = None
    confidence: float = 0.0
    source: str = "llm"
    error: Optional[str] = None


@dataclass  
class BatchResult:
    """Result of a batch categorization operation."""
    results: Dict[str, CategorizationResult] = field(default_factory=dict)
    total_products: int = 0
    successful: int = 0
    failed: int = 0
    tokens_used: int = 0
    estimated_cost_usd: float = 0.0
    duration_seconds: float = 0.0
    model_used: str = ""


@dataclass
class CostTracker:
    """Tracks cumulative costs across categorization operations."""
    total_tokens: int = 0
    total_cost_usd: float = 0.0
    total_products_processed: int = 0
    total_api_calls: int = 0
    last_updated: Optional[datetime] = None
    
    def add(self, tokens: int, cost: float, products: int, calls: int = 1):
        self.total_tokens += tokens
        self.total_cost_usd += cost
        self.total_products_processed += products
        self.total_api_calls += calls
        self.last_updated = datetime.now()
    
    def to_dict(self) -> Dict:
        return {
            "total_tokens": self.total_tokens,
            "total_cost_usd": round(self.total_cost_usd, 4),
            "total_products_processed": self.total_products_processed,
            "total_api_calls": self.total_api_calls,
            "cost_per_product": round(self.total_cost_usd / max(1, self.total_products_processed), 6),
            "last_updated": self.last_updated.isoformat() if self.last_updated else None
        }


class LLMCategorizer:
    """
    Robust LLM-based product categorizer for Dutch supermarket products.
    
    Features:
    - Batch processing (up to 100 products per API call)
    - Subcategory support
    - Cost tracking
    - Retry logic with exponential backoff
    - Category validation against database
    - Learning from corrections
    
    Usage:
        categorizer = LLMCategorizer()
        
        # Single product
        result = await categorizer.categorize_single("AH Melk Halfvol")
        
        # Batch of products
        batch_result = await categorizer.categorize_batch(["Product A", "Product B"])
        
        # All uncategorized products
        batch_result = await categorizer.categorize_all_uncategorized(limit=100)
        
        # Check costs
        costs = categorizer.get_cost_summary()
    """
    
    # Model pricing per 1M tokens (as of Jan 2026)
    MODEL_PRICING = {
        "gpt-4o-mini": {"input": 0.15, "output": 0.60},
        "gpt-4o": {"input": 2.50, "output": 10.00},
        "gpt-4-turbo": {"input": 10.00, "output": 30.00},
    }
    
    DEFAULT_MODEL = "gpt-4o-mini"  # Most cost-effective for this task
    MAX_BATCH_SIZE = 100
    MAX_RETRIES = 3
    PROMPT_VERSION = "2026-02-06-v1"
    
    def __init__(self, model: str = None):
        """Initialize the LLM categorizer."""
        self.model = model or self.DEFAULT_MODEL
        self.cost_tracker = CostTracker()
        self._categories_cache: Optional[Dict] = None
        self._client = None
        
    def _get_client(self):
        """Get or create OpenAI client."""
        if self._client is None:
            import openai
            api_key = os.getenv('OPENAI_API_KEY')
            if not api_key:
                raise ValueError("OPENAI_API_KEY environment variable not set")
            self._client = openai.OpenAI(api_key=api_key)
        return self._client
    
    def _load_categories(self, force_refresh: bool = False) -> Dict[str, Dict]:
        """Load categories and subcategories from database."""
        if self._categories_cache and not force_refresh:
            return self._categories_cache
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Load categories
        cursor.execute("""
            SELECT id, name, icon, color FROM categories ORDER BY sort_order
        """)
        categories = {}
        for row in cursor.fetchall():
            categories[row['name']] = {
                "id": row['id'],
                "icon": row['icon'],
                "color": row['color'],
                "subcategories": []
            }
        
        # Load subcategories
        cursor.execute("""
            SELECT s.id, s.name, s.category_id, c.name as category_name
            FROM subcategories s
            JOIN categories c ON s.category_id = c.id
            ORDER BY s.sort_order
        """)
        for row in cursor.fetchall():
            if row['category_name'] in categories:
                categories[row['category_name']]['subcategories'].append({
                    "id": row['id'],
                    "name": row['name']
                })
        
        conn.close()
        self._categories_cache = categories
        return categories
    
    def _build_system_prompt(self, categories: Dict) -> str:
        """Build a compact, token-efficient system prompt."""
        taxonomy_lines = []
        for name, data in categories.items():
            cat_id = data['id']
            subcats = data.get('subcategories', [])
            if subcats:
                subparts = [f"S{sc['id']} {sc['name']}" for sc in subcats]
                taxonomy_lines.append(f"C{cat_id} {name} | " + "; ".join(subparts))
            else:
                taxonomy_lines.append(f"C{cat_id} {name}")
        
        return (
            "Je categoriseert Nederlandse supermarktproducten. Gebruik alleen de IDs hieronder.\n"
            "Taxonomie (categorieen en subcategorieen):\n"
            + "\n".join(taxonomy_lines)
            + "\nREGELS:\n"
            "1. Gebruik ALLEEN C<id> en S<id>\n"
            "2. Kies de meest specifieke subcategorie indien beschikbaar\n"
            "3. 'Overig' alleen als het nergens past\n"
            "4. BONUS suffix negeren\n"
            "Antwoord uitsluitend met JSON."
        )
    
    def _build_user_prompt(self, products: List[str]) -> str:
        """Build the user prompt for a batch of products."""
        products_json = json.dumps(products, ensure_ascii=False)
        return (
            f"Producten ({len(products)}):\n{products_json}\n\n"
            "Antwoord met JSON in dit format:\n"
            "{\n"
            '  \"productnaam\": {\"category_id\": \"C<id>\", \"subcategory_id\": \"S<id> of null\"},\n'
            "  ...\n"
            "}\n"
            "Geen extra tekst."
        )

    def _build_category_indexes(self, categories: Dict) -> Dict[str, Dict]:
        """Build helper indexes for category/subcategory lookup."""
        category_by_id = {}
        category_by_name = {}
        subcategory_by_id = {}
        subcategory_parent = {}
        
        for name, data in categories.items():
            category_by_id[data['id']] = name
            category_by_name[name] = data['id']
            for sub in data.get('subcategories', []):
                subcategory_by_id[sub['id']] = sub['name']
                subcategory_parent[sub['id']] = name
        
        return {
            "category_by_id": category_by_id,
            "category_by_name": category_by_name,
            "subcategory_by_id": subcategory_by_id,
            "subcategory_parent": subcategory_parent
        }

    def _resolve_category(self, value: Any, indexes: Dict) -> str:
        """Resolve category value to a valid category name."""
        if value is None:
            return "Overig"
        
        if isinstance(value, str):
            value = value.strip()
            if value.upper().startswith("C") and value[1:].isdigit():
                value = int(value[1:])
            elif value.isdigit():
                value = int(value)
            elif value in indexes["category_by_name"]:
                return value
        
        if isinstance(value, int) and value in indexes["category_by_id"]:
            return indexes["category_by_id"][value]
        
        return "Overig"

    def _resolve_subcategory(self, value: Any, category_name: str, indexes: Dict) -> Optional[str]:
        """Resolve subcategory value to a valid subcategory name under category."""
        if value is None:
            return None
        
        subcategory_name = None
        
        if isinstance(value, str):
            value = value.strip()
            if value.upper().startswith("S") and value[1:].isdigit():
                value = int(value[1:])
            elif value.isdigit():
                value = int(value)
            elif value in indexes["subcategory_by_id"].values():
                subcategory_name = value
        
        if isinstance(value, int):
            subcategory_name = indexes["subcategory_by_id"].get(value)
        
        if not subcategory_name:
            return None
        
        # Ensure subcategory belongs to chosen category
        for sub_id, sub_name in indexes["subcategory_by_id"].items():
            if sub_name == subcategory_name:
                if indexes["subcategory_parent"].get(sub_id) == category_name:
                    return subcategory_name
        
        return None
    
    def _estimate_cost(self, input_tokens: int, output_tokens: int) -> float:
        """Estimate cost in USD for token usage."""
        pricing = self.MODEL_PRICING.get(self.model, self.MODEL_PRICING["gpt-4o-mini"])
        input_cost = (input_tokens / 1_000_000) * pricing["input"]
        output_cost = (output_tokens / 1_000_000) * pricing["output"]
        return input_cost + output_cost
    
    async def categorize_single(self, product_name: str) -> CategorizationResult:
        """
        Categorize a single product.
        
        Note: For efficiency, prefer using categorize_batch for multiple products.
        """
        batch_result = await self.categorize_batch([product_name])
        return batch_result.results.get(product_name, CategorizationResult(
            product_name=product_name,
            category="Overig",
            error="Categorization failed"
        ))
    
    async def categorize_batch(
        self, 
        products: List[str],
        save_as_keywords: bool = True,
        apply_to_database: bool = True
    ) -> BatchResult:
        """
        Categorize a batch of products using LLM.
        
        Args:
            products: List of product names to categorize
            save_as_keywords: If True, save successful categorizations as keywords
            apply_to_database: If True, update products in database with new categories
            
        Returns:
            BatchResult with categorization results and cost info
        """
        start_time = time.time()
        result = BatchResult(total_products=len(products), model_used=self.model)
        
        if not products:
            return result
        
        categories = self._load_categories()
        client = self._get_client()
        
        # Process in batches of MAX_BATCH_SIZE
        all_results = {}
        
        for i in range(0, len(products), self.MAX_BATCH_SIZE):
            batch = products[i:i + self.MAX_BATCH_SIZE]
            batch_results, usage = await self._process_batch(client, batch, categories)
            all_results.update(batch_results)
            result.tokens_used += usage.get("total_tokens", 0)
            result.estimated_cost_usd += usage.get("cost", 0.0)
        
        # Process results
        for product_name, cat_result in all_results.items():
            result.results[product_name] = cat_result
            if cat_result.error:
                result.failed += 1
            else:
                result.successful += 1
                
                # Save as keyword for future use
                if save_as_keywords and cat_result.category != "Overig":
                    self._save_as_keyword(product_name, cat_result)
                
                # Apply to database
                if apply_to_database:
                    self._apply_to_database(product_name, cat_result)
        
        result.duration_seconds = time.time() - start_time
        return result
    
    async def _process_batch(
        self, 
        client, 
        products: List[str],
        categories: Dict
    ) -> Tuple[Dict[str, CategorizationResult], Dict[str, Any]]:
        """Process a single batch with retry logic."""
        results = {}
        usage_summary = {"total_tokens": 0, "cost": 0.0}
        indexes = self._build_category_indexes(categories)
        
        for attempt in range(self.MAX_RETRIES):
            try:
                response = client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": self._build_system_prompt(categories)},
                        {"role": "user", "content": self._build_user_prompt(products)}
                    ],
                    temperature=0,
                    response_format={"type": "json_object"}
                )
                
                # Track costs
                input_tokens = getattr(response.usage, "prompt_tokens", 0) or 0
                output_tokens = getattr(response.usage, "completion_tokens", 0) or 0
                total_tokens = input_tokens + output_tokens
                cost = self._estimate_cost(input_tokens, output_tokens)
                
                self.cost_tracker.add(total_tokens, cost, len(products))
                usage_summary["total_tokens"] += total_tokens
                usage_summary["cost"] += cost
                
                # Parse response
                response_text = response.choices[0].message.content
                parsed = json.loads(response_text)
                
                # Validate and create results
                for product_name in products:
                    if product_name in parsed:
                        cat_data = parsed[product_name]
                        if isinstance(cat_data, dict):
                            cat_value = cat_data.get('category_id') or cat_data.get('category')
                            sub_value = cat_data.get('subcategory_id') or cat_data.get('subcategory')
                            confidence = cat_data.get('confidence', 0.7)
                        else:
                            cat_value = cat_data
                            sub_value = None
                            confidence = 0.7
                        
                        category = self._resolve_category(cat_value, indexes)
                        subcategory = self._resolve_subcategory(sub_value, category, indexes)
                        
                        results[product_name] = CategorizationResult(
                            product_name=product_name,
                            category=category,
                            subcategory=subcategory,
                            confidence=confidence if category != "Overig" else 0.0,
                            source="llm"
                        )
                    else:
                        results[product_name] = CategorizationResult(
                            product_name=product_name,
                            category="Overig",
                            error="Product not in LLM response"
                        )
                
                return results, usage_summary
                
            except json.JSONDecodeError as e:
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    for product in products:
                        results[product] = CategorizationResult(
                            product_name=product,
                            category="Overig",
                            error=f"JSON parse error: {str(e)}"
                        )
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(2 ** attempt)
                else:
                    for product in products:
                        results[product] = CategorizationResult(
                            product_name=product,
                            category="Overig",
                            error=str(e)
                        )
        
        return results, usage_summary
    
    def _save_as_keyword(self, product_name: str, result: CategorizationResult):
        """Save successful categorization as a keyword for future use."""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            # Get category ID
            cursor.execute("SELECT id FROM categories WHERE name = ?", (result.category,))
            cat_row = cursor.fetchone()
            if not cat_row:
                conn.close()
                return
            
            cat_id = cat_row['id']
            subcat_id = None
            
            # Get subcategory ID if present
            if result.subcategory:
                cursor.execute(
                    "SELECT id FROM subcategories WHERE name = ? AND category_id = ?",
                    (result.subcategory, cat_id)
                )
                subcat_row = cursor.fetchone()
                if subcat_row:
                    subcat_id = subcat_row['id']
            
            # Create or update product override
            from database import upsert_product_override
            upsert_product_override(
                product_name_pattern=product_name,
                category_id=cat_id,
                subcategory_id=subcat_id,
                match_type="exact",
                source="llm",
                confidence=result.confidence,
                conn=conn
            )
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error saving keyword: {e}")
    
    def _apply_to_database(self, product_name: str, result: CategorizationResult):
        """Apply categorization to all matching products in database."""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            
            cursor.execute("""
                UPDATE products 
                SET user_category = ?, user_subcategory = ?, updated_at = CURRENT_TIMESTAMP
                WHERE original_name = ? AND user_category IS NULL
            """, (result.category, result.subcategory, product_name))
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error applying to database: {e}")
    
    async def categorize_all_uncategorized(
        self, 
        limit: int = 100,
        min_count: int = 1,
        apply_to_database: bool = True,
        save_as_keywords: bool = True
    ) -> BatchResult:
        """
        Categorize all products currently in 'Overig' category.
        
        Args:
            limit: Maximum number of unique products to process
            min_count: Minimum purchase count for products to be processed
            
        Returns:
            BatchResult with all categorization results
        """
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                original_name,
                COUNT(*) as count,
                SUM(price) as total_spent
            FROM products
            LEFT JOIN llm_suggestions ls
                ON ls.product_name = products.original_name
               AND ls.status = 'pending'
            WHERE COALESCE(user_category, auto_category) = 'Overig'
              AND ls.id IS NULL
            GROUP BY original_name
            HAVING count >= ?
            ORDER BY count DESC, total_spent DESC
            LIMIT ?
        """, (min_count, limit))
        
        products = [row['original_name'] for row in cursor.fetchall()]
        conn.close()
        
        if not products:
            return BatchResult()
        
        return await self.categorize_batch(
            products,
            save_as_keywords=save_as_keywords,
            apply_to_database=apply_to_database
        )

    async def suggest_batch(self, products: List[str]) -> BatchResult:
        """Run LLM categorization and store suggestions for review."""
        result = await self.categorize_batch(
            products,
            save_as_keywords=False,
            apply_to_database=False
        )
        
        # Persist as suggestions
        from database import save_llm_suggestions
        categories = self._load_categories()
        indexes = self._build_category_indexes(categories)
        suggestions = []
        for name, r in result.results.items():
            if r.error:
                continue
            category_id = indexes["category_by_name"].get(r.category)
            subcategory_id = None
            if r.subcategory:
                for sub_id, sub_name in indexes["subcategory_by_id"].items():
                    if sub_name == r.subcategory:
                        subcategory_id = sub_id
                        break
            if category_id:
                suggestions.append({
                    "product_name": name,
                    "category_id": category_id,
                    "subcategory_id": subcategory_id,
                    "confidence": r.confidence,
                    "model": self.model,
                    "prompt_version": self.PROMPT_VERSION
                })
        
        if suggestions:
            save_llm_suggestions(suggestions)
        
        return result

    async def suggest_all_uncategorized(self, limit: int = 100, min_count: int = 1) -> BatchResult:
        """Suggest categories for uncategorized products and store suggestions."""
        result = await self.categorize_all_uncategorized(
            limit=limit,
            min_count=min_count,
            apply_to_database=False,
            save_as_keywords=False
        )
        
        from database import save_llm_suggestions
        categories = self._load_categories()
        indexes = self._build_category_indexes(categories)
        suggestions = []
        for name, r in result.results.items():
            if r.error:
                continue
            category_id = indexes["category_by_name"].get(r.category)
            subcategory_id = None
            if r.subcategory:
                for sub_id, sub_name in indexes["subcategory_by_id"].items():
                    if sub_name == r.subcategory:
                        subcategory_id = sub_id
                        break
            if category_id:
                suggestions.append({
                    "product_name": name,
                    "category_id": category_id,
                    "subcategory_id": subcategory_id,
                    "confidence": r.confidence,
                    "model": self.model,
                    "prompt_version": self.PROMPT_VERSION
                })
        
        if suggestions:
            save_llm_suggestions(suggestions)
        
        return result
    
    def get_cost_summary(self) -> Dict:
        """Get summary of costs incurred by this categorizer instance."""
        return self.cost_tracker.to_dict()
    
    def get_uncategorized_count(self) -> int:
        """Get count of products still in 'Overig' category."""
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(DISTINCT original_name) as count
            FROM products
            WHERE COALESCE(user_category, auto_category) = 'Overig'
        """)
        
        result = cursor.fetchone()['count']
        conn.close()
        return result


# ============================================================================
# AGENT-NATIVE API FUNCTIONS
# ============================================================================
# These functions provide atomic, composable operations for AI agents.
# Each function does ONE thing well and can be combined with others.

_categorizer_instance = None

def get_llm_categorizer() -> LLMCategorizer:
    """Get or create singleton LLM categorizer instance."""
    global _categorizer_instance
    if _categorizer_instance is None:
        _categorizer_instance = LLMCategorizer()
    return _categorizer_instance


async def categorize_products_with_llm(
    products: List[str],
    save_as_suggestions: bool = True
) -> Dict:
    """
    Atomic tool: Suggest categories for a list of products using LLM.
    
    This stores suggestions for review instead of applying directly.
    
    Args:
        products: List of product names to categorize
        save_as_suggestions: Whether to save results to the suggestions queue
        
    Returns:
        Dict with results, stats, and cost information
    """
    categorizer = get_llm_categorizer()
    result = await categorizer.suggest_batch(products) if save_as_suggestions else await categorizer.categorize_batch(
        products, save_as_keywords=False, apply_to_database=False
    )
    
    return {
        "results": {
            name: {
                "category": r.category,
                "subcategory": r.subcategory,
                "confidence": r.confidence,
                "error": r.error
            }
            for name, r in result.results.items()
        },
        "stats": {
            "total": result.total_products,
            "successful": result.successful,
            "failed": result.failed,
            "duration_seconds": round(result.duration_seconds, 2)
        },
        "cost": {
            "tokens_used": result.tokens_used,
            "estimated_cost_usd": round(result.estimated_cost_usd, 4),
            "model": result.model_used
        }
    }


async def categorize_all_uncategorized_products(
    limit: int = 100,
    min_count: int = 1,
    save_as_suggestions: bool = True
) -> Dict:
    """
    Atomic tool: Suggest categories for all uncategorized products in the database.
    
    This is useful for batch processing after importing new invoices.
    
    Args:
        limit: Maximum products to process (for cost control)
        
    Returns:
        Dict with results, stats, and cost information
    """
    categorizer = get_llm_categorizer()
    if save_as_suggestions:
        result = await categorizer.suggest_all_uncategorized(limit, min_count=min_count)
    else:
        result = await categorizer.categorize_all_uncategorized(
            limit=limit,
            min_count=min_count,
            apply_to_database=False,
            save_as_keywords=False
        )
    
    return {
        "results": {
            name: {
                "category": r.category,
                "subcategory": r.subcategory,
                "error": r.error
            }
            for name, r in result.results.items()
        },
        "stats": {
            "total": result.total_products,
            "successful": result.successful,
            "failed": result.failed,
            "remaining_uncategorized": categorizer.get_uncategorized_count()
        },
        "cost": categorizer.get_cost_summary()
    }


def get_categorization_cost_estimate(product_count: int, model: str = "gpt-4o-mini") -> Dict:
    """
    Atomic tool: Estimate cost for categorizing N products.
    
    Useful for agents to make cost-aware decisions.
    
    Args:
        product_count: Number of products to estimate for
        model: Model to use for estimate
        
    Returns:
        Dict with cost estimates
    """
    # Rough estimates based on compact prompts
    avg_input_tokens_per_product = 35
    avg_output_tokens_per_product = 20
    
    total_input = product_count * avg_input_tokens_per_product
    total_output = product_count * avg_output_tokens_per_product
    
    pricing = LLMCategorizer.MODEL_PRICING.get(model, LLMCategorizer.MODEL_PRICING["gpt-4o-mini"])
    input_cost = (total_input / 1_000_000) * pricing["input"]
    output_cost = (total_output / 1_000_000) * pricing["output"]
    total_cost = input_cost + output_cost
    
    return {
        "product_count": product_count,
        "model": model,
        "estimated_tokens": total_input + total_output,
        "estimated_cost_usd": round(total_cost, 4),
        "cost_per_product": round(total_cost / max(1, product_count), 6),
        "batches_needed": (product_count + LLMCategorizer.MAX_BATCH_SIZE - 1) // LLMCategorizer.MAX_BATCH_SIZE
    }


def get_llm_categorization_status() -> Dict:
    """
    Atomic tool: Get current status of LLM categorization.
    
    Returns:
        Dict with uncategorized count, costs, and health info
    """
    categorizer = get_llm_categorizer()
    from database import get_llm_suggestions_count
    
    return {
        "uncategorized_products": categorizer.get_uncategorized_count(),
        "pending_suggestions": get_llm_suggestions_count("pending"),
        "cumulative_costs": categorizer.get_cost_summary(),
        "model": categorizer.model,
        "prompt_version": categorizer.PROMPT_VERSION,
        "max_batch_size": categorizer.MAX_BATCH_SIZE,
        "available_models": list(LLMCategorizer.MODEL_PRICING.keys())
    }
