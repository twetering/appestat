"""
Categorization Service for Appestat.
Implements 3-tier categorization: User Overrides -> Keywords -> LLM Fallback
"""

import os
import json
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from pathlib import Path

# Load environment variables
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection


class CategorizationService:
    """
    3-Tier Product Categorization Service:
    1. User Overrides (learned from user corrections)
    2. Database Keywords (persistent, editable)
    3. LLM Fallback (OpenAI GPT for unknown products)
    """
    
    def __init__(self):
        self._keyword_cache = None
        self._override_cache = None
        self._cache_timestamp = None
        self._cache_ttl = 60  # seconds
    
    def _refresh_caches(self, force: bool = False):
        """Refresh keyword and override caches from database."""
        now = datetime.now()
        
        if not force and self._cache_timestamp:
            if (now - self._cache_timestamp).seconds < self._cache_ttl:
                return
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Load keywords with category names
        cursor.execute("""
            SELECT 
                ck.keyword,
                ck.match_type,
                ck.priority,
                c.name as category_name,
                s.name as subcategory_name
            FROM category_keywords ck
            JOIN categories c ON ck.category_id = c.id
            LEFT JOIN subcategories s ON ck.subcategory_id = s.id
            WHERE ck.is_active = 1
            ORDER BY ck.priority DESC, length(ck.keyword) DESC
        """)
        
        self._keyword_cache = [dict(row) for row in cursor.fetchall()]
        
        # Load user overrides
        cursor.execute("""
            SELECT 
                po.product_name_pattern,
                po.match_type,
                c.name as category_name,
                s.name as subcategory_name
            FROM product_overrides po
            JOIN categories c ON po.category_id = c.id
            LEFT JOIN subcategories s ON po.subcategory_id = s.id
            ORDER BY po.times_applied DESC
        """)
        
        self._override_cache = [dict(row) for row in cursor.fetchall()]
        
        conn.close()
        self._cache_timestamp = now
    
    def categorize(self, product_name: str) -> Dict[str, Optional[str]]:
        """
        Categorize a single product using the 3-tier system.
        Returns: {"category": str, "subcategory": str|None, "source": str}
        """
        self._refresh_caches()
        
        # Normalize the product name
        name_lower = self._normalize_name(product_name).lower()
        
        # Tier 1: Check user overrides
        result = self._check_overrides(name_lower, product_name)
        if result:
            return {**result, "source": "user_override"}
        
        # Tier 2: Check database keywords
        result = self._check_keywords(name_lower)
        if result:
            return {**result, "source": "keyword"}
        
        # No match found - return Overig
        return {
            "category": "Overig",
            "subcategory": None,
            "source": "default"
        }
    
    def _normalize_name(self, name: str) -> str:
        """Normalize product name for matching."""
        name = name.strip()
        
        # Remove BONUS suffix
        if name.upper().endswith(' BONUS'):
            name = name[:-6].strip()
        
        # Remove weight/size indicators like "(0.500kg)"
        name = re.sub(r'\s*\([^)]*(?:kg|g|ml|l|cl)\)', '', name, flags=re.IGNORECASE)
        
        return name
    
    def _check_overrides(self, name_lower: str, original_name: str) -> Optional[Dict]:
        """Check user override rules."""
        for override in self._override_cache or []:
            pattern = override['product_name_pattern'].lower()
            match_type = override.get('match_type', 'exact')
            
            matched = False
            if match_type == 'exact':
                matched = name_lower == pattern or original_name.lower() == pattern
            elif match_type == 'prefix':
                matched = name_lower.startswith(pattern)
            elif match_type == 'contains':
                matched = pattern in name_lower
            elif match_type == 'regex':
                try:
                    matched = bool(re.search(pattern, name_lower))
                except:
                    pass
            
            if matched:
                # Update times_applied counter
                self._update_override_usage(override['product_name_pattern'])
                
                return {
                    "category": override['category_name'],
                    "subcategory": override.get('subcategory_name')
                }
        
        return None
    
    def _check_keywords(self, name_lower: str) -> Optional[Dict]:
        """Check keyword rules from database with fuzzy Dutch plural handling."""
        for kw in self._keyword_cache or []:
            keyword = kw['keyword'].lower()
            match_type = kw.get('match_type', 'contains')
            
            matched = False
            if match_type == 'exact':
                matched = name_lower == keyword
            elif match_type == 'prefix':
                matched = name_lower.startswith(keyword)
            elif match_type == 'contains':
                # Standard contains check
                matched = keyword in name_lower
                
                # If not matched, try fuzzy Dutch plural matching
                if not matched:
                    matched = self._fuzzy_dutch_match(keyword, name_lower)
            elif match_type == 'word':
                # Match as whole word
                matched = bool(re.search(r'\b' + re.escape(keyword) + r'\b', name_lower))
            
            if matched:
                return {
                    "category": kw['category_name'],
                    "subcategory": kw.get('subcategory_name')
                }
        
        return None
    
    def _fuzzy_dutch_match(self, keyword: str, product_name: str) -> bool:
        """
        Handle Dutch plural forms and abbreviations.
        Examples: 
        - 'banaan' matches 'bananen' (singular -> plural)
        - 'tomaat' matches 'tomaten'
        - 'ei' matches 'eieren'
        """
        # Generate possible plural forms of the keyword
        keyword_forms = [keyword]
        
        # Dutch plural endings
        if keyword.endswith('a'):
            # banaan -> bananen (a -> anen)
            keyword_forms.append(keyword[:-1] + 'anen')
        if keyword.endswith('aat'):
            # tomaat -> tomaten
            keyword_forms.append(keyword[:-2] + 'ten')
        if keyword.endswith('el'):
            # appel -> appels, appelen
            keyword_forms.append(keyword + 's')
            keyword_forms.append(keyword + 'en')
        if not keyword.endswith('en') and not keyword.endswith('s'):
            # General: add -en, -s
            keyword_forms.append(keyword + 'en')
            keyword_forms.append(keyword + 's')
        
        # Check if any form matches
        for form in keyword_forms:
            if form in product_name:
                return True
        
        # Also check words in product name
        words = product_name.split()
        for word in words:
            # Check if keyword is start of word (handles "banaan" in "bananensmoothie")
            if word.startswith(keyword) or keyword.startswith(word[:4]) if len(word) >= 4 else False:
                return True
        
        return False
    
    def _update_override_usage(self, pattern: str):
        """Update the usage counter for an override."""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE product_overrides 
                SET times_applied = times_applied + 1, last_applied_at = CURRENT_TIMESTAMP
                WHERE product_name_pattern = ?
            """, (pattern,))
            conn.commit()
            conn.close()
        except:
            pass  # Non-critical operation
    
    def learn_from_correction(
        self, 
        product_name: str, 
        category: str, 
        subcategory: Optional[str] = None,
        match_type: str = 'exact',
        apply_retroactively: bool = True
    ) -> Dict:
        """
        Learn from a user correction.
        Creates an override rule and optionally updates all similar products.
        """
        conn = get_connection()
        cursor = conn.cursor()
        
        # Get category ID
        cursor.execute("SELECT id FROM categories WHERE name = ?", (category,))
        cat_row = cursor.fetchone()
        if not cat_row:
            conn.close()
            return {"success": False, "error": f"Category '{category}' not found"}
        
        cat_id = cat_row['id']
        subcat_id = None
        
        # Get subcategory ID if provided
        if subcategory:
            cursor.execute("""
                SELECT id FROM subcategories WHERE name = ? AND category_id = ?
            """, (subcategory, cat_id))
            subcat_row = cursor.fetchone()
            if subcat_row:
                subcat_id = subcat_row['id']
        
        # Create or update override
        normalized_name = self._normalize_name(product_name)
        
        # Check if override already exists
        cursor.execute("""
            SELECT id FROM product_overrides WHERE product_name_pattern = ?
        """, (normalized_name,))
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute("""
                UPDATE product_overrides SET
                    category_id = ?,
                    subcategory_id = ?,
                    match_type = ?,
                    times_applied = times_applied + 1,
                    last_applied_at = CURRENT_TIMESTAMP
                WHERE product_name_pattern = ?
            """, (cat_id, subcat_id, match_type, normalized_name))
        else:
            cursor.execute("""
                INSERT INTO product_overrides (
                    product_name_pattern, category_id, subcategory_id, 
                    match_type, source, confidence, times_applied
                ) VALUES (?, ?, ?, ?, 'user', 1.0, 1)
            """, (normalized_name, cat_id, subcat_id, match_type))
        
        updated_count = 0
        
        # Apply retroactively if requested
        if apply_retroactively:
            cursor.execute("""
                UPDATE products 
                SET user_category = ?, user_subcategory = ?
                WHERE original_name = ? OR original_name = ?
            """, (category, subcategory, normalized_name, product_name))
            updated_count = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        # Invalidate cache
        self._cache_timestamp = None
        
        return {
            "success": True,
            "updated_count": updated_count,
            "pattern": normalized_name,
            "category": category,
            "subcategory": subcategory
        }
    
    def categorize_batch(self, products: List[str]) -> Dict[str, Dict]:
        """
        Categorize multiple products at once.
        Returns dict mapping product name to categorization result.
        """
        results = {}
        uncategorized = []
        
        for product in products:
            result = self.categorize(product)
            results[product] = result
            
            if result['category'] == 'Overig':
                uncategorized.append(product)
        
        return {
            "results": results,
            "uncategorized": uncategorized,
            "stats": {
                "total": len(products),
                "categorized": len(products) - len(uncategorized),
                "uncategorized": len(uncategorized)
            }
        }
    
    async def categorize_with_llm(
        self, 
        products: List[str], 
        save_as_keywords: bool = True
    ) -> Dict[str, Dict]:
        """
        Use OpenAI GPT to categorize products that couldn't be matched by keywords.
        Batches up to 100 products per API call for efficiency.
        """
        import openai
        
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key or api_key.startswith('OPENAI_API_KEY='):
            # Handle malformed env var
            if '=' in api_key:
                api_key = api_key.split('=', 1)[1]
        
        if not api_key:
            return {"error": "OPENAI_API_KEY not configured", "results": {}}
        
        client = openai.OpenAI(api_key=api_key)
        
        # Get available categories
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM categories ORDER BY sort_order")
        categories = [row['name'] for row in cursor.fetchall()]
        conn.close()
        
        results = {}
        batch_size = 50  # Conservative batch size
        
        for i in range(0, len(products), batch_size):
            batch = products[i:i + batch_size]
            
            prompt = f"""Je bent een expert in het categoriseren van Nederlandse supermarktproducten.
Categoriseer elk product in PRECIES één van deze categorieën:
{json.dumps(categories, ensure_ascii=False)}

Producten om te categoriseren:
{json.dumps(batch, ensure_ascii=False)}

Antwoord ALLEEN met valid JSON in dit format (geen andere tekst):
{{"product_naam": "categorie_naam", ...}}

Regels:
- Gebruik EXACT de categorienamen uit de lijst hierboven
- Elk product moet precies één categorie krijgen
- Bij twijfel, kies de meest logische categorie
- "Overig" alleen als het echt nergens past"""

            try:
                response = client.chat.completions.create(
                    model="gpt-4o",  # Using gpt-4o as gpt-5 may not exist yet
                    messages=[
                        {"role": "system", "content": "Je categoriseert supermarktproducten. Antwoord alleen met JSON."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    response_format={"type": "json_object"}
                )
                
                response_text = response.choices[0].message.content
                batch_results = json.loads(response_text)
                
                for product, category in batch_results.items():
                    if category in categories:
                        results[product] = {
                            "category": category,
                            "subcategory": None,
                            "source": "llm"
                        }
                        
                        # Optionally save as keywords for future use
                        if save_as_keywords:
                            self.learn_from_correction(
                                product, category, None, 
                                match_type='exact', 
                                apply_retroactively=True
                            )
                    else:
                        results[product] = {
                            "category": "Overig",
                            "subcategory": None,
                            "source": "llm_fallback"
                        }
                
            except Exception as e:
                print(f"LLM categorization error: {e}")
                # Mark batch as failed
                for product in batch:
                    results[product] = {
                        "category": "Overig",
                        "subcategory": None,
                        "source": "error",
                        "error": str(e)
                    }
        
        return {
            "results": results,
            "stats": {
                "total": len(products),
                "categorized": sum(1 for r in results.values() if r.get('source') == 'llm'),
                "errors": sum(1 for r in results.values() if r.get('source') == 'error')
            }
        }
    
    def get_uncategorized_products(self, limit: int = 100) -> List[Dict]:
        """Get products currently in 'Overig' category for review."""
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                original_name,
                COUNT(*) as count,
                SUM(price) as total_spent
            FROM products
            WHERE COALESCE(user_category, auto_category) = 'Overig'
            GROUP BY original_name
            ORDER BY count DESC, total_spent DESC
            LIMIT ?
        """, (limit,))
        
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return results


# Singleton instance
_service = None

def get_categorization_service() -> CategorizationService:
    global _service
    if _service is None:
        _service = CategorizationService()
    return _service

