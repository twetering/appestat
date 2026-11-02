# CLAUDE.md - AI Agent Instructions for Appestat

## What is This File?

This file contains instructions and context for AI agents (Claude, GPT, etc.) working on the Appestat codebase. It follows the principle of **agent-native architecture**: treating AI agents as first-class developers who need clear, structured guidance.

---

## 🧠 Project Overview

**Appestat** is a personal "food accountant" that helps users analyze their Albert Heijn (Dutch supermarket) grocery spending. The name refers to the brain's appetite regulation mechanism.

### Core Value Proposition
1. **Awareness**: Show users exactly what they spend on groceries
2. **Categorization**: Organize spending into meaningful categories
3. **Budgeting**: Help users set and track budget goals
4. **Long-term Thinking**: Show 30-year projections to motivate behavior change

### Tech Stack
- **Backend**: Python 3.9+ with Flask
- **Database**: SQLite (local, privacy-first)
- **Frontend**: Vanilla HTML/JS with Chart.js
- **LLM Integration**: OpenAI API for intelligent categorization

---

## 🏗️ Agent-Native Architecture

This codebase follows **agent-native design principles**:

### Principle 1: Parity
Everything a user can do in the UI, an agent can do via API.

### Principle 2: Granularity
Tools are more atomic than features. Multiple tool calls compose into features.

### Principle 3: Composability
Agents can combine tools in ways developers didn't anticipate.

### Key Design Patterns
```
Traditional: Feature → Code → Execute
Agent-Native: Outcome → Agent → Tools → Execute
```

---

## 📁 Codebase Structure

```
appestat/
├── app.py                 # Flask application & API routes
├── database.py            # Database operations & schema
├── parser.py              # PDF invoice parsing
├── services/
│   ├── budget.py          # Budget tracking service
│   ├── categorization.py  # 3-tier categorization service
│   └── llm_categorizer.py # Robust LLM-based categorizer
├── sources/
│   ├── ah_online.py       # AH online invoice parser
│   ├── ah_receipt.py      # AH receipt parser
│   ├── plus_receipt.py    # PLUS receipt photos: local OCR + arithmetic validation
│   ├── plus_llm.py        # OpenAI vision fallback for PLUS photos that don't add up
│   └── registry.py        # Source type registry
├── scripts/
│   └── vision_ocr.swift   # Apple Vision OCR helper (compiled on first use)
├── templates/             # Jinja2 HTML templates
├── data/
│   ├── appestat.db        # SQLite database
│   ├── ah_categories.json # Category definitions
│   └── invoices/          # PDF storage
└── migrations/            # Database migrations
```

---

## 🔧 Key Services & How to Use Them

### 1. Categorization Service (`services/categorization.py`)

**Purpose**: Automatically categorize products into categories/subcategories.

**3-Tier System**:
1. **User Overrides** (highest priority): Learned from user corrections
2. **Keywords** (fast, free): Database-driven keyword matching
3. **LLM Fallback** (intelligent, paid): OpenAI GPT for unknowns

**Agent Usage**:
```python
from services.categorization import get_categorization_service

service = get_categorization_service()

# Single product
result = service.categorize("AH Halfvolle Melk")
# Returns: {"category": "Zuivel & Eieren", "subcategory": "Melk", "source": "keyword"}

# Learn from correction
service.learn_from_correction("Product X", "Snacks", apply_retroactively=True)
```

### 2. LLM Categorizer (`services/llm_categorizer.py`)

**Purpose**: Cost-efficient batch categorization using OpenAI.

**Key Features**:
- Batches up to 100 products per API call
- Supports subcategories
- Tracks token usage and costs
- Retry logic with exponential backoff

**Agent Usage**:
```python
from services.llm_categorizer import (
    categorize_products_with_llm,
    categorize_all_uncategorized_products,
    get_categorization_cost_estimate,
    get_llm_categorization_status
)

# Check status before using
status = get_llm_categorization_status()
print(f"Uncategorized: {status['uncategorized_products']}")

# Estimate costs before processing
estimate = get_categorization_cost_estimate(100)
print(f"Estimated cost: ${estimate['estimated_cost_usd']}")

# Process all uncategorized
result = await categorize_all_uncategorized_products(limit=100)
```

### 3. Budget Service (`services/budget.py`)

**Purpose**: Track and manage category-level budgets.

**Key Features**:
- Auto-generate budgets from historical spending
- Track progress vs budget
- Alert system for overspending

**Agent Usage**:
```python
from services.budget import get_budget_service

service = get_budget_service()

# Get dashboard for current year
dashboard = service.get_budget_dashboard(2026)

# Auto-generate budgets (10% reduction from last year)
service.generate_category_budgets_from_history(2026, reduction_percentage=0.1)

# Set specific budget
service.set_category_budget(2026, category_id=5, target_amount=500.00)
```

---

## 🔌 API Reference

All APIs return JSON. See `API.md` for complete documentation.

### Quick Reference

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/analysis` | GET | Get complete spending analysis |
| `/api/invoices/import` | POST | Import all invoices |
| `/api/categorization/llm` | POST | Run LLM categorization |
| `/api/categorization/llm/estimate` | GET | Estimate LLM costs |
| `/api/budget/dashboard` | GET | Get budget status |
| `/api/budget/categories/generate` | POST | Auto-generate budgets |

---

## 🎯 Common Agent Tasks

### Task: Import New Invoices and Categorize

```python
# 1. Import invoices
POST /api/invoices/import {"force": false}

# 2. Check uncategorized count
GET /api/categorization/llm/status

# 3. Estimate cost
GET /api/categorization/llm/estimate?count=50

# 4. If cost acceptable, run LLM categorization
POST /api/categorization/llm {"limit": 50}
```

### Task: Set Up Budget for New Year

```python
# 1. Get current year
current_year = 2026

# 2. Generate budgets from last year with 10% reduction
POST /api/budget/categories/generate {
    "target_year": 2026,
    "reduction_percentage": 0.1
}

# 3. Verify dashboard
GET /api/budget/dashboard?year=2026
```

### Task: Correct a Miscategorized Product

```python
# 1. Learn the correction (applies to all similar products)
POST /api/categorization/learn {
    "product_name": "AH Frambozen",
    "category": "Fruit & Verse sappen",
    "subcategory": "Fruit",
    "apply_retroactively": true
}
```

---

## 📸 PLUS Receipt Photos

Photos (HEIC/JPG/PNG) anywhere under `data/invoices/` (e.g. `data/invoices/plus/`) are
imported by `POST /api/invoices/import` alongside the AH PDFs.

1. **Local OCR** via Apple Vision (`scripts/vision_ocr.swift`, macOS only). Results are cached
   per file hash in `data/cache/ocr/`.
2. **Row reconstruction**: photos are skewed, so amounts are aligned to descriptions by
   estimated shear + order-preserving alignment; several shear hypotheses are tried.
3. **Arithmetic is the judge**: items − Korting + Statiegeld = Subtotaal, and
   Subtotaal + PLUSPunten + acties = Totaal, to the cent. Only then is a reading `valid`.
4. **Fallback**: if the local reading doesn't add up, the photo is sent to OpenAI vision
   (`PLUS_LLM_MODELS`, default `gpt-4o,gpt-4o-mini`; disable with `PLUS_LLM_FALLBACK=0`).
   Its answer must pass the same checks. Otherwise the invoice gets `parse_quality_score < 1`
   and a note asking for manual review.

Conventions: product prices are net of discounts (like AH bonus prices). A receipt-level
action that can't be tied to a product becomes its own negative "Actiekorting ..." line.
Statiegeld is excluded from products (as with AH) but included in `total_amount`.
The receipt date is cut off on most photos, so the photo's EXIF capture time is used.

---

## ⚠️ Important Constraints

### Database
- SQLite with WAL mode for concurrent reads
- File path: `data/appestat.db`
- Never delete without user confirmation

### Privacy
- All data stays local (no cloud sync), with one exception: PLUS photos whose local OCR
  reading doesn't add up are sent to OpenAI (see above)
- Invoice PDFs contain personal data
- Never log or expose personal details

### Cost Awareness
- LLM categorization costs money (~$0.0005/product with gpt-4o-mini)
- Always estimate costs before batch operations
- Track cumulative costs in responses

### Year Handling
- Budget year is always the current calendar year
- Reference year is always (current year - 1)
- Never hardcode year values; use `datetime.now().year`

---

## 🔄 Improvement Areas

When asked to improve Appestat, consider:

### High-Impact, Low-Effort
1. Add keyboard shortcuts for common actions
2. Improve category keyword coverage
3. Add more subcategories for better granularity

### High-Impact, High-Effort
1. Multi-supermarket support (Jumbo, Plus, etc.)
2. Receipt photo OCR integration
3. Meal planning suggestions

### Technical Debt
1. Add comprehensive test coverage
2. Implement proper caching layer
3. Add background job processing for imports

---

## 📊 Database Schema (Key Tables)

```sql
-- Products from invoices
products (
    id, invoice_id, original_name, display_name,
    quantity, price, btw_percentage,
    auto_category, user_category,
    auto_subcategory, user_subcategory,
    is_validated
)

-- Category definitions
categories (
    id, name, color, icon, sort_order, is_system
)

-- Subcategory definitions
subcategories (
    id, name, category_id, icon, sort_order
)

-- Budget periods
budget_periods (
    id, year, name, start_date, end_date, status
)

-- Category budgets
budget_categories (
    id, period_id, category_id, target_amount, warning_threshold
)

-- Keyword rules for categorization
category_keywords (
    id, category_id, subcategory_id, keyword, match_type, priority
)

-- User override rules
product_overrides (
    id, product_name_pattern, category_id, subcategory_id,
    match_type, source, confidence, times_applied
)
```

---

## 🚀 Getting Started (For Agents)

1. **Read this file** to understand the architecture
2. **Read `REQUIREMENTS.md`** for user stories and acceptance criteria
3. **Read `API.md`** for endpoint documentation
4. **Check `SELECTED_IMPROVEMENTS.md`** for the backlog

When making changes:
1. Understand the existing patterns first
2. Follow the 3-tier categorization hierarchy
3. Always use dynamic year handling
4. Add appropriate documentation for new features
5. Consider both user AND agent experience

---

## 📝 Code Style

- Python: Follow PEP 8, use type hints
- JavaScript: Vanilla JS, no frameworks
- HTML: Jinja2 templates, semantic HTML
- SQL: Use parameterized queries, never string concatenation
- Comments: Explain WHY, not WHAT

---

## 🤝 Collaboration Notes

This codebase is designed for human-AI collaboration:

1. **Clear abstractions**: Each service has a single responsibility
2. **Atomic APIs**: Small, composable endpoints
3. **Rich documentation**: Every module explains its purpose
4. **Observable state**: Cost tracking, progress indicators
5. **Graceful degradation**: Fallbacks when services fail

When in doubt, ask the human for clarification rather than making assumptions.

