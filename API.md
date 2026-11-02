# API.md - Complete API Documentation

## Overview

Appestat provides a RESTful JSON API for all operations. This documentation is designed for both human developers and AI agents.

**Base URL**: `http://localhost:5050`

---

## Authentication

Currently no authentication required (local-only application).

---

## Response Format

All endpoints return JSON. Successful responses have status 200.
Error responses include an `error` field with a human-readable message.

```json
// Success
{
  "status": "success",
  "data": { ... }
}

// Error
{
  "error": "Description of what went wrong"
}
```

---

## Endpoints

### 📊 Analysis

#### GET `/api/analysis`

Get complete spending analysis data.

**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| year | string | null | Filter by year (e.g., "2025") |

**Response:**
```json
{
  "summary": {
    "total_spent": 5432.10,
    "total_savings": 543.21,
    "total_invoices": 52,
    "avg_per_invoice": 104.46,
    "avg_monthly": 452.68,
    "date_range": {
      "start": "2025-01-03",
      "end": "2025-12-28"
    }
  },
  "category_totals": {
    "Zuivel & Eieren": 850.50,
    "Groente & Aardappelen": 620.30,
    ...
  },
  "subcategory_totals": {
    "Zuivel & Eieren": {
      "Melk": {"total": 250.00, "count": 48},
      "Yoghurt": {"total": 180.50, "count": 35}
    }
  },
  "monthly_totals": {
    "2025-01": 423.50,
    "2025-02": 389.20,
    ...
  },
  "top_products": [
    {"name": "AH Halfvolle melk", "category": "Zuivel & Eieren", "count": 48, "total": 95.52}
  ],
  "thirty_year_projection": {
    "AH Halfvolle melk": {"monthly": 7.96, "yearly": 95.52, "thirty_years": 9026.93}
  },
  "savings_options": [...],
  "available_years": ["2023", "2024", "2025"]
}
```

---

### 📄 Invoices

#### GET `/api/invoices`

Get all invoices.

**Response:**
```json
[
  {
    "id": 1,
    "filename": "2025-01-03_factuur.pdf",
    "invoice_number": "123456789",
    "invoice_date": "2025-01-03",
    "total_amount": 87.65,
    "total_savings": 12.34,
    "product_count": 23,
    "validated_count": 20
  }
]
```

#### GET `/api/invoices/<invoice_id>`

Get single invoice with all products.

**Response:**
```json
{
  "id": 1,
  "filename": "2025-01-03_factuur.pdf",
  "invoice_date": "2025-01-03",
  "total_amount": 87.65,
  "products": [
    {
      "id": 1,
      "original_name": "AH Halfvolle melk",
      "price": 1.99,
      "quantity": 2,
      "category": "Zuivel & Eieren",
      "subcategory": "Melk"
    }
  ]
}
```

#### POST `/api/invoices/import`

Import all invoices from data/invoices directory.

**Request Body:**
```json
{
  "force": false  // Set true to reimport existing
}
```

**Response:**
```json
{
  "status": "success",
  "imported": 5,
  "skipped": 47,
  "errors": 0,
  "total_amount": 432.10
}
```

#### POST `/api/invoices/<invoice_id>/reparse`

Reparse a specific invoice with current parser.

**Response:**
```json
{
  "status": "reparsed",
  "invoice_id": 1,
  "old_product_count": 23,
  "new_product_count": 24
}
```

---

### 🏷️ Categories

#### GET `/api/categories`

Get all categories.

**Response:**
```json
[
  {
    "id": 1,
    "name": "Zuivel & Eieren",
    "icon": "🥛",
    "color": "#00ADE6",
    "sort_order": 9
  }
]
```

#### POST `/api/categories`

Add a new category.

**Request Body:**
```json
{
  "name": "Nieuwe Categorie",
  "color": "#FF5733",
  "icon": "🆕"
}
```

#### DELETE `/api/categories/<category_id>`

Delete a category. Products are moved to "Overig".

---

### 📦 Products

#### GET `/api/products`

Get all products.

**Query Parameters:**
| Param | Type | Description |
|-------|------|-------------|
| category | string | Filter by category name |

#### GET `/api/products/unique`

Get all unique products with aggregated stats.

**Response:**
```json
[
  {
    "name": "AH Halfvolle melk",
    "category": "Zuivel & Eieren",
    "count": 48,
    "total": 95.52,
    "avg_price": 1.99
  }
]
```

#### GET `/api/products/details/<product_name>`

Get detailed info about a specific product.

**Response:**
```json
{
  "original_name": "AH Halfvolle melk",
  "category": "Zuivel & Eieren",
  "total_spent": 95.52,
  "total_quantity": 48,
  "avg_unit_price": 1.99,
  "price_trend": {
    "direction": "up",
    "percentage": 5.2
  },
  "projection": {
    "monthly": 7.96,
    "yearly": 95.52,
    "thirty_years": 9026.93
  },
  "price_history": [
    {"invoice_date": "2025-01-03", "price": 1.99, "quantity": 2}
  ]
}
```

#### PUT `/api/products/<product_id>/category`

Update product category.

**Request Body:**
```json
{
  "category": "Snacks",
  "apply_to_similar": true  // Apply to all products with same name
}
```

#### PUT `/api/products/<product_id>/subcategory`

Update product subcategory.

**Request Body:**
```json
{
  "subcategory": "Chips",
  "apply_to_similar": true
}
```

---

### 🧠 Categorization

#### GET `/api/categorization/uncategorized`

Get products currently in "Overig" category.

**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| limit | int | 100 | Maximum products to return |

**Response:**
```json
[
  {
    "original_name": "Unknown Product X",
    "count": 5,
    "total_spent": 24.95
  }
]
```

#### POST `/api/categorization/categorize`

Categorize products using keyword matching.

**Request Body:**
```json
{
  "products": ["Product A", "Product B"]
}
```

**Response:**
```json
{
  "results": {
    "Product A": {"category": "Snacks", "subcategory": null, "source": "keyword"},
    "Product B": {"category": "Overig", "subcategory": null, "source": "default"}
  },
  "uncategorized": ["Product B"],
  "stats": {"total": 2, "categorized": 1, "uncategorized": 1}
}
```

#### POST `/api/categorization/learn`

Learn from a user correction.

**Request Body:**
```json
{
  "product_name": "AH Frambozen",
  "category": "Fruit & Verse sappen",
  "subcategory": "Fruit",
  "apply_retroactively": true
}
```

#### POST `/api/categorization/llm`

Suggest LLM categorization for uncategorized products (review queue).

**Request Body:**
```json
{
  "limit": 50,
  "min_count": 1,
  "save_as_suggestions": true
}
```

**Response:**
```json
{
  "results": {
    "Product X": {
      "category": "Snacks",
      "subcategory": "Chips",
      "confidence": 0.7,
      "error": null
    }
  },
  "stats": {
    "total": 50,
    "successful": 48,
    "failed": 2,
    "remaining_uncategorized": 15
  },
  "cost": {
    "tokens_used": 5432,
    "estimated_cost_usd": 0.0027,
    "model": "gpt-4o-mini"
  }
}
```

#### POST `/api/categorization/llm/batch`

Suggest categories for specific products with LLM (review queue).

**Request Body:**
```json
{
  "products": ["Product A", "Product B"],
  "save_as_suggestions": true
}
```

#### GET `/api/categorization/llm/estimate`

Estimate LLM categorization cost.

**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| count | int | 100 | Number of products |
| model | string | gpt-4o-mini | Model to use |

**Response:**
```json
{
  "product_count": 100,
  "model": "gpt-4o-mini",
  "estimated_tokens": 8000,
  "estimated_cost_usd": 0.004,
  "cost_per_product": 0.00004,
  "batches_needed": 1
}
```

#### GET `/api/categorization/llm/status`

Get LLM categorization status.

**Response:**
```json
{
  "uncategorized_products": 65,
  "pending_suggestions": 12,
  "cumulative_costs": {
    "total_tokens": 12500,
    "total_cost_usd": 0.0063,
    "total_products_processed": 150,
    "total_api_calls": 2
  },
  "model": "gpt-4o-mini",
  "prompt_version": "2026-02-06-v1",
  "max_batch_size": 100,
  "available_models": ["gpt-4o-mini", "gpt-4o", "gpt-4-turbo"]
}
```

#### GET `/api/categorization/suggestions`

Get LLM suggestions for review.

**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| status | string | pending | pending/accepted/rejected |
| limit | int | 100 | Max results |

**Response:**
```json
[
  {
    "id": 1,
    "product_name": "AH Frambozen",
    "category_name": "Fruit & Verse sappen",
    "subcategory_name": "Fruit",
    "confidence": 0.7,
    "model": "gpt-4o-mini",
    "prompt_version": "2026-02-06-v1",
    "purchase_count": 6,
    "total_spent": 18.75
  }
]
```

#### POST `/api/categorization/suggestions/accept`

Accept LLM suggestions and apply to products.

**Request Body:**
```json
{
  "suggestion_ids": [1, 2, 3],
  "overrides": [
    {"id": 1, "category": "Zuivel & Eieren", "subcategory": "Yoghurt & Kwark"}
  ]
}
```

#### POST `/api/categorization/suggestions/reject`

Reject LLM suggestions.

**Request Body:**
```json
{
  "suggestion_ids": [1, 2, 3]
}
```

#### GET `/api/categorization/keywords`

Get all categorization keywords.

**Response:**
```json
[
  {
    "id": 1,
    "keyword": "melk",
    "match_type": "contains",
    "priority": 10,
    "category_name": "Zuivel & Eieren",
    "subcategory_name": "Melk"
  }
]
```

#### POST `/api/categorization/keywords`

Add a new keyword rule.

**Request Body:**
```json
{
  "keyword": "frambozen",
  "category": "Fruit & Verse sappen",
  "subcategory": "Fruit",
  "match_type": "contains",
  "priority": 5
}
```

---

### 💰 Budget

#### GET `/api/budget/dashboard`

Get budget dashboard for a year.

**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| year | int | current year | Budget year |

**Response:**
```json
{
  "period": {"id": 1, "year": 2026, "name": "Jaarbudget 2026"},
  "current_year": 2026,
  "reference_year": 2025,
  "total_budget": 6000.00,
  "total_spent": 523.45,
  "reference_total": 6500.00,
  "projected_annual": 6281.40,
  "budget_progress": 8.72,
  "on_track": true,
  "categories": [
    {
      "category_id": 9,
      "category_name": "Zuivel & Eieren",
      "icon": "🥛",
      "target_amount": 900.00,
      "current_spent": 85.50,
      "reference_spent": 920.00,
      "progress": 9.5,
      "change_percentage": -7.1
    }
  ],
  "monthly_spending": {
    "01": 523.45
  },
  "alerts": [],
  "savings_vs_last_year": 5976.55
}
```

#### GET `/api/budget/categories`

Get category budgets for a year.

**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| year | int | current year | Budget year |

#### POST `/api/budget/categories`

Set a category budget.

**Request Body:**
```json
{
  "year": 2026,
  "category_id": 9,
  "target_amount": 900.00,
  "warning_threshold": 0.8,
  "notes": "Reduce milk spending"
}
```

#### POST `/api/budget/categories/generate`

Auto-generate budgets from historical spending.

**Request Body:**
```json
{
  "target_year": 2026,
  "reduction_percentage": 0.1  // 10% reduction
}
```

**Response:**
```json
{
  "status": "generated",
  "target_year": 2026,
  "reduction_percentage": 0.1,
  "categories": [
    {
      "category": "Zuivel & Eieren",
      "reference_spent": 920.00,
      "target_amount": 828.00,
      "success": true
    }
  ]
}
```

#### GET `/api/budget/alerts`

Get active budget alerts.

**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| year | int | current year | Budget year |

#### POST `/api/budget/alerts/<alert_id>/dismiss`

Dismiss a budget alert.

---

### 📈 Insights

#### GET `/api/insights/savings`

Get smart savings insights.

**Response:**
```json
{
  "price_increases": [
    {"name": "Product X", "first_price": 1.50, "last_price": 1.99, "change_pct": 32.7}
  ],
  "premium_vs_huismerk": [
    {"category": "Zuivel", "amerk_spent": 150, "huismerk_spent": 350, "amerk_percentage": 30}
  ],
  "impulse_buys": [
    {"name": "Random Item", "price": 4.99}
  ]
}
```

#### GET `/api/insights/day-analysis`

Get spending analysis by day of week.

**Query Parameters:**
| Param | Type | Description |
|-------|------|-------------|
| year | string | Filter by year |

#### GET `/api/insights/bonus`

Get bonus/discount analysis.

#### GET `/api/insights/prediction`

Get spending prediction for current year.

---

### ⚙️ Admin

#### GET `/api/admin/backup`

Download database backup file.

#### POST `/api/admin/reset`

Reset all data (requires confirmation).

**Request Body:**
```json
{
  "confirm": true
}
```

#### POST `/api/admin/reimport`

Reimport all invoices.

#### POST `/api/admin/run-migrations`

Run pending database migrations.

---

## Error Codes

| Code | Meaning |
|------|---------|
| 400 | Bad Request - Missing or invalid parameters |
| 404 | Not Found - Resource doesn't exist |
| 500 | Server Error - Check logs for details |

---

## Rate Limits

No rate limits for local usage. LLM endpoints are limited by OpenAI quotas.

---

## Versioning

Current API version: **v1** (implicit, no version prefix)

Breaking changes will be announced and documented here.

---

## Examples for AI Agents

### Import and Categorize Workflow

```bash
# 1. Import new invoices
curl -X POST http://localhost:5050/api/invoices/import

# 2. Check uncategorized count
curl http://localhost:5050/api/categorization/llm/status

# 3. Estimate cost
curl "http://localhost:5050/api/categorization/llm/estimate?count=50"

# 4. Run categorization
curl -X POST http://localhost:5050/api/categorization/llm \
  -H "Content-Type: application/json" \
  -d '{"limit": 50}'
```

### Budget Setup Workflow

```bash
# 1. Generate budgets from last year (10% less)
curl -X POST http://localhost:5050/api/budget/categories/generate \
  -H "Content-Type: application/json" \
  -d '{"target_year": 2026, "reduction_percentage": 0.1}'

# 2. Check dashboard
curl "http://localhost:5050/api/budget/dashboard?year=2026"
```
