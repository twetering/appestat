# CLAUDE.md - AI Agent Instructions for Appestat

Instructions and context for AI agents working on Appestat. Agent-native principles apply:
everything a user can do in the UI, an agent can do via the API (see `API.md`).

---

## 🧠 Project Overview

**Appestat** (v3) is a personal "food accountant" for Dutch grocery receipts (Albert Heijn online
invoices and store receipts, PLUS receipt photos). Goals: awareness (what do I spend, on what),
categorisation, budgeting, long-term thinking (30-year projections).

Tech stack: Python 3.10+ / Flask, SQLite (`data/appestat.db`, local), vanilla JS ES modules + Chart.js,
OpenAI (Decisions API + structured LLM output), Apple Vision OCR via Swift for photos.

---

## 📁 Codebase Structure

```
app.py                  builds the Flask app, registers blueprints (no routes here)
web/pages.py            HTML page routes (+ redirects from v2 URLs)
api/
  insights.py           /api/overview, /api/years, /api/insights/prices, /api/meta, /api/status
  catalog.py            /api/catalog… products, categories per product, review queue, /api/category-tree
  receipts.py           /api/receipts…, /api/upload, /api/export/<kind>.csv
  categories.py         categories, subcategories, keyword rules, learn
  budget.py             category budgets, alerts
  admin.py              backup, reset, reimport, migrations, optional AH app link
  compat.py             v2 API kept for agents/scripts (/api/analysis, /api/invoices…, /api/products…)
importers/              receipt parsers: common.py (line model, discounts, reconciliation),
                        ah_online.py, ah_kassabon.py, plus_photo.py,
                        manual.py (purchases without receipt: data/invoices/handmatig/*.json)
parser.py               import entry point: routes files to importers, dedupes, stores, syncs catalog
services/
  metrics.py            THE definitions of every number (spent, groceries, months, projection …)
  insights.py           page read models built on metrics
  catalog.py            product catalog: aliases, variants, LLM normalisation, Decisions labelling
  ai.py                 OpenAI client (Decisions API + JSON completions), usage tracking
  categorization.py     keyword/override rules (fallback when no AI)
  receipts.py           receipt list/detail/export read models
  budget.py             budgets
sources/plus_receipt.py PLUS photo OCR + row reconstruction; plus_llm.py = OpenAI vision fallback
scripts/vision_ocr.swift Apple Vision OCR helper (compiled on first use into data/cache/bin)
scripts/smoke_check.py  renders every page / GET endpoint, prints headline numbers (--save/--compare)
migrations/             versioned schema migrations (run by database.init_database())
templates/base.html     layout + navigation; templates/pages/*.html one per page
static/css/app.css      design tokens (light/dark) + components
static/js/core.js       api(), fmt (nl-NL), h() DOM builder, filters, category picker, tables, charts
static/js/pages/*.js    one module per page
```

---

## 📐 Data model and definitions (do not reinvent these)

```
stores(id, name, chain)                     chain: 'Albert Heijn' | 'PLUS' | manual chains ('Jimmy Joy')
invoices(id, store_id, source_type, invoice_date, total_amount=paid, total_savings=discounts received,
         parse_quality_score (1.0 = reconciles to the cent), parse_notes, file_hash, raw_text, …)
products  = receipt LINES: price (net paid), gross_price, discount_amount, quantity, pack_count,
            weight_kg, alias_key, catalog_id, auto_category/auto_subcategory (mirrors the catalog),
            user_category/user_subcategory (rare per-line exceptions)
invoice_extras(invoice_id, extra_type, description, amount)
            extra_type: deposit | delivery | subscription | savings
catalog_products(id, name, brand, family, category, subcategory, category_source user|decisions|keyword,
                 category_confidence, needs_review, suggested_category/subcategory/confidence)
product_aliases(alias_key, catalog_id, example_name, pack_count, size_value, size_unit, chain, source)
categories + subcategories                  one tree; a subcategory always belongs to its category
```

**Per receipt: paid = Σ product lines + Σ extras.** Every importer must reconcile this to the cent
(`importers.common.reconcile`) or flag the receipt (`parse_quality_score < 1`).

Definitions live in `services/metrics.py` — always use them, never write new spend SQL in a route:
- **spent** = paid − savings extras (koopzegels, PLUSPunten, redeemed Miles are saving, not spending)
- **groceries** = Σ product lines (net of discounts); **extra costs** = deposit + delivery + subscription
- category totals include a "Bijkomende kosten" bucket so categories sum exactly to spent
- per month = spent / calendar months in the period (up to the latest receipt), not months with data
- projection = per month × 12 / × 360 (`metrics.projection`)
- every metric takes the same filters: `year`, `chain`

**Product identity is the catalog**, never the receipt name. Group by `catalog_id` (see
`metrics.top_products`, `catalog.list_products`). Unit price = price / (quantity × pack_count).

**Categories are per catalog product.** Change them with `catalog.set_category()` (validates the
pair, applies to all lines). Never write `auto_category` directly except through `catalog.materialise()`.
A user's choice (`category_source='user'`) is never overwritten by AI; disagreement becomes a suggestion.

---

## 🔁 Import pipeline

`parser.import_invoice(path)` → importer (AH online / AH kassabon / PLUS photo) → reconcile →
content-dedupe (same source + minute + total = same receipt) → `database.save_parsed_receipt()`
(one transaction; `force=True` re-parses in place, keeping ids and manual categories) →
`catalog.sync()` links new names, estimates packs, labels new products.

PLUS photos: local Apple Vision OCR (cached per file hash in `data/cache/ocr/`); only if the reading does
not add up, `sources/plus_llm.py` sends the photo to OpenAI (`PLUS_LLM_MODELS`, `PLUS_LLM_FALLBACK=0`
disables). Receipt date = photo EXIF capture time (the date is usually cut off the photo).

---

## 🤖 AI usage (OpenAI, optional)

- `services/ai.py`: `decide()` (Decisions API, model `APPESTAT_DECISIONS_MODEL`, default `gpt-6-luna`)
  and `json_completion()` (structured output, `APPESTAT_CHAT_MODEL`, default `gpt-5.4-mini`).
- Normalisation (catalog names, brand, pack size): LLM in batches of 50. Labelling: one Decisions call per
  new catalog product, choice among valid "Categorie › Subcategorie" pairs (~3k tokens per call).
- Near-duplicate merge: Decisions predicate per candidate pair, merged only at p ≥ 0.85.
- Without a key everything falls back to rules; check `ai.available()`; never fail an import on AI errors.
- Cost awareness: batch jobs report `ai.usage_report()`; relabelling everything is ~1.3k calls.

---

## 🎯 Common agent tasks

```text
Import new receipts:        POST /api/invoices/import   (or POST /api/upload with files)
What needs attention:       GET  /api/status, GET /api/catalog/review, GET /api/receipts?status=review
Fix a product's category:   PUT  /api/catalog/<id>/category {category, subcategory}
Accept AI suggestion(s):    POST /api/catalog/<id>/suggestion/accept | POST /api/catalog/suggestions/accept-all
Merge / split products:     POST /api/catalog/merge {keep_id, merge_ids} | POST /api/catalog/aliases/move
Numbers for a period:       GET  /api/overview?year=2026&chain=PLUS
Re-read one receipt:        POST /api/receipts/<id>/reparse
```

After any change, run `pytest` and `python scripts/smoke_check.py --compare <saved>`.

---

## ⚠️ Constraints

- **Privacy**: data stays local. Only product names and (fallback only) unreadable receipt photos go to
  OpenAI. Never log personal details (loyalty/card numbers appear on receipts).
- **Years**: never hardcode; default to `datetime.now().year`.
- **SQL**: parameterised only. `invoices.invoice_date` has NUMERIC affinity: compare with full dates
  (`>= '2025-01-01'`) or `strftime('%Y', …)`, never a bare year (`>= '2025'` becomes an integer and matches all). **Frontend**: vanilla JS, render data with `h()`/textContent, never
  `innerHTML` with data; formatting via `fmt` (nl-NL).
- **Database**: never delete user data without confirmation; back up (`/api/admin/backup`) before bulk
  operations. Debug mode only via `APPESTAT_DEBUG=1` (the Werkzeug debugger allows code execution).

---

## 📝 Code style

Python: PEP 8, type hints, comments explain why. `ruff check --select F` must pass (CI).
JavaScript: ES modules, no framework. Dutch UI text, English code and comments.
