# Appestat API (v3)

All endpoints return JSON (except CSV exports and the database backup). Errors: `{"error": "..."}`
with a 4xx/5xx status. Money is in euros as numbers; dates as `YYYY-MM-DD HH:MM:SS`.

Common filters: `year` (e.g. `2026`, empty = all time) and `chain` (`Albert Heijn` | `PLUS` | `Jimmy Joy` | any manual chain, empty = all).
Every number is computed by `services/metrics.py` (see CLAUDE.md for the definitions).

---

## Overview & insights

| Method | Path | Description |
|---|---|---|
| GET | `/api/overview?year&chain` | Dashboard: `summary` (spent, groceries, extra_costs, savings_stamps, bonus_savings, months, avg_per_month, …), `categories` (with subcategories), `monthly`, `yearly`, `stores`, `top_products` (catalog, with projection), `day_of_week`, `prediction`, `budget`, `review_count` |
| GET | `/api/years?chain` | Per year: spent, groceries, extras, savings, bonus, receipts, months, complete; per category per year; monthly per year |
| GET | `/api/insights/prices?chain` | `rising` / `falling` unit prices per product (paid price per piece), `pack_savings` (what the cheapest pack would have saved) |
| GET | `/api/meta` | Years and chains with data (for filters) |
| GET | `/api/status` | `review_products`, `review_receipts` counters |
| GET | `/api/insights/day-analysis?year` · `/bonus?year` · `/prediction?year` | Atomic insights (same definitions) |
| GET | `/api/analysis?year` | v2-compatible dashboard payload |

## Products (catalog)

A catalog product groups every receipt spelling and pack ("Red Bull 8-pack", "24-pack", "RED BULL").

| Method | Path | Description |
|---|---|---|
| GET | `/api/catalog?q&category&subcategory&chain&year&review=1&sort=spent\|name\|count\|recent&limit` | Products with purchases, units, spent, unit_price_avg, variants, chains, category source/confidence, AI suggestion |
| GET | `/api/catalog/<id>` | Detail: aliases, variants (pack × chain with unit price), best_variant, lines (every purchase), unit_price_trend_pct, projection |
| PUT | `/api/catalog/<id>` | Rename `{name}` |
| PUT | `/api/catalog/<id>/category` | `{category, subcategory}` — validated pair; applies to all purchases (past and future) |
| POST | `/api/catalog/<id>/suggestion/accept` | Take the AI suggestion |
| POST | `/api/catalog/<id>/confirm` | Keep current category (marks it as yours) |
| POST | `/api/catalog/suggestions/accept-all` | `{min_confidence: 0.95}` |
| GET | `/api/catalog/review?limit` | Review queue (AI unsure, AI disagrees with you, Overig), by € spent |
| POST | `/api/catalog/merge` | `{keep_id, merge_ids: [...]}` |
| POST | `/api/catalog/aliases/move` | `{alias, target_id}` or `{alias, new_name}` (split off) |
| POST | `/api/catalog/sync` | `{use_ai: true, merge: false}` link new names, estimate packs, label new products |
| GET | `/api/category-tree` | `{tree: {category: [subcategories]}, ai_available}` for pickers |

## Receipts

| Method | Path | Description |
|---|---|---|
| GET | `/api/receipts?year&chain&q&status=review` | Receipts with store, chain, spent, savings_stamps, lines, quality |
| GET | `/api/receipts/<id>` | Lines (with catalog info), extras, `reconciliation` (groceries + extras = paid), prev/next ids |
| GET | `/api/receipts/<id>/text` | Original text |
| POST | `/api/receipts/<id>/validate` | `{validated: true}` |
| POST | `/api/receipts/<id>/reparse` | Re-read from the original file (keeps manual categories) |
| POST | `/api/upload` | multipart `files` (PDF/HEIC/JPG/PNG) → saved under `data/invoices`, imported |
| POST | `/api/receipts/manual` | `{chain, date, total, product?, note?, lines?}` purchase without a receipt (e.g. Jimmy Joy order) → `data/invoices/handmatig/*.json`, imported as its own chain; same chain+date+total = `skipped` |
| POST | `/api/invoices/import` | `{force: false}` import everything in `data/invoices` (recursive) |
| POST | `/api/invoices/import-single` | `{path}` relative to `data/invoices` |
| GET | `/api/export/<bonnen\|regels\|producten\|bijkomend>.csv` | CSV (`;`, decimal comma) |
| GET | `/api/invoices`, `/api/invoices/<id>` | v2-compatible lists |

## Categories & rules

| Method | Path | Description |
|---|---|---|
| GET/POST | `/api/categories` | List (with counts) / add `{name, icon, color}` |
| DELETE | `/api/categories/<id>` | Products move to Overig (review). Overig and Bijkomende kosten are fixed |
| GET/POST | `/api/subcategories` | List / add `{category, name, icon}` |
| DELETE | `/api/subcategories/<id>` | Products keep their category |
| GET | `/api/subcategories/<category>` · `/api/subcategories/totals` | Per category / totals |
| GET/POST | `/api/categorization/keywords` | Keyword rules (fallback without AI) |
| DELETE | `/api/categorization/keywords/<id>` | |
| POST | `/api/migrate` | Re-apply keyword rules to keyword-labelled products |
| POST | `/api/categorization/learn` | `{product_name, category, subcategory}` product-level correction |
| POST | `/api/categorization/categorize` | `{products: [names]}` dry-run with rules |
| PUT | `/api/products/<line_id>/category` · `/subcategory` | `{category, apply_to_similar}` per line; `apply_to_similar` promotes to the product |
| POST | `/api/products/bulk-update` · `/bulk-update-subcategory` | By receipt names; promoted to the product |

## Budget

| Method | Path | Description |
|---|---|---|
| GET | `/api/budget/dashboard?year` | total_budget, total_spent (= spent), expected_by_now (pace), on_track, projected_annual, categories, alerts |
| GET/POST | `/api/budget/categories` | List / set `{year, category_id, target_amount}` |
| POST | `/api/budget/categories/generate` | `{target_year, reduction_percentage: 0.1}` from previous year |
| GET | `/api/budget/alerts?year` · POST `/api/budget/alerts/<id>/dismiss` | |

## Admin

| Method | Path | Description |
|---|---|---|
| GET | `/api/admin/backup` | Download the SQLite database |
| POST | `/api/admin/reimport` | Import new files from `data/invoices` |
| POST | `/api/admin/reset` | `{confirm: true}` delete all receipts and lines (keeps categories, catalog) |
| POST | `/api/admin/run-migrations` | |
| GET/POST/DELETE | `/api/ah/auth…`, POST `/api/ah/sync` | Optional Albert Heijn app link |
