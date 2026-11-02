# REQUIREMENTS.md - User Stories & Feature Specifications

## Purpose

This document defines the requirements for Appestat using user stories, acceptance criteria, and technical specifications. It serves as the source of truth for what the application should do.

---

## 📖 Terminology

| Term | Definition |
|------|------------|
| **Invoice** | PDF document from AH containing order details |
| **Receipt** | Kassabon from in-store purchase |
| **Product** | Single line item from an invoice |
| **Category** | Top-level grouping (e.g., "Zuivel & Eieren") |
| **Subcategory** | Secondary grouping (e.g., "Melk", "Yoghurt") |
| **Budget** | Spending target for a category in a given year |

---

## 🎯 Epic 1: Invoice Management

### US-1.1: Import Online Invoices
**As a** user  
**I want to** import my AH online order invoices  
**So that** I can analyze my spending

**Acceptance Criteria:**
- [ ] User can place PDF files in `data/invoices/` directory
- [ ] System detects new PDFs automatically
- [ ] System extracts: date, total, products, prices, savings
- [ ] Duplicate invoices are detected and skipped
- [ ] Import status is shown to user

**Technical Notes:**
- Parser: `sources/ah_online.py`
- Uses pdfplumber for text extraction
- Hash-based duplicate detection

### US-1.2: Import Kassabonnen
**As a** user  
**I want to** import my in-store receipts  
**So that** both online and in-store purchases are tracked

**Acceptance Criteria:**
- [ ] System supports AH kassabon PDF format
- [ ] Extracts same data as online invoices
- [ ] Marks source as "receipt" vs "online"

**Technical Notes:**
- Parser: `sources/ah_receipt.py`
- Different PDF structure than online invoices

### US-1.3: View Invoice Details
**As a** user  
**I want to** see all products on a specific invoice  
**So that** I can verify parsing accuracy

**Acceptance Criteria:**
- [ ] Shows invoice date, total, savings
- [ ] Lists all products with prices
- [ ] Shows category assigned to each product
- [ ] Allows editing categories per product

---

## 🏷️ Epic 2: Product Categorization

### US-2.1: Automatic Categorization
**As a** user  
**I want** products to be automatically categorized  
**So that** I don't have to manually assign categories

**Acceptance Criteria:**
- [ ] New products are assigned a category on import
- [ ] Uses keyword matching as first approach
- [ ] Falls back to "Overig" if no match
- [ ] Shows categorization source (keyword/llm/manual)

**Technical Notes:**
- 3-tier system: User Overrides → Keywords → LLM
- Keywords defined in `category_keywords` table
- LLM uses OpenAI API (cost: ~$0.0005/product)

### US-2.2: Manual Categorization
**As a** user  
**I want to** correct category assignments  
**So that** analysis is accurate

**Acceptance Criteria:**
- [ ] User can change category via dropdown
- [ ] Option to apply change to all similar products
- [ ] Correction is learned for future imports
- [ ] Shows count of affected products

### US-2.3: LLM Categorization
**As a** user  
**I want to** use AI to categorize unknown products  
**So that** I don't have to manually categorize everything

**Acceptance Criteria:**
- [ ] User can trigger LLM categorization
- [ ] Shows estimated cost before running
- [ ] Batches up to 100 products per API call
- [ ] Shows results with confidence scores
- [ ] Saves successful categorizations as keywords

**Technical Notes:**
- Service: `services/llm_categorizer.py`
- Default model: gpt-4o-mini
- Supports subcategories

### US-2.4: Category Management
**As a** user  
**I want to** add custom categories  
**So that** I can organize products my way

**Acceptance Criteria:**
- [ ] User can add new categories
- [ ] Can set icon and color
- [ ] Can add subcategories
- [ ] Can delete categories (products move to "Overig")

---

## 💰 Epic 3: Budget Management

### US-3.1: Set Category Budgets
**As a** user  
**I want to** set spending budgets per category  
**So that** I can control my spending

**Acceptance Criteria:**
- [ ] User can set target amount per category
- [ ] Budget is for current calendar year
- [ ] Shows progress bar (spent vs budget)
- [ ] Can adjust budget at any time

**Technical Notes:**
- Service: `services/budget.py`
- Year handling must be dynamic (not hardcoded)

### US-3.2: Auto-Generate Budgets
**As a** user  
**I want to** generate budgets from last year's spending  
**So that** I have a realistic starting point

**Acceptance Criteria:**
- [ ] Option: "Same as last year"
- [ ] Option: "10% less than last year"
- [ ] Option: "Custom reduction %"
- [ ] Shows comparison with previous year

### US-3.3: Budget Dashboard
**As a** user  
**I want to** see my budget status at a glance  
**So that** I know if I'm on track

**Acceptance Criteria:**
- [ ] Shows total budget vs total spent
- [ ] Progress bar for overall budget
- [ ] Category breakdown with progress bars
- [ ] Alerts for categories over 80% or 100%
- [ ] Shows on homepage

### US-3.4: Budget Alerts
**As a** user  
**I want to** be warned when approaching budget limits  
**So that** I can adjust my spending

**Acceptance Criteria:**
- [ ] Alert at 80% of budget (warning)
- [ ] Alert at 100% of budget (exceeded)
- [ ] Alert at 120% of budget (critical)
- [ ] User can dismiss alerts
- [ ] Alerts persist until dismissed

---

## 📊 Epic 4: Analysis & Insights

### US-4.1: Spending Overview
**As a** user  
**I want to** see my total spending  
**So that** I understand my grocery costs

**Acceptance Criteria:**
- [ ] Total spent (current year / all time)
- [ ] Total saved (bonus discounts)
- [ ] Average per order
- [ ] Average per month
- [ ] Number of orders

### US-4.2: Category Analysis
**As a** user  
**I want to** see spending per category  
**So that** I know where my money goes

**Acceptance Criteria:**
- [ ] Pie/donut chart of categories
- [ ] Bar chart with amounts
- [ ] Percentage of total per category
- [ ] Expandable subcategories

### US-4.3: Time-Based Analysis
**As a** user  
**I want to** see spending over time  
**So that** I can spot trends

**Acceptance Criteria:**
- [ ] Monthly spending bar chart
- [ ] Year-over-year comparison
- [ ] Day-of-week analysis
- [ ] Filter by year

### US-4.4: 30-Year Projection
**As a** user  
**I want to** see long-term cost of my habits  
**So that** I'm motivated to change behavior

**Acceptance Criteria:**
- [ ] Projects current spending 30 years
- [ ] Shows per-product 30-year cost
- [ ] Assumes 7% compound growth
- [ ] Shows potential savings

### US-4.5: Product Details
**As a** user  
**I want to** see detailed info about a product  
**So that** I understand my purchasing patterns

**Acceptance Criteria:**
- [ ] Total spent on product
- [ ] Purchase count
- [ ] Price history chart
- [ ] Average price
- [ ] Price trend (up/down/stable)

---

## 🔍 Epic 5: Search & Filter

### US-5.1: Product Search
**As a** user  
**I want to** search for products  
**So that** I can find specific items

**Acceptance Criteria:**
- [ ] Search by product name
- [ ] Filter by category
- [ ] Sort by amount/count/name
- [ ] Click to see product details

### US-5.2: Year Filter
**As a** user  
**I want to** filter data by year  
**So that** I can compare periods

**Acceptance Criteria:**
- [ ] Year selector on dashboard
- [ ] Persists across page navigation
- [ ] Option for "All time"
- [ ] Remembers last selection

---

## 🤖 Epic 6: AI Agent Support

### US-6.1: API Completeness
**As an** AI agent  
**I want** API access to all features  
**So that** I can automate tasks

**Acceptance Criteria:**
- [ ] All CRUD operations via API
- [ ] Consistent JSON responses
- [ ] Error messages are informative
- [ ] No feature is UI-only

### US-6.2: Cost Estimation
**As an** AI agent  
**I want** to estimate costs before operations  
**So that** I can make cost-aware decisions

**Acceptance Criteria:**
- [ ] Endpoint to estimate LLM costs
- [ ] Shows tokens and USD estimate
- [ ] Tracks cumulative costs

### US-6.3: Documentation
**As an** AI agent  
**I want** comprehensive documentation  
**So that** I understand how to use the system

**Acceptance Criteria:**
- [ ] CLAUDE.md with architecture overview
- [ ] API.md with endpoint documentation
- [ ] REQUIREMENTS.md with user stories
- [ ] Inline code documentation

---

## 🛡️ Non-Functional Requirements

### NFR-1: Privacy
- All data stored locally
- No cloud sync or external services (except OpenAI)
- No analytics or tracking

### NFR-2: Performance
- Dashboard loads in < 2 seconds
- Search results in < 500ms
- Import 100 invoices in < 30 seconds

### NFR-3: Reliability
- Graceful handling of malformed PDFs
- Retry logic for API calls
- Database backup capability

### NFR-4: Usability
- Mobile-responsive design
- Intuitive navigation
- Clear error messages

### NFR-5: Maintainability
- Clear code structure
- Comprehensive documentation
- Type hints in Python code

---

## 📋 Feature Backlog

See `SELECTED_IMPROVEMENTS.md` for the prioritized backlog of future improvements.

### Priority 1 (Must Have)
- [ ] Dynamic year handling (budget shows current year)
- [ ] Robust LLM categorization with cost tracking
- [ ] Budget dashboard on homepage

### Priority 2 (Should Have)
- [ ] Bulk product selection and categorization
- [ ] Keyboard shortcuts
- [ ] Undo functionality

### Priority 3 (Could Have)
- [ ] Dark mode
- [ ] Export to CSV/Excel
- [ ] Multi-language support

### Priority 4 (Won't Have - This Phase)
- [ ] Multi-supermarket support
- [ ] Meal planning
- [ ] Social features

---

## ✅ Definition of Done

A feature is complete when:
1. All acceptance criteria are met
2. Code is reviewed and follows style guide
3. Feature works on mobile and desktop
4. Error cases are handled gracefully
5. Documentation is updated
6. API endpoint exists (if applicable)

---

## 🔄 Change Log

| Date | Version | Changes |
|------|---------|---------|
| 2026-01-06 | 1.0 | Initial requirements document |

