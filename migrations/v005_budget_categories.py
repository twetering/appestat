"""
Migration: Add category-level budget support.
Extends the budget system with category targets and alerts.
"""

VERSION = "005"
NAME = "budget_categories"
DESCRIPTION = "Add category-level budget tracking and alerts"

UP_SQL = """
-- Create budget periods table
CREATE TABLE IF NOT EXISTS budget_periods (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    year INTEGER NOT NULL,
    name TEXT NOT NULL,
    start_date DATE,
    end_date DATE,
    total_target REAL,
    status TEXT DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(year, name)
);

-- Create category budgets table
CREATE TABLE IF NOT EXISTS budget_categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    period_id INTEGER NOT NULL,
    category_id INTEGER NOT NULL,
    target_amount REAL,
    warning_threshold REAL DEFAULT 0.8,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (period_id) REFERENCES budget_periods(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE,
    UNIQUE(period_id, category_id)
);

-- Create budget alerts table
CREATE TABLE IF NOT EXISTS budget_alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    budget_category_id INTEGER,
    budget_goal_id INTEGER,
    alert_type TEXT NOT NULL,
    message TEXT,
    threshold_percentage REAL,
    triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_dismissed BOOLEAN DEFAULT 0,
    dismissed_at TIMESTAMP,
    FOREIGN KEY (budget_category_id) REFERENCES budget_categories(id) ON DELETE CASCADE,
    FOREIGN KEY (budget_goal_id) REFERENCES budget_goals(id) ON DELETE CASCADE
);

-- Add category_id to budget_goals for linking
ALTER TABLE budget_goals ADD COLUMN category_id INTEGER REFERENCES categories(id);

-- Insert default budget period for 2026
INSERT OR IGNORE INTO budget_periods (year, name, start_date, end_date, status)
VALUES (2026, 'Jaarbudget 2026', '2026-01-01', '2026-12-31', 'active')
"""

DOWN_SQL = """
DROP TABLE IF EXISTS budget_alerts;
DROP TABLE IF EXISTS budget_categories;
DROP TABLE IF EXISTS budget_periods
"""


