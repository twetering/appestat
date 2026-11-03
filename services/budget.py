"""
Budget Service for Appestat.
Handles category and product-level budgets, tracking, and alerts.
"""

from datetime import date
from typing import Dict, List, Optional
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from database import get_connection
from services import metrics


def ensure_extra_costs_category() -> None:
    """'Bijkomende kosten' (statiegeld, bezorging, abonnementen) is budgetable like a category."""
    conn = get_connection()
    conn.execute("""
        INSERT OR IGNORE INTO categories (name, color, icon, is_system, sort_order)
        VALUES (?, '#78909C', '🧾', 1, 999)
    """, (metrics.EXTRA_COSTS_CATEGORY,))
    conn.commit()
    conn.close()


class BudgetService:
    """
    Budget management service.
    Handles category-level and product-level budget tracking.
    """
    
    def get_budget_period(self, year: int) -> Optional[Dict]:
        """Get or create budget period for a year."""
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM budget_periods WHERE year = ?
        """, (year,))
        
        period = cursor.fetchone()
        
        if not period:
            # Create default period
            cursor.execute("""
                INSERT INTO budget_periods (year, name, start_date, end_date, status)
                VALUES (?, ?, ?, ?, 'active')
            """, (year, f'Jaarbudget {year}', f'{year}-01-01', f'{year}-12-31'))
            conn.commit()
            
            cursor.execute("SELECT * FROM budget_periods WHERE year = ?", (year,))
            period = cursor.fetchone()
        
        conn.close()
        return dict(period) if period else None
    
    def get_category_spending(self, year: int) -> Dict[str, Dict]:
        """Actual spending per category for a year (services.metrics, incl. Bijkomende kosten)."""
        return {name: {'spent': v['total'], 'count': v['count']}
                for name, v in metrics.category_totals(year).items()}
    
    def get_category_budgets(self, year: int) -> List[Dict]:
        """Get all category budgets for a year with actual spending."""
        period = self.get_budget_period(year)
        if not period:
            return []
        
        ensure_extra_costs_category()
        current = metrics.category_totals(year)
        reference = metrics.category_totals(year - 1)

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT c.id as category_id, c.name as category_name, c.icon, c.color,
                   bc.target_amount, bc.warning_threshold
            FROM categories c
            LEFT JOIN budget_categories bc ON bc.category_id = c.id AND bc.period_id = ?
        """, (period['id'],))
        rows = [dict(r) for r in cursor.fetchall()]
        conn.close()

        results = []
        for data in sorted(rows, key=lambda r: -reference.get(r['category_name'], {}).get('total', 0)):
            data['current_spent'] = current.get(data['category_name'], {}).get('total', 0.0)
            data['reference_spent'] = reference.get(data['category_name'], {}).get('total', 0.0)
            if not (data['current_spent'] or data['reference_spent'] or data['target_amount'] is not None):
                continue
            
            # Calculate progress percentage
            if data['target_amount'] and data['target_amount'] > 0:
                data['progress'] = (data['current_spent'] / data['target_amount']) * 100
            else:
                data['progress'] = None
            
            # Calculate change from reference year
            if data['reference_spent'] > 0:
                data['change_percentage'] = (
                    (data['current_spent'] - data['reference_spent']) / data['reference_spent']
                ) * 100
            else:
                data['change_percentage'] = None

            results.append(data)

        return results
    
    def set_category_budget(
        self, 
        year: int, 
        category_id: int, 
        target_amount: float,
        warning_threshold: float = 0.8,
        notes: str = None
    ) -> Dict:
        """Set or update a category budget."""
        period = self.get_budget_period(year)
        if not period:
            return {"success": False, "error": "Could not create budget period"}
        
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO budget_categories (period_id, category_id, target_amount, warning_threshold, notes)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(period_id, category_id) DO UPDATE SET
                target_amount = excluded.target_amount,
                warning_threshold = excluded.warning_threshold,
                notes = excluded.notes,
                updated_at = CURRENT_TIMESTAMP
        """, (period['id'], category_id, target_amount, warning_threshold, notes))
        
        conn.commit()
        conn.close()
        
        return {"success": True, "budget_id": cursor.lastrowid}
    
    def generate_category_budgets_from_history(
        self, 
        target_year: int, 
        reduction_percentage: float = 0
    ) -> List[Dict]:
        """
        Auto-generate category budgets based on historical spending.
        
        Args:
            target_year: Year to create budgets for
            reduction_percentage: Optional overall reduction (e.g., 0.1 for 10% less)
        """
        reference_year = target_year - 1
        spending = self.get_category_spending(reference_year)
        
        results = []
        
        conn = get_connection()
        cursor = conn.cursor()
        
        for category_name, data in spending.items():
            # Get category ID
            cursor.execute("SELECT id FROM categories WHERE name = ?", (category_name,))
            cat_row = cursor.fetchone()
            if not cat_row:
                continue
            
            # Calculate target (with optional reduction)
            target = data['spent'] * (1 - reduction_percentage)
            
            result = self.set_category_budget(
                target_year, 
                cat_row['id'], 
                target
            )
            
            results.append({
                "category": category_name,
                "reference_spent": data['spent'],
                "target_amount": target,
                "success": result.get("success", False)
            })
        
        conn.close()
        return results
    
    def get_budget_dashboard(self, year: int) -> Dict:
        """Get comprehensive budget dashboard data."""
        period = self.get_budget_period(year)
        reference_year = year - 1

        current = metrics.summary(year)
        current_total = current['spent']
        reference_total = metrics.summary(reference_year)['spent']
        category_budgets = self.get_category_budgets(year)
        total_budget = sum(cb.get('target_amount') or 0 for cb in category_budgets)
        total_spent = current_total
        monthly_spending = {k[-2:]: v for k, v in metrics.monthly_spent(year).items()}
        alerts = self._get_active_alerts(year)

        # Same projection as the dashboard: average per calendar month so far x 12
        projected_annual = metrics.prediction(year)['predicted_yearly'] if year >= date.today().year \
            else current_total
        # Budget pace: share of the year's budget you may have used by the latest receipt
        months_done = current['months'] if year >= date.today().year else 12
        expected_by_now = total_budget * months_done / 12 if total_budget else None

        return {
            "period": period,
            "current_year": year,
            "reference_year": reference_year,
            "total_budget": total_budget,
            "total_spent": total_spent,
            "reference_total": reference_total,
            "projected_annual": projected_annual,
            "budget_progress": (total_spent / total_budget * 100) if total_budget > 0 else None,
            "categories": category_budgets,
            "monthly_spending": monthly_spending,
            "alerts": alerts,
            "savings_vs_last_year": reference_total - current_total,
            "expected_by_now": round(expected_by_now, 2) if expected_by_now is not None else None,
            "months_elapsed": months_done,
            # On track = not ahead of the linear budget pace (not just "below the full-year budget")
            "on_track": total_spent <= expected_by_now if expected_by_now else None
        }
    
    def _get_active_alerts(self, year: int) -> List[Dict]:
        """Get active (non-dismissed) budget alerts."""
        period = self.get_budget_period(year)
        if not period:
            return []
        
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                ba.*,
                c.name as category_name,
                c.icon as category_icon
            FROM budget_alerts ba
            LEFT JOIN budget_categories bc ON ba.budget_category_id = bc.id
            LEFT JOIN categories c ON bc.category_id = c.id
            WHERE bc.period_id = ? AND ba.is_dismissed = 0
            ORDER BY ba.triggered_at DESC
        """, (period['id'],))
        
        alerts = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return alerts
    
    def check_and_create_alerts(self, year: int) -> List[Dict]:
        """Check budgets and create alerts for thresholds crossed."""
        category_budgets = self.get_category_budgets(year)
        new_alerts = []
        
        conn = get_connection()
        cursor = conn.cursor()
        
        for cb in category_budgets:
            if not cb.get('target_amount'):
                continue
            
            progress = cb['current_spent'] / cb['target_amount']
            threshold = cb.get('warning_threshold', 0.8)
            
            # Check 80% threshold
            if progress >= threshold and progress < 1.0:
                alert = self._create_alert(
                    cursor, cb, 'warning',
                    f"Je hebt {progress*100:.0f}% van je {cb['category_name']} budget gebruikt",
                    threshold * 100
                )
                if alert:
                    new_alerts.append(alert)
            
            # Check 100% threshold
            elif progress >= 1.0 and progress < 1.2:
                alert = self._create_alert(
                    cursor, cb, 'exceeded',
                    f"Je {cb['category_name']} budget is overschreden!",
                    100
                )
                if alert:
                    new_alerts.append(alert)
            
            # Check 120% threshold
            elif progress >= 1.2:
                alert = self._create_alert(
                    cursor, cb, 'critical',
                    f"Je {cb['category_name']} budget is al {progress*100:.0f}% - veel te hoog!",
                    120
                )
                if alert:
                    new_alerts.append(alert)
        
        conn.commit()
        conn.close()
        
        return new_alerts
    
    def _create_alert(self, cursor, category_budget, alert_type, message, threshold):
        """Create an alert if one doesn't already exist for this threshold."""
        # Check if similar alert already exists
        cursor.execute("""
            SELECT id FROM budget_alerts 
            WHERE budget_category_id = ? 
              AND alert_type = ? 
              AND threshold_percentage = ?
        """, (category_budget.get('id'), alert_type, threshold))
        
        if cursor.fetchone():
            return None  # Alert already exists
        
        # Note: budget_category_id would need to be fetched from budget_categories table
        # For now, we'll create a simpler version
        cursor.execute("""
            SELECT bc.id 
            FROM budget_categories bc
            JOIN categories c ON bc.category_id = c.id
            WHERE c.name = ?
        """, (category_budget['category_name'],))
        
        bc_row = cursor.fetchone()
        if not bc_row:
            return None
        
        cursor.execute("""
            INSERT INTO budget_alerts (budget_category_id, alert_type, message, threshold_percentage)
            VALUES (?, ?, ?, ?)
        """, (bc_row['id'], alert_type, message, threshold))
        
        return {
            "type": alert_type,
            "message": message,
            "category": category_budget['category_name']
        }
    
    def dismiss_alert(self, alert_id: int) -> bool:
        """Dismiss an alert."""
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE budget_alerts 
            SET is_dismissed = 1, dismissed_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (alert_id,))
        
        success = cursor.rowcount > 0
        conn.commit()
        conn.close()
        
        return success


# Singleton instance
_service = None


def get_budget_service() -> BudgetService:
    global _service
    if _service is None:
        _service = BudgetService()
    return _service


