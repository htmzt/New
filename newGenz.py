# app/services/overview_charts_service.py
from typing import Dict, Any, List
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.services.base_service import BaseService
from app.query import MERGED_DATA_QUERY
from datetime import datetime, timedelta
import calendar
import logging

logger = logging.getLogger(__name__)


class OverviewChartsService(BaseService):
    """Service for overview charts - received vs paid POs across time periods"""
    
    def __init__(self, db: Session):
        super().__init__(db)
    
    def get_overview_charts(self, user_id: str) -> Dict[str, Any]:
        """
        Get overview charts data for all time periods (current periods only)
        
        NEW LOGIC:
        - Total PO (Received): Based on publish_date (when PO was issued)
        - Total Paid: Based on ac_date and pac_date (when payments were made)
        - Total Closed: Based on when PAC date occurs (final payment)
        
        Args:
            user_id: User ID to filter by
        
        Returns:
            Dictionary with charts data for Total, Weekly, Monthly, Quarter, Yearly
        """
        try:
            # Build base filter
            base_filter = "po.user_id = :user_id"
            params = {"user_id": user_id}
            
            # Get current date info
            today = datetime.now().date()
            
            # Calculate date ranges for current periods
            date_ranges = self._calculate_current_date_ranges(today)
            params.update(date_ranges)
            
            # Build and execute query with AC/PAC date logic
            overview_query = f"""
            WITH base_data AS (
                SELECT 
                    publish_date,
                    ac_date,
                    pac_date,
                    status,
                    line_amount,
                    ac_amount,
                    pac_amount,
                    payment_terms
                FROM (
                    {MERGED_DATA_QUERY.format(base_filter=base_filter)}
                ) AS merged
                WHERE publish_date IS NOT NULL
            )
            SELECT 
                -- ========== TOTAL (All Time) ==========
                -- Total PO: Based on publish_date
                SUM(CASE 
                    WHEN status != 'CANCELLED' 
                    THEN COALESCE(line_amount, 0) 
                    ELSE 0 
                END) as total_received,
                
                -- Total Paid: Based on AC + PAC dates (when payments actually happened)
                SUM(
                    CASE WHEN ac_date IS NOT NULL THEN COALESCE(ac_amount, 0) ELSE 0 END +
                    CASE WHEN pac_date IS NOT NULL THEN COALESCE(pac_amount, 0) ELSE 0 END
                ) as total_paid,
                
                -- ========== WEEKLY (Current Week) ==========
                -- Total PO published this week
                SUM(CASE 
                    WHEN publish_date >= :week_start 
                    AND publish_date <= :week_end 
                    AND status != 'CANCELLED'
                    THEN COALESCE(line_amount, 0) 
                    ELSE 0 
                END) as weekly_received,
                
                -- Total Paid this week (AC + PAC payments made this week)
                SUM(
                    CASE 
                        WHEN ac_date >= :week_start AND ac_date <= :week_end 
                        THEN COALESCE(ac_amount, 0) 
                        ELSE 0 
                    END +
                    CASE 
                        WHEN pac_date >= :week_start AND pac_date <= :week_end 
                        THEN COALESCE(pac_amount, 0) 
                        ELSE 0 
                    END
                ) as weekly_paid,
                
                -- ========== MONTHLY (Current Month) ==========
                -- Total PO published this month
                SUM(CASE 
                    WHEN publish_date >= :month_start 
                    AND publish_date <= :month_end 
                    AND status != 'CANCELLED'
                    THEN COALESCE(line_amount, 0) 
                    ELSE 0 
                END) as monthly_received,
                
                -- Total Paid this month (AC + PAC payments made this month)
                SUM(
                    CASE 
                        WHEN ac_date >= :month_start AND ac_date <= :month_end 
                        THEN COALESCE(ac_amount, 0) 
                        ELSE 0 
                    END +
                    CASE 
                        WHEN pac_date >= :month_start AND pac_date <= :month_end 
                        THEN COALESCE(pac_amount, 0) 
                        ELSE 0 
                    END
                ) as monthly_paid,
                
                -- ========== QUARTER (Current Quarter) ==========
                -- Total PO published this quarter
                SUM(CASE 
                    WHEN publish_date >= :quarter_start 
                    AND publish_date <= :quarter_end 
                    AND status != 'CANCELLED'
                    THEN COALESCE(line_amount, 0) 
                    ELSE 0 
                END) as quarter_received,
                
                -- Total Paid this quarter (AC + PAC payments made this quarter)
                SUM(
                    CASE 
                        WHEN ac_date >= :quarter_start AND ac_date <= :quarter_end 
                        THEN COALESCE(ac_amount, 0) 
                        ELSE 0 
                    END +
                    CASE 
                        WHEN pac_date >= :quarter_start AND pac_date <= :quarter_end 
                        THEN COALESCE(pac_amount, 0) 
                        ELSE 0 
                    END
                ) as quarter_paid,
                
                -- ========== YEARLY (Current Year) ==========
                -- Total PO published this year
                SUM(CASE 
                    WHEN publish_date >= :year_start 
                    AND publish_date <= :year_end 
                    AND status != 'CANCELLED'
                    THEN COALESCE(line_amount, 0) 
                    ELSE 0 
                END) as yearly_received,
                
                -- Total Paid this year (AC + PAC payments made this year)
                SUM(
                    CASE 
                        WHEN ac_date >= :year_start AND ac_date <= :year_end 
                        THEN COALESCE(ac_amount, 0) 
                        ELSE 0 
                    END +
                    CASE 
                        WHEN pac_date >= :year_start AND pac_date <= :year_end 
                        THEN COALESCE(pac_amount, 0) 
                        ELSE 0 
                    END
                ) as yearly_paid
                
            FROM base_data
            """
            
            result = self.db.execute(text(overview_query), params).first()
            
            if not result:
                return self._empty_response()
            
            # Format response
            overview_data = [
                self._format_period_data(
                    "Total",
                    "All Time",
                    float(result.total_received or 0),
                    float(result.total_paid or 0)
                ),
                self._format_period_data(
                    "Weekly",
                    date_ranges['week_label'],
                    float(result.weekly_received or 0),
                    float(result.weekly_paid or 0)
                ),
                self._format_period_data(
                    "Monthly",
                    date_ranges['month_label'],
                    float(result.monthly_received or 0),
                    float(result.monthly_paid or 0)
                ),
                self._format_period_data(
                    "Quarter",
                    date_ranges['quarter_label'],
                    float(result.quarter_received or 0),
                    float(result.quarter_paid or 0)
                ),
                self._format_period_data(
                    "Yearly",
                    date_ranges['year_label'],
                    float(result.yearly_received or 0),
                    float(result.yearly_paid or 0)
                )
            ]
            
            return {
                "success": True,
                "overview_charts": overview_data,
                "generated_at": datetime.now().isoformat(),
                "logic_explanation": {
                    "total_po": "Based on publish_date (when PO was issued)",
                    "total_paid": "Based on ac_date and pac_date (when payments were made)",
                    "pending": "Calculated as: Total PO - Total Paid"
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting overview charts: {str(e)}")
            raise
    
    def _calculate_current_date_ranges(self, today: datetime.date) -> Dict[str, Any]:
        """Calculate all date ranges for current periods"""
        
        # Weekly (current week - Monday to Sunday)
        week_start = today - timedelta(days=today.weekday())  # Monday
        week_end = week_start + timedelta(days=6)  # Sunday
        week_label = f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}"
        
        # Monthly (current month)
        month_start = today.replace(day=1)
        last_day = calendar.monthrange(today.year, today.month)[1]
        month_end = today.replace(day=last_day)
        month_label = today.strftime('%B %Y')
        
        # Quarter (current quarter)
        current_quarter = (today.month - 1) // 3 + 1
        quarter_start_month = (current_quarter - 1) * 3 + 1
        quarter_start = datetime(today.year, quarter_start_month, 1).date()
        quarter_end_month = quarter_start_month + 2
        last_day_quarter = calendar.monthrange(today.year, quarter_end_month)[1]
        quarter_end = datetime(today.year, quarter_end_month, last_day_quarter).date()
        quarter_label = f"Q{current_quarter} {today.year}"
        
        # Yearly (current year)
        year_start = datetime(today.year, 1, 1).date()
        year_end = datetime(today.year, 12, 31).date()
        year_label = str(today.year)
        
        return {
            'week_start': week_start,
            'week_end': week_end,
            'week_label': week_label,
            'month_start': month_start,
            'month_end': month_end,
            'month_label': month_label,
            'quarter_start': quarter_start,
            'quarter_end': quarter_end,
            'quarter_label': quarter_label,
            'year_start': year_start,
            'year_end': year_end,
            'year_label': year_label
        }
    
    def _format_period_data(
        self, 
        period: str, 
        date_range: str, 
        received: float, 
        paid: float
    ) -> Dict[str, Any]:
        """Format a single period's data"""
        pending = received - paid
        completion_percentage = round((paid / received * 100), 2) if received > 0 else 0
        
        return {
            "period": period,
            "date_range": date_range,
            "received_po": round(received, 2),
            "paid_po": round(paid, 2),
            "pending_po": round(pending, 2),
            "completion_percentage": completion_percentage,
            "chart_data": {
                "received": round(received, 2),
                "paid": round(paid, 2),
                "pending": round(pending, 2),
                "paid_percentage": completion_percentage
            }
        }
    
    def _empty_response(self) -> Dict[str, Any]:
        """Return empty response structure"""
        today = datetime.now().date()
        date_ranges = self._calculate_current_date_ranges(today)
        
        return {
            "success": True,
            "overview_charts": [
                self._format_period_data("Total", "All Time", 0, 0),
                self._format_period_data("Weekly", date_ranges['week_label'], 0, 0),
                self._format_period_data("Monthly", date_ranges['month_label'], 0, 0),
                self._format_period_data("Quarter", date_ranges['quarter_label'], 0, 0),
                self._format_period_data("Yearly", date_ranges['year_label'], 0, 0)
            ],
            "generated_at": datetime.now().isoformat()
        }