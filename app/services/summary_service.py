
# app/services/summary_builder_service.py
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
from app.services.base_service import BaseService
from app.query import MERGED_DATA_QUERY
from app.utils import aggregation_helpers as agg
import logging

logger = logging.getLogger(__name__)


class SummaryBuilderService(BaseService):
    """
    Unified summary builder that works for any time period.
    
    Instead of having separate methods for monthly/yearly/weekly,
    we have ONE method that handles all period types.
    """
    
    def __init__(self, db: Session):
        super().__init__(db)
    
    def get_summary(
        self,
        user_id: str,
        period_type: str = "monthly",
        year: Optional[int] = None,
        month: Optional[int] = None,
        week: Optional[int] = None,
        project_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get summary for any period type.
        
        Args:
            user_id: User ID to filter by
            period_type: "weekly", "monthly", or "yearly"
            year: Optional year filter
            month: Optional month filter (only for monthly)
            week: Optional week filter (only for weekly)
            project_name: Optional project name filter
        
        Returns:
            Dictionary with summaries and overall totals
        """
        try:
            # Step 1: Build base filter for merged data query
            base_filter = "po.user_id = :user_id"
            params = {"user_id": user_id}
            
            # Add project filter if provided
            if project_name:
                base_filter += " AND po.project_name ILIKE :project_name"
                params["project_name"] = f"%{project_name}%"
            
            # Step 2: Get period-specific SQL fragments
            period_grouping = agg.get_period_grouping(period_type)
            period_filter, period_params = agg.get_period_filter(
                period_type, year, month, week
            )
            params.update(period_params)
            
            # Step 3: Build the complete query
            # This assembles all the SQL fragments into one query
            summary_query = f"""
            SELECT 
                subquery.project_name,
                {period_grouping},
                
                -- All financial metrics
                {agg.get_financial_aggregations()},
                
                -- All status counts
                {agg.get_status_aggregations()},
                
                -- Payment terms breakdown
                {agg.get_payment_terms_aggregations()},
                
                -- Category breakdown
                {agg.get_category_aggregations()},
                
                -- Date range
                {agg.get_date_range_aggregations()}
                
            FROM (
                {MERGED_DATA_QUERY.format(base_filter=base_filter)}
            ) as subquery
            WHERE subquery.publish_date IS NOT NULL
                AND {period_filter}
            GROUP BY 
                subquery.project_name,
                {self._get_group_by_fields(period_type)}
            ORDER BY 
                {self._get_order_by_fields(period_type)}
            """
            
            # Step 4: Execute query
            result = self.db.execute(text(summary_query), params)
            
            # Step 5: Format results into nice JSON structure
            summaries = self._format_summaries(result, period_type)
            
            # Step 6: Calculate overall totals
            overall_totals = self._get_overall_totals(
                user_id, period_type, year, month, week, project_name
            )
            
            # Step 7: Return everything
            return {
                "summaries": summaries,
                "overall_totals": overall_totals,
                "period_type": period_type,
                "filters_applied": {
                    "year": year,
                    "month": month,
                    "week": week,
                    "project_name": project_name
                },
                "total_summary_records": len(summaries)
            }
            
        except Exception as e:
            logger.error(f"Error getting {period_type} summary: {str(e)}")
            raise
    
    def _get_group_by_fields(self, period_type: str) -> str:
        """
        Get the GROUP BY clause fields for SQL.
        
        Must match the columns in get_period_grouping().
        """
        if period_type == "weekly":
            return """
                DATE_TRUNC('week', publish_date),
                TO_CHAR(DATE_TRUNC('week', publish_date), 'YYYY-MM-DD'),
                EXTRACT(YEAR FROM DATE_TRUNC('week', publish_date)),
                EXTRACT(WEEK FROM publish_date)
            """
        elif period_type == "monthly":
            return """
                EXTRACT(YEAR FROM publish_date),
                EXTRACT(MONTH FROM publish_date),
                TO_CHAR(publish_date, 'Month YYYY')
            """
        elif period_type == "yearly":
            return "EXTRACT(YEAR FROM publish_date)"
        else:
            raise ValueError(f"Unknown period type: {period_type}")
    
    def _get_order_by_fields(self, period_type: str) -> str:
        """Get the ORDER BY clause for SQL."""
        if period_type == "weekly":
            return "year DESC, week_number DESC, subquery.project_name ASC"
        elif period_type == "monthly":
            return "year DESC, month DESC, subquery.project_name ASC"
        elif period_type == "yearly":
            return "year DESC, subquery.project_name ASC"
        else:
            return "subquery.project_name ASC"
    
    def _format_summaries(self, result, period_type: str) -> List[Dict]:
        """
        Format SQL results into JSON structure.
        
        Takes raw database rows and converts to nice dictionaries.
        """
        summaries = []
        
        for row in result:
            # Calculate completion rate
            completion_rate = 0
            if row.total_records > 0:
                completion_rate = round((row.closed_count / row.total_records) * 100, 2)
            
            # Build summary object
            summary = {
                "project_name": row.project_name,
                "period_label": row.period_label,
                
                # Counts
                "total_records": row.total_records,
                "unique_pos": row.unique_pos,
                
                # Financial summary
                "financial_summary": {
                    "total_line_amount": float(row.total_line_amount) if row.total_line_amount else 0,
                    "total_ac_amount": float(row.total_ac_amount) if row.total_ac_amount else 0,
                    "total_pac_amount": float(row.total_pac_amount) if row.total_pac_amount else 0,
                    "total_remaining_amount": float(row.total_remaining_amount) if row.total_remaining_amount else 0,
                },
                
                # Status breakdown
                "status_breakdown": {
                    "closed": row.closed_count,
                    "cancelled": row.cancelled_count,
                    "pending": row.pending_count,
                    "completion_rate": completion_rate
                },
                
                # Payment terms
                "payment_terms_breakdown": {
                    "acpac_100_percent": row.acpac_100_count,
                    "ac_pac_split": row.ac_pac_split_count
                },
                
                # Categories
                "category_breakdown": {
                    "survey": row.survey_count,
                    "transportation": row.transportation_count,
                    "site_engineer": row.site_engineer_count,
                    "service": row.service_count
                },
                
                # Date range
                "date_range": {
                    "earliest_date": row.earliest_date.isoformat() if row.earliest_date else None,
                    "latest_date": row.latest_date.isoformat() if row.latest_date else None
                }
            }
            
            # Add period-specific fields
            if period_type == "yearly":
                summary["year"] = int(row.year) if hasattr(row, 'year') else None
            
            elif period_type == "monthly":
                summary["year"] = int(row.year) if hasattr(row, 'year') else None
                summary["month"] = int(row.month) if hasattr(row, 'month') else None
            
            elif period_type == "weekly":
                summary["year"] = int(row.year) if hasattr(row, 'year') else None
                summary["week_number"] = int(row.week_number) if hasattr(row, 'week_number') else None
                summary["period_date"] = row.period_date.isoformat() if hasattr(row, 'period_date') else None
            
            summaries.append(summary)
        
        return summaries
    
    def _get_overall_totals(
        self,
        user_id: str,
        period_type: str,
        year: Optional[int],
        month: Optional[int],
        week: Optional[int],
        project_name: Optional[str]
    ) -> Dict[str, Any]:
        """Calculate overall totals for the filtered dataset."""
        try:
            # Same base filter as main query
            base_filter = "po.user_id = :user_id"
            params = {"user_id": user_id}
            
            if project_name:
                base_filter += " AND po.project_name ILIKE :project_name"
                params["project_name"] = f"%{project_name}%"
            
            # Get period filter
            period_filter, period_params = agg.get_period_filter(
                period_type, year, month, week
            )
            params.update(period_params)
            
            # Build totals query
            totals_query = f"""
            SELECT 
                {agg.get_financial_aggregations()},
                COUNT(DISTINCT subquery.po_id) as unique_pos,
                COUNT(DISTINCT subquery.project_name) as unique_projects,
                COUNT(CASE WHEN subquery.status = 'CLOSED' THEN 1 END) as total_closed
            FROM (
                {MERGED_DATA_QUERY.format(base_filter=base_filter)}
            ) as subquery
            WHERE subquery.publish_date IS NOT NULL
                AND {period_filter}
            """
            
            result = self.db.execute(text(totals_query), params).first()
            
            if not result or result.total_records == 0:
                return {
                    "total_records": 0,
                    "unique_pos": 0,
                    "unique_projects": 0,
                    "financial_totals": {
                        "total_line_amount": 0,
                        "total_ac_amount": 0,
                        "total_pac_amount": 0,
                        "total_remaining_amount": 0,
                    },
                    "overall_completion_rate": 0
                }
            
            return {
                "total_records": result.total_records,
                "unique_pos": result.unique_pos,
                "unique_projects": result.unique_projects,
                "financial_totals": {
                    "total_line_amount": float(result.total_line_amount) if result.total_line_amount else 0,
                    "total_ac_amount": float(result.total_ac_amount) if result.total_ac_amount else 0,
                    "total_pac_amount": float(result.total_pac_amount) if result.total_pac_amount else 0,
                    "total_remaining_amount": float(result.total_remaining_amount) if result.total_remaining_amount else 0,
                },
                "overall_completion_rate": round((result.total_closed / result.total_records) * 100, 2)
            }
            
        except Exception as e:
            logger.error(f"Error getting overall totals: {str(e)}")
            return {
                "total_records": 0,
                "unique_pos": 0,
                "unique_projects": 0,
                "financial_totals": {
                    "total_line_amount": 0,
                    "total_ac_amount": 0,
                    "total_pac_amount": 0,
                    "total_remaining_amount": 0,
                },
                "overall_completion_rate": 0
            }
    
    def get_available_periods(self, user_id: str, period_type: str = "monthly") -> Dict[str, Any]:
        """
        Get list of available periods in the data.
        Used by frontend to populate filter dropdowns.
        """
        try:
            if period_type == "weekly":
                # Get all weeks that have data
                periods_query = f"""
                SELECT DISTINCT
                    EXTRACT(YEAR FROM subquery.publish_date) as year,
                    EXTRACT(WEEK FROM subquery.publish_date) as week_number,
                    TO_CHAR(DATE_TRUNC('week', subquery.publish_date), 'YYYY-MM-DD') as week_start,
                    COUNT(*) as record_count
                FROM (
                    {MERGED_DATA_QUERY.format(base_filter="po.user_id = :user_id")}
                ) as subquery
                WHERE subquery.publish_date IS NOT NULL
                GROUP BY year, week_number, week_start
                ORDER BY year DESC, week_number DESC
                """
                
                result = self.db.execute(text(periods_query), {"user_id": user_id})
                
                return {
                    "period_type": "weekly",
                    "periods": [
                        {
                            "year": int(row.year),
                            "week_number": int(row.week_number),
                            "week_start": row.week_start,
                            "record_count": row.record_count
                        }
                        for row in result
                    ]
                }
            
            elif period_type == "monthly":
                # Get all months that have data
                periods_query = f"""
                SELECT DISTINCT
                    EXTRACT(YEAR FROM subquery.publish_date) as year,
                    EXTRACT(MONTH FROM subquery.publish_date) as month,
                    TO_CHAR(subquery.publish_date, 'FMMonth') as month_name,
                    COUNT(*) as record_count
                FROM (
                    {MERGED_DATA_QUERY.format(base_filter="po.user_id = :user_id")}
                ) as subquery
                WHERE subquery.publish_date IS NOT NULL
                GROUP BY year, month, month_name
                ORDER BY year DESC, month DESC
                """
                
                result = self.db.execute(text(periods_query), {"user_id": user_id})
                
                # Group by year
                years = {}
                for row in result:
                    year = int(row.year) if row.year else None
                    month = int(row.month) if row.month else None
                    
                    if year not in years:
                        years[year] = {"year": year, "months": [], "total_records": 0}
                    
                    years[year]["months"].append({
                        "month": month,
                        "month_name": row.month_name.strip() if row.month_name else None,
                        "record_count": row.record_count
                    })
                    years[year]["total_records"] += row.record_count
                
                return {
                    "period_type": "monthly",
                    "available_years": sorted(years.keys(), reverse=True) if years else [],
                    "year_details": list(years.values()) if years else []
                }
            
            else:  # yearly
                periods_query = f"""
                SELECT DISTINCT
                    EXTRACT(YEAR FROM subquery.publish_date) as year,
                    COUNT(*) as record_count
                FROM (
                    {MERGED_DATA_QUERY.format(base_filter="po.user_id = :user_id")}
                ) as subquery
                WHERE subquery.publish_date IS NOT NULL
                GROUP BY year
                ORDER BY year DESC
                """
                
                result = self.db.execute(text(periods_query), {"user_id": user_id})
                
                return {
                    "period_type": "yearly",
                    "years": [
                        {
                            "year": int(row.year),
                            "record_count": row.record_count
                        }
                        for row in result
                    ]
                }
                
        except Exception as e:
            logger.error(f"Error getting available periods: {str(e)}")
            raise
    
    def get_project_list(self, user_id: str) -> List[str]:
        """Get list of all project names for filtering."""
        try:
            projects_query = f"""
            SELECT DISTINCT subquery.project_name
            FROM (
                {MERGED_DATA_QUERY.format(base_filter="po.user_id = :user_id")}
            ) as subquery
            WHERE subquery.project_name IS NOT NULL
            ORDER BY subquery.project_name ASC
            """
            
            result = self.db.execute(text(projects_query), {"user_id": user_id})
            return [row.project_name for row in result if row.project_name]
            
        except Exception as e:
            logger.error(f"Error getting project list: {str(e)}")
            raise