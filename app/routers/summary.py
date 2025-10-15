# app/routers/summary.py
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from typing import Optional
from io import BytesIO
import pandas as pd
import logging
from datetime import datetime, timedelta

from app.database import get_db
from app.auth import get_current_user
from app.models import User
from app.services.summary_service import SummaryBuilderService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/summary", tags=["summary"])


@router.get("/monthly")
async def get_monthly_summary(
    year: Optional[int] = Query(None, description="Filter by year (e.g., 2024)"),
    month: Optional[int] = Query(None, ge=1, le=12, description="Filter by month (1-12)"),
    project_name: Optional[str] = Query(None, description="Filter by project name"),
    page: int = Query(1, ge=1, description="Page number (starts at 1)"),
    per_page: int = Query(50, ge=1, le=500, description="Items per page (max 500)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get monthly summary by project name with pagination
    
    **NEW: Now paginated to prevent memory issues!**
    
    Returns aggregated data grouped by project and month.
    Use pagination to load data in chunks.
    
    **Query Parameters:**
    - `year`: Filter by specific year (optional)
    - `month`: Filter by specific month 1-12 (optional, requires year)
    - `project_name`: Filter by project name (optional)
    - `page`: Page number starting from 1 (default: 1)
    - `per_page`: Records per page, max 500 (default: 50)
    
    **Example:**
    - Get first page: `/api/summary/monthly?page=1&per_page=50`
    - Get specific month: `/api/summary/monthly?year=2024&month=3&page=1`
    - Get project data: `/api/summary/monthly?project_name=IAM&page=1`
    """
    try:
        service = SummaryBuilderService(db)
        user_id = str(current_user.id)
        
        result = service.get_summary_paginated(
            user_id=user_id,
            period_type="monthly",
            year=year,
            month=month,
            project_name=project_name,
            page=page,
            per_page=per_page
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_monthly_summary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching monthly summary: {str(e)}")


@router.get("/weekly")
async def get_weekly_summary(
    year: Optional[int] = Query(None, description="Filter by year"),
    week: Optional[int] = Query(None, ge=1, le=53, description="Filter by week number (1-53)"),
    project_name: Optional[str] = Query(None, description="Filter by project name"),
    page: int = Query(1, ge=1, description="Page number (starts at 1)"),
    per_page: int = Query(50, ge=1, le=500, description="Items per page (max 500)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get weekly summary by project name with pagination
    
    **NEW: Now paginated to prevent memory issues!**
    
    Returns aggregated data grouped by project and week.
    Default: Shows last 5 weeks if no filters provided.
    
    **Query Parameters:**
    - `year`: Filter by specific year (optional)
    - `week`: Filter by specific week 1-53 (optional, requires year)
    - `project_name`: Filter by project name (optional)
    - `page`: Page number starting from 1 (default: 1)
    - `per_page`: Records per page, max 500 (default: 50)
    """
    try:
        service = SummaryBuilderService(db)
        user_id = str(current_user.id)
        
        # If no year/week filter, default to current year
        if year is None and week is None:
            year = datetime.now().year
        
        result = service.get_summary_paginated(
            user_id=user_id,
            period_type="weekly",
            year=year,
            week=week,
            project_name=project_name,
            page=page,
            per_page=per_page
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_weekly_summary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching weekly summary: {str(e)}")


@router.get("/yearly")
async def get_yearly_summary(
    project_name: Optional[str] = Query(None, description="Filter by project name"),
    page: int = Query(1, ge=1, description="Page number (starts at 1)"),
    per_page: int = Query(50, ge=1, le=500, description="Items per page (max 500)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get yearly summary aggregated by project and year with pagination
    
    **NEW: Now paginated to prevent memory issues!**
    
    Returns aggregated data grouped by project and year.
    """
    try:
        service = SummaryBuilderService(db)
        user_id = str(current_user.id)
        
        result = service.get_summary_paginated(
            user_id=user_id,
            period_type="yearly",
            project_name=project_name,
            page=page,
            per_page=per_page
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_yearly_summary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching yearly summary: {str(e)}")


@router.get("/periods")
async def get_available_periods(
    period_type: str = Query("monthly", description="Period type: monthly, weekly, yearly"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get available periods for filtering
    
    Returns a list of available years/months/weeks that have data.
    Use this to populate filter dropdowns in your UI.
    
    **Examples:**
    - `GET /api/summary/periods?period_type=monthly`
    - `GET /api/summary/periods?period_type=weekly`
    - `GET /api/summary/periods?period_type=yearly`
    """
    try:
        service = SummaryBuilderService(db)
        user_id = str(current_user.id)
        
        return service.get_available_periods(user_id, period_type)
    except Exception as e:
        logger.error(f"Error in get_available_periods: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving available periods: {str(e)}")


@router.get("/projects")
async def get_project_list(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get list of available project names for filtering"""
    try:
        service = SummaryBuilderService(db)
        user_id = str(current_user.id)
        
        projects = service.get_project_list(user_id)
        return {"projects": projects}
    except Exception as e:
        logger.error(f"Error in get_project_list: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching project list: {str(e)}")


@router.get("/export")
async def export_summary_data(
    year: Optional[int] = Query(None, description="Filter by year"),
    month: Optional[int] = Query(None, ge=1, le=12, description="Filter by month (1-12)"),
    week: Optional[int] = Query(None, ge=1, le=53, description="Filter by week (1-53)"),
    project_name: Optional[str] = Query(None, description="Filter by project name"),
    summary_type: str = Query("monthly", description="Type of summary: 'monthly', 'weekly', or 'yearly'"),
    max_records: int = Query(10000, ge=1, le=50000, description="Maximum records to export (max 50,000)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Export summary data to Excel
    
    **IMPORTANT:** Export is limited to prevent memory issues.
    Use filters to narrow down your export if you hit the limit.
    
    Supports monthly, weekly, and yearly summaries.
    
    **Query Parameters:**
    - `max_records`: Maximum records to export (default 10,000, max 50,000)
    - Use year/month/week/project_name filters to reduce dataset size
    """
    try:
        service = SummaryBuilderService(db)
        user_id = str(current_user.id)
        
        # Get data based on summary type with limit
        if summary_type == "yearly":
            result = service.get_summary_for_export(
                user_id, "yearly", 
                project_name=project_name,
                max_records=max_records
            )
            filename = "yearly_summary.xlsx"
            
        elif summary_type == "weekly":
            result = service.get_summary_for_export(
                user_id, "weekly", 
                year=year, 
                week=week, 
                project_name=project_name,
                max_records=max_records
            )
            filename = "weekly_summary.xlsx"
            
        else:  # monthly
            result = service.get_summary_for_export(
                user_id, "monthly", 
                year=year, 
                month=month, 
                project_name=project_name,
                max_records=max_records
            )
            filename = "monthly_summary.xlsx"
        
        data = result["summaries"]
        
        if not data:
            raise HTTPException(status_code=404, detail="No data found to export")
        
        if result.get("truncated", False):
            logger.warning(f"Export truncated to {max_records} records for user {user_id}")
        
        # Flatten the nested data structure for Excel export
        flattened_data = []
        for item in data:
            flat_item = {
                'Project Name': item.get('project_name'),
                'Period': item.get('period_label'),
            }
            
            # Add period-specific fields
            if summary_type == "yearly":
                flat_item['Year'] = item.get('year')
            elif summary_type == "monthly":
                flat_item['Year'] = item.get('year')
                flat_item['Month'] = item.get('month')
            elif summary_type == "weekly":
                flat_item['Year'] = item.get('year')
                flat_item['Week Number'] = item.get('week_number')
                flat_item['Week Start'] = item.get('period_date')
            
            # Add common fields
            flat_item.update({
                'Total Records': item.get('total_records'),
                'Unique POs': item.get('unique_pos'),
                
                # Financial data
                'Total Line Amount': item.get('financial_summary', {}).get('total_line_amount', 0),
                'Total AC Amount': item.get('financial_summary', {}).get('total_ac_amount', 0),
                'Total PAC Amount': item.get('financial_summary', {}).get('total_pac_amount', 0),
                'Total Remaining Amount': item.get('financial_summary', {}).get('total_remaining_amount', 0),
                
                # Status data
                'Closed Count': item.get('status_breakdown', {}).get('closed', 0),
                'Cancelled Count': item.get('status_breakdown', {}).get('cancelled', 0),
                'Pending Count': item.get('status_breakdown', {}).get('pending', 0),
                'Completion Rate %': item.get('status_breakdown', {}).get('completion_rate', 0),
                
                # Payment terms
                'ACPAC 100% Count': item.get('payment_terms_breakdown', {}).get('acpac_100_percent', 0),
                'AC/PAC Split Count': item.get('payment_terms_breakdown', {}).get('ac_pac_split', 0),
                
                # Categories
                'Survey Count': item.get('category_breakdown', {}).get('survey', 0),
                'Transportation Count': item.get('category_breakdown', {}).get('transportation', 0),
                'Site Engineer Count': item.get('category_breakdown', {}).get('site_engineer', 0),
                'Service Count': item.get('category_breakdown', {}).get('service', 0),
            })
            
            flattened_data.append(flat_item)
        
        df = pd.DataFrame(flattened_data)
        
        # Create Excel file in memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Summary Data', index=False)
            
            # Add a second sheet with overall totals if available
            if "overall_totals" in result:
                totals_data = [{
                    'Metric': 'Total Records',
                    'Value': result["overall_totals"]["total_records"]
                }, {
                    'Metric': 'Unique POs',
                    'Value': result["overall_totals"]["unique_pos"]
                }, {
                    'Metric': 'Unique Projects',
                    'Value': result["overall_totals"]["unique_projects"]
                }, {
                    'Metric': 'Total Line Amount',
                    'Value': result["overall_totals"]["financial_totals"]["total_line_amount"]
                }, {
                    'Metric': 'Total AC Amount',
                    'Value': result["overall_totals"]["financial_totals"]["total_ac_amount"]
                }, {
                    'Metric': 'Total PAC Amount',
                    'Value': result["overall_totals"]["financial_totals"]["total_pac_amount"]
                }, {
                    'Metric': 'Total Remaining Amount',  # ADD THIS LINE
                    'Value': result["overall_totals"]["financial_totals"]["total_remaining_amount"]  # ADD THIS LINE
                },{
                    'Metric': 'Paid Amount (Period)',  # ADD THIS NEW FIELD
                    'Value': result["overall_totals"]["financial_totals"]["paid_amount"]  # ADD THIS NEW FIELD
                }, {
                    'Metric': 'Overall Completion Rate %',
                    'Value': result["overall_totals"]["overall_completion_rate"]
                }]
                
                if result.get("truncated", False):
                    totals_data.insert(0, {
                        'Metric': '⚠️ WARNING',
                        'Value': f'Export limited to {max_records} records. Use filters to narrow results.'
                    })
                
                totals_df = pd.DataFrame(totals_data)
                totals_df.to_excel(writer, sheet_name='Overall Totals', index=False)
        output.seek(0)
        
        return StreamingResponse(
            BytesIO(output.getvalue()),
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        logger.error(f"Error exporting summary data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error exporting summary data: {str(e)}")