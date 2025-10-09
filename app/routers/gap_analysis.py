from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from typing import Optional
from app.database import get_db
from app.auth import get_current_user
from app.models import User
from app.services.gap_analysis_service import GapAnalysisService
from io import BytesIO
import logging
from app.services.gap_aging_service import GapAgingService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/gap-analysis", tags=["gap-analysis"])

@router.get("/financial-summary")
async def get_gap_financial_summary(
    project_name: Optional[str] = Query(None, description="Filter by project name"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get gap financial analysis summary by project in tabular format
    
    Returns:
    - Total PO Received (line amounts)
    - GAP PO OK; AC NOK (AC pending amounts) 
    - GAP AC OK; PAC NOK (PAC pending amounts)
    - Total GAP AC & PAC (combined gap amounts)
    - Gap Percentage by Project
    """
    try:
        service = GapAnalysisService(db)
        financial_summary = service.get_gap_financial_summary_by_project(str(current_user.id), project_name)
        
        return {
            "success": True,
            "data": {
                "financial_summary": financial_summary,
                "column_headers": [
                    "Project Name",
                    "Total PO Received", 
                    "GAP PO OK; AC NOK",
                    "GAP AC OK; PAC NOK", 
                    "Total GAP AC & PAC",
                    "Gap Percentage"
                ]
            }
        }
        
    except Exception as e:
        logger.error(f"Error in gap financial summary endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving financial summary: {str(e)}")


@router.get("/export/gap-financial-summary")
async def export_gap_financial_summary_excel(
    project_name: Optional[str] = Query(None, description="Filter by project name"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Export gap financial summary directly to Excel (optimized for performance)
    
    Creates an Excel file with gap financial analysis by project:
    - GAP by Project
    - Total PO Received 
    - GAP PO Ok; AC Nok
    - GAP AC OK; PAC Nok
    - Total GAP AC & PAC
    - Pourcentage GAP Par Projet
    
    This endpoint is optimized for speed by calculating summaries directly in SQL.
    """
    try:
        service = GapAnalysisService(db)
        excel_data = service.export_gap_financial_summary_to_excel(str(current_user.id), project_name)
        
        # Create filename
        filename = "gap_financial_summary"
        if project_name:
            safe_project_name = "".join(c for c in project_name if c.isalnum() or c in (' ', '-', '_')).strip()
            filename += f"_{safe_project_name}"
        filename += ".xlsx"
        
        # Return as streaming response
        return StreamingResponse(
            BytesIO(excel_data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        logger.error(f"Error in gap financial summary Excel export: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error exporting gap financial summary: {str(e)}")

@router.get("/aging")
async def get_aging_analysis(
    project_name: Optional[str] = Query(None, description="Filter by project name"),
    account_name: Optional[str] = Query(None, description="Filter by account name"),
    category: Optional[str] = Query(None, description="Filter by category (Survey, Transportation, Site Engineer, Service)"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Get aging analysis of pending POs bucketed by days since publish_date
    
    Buckets:
    - 0-15 days: Very recent
    - 16-30 days: Recent
    - 31-60 days: Needs attention
    - 61-90 days: Urgent
    - 90+ days: Critical
    
    Returns:
    - Count of pending POs per bucket
    - Total amounts per bucket
    - AC pending amounts (waiting for first payment)
    - PAC pending amounts (waiting for final 20%)
    - Average age per bucket
    
    All filters are optional and can be combined.
    """
    try:
        service = GapAgingService(db)
        aging_analysis = service.get_aging_analysis(
            user_id=str(current_user.id),
            project_name=project_name,
            account_name=account_name,
            category=category
        )
        
        return aging_analysis
        
    except Exception as e:
        logger.error(f"Error in aging analysis endpoint: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving aging analysis: {str(e)}")


@router.get("/export/aging")
async def export_aging_analysis_excel(
    project_name: Optional[str] = Query(None, description="Filter by project name"),
    account_name: Optional[str] = Query(None, description="Filter by account name"),
    category: Optional[str] = Query(None, description="Filter by category"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Export aging analysis to Excel file
    
    Creates an Excel file with:
    - Sheet 1: Aging Analysis table with color-coded buckets
    - Sheet 2: Summary statistics and applied filters
    
    Color coding:
    - 0-15 days: Green (acceptable)
    - 16-30 days: Blue (recent)
    - 31-60 days: Yellow (needs attention)
    - 61-90 days: Orange (urgent)
    - 90+ days: Red (critical)
    """
    try:
        service = GapAgingService(db)
        excel_data = service.export_aging_analysis_to_excel(
            user_id=str(current_user.id),
            project_name=project_name,
            account_name=account_name,
            category=category
        )
        
        # Create filename
        filename = "gap_aging_analysis"
        if project_name:
            safe_project_name = "".join(c for c in project_name if c.isalnum() or c in (' ', '-', '_')).strip()
            filename += f"_{safe_project_name}"
        if account_name:
            safe_account_name = "".join(c for c in account_name if c.isalnum() or c in (' ', '-', '_')).strip()
            filename += f"_{safe_account_name}"
        if category:
            filename += f"_{category}"
        filename += ".xlsx"
        
        # Return as streaming response
        return StreamingResponse(
            BytesIO(excel_data),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except Exception as e:
        logger.error(f"Error in aging analysis Excel export: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error exporting aging analysis: {str(e)}")