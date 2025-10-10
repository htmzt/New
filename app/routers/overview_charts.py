# app/routers/overview_charts.py
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
import logging

from app.database import get_db
from app.auth import get_current_user
from app.models import User
from app.services.overview_charts_service import OverviewChartsService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/overview-charts", tags=["overview-charts"])


@router.get("")
async def get_overview_charts(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get overview charts data showing Received PO vs Paid PO across current time periods
    
    Returns data for:
    - **Total**: All-time data
    - **Weekly**: Current week (Monday to Sunday)
    - **Monthly**: Current month
    - **Quarter**: Current quarter (Q1, Q2, Q3, or Q4)
    - **Yearly**: Current year
    
    **Response includes:**
    - Received PO amount (all POs excluding CANCELLED)
    - Paid PO amount (only CLOSED POs)
    - Pending PO amount (Received - Paid)
    - Completion percentage
    - Chart data for visualization
    
    **Example Response:**
    ```json
    {
      "success": true,
      "overview_charts": [
        {
          "period": "Total",
          "date_range": "All Time",
          "received_po": 1500000.00,
          "paid_po": 900000.00,
          "pending_po": 600000.00,
          "completion_percentage": 60.0,
          "chart_data": {
            "received": 1500000.00,
            "paid": 900000.00,
            "pending": 600000.00,
            "paid_percentage": 60.0
          }
        },
        {
          "period": "Weekly",
          "date_range": "Jan 06 - Jan 12, 2025",
          "received_po": 50000.00,
          "paid_po": 30000.00,
          "pending_po": 20000.00,
          "completion_percentage": 60.0,
          "chart_data": {...}
        },
        {
          "period": "Monthly",
          "date_range": "January 2025",
          "received_po": 200000.00,
          "paid_po": 120000.00,
          "pending_po": 80000.00,
          "completion_percentage": 60.0,
          "chart_data": {...}
        },
        {
          "period": "Quarter",
          "date_range": "Q1 2025",
          "received_po": 400000.00,
          "paid_po": 250000.00,
          "pending_po": 150000.00,
          "completion_percentage": 62.5,
          "chart_data": {...}
        },
        {
          "period": "Yearly",
          "date_range": "2025",
          "received_po": 800000.00,
          "paid_po": 500000.00,
          "pending_po": 300000.00,
          "completion_percentage": 62.5,
          "chart_data": {...}
        }
      ],
      "generated_at": "2025-01-10T10:30:00"
    }
    ```
    
    **Notes:**
    - All amounts are in the user's currency
    - Received PO = POs with publish_date in period (excluding CANCELLED)
    - Paid PO = Only CLOSED status POs
    - Pending PO = Received - Paid
    - Weekly period: Monday to Sunday of current week
    - Monthly period: 1st to last day of current month
    - Quarter period: Current quarter (Q1: Jan-Mar, Q2: Apr-Jun, Q3: Jul-Sep, Q4: Oct-Dec)
    - Yearly period: January 1 to December 31 of current year
    """
    try:
        service = OverviewChartsService(db)
        user_id = str(current_user.id)
        
        result = service.get_overview_charts(user_id)
        
        return result
        
    except Exception as e:
        logger.error(f"Error in get_overview_charts: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Error retrieving overview charts: {str(e)}"
        )