from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import desc
import logging

from app.database import get_db
from app.auth import get_current_user
from app.models import User, UploadHistory

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/upload-history", tags=["upload-history"])


@router.get("")
async def get_upload_history(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get all upload history for the current user
    
    Returns:
    - List of all uploads with file name, type, total rows, and date
    """
    try:
        user_id = str(current_user.id)
        
        # Get all uploads for this user, ordered by newest first
        uploads = db.query(UploadHistory).filter(
            UploadHistory.user_id == user_id
        ).order_by(desc(UploadHistory.uploaded_at)).all()
        
        # Format simple response
        upload_list = [
            {
                "file_name": upload.file_name,
                "file_type": upload.file_type,
                "total_rows": upload.total_rows,
                "status": upload.status,
                "uploaded_at": upload.uploaded_at.isoformat() if upload.uploaded_at else None
            }
            for upload in uploads
        ]
        
        return {
            "success": True,
            "data": upload_list
        }
        
    except Exception as e:
        logger.error(f"Error retrieving upload history: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error retrieving upload history: {str(e)}")