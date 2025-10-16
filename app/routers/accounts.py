from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.auth import get_current_user
from app.models import User
from app import models

router = APIRouter(prefix="/api/accounts", tags=["accounts"])

@router.get("/all")
async def get_all_accounts(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get all account names and project names for the authenticated user"""
    user_id = str(current_user.id)

    accounts = db.query(
        models.Account.id,
        models.Account.account_name,
        models.Account.project_name
    ).filter(
        models.Account.user_id == user_id
    ).order_by(
        models.Account.account_name,
        models.Account.project_name
    ).all()
    
    # Format the response
    result = [
        {
            "id": str(account.id),
            "account_name": account.account_name,
            "project_name": account.project_name
        }
        for account in accounts
    ]
    
    return result
@router.get("/review")
async def get_accounts_for_review(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get all accounts that need review for the authenticated user"""
    user_id = str(current_user.id)

    accounts_to_review = db.query(models.Account).filter(
        models.Account.user_id == user_id,
        models.Account.needs_review == True
    ).order_by(models.Account.project_name).all()
    
    return accounts_to_review


@router.put("/{account_id}")
async def update_account(
    account_id: str,
    account_data: dict,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update an account and mark it as reviewed"""
    account = db.query(models.Account).filter(
        models.Account.id == account_id
    ).first()
    
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    
    # Verify ownership
    if account.user_id != str(current_user.id):
        raise HTTPException(status_code=403, detail="Not authorized to update this account")
        
    account.account_name = account_data.get('account_name', '').strip()
    account.needs_review = False
    
    db.commit()
    db.refresh(account)
    
    return account

