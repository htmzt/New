# app/services/password_reset_service.py
# FIXED VERSION - Replace your entire file with this

import secrets
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.models import User, PasswordResetToken
from app.auth import get_password_hash
from app.services.email_service import EmailService
import logging

logger = logging.getLogger(__name__)


class PasswordResetService:
    """
    Service for handling password reset operations
    """
    
    # Token validity period (30 minutes)
    TOKEN_EXPIRY_MINUTES = 30
    
    # Maximum number of active tokens per user
    MAX_TOKENS_PER_USER = 5
    
    def __init__(self, db: Session):
        self.db = db
        self.email_service = EmailService()
    
    def create_reset_token(self, email: str) -> Tuple[bool, str]:
        """
        Create a password reset token for the user
        
        Args:
            email: User's email address
        
        Returns:
            Tuple of (success, message)
            
        Note: Always returns success to prevent user enumeration
        """
        try:
            # Find user by email
            user = self.db.query(User).filter(User.email == email.lower()).first()
            
            if not user:
                # Don't reveal that user doesn't exist
                logger.warning(f"Password reset requested for non-existent email: {email}")
                return True, "If an account exists with this email, a reset link has been sent."
            
            # Clean up old expired tokens for this user
            self._cleanup_expired_tokens(user.id)
            
            # Check if user has too many active tokens
            # FIX: Use timezone-aware datetime
            now = datetime.now(timezone.utc)
            active_tokens = self.db.query(PasswordResetToken).filter(
                and_(
                    PasswordResetToken.user_id == user.id,
                    PasswordResetToken.is_used == False,
                    PasswordResetToken.expires_at > now
                )
            ).count()
            
            if active_tokens >= self.MAX_TOKENS_PER_USER:
                logger.warning(f"User {user.email} has too many active reset tokens")
                return True, "If an account exists with this email, a reset link has been sent."
            
            # Generate secure random token
            token = secrets.token_urlsafe(32)
            
            # Hash the token for storage (never store plain tokens!)
            token_hash = self._hash_token(token)
            
            # FIX: Create timezone-aware expiry datetime
            expires_at = datetime.now(timezone.utc) + timedelta(minutes=self.TOKEN_EXPIRY_MINUTES)
            
            # Create reset token record
            reset_token = PasswordResetToken(
                user_id=user.id,
                token_hash=token_hash,
                expires_at=expires_at
            )
            
            self.db.add(reset_token)
            self.db.commit()
            
            # Send email with the plain token (not the hash!)
            user_name = f"{user.prenom} {user.nom}"
            email_sent = self.email_service.send_password_reset_email(
                email=user.email,
                token=token,
                user_name=user_name
            )
            
            if email_sent:
                logger.info(f"✅ Password reset email sent to {user.email}")
            else:
                logger.error(f"❌ Failed to send password reset email to {user.email}")
            
            return True, "If an account exists with this email, a reset link has been sent."
            
        except Exception as e:
            logger.error(f"Error creating reset token: {str(e)}")
            self.db.rollback()
            # Still return success to prevent enumeration
            return True, "If an account exists with this email, a reset link has been sent."
    
    def verify_reset_token(self, token: str) -> Tuple[bool, Optional[User], str]:
        """
        Verify a password reset token
        
        Args:
            token: The reset token
        
        Returns:
            Tuple of (is_valid, user, message)
        """
        try:
            # Hash the provided token to compare with stored hash
            token_hash = self._hash_token(token)
            
            # Find the token record
            reset_token = self.db.query(PasswordResetToken).filter(
                PasswordResetToken.token_hash == token_hash
            ).first()
            
            if not reset_token:
                return False, None, "Invalid or expired reset token"
            
            # FIX: Use timezone-aware datetime for comparison
            now = datetime.now(timezone.utc)
            
            # Check if token is used
            if reset_token.is_used:
                return False, None, "This reset token has already been used"
            
            # Check if token is expired
            if reset_token.expires_at <= now:
                return False, None, "This reset token has expired"
            
            # Get the user
            user = self.db.query(User).filter(User.id == reset_token.user_id).first()
            
            if not user:
                return False, None, "User not found"
            
            if not user.is_active:
                return False, None, "Account is deactivated"
            
            return True, user, "Token is valid"
            
        except Exception as e:
            logger.error(f"Error verifying reset token: {str(e)}")
            return False, None, "An error occurred while verifying the token"
    
    def reset_password(self, token: str, new_password: str) -> Tuple[bool, str]:
        """
        Reset user password using a valid token
        
        Args:
            token: The reset token
            new_password: The new password
        
        Returns:
            Tuple of (success, message)
        """
        try:
            # Verify token
            is_valid, user, message = self.verify_reset_token(token)
            
            if not is_valid or not user:
                return False, message
            
            # Hash the provided token to find the record
            token_hash = self._hash_token(token)
            reset_token = self.db.query(PasswordResetToken).filter(
                PasswordResetToken.token_hash == token_hash
            ).first()
            
            # FIX: Use timezone-aware datetime
            now = datetime.now(timezone.utc)
            
            # Update user password
            user.password_hash = get_password_hash(new_password)
            user.updated_at = now
            
            # Mark token as used
            reset_token.is_used = True
            reset_token.used_at = now
            
            # Invalidate all other reset tokens for this user (security measure)
            self.db.query(PasswordResetToken).filter(
                and_(
                    PasswordResetToken.user_id == user.id,
                    PasswordResetToken.id != reset_token.id,
                    PasswordResetToken.is_used == False
                )
            ).update({
                'is_used': True,
                'used_at': now
            })
            
            self.db.commit()
            
            # Send confirmation email
            user_name = f"{user.prenom} {user.nom}"
            self.email_service.send_password_changed_notification(user.email, user_name)
            
            logger.info(f"✅ Password reset successful for user: {user.email}")
            
            return True, "Password has been reset successfully"
            
        except Exception as e:
            logger.error(f"Error resetting password: {str(e)}")
            self.db.rollback()
            return False, "An error occurred while resetting the password"
    
    def _hash_token(self, token: str) -> str:
        """
        Hash a token using SHA-256
        
        We hash tokens before storing them in the database so that
        if the database is compromised, the tokens cannot be used.
        """
        return hashlib.sha256(token.encode()).hexdigest()
    
    def _cleanup_expired_tokens(self, user_id):
        """
        Delete expired tokens for a user
        """
        try:
            # FIX: Use timezone-aware datetime
            now = datetime.now(timezone.utc)
            
            deleted = self.db.query(PasswordResetToken).filter(
                and_(
                    PasswordResetToken.user_id == user_id,
                    PasswordResetToken.expires_at <= now
                )
            ).delete()
            
            if deleted > 0:
                self.db.commit()
                logger.info(f"Cleaned up {deleted} expired tokens for user {user_id}")
                
        except Exception as e:
            logger.error(f"Error cleaning up expired tokens: {str(e)}")
            self.db.rollback()
    
    def cleanup_all_expired_tokens(self):
        """
        Cleanup all expired tokens in the database
        This should be run periodically (e.g., daily cron job)
        """
        try:
            # FIX: Use timezone-aware datetime
            now = datetime.now(timezone.utc)
            
            deleted = self.db.query(PasswordResetToken).filter(
                PasswordResetToken.expires_at <= now
            ).delete()
            
            self.db.commit()
            logger.info(f"🧹 Cleaned up {deleted} expired password reset tokens")
            return deleted
            
        except Exception as e:
            logger.error(f"Error in cleanup_all_expired_tokens: {str(e)}")
            self.db.rollback()
            return 0