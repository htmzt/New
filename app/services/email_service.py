# app/services/email_service.py
import os
import logging
from typing import Optional
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

logger = logging.getLogger(__name__)


class EmailService:
    """
    Email service for sending password reset emails
    
    For development: Logs emails to console
    For production: Sends via SMTP
    """
    
    def __init__(self):
        self.smtp_host = os.getenv("SMTP_HOST")
        self.smtp_port = int(os.getenv("SMTP_PORT"))
        self.smtp_user = os.getenv("SMTP_USER")
        self.smtp_password = os.getenv("SMTP_PASSWORD")
        self.from_email = os.getenv("FROM_EMAIL")
        self.frontend_url = os.getenv("FRONTEND_URL")
        self.environment = os.getenv("ENVIRONMENT", "development")
    
    def send_password_reset_email(self, email: str, token: str, user_name: str) -> bool:
        """
        Send password reset email
        
        Args:
            email: Recipient email
            token: Reset token
            user_name: User's name for personalization
        
        Returns:
            True if sent successfully, False otherwise
        """
        reset_link = f"{self.frontend_url}/reset-password?token={token}"
        
        subject = "Reset Your Password"
        
        # HTML email body
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background-color: #4CAF50; color: white; padding: 20px; text-align: center; }}
                .content {{ background-color: #f9f9f9; padding: 30px; }}
                .button {{ 
                    display: inline-block; 
                    padding: 12px 30px; 
                    background-color: #4CAF50; 
                    color: white; 
                    text-decoration: none; 
                    border-radius: 5px; 
                    margin: 20px 0;
                }}
                .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
                .warning {{ color: #d32f2f; font-weight: bold; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Password Reset Request</h1>
                </div>
                <div class="content">
                    <p>Hello {user_name},</p>
                    
                    <p>We received a request to reset your password. Click the button below to create a new password:</p>
                    
                    <div style="text-align: center;">
                        <a href="{reset_link}" class="button">Reset Password</a>
                    </div>
                    
                    <p>Or copy and paste this link into your browser:</p>
                    <p style="word-break: break-all; color: #666;">{reset_link}</p>
                    
                    <p class="warning">⚠️ This link will expire in 30 minutes.</p>
                    
                    <p>If you didn't request a password reset, you can safely ignore this email. Your password will remain unchanged.</p>
                    
                    <p>Best regards,<br>The Support Team</p>
                </div>
                <div class="footer">
                    <p>This is an automated email. Please do not reply.</p>
                    <p>© 2025 Your Company. All rights reserved.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Plain text fallback
        text_body = f"""
        Hello {user_name},
        
        We received a request to reset your password.
        
        Click this link to reset your password:
        {reset_link}
        
        This link will expire in 30 minutes.
        
        If you didn't request a password reset, you can safely ignore this email.
        
        Best regards,
        The Support Team
        """
        
        if self.environment == "development":
            # Development: Log to console
            return self._log_email_to_console(email, subject, text_body, reset_link)
        else:
            # Production: Send via SMTP
            return self._send_via_smtp(email, subject, html_body, text_body)
    
    def _log_email_to_console(self, to_email: str, subject: str, body: str, reset_link: str) -> bool:
        """Log email to console for development"""
        logger.info("=" * 80)
        logger.info("📧 PASSWORD RESET EMAIL (Development Mode)")
        logger.info("=" * 80)
        logger.info(f"To: {to_email}")
        logger.info(f"Subject: {subject}")
        logger.info("-" * 80)
        logger.info("Body:")
        logger.info(body)
        logger.info("-" * 80)
        logger.info(f"🔗 RESET LINK: {reset_link}")
        logger.info("=" * 80)
        
        # Also print to console for visibility
        print("\n" + "=" * 80)
        print("📧 PASSWORD RESET EMAIL")
        print("=" * 80)
        print(f"To: {to_email}")
        print(f"🔗 Reset Link: {reset_link}")
        print("=" * 80 + "\n")
        
        return True
    
    def _send_via_smtp(self, to_email: str, subject: str, html_body: str, text_body: str) -> bool:
        """Send email via SMTP for production"""
        if not self.smtp_user or not self.smtp_password:
            logger.error("SMTP credentials not configured. Cannot send email.")
            return False
        
        try:
            # Create message
            message = MIMEMultipart("alternative")
            message["Subject"] = subject
            message["From"] = self.from_email
            message["To"] = to_email
            
            # Attach both plain text and HTML versions
            part1 = MIMEText(text_body, "plain")
            part2 = MIMEText(html_body, "html")
            message.attach(part1)
            message.attach(part2)
            
            # Send email
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(message)
            
            logger.info(f"✅ Password reset email sent to {to_email}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to send email to {to_email}: {str(e)}")
            return False
    
    def send_password_changed_notification(self, email: str, user_name: str) -> bool:
        """
        Send notification that password was changed successfully
        """
        subject = "Password Changed Successfully"
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                .header {{ background-color: #4CAF50; color: white; padding: 20px; text-align: center; }}
                .content {{ background-color: #f9f9f9; padding: 30px; }}
                .footer {{ text-align: center; padding: 20px; color: #666; font-size: 12px; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>✅ Password Changed</h1>
                </div>
                <div class="content">
                    <p>Hello {user_name},</p>
                    
                    <p>Your password has been changed successfully.</p>
                    
                    <p>If you did not make this change, please contact our support team immediately.</p>
                    
                    <p>Best regards,<br>The Support Team</p>
                </div>
                <div class="footer">
                    <p>This is an automated email. Please do not reply.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        Hello {user_name},
        
        Your password has been changed successfully.
        
        If you did not make this change, please contact our support team immediately.
        
        Best regards,
        The Support Team
        """
        
        if self.environment == "development":
            logger.info(f"📧 Password changed notification would be sent to {email}")
            return True
        else:
            return self._send_via_smtp(email, subject, html_body, text_body)