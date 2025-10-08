# app/services/email_service.py
import os
import logging

# Check if resend is installed
try:
    import resend
    RESEND_AVAILABLE = True
except ImportError:
    RESEND_AVAILABLE = False
    print("⚠️ WARNING: 'resend' package not installed. Email sending will fail in production.")

logger = logging.getLogger(__name__)


class EmailService:
    """
    Email service using Resend for sending password reset emails
    
    For development: Logs emails to console
    For production: Sends via Resend API
    """
    
    def __init__(self):
        self.resend_api_key = os.getenv("RESEND_API_KEY")
        self.from_email = os.getenv("FROM_EMAIL", "onboarding@resend.dev")
        self.frontend_url = os.getenv("FRONTEND_URL", "http://localhost:3000")
        self.environment = os.getenv("ENVIRONMENT", "development")
        
        # Set Resend API key
        if self.resend_api_key and RESEND_AVAILABLE:
            resend.api_key = self.resend_api_key
    
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
        
        if self.environment == "development":
            # Development: Log to console
            return self._log_email_to_console(email, user_name, reset_link)
        else:
            # Production: Send via Resend
            return self._send_via_resend(email, user_name, reset_link)
    
    def send_password_changed_notification(self, email: str, user_name: str) -> bool:
        """
        Send notification that password was changed successfully
        
        Args:
            email: Recipient email
            user_name: User's name for personalization
        
        Returns:
            True if sent successfully, False otherwise
        """
        
        if self.environment == "development":
            logger.info(f"📧 Password changed notification would be sent to {email}")
            print(f"\n✅ Password changed notification for: {email} ({user_name})\n")
            return True
        
        if not self.resend_api_key or not RESEND_AVAILABLE:
            logger.error("RESEND_API_KEY not configured or resend not installed. Cannot send email.")
            return False
        
        try:
            html_body = self._get_password_changed_html(user_name)
            
            params = {
                "from": self.from_email,
                "to": [email],
                "subject": "✅ Password Changed Successfully",
                "html": html_body,
            }
            
            response = resend.Emails.send(params)
            
            logger.info(f"✅ Password changed notification sent to {email}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to send password changed notification: {str(e)}")
            return False
    
    def _send_via_resend(self, to_email: str, user_name: str, reset_link: str) -> bool:
        """Send email via Resend API for production"""
        
        if not self.resend_api_key or not RESEND_AVAILABLE:
            logger.error("RESEND_API_KEY not configured or resend not installed. Cannot send email.")
            return False
        
        try:
            # Prepare email parameters
            params = {
                "from": self.from_email,
                "to": [to_email],
                "subject": "Reset Your Password",
                "html": self._get_reset_password_html(user_name, reset_link),
            }
            
            # Send email using Resend
            response = resend.Emails.send(params)
            
            logger.info(f"✅ Password reset email sent to {to_email}")
            logger.debug(f"Resend response: {response}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to send email to {to_email}: {str(e)}")
            return False
    
    def _get_reset_password_html(self, user_name: str, reset_link: str) -> str:
        """Generate HTML email body for password reset"""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                body {{ 
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                    line-height: 1.6; 
                    color: #333;
                    margin: 0;
                    padding: 0;
                    background-color: #f4f4f4;
                }}
                .container {{ 
                    max-width: 600px; 
                    margin: 20px auto; 
                    background-color: white;
                    border-radius: 8px;
                    overflow: hidden;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .header {{ 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white; 
                    padding: 30px 20px; 
                    text-align: center;
                }}
                .header h1 {{
                    margin: 0;
                    font-size: 24px;
                    font-weight: 600;
                }}
                .content {{ 
                    padding: 40px 30px;
                }}
                .content p {{
                    margin: 0 0 15px 0;
                    font-size: 16px;
                }}
                .button {{ 
                    display: inline-block; 
                    padding: 14px 32px; 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white !important; 
                    text-decoration: none; 
                    border-radius: 6px; 
                    margin: 25px 0;
                    font-weight: 600;
                    font-size: 16px;
                    transition: transform 0.2s;
                }}
                .button:hover {{
                    transform: translateY(-2px);
                }}
                .link-box {{
                    background-color: #f8f9fa;
                    padding: 15px;
                    border-radius: 6px;
                    word-break: break-all;
                    font-size: 14px;
                    color: #666;
                    margin: 20px 0;
                }}
                .warning {{ 
                    background-color: #fff3cd;
                    border-left: 4px solid #ffc107;
                    padding: 15px;
                    margin: 20px 0;
                    border-radius: 4px;
                }}
                .warning-text {{
                    color: #856404;
                    font-weight: 600;
                    margin: 0;
                }}
                .footer {{ 
                    text-align: center; 
                    padding: 20px; 
                    background-color: #f8f9fa;
                    color: #666; 
                    font-size: 13px;
                }}
                .footer p {{
                    margin: 5px 0;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🔐 Password Reset Request</h1>
                </div>
                <div class="content">
                    <p>Hello <strong>{user_name}</strong>,</p>
                    
                    <p>We received a request to reset your password. Click the button below to create a new password:</p>
                    
                    <div style="text-align: center;">
                        <a href="{reset_link}" class="button">Reset Password</a>
                    </div>
                    
                    <p>Or copy and paste this link into your browser:</p>
                    <div class="link-box">{reset_link}</div>
                    
                    <div class="warning">
                        <p class="warning-text">⚠️ This link will expire in 30 minutes.</p>
                    </div>
                    
                    <p>If you didn't request a password reset, you can safely ignore this email. Your password will remain unchanged.</p>
                    
                    <p style="margin-top: 30px;">Best regards,<br><strong>The Support Team</strong></p>
                </div>
                <div class="footer">
                    <p>This is an automated email. Please do not reply.</p>
                    <p>© 2025 Your Company. All rights reserved.</p>
                </div>
            </div>
        </body>
        </html>
        """
    
    def _get_password_changed_html(self, user_name: str) -> str:
        """Generate HTML email body for password changed notification"""
        return f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                body {{ 
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                    line-height: 1.6; 
                    color: #333;
                    margin: 0;
                    padding: 0;
                    background-color: #f4f4f4;
                }}
                .container {{ 
                    max-width: 600px; 
                    margin: 20px auto; 
                    background-color: white;
                    border-radius: 8px;
                    overflow: hidden;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .header {{ 
                    background: linear-gradient(135deg, #10b981 0%, #059669 100%);
                    color: white; 
                    padding: 30px 20px; 
                    text-align: center;
                }}
                .header h1 {{
                    margin: 0;
                    font-size: 24px;
                    font-weight: 600;
                }}
                .success-icon {{
                    font-size: 48px;
                    margin-bottom: 10px;
                }}
                .content {{ 
                    padding: 40px 30px;
                }}
                .content p {{
                    margin: 0 0 15px 0;
                    font-size: 16px;
                }}
                .alert-box {{
                    background-color: #fff3cd;
                    border-left: 4px solid #ffc107;
                    padding: 15px;
                    margin: 20px 0;
                    border-radius: 4px;
                }}
                .footer {{ 
                    text-align: center; 
                    padding: 20px; 
                    background-color: #f8f9fa;
                    color: #666; 
                    font-size: 13px;
                }}
                .footer p {{
                    margin: 5px 0;
                }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <div class="success-icon">✅</div>
                    <h1>Password Changed Successfully</h1>
                </div>
                <div class="content">
                    <p>Hello <strong>{user_name}</strong>,</p>
                    
                    <p>Your password has been changed successfully. You can now log in with your new password.</p>
                    
                    <div class="alert-box">
                        <p style="margin: 0; color: #856404; font-weight: 600;">
                            ⚠️ If you did not make this change, please contact our support team immediately.
                        </p>
                    </div>
                    
                    <p style="margin-top: 30px;">Best regards,<br><strong>The Support Team</strong></p>
                </div>
                <div class="footer">
                    <p>This is an automated email. Please do not reply.</p>
                    <p>© 2025 Your Company. All rights reserved.</p>
                </div>
            </div>
        </body>
        </html>
        """
    
    def _log_email_to_console(self, to_email: str, user_name: str, reset_link: str) -> bool:
        """Log email to console for development"""
        logger.info("=" * 80)
        logger.info("📧 PASSWORD RESET EMAIL (Development Mode - Using Resend)")
        logger.info("=" * 80)
        logger.info(f"To: {to_email}")
        logger.info(f"User: {user_name}")
        logger.info(f"Subject: Reset Your Password")
        logger.info("-" * 80)
        logger.info(f"🔗 RESET LINK: {reset_link}")
        logger.info("=" * 80)
        
        # Also print to console for visibility
        print("\n" + "=" * 80)
        print("📧 PASSWORD RESET EMAIL")
        print("=" * 80)
        print(f"To: {to_email}")
        print(f"User: {user_name}")
        print(f"🔗 Reset Link: {reset_link}")
        print("=" * 80 + "\n")
        
        return True