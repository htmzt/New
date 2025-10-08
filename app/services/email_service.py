# app/services/email_service.py
import resend
import os
from typing import Optional

class EmailService:
    def __init__(self):
        resend.api_key = os.getenv("RESEND_API_KEY")
        self.from_email = os.getenv("FROM_EMAIL", "onboarding@resend.dev")
        self.frontend_url = os.getenv("FRONTEND_URL")
    
    def send_password_reset_email(self, email: str, token: str, user_name: str) -> bool:
        reset_link = f"{self.frontend_url}/reset-password?token={token}"
        
        try:
            params = {
                "from": self.from_email,
                "to": [email],
                "subject": "Reset Your Password",
                "html": self._get_reset_html(user_name, reset_link)
            }
            
            resend.Emails.send(params)
            return True
            
        except Exception as e:
            print(f"Error sending email: {e}")
            return False
    
    def _get_reset_html(self, user_name: str, reset_link: str) -> str:
        return f"""
        <!DOCTYPE html>
        <html>
        <body style="font-family: Arial, sans-serif;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                <h2>Password Reset Request</h2>
                <p>Hello {user_name},</p>
                <p>Click the button below to reset your password:</p>
                <a href="{reset_link}" 
                   style="display: inline-block; padding: 12px 30px; background-color: #4CAF50; 
                          color: white; text-decoration: none; border-radius: 5px; margin: 20px 0;">
                    Reset Password
                </a>
                <p>Or copy this link: {reset_link}</p>
                <p style="color: #d32f2f;"><strong>This link expires in 30 minutes.</strong></p>
                <p>If you didn't request this, ignore this email.</p>
            </div>
        </body>
        </html>
        """