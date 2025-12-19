# utils/validation_notification_service.py

import asyncio
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional

from .send_email import send_error_email
from . import mailer


class ValidationFailureNotificationService:
    """
    Service for sending modern validation failure email notifications.
    Handles customer validation failures and portal status issues.
    """
    
    def __init__(self):
        pass
    
    def _generate_modern_html_email(
        self,
        request_id: int,
        submission_id: int,
        validation_failures: List[Dict[str, Any]],
        portal_failures: List[Dict[str, Any]]
    ) -> str:
        """
        Generate a modern HTML email template for validation failures.
        
        Args:
            request_id: The RequestId that failed validation
            submission_id: The SubmissionId
            validation_failures: List of validation failure details
            portal_failures: List of portal status failures
            
        Returns:
            HTML email content string
        """
        
        # Generate failure summary
        failure_count = len(validation_failures)
        portal_count = len(portal_failures)
        
        # Create validation failures HTML
        validation_html = ""
        if validation_failures:
            validation_html = "<div style='background: #fff5f5; border: 1px solid #fed7d7; border-radius: 8px; padding: 20px; margin: 20px 0;'>"
            validation_html += "<h3 style='color: #e53e3e; margin-top: 0; display: flex; align-items: center;'>"
            validation_html += "<span style='margin-right: 10px;'>⚠️</span>Validation Rule Failures</h3>"
            
            for i, failure in enumerate(validation_failures, 1):
                validation_html += f"""
                <div style='background: #ffffff; border-left: 4px solid #e53e3e; padding: 15px; margin: 10px 0; border-radius: 0 4px 4px 0;'>
                    <div style='font-weight: bold; color: #2d3748; margin-bottom: 8px;'>
                        #{i} - Rule: {failure.get('ValidationRule', 'Unknown')}
                    </div>
                    <div style='color: #e53e3e; font-size: 14px; line-height: 1.4;'>
                        {failure.get('ValidationError', 'No error message provided')}
                    </div>
                </div>
                """
            
            validation_html += "</div>"
        
        # Create portal failures HTML
        portal_html = ""
        if portal_failures:
            portal_html = "<div style='background: #fef5e7; border: 1px solid #f6ad55; border-radius: 8px; padding: 20px; margin: 20px 0;'>"
            portal_html += "<h3 style='color: #d69e2e; margin-top: 0; display: flex; align-items: center;'>"
            portal_html += "<span style='margin-right: 10px;'>🚫</span>Portal Status Failures</h3>"
            
            for i, portal in enumerate(portal_failures, 1):
                portal_html += f"""
                <div style='background: #ffffff; border-left: 4px solid #d69e2e; padding: 15px; margin: 10px 0; border-radius: 0 4px 4px 0;'>
                    <div style='font-weight: bold; color: #2d3748;'>
                        Portal: {portal.get('PortalName', 'Unknown')}
                    </div>
                    <div style='color: #d69e2e; font-size: 14px;'>
                        Status: {portal.get('Status', 'Unknown')}
                    </div>
                </div>
                """
            
            portal_html += "</div>"
        
        # Generate timestamp
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="utf-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Customer Validation Failure - Request #{request_id}</title>
        </head>
        <body style="margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f7fafc;">
            
            <!-- Main Container -->
            <div style="max-width: 800px; margin: 0 auto; background-color: #ffffff; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);">
                
                <!-- Header -->
                <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; text-align: center;">
                    <h1 style="margin: 0; font-size: 28px; font-weight: 300;">
                        🚨 Customer Validation Failure
                    </h1>
                    <p style="margin: 10px 0 0 0; opacity: 0.9; font-size: 16px;">
                        OCR Processing & Validation System Alert
                    </p>
                </div>
                
                <!-- Content -->
                <div style="padding: 30px;">
                    
                    <!-- Alert Summary -->
                    <div style="background: #fff5f5; border: 1px solid #fed7d7; border-radius: 8px; padding: 20px; margin-bottom: 25px;">
                        <h2 style="color: #e53e3e; margin-top: 0; display: flex; align-items: center;">
                            <span style="margin-right: 10px;">❌</span>
                            Portal Processing Failed
                        </h2>
                        <p style="color: #2d3748; margin: 10px 0; line-height: 1.6;">
                            Customer validation has failed, preventing portal operations from proceeding. 
                            The following issues need to be resolved before the request can be processed successfully.
                        </p>
                    </div>
                    
                    <!-- Request Details -->
                    <div style="background: #f7fafc; border-radius: 8px; padding: 20px; margin: 20px 0;">
                        <h3 style="color: #2d3748; margin-top: 0;">📋 Request Information</h3>
                        <table style="width: 100%; border-collapse: collapse;">
                            <tr>
                                <td style="padding: 8px 0; font-weight: bold; color: #4a5568; width: 150px;">Request ID:</td>
                                <td style="padding: 8px 0; color: #2d3748;">#{request_id}</td>
                            </tr>
                            <tr>
                                <td style="padding: 8px 0; font-weight: bold; color: #4a5568;">Submission ID:</td>
                                <td style="padding: 8px 0; color: #2d3748;">#{submission_id}</td>
                            </tr>
                            <tr>
                                <td style="padding: 8px 0; font-weight: bold; color: #4a5568;">Failed Rules:</td>
                                <td style="padding: 8px 0; color: #e53e3e; font-weight: bold;">{failure_count}</td>
                            </tr>
                            <tr>
                                <td style="padding: 8px 0; font-weight: bold; color: #4a5568;">Portal Failures:</td>
                                <td style="padding: 8px 0; color: #d69e2e; font-weight: bold;">{portal_count}</td>
                            </tr>
                            <tr>
                                <td style="padding: 8px 0; font-weight: bold; color: #4a5568;">Timestamp:</td>
                                <td style="padding: 8px 0; color: #2d3748;">{timestamp}</td>
                            </tr>
                        </table>
                    </div>
                    
                    {validation_html}
                    {portal_html}
                    
                    <!-- Impact Statement -->
                    <div style="background: #edf2f7; border-left: 4px solid #4299e1; padding: 20px; margin: 25px 0; border-radius: 0 8px 8px 0;">
                        <h3 style="color: #2b6cb0; margin-top: 0; display: flex; align-items: center;">
                            <span style="margin-right: 10px;">ℹ️</span>
                            Impact & Next Steps
                        </h3>
                        <ul style="color: #2d3748; margin: 10px 0; padding-left: 20px; line-height: 1.6;">
                            <li><strong>Portal Operations:</strong> All portal operations have been suspended for this request</li>
                            <li><strong>Customer Status:</strong> Customer record marked with validation failure</li>
                            <li><strong>Required Action:</strong> Review and resolve the validation issues listed above</li>
                            <li><strong>System Status:</strong> OCR processing completed successfully, but validation checks failed</li>
                        </ul>
                    </div>
                    
                    <!-- Resolution Guide -->
                    <div style="background: #f0fff4; border: 1px solid #9ae6b4; border-radius: 8px; padding: 20px; margin: 25px 0;">
                        <h3 style="color: #38a169; margin-top: 0; display: flex; align-items: center;">
                            <span style="margin-right: 10px;">🔧</span>
                            Resolution Steps
                        </h3>
                        <ol style="color: #2d3748; margin: 10px 0; padding-left: 20px; line-height: 1.6;">
                            <li>Review each validation failure listed above</li>
                            <li>Correct the underlying data or business logic issues</li>
                            <li>Re-run the validation process for the affected request</li>
                            <li>Monitor portal status to ensure successful processing</li>
                        </ol>
                    </div>
                    
                </div>
                
                <!-- Footer -->
                <div style="background-color: #edf2f7; padding: 20px; text-align: center; border-top: 1px solid #e2e8f0;">
                    <p style="margin: 0; color: #718096; font-size: 14px;">
                        🤖 This is an automated notification from the OCR Processing & Validation System
                    </p>
                    <p style="margin: 5px 0 0 0; color: #a0aec0; font-size: 12px;">
                        Generated on {timestamp}
                    </p>
                </div>
                
            </div>
        </body>
        </html>
        """
        
        return html_content
    
    async def _send_html_email(
        self,
        subject: str,
        html_body: str,
        text_body: str,
        logger=None
    ) -> None:
        """
        Send HTML email using the mailer directly with HTML content type.
        
        Args:
            subject: Email subject
            html_body: HTML email content
            text_body: Plain text fallback
            logger: Optional logger
        """
        try:
            # Read email configuration from config.ini
            from .send_email import _read_ini
            cfg = _read_ini()
            
            if not cfg.get("send_emails"):
                if logger:
                    logger.log_info("Email notifications are disabled in config.ini; skipping validation email.")
                return

            # Build mailer-friendly dict with HTML support
            email_cfg = {
                "provider": cfg.get("provider"),
                "smtp_host": cfg.get("smtp_server"),
                "smtp_port": cfg.get("smtp_port"),
                "use_tls": True,
                "sender": {"email": cfg.get("sender_email"), "name": "OCR Validation System"},
                "auth": {"username": cfg.get("sender_email"), "password": cfg.get("sender_password")},
                "recipients": {
                    "to": list(cfg.get("recipient_emails") or []),
                    "cc": list(cfg.get("cc_emails") or []),
                    "bcc": list(cfg.get("bcc_emails") or [])
                },
            }
            
            # Get access token for HTML email sending
            token = mailer.get_valid_access_token()
            
            # Prepare recipients
            to_list = [{"emailAddress": {"address": addr}} for addr in email_cfg["recipients"]["to"]]
            cc_list = [{"emailAddress": {"address": addr}} for addr in email_cfg["recipients"]["cc"]]
            bcc_list = [{"emailAddress": {"address": addr}} for addr in email_cfg["recipients"]["bcc"]]
            
            # Create message with HTML content
            message = {
                "message": {
                    "subject": subject,
                    "body": {
                        "contentType": "HTML",  # Use HTML instead of Text
                        "content": html_body
                    },
                    "toRecipients": to_list,
                    "ccRecipients": cc_list,
                    "bccRecipients": bcc_list,
                    "attachments": []
                },
                "saveToSentItems": True
            }
            
            # Send using Microsoft Graph API
            import requests
            url = "https://graph.microsoft.com/v1.0/me/sendMail"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json" 
            }
            
            response = requests.post(url, headers=headers, json=message)
            
            if response.status_code == 202:
                if logger:
                    logger.log_info("HTML validation failure notification sent successfully")
            else:
                raise RuntimeError(f"Failed to send HTML email: {response.status_code} {response.text}")
                
        except Exception as e:
            if logger:
                logger.log_error(f"Failed to send HTML email: {str(e)}", e)
            raise
    
    async def send_validation_failure_notification(
        self,
        request_id: int,
        submission_id: int,
        validation_failures: List[Dict[str, Any]],
        portal_failures: List[Dict[str, Any]],
        logger=None
    ) -> None:
        """
        Send a modern validation failure email notification.
        
        Args:
            request_id: The RequestId that failed validation
            submission_id: The SubmissionId
            validation_failures: List of validation failure details
            portal_failures: List of portal status failures
            logger: Optional logger instance
        """
        try:
            if logger:
                logger.log_info(f"Preparing validation failure notification for RequestId={request_id}")
            
            # Generate subject
            failure_count = len(validation_failures)
            portal_count = len(portal_failures)
            
            subject = f"🚨 Customer Validation Failed - Request #{request_id} ({failure_count} rule violations)"
            
            # Generate modern HTML email body
            html_body = self._generate_modern_html_email(
                request_id=request_id,
                submission_id=submission_id,
                validation_failures=validation_failures,
                portal_failures=portal_failures
            )
            
            # Generate plain text fallback
            text_body = f"""
Customer Validation Failure Alert

Request ID: #{request_id}
Submission ID: #{submission_id}
Failed Validation Rules: {failure_count}
Portal Failures: {portal_count}
Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}

IMPACT:
- Portal operations have been suspended for this request
- Customer validation has failed, preventing normal processing
- Manual intervention required to resolve validation issues

VALIDATION FAILURES:
"""
            
            for i, failure in enumerate(validation_failures, 1):
                text_body += f"""
{i}. Rule: {failure.get('ValidationRule', 'Unknown')}
   Error: {failure.get('ValidationError', 'No error message provided')}
"""
            
            if portal_failures:
                text_body += "\nPORTAL FAILURES:\n"
                for i, portal in enumerate(portal_failures, 1):
                    text_body += f"{i}. Portal: {portal.get('PortalName', 'Unknown')} - Status: {portal.get('Status', 'Unknown')}\n"
            
            text_body += """
NEXT STEPS:
1. Review each validation failure listed above
2. Correct the underlying data or business logic issues
3. Re-run the validation process for the affected request
4. Monitor portal status to ensure successful processing

This is an automated notification from the OCR Processing & Validation System.
"""
            
            # Try to send HTML email first, fallback to text if needed
            try:
                # Try to send HTML email using mailer directly
                await self._send_html_email(
                    subject=subject,
                    html_body=html_body,
                    text_body=text_body,
                    logger=logger
                )
            except Exception as html_error:
                # Fallback to plain text email if HTML fails
                if logger:
                    logger.log_warning(f"HTML email failed, falling back to text: {str(html_error)}")
                
                await send_error_email(
                    subject=subject,
                    body=text_body,
                    logger=logger
                )
            
            if logger:
                logger.log_info(f"Validation failure notification sent successfully for RequestId={request_id}")
                
        except Exception as e:
            error_msg = f"Failed to send validation failure notification for RequestId={request_id}: {str(e)}"
            if logger:
                logger.log_error(error_msg, e)
            else:
                print(f"[ERROR] {error_msg}")
            raise


# Global instance for easy access
_validation_notification_service = ValidationFailureNotificationService()


async def send_validation_failure_notification(
    request_id: int,
    submission_id: int,
    validation_failures: List[Dict[str, Any]],
    portal_failures: List[Dict[str, Any]],
    logger=None
) -> None:
    """
    Convenience function to send validation failure notifications.
    
    Args:
        request_id: The RequestId that failed validation
        submission_id: The SubmissionId
        validation_failures: List of validation failure details
        portal_failures: List of portal status failures
        logger: Optional logger instance
    """
    await _validation_notification_service.send_validation_failure_notification(
        request_id=request_id,
        submission_id=submission_id,
        validation_failures=validation_failures,
        portal_failures=portal_failures,
        logger=logger
    )