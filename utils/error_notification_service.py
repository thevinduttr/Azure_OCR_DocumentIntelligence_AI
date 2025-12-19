# utils/error_notification_service.py

import asyncio
import json
import time
import traceback
from pathlib import Path
from typing import Dict, Any, List, Optional
import logging

from . import mailer
# Import send_error_email dynamically to avoid circular imports
try:
    from .send_email import send_error_email
    _send_email_available = True
except ImportError:
    _send_email_available = False
    send_error_email = None


class OCRErrorNotificationService:
    """
    Comprehensive error notification service for OCR processing system.
    Handles Azure Document Intelligence API errors and other processing failures
    with detailed error analysis and email notifications.
    """
    
    def __init__(self):
        self.error_counts = {}
        self.last_notification_time = {}
    
    def get_error_description(self, error: Exception, context: str = "") -> Dict[str, Any]:
        """
        Analyze error and provide detailed description for support team.
        
        Args:
            error: The exception that occurred
            context: Additional context about where the error occurred
            
        Returns:
            Dictionary with error details including severity, category, and recommended actions
        """
        error_str = str(error).lower()
        error_type = type(error).__name__
        
        error_info = {
            "error_type": error_type,
            "error_message": str(error),
            "context": context,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "severity": "MEDIUM",
            "category": "UNKNOWN",
            "recommended_actions": [],
            "is_recoverable": False,
            "technical_details": {}
        }
        
        # Azure Document Intelligence specific errors
        if "invalidargument" in error_str and "pages" in error_str:
            error_info.update({
                "severity": "HIGH",
                "category": "AZURE_DOCUMENT_INTELLIGENCE",
                "root_cause": "Page Range Parameter Error",
                "description": "Azure Document Intelligence API received an invalid page range parameter. The specified pages parameter exceeds the actual number of pages in the document.",
                "likely_causes": [
                    "Document has fewer pages than expected",
                    "Empty or corrupted PDF file",
                    "PDF merger created an invalid document structure",
                    "API parameter configuration error"
                ],
                "recommended_actions": [
                    "Verify PDF file integrity and page count",
                    "Check document merger process for errors",
                    "Validate OCR input parameters",
                    "Review Azure DI API configuration",
                    "Contact Azure support if issue persists"
                ],
                "is_recoverable": True,
                "technical_details": {
                    "api_service": "Azure Document Intelligence",
                    "error_code": "InvalidArgument",
                    "inner_error": "InvalidParameter - pages parameter invalid"
                }
            })
        
        elif "invalidargument" in error_str:
            error_info.update({
                "severity": "HIGH",
                "category": "AZURE_DOCUMENT_INTELLIGENCE",
                "root_cause": "API Parameter Error",
                "description": "Azure Document Intelligence API call failed due to invalid parameters.",
                "likely_causes": [
                    "Invalid document format or content",
                    "Corrupted file data",
                    "Unsupported document type",
                    "API configuration mismatch"
                ],
                "recommended_actions": [
                    "Validate document format and content",
                    "Check Azure DI endpoint configuration",
                    "Verify API key and permissions",
                    "Test with a known good document"
                ],
                "is_recoverable": True,
                "technical_details": {
                    "api_service": "Azure Document Intelligence",
                    "error_code": "InvalidArgument"
                }
            })
        
        elif "authentication" in error_str or "unauthorized" in error_str:
            error_info.update({
                "severity": "CRITICAL",
                "category": "AUTHENTICATION",
                "root_cause": "Authentication Failure",
                "description": "Failed to authenticate with Azure services.",
                "likely_causes": [
                    "Expired or invalid API key",
                    "Incorrect endpoint configuration",
                    "Service quota exceeded",
                    "Network connectivity issues"
                ],
                "recommended_actions": [
                    "Verify API keys in configuration files",
                    "Check service quota and billing status",
                    "Test network connectivity to Azure endpoints",
                    "Regenerate API keys if necessary"
                ],
                "is_recoverable": True,
                "technical_details": {
                    "requires_immediate_attention": True
                }
            })
        
        elif "timeout" in error_str or "connection" in error_str:
            error_info.update({
                "severity": "MEDIUM",
                "category": "NETWORK",
                "root_cause": "Network Connectivity Issue",
                "description": "Network timeout or connection failure when calling Azure services.",
                "likely_causes": [
                    "Network connectivity issues",
                    "Azure service temporary outage",
                    "Firewall or proxy blocking requests",
                    "Large document processing timeout"
                ],
                "recommended_actions": [
                    "Check network connectivity",
                    "Verify firewall and proxy settings",
                    "Check Azure service status",
                    "Implement retry mechanism with exponential backoff"
                ],
                "is_recoverable": True,
                "technical_details": {
                    "retry_recommended": True
                }
            })
        
        elif "filenotfounderror" in error_type.lower():
            error_info.update({
                "severity": "HIGH",
                "category": "FILE_SYSTEM",
                "root_cause": "Missing File",
                "description": "Required file not found in the file system.",
                "likely_causes": [
                    "File was deleted or moved",
                    "Incorrect file path configuration",
                    "Permission issues",
                    "Previous processing step failed"
                ],
                "recommended_actions": [
                    "Verify file paths and permissions",
                    "Check previous processing steps",
                    "Review file cleanup processes",
                    "Implement file existence validation"
                ],
                "is_recoverable": False,
                "technical_details": {
                    "file_operation": "READ"
                }
            })
        
        elif "json" in error_str and "decode" in error_str:
            error_info.update({
                "severity": "HIGH",
                "category": "DATA_PROCESSING",
                "root_cause": "JSON Parsing Error",
                "description": "Failed to parse JSON response or data file.",
                "likely_causes": [
                    "Malformed JSON response from API",
                    "Corrupted data file",
                    "Encoding issues",
                    "Incomplete data transmission"
                ],
                "recommended_actions": [
                    "Validate JSON structure and encoding",
                    "Check API response format",
                    "Implement robust JSON parsing with error handling",
                    "Log raw response data for debugging"
                ],
                "is_recoverable": False,
                "technical_details": {
                    "data_format": "JSON"
                }
            })
        
        # Add more specific error patterns as needed
        
        return error_info
    
    async def send_error_notification(
        self,
        error: Exception,
        context: str = "",
        submission_id: Optional[int] = None,
        request_id: Optional[int] = None,
        additional_info: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None
    ) -> None:
        """
        Send detailed error notification email to support team.
        
        Args:
            error: The exception that occurred
            context: Context where the error occurred (e.g., "OCR Processing", "Document Merge")
            submission_id: ID of the submission being processed
            request_id: ID of the request
            additional_info: Additional context information
            logger: Logger instance for logging
        """
        try:
            # Get detailed error analysis
            error_details = self.get_error_description(error, context)
            
            # Prepare email content
            subject = f"🚨 OCR Processing Error - {error_details['category']} ({error_details['severity']})"
            
            # Build detailed error report
            body_parts = [
                "=" * 80,
                "OCR PROCESSING ERROR NOTIFICATION",
                "=" * 80,
                "",
                f"📅 Timestamp: {error_details['timestamp']}",
                f"🔥 Severity: {error_details['severity']}",
                f"📂 Category: {error_details['category']}",
                f"🎯 Context: {context}",
                ""
            ]
            
            if submission_id:
                body_parts.append(f"📋 Submission ID: {submission_id}")
            if request_id:
                body_parts.append(f"🆔 Request ID: {request_id}")
            
            body_parts.extend([
                "",
                "🚫 ERROR DETAILS",
                "-" * 40,
                f"Error Type: {error_details['error_type']}",
                f"Error Message: {error_details['error_message']}",
                ""
            ])
            
            if "root_cause" in error_details:
                body_parts.extend([
                    f"🔍 Root Cause: {error_details['root_cause']}",
                    f"📝 Description: {error_details['description']}",
                    ""
                ])
            
            if error_details.get("likely_causes"):
                body_parts.extend([
                    "🤔 LIKELY CAUSES",
                    "-" * 40
                ])
                for i, cause in enumerate(error_details["likely_causes"], 1):
                    body_parts.append(f"{i}. {cause}")
                body_parts.append("")
            
            if error_details.get("recommended_actions"):
                body_parts.extend([
                    "💡 RECOMMENDED ACTIONS",
                    "-" * 40
                ])
                for i, action in enumerate(error_details["recommended_actions"], 1):
                    body_parts.append(f"{i}. {action}")
                body_parts.append("")
            
            # Technical details
            if error_details.get("technical_details"):
                body_parts.extend([
                    "🔧 TECHNICAL DETAILS",
                    "-" * 40
                ])
                for key, value in error_details["technical_details"].items():
                    body_parts.append(f"{key.replace('_', ' ').title()}: {value}")
                body_parts.append("")
            
            # Additional context
            if additional_info:
                body_parts.extend([
                    "📊 ADDITIONAL INFORMATION",
                    "-" * 40
                ])
                for key, value in additional_info.items():
                    body_parts.append(f"{key.replace('_', ' ').title()}: {value}")
                body_parts.append("")
            
            # Stack trace
            body_parts.extend([
                "🔍 STACK TRACE",
                "-" * 40,
                traceback.format_exc(),
                "",
                "=" * 80,
                "Please review this error and take appropriate action.",
                "Contact the development team if you need assistance.",
                "=" * 80
            ])
            
            body = "\n".join(body_parts)
            
            # Send notification - use only direct mailer to avoid duplicates
            try:
                # Build email configuration from config.ini
                import configparser
                config_path = Path("config.ini")
                config = configparser.ConfigParser()
                if config_path.exists():
                    config.read(config_path)
                    
                if config.getboolean("EMAIL", "send_emails", fallback=False):
                    recipients = [r.strip() for r in config.get("EMAIL", "recipient_emails", fallback="").split(",") if r.strip()]
                    
                    if recipients:
                        email_cfg = {
                            "recipients": {
                                "to": recipients,
                                "cc": [],
                                "bcc": []
                            }
                        }
                        
                        await mailer.send_email_async(
                            email_cfg,
                            subject=subject,
                            body=body,
                            logger=logger
                        )
                        
                        if logger:
                            if hasattr(logger, 'log_info'):
                                logger.log_info(f"Error notification sent for {error_details['category']} error")
                            elif hasattr(logger, 'info'):
                                logger.info(f"Error notification sent for {error_details['category']} error")
                    else:
                        if logger:
                            if hasattr(logger, 'log_warning'):
                                logger.log_warning("No email recipients configured - error notification not sent")
                            elif hasattr(logger, 'warning'):
                                logger.warning("No email recipients configured - error notification not sent")
                else:
                    if logger:
                        if hasattr(logger, 'log_info'):
                            logger.log_info("Email notifications disabled - error notification not sent")
                        elif hasattr(logger, 'info'):
                            logger.info("Email notifications disabled - error notification not sent")
                            
            except Exception as email_error:
                if logger:
                    if hasattr(logger, 'log_error'):
                        logger.log_error(f"Failed to send error notification email: {str(email_error)}", email_error)
                    elif hasattr(logger, 'error'):
                        logger.error(f"Failed to send error notification email: {str(email_error)}")
        except Exception as notify_error:
            if logger:
                if hasattr(logger, 'log_error'):
                    logger.log_error(f"Failed to send error notification: {str(notify_error)}", notify_error)
                elif hasattr(logger, 'error'):
                    logger.error(f"Failed to send error notification: {str(notify_error)}")
    
    async def notify_azure_di_error(
        self,
        error: Exception,
        pdf_path: Path,
        submission_id: Optional[int] = None,
        request_id: Optional[int] = None,
        logger: Optional[logging.Logger] = None
    ) -> None:
        """
        Specialized notification for Azure Document Intelligence errors.
        """
        try:
            # Get file information
            file_info = {}
            if pdf_path.exists():
                file_stat = pdf_path.stat()
                file_info = {
                    "file_name": pdf_path.name,
                    "file_size_mb": round(file_stat.st_size / (1024 * 1024), 2),
                    "file_path": str(pdf_path),
                    "file_exists": True
                }
            else:
                file_info = {
                    "file_name": pdf_path.name,
                    "file_path": str(pdf_path),
                    "file_exists": False,
                    "error_note": "PDF file does not exist"
                }
            
            await self.send_error_notification(
                error=error,
                context="Azure Document Intelligence OCR Processing",
                submission_id=submission_id,
                request_id=request_id,
                additional_info=file_info,
                logger=logger
            )
            
        except Exception as e:
            if logger:
                if hasattr(logger, 'log_error'):
                    logger.log_error(f"Failed to send Azure DI error notification: {str(e)}", e)
                elif hasattr(logger, 'error'):
                    logger.error(f"Failed to send Azure DI error notification: {str(e)}")
    
    async def notify_processing_failure(
        self,
        step_name: str,
        error: Exception,
        submission_id: Optional[int] = None,
        request_id: Optional[int] = None,
        additional_context: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None
    ) -> None:
        """
        General processing failure notification.
        """
        try:
            context_info = additional_context or {}
            context_info.update({
                "processing_step": step_name,
                "failure_point": "System Processing Pipeline"
            })
            
            await self.send_error_notification(
                error=error,
                context=f"Processing Step: {step_name}",
                submission_id=submission_id,
                request_id=request_id,
                additional_info=context_info,
                logger=logger
            )
            
        except Exception as e:
            if logger:
                if hasattr(logger, 'log_error'):
                    logger.log_error(f"Failed to send processing failure notification: {str(e)}", e)
                elif hasattr(logger, 'error'):
                    logger.error(f"Failed to send processing failure notification: {str(e)}")


# Global instance
error_notification_service = OCRErrorNotificationService()


async def send_azure_di_error_notification(
    error: Exception,
    pdf_path: Path,
    submission_id: Optional[int] = None,
    request_id: Optional[int] = None,
    logger: Optional[logging.Logger] = None
) -> None:
    """
    Convenience function for Azure Document Intelligence error notifications.
    """
    await error_notification_service.notify_azure_di_error(
        error=error,
        pdf_path=pdf_path,
        submission_id=submission_id,
        request_id=request_id,
        logger=logger
    )


async def send_processing_error_notification(
    step_name: str,
    error: Exception,
    submission_id: Optional[int] = None,
    request_id: Optional[int] = None,
    additional_context: Optional[Dict[str, Any]] = None,
    logger: Optional[logging.Logger] = None
) -> None:
    """
    Convenience function for general processing error notifications.
    """
    await error_notification_service.notify_processing_failure(
        step_name=step_name,
        error=error,
        submission_id=submission_id,
        request_id=request_id,
        additional_context=additional_context,
        logger=logger
    )