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
    with batched email notifications.
    """
    
    def __init__(self):
        self.error_counts = {}
        self.last_notification_time = {}
        self.error_batch = []  # Store errors to batch together
        self.batch_timeout = 10  # Send batch after 10 seconds (faster response)
        self.batch_timer = None
    
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
    
    def add_error_to_batch(
        self,
        error: Exception,
        context: str = "",
        submission_id: Optional[int] = None,
        request_id: Optional[int] = None,
        additional_info: Optional[Dict[str, Any]] = None,
        logger: Optional[logging.Logger] = None
    ) -> None:
        """Add error to batch for consolidated notification."""
        error_details = self.get_error_description(error, context)
        
        batch_item = {
            "error": error,
            "context": context,
            "submission_id": submission_id,
            "request_id": request_id,
            "additional_info": additional_info or {},
            "error_details": error_details,
            "timestamp": time.strftime("%H:%M:%S")
        }
        
        self.error_batch.append(batch_item)
        
        # Start/restart batch timer
        if self.batch_timer:
            self.batch_timer.cancel()
        
        self.batch_timer = asyncio.get_event_loop().call_later(
            self.batch_timeout, 
            lambda: asyncio.create_task(self.send_batched_notification(logger))
        )
    
    async def send_batched_notification(self, logger: Optional[logging.Logger] = None) -> None:
        """Send a single consolidated email for all batched errors."""
        if not self.error_batch:
            return
        
        try:
            # Build email configuration from config.ini
            import configparser
            config_path = Path("config.ini")
            config = configparser.ConfigParser()
            if config_path.exists():
                config.read(config_path)
                
            if not config.getboolean("EMAIL", "send_emails", fallback=False):
                return
            
            recipients = [r.strip() for r in config.get("EMAIL", "recipient_emails", fallback="").split(",") if r.strip()]
            if not recipients:
                return
            
            # Create modern, concise email
            error_count = len(self.error_batch)
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Group errors by category for better organization
            error_groups = {}
            for item in self.error_batch:
                category = item["error_details"].get("category", "UNKNOWN")
                if category not in error_groups:
                    error_groups[category] = []
                error_groups[category].append(item)
            
            subject = f"🚨 OCR System Alert - {error_count} Error{'s' if error_count > 1 else ''} Detected"
            
            body_parts = [
                f"📅 Time: {current_time}",
                f"⚠️  Total Errors: {error_count}",
                "",
                "📋 ERROR SUMMARY",
                "=" * 50
            ]
            
            for category, items in error_groups.items():
                category_name = category.replace("_", " ").title()
                body_parts.extend([
                    f"",
                    f"🔸 {category_name} ({len(items)} error{'s' if len(items) > 1 else ''})"
                ])
                
                for i, item in enumerate(items[:3], 1):  # Show max 3 errors per category
                    error_msg = str(item["error"])[:100] + "..." if len(str(item["error"])) > 100 else str(item["error"])
                    body_parts.append(f"   {i}. {item['timestamp']} - {error_msg}")
                    if item["submission_id"]:
                        body_parts.append(f"      ID: {item['submission_id']}")
                
                if len(items) > 3:
                    body_parts.append(f"   ... and {len(items) - 3} more")
            
            # Add quick action summary
            body_parts.extend([
                "",
                "🔧 QUICK ACTIONS",
                "=" * 50
            ])
            
            action_summary = set()
            for item in self.error_batch:
                for action in item["error_details"].get("recommended_actions", [])[:2]:  # Top 2 actions
                    action_summary.add(action[:80] + "..." if len(action) > 80 else action)
            
            for i, action in enumerate(sorted(action_summary)[:5], 1):  # Max 5 actions
                body_parts.append(f"{i}. {action}")
            
            body_parts.extend([
                "",
                "📞 Need Help? Contact the development team",
                f"🕒 Next check in 30 minutes if issues persist"
            ])
            
            body = "\n".join(body_parts)
            
            # Send consolidated notification
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
                    logger.log_info(f"Sent consolidated error notification for {error_count} errors")
                elif hasattr(logger, 'info'):
                    logger.info(f"Sent consolidated error notification for {error_count} errors")
        
        except Exception as e:
            if logger:
                if hasattr(logger, 'log_error'):
                    logger.log_error(f"Failed to send batched error notification: {str(e)}", e)
                elif hasattr(logger, 'error'):
                    logger.error(f"Failed to send batched error notification: {str(e)}")
        
        finally:
            # Clear batch after sending
            self.error_batch = []
            self.batch_timer = None
    
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
        Add error to batch for consolidated notification instead of sending immediately.
        This ensures multiple errors get batched together into a single, modern email.
        """
        self.add_error_to_batch(
            error=error,
            context=context,
            submission_id=submission_id,
            request_id=request_id,
            additional_info=additional_info,
            logger=logger
        )
        
        if logger:
            if hasattr(logger, 'log_info'):
                logger.log_info(f"Error added to notification batch: {type(error).__name__}")
            elif hasattr(logger, 'info'):
                logger.info(f"Error added to notification batch: {type(error).__name__}")

    async def force_send_batch(self, logger: Optional[logging.Logger] = None) -> None:
        """Force immediate sending of batched errors (useful for testing or critical errors)."""
        if self.batch_timer:
            self.batch_timer.cancel()
            self.batch_timer = None
        
        await self.send_batched_notification(logger)
    
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