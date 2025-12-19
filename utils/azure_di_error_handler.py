# utils/azure_di_error_handler.py

import json
import time
from pathlib import Path
from typing import Dict, Any, Optional
import logging

from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential

from config.settings import AZURE_DI_ENDPOINT, AZURE_DI_KEY, AZURE_DI_LAYOUT_MODEL_ID
from utils.error_notification_service import send_azure_di_error_notification


class AzureDocumentIntelligenceErrorHandler:
    """
    Specialized error handler for Azure Document Intelligence API errors.
    Provides enhanced error detection, recovery, and notification.
    """
    
    @staticmethod
    def analyze_pdf_structure(pdf_path: Path) -> Dict[str, Any]:
        """
        Analyze PDF structure to provide diagnostics for Azure DI errors.
        """
        try:
            from pypdf import PdfReader
            
            if not pdf_path.exists():
                return {"error": "PDF file does not exist", "file_exists": False}
            
            reader = PdfReader(str(pdf_path))
            
            analysis = {
                "file_exists": True,
                "file_size_mb": round(pdf_path.stat().st_size / (1024 * 1024), 2),
                "page_count": len(reader.pages),
                "is_encrypted": reader.is_encrypted,
                "pdf_version": getattr(reader, 'pdf_header', 'Unknown'),
                "pages_analysis": []
            }
            
            # Analyze each page
            for i, page in enumerate(reader.pages, 1):
                page_info = {
                    "page_number": i,
                    "has_text": bool(page.extract_text().strip()),
                    "text_length": len(page.extract_text()),
                    "rotation": page.get('/Rotate', 0)
                }
                analysis["pages_analysis"].append(page_info)
            
            return analysis
            
        except Exception as e:
            return {
                "error": f"Failed to analyze PDF: {str(e)}",
                "file_exists": pdf_path.exists(),
                "file_size_mb": round(pdf_path.stat().st_size / (1024 * 1024), 2) if pdf_path.exists() else 0
            }
    
    @staticmethod
    async def handle_azure_di_error(
        error: Exception,
        pdf_path: Path,
        submission_id: Optional[int] = None,
        request_id: Optional[int] = None,
        logger: Optional[logging.Logger] = None
    ) -> Dict[str, Any]:
        """
        Comprehensive error handling for Azure Document Intelligence errors.
        
        Returns:
            Dictionary with error analysis and recovery recommendations
        """
        error_str = str(error).lower()
        
        # Analyze PDF structure
        pdf_analysis = AzureDocumentIntelligenceErrorHandler.analyze_pdf_structure(pdf_path)
        
        error_analysis = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "error_type": type(error).__name__,
            "error_message": str(error),
            "pdf_analysis": pdf_analysis,
            "recovery_attempted": False,
            "recovery_successful": False,
            "recommendations": []
        }
        
        # Specific handling for page range errors
        if "invalidargument" in error_str and "pages" in error_str:
            error_analysis.update({
                "error_category": "PAGE_RANGE_ERROR",
                "root_cause": "Invalid page range parameter",
                "description": (
                    "Azure Document Intelligence received a page range parameter that exceeds "
                    "the actual number of pages in the document. This typically happens when "
                    "the document has fewer pages than expected or contains empty/corrupted pages."
                )
            })
            
            # Check if PDF has pages
            if pdf_analysis.get("page_count", 0) == 0:
                error_analysis["recommendations"].extend([
                    "The PDF appears to have 0 pages - check document merger process",
                    "Verify that all source documents were properly merged",
                    "Check for empty or corrupted source files"
                ])
            elif pdf_analysis.get("page_count", 0) < 5:  # Assuming expectation of more pages
                error_analysis["recommendations"].extend([
                    f"PDF has only {pdf_analysis.get('page_count', 0)} pages - verify this is expected",
                    "Check if any source documents failed to merge properly",
                    "Review document merger logs for warnings"
                ])
            else:
                error_analysis["recommendations"].extend([
                    "PDF structure appears normal - this may be an API parameter issue",
                    "Review Azure DI API call parameters",
                    "Check if any custom page range parameters are being set"
                ])
        
        elif "invalidargument" in error_str:
            error_analysis.update({
                "error_category": "INVALID_PARAMETER",
                "root_cause": "Invalid API parameters",
                "description": "Azure Document Intelligence API call failed due to invalid parameters."
            })
            
            if pdf_analysis.get("is_encrypted", False):
                error_analysis["recommendations"].extend([
                    "PDF is encrypted - Azure DI may not support encrypted documents",
                    "Try processing the document without encryption"
                ])
            elif pdf_analysis.get("file_size_mb", 0) > 500:  # Large file
                error_analysis["recommendations"].extend([
                    f"PDF is very large ({pdf_analysis.get('file_size_mb', 0)} MB)",
                    "Consider splitting the document into smaller parts",
                    "Check Azure DI service limits for file size"
                ])
            else:
                error_analysis["recommendations"].extend([
                    "Check document format compatibility with Azure DI",
                    "Verify API endpoint and authentication",
                    "Review Azure DI service status"
                ])
        
        # Attempt recovery for certain error types
        if error_analysis["error_category"] == "PAGE_RANGE_ERROR":
            error_analysis["recovery_attempted"] = True
            
            if logger:
                if hasattr(logger, 'log_info'):
                    logger.log_info("Attempting recovery for page range error...")
                elif hasattr(logger, 'info'):
                    logger.info("Attempting recovery for page range error...")
            
            # Recovery strategy: try processing without explicit page parameters
            try:
                recovery_result = await AzureDocumentIntelligenceErrorHandler._attempt_recovery(
                    pdf_path, logger
                )
                error_analysis["recovery_successful"] = recovery_result["success"]
                error_analysis["recovery_details"] = recovery_result
                
            except Exception as recovery_error:
                error_analysis["recovery_error"] = str(recovery_error)
                if logger:
                    if hasattr(logger, 'log_error'):
                        logger.log_error(f"Recovery attempt failed: {str(recovery_error)}", recovery_error)
                    elif hasattr(logger, 'error'):
                        logger.error(f"Recovery attempt failed: {str(recovery_error)}")
        
        # Note: Error notification is handled by the calling code to avoid duplicates
        error_analysis["notification_sent"] = False
        error_analysis["notification_handled_by_caller"] = True
        
        return error_analysis
    
    @staticmethod
    async def _attempt_recovery(pdf_path: Path, logger: Optional[logging.Logger] = None) -> Dict[str, Any]:
        """
        Attempt to recover from Azure DI errors by retrying with basic parameters.
        """
        try:
            if logger:
                if hasattr(logger, 'log_info'):
                    logger.log_info("Attempting Azure DI recovery with basic parameters...")
                elif hasattr(logger, 'info'):
                    logger.info("Attempting Azure DI recovery with basic parameters...")
            
            client = DocumentIntelligenceClient(
                endpoint=AZURE_DI_ENDPOINT,
                credential=AzureKeyCredential(AZURE_DI_KEY),
            )
            
            with pdf_path.open("rb") as f:
                poller = client.begin_analyze_document(
                    model_id=AZURE_DI_LAYOUT_MODEL_ID,
                    body=f.read(),
                    content_type="application/pdf"
                    # No additional parameters that might cause issues
                )
                
                result = poller.result()
                
                return {
                    "success": True,
                    "pages_processed": len(result.pages),
                    "recovery_method": "basic_parameters"
                }
                
        except Exception as recovery_error:
            return {
                "success": False,
                "error": str(recovery_error),
                "recovery_method": "basic_parameters"
            }


async def handle_azure_di_error(
    error: Exception,
    pdf_path: Path,
    submission_id: Optional[int] = None,
    request_id: Optional[int] = None,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Any]:
    """
    Convenience function for handling Azure Document Intelligence errors.
    """
    return await AzureDocumentIntelligenceErrorHandler.handle_azure_di_error(
        error=error,
        pdf_path=pdf_path,
        submission_id=submission_id,
        request_id=request_id,
        logger=logger
    )