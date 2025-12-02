# services/logging_service.py

import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union, List
from contextlib import contextmanager
import logging
from logging.handlers import RotatingFileHandler

class OCRLogger:
    """
    Comprehensive logging service for the complete OCR document processing system.
    Captures ALL system operations including OCR, database, blob storage, customer processing, 
    validations, and any other system activities for a complete audit trail.
    
    Log file format: <submission_id>-<request_id>-<timestamp>.log
    Folder structure: logs/2025-12-02/
    
    Features:
    - Database operation logging (queries, updates, inserts)
    - Blob storage operation logging (uploads, downloads)
    - Customer data processing and validation logging
    - Error tracking and exception handling
    - Performance monitoring for all operations
    - Complete system state tracking
    """
    
    def __init__(self, logs_base_dir: Path):
        self.logs_base_dir = Path(logs_base_dir)
        self.current_logger: Optional[logging.Logger] = None
        self.current_log_file: Optional[Path] = None
        self.processing_stats = {
            'start_time': None,
            'end_time': None,
            'total_documents': 0,
            'processed_documents': 0,
            'ocr_pages': 0,
            'classified_pages': 0,
            'final_documents': 0,
            'errors': [],
            'warnings': [],
            'status': 'STARTING'
        }
    
    def start_request_logging(
        self, 
        submission_id: int, 
        request_id: int, 
        priority_level: str = None
    ) -> Path:
        """
        Initialize logging for a specific request.
        Creates date folder and request-specific log file.
        
        Returns: Path to the created log file
        """
        # Create date-based folder structure
        today = datetime.now()
        date_folder = today.strftime("%Y-%m-%d")
        log_dir = self.logs_base_dir / date_folder
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create timestamp for file name
        timestamp = today.strftime("%H-%M-%S")
        log_filename = f"{submission_id}-{request_id}-{timestamp}.log"
        self.current_log_file = log_dir / log_filename
        
        # Setup logger
        self.current_logger = self._setup_logger(
            submission_id=submission_id,
            request_id=request_id,
            log_file=self.current_log_file
        )
        
        # Initialize processing stats
        self.processing_stats = {
            'submission_id': submission_id,
            'request_id': request_id,
            'priority_level': priority_level,
            'start_time': datetime.now().isoformat(),
            'end_time': None,
            'total_documents': 0,
            'processed_documents': 0,
            'downloaded_documents': 0,
            'ocr_pages': 0,
            'classified_pages': 0,
            'final_documents': 0,
            'uploaded_documents': 0,
            'customer_fields_updated': 0,
            'errors': [],
            'warnings': [],
            'status': 'PROCESSING',
            'processing_steps': []
        }
        
        # Log session start
        self.log_info(f"="*80)
        self.log_info(f"OCR PROCESSING STARTED")
        self.log_info(f"="*80)
        self.log_info(f"Submission ID: {submission_id}")
        self.log_info(f"Request ID: {request_id}")
        if priority_level:
            self.log_info(f"Priority Level: {priority_level}")
        self.log_info(f"Start Time: {self.processing_stats['start_time']}")
        self.log_info(f"Log File: {self.current_log_file}")
        self.log_info("")
        
        return self.current_log_file
    
    def _setup_logger(self, submission_id: int, request_id: int, log_file: Path) -> logging.Logger:
        """Setup and configure the logger for this request."""
        logger_name = f"ocr_{submission_id}_{request_id}"
        
        # Remove existing handlers to avoid duplicates
        logger = logging.getLogger(logger_name)
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        logger.setLevel(logging.DEBUG)
        
        # File handler with rotation
        file_handler = RotatingFileHandler(
            filename=str(log_file),
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)-8s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Console handler for real-time monitoring
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s: %(message)s',
            datefmt='%H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        logger.propagate = False
        return logger
    
    def log_step(self, step_name: str, details: str = "", status: str = "IN_PROGRESS"):
        """Log a processing step with details."""
        step_info = {
            'step': step_name,
            'timestamp': datetime.now().isoformat(),
            'status': status,
            'details': details
        }
        self.processing_stats['processing_steps'].append(step_info)
        
        if status == "COMPLETED":
            self.log_info(f"✅ STEP COMPLETED: {step_name}")
        elif status == "FAILED":
            self.log_error(f"❌ STEP FAILED: {step_name}")
        else:
            self.log_info(f"🔄 STEP STARTED: {step_name}")
        
        if details:
            self.log_info(f"   Details: {details}")
    
    def log_document_info(self, document_count: int, document_details: List[Dict] = None):
        """Log information about documents being processed."""
        self.processing_stats['total_documents'] = document_count
        
        self.log_info(f"📄 DOCUMENTS TO PROCESS: {document_count}")
        
        if document_details:
            for i, doc in enumerate(document_details, 1):
                blob_path = doc.get('BlobPath', 'Unknown')
                content_type = doc.get('ContentType', 'Unknown')
                file_size = doc.get('FileSizeBytes', 0)
                
                size_mb = round(file_size / (1024 * 1024), 2) if file_size else 0
                self.log_info(f"   {i}. {Path(blob_path).name} ({content_type}, {size_mb} MB)")
    
    def log_download_progress(self, downloaded_count: int, total_count: int, current_file: str = ""):
        """Log download progress."""
        self.processing_stats['downloaded_documents'] = downloaded_count
        
        progress = (downloaded_count / total_count * 100) if total_count > 0 else 0
        self.log_info(f"📥 DOWNLOAD PROGRESS: {downloaded_count}/{total_count} ({progress:.1f}%)")
        
        if current_file:
            self.log_info(f"   Currently downloading: {current_file}")
    
    def log_ocr_results(self, page_count: int, ocr_file: Path):
        """Log OCR processing results."""
        self.processing_stats['ocr_pages'] = page_count
        
        self.log_info(f"🔍 OCR COMPLETED: {page_count} pages processed")
        self.log_info(f"   OCR output file: {ocr_file.name}")
    
    def log_classification_results(self, page_count: int, classification_file: Path, doc_types: Dict[str, int] = None):
        """Log AI classification results."""
        self.processing_stats['classified_pages'] = page_count
        
        self.log_info(f"🤖 AI CLASSIFICATION COMPLETED: {page_count} pages classified")
        self.log_info(f"   Classification file: {classification_file.name}")
        
        if doc_types:
            self.log_info("   Document types found:")
            for doc_type, count in doc_types.items():
                self.log_info(f"     - {doc_type}: {count} page(s)")
    
    def log_final_documents(self, final_docs: List[Dict]):
        """Log final document generation results."""
        self.processing_stats['final_documents'] = len(final_docs)
        
        self.log_info(f"📋 FINAL DOCUMENTS CREATED: {len(final_docs)}")
        
        for doc in final_docs:
            doc_type = doc.get('doc_type', 'Unknown')
            file_path = doc.get('path', Path('Unknown'))
            file_size = file_path.stat().st_size if isinstance(file_path, Path) and file_path.exists() else 0
            size_mb = round(file_size / (1024 * 1024), 2) if file_size else 0
            
            self.log_info(f"   - {doc_type}: {Path(file_path).name if file_path else 'N/A'} ({size_mb} MB)")
    
    def log_upload_progress(self, uploaded_count: int, total_count: int, current_file: str = ""):
        """Log upload progress to blob storage."""
        self.processing_stats['uploaded_documents'] = uploaded_count
        
        progress = (uploaded_count / total_count * 100) if total_count > 0 else 0
        self.log_info(f"☁️ UPLOAD PROGRESS: {uploaded_count}/{total_count} ({progress:.1f}%)")
        
        if current_file:
            self.log_info(f"   Currently uploading: {current_file}")
    
    def log_customer_updates(self, update_count: int, updates: Dict[str, Any] = None):
        """Log customer database updates."""
        self.processing_stats['customer_fields_updated'] = update_count
        
        self.log_info(f"👤 CUSTOMER UPDATES: {update_count} fields updated")
        
        if updates:
            self.log_info("   Updated fields:")
            for field, value in updates.items():
                # Truncate long values for readability
                display_value = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                self.log_info(f"     - {field}: {display_value}")
    
    def log_database_operation(self, operation: str, table: str, details: str = "", affected_rows: int = 0):
        """Log database operations (SELECT, INSERT, UPDATE, DELETE)."""
        self.log_info(f"🗄️ DATABASE {operation.upper()}: {table}")
        if details:
            self.log_info(f"   Query details: {details}")
        if affected_rows > 0:
            self.log_info(f"   Affected rows: {affected_rows}")
    
    def log_database_query(self, query: str, params: tuple = None, result_count: int = None):
        """Log detailed database query execution."""
        # Sanitize query for logging (remove sensitive data patterns)
        sanitized_query = self._sanitize_query(query)
        self.log_debug(f"🔍 SQL QUERY: {sanitized_query}")
        
        if params:
            # Don't log actual parameter values for security, just count
            self.log_debug(f"   Parameters: {len(params)} values provided")
        
        if result_count is not None:
            self.log_info(f"   Query returned: {result_count} row(s)")
    
    def log_blob_operation(self, operation: str, container: str, blob_path: str, details: str = ""):
        """Log blob storage operations."""
        self.log_info(f"☁️ BLOB {operation.upper()}: {container}/{blob_path}")
        if details:
            self.log_info(f"   {details}")
    
    def log_blob_download(self, container: str, blob_path: str, local_path: str, file_size: int = 0):
        """Log blob download operations."""
        size_mb = round(file_size / (1024 * 1024), 2) if file_size > 0 else 0
        self.log_info(f"📥 BLOB DOWNLOAD: {container}/{blob_path}")
        self.log_info(f"   Local path: {local_path}")
        if size_mb > 0:
            self.log_info(f"   File size: {size_mb} MB")
    
    def log_blob_upload(self, container: str, blob_path: str, local_path: str, file_size: int = 0):
        """Log blob upload operations."""
        size_mb = round(file_size / (1024 * 1024), 2) if file_size > 0 else 0
        self.log_info(f"☁️ BLOB UPLOAD: {container}/{blob_path}")
        self.log_info(f"   Source: {local_path}")
        if size_mb > 0:
            self.log_info(f"   File size: {size_mb} MB")
    
    def log_validation_step(self, validation_name: str, status: str, details: str = "", errors: List[str] = None):
        """Log customer validation steps."""
        status_icon = "✅" if status == "PASSED" else "❌" if status == "FAILED" else "⚠️"
        self.log_info(f"{status_icon} VALIDATION: {validation_name} - {status}")
        
        if details:
            self.log_info(f"   Details: {details}")
        
        if errors:
            self.log_info(f"   Validation errors:")
            for error in errors:
                self.log_info(f"     - {error}")
    
    def log_file_operation(self, operation: str, file_path: str, details: str = ""):
        """Log file system operations."""
        self.log_info(f"📁 FILE {operation.upper()}: {file_path}")
        if details:
            self.log_info(f"   {details}")
    
    def log_api_call(self, service: str, endpoint: str, method: str = "POST", status_code: int = None, duration_ms: int = None):
        """Log external API calls."""
        self.log_info(f"🌐 API CALL: {service} - {method} {endpoint}")
        if status_code:
            status_icon = "✅" if 200 <= status_code < 300 else "❌" if status_code >= 400 else "⚠️"
            self.log_info(f"   {status_icon} Response: {status_code}")
        if duration_ms:
            self.log_info(f"   Duration: {duration_ms}ms")
    
    def log_configuration_load(self, config_file: str, status: str, details: str = ""):
        """Log configuration loading operations."""
        status_icon = "✅" if status == "SUCCESS" else "❌" if status == "FAILED" else "⚠️"
        self.log_info(f"{status_icon} CONFIG: {config_file} - {status}")
        if details:
            self.log_info(f"   {details}")
    
    def log_system_resource(self, resource_type: str, value: str, unit: str = ""):
        """Log system resource usage."""
        self.log_info(f"📊 RESOURCE: {resource_type} = {value} {unit}".strip())
    
    def log_data_processing(self, operation: str, input_count: int, output_count: int, details: str = ""):
        """Log data processing operations."""
        self.log_info(f"⚙️ DATA PROCESSING: {operation}")
        self.log_info(f"   Input: {input_count} items")
        self.log_info(f"   Output: {output_count} items")
        if details:
            self.log_info(f"   Details: {details}")
    
    def log_business_logic(self, operation: str, result: str, details: str = ""):
        """Log business logic operations and decisions."""
        self.log_info(f"🧠 BUSINESS LOGIC: {operation} → {result}")
        if details:
            self.log_info(f"   Details: {details}")
    
    def log_security_event(self, event_type: str, details: str, severity: str = "INFO"):
        """Log security-related events."""
        severity_icon = "🔒" if severity == "INFO" else "⚠️" if severity == "WARNING" else "🚨"
        self.log_info(f"{severity_icon} SECURITY: {event_type}")
        self.log_info(f"   {details}")
        
        if severity == "WARNING":
            self.log_warning(f"Security event: {event_type} - {details}")
        elif severity == "ERROR":
            self.log_error(f"Security event: {event_type} - {details}")
    
    def log_performance_metric(self, metric_name: str, value: float, unit: str = "ms"):
        """Log performance metrics."""
        self.log_info(f"⏱️ PERFORMANCE: {metric_name} = {value} {unit}")
    
    def log_cache_operation(self, operation: str, key: str, hit: bool = None):
        """Log caching operations."""
        if hit is not None:
            result = "HIT" if hit else "MISS"
            self.log_info(f"💾 CACHE {operation.upper()}: {key} - {result}")
        else:
            self.log_info(f"💾 CACHE {operation.upper()}: {key}")
    
    def _sanitize_query(self, query: str) -> str:
        """Sanitize SQL query for logging by removing potential sensitive data."""
        import re
        
        # Remove potential sensitive values from WHERE clauses
        query = re.sub(r"=\s*'[^']*'", "= '***'", query)
        query = re.sub(r"=\s*\d+", "= ***", query)
        
        # Limit query length for readability
        if len(query) > 200:
            query = query[:197] + "..."
        
        return query
    
    def log_info(self, message: str):
        """Log info level message."""
        if self.current_logger:
            self.current_logger.info(message)
    
    def log_warning(self, message: str):
        """Log warning level message."""
        if self.current_logger:
            self.current_logger.warning(message)
        self.processing_stats['warnings'].append({
            'timestamp': datetime.now().isoformat(),
            'message': message
        })
    
    def log_error(self, message: str, exception: Exception = None):
        """Log error level message."""
        if self.current_logger:
            self.current_logger.error(message)
            if exception:
                self.current_logger.error(f"Exception details: {str(exception)}")
        
        error_info = {
            'timestamp': datetime.now().isoformat(),
            'message': message
        }
        if exception:
            error_info['exception'] = str(exception)
            error_info['exception_type'] = type(exception).__name__
        
        self.processing_stats['errors'].append(error_info)
    
    def log_debug(self, message: str):
        """Log debug level message."""
        if self.current_logger:
            self.current_logger.debug(message)
    
    def complete_request_logging(self, status: str = "SUCCESS"):
        """
        Complete the logging session and write final summary.
        
        Args:
            status: Final processing status ("SUCCESS", "FAILED", "PARTIAL")
        """
        if not self.current_logger:
            return
        
        # Update final stats
        self.processing_stats['end_time'] = datetime.now().isoformat()
        self.processing_stats['status'] = status
        
        # Calculate duration
        start_time = datetime.fromisoformat(self.processing_stats['start_time'])
        end_time = datetime.fromisoformat(self.processing_stats['end_time'])
        duration = end_time - start_time
        
        # Log completion summary
        self.log_info("")
        self.log_info(f"="*80)
        self.log_info(f"PROCESSING COMPLETED - STATUS: {status}")
        self.log_info(f"="*80)
        
        # Processing statistics
        stats = self.processing_stats
        self.log_info(f"📊 PROCESSING STATISTICS:")
        self.log_info(f"   Duration: {duration}")
        self.log_info(f"   Total Documents: {stats['total_documents']}")
        self.log_info(f"   Downloaded: {stats['downloaded_documents']}")
        self.log_info(f"   OCR Pages: {stats['ocr_pages']}")
        self.log_info(f"   Classified Pages: {stats['classified_pages']}")
        self.log_info(f"   Final Documents: {stats['final_documents']}")
        self.log_info(f"   Uploaded Documents: {stats['uploaded_documents']}")
        self.log_info(f"   Customer Fields Updated: {stats['customer_fields_updated']}")
        self.log_info(f"   Warnings: {len(stats['warnings'])}")
        self.log_info(f"   Errors: {len(stats['errors'])}")
        
        # Processing steps summary
        self.log_info(f"\n🔄 PROCESSING STEPS SUMMARY:")
        for step in stats['processing_steps']:
            step_status = step['status']
            step_icon = "✅" if step_status == "COMPLETED" else "❌" if step_status == "FAILED" else "🔄"
            self.log_info(f"   {step_icon} {step['step']} ({step_status})")
        
        # Error summary
        if stats['errors']:
            self.log_info(f"\n❌ ERROR SUMMARY:")
            for i, error in enumerate(stats['errors'], 1):
                self.log_info(f"   {i}. {error['message']}")
        
        # Warning summary
        if stats['warnings']:
            self.log_info(f"\n⚠️ WARNING SUMMARY:")
            for i, warning in enumerate(stats['warnings'], 1):
                self.log_info(f"   {i}. {warning['message']}")
        
        self.log_info("")
        self.log_info(f"End Time: {stats['end_time']}")
        self.log_info(f"Log file saved: {self.current_log_file}")
        self.log_info(f"="*80)
        
        # Write JSON summary for potential programmatic access
        self._write_json_summary()
        
        # Clean up
        self._cleanup_logger()
    
    def _write_json_summary(self):
        """Write a JSON summary file alongside the log file."""
        if not self.current_log_file:
            return
        
        json_file = self.current_log_file.with_suffix('.json')
        
        try:
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(self.processing_stats, f, indent=2, ensure_ascii=False)
        except Exception as e:
            self.log_warning(f"Failed to write JSON summary: {str(e)}")
    
    def _cleanup_logger(self):
        """Clean up logger resources."""
        if self.current_logger:
            # Close and remove handlers
            for handler in self.current_logger.handlers[:]:
                handler.close()
                self.current_logger.removeHandler(handler)
            
            self.current_logger = None
        
        self.current_log_file = None
    
    @contextmanager
    def request_context(self, submission_id: int, request_id: int, priority_level: str = None):
        """
        Context manager for request logging.
        
        Usage:
            with ocr_logger.request_context(123, 456, "High"):
                # Processing code here
                pass
        """
        log_file = None
        try:
            log_file = self.start_request_logging(submission_id, request_id, priority_level)
            yield self
        except Exception as e:
            self.log_error(f"Request processing failed: {str(e)}", e)
            self.complete_request_logging("FAILED")
            raise
        else:
            self.complete_request_logging("SUCCESS")
        finally:
            if log_file and self.current_logger:
                self._cleanup_logger()


# Global logger instance (will be initialized in settings.py)
ocr_logger: Optional[OCRLogger] = None

def get_ocr_logger() -> OCRLogger:
    """Get the global OCR logger instance."""
    if ocr_logger is None:
        raise RuntimeError("OCR logger not initialized. Call initialize_logger() first.")
    return ocr_logger

def initialize_logger(logs_dir: Path):
    """Initialize the global OCR logger."""
    global ocr_logger
    ocr_logger = OCRLogger(logs_dir)