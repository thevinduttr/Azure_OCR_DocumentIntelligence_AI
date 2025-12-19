# services/db_service.py

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import pyodbc

from config.settings import (
    DB_CONNECTION_STRING,
    DB_DEFAULT_OCR_STATUS,
    DB_STORAGE_RETENTION_DAYS,
    DOC_TYPE_NAME_TO_DB_CODE,
)

from utils.date_utils import normalize_date_for_sql

# Import logging service with error handling
try:
    from services.logging_service import get_ocr_logger
    _logger_available = True
except ImportError:
    _logger_available = False

def _log_if_available(func_name, *args, **kwargs):
    """Helper to log if logger is available."""
    if _logger_available:
        try:
            logger = get_ocr_logger()
            getattr(logger, func_name)(*args, **kwargs)
        except Exception:
            pass  # Continue if logging fails

def _get_db_connection():
    if not DB_CONNECTION_STRING:
        error_msg = "Database connection string is not configured."
        _log_if_available('log_error', error_msg)
        raise ValueError(error_msg)
    
    _log_if_available('log_database_operation', 'CONNECT', 'DATABASE', 'Establishing database connection')
    
    try:
        conn = pyodbc.connect(DB_CONNECTION_STRING)
        _log_if_available('log_database_operation', 'CONNECT', 'DATABASE', 'Database connection established successfully')
        return conn
    except Exception as e:
        _log_if_available('log_error', f'Database connection failed: {str(e)}', e)
        raise

def fetch_next_submission_to_process() -> Optional[Dict[str, Any]]:
    """
    Get the next submission to process based on:
      - IsProcessed = 0
      - PriorityLevel: High -> Medium -> Normal
      - Earliest ReceivedAt first within same priority
    Returns a single row dict or None.
    """
    _log_if_available('log_database_operation', 'SELECT', 'Submissions', 'Fetching next submission to process')
    
    if not DB_CONNECTION_STRING:
        error_msg = "Database connection string is not configured."
        _log_if_available('log_error', error_msg)
        raise ValueError(error_msg)

    conn = pyodbc.connect(DB_CONNECTION_STRING)
    try:
        cursor = conn.cursor()
        sql = """
        SELECT TOP (1)
            Id,
            RequestId,
            OutletLinkId,
            OutletId,
            CustomerId,
            Mode,
            ReceivedAt,
            OptOutFlag,
            OptOutRequestedAt,
            IsProcessed,
            ProcessedAt,
            DocumentCount,
            PriorityLevel,
            Metadata,
            IsDeleted,
            DeletedAt
        FROM [dbo].[Submissions]
        WHERE IsProcessed = 0
          AND (IsDeleted = 0 OR IsDeleted IS NULL)
        ORDER BY
            CASE
                WHEN PriorityLevel = 'High' THEN 1
                WHEN PriorityLevel = 'Medium' THEN 2
                ELSE 3
            END,
            ReceivedAt ASC
        """
        
        _log_if_available('log_database_query', sql, None, None)
        cursor.execute(sql)
        row = cursor.fetchone()
        
        if not row:
            _log_if_available('log_database_query', sql, None, 0)
            _log_if_available('log_info', 'No pending submissions found')
            return None

        columns = [c[0] for c in cursor.description]
        result = {col: val for col, val in zip(columns, row)}
        
        _log_if_available('log_database_query', sql, None, 1)
        _log_if_available('log_info', f'Found submission: ID={result.get("Id")}, RequestId={result.get("RequestId")}, Priority={result.get("PriorityLevel")}')
        
        return result
    except Exception as e:
        _log_if_available('log_error', f'Failed to fetch next submission: {str(e)}', e)
        raise
    finally:
        conn.close()


def mark_submission_as_processed(submission_id: int) -> None:
    """
    Mark a submission as processed (IsProcessed = 1, ProcessedAt = now).
    """
    _log_if_available('log_database_operation', 'UPDATE', 'Submissions', f'Marking submission {submission_id} as processed')
    
    if not DB_CONNECTION_STRING:
        error_msg = "Database connection string is not configured."
        _log_if_available('log_error', error_msg)
        raise ValueError(error_msg)

    conn = pyodbc.connect(DB_CONNECTION_STRING)
    try:
        cursor = conn.cursor()
        sql = """
        UPDATE [dbo].[Submissions]
        SET IsProcessed = 1,
            ProcessedAt = GETUTCDATE()
        WHERE Id = ? AND IsProcessed = 0
        """
        
        _log_if_available('log_database_query', sql, (submission_id,), None)
        cursor.execute(sql, submission_id)
        affected_rows = cursor.rowcount
        conn.commit()
        
        _log_if_available('log_database_operation', 'UPDATE', 'Submissions', f'Successfully marked submission {submission_id} as processed', affected_rows)
        
        if affected_rows == 0:
            _log_if_available('log_warning', f'No rows updated when marking submission {submission_id} as processed - submission may already be processed')
        
    except Exception as e:
        _log_if_available('log_error', f'Failed to mark submission {submission_id} as processed: {str(e)}', e)
        raise
    finally:
        conn.close()


def fetch_processed_documents_for(submission_id: int, request_id: int) -> List[Dict[str, Any]]:
    """
    Get Document rows for a given SubmissionId + RequestId
    with OcrStatus = 'PENDING'.
    """
    _log_if_available('log_database_operation', 'SELECT', 'Documents', f'Fetching processed documents for SubmissionId={submission_id}, RequestId={request_id}')
    
    if not DB_CONNECTION_STRING:
        error_msg = "Database connection string is not configured."
        _log_if_available('log_error', error_msg)
        raise ValueError(error_msg)

    conn = pyodbc.connect(DB_CONNECTION_STRING)
    try:
        cursor = conn.cursor()
        sql = """
        SELECT
            DocumentId,
            SubmissionId,
            RequestId,
            DocumentType,
            BlobUrl,
            BlobContainer,
            BlobPath,
            ContentType,
            FileSizeBytes,
            UploadedAt,
            OcrStatus,
            StorageRetentionUntil,
            IsDeleted,
            DeletedAt
        FROM [dbo].[Documents]
        WHERE SubmissionId = ?
          AND RequestId = ?
          AND OcrStatus = 'PENDING'
          AND (IsDeleted = 0 OR IsDeleted IS NULL)
        ORDER BY UploadedAt ASC
        """
        
        _log_if_available('log_database_query', sql, (submission_id, request_id), None)
        cursor.execute(sql, submission_id, request_id)

        columns = [c[0] for c in cursor.description]
        rows: List[Dict[str, Any]] = []
        for db_row in cursor.fetchall():
            row_dict = {col: val for col, val in zip(columns, db_row)}
            rows.append(row_dict)

        _log_if_available('log_database_query', sql, (submission_id, request_id), len(rows))
        _log_if_available('log_info', f'Found {len(rows)} pending documents for processing')
        
        # Log document details
        for i, row in enumerate(rows, 1):
            blob_path = row.get('BlobPath', 'Unknown')
            content_type = row.get('ContentType', 'Unknown')
            file_size = row.get('FileSizeBytes', 0)
            size_mb = round(file_size / (1024 * 1024), 2) if file_size else 0
            _log_if_available('log_info', f'   Document {i}: {blob_path} ({content_type}, {size_mb} MB)')

        return rows
    except Exception as e:
        _log_if_available('log_error', f'Failed to fetch processed documents: {str(e)}', e)
        raise
    finally:
        conn.close()


def build_document_row(
    *,
    doc_type_name: str,
    blob_info: Dict[str, str],
    submission_id: Optional[int] = None,
    request_id: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Build a dict representing one row for dbo.Documents based on doc type and blob info.
    """
    doc_type_code = DOC_TYPE_NAME_TO_DB_CODE.get(doc_type_name)

    uploaded_at = datetime.utcnow()
    retention_until = (
        uploaded_at + timedelta(days=DB_STORAGE_RETENTION_DAYS)
        if DB_STORAGE_RETENTION_DAYS
        else None
    )

    return {
        "SubmissionId": submission_id,
        "RequestId": request_id,
        "DocumentType": doc_type_code,
        "BlobUrl": blob_info["blob_url"],
        "BlobContainer": blob_info["container"],
        "BlobPath": blob_info["blob_path"],
        "FileName": blob_info["file_name"],
        "ContentType": blob_info["content_type"],
        "FileSizeBytes": int(blob_info["file_size"]),
        "UploadedAt": uploaded_at,
        "OcrStatus": DB_DEFAULT_OCR_STATUS,
        "StorageRetentionUntil": retention_until,
        "IsDeleted": 0,
        "DeletedAt": None,
    }


def check_document_exists(request_id: int, document_type: str) -> Optional[int]:
    """
    Check if a document exists for the given RequestId and DocumentType.
    Returns the DocumentId if exists, None otherwise.
    """
    if not DB_CONNECTION_STRING:
        raise ValueError("Database connection string is not configured.")
    
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    try:
        cursor = conn.cursor()
        sql = """
        SELECT DocumentId
        FROM [dbo].[ProcessedDocument]
        WHERE RequestId = ?
          AND DocumentType = ?
          AND (IsDeleted = 0 OR IsDeleted IS NULL)
        """
        cursor.execute(sql, request_id, document_type)
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def update_document(document_id: int, row: Dict[str, Any]) -> None:
    """
    Update an existing document record in [dbo].[ProcessedDocument].
    """
    if not DB_CONNECTION_STRING:
        raise ValueError("Database connection string is not configured.")
    
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    try:
        cursor = conn.cursor()
        sql = """
        UPDATE [dbo].[ProcessedDocument]
        SET BlobUrl = ?,
            BlobContainer = ?,
            BlobPath = ?,
            FileName = ?,
            ContentType = ?,
            FileSizeBytes = ?,
            UploadedAt = ?,
            OcrStatus = ?,
            StorageRetentionUntil = ?,
            IsDeleted = ?,
            DeletedAt = ?
        WHERE DocumentId = ?
        """
        cursor.execute(
            sql,
            row["BlobUrl"],
            row["BlobContainer"],
            row["BlobPath"],
            row["FileName"],
            row["ContentType"],
            row["FileSizeBytes"],
            row["UploadedAt"],
            row["OcrStatus"],
            row["StorageRetentionUntil"],
            row["IsDeleted"],
            row["DeletedAt"],
            document_id,
        )
        conn.commit()
        print(f"[OK] Updated existing document (DocumentId={document_id})")
    finally:
        conn.close()


def insert_documents(rows: List[Dict[str, Any]]) -> None:
    """
    Insert or update multiple rows into [dbo].[ProcessedDocument].
    
    For each row:
    - Check if a document exists with the same RequestId and DocumentType
    - If exists: update the existing record
    - If not exists: insert a new record
    """
    if not rows:
        _log_if_available('log_info', 'No documents to insert/update')
        return

    _log_if_available('log_database_operation', 'INSERT/UPDATE', 'ProcessedDocument', f'Processing {len(rows)} document records')

    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        inserted_count = 0
        updated_count = 0
        
        for i, row in enumerate(rows, 1):
            request_id = row["RequestId"]
            document_type = row["DocumentType"]
            
            _log_if_available('log_info', f'Processing document {i}/{len(rows)}: RequestId={request_id}, DocumentType={document_type}')
            
            # Check if document exists
            existing_doc_id = check_document_exists(request_id, document_type)
            
            if existing_doc_id:
                # Update existing document
                _log_if_available('log_info', f'Document exists (RequestId={request_id}, DocumentType={document_type}). Updating...')
                update_document(existing_doc_id, row)
                updated_count += 1
            else:
                # Insert new document
                _log_if_available('log_info', f'Document does not exist (RequestId={request_id}, DocumentType={document_type}). Inserting...')
                sql = """
                INSERT INTO [dbo].[ProcessedDocument] (
                    SubmissionId,
                    RequestId,
                    DocumentType,
                    BlobUrl,
                    BlobContainer,
                    BlobPath,
                    FileName,
                    ContentType,
                    FileSizeBytes,
                    UploadedAt,
                    OcrStatus,
                    StorageRetentionUntil,
                    IsDeleted,
                    DeletedAt
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                
                params = (
                    row["SubmissionId"],
                    row["RequestId"],
                    row["DocumentType"],
                    row["BlobUrl"],
                    row["BlobContainer"],
                    row["BlobPath"],
                    row["FileName"],
                    row["ContentType"],
                    row["FileSizeBytes"],
                    row["UploadedAt"],
                    row["OcrStatus"],
                    row["StorageRetentionUntil"],
                    row["IsDeleted"],
                    row["DeletedAt"],
                )
                
                _log_if_available('log_database_query', sql, params, None)
                cursor.execute(sql, *params)
                conn.commit()
                inserted_count += 1
                _log_if_available('log_info', f'Inserted new document (RequestId={request_id}, DocumentType={document_type})')
        
        _log_if_available('log_database_operation', 'INSERT/UPDATE', 'ProcessedDocument', 
                         f'Completed: {inserted_count} inserted, {updated_count} updated', 
                         inserted_count + updated_count)
                         
    except Exception as e:
        _log_if_available('log_error', f'Failed to insert/update documents: {str(e)}', e)
        raise
    finally:
        conn.close()


# List of Customers columns that are datetime/date
DATE_COLUMNS = {
    "EmiratesIDExpiryDate",
    "EmiratesIDIssueDate",
    "LicenseIssueDate",
    "LicenseExpiryDate",
    "DateOfFirstRegistration",
    "InsuranceExpiryDate",
    "MulkiyaExpiryDate",
    "DateOfBirth"
}

def update_customers_fields(request_id: int, updates: Dict[str, Any]) -> None:
    """
    UPDATE existing [dbo].[Customers] row for the given RequestId.
    Date columns are normalized using utils.date_utils.normalize_date_for_sql.
    """
    _log_if_available('log_database_operation', 'UPDATE', 'Customers', f'Updating customer fields for RequestId={request_id}')
    
    if not DB_CONNECTION_STRING:
        error_msg = "Database connection string is not configured."
        _log_if_available('log_error', error_msg)
        raise ValueError(error_msg)
        
    if not updates:
        _log_if_available('log_info', 'No Customers fields to update')
        return

    _log_if_available('log_info', f'Updating {len(updates)} customer fields for RequestId={request_id}')
    
    # Log the fields being updated (without values for security)
    field_names = list(updates.keys())
    _log_if_available('log_info', f'Fields to update: {", ".join(field_names)}')

    # Normalize values (especially dates)
    prepared_updates: Dict[str, Any] = {}
    date_fields_processed = []
    
    for col, val in updates.items():
        if col in DATE_COLUMNS:
            original_val = val
            normalized_val = normalize_date_for_sql(val)
            prepared_updates[col] = normalized_val
            date_fields_processed.append(col)
            
            if normalized_val != original_val:
                _log_if_available('log_info', f'Date field {col}: normalized from "{original_val}" to "{normalized_val}"')
        else:
            prepared_updates[col] = val
    
    if date_fields_processed:
        _log_if_available('log_info', f'Processed date fields: {", ".join(date_fields_processed)}')

    columns: List[str] = list(prepared_updates.keys())
    set_clause = ", ".join(f"{col} = ?" for col in columns)

    sql = f"""
        UPDATE [dbo].[Customers]
        SET {set_clause}, UpdatedAt = GETUTCDATE()
        WHERE RequestId = ?
    """

    params: List[Any] = [prepared_updates[col] for col in columns]
    params.append(request_id)

    conn = pyodbc.connect(DB_CONNECTION_STRING)
    try:
        cursor = conn.cursor()
        _log_if_available('log_database_query', sql, tuple(params), None)
        cursor.execute(sql, params)
        affected_rows = cursor.rowcount
        conn.commit()
        
        _log_if_available('log_database_operation', 'UPDATE', 'Customers', 
                         f'Updated customer fields for RequestId={request_id}', affected_rows)
        
        if affected_rows == 0:
            _log_if_available('log_warning', f'No customer record found for RequestId={request_id}')
        else:
            _log_if_available('log_info', f'Successfully updated {len(columns)} fields for RequestId={request_id}')
            
    except Exception as e:
        _log_if_available('log_error', f'Failed to update customer fields: {str(e)}', e)
        raise
    finally:
        conn.close()


def update_customers_ocr_status(request_id: int, status: str) -> None:
    """
    Update OcrStatus field in [dbo].[Customers] for the given RequestId.
    
    Args:
        request_id: The RequestId to update
        status: Either 'SUCCESS' or 'FAILED'
    """
    if not DB_CONNECTION_STRING:
        raise ValueError("Database connection string is not configured.")
    
    if status not in ['SUCCESS', 'FAILED']:
        raise ValueError(f"Invalid status: {status}. Must be 'SUCCESS' or 'FAILED'.")
    
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    try:
        cursor = conn.cursor()
        sql = """
        UPDATE [dbo].[Customers]
        SET OcrStatus = ?, UpdatedAt = GETUTCDATE()
        WHERE RequestId = ?
        """
        cursor.execute(sql, status, request_id)
        conn.commit()
        print(f"[OK] Updated Customers OcrStatus to '{status}' for RequestId={request_id}")
    finally:
        conn.close()


def execute_customer_validations(request_id: int) -> None:
    """
    Execute the customer validation stored procedure.
    
    Args:
        request_id: The RequestId to validate
    """
    _log_if_available('log_database_operation', 'EXEC', 'ExecuteAllCustomerValidations', f'Running validations for RequestId={request_id}')
    
    conn = None
    cursor = None
    
    try:
        # Get a fresh database connection with proper settings
        conn = _get_db_connection()
        
        # Set connection to autocommit mode to avoid transaction context issues
        conn.autocommit = True
        
        cursor = conn.cursor()
        
        stored_proc = "EXEC ExecuteAllCustomerValidations @RequestId = ?"
        _log_if_available('log_database_query', stored_proc, (request_id,), None)
        
        # Execute the stored procedure
        cursor.execute(stored_proc, (request_id,))
        
        # Process any results from the stored procedure (if any)
        while cursor.nextset():
            pass
        
        _log_if_available('log_database_operation', 'EXEC', 'ExecuteAllCustomerValidations', 
                         f'Successfully executed customer validations for RequestId={request_id}')
        _log_if_available('log_info', f'Customer validation procedure completed for RequestId={request_id}')
        
    except Exception as e:
        _log_if_available('log_error', f'Failed to execute customer validations for RequestId={request_id}: {str(e)}', e)
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass


def get_customer_validation_status(request_id: int) -> Optional[Dict[str, Any]]:
    """
    Get the customer validation status for a specific RequestId.
    
    Args:
        request_id: The RequestId to check
        
    Returns:
        Dict with Id, RequestId, and ValidationStatus or None if not found
    """
    _log_if_available('log_database_operation', 'SELECT', 'Customers', f'Checking validation status for RequestId={request_id}')
    
    conn = None
    cursor = None
    
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        sql = """
        SELECT TOP (1) 
            Id,
            RequestId,
            ValidationStatus
        FROM [dbo].[Customers]
        WHERE RequestId = ?
        """
        
        _log_if_available('log_database_query', sql, (request_id,), None)
        cursor.execute(sql, (request_id,))
        row = cursor.fetchone()
        
        if row:
            result = {
                "Id": row[0],
                "RequestId": row[1],
                "ValidationStatus": row[2],
            }
            _log_if_available('log_info', f'Customer validation status for RequestId={request_id}: {result["ValidationStatus"]}')
            return result
        else:
            _log_if_available('log_warning', f'No customer record found for RequestId={request_id}')
            return None
            
    except Exception as e:
        _log_if_available('log_error', f'Failed to get customer validation status for RequestId={request_id}: {str(e)}', e)
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass


def check_portal_status_failures(request_id: int) -> List[Dict[str, Any]]:
    """
    Check if there are any portal status failures for a RequestId.
    
    Args:
        request_id: The RequestId to check
        
    Returns:
        List of dictionaries with RequestId, PortalName, and Status
    """
    _log_if_available('log_database_operation', 'SELECT', 'RequestsPortalStatus', f'Checking portal status failures for RequestId={request_id}')
    
    conn = None
    cursor = None
    
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        sql = """
        SELECT 
            RequestId,
            PortalName,
            Status
        FROM [dbo].[RequestsPortalStstus]
        WHERE RequestId = ? 
          AND PortalName = 'AllPortals'
          AND Status = 'FAILED'
        """
        
        _log_if_available('log_database_query', sql, (request_id,), None)
        cursor.execute(sql, (request_id,))
        rows = cursor.fetchall()
        
        results = []
        for row in rows:
            results.append({
                "RequestId": row[0],
                "PortalName": row[1],
                "Status": row[2],
            })
        
        _log_if_available('log_info', f'Found {len(results)} portal status failures for RequestId={request_id}')
        return results
            
    except Exception as e:
        _log_if_available('log_error', f'Failed to check portal status failures for RequestId={request_id}: {str(e)}', e)
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass


def get_validation_failure_details(request_id: int) -> List[Dict[str, Any]]:
    """
    Get detailed validation failure information for a RequestId.
    
    Args:
        request_id: The RequestId to check
        
    Returns:
        List of dictionaries with Id, RequestId, ValidationRule, and ValidationError
    """
    _log_if_available('log_database_operation', 'SELECT', 'RequestsValidationFailures', f'Getting validation failure details for RequestId={request_id}')
    
    conn = None
    cursor = None
    
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        
        sql = """
        SELECT 
            Id,
            RequestId,
            ValidationRule,
            ValidationError
        FROM [dbo].[RequestsValidationFailures]
        WHERE RequestId = ?
        ORDER BY Id
        """
        
        _log_if_available('log_database_query', sql, (request_id,), None)
        cursor.execute(sql, (request_id,))
        rows = cursor.fetchall()
        
        results = []
        for row in rows:
            results.append({
                "Id": row[0],
                "RequestId": row[1],
                "ValidationRule": row[2],
                "ValidationError": row[3],
            })
        
        _log_if_available('log_info', f'Found {len(results)} validation failure details for RequestId={request_id}')
        return results
            
    except Exception as e:
        _log_if_available('log_error', f'Failed to get validation failure details for RequestId={request_id}: {str(e)}', e)
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except:
                pass
        if conn:
            try:
                conn.close()
            except:
                pass