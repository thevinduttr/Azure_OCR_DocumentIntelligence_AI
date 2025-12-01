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

def _get_db_connection():
    if not DB_CONNECTION_STRING:
        raise ValueError("Database connection string is not configured.")
    return pyodbc.connect(DB_CONNECTION_STRING)

def fetch_next_submission_to_process() -> Optional[Dict[str, Any]]:
    """
    Get the next submission to process based on:
      - IsProcessed = 0
      - PriorityLevel: High -> Medium -> Normal
      - Earliest ReceivedAt first within same priority
    Returns a single row dict or None.
    """
    if not DB_CONNECTION_STRING:
        raise ValueError("Database connection string is not configured.")

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
        cursor.execute(sql)
        row = cursor.fetchone()
        if not row:
            return None

        columns = [c[0] for c in cursor.description]
        return {col: val for col, val in zip(columns, row)}
    finally:
        conn.close()


def mark_submission_as_processed(submission_id: int) -> None:
    """
    Mark a submission as processed (IsProcessed = 1, ProcessedAt = now).
    """
    if not DB_CONNECTION_STRING:
        raise ValueError("Database connection string is not configured.")

    conn = pyodbc.connect(DB_CONNECTION_STRING)
    try:
        cursor = conn.cursor()
        sql = """
        UPDATE [dbo].[Submissions]
        SET IsProcessed = 1,
            ProcessedAt = GETUTCDATE()
        WHERE Id = ? AND IsProcessed = 0
        """
        cursor.execute(sql, submission_id)
        conn.commit()
    finally:
        conn.close()


def fetch_processed_documents_for(submission_id: int, request_id: int) -> List[Dict[str, Any]]:
    """
    Get Document rows for a given SubmissionId + RequestId
    with OcrStatus = 'PENDING'.
    """
    if not DB_CONNECTION_STRING:
        raise ValueError("Database connection string is not configured.")

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
        cursor.execute(sql, submission_id, request_id)

        columns = [c[0] for c in cursor.description]
        rows: List[Dict[str, Any]] = []
        for db_row in cursor.fetchall():
            row_dict = {col: val for col, val in zip(columns, db_row)}
            rows.append(row_dict)

        return rows
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
        return

    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
        
        for row in rows:
            request_id = row["RequestId"]
            document_type = row["DocumentType"]
            
            # Check if document exists
            existing_doc_id = check_document_exists(request_id, document_type)
            
            if existing_doc_id:
                # Update existing document
                print(f"[INFO] Document exists (RequestId={request_id}, DocumentType={document_type}). Updating...")
                update_document(existing_doc_id, row)
            else:
                # Insert new document
                print(f"[INFO] Document does not exist (RequestId={request_id}, DocumentType={document_type}). Inserting...")
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
                cursor.execute(
                    sql,
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
                conn.commit()
                print(f"[OK] Inserted new document (RequestId={request_id}, DocumentType={document_type})")
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
    if not DB_CONNECTION_STRING:
        raise ValueError("Database connection string is not configured.")
    if not updates:
        print("[INFO] No Customers fields to update.")
        return

    # Normalize values (especially dates)
    prepared_updates: Dict[str, Any] = {}
    for col, val in updates.items():
        if col in DATE_COLUMNS:
            prepared_updates[col] = normalize_date_for_sql(val)
        else:
            prepared_updates[col] = val

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
        cursor.execute(sql, params)
        conn.commit()
        print(f"[OK] Updated Customers for RequestId={request_id}")
        print(f"     Columns updated: {', '.join(columns)}")
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
    conn = _get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "EXEC ExecuteAllCustomerValidations @RequestId = ?",
            (request_id,)
        )
        conn.commit()
        print(f"[INFO] Executed customer validations for RequestId={request_id}")
    except Exception as e:
        print(f"[ERROR] Failed to execute customer validations: {e}")
        raise
    finally:
        cursor.close()
        conn.close()