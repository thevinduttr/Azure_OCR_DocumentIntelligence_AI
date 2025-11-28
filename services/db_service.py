# services/db_service.py

from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

import pyodbc

from config.settings import (
    DB_CONNECTION_STRING,
    DB_DEFAULT_SUBMISSION_ID,
    DB_DEFAULT_REQUEST_ID,
    DB_DEFAULT_OCR_STATUS,
    DB_STORAGE_RETENTION_DAYS,
    DOC_TYPE_NAME_TO_DB_CODE,
)


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
        FROM [dbo].[Document]
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


def insert_documents(rows: List[Dict[str, Any]]) -> None:
    """
    Insert multiple rows into [dbo].[Documents].

    Assumes identity column [DocumentId] is auto-generated (IDENTITY).
    """
    if not rows:
        return

    conn = _get_db_connection()
    try:
        cursor = conn.cursor()
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

        for row in rows:
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
    finally:
        conn.close()
