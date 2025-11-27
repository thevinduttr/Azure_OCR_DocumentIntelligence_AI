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
        "SubmissionId": submission_id or DB_DEFAULT_SUBMISSION_ID,
        "RequestId": request_id or DB_DEFAULT_REQUEST_ID,
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
        INSERT INTO [dbo].[Documents] (
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
