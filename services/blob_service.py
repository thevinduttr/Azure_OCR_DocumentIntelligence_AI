# services/blob_service.py

from pathlib import Path
from typing import Dict

from azure.storage.blob import BlobServiceClient, ContentSettings

from config.settings import (
    BLOB_CONNECTION_STRING,
    BLOB_CONTAINER_NAME,
    BLOB_PARENT_PREFIX,
    BLOB_PROCESSED_SUBFOLDER,
    BLOB_ACCOUNT_URL,
)


_blob_client = None


def _get_blob_service_client() -> BlobServiceClient:
    global _blob_client
    if _blob_client is None:
        if not BLOB_CONNECTION_STRING:
            raise ValueError("Blob storage connection string is not configured.")
        _blob_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    return _blob_client


def upload_file_to_blob(
    file_path: Path,
    parent_prefix: str | None = None,
    content_type: str = "application/pdf",
) -> Dict[str, str]:
    """
    Upload a file to Azure Blob Storage under:
    <parent_prefix>/processed_document/<FileName>

    Returns dict with: container, blob_path, blob_url, file_name, content_type, file_size
    """
    if not file_path.exists():
        raise FileNotFoundError(f"File to upload not found: {file_path}")

    service_client = _get_blob_service_client()
    container_client = service_client.get_container_client(BLOB_CONTAINER_NAME)

    # Folder part: e.g. "2025/11/12/613690000"
    base_prefix = parent_prefix if parent_prefix is not None else BLOB_PARENT_PREFIX or ""
    base_prefix = base_prefix.strip("/")

    # Build blob path: 2025/11/12/613690000/processed_document/filename.pdf
    file_name = file_path.name
    if base_prefix:
        blob_path = f"{base_prefix}/{BLOB_PROCESSED_SUBFOLDER}/{file_name}"
    else:
        blob_path = f"{BLOB_PROCESSED_SUBFOLDER}/{file_name}"

    with file_path.open("rb") as data:
        container_client.upload_blob(
            name=blob_path,
            data=data,
            overwrite=True,
            content_settings=ContentSettings(content_type=content_type),
        )

    file_size = file_path.stat().st_size

    # Blob URL: <account_url>/<container>/<blob_path>
    base_url = BLOB_ACCOUNT_URL.rstrip("/")
    blob_url = f"{base_url}/{BLOB_CONTAINER_NAME}/{blob_path}"

    return {
        "container": BLOB_CONTAINER_NAME,
        "blob_path": blob_path,
        "blob_url": blob_url,
        "file_name": file_name,
        "content_type": content_type,
        "file_size": str(file_size),
    }
