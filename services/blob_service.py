# services/blob_service.py

from pathlib import Path
from typing import Dict
import time

from azure.storage.blob import BlobServiceClient, ContentSettings

from config.settings import (
    BLOB_CONNECTION_STRING,
    BLOB_CONTAINER_NAME,
    BLOB_PARENT_PREFIX,
    BLOB_PROCESSED_SUBFOLDER,
    BLOB_ACCOUNT_URL,
)

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


_blob_client = None


def _get_blob_service_client() -> BlobServiceClient:
    global _blob_client
    if _blob_client is None:
        if not BLOB_CONNECTION_STRING:
            error_msg = "Blob storage connection string is not configured."
            _log_if_available('log_error', error_msg)
            raise ValueError(error_msg)
        
        _log_if_available('log_info', 'Initializing Azure Blob Service Client')
        try:
            _blob_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
            _log_if_available('log_info', 'Azure Blob Service Client initialized successfully')
        except Exception as e:
            _log_if_available('log_error', f'Failed to initialize Blob Service Client: {str(e)}', e)
            raise
    
    return _blob_client

def download_blob_to_file(container: str, blob_path: str, target_path: Path) -> None:
    start_time = time.time()
    _log_if_available('log_blob_operation', 'DOWNLOAD', container, blob_path, f'Downloading to {target_path}')
    
    try:
        service_client = _get_blob_service_client()
        container_client = service_client.get_container_client(container)

        # Create target directory if needed
        target_path.parent.mkdir(parents=True, exist_ok=True)
        _log_if_available('log_file_operation', 'CREATE_DIR', str(target_path.parent), 'Created target directory')

        # Download blob
        _log_if_available('log_info', f'Starting blob download: {container}/{blob_path}')
        
        with target_path.open("wb") as f_out:
            stream = container_client.download_blob(blob_path)
            data = stream.readall()
            f_out.write(data)
            
            # Log download details
            file_size = len(data)
            duration_ms = int((time.time() - start_time) * 1000)
            
            _log_if_available('log_blob_download', container, blob_path, str(target_path), file_size)
            _log_if_available('log_performance_metric', f'Blob Download ({blob_path})', duration_ms, 'ms')
            
            if file_size > 0:
                speed_mbps = (file_size / (1024 * 1024)) / max((duration_ms / 1000), 0.001)
                _log_if_available('log_performance_metric', f'Download Speed ({blob_path})', round(speed_mbps, 2), 'MB/s')
        
        _log_if_available('log_info', f'Successfully downloaded blob to {target_path}')
        
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        _log_if_available('log_error', f'Failed to download blob {container}/{blob_path}: {str(e)}', e)
        _log_if_available('log_performance_metric', f'Failed Blob Download ({blob_path})', duration_ms, 'ms')
        raise


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
    start_time = time.time()
    
    if not file_path.exists():
        error_msg = f"File to upload not found: {file_path}"
        _log_if_available('log_error', error_msg)
        raise FileNotFoundError(error_msg)

    # Get file information
    file_size = file_path.stat().st_size
    size_mb = round(file_size / (1024 * 1024), 2)
    
    _log_if_available('log_info', f'Preparing to upload file: {file_path.name} ({size_mb} MB, {content_type})')
    
    try:
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
        
        _log_if_available('log_blob_operation', 'UPLOAD', BLOB_CONTAINER_NAME, blob_path, f'Uploading {file_name} ({size_mb} MB)')

        # Upload the file
        _log_if_available('log_info', f'Starting blob upload: {BLOB_CONTAINER_NAME}/{blob_path}')
        
        with file_path.open("rb") as data:
            container_client.upload_blob(
                name=blob_path,
                data=data,
                overwrite=True,
                content_settings=ContentSettings(content_type=content_type),
            )

        # Calculate performance metrics
        duration_ms = int((time.time() - start_time) * 1000)
        
        # Blob URL: <account_url>/<container>/<blob_path>
        base_url = BLOB_ACCOUNT_URL.rstrip("/")
        blob_url = f"{base_url}/{BLOB_CONTAINER_NAME}/{blob_path}"
        
        result = {
            "container": BLOB_CONTAINER_NAME,
            "blob_path": blob_path,
            "blob_url": blob_url,
            "file_name": file_name,
            "content_type": content_type,
            "file_size": str(file_size),
        }
        
        # Log successful upload
        _log_if_available('log_blob_upload', BLOB_CONTAINER_NAME, blob_path, str(file_path), file_size)
        _log_if_available('log_performance_metric', f'Blob Upload ({file_name})', duration_ms, 'ms')
        
        if size_mb > 0 and duration_ms > 0:
            speed_mbps = size_mb / max((duration_ms / 1000), 0.001)
            _log_if_available('log_performance_metric', f'Upload Speed ({file_name})', round(speed_mbps, 2), 'MB/s')
        
        _log_if_available('log_info', f'Successfully uploaded {file_name} to blob storage')
        _log_if_available('log_info', f'Blob URL: {blob_url}')
        
        return result
        
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        _log_if_available('log_error', f'Failed to upload file {file_name} to blob storage: {str(e)}', e)
        _log_if_available('log_performance_metric', f'Failed Blob Upload ({file_name})', duration_ms, 'ms')
        raise
