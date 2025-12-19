# services/ocr_batch_processor.py

from pathlib import Path
from typing import Dict, Any, List
import json
from collections import Counter

from config.settings import RAW_DOCUMENTS_DIR, PROCESSED_PDF_PATH
from services.db_service import (
    fetch_next_submission_to_process,
    mark_submission_as_processed,
    fetch_processed_documents_for,
    build_document_row,
    insert_documents,
)
from services.blob_service import download_blob_to_file, upload_file_to_blob
from services.document_merger import merge_documents_to_pdf
from services.azure_ocr_client import analyze_processed_pdf
from services.document_classifier import classify_document_from_ocr_json
from services.final_document_builder import build_final_documents_from_classification
from services.logging_service import get_ocr_logger
from utils.error_notification_service import send_processing_error_notification


def _clean_raw_documents_folder() -> None:
    RAW_DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)
    for item in RAW_DOCUMENTS_DIR.iterdir():
        if item.is_file():
            item.unlink()

def _content_type_to_extension(content_type: str) -> str:
    """
    Map ContentType from DB to a suitable file extension.
    """
    ct = (content_type or "").lower().strip()

    if ct == "application/pdf":
        return ".pdf"
    if ct in ("image/jpeg", "image/jpg"):
        return ".jpg"
    if ct == "image/png":
        return ".png"
    if ct in ("image/tiff", "image/tif"):
        return ".tiff"
    if ct == "image/bmp":
        return ".bmp"
    if ct == "image/gif":
        return ".gif"

    # Fallback: default to .bin if unknown
    return ".bin"

def _extract_parent_prefix_from_blob_path(blob_path: str) -> str:
    """
    e.g. '2025/11/12/613690000/WhatsApp-Image-2025-09-25.jpg'
    -> '2025/11/12/613690000'
    """
    parts = blob_path.rsplit("/", 1)
    return parts[0] if len(parts) > 1 else ""


def process_next_submission() -> None:
    """
    Process the next available submission based on:
      - IsProcessed = 0
      - PriorityLevel: High -> Medium -> Normal
      - Oldest ReceivedAt in that priority

    Steps:
      1. Pick one submission.
      2. Mark IsProcessed = 1.
      3. Fetch its pending ProcessedDocument rows.
      4. Download blobs into RAW_DOCUMENTS_DIR.
      5. Merge -> OCR -> Classify -> Build final docs.
      6. Upload final docs to blob (under <main_prefix>/processed_document/).
      7. Insert rows into [dbo].[Documents] using SubmissionId + RequestId.
    """
    submission = fetch_next_submission_to_process()
    if not submission:
        print("[INFO] No submissions pending for OCR.")
        return

    submission_id = submission["Id"]
    request_id = submission["RequestId"]
    priority_level = submission.get("PriorityLevel", "Normal")
    
    # Initialize logging
    logger = get_ocr_logger()
    logger.start_request_logging(
        submission_id=submission_id, 
        request_id=request_id,
        priority_level=priority_level
    )
    
    logger.log_step("Submission Selection", f"Selected submission {submission_id} with priority {priority_level}")

    # Mark as processed immediately (we are taking it for processing)
    logger.log_step("Submission Lock", "Marking submission as processed")
    mark_submission_as_processed(submission_id)
    logger.log_step("Submission Lock", "Submission locked for processing", "COMPLETED")

    try:
        # Fetch ProcessedDocument rows for this submission/request
        logger.log_step("Document Discovery", "Fetching documents for processing")
        processed_docs = fetch_processed_documents_for(submission_id=submission_id, request_id=request_id)
        if not processed_docs:
            logger.log_warning(f"No pending ProcessedDocument rows for SubmissionId={submission_id}, RequestId={request_id}")
            logger.log_step("Document Discovery", "No documents found", "FAILED")
            logger.complete_request_logging("FAILED")
            return

        logger.log_document_info(len(processed_docs), processed_docs)
        logger.log_step("Document Discovery", f"Found {len(processed_docs)} documents", "COMPLETED")

        # Clean raw_documents folder and download fresh files there
        logger.log_step("Document Download", "Starting document download")
        _clean_raw_documents_folder()

        downloaded_count = 0
        for i, row in enumerate(processed_docs, 1):
            container = row["BlobContainer"]
            blob_path = row["BlobPath"]
            file_name = Path(blob_path).name
            local_path = RAW_DOCUMENTS_DIR / file_name

            logger.log_download_progress(i-1, len(processed_docs), file_name)
            download_blob_to_file(container=container, blob_path=blob_path, target_path=local_path)
            downloaded_count += 1
            logger.log_download_progress(downloaded_count, len(processed_docs))

        logger.log_step("Document Download", f"Downloaded {downloaded_count} documents successfully", "COMPLETED")

        # Parent prefix from first doc's BlobPath (common main folder)
        first_blob_path = processed_docs[0]["BlobPath"]
        parent_prefix = _extract_parent_prefix_from_blob_path(first_blob_path)

        # Merge all raw docs -> single processed PDF
        logger.log_step("Document Merge", "Merging documents into single PDF")
        merged_pdf_path = merge_documents_to_pdf(RAW_DOCUMENTS_DIR, PROCESSED_PDF_PATH)
        logger.log_step("Document Merge", f"Merged PDF created: {merged_pdf_path.name}", "COMPLETED")

        # OCR
        logger.log_step("OCR Processing", "Starting Azure Document Intelligence OCR")
        ocr_json_path = analyze_processed_pdf(merged_pdf_path)
        
        # Read OCR results for statistics
        try:
            with open(ocr_json_path, 'r', encoding='utf-8') as f:
                ocr_data = json.load(f)
            page_count = len(ocr_data.get('Pages', []))
            logger.log_ocr_results(page_count, ocr_json_path)
        except Exception as e:
            logger.log_warning(f"Could not read OCR results: {str(e)}")
            page_count = 0
        
        logger.log_step("OCR Processing", f"OCR completed for {page_count} pages", "COMPLETED")

        # AI classify
        logger.log_step("AI Classification", "Starting document classification")
        classified_json_path = classify_document_from_ocr_json(ocr_json_path)
        
        # Analyze classification results
        try:
            with open(classified_json_path, 'r', encoding='utf-8') as f:
                classification_data = json.load(f)
            pages = classification_data.get('Pages', [])
            doc_types = Counter(page.get('Doc Type', 'Unknown') for page in pages)
            logger.log_classification_results(len(pages), classified_json_path, dict(doc_types))
        except Exception as e:
            logger.log_warning(f"Could not analyze classification results: {str(e)}")
        
        logger.log_step("AI Classification", "Classification completed", "COMPLETED")

        # Build final PDFs by Doc Type (Emirates ID, DL, Mulkiya, Other)
        logger.log_step("Final Document Generation", "Creating final documents by type")
        final_docs = build_final_documents_from_classification(
            merged_pdf_path=merged_pdf_path,
            classified_json_path=classified_json_path,
        )
        logger.log_final_documents(final_docs)
        logger.log_step("Final Document Generation", f"Generated {len(final_docs)} final documents", "COMPLETED")

        # Upload final docs and insert into dbo.Documents
        logger.log_step("Document Upload", "Uploading documents to blob storage")
        rows_to_insert: List[Dict[str, Any]] = []
        uploaded_count = 0

        for i, d in enumerate(final_docs, 1):
            doc_type_name = d["doc_type"]
            file_path = d["path"]

            logger.log_upload_progress(i-1, len(final_docs), Path(file_path).name)

            blob_info = upload_file_to_blob(
                file_path=file_path,
                parent_prefix=parent_prefix,
                content_type="application/pdf",
            )

            row = build_document_row(
                doc_type_name=doc_type_name,
                blob_info=blob_info,
                submission_id=submission_id,
                request_id=request_id,
            )
            rows_to_insert.append(row)
            uploaded_count += 1
            logger.log_upload_progress(uploaded_count, len(final_docs))

        logger.log_step("Database Insert", "Inserting document metadata")
        insert_documents(rows_to_insert)
        logger.log_step("Database Insert", f"Inserted {len(rows_to_insert)} document records", "COMPLETED")
        logger.log_step("Document Upload", "Upload completed successfully", "COMPLETED")
        
        logger.complete_request_logging("SUCCESS")
        
    except Exception as e:
        logger.log_error(f"Processing failed: {str(e)}", e)
        
        # Send error notification
        try:
            import asyncio
            
            # Determine the processing step where the error occurred
            error_context = {
                "submission_id": submission_id,
                "request_id": request_id,
                "priority_level": priority_level,
                "documents_count": len(processed_docs) if 'processed_docs' in locals() else 0,
                "error_location": "OCR Batch Processing Pipeline"
            }
            
            # Try to run the notification
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.create_task(send_processing_error_notification(
                        step_name="OCR Batch Processing",
                        error=e,
                        submission_id=submission_id,
                        request_id=request_id,
                        additional_context=error_context,
                        logger=logger
                    ))
                else:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(send_processing_error_notification(
                        step_name="OCR Batch Processing",
                        error=e,
                        submission_id=submission_id,
                        request_id=request_id,
                        additional_context=error_context,
                        logger=logger
                    ))
                    loop.close()
            except RuntimeError:
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(send_processing_error_notification(
                        step_name="OCR Batch Processing",
                        error=e,
                        submission_id=submission_id,
                        request_id=request_id,
                        additional_context=error_context,
                        logger=logger
                    ))
                    loop.close()
                except Exception as notification_error:
                    logger.log_error(f"Failed to send error notification: {str(notification_error)}")
                    
        except Exception as notification_error:
            logger.log_error(f"Failed to send error notification: {str(notification_error)}")
        
        logger.complete_request_logging("FAILED")
        raise
