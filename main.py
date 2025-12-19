# main.py

from pathlib import Path
import time
import traceback
import json
from collections import Counter

from config.settings import RAW_DOCUMENTS_DIR, PROCESSED_PDF_PATH , DATA_DIR
from services.db_service import (
    fetch_next_submission_to_process,
    mark_submission_as_processed,
    fetch_processed_documents_for,
    build_document_row,
    insert_documents,
    update_customers_fields,
    update_customers_ocr_status,
    execute_customer_validations,
)
from services.blob_service import download_blob_to_file, upload_file_to_blob
from services.document_merger import merge_documents_to_pdf
from services.azure_ocr_client import analyze_processed_pdf
from services.document_classifier import classify_document_from_ocr_json
from services.final_document_builder import build_final_documents_from_classification
from services.customer_data_mapper import build_customer_updates_from_classification
from services.logging_service import get_ocr_logger
from utils.error_notification_service import send_processing_error_notification
from utils.env_config import load_env_file
from config.settings import PROJECT_ROOT

# Load environment variables for email configuration
load_env_file(PROJECT_ROOT / "config" / "mail.env")



def build_local_filename(blob_path: str, content_type: str) -> str:
    """
    Build a local filename using BlobPath and ContentType.
    - If BlobPath already has an extension, keep it.
    - If not, infer extension from ContentType (pdf/jpeg/png/tiff).
    """
    name = Path(blob_path).name
    stem = Path(name).stem
    suffix = Path(name).suffix

    if suffix:
        return name

    ct = (content_type or "").lower()

    if "pdf" in ct:
        ext = ".pdf"
    elif "jpeg" in ct or "jpg" in ct:
        ext = ".jpg"
    elif "png" in ct:
        ext = ".png"
    elif "tif" in ct:
        ext = ".tif"
    else:
        ext = ""

    return stem + ext


def clean_raw_documents_folder() -> None:
    RAW_DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)
    for item in RAW_DOCUMENTS_DIR.iterdir():
        if item.is_file():
            item.unlink()


def clean_data_folder() -> None:
    """
    Clean the DATA_DIR folder at startup.
    Removes all files and subdirectories, skipping hidden folders.
    """
    import shutil
    
    if DATA_DIR.exists():
        for item in DATA_DIR.iterdir():
            # Skip hidden files/folders (starting with .)
            if item.name.startswith('.'):
                continue
                
            try:
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            except Exception as e:
                print(f"[WARN] Could not delete {item}: {e}")
        print(f"[INFO] Cleaned data folder: {DATA_DIR}")
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def extract_parent_prefix_from_blob_path(blob_path: str) -> str:
    """
    e.g. '2025/11/12/613690000/WhatsApp-Image-2025-09-25.jpg'
    -> '2025/11/12/613690000'
    """
    parts = blob_path.rsplit("/", 1)
    return parts[0] if len(parts) > 1 else ""


def main():
    print("[INFO] Starting continuous OCR processing service...")
    
    while True:
        logger = None
        try:
            # 1) Get next submission based on priority (High -> Medium -> Normal)
            submission = fetch_next_submission_to_process()
            if not submission:
                print("[INFO] No submissions pending for OCR. Waiting 10 seconds...")
                time.sleep(2.5)
                continue
            
            submission_id = submission["Id"]
            request_id = submission["RequestId"]
            priority_level = submission.get("PriorityLevel", "Normal")
            
            # Initialize logging for this request
            logger = get_ocr_logger()
            log_file = logger.start_request_logging(
                submission_id=submission_id, 
                request_id=request_id,
                priority_level=priority_level
            )
            
            logger.log_step("Data Folder Cleanup", "Cleaning data folder before processing")
            clean_data_folder()
            logger.log_step("Data Folder Cleanup", "Data folder cleaned successfully", "COMPLETED")
            
            logger.log_step("Submission Lock", f"Marking submission {submission_id} as processed")
            # 2) Mark it as taken for processing
            mark_submission_as_processed(submission_id)
            logger.log_step("Submission Lock", "Submission locked for processing", "COMPLETED")
            
            try:
                # 3) Get ProcessedDocument rows for this submission & request
                logger.log_step("Document Discovery", "Fetching documents for processing")
                processed_docs = fetch_processed_documents_for(submission_id=submission_id, request_id=request_id)
                if not processed_docs:
                    logger.log_warning(f"No pending ProcessedDocument rows found for SubmissionId={submission_id}, RequestId={request_id}")
                    logger.log_step("Document Discovery", "No documents found to process", "FAILED")
                    update_customers_ocr_status(request_id=request_id, status="FAILED")
                    continue

                logger.log_document_info(len(processed_docs), processed_docs)
                logger.log_step("Document Discovery", f"Found {len(processed_docs)} documents to process", "COMPLETED")

                # 4) Download all source docs to raw_documents folder using correct content type/extension
                logger.log_step("Document Download", "Starting document download from blob storage")
                clean_raw_documents_folder()

                downloaded_count = 0
                for i, row in enumerate(processed_docs, 1):
                    container = row["BlobContainer"]
                    blob_path = row["BlobPath"]
                    content_type = row.get("ContentType") or ""

                    local_name = build_local_filename(blob_path, content_type)
                    local_path = RAW_DOCUMENTS_DIR / local_name

                    logger.log_download_progress(i-1, len(processed_docs), Path(blob_path).name)
                    download_blob_to_file(container=container, blob_path=blob_path, target_path=local_path)
                    downloaded_count += 1
                    logger.log_download_progress(downloaded_count, len(processed_docs))

                logger.log_step("Document Download", f"Downloaded {downloaded_count} documents successfully", "COMPLETED")
                
                # 5) Determine parent prefix for upload from first BlobPath
                first_blob_path = processed_docs[0]["BlobPath"]
                parent_prefix = extract_parent_prefix_from_blob_path(first_blob_path)

                # 6) Merge all downloaded docs into a single PDF
                logger.log_step("Document Merge", "Merging all documents into single PDF")
                merged_pdf_path = merge_documents_to_pdf(RAW_DOCUMENTS_DIR, PROCESSED_PDF_PATH)
                logger.log_step("Document Merge", f"Merged PDF created: {merged_pdf_path.name}", "COMPLETED")

                # 7) OCR with Azure Document Intelligence
                logger.log_step("OCR Processing", "Starting Azure Document Intelligence OCR")
                ocr_json_path = analyze_processed_pdf(merged_pdf_path)
                
                # Read OCR results to get page count
                try:
                    with open(ocr_json_path, 'r', encoding='utf-8') as f:
                        ocr_data = json.load(f)
                    page_count = len(ocr_data.get('Pages', []))
                    logger.log_ocr_results(page_count, ocr_json_path)
                except Exception as e:
                    logger.log_warning(f"Could not read OCR results for statistics: {str(e)}")
                    page_count = 0
                
                logger.log_step("OCR Processing", f"OCR completed for {page_count} pages", "COMPLETED")

                # 8) Classification & field extraction using Azure OpenAI
                logger.log_step("AI Classification", "Starting Azure OpenAI document classification")
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
                
                logger.log_step("AI Classification", "AI classification completed successfully", "COMPLETED")

                # 9) Build final PDFs by Doc Type (Emirates ID, Driving License, Vehicle Registration, Other)
                logger.log_step("Final Document Generation", "Creating final documents by document type")
                final_docs = build_final_documents_from_classification(
                    merged_pdf_path=merged_pdf_path,
                    classified_json_path=classified_json_path,
                )
                logger.log_final_documents(final_docs)
                logger.log_step("Final Document Generation", f"Generated {len(final_docs)} final documents", "COMPLETED")

                # 10) Upload final PDFs to blob and insert metadata into dbo.Documents
                logger.log_step("Document Upload", "Uploading final documents to blob storage")
                rows_to_insert = []
                uploaded_count = 0

                for i, d in enumerate(final_docs, 1):
                    doc_type_name = d["doc_type"]
                    file_path = d["path"]

                    logger.log_upload_progress(i-1, len(final_docs), Path(file_path).name)
                    
                    blob_info = upload_file_to_blob(
                        file_path=file_path,
                        parent_prefix=parent_prefix,  # same main path, under processed_document/
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

                logger.log_step("Database Insert", "Inserting document metadata into database")
                insert_documents(rows_to_insert)
                logger.log_step("Database Insert", f"Inserted {len(rows_to_insert)} document records", "COMPLETED")
                logger.log_step("Document Upload", "All documents uploaded successfully", "COMPLETED")

                # 11) Build Customers field updates from AI classification
                logger.log_step("Customer Data Extraction", "Extracting customer data from AI classification")
                customer_updates = build_customer_updates_from_classification(
                    classified_json_path=classified_json_path,
                )
                logger.log_customer_updates(len(customer_updates), customer_updates)
                logger.log_step("Customer Data Extraction", f"Extracted {len(customer_updates)} customer fields", "COMPLETED")

                # 12) Apply updates to existing Customers row for this RequestId
                logger.log_step("Customer Database Update", "Updating customer record in database")
                update_customers_fields(
                    request_id=request_id,
                    updates=customer_updates,
                )
                logger.log_step("Customer Database Update", "Customer record updated successfully", "COMPLETED")
                
                # 13) Mark OCR processing as successful
                logger.log_step("Status Update", "Marking OCR processing as successful")
                update_customers_ocr_status(request_id=request_id, status="SUCCESS")
                logger.log_step("Status Update", "OCR status updated to SUCCESS", "COMPLETED")
                
                # 14) Execute customer validations stored procedure
                logger.log_step("Customer Validation", "Executing customer validation procedures")
                try:
                    execute_customer_validations(request_id=request_id)
                    logger.log_step("Customer Validation", "Customer validations completed", "COMPLETED")
                    logger.log_info(f"Customer validations executed successfully for RequestId={request_id}")
                except Exception as validation_error:
                    error_msg = f"Customer validation failed for RequestId={request_id}: {str(validation_error)}"
                    logger.log_warning(error_msg)
                    logger.log_step("Customer Validation", "Validation failed but continuing", "FAILED")
                    # Don't mark OCR as failed - validation is separate from OCR processing
                
                logger.complete_request_logging("SUCCESS")
                
            except Exception as e:
                if logger:
                    logger.log_error(f"Processing failed for RequestId={request_id}: {str(e)}", e)
                    logger.log_step("Error Recovery", "Marking OCR processing as failed")
                else:
                    print(f"\n[ERROR] Processing failed for RequestId={request_id}: {str(e)}")
                    traceback.print_exc()
                
                # Send error notification
                try:
                    import asyncio
                    
                    error_context = {
                        "submission_id": submission_id,
                        "request_id": request_id,
                        "priority_level": priority_level,
                        "documents_processed": len(processed_docs) if 'processed_docs' in locals() else 0,
                        "processing_stage": "Main OCR Processing Pipeline"
                    }
                    
                    # Determine processing step based on local variables
                    if 'merged_pdf_path' not in locals():
                        step_name = "Document Download & Merge"
                    elif 'ocr_json_path' not in locals():
                        step_name = "Azure Document Intelligence OCR"
                    elif 'classified_json_path' not in locals():
                        step_name = "AI Document Classification"
                    elif 'final_docs' not in locals():
                        step_name = "Final Document Generation"
                    elif 'rows_to_insert' not in locals():
                        step_name = "Document Upload"
                    else:
                        step_name = "Customer Data Processing"
                    
                    error_context["failed_step"] = step_name
                    
                    try:
                        loop = asyncio.get_event_loop()
                        if loop.is_running():
                            asyncio.create_task(send_processing_error_notification(
                                step_name=step_name,
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
                                step_name=step_name,
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
                                step_name=step_name,
                                error=e,
                                submission_id=submission_id,
                                request_id=request_id,
                                additional_context=error_context,
                                logger=logger
                            ))
                            loop.close()
                        except Exception as notification_error:
                            if logger:
                                logger.log_error(f"Failed to send error notification: {str(notification_error)}")
                            
                except Exception as notification_error:
                    if logger:
                        logger.log_error(f"Failed to send error notification: {str(notification_error)}")
                
                # Mark OCR processing as failed
                try:
                    update_customers_ocr_status(request_id=request_id, status="FAILED")
                    if logger:
                        logger.log_step("Error Recovery", "OCR status updated to FAILED", "COMPLETED")
                        logger.complete_request_logging("FAILED")
                except Exception as update_error:
                    error_msg = f"Failed to update OcrStatus to FAILED: {str(update_error)}"
                    if logger:
                        logger.log_error(error_msg, update_error)
                    else:
                        print(f"[ERROR] {error_msg}")

            
            # Small delay before processing next submission
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("\n[INFO] Shutting down OCR processing service...")
            if logger:
                logger.log_info("Service shutdown requested by user")
            break
        except Exception as outer_error:
            error_msg = f"Unexpected error in main loop: {str(outer_error)}"
            if logger:
                logger.log_error(error_msg, outer_error)
                logger.complete_request_logging("FAILED")
            else:
                print(f"\n[ERROR] {error_msg}")
                traceback.print_exc()
            print("[INFO] Continuing with next iteration after 5 seconds...")
            time.sleep(2.5)


if __name__ == "__main__":
    main()
