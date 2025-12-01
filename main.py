# main.py

from pathlib import Path
import time
import traceback

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
        try:
            # 1) Get next submission based on priority (High -> Medium -> Normal)
            submission = fetch_next_submission_to_process()
            if not submission:
                print("[INFO] No submissions pending for OCR. Waiting 10 seconds...")
                time.sleep(2.5)
                continue
            
            # Clean data folder at startup
            clean_data_folder()
            
            submission_id = submission["Id"]
            request_id = submission["RequestId"]
            print(f"\n[INFO] Selected SubmissionId={submission_id}, RequestId={request_id} for processing.")

            # 2) Mark it as taken for processing
            mark_submission_as_processed(submission_id)
            print(f"[INFO] Marked SubmissionId={submission_id} as processed (IsProcessed=1).")
            
            try:
                # 3) Get ProcessedDocument rows for this submission & request
                processed_docs = fetch_processed_documents_for(submission_id=submission_id, request_id=request_id)
                if not processed_docs:
                    print(f"[WARN] No pending ProcessedDocument rows for SubmissionId={submission_id}, RequestId={request_id}.")
                    update_customers_ocr_status(request_id=request_id, status="FAILED")
                    continue

                # 4) Download all source docs to raw_documents folder using correct content type/extension
                clean_raw_documents_folder()

                for row in processed_docs:
                    container = row["BlobContainer"]
                    blob_path = row["BlobPath"]
                    content_type = row.get("ContentType") or ""

                    local_name = build_local_filename(blob_path, content_type)
                    local_path = RAW_DOCUMENTS_DIR / local_name

                    print(f"  - Downloading {blob_path} ({content_type}) -> {local_path}")
                    download_blob_to_file(container=container, blob_path=blob_path, target_path=local_path)

                # 5) Determine parent prefix for upload from first BlobPath
                first_blob_path = processed_docs[0]["BlobPath"]
                parent_prefix = extract_parent_prefix_from_blob_path(first_blob_path)

                # 6) Merge all downloaded docs into a single PDF
                merged_pdf_path = merge_documents_to_pdf(RAW_DOCUMENTS_DIR, PROCESSED_PDF_PATH)
                print(f"  [OK] Merged PDF: {merged_pdf_path}")

                # 7) OCR with Azure Document Intelligence
                ocr_json_path = analyze_processed_pdf(merged_pdf_path)
                print(f"  [OK] OCR JSON: {ocr_json_path}")

                # 8) Classification & field extraction using Azure OpenAI
                classified_json_path = classify_document_from_ocr_json(ocr_json_path)
                print(f"  [OK] Classified JSON: {classified_json_path}")

                # 9) Build final PDFs by Doc Type (Emirates ID, Driving License, Vehicle Registration, Other)
                final_docs = build_final_documents_from_classification(
                    merged_pdf_path=merged_pdf_path,
                    classified_json_path=classified_json_path,
                )
                for d in final_docs:
                    print(f"  [OK] Final doc: {d['doc_type']} -> {d['path']}")

                # 10) Upload final PDFs to blob and insert metadata into dbo.Documents
                rows_to_insert = []

                for d in final_docs:
                    doc_type_name = d["doc_type"]
                    file_path = d["path"]

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

                insert_documents(rows_to_insert)
                print(f"  [OK] Uploaded {len(rows_to_insert)} final docs and inserted into [dbo].[Documents].")

                # 11) Build Customers field updates from AI classification
                customer_updates = build_customer_updates_from_classification(
                    classified_json_path=classified_json_path,
                )
                print(f"  [OK] Built customer updates: {customer_updates}")

                # 12) Apply updates to existing Customers row for this RequestId
                update_customers_fields(
                    request_id=request_id,
                    updates=customer_updates,
                )
                print(f"  [OK] Updated Customers record for RequestId={request_id}.")
                
                # 13) Mark OCR processing as successful
                update_customers_ocr_status(request_id=request_id, status="SUCCESS")
                print(f"\n[SUCCESS] All processing steps completed successfully for RequestId={request_id}")
                
            except Exception as e:
                print(f"\n[ERROR] Processing failed for RequestId={request_id}: {str(e)}")
                traceback.print_exc()
                
                # Mark OCR processing as failed
                try:
                    update_customers_ocr_status(request_id=request_id, status="FAILED")
                except Exception as update_error:
                    print(f"[ERROR] Failed to update OcrStatus to FAILED: {str(update_error)}")
            
            # 14) Execute customer validations stored procedure (outside try-except)
            try:
                execute_customer_validations(request_id=request_id, portal_name_list='')
                print(f"[OK] Customer validations executed for RequestId={request_id}")
            except Exception as validation_error:
                print(f"[WARN] Customer validation failed for RequestId={request_id}: {str(validation_error)}")
                # Don't mark OCR as failed - validation is separate from OCR processing
            
            # Small delay before processing next submission
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("\n[INFO] Shutting down OCR processing service...")
            break
        except Exception as outer_error:
            print(f"\n[ERROR] Unexpected error in main loop: {str(outer_error)}")
            traceback.print_exc()
            print("[INFO] Continuing with next iteration after 5 seconds...")
            time.sleep(2.5)


if __name__ == "__main__":
    main()
