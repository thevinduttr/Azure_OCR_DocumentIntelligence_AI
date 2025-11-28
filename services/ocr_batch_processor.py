# services/ocr_batch_processor.py

from pathlib import Path
from typing import Dict, Any, List

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
    print(f"[INFO] Selected SubmissionId={submission_id}, RequestId={request_id} for processing.")

    # Mark as processed immediately (we are taking it for processing)
    mark_submission_as_processed(submission_id)
    print(f"[INFO] Marked SubmissionId={submission_id} as processed (IsProcessed=1).")

    # Fetch ProcessedDocument rows for this submission/request
    processed_docs = fetch_processed_documents_for(submission_id=submission_id, request_id=request_id)
    if not processed_docs:
        print(f"[WARN] No pending ProcessedDocument rows for SubmissionId={submission_id}, RequestId={request_id}.")
        return

    # Clean raw_documents folder and download fresh files there
    _clean_raw_documents_folder()

    for row in processed_docs:
        container = row["BlobContainer"]
        blob_path = row["BlobPath"]
        file_name = Path(blob_path).name
        local_path = RAW_DOCUMENTS_DIR / file_name

        print(f"  - Downloading {blob_path} -> {local_path}")
        download_blob_to_file(container=container, blob_path=blob_path, target_path=local_path)

    # Parent prefix from first doc's BlobPath (common main folder)
    first_blob_path = processed_docs[0]["BlobPath"]
    parent_prefix = _extract_parent_prefix_from_blob_path(first_blob_path)

    # Merge all raw docs -> single processed PDF
    merged_pdf_path = merge_documents_to_pdf(RAW_DOCUMENTS_DIR, PROCESSED_PDF_PATH)
    print(f"  [OK] Merged PDF: {merged_pdf_path}")

    # OCR
    ocr_json_path = analyze_processed_pdf(merged_pdf_path)
    print(f"  [OK] OCR JSON: {ocr_json_path}")

    # AI classify
    classified_json_path = classify_document_from_ocr_json(ocr_json_path)
    print(f"  [OK] Classified JSON: {classified_json_path}")

    # Build final PDFs by Doc Type (Emirates ID, DL, Mulkiya, Other)
    final_docs = build_final_documents_from_classification(
        merged_pdf_path=merged_pdf_path,
        classified_json_path=classified_json_path,
    )
    for d in final_docs:
        print(f"  [OK] Final doc: {d['doc_type']} -> {d['path']}")

    # Upload final docs and insert into dbo.Documents
    rows_to_insert: List[Dict[str, Any]] = []

    for d in final_docs:
        doc_type_name = d["doc_type"]
        file_path = d["path"]

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

    insert_documents(rows_to_insert)
    print(f"  [OK] Uploaded {len(rows_to_insert)} final docs and inserted into [dbo].[Documents].")
