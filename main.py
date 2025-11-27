# main.py

from services.document_merger import merge_documents_to_pdf
from services.azure_ocr_client import analyze_processed_pdf
from services.document_classifier import classify_document_from_ocr_json
from services.final_document_builder import build_final_documents_from_classification
from services.blob_service import upload_file_to_blob
from services.db_service import build_document_row, insert_documents
from config.settings import RAW_DOCUMENTS_DIR, PROCESSED_PDF_PATH ,BLOB_PARENT_PREFIX





def main():
    # Step 1: Merge docs into single PDF
    print(f"Input folder : {RAW_DOCUMENTS_DIR}")
    print(f"Output PDF   : {PROCESSED_PDF_PATH}")
    
    merged_pdf_path = merge_documents_to_pdf(RAW_DOCUMENTS_DIR, PROCESSED_PDF_PATH)
    print(f"[OK] Merged PDF created at: {merged_pdf_path}")

    # Step 2: Run Azure Document Intelligence (OCR → layout JSON)
    ocr_json_path = analyze_processed_pdf(merged_pdf_path)
    print(f"[OK] OCR layout JSON saved at: {ocr_json_path}")

    # Step 3: Run Azure OpenAI on OCR JSON (classification + fields)
    classified_json_path = classify_document_from_ocr_json(ocr_json_path)
    print(f"[OK] Classified document JSON saved at: {classified_json_path}")

    # Step 4: Build final grouped documents (by Doc Type)
    final_docs = build_final_documents_from_classification(
        merged_pdf_path=merged_pdf_path,
        classified_json_path=classified_json_path,
    )
    print("[OK] Final document PDFs created:")
    for d in final_docs:
        print(f"  - {d['doc_type']}: {d['path']}")

    # Step 5: Upload final documents to Blob and insert metadata into dbo.Documents
    rows_to_insert = []

    for d in final_docs:
        doc_type_name = d["doc_type"]
        file_path = d["path"]

        blob_info = upload_file_to_blob(
            file_path=file_path,
            parent_prefix=BLOB_PARENT_PREFIX,  # later you can pass value from another table
            content_type="application/pdf",
        )

        row = build_document_row(
            doc_type_name=doc_type_name,
            blob_info=blob_info,
            # submission_id=..., request_id=...  # if you want to override 70/65 later
        )
        rows_to_insert.append(row)

    insert_documents(rows_to_insert)
    print("[OK] Uploaded to blob and inserted metadata into [dbo].[Documents].")


if __name__ == "__main__":
    main()
