# main.py

from services.document_merger import merge_documents_to_pdf
from services.azure_ocr_client import analyze_processed_pdf
from services.document_classifier import classify_document_from_ocr_json
from services.final_document_builder import build_final_documents_from_classification
from config.settings import RAW_DOCUMENTS_DIR, PROCESSED_PDF_PATH


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

    # Step 4: Build final separated documents (Emirates ID, DL, Mulkiya)
    final_docs = build_final_documents_from_classification(
        merged_pdf_path=merged_pdf_path,
        classified_json_path=classified_json_path,
    )
    print("[OK] Final document PDFs created:")
    for p in final_docs:
        print(f"  - {p}")


if __name__ == "__main__":
    main()
