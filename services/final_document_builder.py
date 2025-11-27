# services/final_document_builder.py

import json
from pathlib import Path
from typing import Any, Dict, List

from pypdf import PdfReader, PdfWriter

from config.settings import FINAL_DOCUMENTS_DIR, DOC_TYPE_NAME_TO_FILENAME


def build_final_documents_from_classification(
    merged_pdf_path: Path,
    classified_json_path: Path,
) -> List[Dict[str, Any]]:
    """
    Using the merged PDF and the classified JSON output, create one PDF per document type:
    - Emirates ID document: all pages where Doc Type == configured "Emirates ID"
    - Driving License document: all pages where Doc Type == configured "Driving License"
    - Vehicle Registration document: all pages where Doc Type == configured "Vehicle Registration"
    - Other Document: all pages where Doc Type == configured "Other Document"

    Returns: list of paths to created PDF files.
    Build one PDF per document type (including Other Document) and
    return a list of { "doc_type": <name>, "path": <Path> }.
    """
    if not merged_pdf_path.exists():
        raise FileNotFoundError(f"Merged PDF not found: {merged_pdf_path}")
    if not classified_json_path.exists():
        raise FileNotFoundError(f"Classified JSON not found: {classified_json_path}")

    if not DOC_TYPE_NAME_TO_FILENAME:
        raise ValueError("Document type configuration is empty. Check config/doc_types.yml")

    FINAL_DOCUMENTS_DIR.mkdir(parents=True, exist_ok=True)

    # Load classification JSON
    with classified_json_path.open("r", encoding="utf-8") as f:
        classification: Dict[str, Any] = json.load(f)

    pages_info: List[Dict[str, Any]] = classification.get("Pages", [])
    if not pages_info:
        return []

    # Read merged PDF
    reader = PdfReader(str(merged_pdf_path))

    # Map: doc_type_name -> list of 0-based page indices
    doc_type_to_pages: Dict[str, List[int]] = {name: [] for name in DOC_TYPE_NAME_TO_FILENAME.keys()}

    for page_entry in pages_info:
        doc_type = page_entry.get("Doc Type")
        page_number = page_entry.get("page")

        # Only care about configured doc types (including "Other Document")
        if doc_type not in DOC_TYPE_NAME_TO_FILENAME:
            continue
        if not isinstance(page_number, int):
            continue

        page_index = page_number - 1
        if 0 <= page_index < len(reader.pages):
            doc_type_to_pages[doc_type].append(page_index)

    created_docs: List[Dict[str, Any]] = []
    # Build one PDF per configured doc type that has pages
    for doc_type_name, page_indices in doc_type_to_pages.items():
        if not page_indices:
            continue

        writer = PdfWriter()
        for idx in page_indices:
            writer.add_page(reader.pages[idx])

        filename = DOC_TYPE_NAME_TO_FILENAME[doc_type_name]
        output_path = FINAL_DOCUMENTS_DIR / filename

        with output_path.open("wb") as f_out:
            writer.write(f_out)

        created_docs.append(
            {
                "doc_type": doc_type_name,
                "path": output_path,
            }
        )

    return created_docs
