# services/azure_ocr_client.py

import json
from pathlib import Path

from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential

from config.settings import (
    AZURE_DI_ENDPOINT,
    AZURE_DI_KEY,
    AZURE_DI_LAYOUT_MODEL_ID,
    OCR_OUTPUT_DIR,
)


def analyze_processed_pdf(pdf_path: Path) -> Path:
    """
    Run Azure Document Intelligence (layout) on the given PDF
    and save the PURE OCR output (no invoice splitting) as JSON.

    JSON structure:
    {
      "FileName": "...",
      "PageCount": N,
      "Pages": [
        {
          "PageNumber": 1,
          "Text": "...."
        },
        ...
      ],
      "FullText": "all pages concatenated"
    }
    """
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    if not AZURE_DI_ENDPOINT or not AZURE_DI_KEY:
        raise ValueError("Azure Document Intelligence endpoint/key are not configured.")

    # Ensure output folder exists
    OCR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Initialize Azure client
    client = DocumentIntelligenceClient(
        endpoint=AZURE_DI_ENDPOINT,
        credential=AzureKeyCredential(AZURE_DI_KEY),
    )

    # Analyze whole PDF with layout model
    with pdf_path.open("rb") as f:
        poller = client.begin_analyze_document(
            model_id=AZURE_DI_LAYOUT_MODEL_ID,
            body=f.read(),
            content_type="application/pdf",
        )
        result = poller.result()

    pages_data = []
    full_text_parts = []

    for page in result.pages:
        line_texts = [line.content for line in page.lines]
        page_text = " ".join(line_texts).strip()

        pages_data.append(
            {
                "PageNumber": page.page_number,
                "Text": page_text,
            }
        )

        if page_text:
            full_text_parts.append(page_text)

    full_text = "\n".join(full_text_parts).strip()

    output = {
        "FileName": pdf_path.name,
        "PageCount": len(result.pages),
        "Pages": pages_data,
        "FullText": full_text,
    }

    output_json_path = OCR_OUTPUT_DIR / f"{pdf_path.stem}_layout.json"
    with output_json_path.open("w", encoding="utf-8") as out_file:
        json.dump(output, out_file, indent=4, ensure_ascii=False)

    return output_json_path
