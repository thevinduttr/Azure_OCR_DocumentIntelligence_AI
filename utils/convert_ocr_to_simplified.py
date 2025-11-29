"""
Utility script to convert full OCR JSON to simplified format.
This reduces token usage when sending to AI models.

Usage:
    python utils/convert_ocr_to_simplified.py <input_json_file> [output_json_file]

Example:
    python utils/convert_ocr_to_simplified.py data/ocr_output/document_layout.json
    python utils/convert_ocr_to_simplified.py data/ocr_output/document_layout.json data/ocr_output/simplified.json
"""

import json
import sys
from pathlib import Path


def convert_to_simplified_format(ocr_data: dict) -> dict:
    """
    Convert full OCR JSON to simplified format with only page number and text content.
    
    Input format (Azure Document Intelligence output):
    {
        "FileName": "...",
        "PageCount": N,
        "Pages": [{"PageNumber": 1, "Text": "..."}],
        "FullText": "..."
    }
    
    OR Alternative format (raw analyzeResult):
    {
        "analyzeResult": {
            "pages": [
                {
                    "pageNumber": 1,
                    "words": [{"content": "word1"}, {"content": "word2"}, ...]
                }
            ]
        }
    }
    
    Output format:
    {
        "Pages": [
            {"page": 1, "page_content": "..."},
            {"page": 2, "page_content": "..."}
        ]
    }
    """
    result = {"Pages": []}
    
    # Check if input has our standard format
    if "Pages" in ocr_data and isinstance(ocr_data["Pages"], list):
        for page in ocr_data["Pages"]:
            page_number = page.get("PageNumber") or page.get("page")
            page_text = page.get("Text", "")
            
            result["Pages"].append({
                "page": page_number,
                "page_content": page_text
            })
    
    # Check if input has Azure raw analyzeResult format
    elif "analyzeResult" in ocr_data:
        pages = ocr_data.get("analyzeResult", {}).get("pages", [])
        for pg in pages:
            page_number = pg.get("pageNumber")
            page_text = " ".join(w.get("content", "") for w in pg.get("words", []))
            
            result["Pages"].append({
                "page": page_number,
                "page_content": page_text
            })
    
    else:
        raise ValueError("Unrecognized OCR JSON format. Expected 'Pages' or 'analyzeResult' key.")
    
    return result


def main():
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python convert_ocr_to_simplified.py <input_json_file> [output_json_file]")
        print("\nExample:")
        print("  python utils/convert_ocr_to_simplified.py data/ocr_output/document_layout.json")
        sys.exit(1)

    input_file = Path(sys.argv[1])
    
    # Determine output file path
    if len(sys.argv) >= 3:
        output_file = Path(sys.argv[2])
    else:
        # Auto-generate output filename
        output_file = input_file.parent / f"{input_file.stem}_simplified.json"

    # Validate input file exists
    if not input_file.exists():
        print(f"❌ Error: Input file not found: {input_file}")
        sys.exit(1)

    print(f"📖 Reading OCR JSON from: {input_file}")
    
    # Load OCR JSON
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            ocr_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"❌ Error: Invalid JSON file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error reading file: {e}")
        sys.exit(1)

    print(f"✓ Loaded OCR data")
    
    # Convert to simplified format
    try:
        simplified_data = convert_to_simplified_format(ocr_data)
    except ValueError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    
    page_count = len(simplified_data.get("Pages", []))
    print(f"✓ Converted {page_count} pages to simplified format")

    # Write output JSON
    try:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(simplified_data, f, indent=4, ensure_ascii=False)
    except Exception as e:
        print(f"❌ Error writing output file: {e}")
        sys.exit(1)

    print(f"💾 Simplified JSON saved to: {output_file}")
    
    # Calculate size reduction
    input_size = input_file.stat().st_size
    output_size = output_file.stat().st_size
    reduction_percent = ((input_size - output_size) / input_size * 100) if input_size > 0 else 0
    
    print(f"\n📊 File size comparison:")
    print(f"   Original:   {input_size:,} bytes")
    print(f"   Simplified: {output_size:,} bytes")
    print(f"   Reduction:  {reduction_percent:.1f}%")
    print(f"\n✅ Conversion completed successfully!")


if __name__ == "__main__":
    main()
