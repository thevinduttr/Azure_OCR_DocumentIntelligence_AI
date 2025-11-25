# tests/test_document_merger.py

import sys
from pathlib import Path

# Ensure project root (where main.py is) is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pypdf import PdfReader, PdfWriter
from PIL import Image

from services.document_merger import merge_documents_to_pdf


def _create_sample_pdf(path: Path):
    """Create a simple 1-page PDF for testing."""
    writer = PdfWriter()
    writer.add_blank_page(width=200, height=200)
    with path.open("wb") as f:
        writer.write(f)


def _create_sample_image(path: Path):
    """Create a simple RGB image and save as JPG/PNG."""
    img = Image.new("RGB", (200, 200), color=(255, 0, 0))
    img.save(path)


def test_merge_documents_to_pdf(tmp_path: Path):
    # Arrange: create input directory with mixed files
    input_dir = tmp_path / "raw_docs"
    input_dir.mkdir()

    pdf_file = input_dir / "sample1.pdf"
    img_file = input_dir / "sample2.jpg"

    _create_sample_pdf(pdf_file)
    _create_sample_image(img_file)

    output_dir = tmp_path / "processed_docs"
    output_pdf = output_dir / "processed_document.pdf"

    # Act
    merged_path = merge_documents_to_pdf(input_dir, output_pdf)

    # Assert: output exists
    assert merged_path.exists(), "Merged PDF was not created."

    # Assert: output is a valid PDF with expected number of pages (2 files -> 2 pages)
    reader = PdfReader(str(merged_path))
    assert len(reader.pages) == 2, f"Expected 2 pages, got {len(reader.pages)}"
