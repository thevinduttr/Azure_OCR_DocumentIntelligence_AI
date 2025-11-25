# services/document_merger.py

import io
from pathlib import Path

from pypdf import PdfReader, PdfWriter
from PIL import Image

SUPPORTED_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}


def merge_documents_to_pdf(input_dir: Path, output_pdf_path: Path) -> Path:
    """
    Merge all PDF and image documents in input_dir into a single PDF.
    """
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")

    # Make sure output directory exists
    output_pdf_path.parent.mkdir(parents=True, exist_ok=True)

    writer = PdfWriter()

    files = sorted(
        [f for f in input_dir.iterdir() if f.is_file()],
        key=lambda x: x.name.lower()
    )

    if not files:
        raise ValueError(f"No files found in input directory: {input_dir}")

    for file in files:
        ext = file.suffix.lower()

        if ext == ".pdf":
            _append_pdf(file, writer)
        elif ext in SUPPORTED_IMAGE_EXTENSIONS:
            _append_image_as_pdf(file, writer)
        else:
            print(f"[WARN] Skipping unsupported file type: {file.name}")

    with output_pdf_path.open("wb") as f_out:
        writer.write(f_out)

    return output_pdf_path


def _append_pdf(pdf_path: Path, writer: PdfWriter) -> None:
    reader = PdfReader(str(pdf_path))
    for page in reader.pages:
        writer.add_page(page)


def _append_image_as_pdf(image_path: Path, writer: PdfWriter) -> None:
    with Image.open(str(image_path)) as img:
        img = img.convert("RGB")

        buffer = io.BytesIO()
        img.save(buffer, format="PDF")
        buffer.seek(0)

        reader = PdfReader(buffer)
        writer.add_page(reader.pages[0])
