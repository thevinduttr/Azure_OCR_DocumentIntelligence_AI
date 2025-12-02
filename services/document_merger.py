# services/document_merger.py

import io
from pathlib import Path
import time

from pypdf import PdfReader, PdfWriter
from PIL import Image

SUPPORTED_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".tif", ".tiff"}

# Import logging service with error handling
try:
    from services.logging_service import get_ocr_logger
    _logger_available = True
except ImportError:
    _logger_available = False

def _log_if_available(func_name, *args, **kwargs):
    """Helper to log if logger is available."""
    if _logger_available:
        try:
            logger = get_ocr_logger()
            getattr(logger, func_name)(*args, **kwargs)
        except Exception:
            pass  # Continue if logging fails


def merge_documents_to_pdf(input_dir: Path, output_pdf_path: Path) -> Path:
    """
    Merge all PDF and image documents in input_dir into a single PDF.
    """
    start_time = time.time()
    _log_if_available('log_info', f'Starting document merge process')
    _log_if_available('log_info', f'Input directory: {input_dir}')
    _log_if_available('log_info', f'Output file: {output_pdf_path}')
    
    if not input_dir.exists():
        error_msg = f"Input directory does not exist: {input_dir}"
        _log_if_available('log_error', error_msg)
        raise FileNotFoundError(error_msg)

    # Make sure output directory exists
    output_pdf_path.parent.mkdir(parents=True, exist_ok=True)
    _log_if_available('log_file_operation', 'CREATE_DIR', str(output_pdf_path.parent), 'Created output directory')

    writer = PdfWriter()

    files = sorted(
        [f for f in input_dir.iterdir() if f.is_file()],
        key=lambda x: x.name.lower()
    )

    if not files:
        error_msg = f"No files found in input directory: {input_dir}"
        _log_if_available('log_error', error_msg)
        raise ValueError(error_msg)

    _log_if_available('log_info', f'Found {len(files)} files to merge:')
    
    pdf_count = 0
    image_count = 0
    skipped_count = 0
    total_pages = 0
    
    for i, file in enumerate(files, 1):
        ext = file.suffix.lower()
        file_size_mb = round(file.stat().st_size / (1024 * 1024), 2)
        
        _log_if_available('log_info', f'  {i}. {file.name} ({ext}, {file_size_mb} MB)')
        
        try:
            if ext == ".pdf":
                pages_added = _append_pdf(file, writer)
                pdf_count += 1
                total_pages += pages_added
                _log_if_available('log_info', f'     ✓ Added PDF with {pages_added} page(s)')
            elif ext in SUPPORTED_IMAGE_EXTENSIONS:
                _append_image_as_pdf(file, writer)
                image_count += 1
                total_pages += 1
                _log_if_available('log_info', f'     ✓ Added image as 1 page')
            else:
                skipped_count += 1
                _log_if_available('log_warning', f'Skipping unsupported file type: {file.name}')
        except Exception as e:
            _log_if_available('log_error', f'Failed to process file {file.name}: {str(e)}', e)
            raise

    # Write the merged PDF
    _log_if_available('log_file_operation', 'WRITE', str(output_pdf_path), 'Writing merged PDF')
    
    try:
        with output_pdf_path.open("wb") as f_out:
            writer.write(f_out)
        
        # Get final file size
        final_size_mb = round(output_pdf_path.stat().st_size / (1024 * 1024), 2)
        duration_ms = int((time.time() - start_time) * 1000)
        
        # Log completion statistics
        _log_if_available('log_info', f'Document merge completed successfully!')
        _log_if_available('log_info', f'Merge Statistics:')
        _log_if_available('log_info', f'  - PDF files processed: {pdf_count}')
        _log_if_available('log_info', f'  - Image files processed: {image_count}')
        _log_if_available('log_info', f'  - Files skipped: {skipped_count}')
        _log_if_available('log_info', f'  - Total pages in merged PDF: {total_pages}')
        _log_if_available('log_info', f'  - Final file size: {final_size_mb} MB')
        
        _log_if_available('log_data_processing', 'Document Merge', len(files), 1, f'Merged {len(files)} files into single PDF')
        _log_if_available('log_performance_metric', 'Document Merge Duration', duration_ms, 'ms')
        _log_if_available('log_file_operation', 'WRITE', str(output_pdf_path), f'Successfully created merged PDF ({final_size_mb} MB)')
        
        return output_pdf_path
        
    except Exception as e:
        _log_if_available('log_error', f'Failed to write merged PDF: {str(e)}', e)
        raise


def _append_pdf(pdf_path: Path, writer: PdfWriter) -> int:
    """Append PDF pages to writer and return number of pages added."""
    try:
        reader = PdfReader(str(pdf_path))
        page_count = len(reader.pages)
        
        for page in reader.pages:
            writer.add_page(page)
        
        _log_if_available('log_debug', f'Successfully processed PDF: {pdf_path.name} ({page_count} pages)')
        return page_count
        
    except Exception as e:
        _log_if_available('log_error', f'Failed to process PDF {pdf_path.name}: {str(e)}', e)
        raise


def _append_image_as_pdf(image_path: Path, writer: PdfWriter) -> None:
    """Convert image to PDF and append to writer."""
    try:
        with Image.open(str(image_path)) as img:
            # Get image info for logging
            width, height = img.size
            mode = img.mode
            
            _log_if_available('log_debug', f'Processing image: {image_path.name} ({width}x{height}, {mode})')
            
            img = img.convert("RGB")

            buffer = io.BytesIO()
            img.save(buffer, format="PDF")
            buffer.seek(0)

            reader = PdfReader(buffer)
            writer.add_page(reader.pages[0])
            
            _log_if_available('log_debug', f'Successfully converted image to PDF: {image_path.name}')
            
    except Exception as e:
        _log_if_available('log_error', f'Failed to process image {image_path.name}: {str(e)}', e)
        raise
