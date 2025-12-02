# services/azure_ocr_client.py

import json
import time
from pathlib import Path

from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential

from config.settings import (
    AZURE_DI_ENDPOINT,
    AZURE_DI_KEY,
    AZURE_DI_LAYOUT_MODEL_ID,
    OCR_OUTPUT_DIR,
)

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
    start_time = time.time()
    _log_if_available('log_info', f'Starting Azure Document Intelligence OCR analysis')
    _log_if_available('log_info', f'Input PDF: {pdf_path.name}')
    
    if not pdf_path.exists():
        error_msg = f"PDF not found: {pdf_path}"
        _log_if_available('log_error', error_msg)
        raise FileNotFoundError(error_msg)
    
    # Log file info
    file_size_mb = round(pdf_path.stat().st_size / (1024 * 1024), 2)
    _log_if_available('log_info', f'PDF file size: {file_size_mb} MB')

    if not AZURE_DI_ENDPOINT or not AZURE_DI_KEY:
        error_msg = "Azure Document Intelligence endpoint/key are not configured."
        _log_if_available('log_error', error_msg)
        _log_if_available('log_configuration_load', 'azure.yml', 'FAILED', 'Missing endpoint or key')
        raise ValueError(error_msg)
    
    _log_if_available('log_configuration_load', 'azure.yml', 'SUCCESS', f'Using endpoint: {AZURE_DI_ENDPOINT[:50]}...')

    # Ensure output folder exists
    OCR_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    _log_if_available('log_file_operation', 'CREATE_DIR', str(OCR_OUTPUT_DIR), 'Created OCR output directory')

    # Initialize Azure client
    _log_if_available('log_info', 'Initializing Azure Document Intelligence client')
    try:
        client = DocumentIntelligenceClient(
            endpoint=AZURE_DI_ENDPOINT,
            credential=AzureKeyCredential(AZURE_DI_KEY),
        )
        _log_if_available('log_info', 'Azure Document Intelligence client initialized successfully')
    except Exception as e:
        _log_if_available('log_error', f'Failed to initialize Azure DI client: {str(e)}', e)
        raise

    # Analyze whole PDF with layout model
    _log_if_available('log_info', f'Starting OCR analysis with model: {AZURE_DI_LAYOUT_MODEL_ID}')
    _log_if_available('log_api_call', 'Azure Document Intelligence', 'begin_analyze_document', 'POST')
    
    try:
        with pdf_path.open("rb") as f:
            pdf_content = f.read()
            
        poller = client.begin_analyze_document(
            model_id=AZURE_DI_LAYOUT_MODEL_ID,
            body=pdf_content,
            content_type="application/pdf",
        )
        
        _log_if_available('log_info', 'OCR analysis request submitted, waiting for results...')
        
        # Wait for results and measure time
        api_start_time = time.time()
        result = poller.result()
        api_duration_ms = int((time.time() - api_start_time) * 1000)
        
        _log_if_available('log_api_call', 'Azure Document Intelligence', 'analyze_document', 'POST', 200, api_duration_ms)
        _log_if_available('log_info', f'OCR analysis completed in {api_duration_ms}ms')
        
    except Exception as e:
        api_duration_ms = int((time.time() - start_time) * 1000) if 'api_start_time' not in locals() else int((time.time() - api_start_time) * 1000)
        _log_if_available('log_api_call', 'Azure Document Intelligence', 'analyze_document', 'POST', None, api_duration_ms)
        _log_if_available('log_error', f'Azure Document Intelligence API call failed: {str(e)}', e)
        raise

    # Process OCR results
    _log_if_available('log_info', 'Processing OCR results...')
    
    pages_data = []
    full_text_parts = []
    total_text_length = 0
    page_text_lengths = []

    for page in result.pages:
        line_texts = [line.content for line in page.lines]
        page_text = " ".join(line_texts).strip()
        page_text_length = len(page_text)
        
        pages_data.append(
            {
                "PageNumber": page.page_number,
                "Text": page_text,
            }
        )

        if page_text:
            full_text_parts.append(page_text)
            total_text_length += page_text_length
            page_text_lengths.append(page_text_length)
        
        _log_if_available('log_debug', f'Page {page.page_number}: {len(page.lines)} lines, {page_text_length} characters')

    full_text = "\n".join(full_text_parts).strip()
    page_count = len(result.pages)
    
    _log_if_available('log_info', f'OCR Results Summary:')
    _log_if_available('log_info', f'  - Pages processed: {page_count}')
    _log_if_available('log_info', f'  - Total characters extracted: {total_text_length:,}')
    _log_if_available('log_info', f'  - Average characters per page: {total_text_length // max(page_count, 1):,}')
    
    _log_if_available('log_data_processing', 'OCR Text Extraction', page_count, page_count, 
                     f'Extracted text from {page_count} pages')

    output = {
        "FileName": pdf_path.name,
        "PageCount": page_count,
        "Pages": pages_data,
        "FullText": full_text,
    }

    # Save full OCR output
    output_json_path = OCR_OUTPUT_DIR / f"{pdf_path.stem}_layout.json"
    _log_if_available('log_file_operation', 'WRITE', str(output_json_path), 'Saving full OCR JSON')
    
    try:
        with output_json_path.open("w", encoding="utf-8") as out_file:
            json.dump(output, out_file, indent=4, ensure_ascii=False)
        
        full_json_size_kb = round(output_json_path.stat().st_size / 1024, 2)
        _log_if_available('log_info', f'Full OCR JSON saved: {output_json_path.name} ({full_json_size_kb} KB)')
        
    except Exception as e:
        _log_if_available('log_error', f'Failed to save full OCR JSON: {str(e)}', e)
        raise

    # Create simplified version for AI to reduce token usage
    _log_if_available('log_info', 'Creating simplified OCR format for AI processing...')
    
    try:
        simplified_output = convert_to_simplified_format(output)
        simplified_json_path = OCR_OUTPUT_DIR / f"{pdf_path.stem}_simplified.json"
        
        with simplified_json_path.open("w", encoding="utf-8") as out_file:
            json.dump(simplified_output, out_file, indent=4, ensure_ascii=False)
        
        simplified_json_size_kb = round(simplified_json_path.stat().st_size / 1024, 2)
        reduction_percent = ((full_json_size_kb - simplified_json_size_kb) / full_json_size_kb * 100) if full_json_size_kb > 0 else 0
        
        _log_if_available('log_info', f'Simplified OCR JSON saved: {simplified_json_path.name} ({simplified_json_size_kb} KB)')
        _log_if_available('log_performance_metric', 'OCR JSON Size Reduction', round(reduction_percent, 1), '%')
        _log_if_available('log_info', f'Token optimization: Reduced JSON size by {reduction_percent:.1f}% for AI processing')
        
    except Exception as e:
        _log_if_available('log_error', f'Failed to create simplified OCR JSON: {str(e)}', e)
        raise
    
    # Final performance metrics
    total_duration_ms = int((time.time() - start_time) * 1000)
    _log_if_available('log_performance_metric', 'Total OCR Processing Duration', total_duration_ms, 'ms')
    
    if page_count > 0:
        avg_time_per_page = total_duration_ms / page_count
        _log_if_available('log_performance_metric', 'Average OCR Time Per Page', round(avg_time_per_page, 1), 'ms')
    
    _log_if_available('log_info', f'OCR processing completed successfully in {total_duration_ms}ms')

    return simplified_json_path


def convert_to_simplified_format(ocr_data: dict) -> dict:
    """
    Convert full OCR output to simplified format with only page number and text content.
    This reduces token usage when sending to Azure OpenAI.
    
    Input format:
    {
        "FileName": "...",
        "PageCount": N,
        "Pages": [{"PageNumber": 1, "Text": "..."}],
        "FullText": "..."
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
    
    for page in ocr_data.get("Pages", []):
        page_number = page.get("PageNumber")
        page_text = page.get("Text", "")
        
        result["Pages"].append({
            "page": page_number,
            "page_content": page_text
        })
    
    return result
