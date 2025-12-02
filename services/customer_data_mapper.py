"""
Customer Data Mapper Service
Loads classified document JSON, validates extracted fields, detects conflicts,
and builds database updates based on customer_mapping.yml configuration.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from config.settings import CUSTOMER_DOC_TYPES, CUSTOMER_FIELD_MAPPING

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


def _load_classification_json(path: Path) -> Dict[str, Any]:
    """Load classified JSON file."""
    if not path.exists():
        raise FileNotFoundError(f"Classified JSON not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _build_document_dataframes(pages: List[Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
    """
    Build DataFrames organized by document type.
    Returns: {doc_type: DataFrame} where df columns are the extracted fields.
    """
    # Build reverse mapping: AI doc type name → config key
    name_to_key: Dict[str, str] = {}
    for key, config in CUSTOMER_DOC_TYPES.items():
        for name in config.get("names", []):
            name_to_key[name] = key
    
    # Group pages by document type
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for page in pages:
        doc_type = page.get("Doc Type", "").strip()
        config_key = name_to_key.get(doc_type, "other")
        
        if config_key == "other":
            continue  # Skip "Other Document" pages
        
        grouped.setdefault(config_key, []).append(page)
    
    # Create DataFrames
    dfs: Dict[str, pd.DataFrame] = {}
    for config_key, page_list in grouped.items():
        if page_list:
            dfs[config_key] = pd.DataFrame(page_list)
    
    return dfs


def _validate_document_fields(dfs: Dict[str, pd.DataFrame]) -> None:
    """
    Validate that required fields are present and non-empty for each document.
    Print missing fields per document page.
    """
    print("\n" + "="*80)
    print("DOCUMENT FIELD VALIDATION")
    print("="*80)
    
    for doc_key, df in dfs.items():
        if df.empty:
            continue
        
        config = CUSTOMER_DOC_TYPES.get(doc_key, {})
        required_fields = config.get("required_fields", [])
        doc_names = config.get("names", [doc_key])
        doc_name = doc_names[0] if doc_names else doc_key
        
        if not required_fields:
            print(f"\n[{doc_name}] No required fields configured")
            continue
        
        print(f"\n[{doc_name}] Validating {len(df)} page(s)")
        
        for idx, row in df.iterrows():
            page_num = row.get("page", "?")
            missing = []
            
            for field in required_fields:
                value = row.get(field, "")
                if not isinstance(value, str) or not value.strip():
                    missing.append(field)
            
            if missing:
                print(f"  ❌ Page {page_num}: Missing fields → {', '.join(missing)}")
            else:
                print(f"  ✅ Page {page_num}: All required fields present")


def _extract_field_with_priority(
    dfs: Dict[str, pd.DataFrame],
    sources: List[Dict[str, Any]],
    db_column: str
) -> Optional[str]:
    """
    Extract field value from multiple sources based on priority.
    Detects conflicts when multiple documents have different values.
    Returns: (value, conflicts_detected)
    """
    # Sort sources by priority (lower number = higher priority)
    sorted_sources = sorted(sources, key=lambda x: x.get("priority", 999))
    
    values: List[tuple[str, str, int]] = []  # (value, doc_type, priority)
    
    for source in sorted_sources:
        doc_type = source["document"]
        field_name = source["field"]
        priority = source.get("priority", 999)
        
        # Find matching document in dfs
        doc_key = _find_doc_key_by_name(doc_type)
        if not doc_key or doc_key not in dfs:
            continue
        
        df = dfs[doc_key]
        if field_name not in df.columns:
            continue
        
        # Get first non-empty value from this document
        for value in df[field_name]:
            if isinstance(value, str) and value.strip():
                values.append((value.strip(), doc_type, priority))
                break
    
    if not values:
        return None
    
    # Check for conflicts (different values from different documents)
    if len(values) > 1:
        unique_values = set(v[0] for v in values)
        if len(unique_values) > 1:
            _print_conflict(db_column, values)
    
    # Return highest priority value
    return values[0][0]


def _find_doc_key_by_name(doc_name: str) -> Optional[str]:
    """Find document config key by AI document type name."""
    for key, config in CUSTOMER_DOC_TYPES.items():
        if doc_name in config.get("names", []):
            return key
    return None


def _print_conflict(db_column: str, values: List[tuple[str, str, int]]) -> None:
    """Print conflict warning when same field has different values from different documents."""
    print(f"\n⚠️  CONFLICT DETECTED for '{db_column}':")
    for value, doc_type, priority in values:
        marker = "✓ USING" if priority == values[0][2] else "✗ IGNORED"
        print(f"    {marker} [{doc_type}] = '{value}' (priority: {priority})")


def _apply_transform(value: Optional[str], transform: Optional[str]) -> Any:
    """Apply transformation to extracted value."""
    if value is None or transform is None:
        return value
    
    if transform == "invert_yes_no":
        # "Yes" → False (is GCC), empty/other → True (non-GCC)
        return False if value.lower() == "yes" else True
    
    return value


def build_customer_updates_from_classification(
    classified_json_path: Path,
) -> Dict[str, Any]:
    """
    Main entry point: Load classified JSON, validate, extract, and build DB updates.
    Returns: Dict of {DB_Column: value} ready for database update.
    """
    _log_if_available('log_step', 'Customer Data Mapping', 'Starting customer data extraction from classification')
    
    # Debug: Check if configuration is loaded
    if not CUSTOMER_DOC_TYPES:
        error_msg = "CUSTOMER_DOC_TYPES is empty! Check customer_mapping.yml at config/customer_mapping.yml"
        _log_if_available('log_error', error_msg)
        _log_if_available('log_configuration_load', 'customer_mapping.yml', 'FAILED', 'CUSTOMER_DOC_TYPES not found')
        return {}
    
    if not CUSTOMER_FIELD_MAPPING:
        error_msg = "CUSTOMER_FIELD_MAPPING is empty! Check customer_mapping.yml"
        _log_if_available('log_error', error_msg)
        _log_if_available('log_configuration_load', 'customer_mapping.yml', 'FAILED', 'CUSTOMER_FIELD_MAPPING not found')
        return {}
    
    _log_if_available('log_configuration_load', 'customer_mapping.yml', 'SUCCESS', 
                     f'Loaded {len(CUSTOMER_DOC_TYPES)} document types and {len(CUSTOMER_FIELD_MAPPING)} field mappings')
    _log_if_available('log_info', f'Configuration loaded: {len(CUSTOMER_DOC_TYPES)} document types, {len(CUSTOMER_FIELD_MAPPING)} field mappings')
    
    # 1. Load classified JSON
    _log_if_available('log_step', 'JSON Loading', 'Loading classification JSON file')
    data = _load_classification_json(classified_json_path)
    pages = data.get("Pages", [])
    if not isinstance(pages, list):
        pages = []
    
    _log_if_available('log_info', f'Loaded {len(pages)} pages from classification JSON: {classified_json_path.name}')
    _log_if_available('log_data_processing', 'JSON Loading', 1, len(pages), f'Processed classification file')
    
    if len(pages) == 0:
        _log_if_available('log_warning', 'No pages found in classification JSON')
        _log_if_available('log_step', 'JSON Loading', 'No pages to process', 'FAILED')
        return {}
    
    _log_if_available('log_step', 'JSON Loading', f'Successfully loaded {len(pages)} pages', 'COMPLETED')
    
    # 2. Build document-wise DataFrames
    _log_if_available('log_step', 'DataFrame Building', 'Creating document-wise DataFrames')
    dfs = _build_document_dataframes(pages)
    
    _log_if_available('log_info', f'Created DataFrames for {len(dfs)} document types:')
    for doc_key, df in dfs.items():
        doc_names = CUSTOMER_DOC_TYPES.get(doc_key, {}).get("names", [doc_key])
        doc_name = doc_names[0] if doc_names else doc_key
        _log_if_available('log_info', f'   - {doc_name}: {len(df)} page(s), {len(df.columns)} fields')
    
    _log_if_available('log_data_processing', 'DataFrame Building', len(pages), len(dfs), 'Grouped pages by document type')
    _log_if_available('log_step', 'DataFrame Building', f'Created {len(dfs)} DataFrames', 'COMPLETED')
    
    # 3. Validate document fields (prints missing fields)
    _log_if_available('log_step', 'Field Validation', 'Validating required document fields')
    _validate_document_fields(dfs)
    _log_if_available('log_step', 'Field Validation', 'Field validation completed', 'COMPLETED')
    
    # 4. Extract and map to database columns
    _log_if_available('log_step', 'Field Extraction', 'Extracting and mapping database fields')
    
    updates: Dict[str, Any] = {}
    successful_extractions = 0
    failed_extractions = 0
    
    for db_column, mapping in CUSTOMER_FIELD_MAPPING.items():
        sources = mapping.get("sources", [])
        transform = mapping.get("transform")
        
        if not sources:
            _log_if_available('log_warning', f'No sources configured for field {db_column}')
            continue
        
        _log_if_available('log_debug', f'Processing field: {db_column} with {len(sources)} source(s)')
        
        value = _extract_field_with_priority(dfs, sources, db_column)
        original_value = value
        value = _apply_transform(value, transform)
        
        if transform and original_value != value:
            _log_if_available('log_info', f'Applied transform "{transform}" to {db_column}: "{original_value}" → "{value}"')
        
        if value is not None and value != "":
            updates[db_column] = value
            successful_extractions += 1
            # Don't log actual values for security, just field names
            _log_if_available('log_info', f'✓ Extracted {db_column}')
        else:
            failed_extractions += 1
            _log_if_available('log_warning', f'✗ Could not extract {db_column}')
    
    _log_if_available('log_data_processing', 'Field Extraction', len(CUSTOMER_FIELD_MAPPING), 
                     successful_extractions, f'Successfully extracted {successful_extractions}/{len(CUSTOMER_FIELD_MAPPING)} fields')
    _log_if_available('log_step', 'Field Extraction', f'Extracted {successful_extractions} fields', 'COMPLETED')
    
    # 5. Final validation summary
    _log_if_available('log_step', 'Final Validation', 'Performing final data validation')
    
    essential = ["FirstName", "LastName", "Gender", "Nationality", "EmiratesID",
                 "EmiratesIDExpiryDate", "LicenseNumber", "LicenseExpiryDate",
                 "Make", "Model", "YearOfManufacture", "ChassisNumber", "EngineNumber"]
    
    missing_essential = [f for f in essential if f not in updates or not updates[f]]
    present_essential = [f for f in essential if f in updates and updates[f]]
    
    if missing_essential:
        _log_if_available('log_validation_step', 'Essential Fields Check', 'FAILED', 
                         f'Missing {len(missing_essential)} essential fields', missing_essential)
        for field in missing_essential:
            _log_if_available('log_warning', f'Missing essential field: {field}')
    else:
        _log_if_available('log_validation_step', 'Essential Fields Check', 'PASSED', 
                         'All essential fields extracted successfully')
    
    # Log extraction statistics
    _log_if_available('log_info', f'Extraction Results:')
    _log_if_available('log_info', f'  - Total fields extracted: {len(updates)}')
    _log_if_available('log_info', f'  - Essential fields present: {len(present_essential)}/{len(essential)}')
    _log_if_available('log_info', f'  - Essential fields missing: {len(missing_essential)}')
    
    if len(updates) > 0:
        _log_if_available('log_info', f'Successfully extracted customer data with {len(updates)} fields')
        _log_if_available('log_step', 'Final Validation', f'Validation completed - {len(updates)} fields ready for database', 'COMPLETED')
    else:
        _log_if_available('log_error', 'No customer data could be extracted')
        _log_if_available('log_step', 'Final Validation', 'No data extracted', 'FAILED')
    
    return updates
