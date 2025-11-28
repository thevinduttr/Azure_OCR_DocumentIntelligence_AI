# services/customer_data_mapper.py

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from config.settings import (
    CUSTOMER_DOC_TYPES,
    CUSTOMER_VALIDATION,
    CUSTOMER_FIELD_MAPPING,
)


# LOAD CLASSIFICATION JSON
def _load_classification_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Classified JSON not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# GROUP BY DOC TYPE (based on customer_mapping.yml)
def _group_pages_by_doc_type_group(pages: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Map AI 'Doc Type' into logical groups defined in customer_mapping.yml
    """
    # Build reverse lookup: doc_type_label -> group_key
    name_to_group: Dict[str, str] = {}
    for group_key, cfg in CUSTOMER_DOC_TYPES.items():
        for label in cfg.get("names", []):
            name_to_group[label] = group_key

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for page in pages:
        raw_label = page.get("Doc Type", "").strip()
        group_key = name_to_group.get(raw_label, "other")
        grouped.setdefault(group_key, []).append(page)
    return grouped


# BUILD DATAFRAMES PER DOC TYPE GROUP
def _build_dataframes_by_group(grouped_pages: Dict[str, List[Dict[str, Any]]]) -> Dict[str, pd.DataFrame]:
    dfs: Dict[str, pd.DataFrame] = {}
    for group_key, pages in grouped_pages.items():
        if not pages:
            dfs[group_key] = pd.DataFrame()
        else:
            dfs[group_key] = pd.DataFrame(pages)
    return dfs


# VALUE PICKING HELPERS
def _first_non_empty(items: List[Any]) -> Optional[str]:
    for v in items:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _choose_best_latin(items: List[Any]) -> Optional[str]:
    """
    Prefer values containing Latin letters (English override when Arabic is present).
    """
    latin = [v for v in items if isinstance(v, str) and re.search(r'[A-Za-z]', v)]
    if latin:
        return latin[0].strip()
    return _first_non_empty(items)


def _choose_best_emirates_id(items: List[Any]) -> Optional[str]:
    """
    Prefer IDs with dashes (standard EID formatting)
    """
    with_dash = [v for v in items if isinstance(v, str) and "-" in v]
    if with_dash:
        return with_dash[0].strip()
    return _first_non_empty(items)


# EXTRACT FIELD FROM A SPECIFIC GROUP
def _extract_value_from_group(
    dfs_by_group: Dict[str, pd.DataFrame],
    doc_type_group: str,
    field_name: str,
) -> Optional[str]:

    df = dfs_by_group.get(doc_type_group)
    if df is None or df.empty:
        return None

    if field_name not in df.columns:
        return None

    return _first_non_empty(df[field_name].tolist())


# RESOLVE FIELD VALUE BASED ON MAPPING ENTRY
def _resolve_field_value(
    dfs_by_group: Dict[str, pd.DataFrame],
    mapping_entry: Dict[str, Any],
    db_column_name: str,
) -> Optional[str]:

    # constant → fixed value
    constant_value = mapping_entry.get("constant")
    if constant_value is not None:
        return constant_value

    sources = mapping_entry.get("sources", [])
    if not sources:
        return None

    # special cases
    if db_column_name == "EmiratesID":
        candidates = [
            _extract_value_from_group(dfs_by_group, src["doc_type_group"], src["field"])
            for src in sources
        ]
        return _choose_best_emirates_id(candidates)

    if db_column_name in ("FirstName", "LastName"):
        candidates = [
            _extract_value_from_group(dfs_by_group, src["doc_type_group"], src["field"])
            for src in sources
        ]
        return _choose_best_latin(candidates)

    # default: first non-empty of all sources combined
    for src in sources:
        v = _extract_value_from_group(dfs_by_group, src["doc_type_group"], src["field"])
        if isinstance(v, str) and v.strip():
            return v.strip()

    return None


# VALIDATION (PRINT MISSING FIELDS PER DOCUMENT)
def _validate_docs_per_group(dfs_by_group: Dict[str, pd.DataFrame]) -> None:
    for group_key, df in dfs_by_group.items():
        if df is None or df.empty:
            continue

        cfg = CUSTOMER_VALIDATION.get(group_key, {})
        required = cfg.get("required_fields", [])

        if not required:
            continue

        print(f"[INFO] Validating group: {group_key} (rows={len(df)})")

        for idx, row in df.iterrows():
            missing_fields: List[str] = []

            for field in required:
                val = row.get(field)
                if not isinstance(val, str) or not val.strip():
                    missing_fields.append(field)

            if missing_fields:
                print(f"  - Page={row.get('page')} Missing: {', '.join(missing_fields)}")


# MAIN ENTRY – RETURN DICT OF {COLUMN → VALUE} FOR DB UPDATE
def build_customer_updates_from_classification(
    classified_json_path: Path,
) -> Dict[str, Any]:

    # 1) Load classified JSON
    data = _load_classification_json(classified_json_path)
    pages = data.get("Pages", [])
    if not isinstance(pages, list):
        pages = []

    # 2) Group & df build
    grouped = _group_pages_by_doc_type_group(pages)
    dfs_by_group = _build_dataframes_by_group(grouped)

    # 3) Validate docs (log only)
    _validate_docs_per_group(dfs_by_group)

    # 4) Build result dict
    updates: Dict[str, Any] = {}

    for db_column, mapping_entry in CUSTOMER_FIELD_MAPPING.items():
        value = _resolve_field_value(
            dfs_by_group=dfs_by_group,
            mapping_entry=mapping_entry,
            db_column_name=db_column,
        )
        if value is not None and isinstance(value, str):
            value = value.strip()

        updates[db_column] = value

    # 5) Additional global warnings for essential fields
    essential_fields = [
        "FirstName", "LastName", "Gender", "Nationality",
        "EmiratesID", "EmiratesIDExpiryDate",
        "LicenseNumber", "LicenseExpiryDate",
        "Make", "Model", "YearOfManufacture",
        "ChassisNumber", "EngineNumber",
    ]
    missing = [f for f in essential_fields if not updates.get(f)]
    if missing:
        print("[WARN] Missing essential customer fields →")
        for f in missing:
            print(f"   - {f}")

    return updates
