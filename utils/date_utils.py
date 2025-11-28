# utils/date_utils.py

from __future__ import annotations

from datetime import datetime, date
from typing import Any, Optional, Iterable

# Common date formats we expect from AI/Docs:
DEFAULT_DATE_FORMATS: Iterable[str] = (
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%Y-%m-%d",
    "%d/%m/%y",
    "%d-%m-%y",
)


def normalize_date_for_sql(
    value: Any,
    formats: Iterable[str] = DEFAULT_DATE_FORMATS,
) -> Optional[str]:
    """
    Convert common date string formats (DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD, etc.)
    into a safe ISO string 'YYYY-MM-DD' for SQL Server.

    If parsing fails, return None → will send NULL to SQL, avoiding conversion errors
    AND avoiding ODBC 'optional feature not implemented' from binding raw date objects.
    """
    if value is None:
        return None

    # Already a datetime/date → convert to ISO string
    if isinstance(value, datetime):
        return value.date().strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    s = str(value).strip()
    if not s:
        return None

    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date().strftime("%Y-%m-%d")
        except ValueError:
            continue

    # If we reach here, parsing failed
    print(f"[WARN] normalize_date_for_sql: could not parse date value '{s}', sending NULL to SQL.")
    return None
