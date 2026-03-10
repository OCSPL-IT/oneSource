# ACCOUNTS/services/receivables_parsers.py

from __future__ import annotations

from datetime import datetime, date
from typing import Optional, Any


def _parse_ui_date(s: Any) -> Optional[date]:
    """
    Parses UI date strings into date.
    Supports common formats:
      - dd-mm-yyyy
      - dd/mm/yyyy
      - yyyy-mm-dd
      - yyyy/mm/dd
    Returns date or None.
    """
    if not s:
        return None

    if isinstance(s, date):
        return s

    txt = str(s).strip()
    if not txt:
        return None

    fmts = [
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%Y/%m/%d",
    ]

    for f in fmts:
        try:
            return datetime.strptime(txt, f).date()
        except Exception:
            continue

    return None


def _parse_sql_display_date(s: Any) -> Optional[date]:
    """
    Parses ERP/SQL display strings into date.
    Supports:
      - '08 Dec 2025'
      - '08-Dec-2025'
      - '08/Dec/2025'
      - '2025-12-08'
      - '20251208'
      - '20231031' (yyyymmdd)
    Returns date or None.
    """
    if not s:
        return None

    if isinstance(s, date):
        return s

    txt = str(s).strip()
    if not txt:
        return None

    # yyyymmdd numeric
    if txt.isdigit() and len(txt) == 8:
        try:
            return datetime.strptime(txt, "%Y%m%d").date()
        except Exception:
            pass

    fmts = [
        "%d %b %Y",     # 08 Dec 2025
        "%d-%b-%Y",     # 08-Dec-2025
        "%d/%b/%Y",     # 08/Dec/2025
        "%Y-%m-%d",     # 2025-12-08
        "%Y/%m/%d",     # 2025/12/08
        "%d-%m-%Y",     # sometimes ERP sends this too
        "%d/%m/%Y",
    ]

    for f in fmts:
        try:
            return datetime.strptime(txt, f).date()
        except Exception:
            continue

    return None
