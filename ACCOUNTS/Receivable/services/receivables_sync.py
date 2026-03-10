# ACCOUNTS/services/receivables_sync.py
import os
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from django.conf import settings
from django.db import connections, transaction
from django.db.models import Max
from django.utils import timezone

from ACCOUNTS.Receivable.models import ReceivableSnapshotRow

log = logging.getLogger(__name__)

BASE_FROM_DATE = date(2025, 4, 1)  # FY start


# -----------------------------
# Small helpers
# -----------------------------
def _env_first(*names, default=None):
    for n in names:
        v = os.environ.get(n)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


def _detect_erp_alias() -> str:
    """
    Prefer explicit env alias. Else try common aliases in this project.
    Falls back to any non-default DB, else 'default' (not ideal, but safe).
    """
    env_alias = _env_first("RECEIVABLES_ERP_DB_ALIAS", "ERP_DB_ALIAS", default=None)
    if env_alias and env_alias in connections.databases:
        return env_alias

    # common names used in your codebase
    for cand in ("readonly_db", "readonly"):
        if cand in connections.databases:
            return cand

    # pick first non-default if any
    for a in connections.databases.keys():
        if a != "default":
            return a

    return "default"


def _parse_date_any(v):
    if not v:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()

    s = str(v).strip()
    if not s:
        return None

    # common ERP formats
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d %b %Y", "%d-%b-%Y", "%d %B %Y", "%Y%m%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def _row_effective_date(r: dict):
    """
    Date used for range filtering during snapshot:
    Prefer Trans Date; else Due Date; else Overdue Date.
    """
    d = _parse_date_any(r.get("Trans Date") or r.get("TrnDate"))
    if d:
        return d
    d = _parse_date_any(r.get("Due Date") or r.get("DueDate"))
    if d:
        return d
    d = _parse_date_any(r.get("Overdue Date") or r.get("OverdueDate"))
    return d


def _first_resultset(cursor) -> bool:
    """
    SQL Server may emit rowcount/empty resultsets before actual tabular resultset.
    Advance cursor to the first resultset that has columns (cursor.description).
    """
    while cursor.description is None:
        has_next = cursor.nextset()
        if not has_next:
            return False
    return True


def _dictfetchall(cursor) -> List[Dict[str, Any]]:
    cols = [c[0] for c in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]


def _json_safe(v: Any):
    """
    Ensure values in raw dict are JSON-serializable.
    JSONField will choke on date/datetime/Decimal/bytes in many DB backends.
    """
    if v is None:
        return None
    if isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, Decimal):
        # keep numeric meaning without float rounding surprises
        return str(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray, memoryview)):
        # rarely expected here; store as repr to avoid crash
        return v.hex()
    return str(v)


def _sanitize_row(r: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _json_safe(v) for k, v in (r or {}).items()}


def latest_snapshot_date(default=None):
    """
    Latest snapshot_date available in ReceivableSnapshotRow.
    """
    d = ReceivableSnapshotRow.objects.aggregate(mx=Max("snapshot_date")).get("mx")
    return d or default


# -----------------------------
# ERP fetch
# -----------------------------
def fetch_erp_receivables_rows(from_date: date, to_date: date) -> List[Dict[str, Any]]:
    """
    Fetch ERP receivables rows from ERP DB (readonly) via:
      1) Stored Procedure (if configured), else
      2) Your RECEIVABLES_SQL from ACCOUNTS/services.py

    Output must match keys used by dashboard:
      Party Code, Party Name, Trans No, Trans Date, Due Date, Outstanding Amt, etc.
    """
    erp_alias = _detect_erp_alias()

    # (A) Prefer SP if configured
    sp_name = (
        _env_first("RECEIVABLES_ERP_SP", "ERP_RECEIVABLES_SP", "RECEIVABLES_SP", default=None)
        or getattr(settings, "RECEIVABLES_ERP_SP", None)
    )

    if sp_name:
        sql = f"EXEC {sp_name} ?, ?"
        with connections[erp_alias].cursor() as cursor:
            cursor.execute(sql, [from_date, to_date])
            if not _first_resultset(cursor):
                return []
            return _dictfetchall(cursor)

    # (B) Fallback to RECEIVABLES_SQL (your existing query)
    # IMPORTANT: do NOT import receivables_dashboard here (circular).
    from ACCOUNTS.Receivable.services import RECEIVABLES_SQL  # noqa: WPS433

    with connections[erp_alias].cursor() as cursor:
        cursor.execute(RECEIVABLES_SQL)

        # Advance to first real resultset
        if not _first_resultset(cursor):
            return []

        rows = _dictfetchall(cursor)

    # Since RECEIVABLES_SQL doesn’t accept range params, filter rows here
    out = []
    for r in rows:
        eff = _row_effective_date(r)
        if eff is None:
            continue
        if eff < from_date or eff > to_date:
            continue
        out.append(r)

    return out


# -----------------------------
# Sync: ERP -> Django snapshot table
# -----------------------------
@transaction.atomic
def sync_receivables_snapshot(
    *,
    as_of_date: Optional[date] = None,
    snapshot_date: Optional[date] = None,
    replace: bool = True,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
) -> int:
    """
    Pull ERP receivables rows for a date range and store into ReceivableSnapshotRow.

    Defaults:
      from_date     = 01-04-2025
      to_date       = as_of_date (today if not provided)
      snapshot_date = today if not provided
      replace       = delete existing rows for snapshot_date before insert
    """
    as_of = as_of_date or timezone.localdate()
    snap = snapshot_date or timezone.localdate()

    start = from_date or BASE_FROM_DATE
    end = to_date or as_of
    if start > end:
        start, end = end, start

    # Fetch ERP rows
    erp_rows = fetch_erp_receivables_rows(start, end)

    if replace:
        ReceivableSnapshotRow.objects.filter(snapshot_date=snap).delete()

    # Determine which fields exist on your model (schema-safe)
    concrete_fields = {
        f.name
        for f in ReceivableSnapshotRow._meta.get_fields()
        if getattr(f, "concrete", False) and not getattr(f, "many_to_many", False)
    }

    objs = []
    for r in erp_rows:
        raw = _sanitize_row(r)

        # Common ERP lid
        erp_lid = r.get("lid") or r.get("Lid") or r.get("lId") or r.get("lID") or 0
        try:
            erp_lid = int(erp_lid or 0)
        except Exception:
            erp_lid = 0

        kwargs = {}
        if "snapshot_date" in concrete_fields:
            kwargs["snapshot_date"] = snap

        # optional fields (only if your model has them)
        if "as_of_date" in concrete_fields:
            kwargs["as_of_date"] = as_of
        if "erp_lid" in concrete_fields:
            kwargs["erp_lid"] = erp_lid

        # raw JSON payload (expected field name: raw)
        if "raw" in concrete_fields:
            kwargs["raw"] = raw
        else:
            # If your model uses a different JSONField name, update here.
            raise RuntimeError("ReceivableSnapshotRow model must have a JSONField named 'raw'.")

        objs.append(ReceivableSnapshotRow(**kwargs))

    # Bulk insert
    if objs:
        ReceivableSnapshotRow.objects.bulk_create(objs, batch_size=2000)

    return len(objs)
