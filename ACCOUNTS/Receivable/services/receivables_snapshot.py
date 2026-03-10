# ACCOUNTS/services/receivables_snapshot.py

from datetime import datetime, date as date_cls
from decimal import Decimal
import base64

from django.core.cache import cache
from django.db import connections, transaction
from django.db.models import Max
from django.utils import timezone

from ACCOUNTS.Receivable.models import ReceivableSnapshotRow


# -----------------------------
# Date parsing helpers (local, minimal)
# -----------------------------
def _parse_ui_date(s):
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def _parse_sql_display_date(s):
    if not s:
        return None
    s = str(s).strip()
    if not s:
        return None
    formats = (
        "%d %b %Y",    # 08 Dec 2025
        "%d-%b-%Y",    # 08-Dec-2025
        "%d %B %Y",    # 08 December 2025
        "%d/%m/%Y",    # 08/12/2025
        "%Y%m%d",      # 20251208
        "%Y-%m-%d",    # 2025-12-08
    )
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def _parse_any_date(v):
    if not v:
        return None
    if isinstance(v, date_cls) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    s = str(v).strip()
    return _parse_ui_date(s) or _parse_sql_display_date(s)


# -----------------------------
# Snapshot selection
# -----------------------------
def latest_snapshot_date():
    """
    Returns the latest snapshot_date present in ReceivableSnapshotRow, or None.
    Cached briefly to avoid repeated MAX() under load.
    """
    ck = "receivables_snapshot_latest_date::v1"
    cached = cache.get(ck)
    # NOTE: cache.get returns None for both "missing" and "stored None".
    # We accept that; if there are no rows, we'll hit DB again.
    if cached is not None:
        return cached

    mx = ReceivableSnapshotRow.objects.aggregate(mx=Max("snapshot_date")).get("mx")
    cache.set(ck, mx, 60)
    return mx


def _pick_snapshot_date(as_of_date=None, snapshot_date=None):
    """
    Choose which snapshot_date to read.

    Priority:
      1) snapshot_date (explicit) if exists
      2) as_of_date if exists
      3) latest available snapshot_date
    """
    if not snapshot_date and not as_of_date:
        return latest_snapshot_date()

    candidates = []
    if snapshot_date:
        candidates.append(snapshot_date)
    if as_of_date and as_of_date != snapshot_date:
        candidates.append(as_of_date)

    if candidates:
        found = set(
            ReceivableSnapshotRow.objects
            .filter(snapshot_date__in=candidates)
            .values_list("snapshot_date", flat=True)
            .distinct()
        )
        if snapshot_date and snapshot_date in found:
            return snapshot_date
        if as_of_date and as_of_date in found:
            return as_of_date

    return latest_snapshot_date()


# -----------------------------
# Row builder
# -----------------------------
def _row_to_dict(obj):
    """
    Return raw dict (ERP-shaped) while ensuring cached date keys exist.
    Works even if 'raw' is empty by falling back to column-derived minimal dict.
    """
    raw = {}
    try:
        raw = dict(obj.raw or {})
    except Exception:
        raw = {}

    if not raw:
        raw = {
            "Company Name": getattr(obj, "company_name", "") or "",
            "Party Code": getattr(obj, "party_code", "") or "",
            "Party Name": getattr(obj, "party_name", "") or "",
            "Trans Type": getattr(obj, "trans_type", "") or "",
            "Trans No": getattr(obj, "trans_no", "") or "",
            "Trans Date": getattr(obj, "trans_date_display", "") or "",
            "Due Date": getattr(obj, "due_date_display", "") or "",
            "Overdue Date": getattr(obj, "overdue_date_display", "") or "",
            "Bill Amt": getattr(obj, "bill_amt", 0) or 0,
            "Paid Amt": getattr(obj, "paid_amt", 0) or 0,
            "Outstanding Amt": getattr(obj, "outstanding_amt", 0) or 0,
            "Item Name": getattr(obj, "item_name", "") or "",
            "Location": getattr(obj, "location", "") or "",
        }

    # Add cached parsed dates for speed (do not change visible keys)
    trans_dt = getattr(obj, "trans_date", None)
    due_dt = getattr(obj, "due_date", None)
    overdue_dt = getattr(obj, "overdue_date", None)

    if trans_dt is None:
        trans_dt = _parse_any_date(raw.get("Trans Date") or "")
    if due_dt is None:
        due_dt = _parse_any_date(raw.get("Due Date") or raw.get("DueDate") or "")
    if overdue_dt is None:
        overdue_dt = _parse_any_date(raw.get("Overdue Date") or "")

    raw["_trans_dt"] = trans_dt
    raw["_due_dt"] = due_dt
    raw["_overdue_dt"] = overdue_dt

    return raw


# -----------------------------
# Snapshot fetch API (existing flow)
# -----------------------------
def fetch_receivables_raw_from_snapshot(*, as_of_date=None, snapshot_date=None, include_all=True):
    """
    Returns list[dict] identical (as much as possible) to ERP SQL output.

    - Reads from ReceivableSnapshotRow in DEFAULT Django DB.
    - include_all=True  -> return all rows for that snapshot date
    - include_all=False -> return only rows where Outstanding Amt != 0 (OPEN rows)
    """
    snap_date = _pick_snapshot_date(as_of_date=as_of_date, snapshot_date=snapshot_date)
    if not snap_date:
        return []

    cache_key = f"receivables_snapshot_rows::{snap_date.isoformat()}::all={int(bool(include_all))}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    qs = ReceivableSnapshotRow.objects.filter(snapshot_date=snap_date)

    if not include_all:
        qs = qs.exclude(outstanding_amt=0)

    qs = qs.only(
        "raw",
        "company_name", "party_code", "party_name",
        "trans_type", "trans_no",
        "trans_date_display", "due_date_display", "overdue_date_display",
        "bill_amt", "paid_amt", "outstanding_amt",
        "item_name", "location",
    )

    data = []
    for obj in qs.iterator(chunk_size=5000):
        data.append(_row_to_dict(obj))

    cache.set(cache_key, data, 900)
    return data


# =============================================================================
# NEW: Sync snapshot service (to fix your ImportError + enable frontend sync)
# =============================================================================

def _to_decimal(val, default=Decimal("0")):
    try:
        s = str(val or "").replace(",", "").strip()
        return Decimal(s) if s else default
    except Exception:
        return default


def _json_safe(obj):
    """
    Convert Python objects into JSON-serializable types.
    - Decimal -> str (preserves precision)
    - date/datetime -> ISO string
    - bytes -> base64 string
    - dict/list/tuple/set -> recursive
    """
    if obj is None:
        return None

    if isinstance(obj, Decimal):
        return str(obj)

    if isinstance(obj, (date_cls, datetime)):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)

    if isinstance(obj, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(obj)).decode("ascii")

    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            if isinstance(k, str) and k.startswith("_"):
                continue
            out[str(k)] = _json_safe(v)
        return out

    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]

    return obj


def _invalidate_snapshot_cache(snapshot_date):
    """
    LocMemCache does NOT support delete_pattern, so we delete exact keys.
    """
    if not snapshot_date:
        return
    # latest date cache
    cache.delete("receivables_snapshot_latest_date::v1")
    # row caches for this date
    cache.delete(f"receivables_snapshot_rows::{snapshot_date.isoformat()}::all=1")
    cache.delete(f"receivables_snapshot_rows::{snapshot_date.isoformat()}::all=0")


def _dedup_by_model_unique_key(rows, snapshot_date):
    """
    Match your DB UniqueConstraint:
      (snapshot_date, erp_lid, party_code, trans_no)
    """
    seen = set()
    out = []
    dropped = 0

    for r in rows:
        erp_lid = r.get("lid") or r.get("lId") or r.get("lID")
        party_code = (r.get("Party Code") or "").strip().upper()
        trans_no = (r.get("Trans No") or "").strip().upper()

        k = (snapshot_date, erp_lid or None, party_code, trans_no)
        if k in seen:
            dropped += 1
            continue
        seen.add(k)
        out.append(r)

    return out, dropped


def sync_receivables_snapshot(*, snapshot_date=None, keep_internal=False, stdout=None):
    """
    Sync ERP receivables into ReceivableSnapshotRow (default DB).

    - snapshot_date: date object or None (defaults to today localdate)
    - keep_internal: bool, default False (exclude internal parties)
    - stdout: optional file-like (e.g. management command self.stdout)

    Returns dict with counts.
    """
    # Import inside to avoid circular imports / startup failures
    from ACCOUNTS.Receivable.services.receivables_service import (
        RECEIVABLES_SQL,
        _prepare_rows_inplace,
        _is_internal_transfer_party,
    )

    def log(msg):
        if stdout:
            try:
                stdout.write(msg)
            except Exception:
                pass

    snapshot_date = snapshot_date or timezone.localdate()

    lock_key = f"receivables_snapshot_sync_lock::{snapshot_date.isoformat()}"
    # 30 min lock to avoid double click on frontend
    if not cache.add(lock_key, "1", timeout=60 * 30):
        return {"ok": False, "error": "Sync already running for this date."}

    try:
        log(f"Syncing receivables snapshot for: {snapshot_date}")

        db_alias = "readonly_db"
        with connections[db_alias].cursor() as cursor:
            cursor.execute(RECEIVABLES_SQL)

            while cursor.description is None:
                if not cursor.nextset():
                    return {"ok": False, "error": "No resultset returned by SQL."}

            cols = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        data = [dict(zip(cols, r)) for r in rows]
        _prepare_rows_inplace(data)

        if not keep_internal:
            data = [r for r in data if not _is_internal_transfer_party(r)]

        fetched = len(data)
        log(f"Rows fetched after filtering: {fetched}")

        data, dropped = _dedup_by_model_unique_key(data, snapshot_date)
        log(f"Duplicate rows dropped before insert: {dropped}")
        log(f"Rows after de-dup: {len(data)}")

        objs = []
        for r in data:
            objs.append(
                ReceivableSnapshotRow(
                    snapshot_date=snapshot_date,
                    erp_lid=r.get("lid") or r.get("lId") or r.get("lID"),
                    erp_acc_id=r.get("lAccId"),
                    erp_comp_id=r.get("lCompId"),
                    erp_typ_id=r.get("lTypId"),
                    company_name=(r.get("Company Name") or ""),
                    party_code=(r.get("Party Code") or ""),
                    party_name=(r.get("Party Name") or ""),
                    trans_type=(r.get("Trans Type") or ""),
                    trans_no=(r.get("Trans No") or ""),
                    trans_date_display=(r.get("Trans Date") or ""),
                    due_date_display=(r.get("Due Date") or r.get("DueDate") or ""),
                    overdue_date_display=(r.get("Overdue Date") or ""),
                    bill_amt=_to_decimal(r.get("Bill Amt") or 0),
                    paid_amt=_to_decimal(r.get("Paid Amt") or 0),
                    outstanding_amt=_to_decimal(r.get("Outstanding Amt") or 0),
                    item_name=(r.get("Item Name") or ""),
                    location=(r.get("Location") or ""),
                    raw=_json_safe(r),
                )
            )

        with transaction.atomic(using="default"):
            ReceivableSnapshotRow.objects.filter(snapshot_date=snapshot_date).delete()
            ReceivableSnapshotRow.objects.bulk_create(objs, batch_size=2000)

        _invalidate_snapshot_cache(snapshot_date)

        log(f"Snapshot sync complete: {len(objs)} rows stored (deduped).")

        return {
            "ok": True,
            "snapshot_date": snapshot_date,
            "fetched": fetched,
            "dropped_duplicates": dropped,
            "stored": len(objs),
        }

    finally:
        cache.delete(lock_key)


__all__ = [
    "latest_snapshot_date",
    "fetch_receivables_raw_from_snapshot",
    "sync_receivables_snapshot",
]
