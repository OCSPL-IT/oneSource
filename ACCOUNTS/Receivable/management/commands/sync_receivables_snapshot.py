# ACCOUNTS/management/commands/sync_receivables_snapshot.py

import base64
from datetime import date, datetime, timedelta
from decimal import Decimal

from django.core.cache import cache
from django.core.management.base import BaseCommand
from django.db import connections, transaction
from django.utils import timezone

from ACCOUNTS.Receivable.models import ReceivableSnapshotRow
from ACCOUNTS.Receivable.services.receivables_service import (
    RECEIVABLES_SQL,
    _prepare_rows_inplace,
    _is_internal_transfer_party,
)

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------

def _pick_first(d, keys, default=""):
    for k in keys:
        if not k:
            continue
        if k in d:
            v = d.get(k)
            if v not in (None, "", "NA", "N/A"):
                return v
        lk = str(k).lower()
        if lk in d:
            v = d.get(lk)
            if v not in (None, "", "NA", "N/A"):
                return v
        uk = str(k).upper()
        if uk in d:
            v = d.get(uk)
            if v not in (None, "", "NA", "N/A"):
                return v
    return default


def _extract_customer_po_no(row: dict) -> str:
    return str(_pick_first(row, [
        "Customer PO No.", "Customer PO No", "CustomerPONo", "Customer PO Number",
        "customer_po_no", "cust_po_no", "po_no", "pono", "poNumber", "PONumber",
    ], default="") or "").strip()


def _extract_customer_po_date_display(row: dict) -> str:
    return str(_pick_first(row, [
        "Customer PO Date", "CustomerPODate",
        "customer_po_date", "cust_po_date", "po_date", "podate", "poDate",
    ], default="") or "").strip()


# -----------------------------------------------------------------------------
# ✅ Instrument / Cheque / Internet Number extractor (SAFE)
# -----------------------------------------------------------------------------
def _extract_instrument_no(row: dict) -> str:
    """
    Extract 'Instrument No' (Cheque/Instrument/Internet number) from ERP row.
    Works with the SQL column name: [Instrument No]
    Includes fallback keys to be safe across naming variations.
    """
    return str(_pick_first(row, [
        "Instrument No", "Instrument No.", "Instrument Number",
        "Cheque No", "Cheque No.", "Chq No", "Chq No.", "ChqNo",
        "internet number", "Internet Number", "Internet No", "Internet No.",
        "instrument_no", "cheque_no", "chq_no",
    ], default="") or "").strip()


# -----------------------------------------------------------------------------
# Optional progress reporter (SAFE no-op)
# -----------------------------------------------------------------------------
def _progress(run_id: str, *, status=None, percent=None, step=None, message=None, **extra):
    if not run_id:
        return
    try:
        from ACCOUNTS.Receivable.services.receivables_sync_state import set_state  # optional
        payload = {}
        if status is not None:
            payload["status"] = status
        if percent is not None:
            payload["percent"] = int(percent)
        if step is not None:
            payload["step"] = str(step)
        if message is not None:
            payload["message"] = str(message)
        payload.update(extra or {})
        set_state(run_id, **payload)
    except Exception:
        return


# -----------------------------------------------------------------------------
# Cache cleanup (safe): LocMemCache has no delete_pattern
# -----------------------------------------------------------------------------
def _safe_cache_delete_pattern(pattern: str):
    try:
        delete_pattern = getattr(cache, "delete_pattern", None)
        if callable(delete_pattern):
            delete_pattern(pattern)
            return

        # dev-only safety
        if cache.__class__.__name__ == "LocMemCache":
            cache.clear()
    except Exception:
        pass


# -----------------------------------------------------------------------------
# ✅ Snapshot retention cleanup (keep last N snapshots, NOT days)
# -----------------------------------------------------------------------------
def purge_old_snapshots_keep_last_n(keep_last: int = 2, *, using="default") -> int:
    """
    Keep only the most recent `keep_last` distinct snapshot_date values.
    Deletes ALL rows for snapshot_date values older than those kept.

    This matches requirement: "Keep only the last two snapshots".
    Runs AFTER successful insert to avoid data loss if sync fails.
    """
    try:
        keep_last = int(keep_last or 0)
    except Exception:
        keep_last = 0

    # 0 or negative => do nothing (safety)
    if keep_last <= 0:
        return 0

    # Get last N snapshot dates that exist in DB (distinct)
    keep_dates = list(
        ReceivableSnapshotRow.objects.using(using)
        .values_list("snapshot_date", flat=True)
        .distinct()
        .order_by("-snapshot_date")[:keep_last]
    )

    if not keep_dates:
        return 0

    qs = ReceivableSnapshotRow.objects.using(using).exclude(snapshot_date__in=keep_dates)
    deleted, _ = qs.delete()
    return deleted


# -----------------------------------------------------------------------------
# Optional schema detection (safe)
# -----------------------------------------------------------------------------
_MODEL_FIELD_CACHE = {}

def _model_has_field(field_name: str) -> bool:
    v = _MODEL_FIELD_CACHE.get(field_name)
    if v is not None:
        return v
    try:
        ReceivableSnapshotRow._meta.get_field(field_name)
        _MODEL_FIELD_CACHE[field_name] = True
        return True
    except Exception:
        _MODEL_FIELD_CACHE[field_name] = False
        return False


def _parse_any_date(v):
    if not v:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()

    s = str(v).strip()
    if not s:
        return None

    fmts = (
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d %b %Y",
        "%d-%b-%Y",
        "%d %B %Y",
        "%Y%m%d",
    )
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    return None


def _json_safe(obj):
    if obj is None:
        return None

    if isinstance(obj, Decimal):
        return str(obj)

    if isinstance(obj, (date, datetime)):
        return obj.isoformat()

    if isinstance(obj, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(obj)).decode("ascii")

    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            # drop internal prep keys
            if isinstance(k, str) and k.startswith("_"):
                continue
            out[str(k)] = _json_safe(v)
        return out

    if isinstance(obj, (list, tuple, set)):
        return [_json_safe(v) for v in obj]

    return obj


def _to_decimal(val, default=Decimal("0")):
    try:
        s = str(val or "").replace(",", "").strip()
        return Decimal(s) if s else default
    except Exception:
        return default


def _norm_key_str(s):
    return str(s or "").strip().upper()


def _norm_int_or_none(v):
    """
    Canonical normalization for ERP LID keys AND DB field value:
      - int if possible
      - None if blank
      - else UPPER stripped string (only used for dedupe key safety)
    """
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return _norm_key_str(s)


def _erp_lid_for_model(v):
    """
    BigIntegerField expects int/None. Never store text in erp_lid.
    """
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def _dedupe_rows(rows, snapshot_date):
    """
    Deduplicate rows to satisfy UNIQUE constraint:
      (snapshot_date, erp_lid, party_code, trans_no)
    """
    seen = set()
    out = []
    dropped = 0

    for r in rows:
        erp_lid_raw = r.get("lid") or r.get("lId") or r.get("lID")
        erp_lid_key = _norm_int_or_none(erp_lid_raw)

        party_code = _norm_key_str(r.get("Party Code"))
        trans_no = _norm_key_str(r.get("Trans No"))

        key = (snapshot_date, erp_lid_key, party_code, trans_no)
        if key in seen:
            dropped += 1
            continue

        seen.add(key)
        out.append(r)

    return out, dropped


class Command(BaseCommand):
    help = "Sync ERP receivables into Django DB snapshot table (default DB)."

    def add_arguments(self, parser):
        parser.add_argument(
            "--as-of",
            dest="as_of",
            default="",
            help="Snapshot date in YYYY-MM-DD (optional). Default: today.",
        )
        parser.add_argument(
            "--keep-internal",
            action="store_true",
            help="If set, internal parties will also be stored. Default: excluded.",
        )
        parser.add_argument(
            "--run-id",
            dest="run_id",
            default="",
            help="Optional run identifier for progress tracking (frontend).",
        )
        parser.add_argument(
            "--keep-snapshots",
            dest="keep_snapshots",
            default="2",
            help="How many snapshot dates to keep. Default: 2. Use 0 to disable purge.",
        )

    def handle(self, *args, **options):
        run_id = (options.get("run_id") or "").strip()
        as_of_str = (options.get("as_of") or "").strip()
        keep_internal = bool(options.get("keep_internal"))

        _progress(run_id, status="running", percent=2, step="starting", message="Starting snapshot sync...")

        if as_of_str:
            try:
                snapshot_date = datetime.strptime(as_of_str, "%Y-%m-%d").date()
            except ValueError:
                _progress(run_id, status="error", percent=100, step="error", message="Invalid --as-of. Use YYYY-MM-DD.")
                raise SystemExit("Invalid --as-of. Use YYYY-MM-DD.")
        else:
            snapshot_date = timezone.localdate()

        self.stdout.write(self.style.WARNING(f"Syncing receivables snapshot for: {snapshot_date}"))
        _progress(run_id, percent=8, step="fetching", message=f"Fetching rows from ERP for {snapshot_date}...")

        # 1) Pull from ERP (readonly_db)
        db_alias = "readonly_db"
        with connections[db_alias].cursor() as cursor:
            cursor.execute(RECEIVABLES_SQL)

            # Move to the final resultset
            while cursor.description is None:
                if not cursor.nextset():
                    self.stdout.write(self.style.ERROR("No resultset returned by SQL."))
                    _progress(run_id, status="error", percent=100, step="error", message="No resultset returned by SQL.")
                    return

            cols = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        data = [dict(zip(cols, r)) for r in rows]
        _progress(run_id, percent=20, step="preparing", message=f"Rows fetched: {len(data)}. Preparing rows...")

        # Prepare same as existing code (adds internal keys only)
        _prepare_rows_inplace(data)

        if not keep_internal:
            before = len(data)
            data = [r for r in data if not _is_internal_transfer_party(r)]
            _progress(run_id, percent=28, step="filtering", message=f"Filtered internal parties. {before} → {len(data)}")

        self.stdout.write(self.style.WARNING(f"Rows fetched after filtering: {len(data)}"))

        # 1.1) Deduplicate to satisfy unique constraint
        _progress(run_id, percent=35, step="dedup", message="Deduplicating rows...")
        data, dropped = _dedupe_rows(data, snapshot_date)
        if dropped:
            self.stdout.write(self.style.WARNING(f"Duplicate rows dropped before insert: {dropped}"))
        self.stdout.write(self.style.WARNING(f"Rows after de-dup: {len(data)}"))

        _progress(
            run_id,
            percent=45,
            step="dedup",
            message=f"De-dup complete. Rows: {len(data)} (dropped {dropped})",
            deduped_rows=len(data),
            dropped_rows=dropped,
        )

        # Detect optional fields once
        has_trans_date = _model_has_field("trans_date")
        has_due_date = _model_has_field("due_date")
        has_overdue_date = _model_has_field("overdue_date")
        has_po_no = _model_has_field("customer_po_no")
        has_po_date_display = _model_has_field("customer_po_date_display")
        has_po_date = _model_has_field("customer_po_date")
        has_instrument_no = _model_has_field("instrument_no")

        _progress(run_id, percent=55, step="building", message="Building snapshot objects...")

        objs = []
        for r in data:
            trans_dt = _parse_any_date(r.get("Trans Date") or "") if has_trans_date else None
            due_dt = _parse_any_date(r.get("Due Date") or r.get("DueDate") or "") if has_due_date else None
            overdue_dt = _parse_any_date(r.get("Overdue Date") or "") if has_overdue_date else None

            po_no = _extract_customer_po_no(r)
            po_date_display = _extract_customer_po_date_display(r)
            po_dt = _parse_any_date(po_date_display) if po_date_display else None

            inst_no = _extract_instrument_no(r) if has_instrument_no else ""

            erp_lid_raw = r.get("lid") or r.get("lId") or r.get("lID")
            erp_lid_val = _erp_lid_for_model(erp_lid_raw)

            obj_kwargs = dict(
                snapshot_date=snapshot_date,

                erp_lid=erp_lid_val,
                erp_acc_id=r.get("lAccId"),
                erp_comp_id=r.get("lCompId"),
                erp_typ_id=r.get("lTypId"),

                company_name=str(r.get("Company Name") or "").strip(),
                party_code=str(r.get("Party Code") or "").strip(),
                party_name=str(r.get("Party Name") or "").strip(),

                trans_type=str(r.get("Trans Type") or "").strip(),
                trans_no=str(r.get("Trans No") or "").strip(),

                trans_date_display=str(r.get("Trans Date") or "").strip(),
                due_date_display=str(r.get("Due Date") or r.get("DueDate") or "").strip(),
                overdue_date_display=str(r.get("Overdue Date") or "").strip(),

                bill_amt=_to_decimal(r.get("Bill Amt") or 0),
                paid_amt=_to_decimal(r.get("Paid Amt") or 0),
                outstanding_amt=_to_decimal(r.get("Outstanding Amt") or 0),

                item_name=str(r.get("Item Name") or "").strip(),
                location=str(r.get("Location") or "").strip(),

                raw=_json_safe(r),
            )

            if has_trans_date:
                obj_kwargs["trans_date"] = trans_dt
            if has_due_date:
                obj_kwargs["due_date"] = due_dt
            if has_overdue_date:
                obj_kwargs["overdue_date"] = overdue_dt

            if has_po_no:
                obj_kwargs["customer_po_no"] = po_no
            if has_po_date_display:
                obj_kwargs["customer_po_date_display"] = po_date_display
            if has_po_date:
                obj_kwargs["customer_po_date"] = po_dt

            if has_instrument_no:
                obj_kwargs["instrument_no"] = inst_no

            objs.append(ReceivableSnapshotRow(**obj_kwargs))

        _progress(run_id, percent=65, step="validating", message="Validating duplicates before insert...")

        check_seen = set()
        dup_samples = []
        for o in objs:
            k = (
                o.snapshot_date,
                _norm_int_or_none(o.erp_lid),
                _norm_key_str(o.party_code),
                _norm_key_str(o.trans_no),
            )
            if k in check_seen:
                if len(dup_samples) < 5:
                    dup_samples.append(k)
            else:
                check_seen.add(k)

        if dup_samples:
            self.stdout.write(self.style.ERROR(f"Still duplicate keys in objs (sample): {dup_samples[:5]}"))
            _progress(run_id, status="error", percent=100, step="error", message="Duplicate keys still present before insert.")
            raise SystemExit("Duplicate keys still present in objs before insert.")

        _progress(run_id, percent=70, step="inserting", message="Deleting existing snapshot rows...")

        batch_size = 2000
        total = len(objs)
        inserted = 0

        # ✅ ensure delete + insert uses same DB alias and is atomic
        with transaction.atomic(using="default"):
            ReceivableSnapshotRow.objects.using("default").filter(snapshot_date=snapshot_date).delete()

            for i in range(0, total, batch_size):
                chunk = objs[i:i + batch_size]
                ReceivableSnapshotRow.objects.using("default").bulk_create(chunk, batch_size=batch_size)
                inserted += len(chunk)

                pct = 70 + int((inserted / max(total, 1)) * 25)
                _progress(
                    run_id,
                    percent=min(pct, 95),
                    step="inserting",
                    message=f"Inserting... {inserted}/{total}",
                    inserted_rows=inserted,
                    total_rows=total,
                )

        # -----------------------
        # ✅ Retention cleanup: keep last N snapshots (after successful insert)
        # -----------------------
        keep_snapshots = (options.get("keep_snapshots") or "2")
        _progress(run_id, percent=95, step="purging", message=f"Purging old snapshots. Keeping last {keep_snapshots} snapshots...")
        try:
            deleted_count = purge_old_snapshots_keep_last_n(keep_last=keep_snapshots, using="default")
            _progress(
                run_id,
                percent=95,
                step="purging",
                message=f"Purge complete. Deleted rows: {deleted_count}",
                deleted_rows=deleted_count,
            )
        except Exception:
            # Keep sync successful even if purge fails
            _progress(run_id, percent=95, step="purging", message="Purge failed (ignored).")

        _progress(run_id, percent=96, step="clearing_cache", message="Clearing caches...")

        _safe_cache_delete_pattern("receivables_snapshot_rows::*")
        _safe_cache_delete_pattern("receivables_raw_*")
        cache.delete("receivables_raw_all_v2")

        self.stdout.write(self.style.SUCCESS(f"Snapshot sync complete: {len(objs)} rows stored (deduped)."))
        _progress(run_id, status="done", percent=100, step="done", message=f"Snapshot sync complete: {len(objs)} rows stored.")
