# ACCOUNTS/services/receivables_dashboard.py
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal

from django.core.cache import cache
from django.utils import timezone

from ACCOUNTS.Receivable.models import ReceivableSnapshotRow
from ACCOUNTS.Receivable.services.receivables_sync import latest_snapshot_date

# Canonical import (prevents ImportError issues)
from ACCOUNTS.Receivable.services.company_groups import COMPANY_GROUPS, get_company_group

# -----------------------------
# Performance knobs (safe defaults)
# -----------------------------
MAX_DETAIL_ROWS = 2000           # prevent template rendering 60k rows
CACHE_SECONDS = 300              # cache computed dashboard context
ITER_CHUNK_SIZE = 10000          # faster iteration on MSSQL

BASE_START_DATE = date(2025, 4, 1)


# -----------------------------
# Generic helpers
# -----------------------------
def _to_decimal(val, default=Decimal("0")):
    try:
        s = str(val or "").replace(",", "").strip()
        return Decimal(s) if s else default
    except Exception:
        return default


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
        "%d-%b-%Y",    # 05-Sep-2025 (your PO Date format)
        "%d %b %Y",    # 05 Sep 2025 (variant)
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
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    s = str(v).strip()
    return _parse_ui_date(s) or _parse_sql_display_date(s)


def _to_date_obj(v):
    return _parse_any_date(v)


def _norm(s):
    return (str(s or "").strip().lower())


def _bill_key(party_code, invoice_no):
    return f"{(party_code or '').strip()}||{(invoice_no or '').strip()}"


def _pick_first(d, keys, default=""):
    if not isinstance(d, dict):
        return default
    for k in keys:
        v = d.get(k)
        if v not in (None, "", "NA", "N/A"):
            return v
    return default


# -----------------------------
# Company group filter
# -----------------------------
def _apply_company_group_filter(qs, company_group: str):
    g = (company_group or "").strip().upper()
    if not g or g == "ALL":
        return qs

    if g == "OTHER":
        known = set()
        for names in COMPANY_GROUPS.values():
            known.update(names)
        return qs.exclude(company_name__in=list(known))

    names = COMPANY_GROUPS.get(g) or []
    if not names:
        return qs
    return qs.filter(company_name__in=names)


# -----------------------------
# Snapshot helpers
# -----------------------------
_LATEST_SNAP_CACHE_KEY = "rcv_latest_snapshot_date_v1"

def _latest_snapshot_date_cached():
    snap = cache.get(_LATEST_SNAP_CACHE_KEY)
    if snap is not None:
        return snap
    snap = latest_snapshot_date()
    cache.set(_LATEST_SNAP_CACHE_KEY, snap, 60)  # 60s is enough and safe
    return snap

def _snapshot_date_or_latest(snapshot_date=None):
    return snapshot_date or _latest_snapshot_date_cached()


# def _snapshot_date_or_latest(snapshot_date=None):
#     return snapshot_date or latest_snapshot_date()


def _snapshot_values_qs(snapshot_date=None, *, include_raw=False, company_group=None):
    snap = _snapshot_date_or_latest(snapshot_date)
    if not snap:
        return ReceivableSnapshotRow.objects.none()

    fields = [
        "id",  # ✅ ADD THIS
        "snapshot_date",
        "company_name", "party_code", "party_name",
        "trans_type", "trans_no",
        "trans_date_display", "due_date_display", "overdue_date_display",
        "trans_date", "due_date", "overdue_date",
        "bill_amt", "paid_amt", "outstanding_amt",
        "item_name", "location",
        "erp_lid", "erp_acc_id", "erp_comp_id", "erp_typ_id",
    ]
    if include_raw:
        fields.append("raw")

    qs = ReceivableSnapshotRow.objects.filter(snapshot_date=snap)
    qs = _apply_company_group_filter(qs, company_group)
    return qs.values(*fields)

# -----------------------------
# PO extraction (FIXED for your raw keys)
# -----------------------------
_PO_NO_KEYS = ("Customer PO No.", "Customer PO No")
_PO_DATE_KEYS = ("Customer PO Date", "Customer PO Date.")

def _extract_customer_po(raw: dict):
    """
    Returns: (po_no: str, po_date_display: str, po_date_obj: date|None)

    Your data uses title-case keys:
      - 'Customer PO No.'
      - 'Customer PO Date'
    """
    if not isinstance(raw, dict):
        return "", "", None

    po_no = _pick_first(raw, _PO_NO_KEYS, default="")
    po_dt_disp = _pick_first(raw, _PO_DATE_KEYS, default="")

    po_dt_obj = _parse_any_date(po_dt_disp)  # handles '05-Sep-2025' etc.
    return (str(po_no).strip(), str(po_dt_disp).strip(), po_dt_obj)


def _values_row_to_dict(v):
    """
    Convert values() row into ERP-shaped dict used by dashboard logic.
    Also attaches PO fields (customer_po_no/customer_po_date) from raw when available.
    """
    r = {
        "Company Name": v.get("company_name") or "",
        "Party Code": v.get("party_code") or "",
        "Party Name": v.get("party_name") or "",
        "Trans Type": v.get("trans_type") or "",
        "Trans No": v.get("trans_no") or "",
        "Trans Date": v.get("trans_date_display") or "",
        "Due Date": v.get("due_date_display") or "",
        "Overdue Date": v.get("overdue_date_display") or "",
        "Bill Amt": v.get("bill_amt") or 0,
        "Paid Amt": v.get("paid_amt") or 0,
        "Outstanding Amt": v.get("outstanding_amt") or 0,
        "Item Name": v.get("item_name") or "",
        "Location": v.get("location") or "",
        "lid": v.get("erp_lid"),
        "lAccId": v.get("erp_acc_id"),
        "lCompId": v.get("erp_comp_id"),
        "lTypId": v.get("erp_typ_id"),
    }

    # Parsed date caches (prefer real DateField; fallback to display parsing)
    r["_trans_dt"] = v.get("trans_date") or _parse_any_date(r.get("Trans Date"))
    r["_due_dt"] = v.get("due_date") or _parse_any_date(r.get("Due Date"))
    r["_overdue_dt"] = v.get("overdue_date") or _parse_any_date(r.get("Overdue Date"))

    raw = {}
    if "raw" in v:
        raw = v.get("raw") or {}
        r["raw"] = raw

    
    # ✅ Add normalized aliases for templates
    r["customer_po_no"] = _pick_first(raw, ("Customer PO No.", "Customer PO No"), default="")
    r["customer_po_date"] = _pick_first(raw, ("Customer PO Date", "Customer PO Date."), default="")

    # Attach PO fields for downstream use (template + detail_rows)
    po_no, po_date_disp, po_date_obj = _extract_customer_po(raw)
    r["customer_po_no"] = po_no
    r["customer_po_date"] = po_date_disp
    r["_customer_po_date_obj"] = po_date_obj  # optional internal use

    return r


def fetch_receivables_raw(snapshot_date=None, company_group=None):
    """
    OPEN only (Outstanding != 0) from snapshot.
    NOTE: include_raw=True so PO is available if needed downstream.
    """
    qs = _snapshot_values_qs(snapshot_date, include_raw=True, company_group=company_group).exclude(outstanding_amt=0)

    rows = []
    for v in qs.iterator(chunk_size=ITER_CHUNK_SIZE):
        r = _values_row_to_dict(v)
        os_amt = _to_decimal(r.get("Outstanding Amt") or 0)
        if abs(os_amt) <= Decimal("0.0001"):
            continue
        rows.append(r)
    return rows


def fetch_receivables_raw_all(snapshot_date=None, company_group=None):
    """
    ALL rows including adjusted (Outstanding may be 0).
    NOTE: include_raw=True so PO is available if needed downstream.
    """
    qs = _snapshot_values_qs(snapshot_date, include_raw=True, company_group=company_group)

    rows = []
    for v in qs.iterator(chunk_size=ITER_CHUNK_SIZE):
        rows.append(_values_row_to_dict(v))
    return rows


# -----------------------------
# Filters
# -----------------------------
def _compute_overdue_days(row, as_of_date: date):
    dd = row.get("_overdue_dt") or row.get("_due_dt") or row.get("_trans_dt")
    if not dd:
        dd = (
            _parse_any_date(row.get("Overdue Date"))
            or _parse_any_date(row.get("Due Date"))
            or _parse_any_date(row.get("Trans Date"))
        )
    if not dd:
        return 0
    return max((as_of_date - dd).days, 0)


def _passes_overdue_aging_filters(r, *, aging="", overdue="", as_of_date=None):
    as_of_date = as_of_date or timezone.localdate()
    od = _compute_overdue_days(r, as_of_date)

    overdue_norm = str(overdue or "").strip().lower()
    aging_norm = str(aging or "").strip()

    if overdue_norm == "overdue" and od <= 0:
        return False, od
    if overdue_norm == "not_overdue" and od > 0:
        return False, od

    if aging_norm and aging_norm.upper() not in ("ALL",):
        a = aging_norm
        if a.startswith(">"):
            try:
                lim = int(a[1:])
            except Exception:
                lim = None
            if lim is not None and od <= lim:
                return False, od
        elif "-" in a:
            try:
                lo, hi = a.split("-", 1)
                lo, hi = int(lo.strip()), int(hi.strip())
                if not (lo <= od <= hi):
                    return False, od
            except Exception:
                pass

    return True, od


# -----------------------------
# Weekly targets helpers (unchanged semantics)
# -----------------------------
def _get_due_display_and_date(row):
    due_display = (
        row.get("Due Date")
        or row.get("Overdue Date")
        or row.get("Trans Date")
        or ""
    )
    return due_display, _parse_any_date(due_display)


def get_open_bills_for_party(party_code=None, party_name=None, company_group="ALL", from_date=None, to_date=None):
    rows = fetch_receivables_raw(company_group=company_group)

    pc = _norm(party_code)
    pn = _norm(party_name)

    from_dt = _to_date_obj(from_date)
    to_dt = _to_date_obj(to_date)

    out = []
    for r in rows:
        r_pc = _norm(r.get("Party Code"))
        r_pn = _norm(r.get("Party Name"))

        if pc:
            if r_pc != pc:
                continue
        elif pn:
            if pn not in r_pn:
                continue

        os_amt = _to_decimal(r.get("Outstanding Amt") or 0)
        if abs(os_amt) <= Decimal("0.0001"):
            continue

        due_display, due_dt = _get_due_display_and_date(r)
        if from_dt and (due_dt is None or due_dt < from_dt):
            continue
        if to_dt and (due_dt is None or due_dt > to_dt):
            continue

        bill_amt = _to_decimal(r.get("Bill Amt") or 0)

        out.append({
            "company_name": r.get("Company Name") or "",
            "party_code": r.get("Party Code") or "",
            "party_name": r.get("Party Name") or "",
            "invoice_no": r.get("Trans No") or "",
            "invoice_date": r.get("Trans Date") or "",
            "due_date": due_display,
            "due_date_dt": due_dt,
            "bill_amount": bill_amt,
            "outstanding_amount": os_amt,
        })

    out.sort(key=lambda x: (x["due_date_dt"] or date.max, (x["invoice_no"] or "")))
    return out


def get_open_bills_for_period(company_group="ALL", from_date=None, to_date=None, *, start_date=None, end_date=None, party_code=None, party_name=None):
    if start_date is None and from_date is not None:
        start_date = from_date
    if end_date is None and to_date is not None:
        end_date = to_date

    return get_open_bills_for_party(
        party_code=party_code,
        party_name=party_name,
        company_group=company_group or "ALL",
        from_date=start_date,
        to_date=end_date,
    )

# -----------------------------
# Dashboard context (FAST from snapshot)
# -----------------------------
def _cache_key_for_dashboard(filters, snap, as_of_date, all_mode):
    f = filters or {}
    parts = [
        "rcv_dash_v2",
        str(snap or ""),
        str(as_of_date or ""),
        "all" if all_mode else "open",
        (f.get("customer") or "").strip().lower(),
        (f.get("aging") or "").strip().lower(),
        (f.get("overdue") or "").strip().lower(),
        (f.get("company") or "").strip().lower(),
        (f.get("company_group") or "").strip().upper(),
        (f.get("from_date") or "").strip(),
        (f.get("to_date") or "").strip(),
        (f.get("show") or "").strip().lower(),
        (f.get("include_all") or "").strip().lower(),
    ]
    return "::".join(parts)


def build_receivable_dashboard_context(filters):
    customer = (filters.get("customer") or "")
    aging = (filters.get("aging") or "")
    overdue = (filters.get("overdue") or "")
    company = (filters.get("company") or "")
    company_group = (filters.get("company_group") or "").strip().upper()

    from_dt = _parse_ui_date(filters.get("from_date") or "")
    to_dt = _parse_ui_date(filters.get("to_date") or "")

    if not from_dt:
        from_dt = BASE_START_DATE

    show = str(filters.get("show") or "").strip().lower()
    include_all = str(filters.get("include_all") or "").strip().lower() in ("1", "true", "yes", "y")
    all_mode = (
        include_all
        or (show in ("all",))
        or (str(aging).strip().upper() == "ALL")
        or (str(overdue).strip().upper() == "ALL")
    )

    as_of_date = to_dt or timezone.localdate()
    snap = _snapshot_date_or_latest(None)

    ck = _cache_key_for_dashboard(filters, snap, as_of_date, all_mode)
    cached = cache.get(ck)
    if cached is not None:
        return cached

    # include_raw=True so PO fields can be extracted from raw JSON
    qs = _snapshot_values_qs(None, include_raw=False, company_group=company_group)
    # IMPORTANT: do NOT exclude outstanding=0 here
    # We will treat "open-only" logic inside the loop so totals stay consistent.


    if customer.strip():
        qs = qs.filter(party_name__icontains=customer.strip())

    if company.strip():
        qs = qs.filter(company_name__icontains=company.strip())

    dec = Decimal

    total_bill = dec("0")
    total_paid = dec("0")
    total_os = dec("0")

    aging_buckets = {
        "0-30 days": dec("0"),
        "31-60 days": dec("0"),
        "61-90 days": dec("0"),
        "91-120 days": dec("0"),
        "121-180 days": dec("0"),
        ">180 days": dec("0"),
    }

    customer_out = defaultdict(Decimal)
    monthly_out = defaultdict(Decimal)
    detail_rows = []

    today = as_of_date
    week_start_current = today - timedelta(days=today.weekday())
    week_end_current = week_start_current + timedelta(days=6)

    prev_week_start = week_start_current - timedelta(days=7)
    prev_week_end = week_start_current - timedelta(days=1)

    prev_week_received = dec("0")
    this_week_incoming = []
    this_week_incoming_total = dec("0")

    filtered_count = 0

    # ✅ Collect detail row IDs to hydrate raw JSON later (only for rendered rows)
    detail_ids = []

    for v in qs.iterator(chunk_size=ITER_CHUNK_SIZE):
        # include_raw=False here; raw/PO will be hydrated only for detail rows
        r = _values_row_to_dict(v)

        ok, od = _passes_overdue_aging_filters(r, aging=aging, overdue=overdue, as_of_date=today)
        if not ok:
            continue

        # Date window semantics kept
        doc_date = r.get("_overdue_dt") or r.get("_due_dt")
        trn_dt = r.get("_trans_dt")
        if doc_date is None:
            doc_date = trn_dt

        if from_dt and (doc_date is None or doc_date < from_dt):
            continue
        if to_dt and (doc_date is None or doc_date > to_dt):
            continue

        filtered_count += 1

        bill = _to_decimal(r.get("Bill Amt") or 0, default=dec("0"))
        paid = _to_decimal(r.get("Paid Amt") or 0, default=dec("0"))
        os_amt = _to_decimal(r.get("Outstanding Amt") or 0, default=dec("0"))

        # Always accumulate totals for the SAME filtered population (ERP-style totals)
        total_bill += bill
        total_paid += paid
        total_os += os_amt

        is_open = (abs(os_amt) > dec("0.0001"))

        # If not all_mode, show only OPEN in registry/analytics (same as your intent)
        if (not all_mode) and (not is_open):
            continue

        # Aging buckets
        if 0 < od <= 30:
            aging_buckets["0-30 days"] += os_amt
        elif 30 < od <= 60:
            aging_buckets["31-60 days"] += os_amt
        elif 60 < od <= 90:
            aging_buckets["61-90 days"] += os_amt
        elif 90 < od <= 120:
            aging_buckets["91-120 days"] += os_amt
        elif 120 < od <= 180:
            aging_buckets["121-180 days"] += os_amt
        elif od > 180:
            aging_buckets[">180 days"] += os_amt

        party_name = r.get("Party Name") or "Unknown"
        customer_out[party_name] += os_amt

        if doc_date:
            month_key = date(doc_date.year, doc_date.month, 1)
            monthly_out[month_key] += os_amt

        # Previous week received (as per your existing logic here)
        if trn_dt and prev_week_start <= trn_dt <= prev_week_end:
            prev_week_received += paid

        # Incoming this week
        if doc_date and week_start_current <= doc_date <= week_end_current and os_amt > 0:
            this_week_incoming.append({
                "company_name": r.get("Company Name"),
                "party_code": r.get("Party Code"),
                "party_name": party_name,
                "invoice_number": r.get("Trans No") or "",
                "due_date": doc_date,
                "due_date_display": doc_date.strftime("%d-%b-%Y"),
                "outstanding_amt": os_amt,
            })
            this_week_incoming_total += os_amt

        status = "CLOSED" if os_amt <= 0 else ("OPEN" if paid == 0 else "PARTIAL")

        company_name = r.get("Company Name") or ""
        cg = get_company_group(company_name)
        due_display = r.get("Overdue Date") or r.get("Due Date") or r.get("Trans Date") or ""

        # ✅ Detail table (truncate)
        if len(detail_rows) < MAX_DETAIL_ROWS:
            row_id = v.get("id")  # values() includes id
            detail_ids.append(row_id)

            detail_rows.append({
                "_row_id": row_id,  # temporary internal for hydration

                "company_name": company_name,
                "party_code": r.get("Party Code"),
                "party_name": party_name,
                "bill_amt": bill,
                "paid_amt": paid,
                "outstanding_amt": os_amt,
                "os_amt": os_amt,
                "trans_no": r.get("Trans No"),
                "trans_date": r.get("Trans Date"),
                "overdue_days": od,
                "days_overdue": od,
                "overdue_date": due_display,
                "item_name": r.get("Item Name"),
                "location": r.get("Location"),
                "company_group": cg,

                # placeholders (hydrated after loop)
                "customer_po_no": "",
                "customer_po_date": "",
                "raw": {},

                # Back-compat keys
                "customer_name": party_name,
                "invoice_number": r.get("Trans No") or "",
                "invoice_date": r.get("Trans Date") or "",
                "due_date": due_display or "",
                "invoice_amount": bill,
                "received_amount": paid,
                "balance_amount": os_amt,
                "status": status,
            })

    # ✅ Hydrate raw + PO only for the truncated detail rows (one DB hit)
    if detail_rows and detail_ids:
        raw_map = dict(
            ReceivableSnapshotRow.objects
            .filter(id__in=detail_ids)
            .values_list("id", "raw")
        )

        for d in detail_rows:
            rid = d.pop("_row_id", None)
            raw = raw_map.get(rid) or {}
            d["raw"] = raw

            po_no, po_date_disp, _po_dt_obj = _extract_customer_po(raw)
            d["customer_po_no"] = po_no
            d["customer_po_date"] = po_date_disp
    
    # END hydrate

    this_week_incoming.sort(key=lambda x: (x["due_date"], (x["party_name"] or "")))

    total_os_crore = (total_os / dec("10000000")) if total_os else dec("0")

    summary = {
        "total_invoiced": total_bill,
        "total_received": total_paid,
        "total_outstanding": total_os,

        "total_bill_amt": total_bill,
        "total_paid_amt": total_paid,
        "total_os_amt": total_os,
        "total_os": total_os,

        "total_outstanding_crore": total_os_crore,
        "total_os_crore": total_os_crore,
    }

    aging_data = [{"aging_bucket": k, "bucket_label": k, "outstanding": v} for k, v in aging_buckets.items()]
    customer_data = [
        {"customer_name": n, "party_name": n, "outstanding": a, "outstanding_amount": a}
        for n, a in sorted(customer_out.items(), key=lambda kv: kv[1], reverse=True)[:20]
    ]
    monthly_data = [
        {"month": m, "month_label": m.strftime("%b %Y"), "outstanding": amt}
        for m, amt in sorted(monthly_out.items())
    ]

    result = {
        "summary": summary,
        "aging_data": aging_data,
        "customer_data": customer_data,
        "monthly_data": monthly_data,
        "previous_week_received": prev_week_received,
        "this_week_incoming": this_week_incoming,
        "this_week_incoming_total": this_week_incoming_total,
        "receivables": detail_rows,
        "as_of_date": as_of_date,
        "all_mode": all_mode,

        # debug/perf
        "detail_limit": MAX_DETAIL_ROWS,
        "detail_total_count": filtered_count,
        "detail_truncated": (filtered_count > MAX_DETAIL_ROWS),
        "snapshot_date": snap,
    }

    cache.set(ck, result, CACHE_SECONDS)
    return result

def _get_paid_amount(row):
    """
    Backward-compatible paid amount getter.
    (Your snapshot rows usually have 'Paid Amt', but keep alternatives safely.)
    """
    return (
        row.get("Paid Amt") if row.get("Paid Amt") is not None else
        row.get("Paid Amount") if row.get("Paid Amount") is not None else
        row.get("Received Amt") if row.get("Received Amt") is not None else
        row.get("Received Amount") if row.get("Received Amount") is not None else 0
    )


def get_received_rows_for_snapshot(company_group="ALL", *, snapshot_date=None, party_name=None):
    """
    Returns invoice rows with paid_amt > 0 from a single snapshot date
    (default: latest snapshot).

    This matches the import used in ACCOUNTS/views.py:
        from ACCOUNTS.services.receivables_dashboard import get_received_rows_for_snapshot
    """
    snap = snapshot_date or latest_snapshot_date()
    if not snap:
        return []

    # Use values() to keep it fast; include_raw=True is fine (PO not required here)
    qs = _snapshot_values_qs(snap, include_raw=False, company_group=company_group)

    if party_name:
        qs = qs.filter(party_name__icontains=str(party_name).strip())

    qs = qs.filter(paid_amt__gt=0).order_by("party_name", "trans_no")

    out = []
    for v in qs.iterator(chunk_size=ITER_CHUNK_SIZE):
        r = _values_row_to_dict(v)

        paid = _to_decimal(r.get("Paid Amt") or 0)
        if paid <= 0:
            continue

        out.append({
            "company_name": r.get("Company Name") or "",
            "party_code": (r.get("Party Code") or "").strip(),
            "party_name": (r.get("Party Name") or "").strip(),
            "invoice_no": (r.get("Trans No") or "").strip(),
            "invoice_date": r.get("Trans Date") or "",
            "due_date": r.get("Due Date") or "",
            "bill_amount": _to_decimal(r.get("Bill Amt") or 0),
            "paid_amount": paid,
            "outstanding_amount": _to_decimal(r.get("Outstanding Amt") or 0),
            "bill_key": _bill_key(r.get("Party Code"), r.get("Trans No")),
            "snapshot_date": snap,
        })

    return out

def get_received_rows_for_period(company_group="ALL", *, start_date=None, end_date=None):
    """
    Returns rows with paid_amt > 0 within a given trans_date window.
    This matches the import used in ACCOUNTS/views.py:
        from ACCOUNTS.services.receivables_dashboard import get_received_rows_for_period, BASE_START_DATE
    """
    start_dt = _to_date_obj(start_date)
    end_dt = _to_date_obj(end_date)

    # include_raw=True not mandatory for this output, but safe/consistent
    rows = fetch_receivables_raw_all(company_group=company_group)

    out = []
    for r in rows:
        trn_dt = r.get("_trans_dt") or _parse_any_date(r.get("Trans Date") or "")
        if not trn_dt:
            continue
        if start_dt and trn_dt < start_dt:
            continue
        if end_dt and trn_dt > end_dt:
            continue

        paid = _to_decimal(_get_paid_amount(r) or 0)
        if paid <= 0:
            continue

        out.append({
            "company_name": r.get("Company Name") or "",
            "party_code": (r.get("Party Code") or "").strip(),
            "party_name": (r.get("Party Name") or "").strip(),
            "invoice_no": (r.get("Trans No") or "").strip(),
            "trans_date": r.get("Trans Date") or "",
            "paid_amount": paid,
            "bill_amount": _to_decimal(r.get("Bill Amt") or 0),
            "outstanding_amount": _to_decimal(r.get("Outstanding Amt") or 0),
            "bill_key": _bill_key(r.get("Party Code"), r.get("Trans No")),
        })

    out.sort(key=lambda x: ((x.get("party_name") or ""), (x.get("invoice_no") or "")))
    return out


