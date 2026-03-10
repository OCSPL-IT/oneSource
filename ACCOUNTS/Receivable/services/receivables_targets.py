# ACCOUNTS/Receivable/services/receivables_targets.py

from __future__ import annotations

from decimal import Decimal
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple
import importlib
import json
import re
from collections import defaultdict


# ---------------------------------------------------------------------
# Dependency resolver (adjust DEP_IMPORT_PATHS to your project)
# ---------------------------------------------------------------------
DEP_IMPORT_PATHS = [
    "ACCOUNTS.services.receivables_erp",
    "ACCOUNTS.services.receivables_data",
    "ACCOUNTS.services.receivables_dashboard",  # only if safe; may cause circular if it imports targets
    "ACCOUNTS.utils.receivables",
]


_KEY_CLEAN_RE = re.compile(r"[^A-Z0-9]+")

def _key_clean(v: Any) -> str:
    s = str(v or "").strip().upper()
    if not s:
        return ""
    return _KEY_CLEAN_RE.sub("", s)

def _resolve_external_callable(name: str):
    """Resolve callable ONLY from DEP_IMPORT_PATHS (skip this module globals)."""
    for mod_path in DEP_IMPORT_PATHS:
        try:
            mod = importlib.import_module(mod_path)
            obj = getattr(mod, name, None)
            if callable(obj):
                return obj
        except Exception:
            continue
    return None


def _resolve_callable(name: str):
    """
    Resolve a callable dependency from:
      1) this module's globals (fallbacks / injected)
      2) any module in DEP_IMPORT_PATHS
    """
    obj = globals().get(name)
    if callable(obj):
        return obj

    obj = _resolve_external_callable(name)
    if callable(obj):
        return obj

    return None


def _require(name: str):
    fn = _resolve_callable(name)
    if not callable(fn):
        raise ImportError(
            f"Missing dependency '{name}'. "
            f"Please add the correct module into DEP_IMPORT_PATHS in receivables_targets.py "
            f"or define '{name}' in an importable module."
        )
    return fn


# ---------------------------------------------------------------------
# ✅ Local fallbacks (prevent 500s when dep injection is not configured)
# ---------------------------------------------------------------------
def get_company_group(obj: Any) -> str:
    """
    Fallback get_company_group.

    Accepts:
      - dict row (ERP row) OR
      - company name string OR
      - any object with .company_group

    Returns "OCSPL" / "OCCHEM" / "".
    """
    if obj is None:
        return ""

    if isinstance(obj, dict):
        v = (
            obj.get("company_group")
            or obj.get("CompanyGroup")
            or obj.get("Company Group")
            or obj.get("Company Name")
            or obj.get("Company")
            or ""
        )
    else:
        v = getattr(obj, "company_group", None)
        if v is None:
            v = obj

    s = str(v or "").strip().upper()

    if s in ("OCSPL", "OCCHEM"):
        return s
    if "OCSPL" in s or "OC SPECIAL" in s or "OC SPECIALITIES" in s:
        return "OCSPL"
    if "OCCHEM" in s or "OC CHEM" in s:
        return "OCCHEM"
    return ""


_MONTH_DOT_RE = re.compile(r"(?<=\b[A-Za-z]{3})\.(?=\s)")  # "Jan. 1, 2026" -> "Jan 1, 2026"


def _parse_ui_date(s: Any) -> Optional[date]:
    if not s:
        return None
    ss = " ".join(str(s).strip().split())
    if not ss:
        return None

    candidates = [ss]
    ss_nodot = _MONTH_DOT_RE.sub("", ss)
    if ss_nodot != ss:
        candidates.append(ss_nodot)

    fmts = (
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%d/%m/%Y",
        "%d-%b-%Y",
        "%d %b %Y",
        "%d %B %Y",
        "%b %d, %Y",
        "%b. %d, %Y",
        "%B %d, %Y",
    )

    for c in candidates:
        for fmt in fmts:
            try:
                return datetime.strptime(c, fmt).date()
            except Exception:
                pass
    return None


def _parse_sql_display_date(s: Any) -> Optional[date]:
    if not s:
        return None
    ss = " ".join(str(s).strip().split())
    if not ss:
        return None

    candidates = [ss]
    ss_nodot = _MONTH_DOT_RE.sub("", ss)
    if ss_nodot != ss:
        candidates.append(ss_nodot)

    fmts = (
        "%d-%b-%Y",
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%b %d, %Y",
        "%b. %d, %Y",
        "%B %d, %Y",
        "%d %b %Y",
        "%d %B %Y",
    )

    for c in candidates:
        for fmt in fmts:
            try:
                return datetime.strptime(c, fmt).date()
            except Exception:
                pass
    return None


def _safe_company_group(get_cg_fn, row: Dict[str, Any]) -> str:
    try:
        cg = get_cg_fn(row)
        return (cg or "").strip()
    except TypeError:
        try:
            cg = get_cg_fn(row.get("Company Name") or "")
            return (cg or "").strip()
        except Exception:
            return ""


# ---------------------------------------------------------------------
# Snapshot fallback (only used if ERP callable not found)
# ---------------------------------------------------------------------
def _import_first_attr(module_paths: List[str], attr_names: List[str]):
    for mp in module_paths:
        try:
            mod = importlib.import_module(mp)
        except Exception:
            continue
        for an in attr_names:
            obj = getattr(mod, an, None)
            if obj is not None:
                return obj
    return None


def _get_snapshot_model():
    module_candidates = [
        "ACCOUNTS.Receivable.models",
        "ACCOUNTS.models",
        "ACCOUNTS.Receivable.snapshot_models",
        "ACCOUNTS.Receivable.receivables_models",
    ]
    model_candidates = [
        "ReceivableSnapshot",
        "ReceivablesSnapshot",
        "ReceivableOutstandingSnapshot",
        "ReceivablesOutstandingSnapshot",
        "ReceivablesSnapshotRow",
    ]
    return _import_first_attr(module_candidates, model_candidates)


def _obj_to_row_dict(obj: Any) -> Dict[str, Any]:
    for fname in ("data", "row", "payload", "json", "row_json", "raw"):
        if hasattr(obj, fname):
            v = getattr(obj, fname)
            if isinstance(v, dict):
                return v
            if isinstance(v, str) and v.strip():
                try:
                    parsed = json.loads(v)
                    if isinstance(parsed, dict):
                        return parsed
                except Exception:
                    pass

    d: Dict[str, Any] = {}
    try:
        for f in obj._meta.fields:
            d[f.verbose_name] = getattr(obj, f.name)
            d[f.name] = getattr(obj, f.name)
    except Exception:
        pass

    return d


def _snapshot_fetch_rows(open_only: bool) -> List[Dict[str, Any]]:
    Model = _get_snapshot_model()
    if Model is None:
        return []

    qs = Model.objects.all()
    out: List[Dict[str, Any]] = []
    for obj in qs:
        r = _obj_to_row_dict(obj)
        if not isinstance(r, dict):
            continue

        if open_only:
            os_amt = _to_decimal(
                r.get("Outstanding Amt") or r.get("outstanding_amount") or r.get("Outstanding") or 0
            )
            if os_amt <= 0:
                continue

        out.append(r)

    return out

# ---------------------------------------------------------------------
# IMPORTANT: ERP function wrappers (do NOT override real implementations)
# ---------------------------------------------------------------------
def fetch_receivables_raw() -> List[Dict[str, Any]]:
    fn = _resolve_external_callable("fetch_receivables_raw")
    if callable(fn):
        return fn()
    return _snapshot_fetch_rows(open_only=True)


def fetch_receivables_raw_all() -> List[Dict[str, Any]]:
    fn = _resolve_external_callable("fetch_receivables_raw_all")
    if callable(fn):
        return fn()
    return _snapshot_fetch_rows(open_only=False)

# ---------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------
def _pick(row: Dict[str, Any], *keys: str, default: Any = "") -> Any:
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip() != "":
            return v
    return default


def _truthy(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "pdc")


def _looks_like_cheque_mode(mode: str) -> bool:
    m = (mode or "").strip().lower()
    return ("cheque" in m) or ("chq" in m) or ("pdc" in m)


def _norm(s: Any) -> str:
    return str(s or "").strip().lower()


def _to_decimal(val: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        s = str(val or "").replace(",", "").strip()
        return Decimal(s) if s else default
    except Exception:
        return default

def _to_date_obj(v: Any) -> Optional[date]:
    if not v:
        return None
    if isinstance(v, date):
        return v
    s = str(v).strip()
    parse_ui = _require("_parse_ui_date")
    parse_disp = _require("_parse_sql_display_date")
    return parse_ui(s) or parse_disp(s)


def _parse_any_date(s: Any) -> Optional[date]:
    if not s:
        return None
    parse_ui = _require("_parse_ui_date")
    parse_disp = _require("_parse_sql_display_date")
    ss = str(s).strip()
    return parse_disp(ss) or parse_ui(ss)


def _get_due_display_and_date(row: Dict[str, Any]) -> Tuple[str, Optional[date]]:
    """
    IMPORTANT FIX:
    Do NOT fallback to 'Trans Date' as due date, otherwise Terms becomes 0.
    """
    due_display = (
        row.get("Due Date")
        or row.get("Overdue Date")
        or row.get("DueDate")
        or row.get("Due_Date")
        or row.get("Due")
        or ""
    )
    return str(due_display or ""), _parse_any_date(due_display)


def _get_trans_date(row: Dict[str, Any]) -> Optional[date]:
    dt = row.get("_trans_dt")
    if dt is not None:
        return dt
    return _parse_any_date(row.get("Trans Date") or row.get("Invoice Date") or row.get("invoice_date") or "")

def _get_paid_amount(row: Dict[str, Any]) -> Any:
    return (
        row.get("Paid Amt")
        if row.get("Paid Amt") is not None
        else row.get("Paid Amount")
        if row.get("Paid Amount") is not None
        else row.get("Received Amt")
        if row.get("Received Amt") is not None
        else row.get("Received Amount")
        if row.get("Received Amount") is not None
        else 0
    )


def _bill_key(party_code: Any, invoice_no: Any) -> str:
    return f"{(party_code or '').strip()}||{(invoice_no or '').strip()}"


def _get_all_rows_fallback() -> List[Dict[str, Any]]:
    """
    Now safe because we always have wrappers:
      - fetch_receivables_raw_all()
      - fetch_receivables_raw()
    """
    rows = fetch_receivables_raw_all()
    if rows:
        return rows
    return fetch_receivables_raw()


def _today_local() -> date:
    """Use Django localdate if available, else date.today()."""
    try:
        from django.utils import timezone
        return timezone.localdate()
    except Exception:
        return date.today()


def _compute_pay_terms_days(trans_dt: Optional[date], due_dt: Optional[date]) -> int:
    """
    Pay Terms (Days) = Due Date - Trans Date
    (taking reference of Trs date)
    """
    if not trans_dt or not due_dt:
        return 0
    d = (due_dt - trans_dt).days
    return d if d > 0 else 0


def _compute_overdue_days(due_dt: Optional[date], *, today: Optional[date] = None) -> int:
    """
    Overdue Days = Today - Due Date if past due else 0
    """
    if not due_dt:
        return 0
    if today is None:
        today = _today_local()
    return (today - due_dt).days if today > due_dt else 0


# ---------------------------------------------------------------------
# ✅ NEW: Normalizers + ERP receipt lookup for PDC enrichment (non-breaking)
# ---------------------------------------------------------------------
_KEY_CLEAN_RE = re.compile(r"[^A-Z0-9]+")


def _key_clean(v: Any) -> str:
    """
    Normalized key for matching:
      - uppercase
      - strip
      - remove non-alphanumerics (spaces, dashes, slashes, etc.)
    This makes "123/ABC-9" and "123ABC9" match.
    """
    s = str(v or "").strip().upper()
    if not s:
        return ""
    return _KEY_CLEAN_RE.sub("", s)


def build_erp_receipt_lookup_for_pdc_match(
    company_group: str = "ALL",
    *,
    # IMPORTANT: default is NO receipt date filtering, so PDC entry can match receipts posted later.
    start_date=None,
    end_date=None,
    filter_by_trans_date: bool = False,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Build lookup keyed by (InstrumentNo, InvoiceRefNo) for ERP receipts.

    Match conditions (your requirement):
      - Django cheque_no == ERP Instrument No.
      - Django invoice_number == ERP Ref No (invoice reference)
      - ERP Paid Amt > 0

    Returned dict value contains receipt fields used to enrich PDC list.
    """
    start_dt = _to_date_obj(start_date) if filter_by_trans_date else None
    end_dt = _to_date_obj(end_date) if filter_by_trans_date else None

    get_company_group_fn = _require("get_company_group")
    rows = _get_all_rows_fallback()

    lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for r in rows:
        cg = _safe_company_group(get_company_group_fn, r)
        if company_group and company_group != "ALL" and cg != company_group:
            continue

        paid = _to_decimal(_get_paid_amount(r) or 0)
        if paid <= 0:
            continue

        if filter_by_trans_date:
            trn_dt = _get_trans_date(r)
            if not trn_dt:
                continue
            if start_dt and trn_dt < start_dt:
                continue
            if end_dt and trn_dt > end_dt:
                continue

        # ERP Instrument No (from TXNCF) - key for matching cheque_no
        inst = _pick(
            r,
            "sExtNo",              # ✅ ADD (most important)
            "Ext No",
            "ExtNo",
            "Instrument No.",
            "Instrument No",
            "InstrumentNo",
            "Cheque No",
            "Chq No",
            "ChequeNo",
            "Instrument No. ",
            default=""
        )
        inst_k = _key_clean(inst)
        if not inst_k:
            continue

        # ERP Invoice reference (Ref No / Reference No / Invoice No) - key for matching invoice_number
        invref = _pick(
            r,
            "Ref No",
            "RefNo",
            "Reference No",
            "Invoice No",
            "InvoiceNo",
            default=""
        )
        inv_k = _key_clean(invref)
        if not inv_k:
            continue

        receipt_no = str(_pick(r, "Receipt No", "Voucher No", "Trans No", "ReceiptNo", default="")).strip()
        receipt_date = str(_pick(r, "Trans Date", "Receipt Date", "Voucher Date", default="")).strip()

        pay_mode = str(_pick(r, "Pay Mode", "Payment Mode", "Instrument Type", "Mode", default="")).strip()

        key = (inst_k, inv_k)

        # Keep latest by trans date if possible, else overwrite with non-empty receipt_no.
        existing = lookup.get(key)
        if existing:
            # Compare trans dates if parseable; else prefer non-empty receipt_no / higher paid
            prev_dt = _parse_any_date(existing.get("_receipt_date_raw"))
            cur_dt = _parse_any_date(receipt_date)

            take = False
            if prev_dt is None and cur_dt is not None:
                take = True
            elif prev_dt is not None and cur_dt is not None and cur_dt >= prev_dt:
                take = True
            elif (existing.get("receipt_no") in (None, "", "-")) and receipt_no:
                take = True
            elif paid > _to_decimal(existing.get("paid_amount") or 0):
                take = True

            if take:
                existing.update({
                    "receipt_no": receipt_no,
                    "receipt_date": receipt_date,
                    "paid_amount": paid,
                    "pay_mode": pay_mode,
                    "_receipt_date_raw": receipt_date,
                })
        else:
            lookup[key] = {
                "receipt_no": receipt_no,
                "receipt_date": receipt_date,
                "paid_amount": paid,
                "pay_mode": pay_mode,
                "_receipt_date_raw": receipt_date,  # internal only
            }

    # strip internal field
    for v in lookup.values():
        v.pop("_receipt_date_raw", None)

    return lookup


# -------------------------------------------------------------------------
# ✅ 0) Receivable Entry rows (THIS SHOULD BE YOUR DJANGO ENTRIES)
# -------------------------------------------------------------------------
def get_receivable_entries_for_period(company_group: str = "ALL", *, start_date=None, end_date=None):
    """
    Pulls Receivable *entry* rows from your Django Receivable model,
    filtered by entry_date in [start_date, end_date].

    This is used for the Excel sheet "Receivable Entry" in target_detail_excel.

    Output keys match your Excel writer:
      mode, cheque_no, cheque_date, is_pdc, pdc_date, amount,
      received_amount, balance_amount, status, remarks

    ✅ Enhancement (non-breaking, additive keys):
      If a row is PDC-like and cheque_no + invoice_no exist, attach matching ERP receipt details:
        erp_receipt_no, erp_receipt_date, erp_paid_amount, erp_pay_mode
    """
    start_dt = _to_date_obj(start_date)
    end_dt = _to_date_obj(end_date)

    # Import Receivable model safely
    Receivable = _import_first_attr(
        ["ACCOUNTS.Receivable.models", "ACCOUNTS.models"],
        ["Receivable"]
    )
    if Receivable is None:
        return []

    qs = Receivable.objects.all()

    if company_group and company_group != "ALL":
        # field name could be company_group; keep safe
        try:
            qs = qs.filter(company_group=company_group)
        except Exception:
            pass

    if start_dt:
        qs = qs.filter(entry_date__gte=start_dt)
    if end_dt:
        qs = qs.filter(entry_date__lte=end_dt)

    qs = qs.order_by("customer_name", "invoice_number")

    # ✅ Build ERP receipt lookup lazily (only if needed)
    # We do NOT filter receipts by trans-date by default, to allow matching receipts posted later.
    erp_lookup: Optional[Dict[Tuple[str, str], Dict[str, Any]]] = None

    out: List[Dict[str, Any]] = []
    today = _today_local()

    for obj in qs:
        inv_amt = _to_decimal(getattr(obj, "invoice_amount", None), default=Decimal("0"))
        rec_amt = _to_decimal(getattr(obj, "received_amount", None), default=Decimal("0"))

        bal = getattr(obj, "balance_amount", None)
        bal_amt = _to_decimal(bal, default=(inv_amt - rec_amt))

        typ = str(getattr(obj, "type", "") or "").strip()
        mode = typ or ""

        cheque_no = str(getattr(obj, "cheque_no", "") or "").strip()
        cheque_date = getattr(obj, "cheque_date", None)
        is_pdc = False

        # if model has is_pdc use it, else infer
        if hasattr(obj, "is_pdc"):
            is_pdc = bool(getattr(obj, "is_pdc") or False)
        else:
            if "PDC" in mode.upper():
                is_pdc = True
            elif cheque_date and isinstance(cheque_date, date) and cheque_date > today:
                is_pdc = True

        remarks = (
            getattr(obj, "narration", None)
            or getattr(obj, "remarks", None)
            or ""
        )

        invoice_no = getattr(obj, "invoice_number", "") or ""
        invoice_no_s = str(invoice_no).strip()

        row = {
            "party_code": getattr(obj, "customer_code", "") or "",
            "party_name": getattr(obj, "customer_name", "") or "",

            "invoice_no": invoice_no_s,
            "invoice_date": getattr(obj, "invoice_date", None),
            "due_date": getattr(obj, "due_date", None),

            "mode": mode,
            "cheque_no": cheque_no,
            "cheque_date": cheque_date,
            "is_pdc": bool(is_pdc),
            "pdc_date": cheque_date if is_pdc else "",

            "amount": inv_amt,
            "received_amount": rec_amt,
            "balance_amount": bal_amt,

            "status": getattr(obj, "status", "") or "",
            "remarks": remarks,

            # ✅ New (additive) ERP enrichment fields (defaults)
            "erp_receipt_no": "",
            "erp_receipt_date": "",
            "erp_paid_amount": None,
            "erp_pay_mode": "",
        }

        # ✅ Enrich only for PDC-like rows where we have both keys
        if is_pdc and cheque_no and invoice_no_s:
            if erp_lookup is None:
                erp_lookup = build_erp_receipt_lookup_for_pdc_match(
                    company_group=company_group or "ALL",
                    start_date=start_dt,
                    end_date=end_dt,
                    filter_by_trans_date=False,  # key requirement: match even if receipt posted later
                )

            key = (_key_clean(cheque_no), _key_clean(invoice_no_s))
            match = erp_lookup.get(key) if erp_lookup else None
            if match:
                row["erp_receipt_no"] = match.get("receipt_no") or ""
                row["erp_receipt_date"] = match.get("receipt_date") or ""
                row["erp_paid_amount"] = match.get("paid_amount")
                row["erp_pay_mode"] = match.get("pay_mode") or ""

        out.append(row)

    # --- after out.append(row) loop finishes ---
    # If you want ERP receipt to appear even if posted AFTER the selected week,
    # DO NOT restrict the ERP index by start/end dates here.
    erp_index = build_erp_receipts_lookup_for_week(
        company_group=company_group or "ALL",
        start_date=None,   # ✅ IMPORTANT: do not filter by week for matching
        end_date=None,
    )

    # Attach receipt_no/receipt_date/erp_paid_total/erp_receipts/erp_matched
    out = attach_erp_receipts_to_pdc_entries(out, erp_index)

    return out

# ---------------------------------------------------------------------
# ✅ NEW (Additive): invoice reference extractor for receipts (Against Target)
#   This is the ONLY functional change needed to fix Target Detail received.
# ---------------------------------------------------------------------
_KEY_CLEAN_RE = re.compile(r"[^A-Z0-9]+")
_CMU_RE = re.compile(r"\b(CMU\d{6,})\b", re.I)   # adjust if your invoice numbering differs
_LONG_TOKEN_RE = re.compile(r"\b([A-Z0-9]{8,})\b", re.I)

def _key_clean(v: Any) -> str:
    s = str(v or "").strip().upper()
    if not s:
        return ""
    return _KEY_CLEAN_RE.sub("", s)

def _extract_receipt_invoice_ref(row: Dict[str, Any]) -> str:
    """
    Receipt-to-invoice reference (what should match selected bill invoice_no).
    Priority:
      1) Explicit ref/invoice keys (Ref No / Reference No / Invoice No / Bill No)
      2) Raw dict nested (if present)
      3) Parse from narration-like fields (CMU... token or long token)
    Returns cleaned (key_clean) string.
    """
    # direct keys
    for k in (
        "Ref No", "RefNo", "ref_no",
        "Reference No", "ReferenceNo", "reference_no",
        "Invoice No", "InvoiceNo", "invoice_no",
        "Bill No", "BillNo", "bill_no",
        "Against Bill", "AgainstBill",
    ):
        v = row.get(k)
        if v:
            return _key_clean(v)

    # raw nested if snapshot row dict is stored as `raw`
    raw = row.get("raw")
    if isinstance(raw, dict):
        for k in (
            "Ref No", "RefNo", "ref_no",
            "Reference No", "ReferenceNo", "reference_no",
            "Invoice No", "InvoiceNo", "invoice_no",
            "Bill No", "BillNo", "bill_no",
            "Against Bill", "AgainstBill",
        ):
            v = raw.get(k)
            if v:
                return _key_clean(v)

    # parse from text blob
    blob = " ".join([
        str(row.get("Narration") or ""),
        str(row.get("narration") or ""),
        str(row.get("remarks") or ""),
        str(row.get("particulars") or ""),
        str(row.get("trans_ref") or ""),
        str(row.get("Trans No") or ""),   # sometimes contains both receipt + invoice tokens
        str(row.get("trans_no") or ""),
    ]).strip().upper()

    if blob:
        m = _CMU_RE.search(blob)
        if m:
            return _key_clean(m.group(1))

        tokens = _LONG_TOKEN_RE.findall(blob)
        for t in tokens:
            tt = (t or "").strip().upper()
            if any(ch.isdigit() for ch in tt):
                return _key_clean(tt)

    return ""

# -------------------------------------------------------------------------
# 1) OPEN bills for selection (Outstanding > 0) from ERP/Snapshot
# -------------------------------------------------------------------------
def get_open_bills_for_party(
    party_code: Optional[str] = None,
    party_name: Optional[str] = None,
    company_group: str = "ALL",
    from_date=None,
    to_date=None,
    *,
    bill_cutoff_date=None,
):
    rows = fetch_receivables_raw()  # OPEN only (safe wrapper)

    pc = _norm(party_code)
    pn = _norm(party_name)

    from_dt = _to_date_obj(from_date)
    to_dt = _to_date_obj(to_date)

    cutoff_dt = _to_date_obj(bill_cutoff_date) if bill_cutoff_date else None

    get_company_group_fn = _require("get_company_group")
    today = _today_local()

    out: List[Dict[str, Any]] = []
    for r in rows:
        cg = _safe_company_group(get_company_group_fn, r)
        if company_group and company_group != "ALL" and cg != company_group:
            continue

        if cutoff_dt:
            trn_dt_cut = _get_trans_date(r)
            if trn_dt_cut and trn_dt_cut > cutoff_dt:
                continue

        r_pc = _norm(r.get("Party Code") or r.get("party_code"))
        r_pn = _norm(r.get("Party Name") or r.get("party_name"))

        if pc:
            if r_pc != pc:
                continue
        elif pn:
            if pn not in r_pn:
                continue

        os_amt = _to_decimal(r.get("Outstanding Amt") or r.get("outstanding_amount") or 0)
        if abs(os_amt) <= Decimal("0.0001"):
            continue

        due_display, due_dt = _get_due_display_and_date(r)

        # ✅ Trans Date (Trs date) parsing
        trans_display = (
            r.get("Trans Date")
            or r.get("Invoice Date")
            or r.get("invoice_date")
            or ""
        )
        trans_dt = _parse_any_date(trans_display)

        # ✅ Pay terms / Overdue
        pay_terms_days = _compute_pay_terms_days(trans_dt, due_dt)
        overdue_days = _compute_overdue_days(due_dt, today=today)

        # existing filters by due-date period
        if from_dt and (due_dt is None or due_dt < from_dt):
            continue
        if to_dt and (due_dt is None or due_dt > to_dt):
            continue

        bill_amt = _to_decimal(r.get("Bill Amt") or r.get("bill_amount") or 0)

        out.append({
            "company_name": r.get("Company Name") or r.get("company_name") or "",
            "party_code": r.get("Party Code") or r.get("party_code") or "",
            "party_name": r.get("Party Name") or r.get("party_name") or "",
            "invoice_no": r.get("Trans No") or r.get("invoice_no") or "",

            # keep your existing fields
            "invoice_date": trans_display,
            "invoice_date_dt": trans_dt,
            "due_date": due_display,
            "due_date_dt": due_dt,

            "bill_amount": bill_amt,
            "outstanding_amount": os_amt,

            # ✅ NEW: for UI badges + Excel
            "pay_terms_days": pay_terms_days,
            "overdue_days": overdue_days,
        })

    out.sort(key=lambda x: (x["due_date_dt"] or date.max, (x.get("invoice_no") or "")))
    return out


def get_open_bills_for_period(
    company_group: str = "ALL",
    from_date=None,
    to_date=None,
    *,
    start_date=None,
    end_date=None,
    party_code: Optional[str] = None,
    party_name: Optional[str] = None,
    bill_cutoff_date=None,
):
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
        bill_cutoff_date=bill_cutoff_date,
    )


def get_open_bills_for_period_snapshot(
    *,
    start_date=None,
    end_date=None,
    company_group: str = "ALL",
    party_code: Optional[str] = None,
    party_name: Optional[str] = None,
    bill_cutoff_date=None,
):
    return get_open_bills_for_period(
        company_group=company_group,
        start_date=start_date,
        end_date=end_date,
        party_code=party_code,
        party_name=party_name,
        bill_cutoff_date=bill_cutoff_date,
    )


# -------------------------------------------------------------------------
# 2) RECEIPTS (Paid Amt) reporting for target detail split (ERP/Snapshot)
# -------------------------------------------------------------------------
_INST_CLEAN_RE = re.compile(r"[^A-Z0-9]+")

def _norm_inst(v: Any) -> str:
    s = str(v or "").strip().upper()
    if not s:
        return ""
    if s.startswith("#"):
        s = s[1:]
    s = _INST_CLEAN_RE.sub("", s)
    if s.isdigit():
        s = s.lstrip("0") or "0"
    return s

def _get_receipt_invoice_ref(row: Dict[str, Any]) -> str:
    """
    For receipt (Paid Amt) rows, invoice reference is usually NOT Trans No.
    It is commonly in Ref No / Reference No / Invoice No / Bill No.
    Return raw string (not cleaned) so caller can decide normalization.
    """
    return str(
        row.get("Ref No")
        or row.get("RefNo")
        or row.get("Reference No")
        or row.get("Invoice No")
        or row.get("InvoiceNo")
        or row.get("Bill No")
        or row.get("BillNo")
        or row.get("Bill Ref No")
        or row.get("BillRefNo")
        or ""
    ).strip()

def build_paid_lookup_for_period(
    company_group: str = "ALL",
    *,
    start_date=None,
    end_date=None,
):
    """
    EXISTING FLOW PRESERVED:
      - filters by company_group
      - filters by trans_date between start/end
      - paid > 0
      - returns dict: bill_key -> total paid

    ✅ FIX (non-breaking):
      - invoice_no used in bill_key is now the receipt's INVOICE REFERENCE (Ref No / Invoice No),
        NOT the receipt voucher "Trans No".
      - This makes keys align with PaymentTargetLine.invoice_no.
    """
    start_dt = _to_date_obj(start_date)
    end_dt = _to_date_obj(end_date)

    get_company_group_fn = _require("get_company_group")
    rows = _get_all_rows_fallback()

    lookup: Dict[str, Decimal] = {}
    for r in rows:
        cg = _safe_company_group(get_company_group_fn, r)
        if company_group and company_group != "ALL" and cg != company_group:
            continue

        trn_dt = _get_trans_date(r)
        if not trn_dt:
            continue
        if start_dt and trn_dt < start_dt:
            continue
        if end_dt and trn_dt > end_dt:
            continue

        paid = _to_decimal(_get_paid_amount(r) or 0)
        if paid <= 0:
            continue

        party_code = (r.get("Party Code") or r.get("party_code") or "").strip()

        # ✅ Receipt rows: use invoice reference (Ref No / Invoice No / Bill No), not Trans No
        invoice_ref_clean = _extract_receipt_invoice_ref(r)  # already cleaned

        # Fallback only if snapshot truly stores invoice in Trans No (rare)
        if not invoice_ref:
            invoice_ref = (r.get("Trans No") or r.get("invoice_no") or "").strip()

        # ✅ Normalize like your matching logic expects (handles / - spaces)
        invoice_ref_clean = _key_clean(invoice_ref)

        key = _bill_key(party_code, invoice_ref_clean)
        lookup[key] = lookup.get(key, Decimal("0")) + paid

    return lookup


def get_received_rows_for_period(
    company_group: str = "ALL",
    *,
    start_date=None,
    end_date=None,
):
    """
    Kept same behavior: returns receipt rows for "Week Receipts" section.

    ✅ Update (non-breaking): for receipt rows, display/match the *invoice reference*
    (Ref No / Reference No / Invoice No / Bill No) instead of using Trans No (which is
    usually the receipt/voucher number in ERP).

    NOTE: This does not change Target Detail calculation by itself, but it ensures the
    week receipts list + bill_key aligns with invoice refs.
    """
    start_dt = _to_date_obj(start_date)
    end_dt = _to_date_obj(end_date)

    get_company_group_fn = _require("get_company_group")
    rows = _get_all_rows_fallback()

    out: List[Dict[str, Any]] = []
    for r in rows:
        cg = _safe_company_group(get_company_group_fn, r)
        if company_group and company_group != "ALL" and cg != company_group:
            continue

        trn_dt = _get_trans_date(r)
        if not trn_dt:
            continue
        if start_dt and trn_dt < start_dt:
            continue
        if end_dt and trn_dt > end_dt:
            continue

        paid = _to_decimal(_get_paid_amount(r) or 0)
        if paid <= 0:
            continue

        party_code = (r.get("Party Code") or r.get("party_code") or "").strip()

        # ✅ Receipt voucher no (for display as Receipt No)
        receipt_no = str(
            r.get("Trans No")
            or r.get("trans_no")
            or r.get("Receipt No")
            or r.get("Voucher No")
            or ""
        ).strip()

        receipt_date = str(r.get("Trans Date") or r.get("trans_date") or "").strip()

        # ✅ Invoice reference for matching selected bills (NOT Trans No for receipts)
        invoice_ref = str(
            r.get("Ref No")
            or r.get("RefNo")
            or r.get("Reference No")
            or r.get("Invoice No")
            or r.get("InvoiceNo")
            or r.get("Bill No")
            or r.get("BillNo")
            or r.get("Bill Ref No")
            or r.get("BillRefNo")
            or ""
        ).strip()

        # Fallback only if your data truly stores invoice in Trans No / invoice_no
        if not invoice_ref:
            invoice_ref = str(r.get("invoice_no") or r.get("Invoice No") or "").strip()
        if not invoice_ref:
            # last fallback: keep old behavior (but this is usually receipt voucher)
            invoice_ref = str(r.get("Trans No") or r.get("invoice_no") or "").strip()

        # ✅ Use cleaned invoice ref for bill_key so it matches your target detail normalization
        invoice_ref_clean = _key_clean(invoice_ref)

        out.append({
            "company_name": r.get("Company Name") or r.get("company_name") or "",
            "party_code": party_code,
            "party_name": r.get("Party Name") or r.get("party_name") or "",

            # show human invoice ref in list
            "invoice_no": invoice_ref,

            "trans_date": receipt_date,
            "paid_amount": paid,
            "bill_amount": _to_decimal(r.get("Bill Amt") or r.get("bill_amount") or 0),
            "outstanding_amount": _to_decimal(r.get("Outstanding Amt") or r.get("outstanding_amount") or 0),

            # ✅ bill_key used elsewhere (align to invoice ref)
            "bill_key": _bill_key(party_code, invoice_ref_clean),

            "instrument_no": (
                r.get("sExtNo")
                or r.get("instrument_no")
                or r.get("Instrument No.")
                or r.get("Instrument No")
                or r.get("InstrumentNo")
                or r.get("Cheque No")
                or r.get("Chq No")
                or r.get("ChequeNo")
                or ""
            ),

            # receipt voucher details
            "receipt_no": receipt_no,
            "receipt_date": receipt_date,
            "raw": r,
        })

    out.sort(key=lambda x: ((x.get("party_name") or ""), (x.get("invoice_no") or "")))
    return out

# ---------------------------------------------------------------------
# 3) ERP Receipt lookup builder + PDC entry attachment (kept as-is)
# ---------------------------------------------------------------------
def _norm_key(v: Any) -> str:
    return str(v or "").strip().upper()

def _get_erp_invoice_ref_no(row: Dict[str, Any]) -> str:
    return _norm_key(
        row.get("Ref No")
        or row.get("RefNo")
        or row.get("Reference No")
        or row.get("Invoice No")
        or row.get("InvoiceNo")
        or row.get("Bill No")
        or row.get("BillNo")
        or ""
    )

def _get_erp_instrument_no(row: Dict[str, Any]) -> str:
    return _norm_key(
        row.get("sExtNo")
        or row.get("instrument_no")
        or row.get("Instrument No.")
        or row.get("Instrument No")
        or row.get("InstrumentNo")
        or row.get("Cheque No")
        or row.get("Chq No")
        or row.get("ChequeNo")
        or ""
    )

def build_erp_receipts_lookup_for_week(
    company_group: str = "ALL",
    *,
    start_date=None,
    end_date=None,
) -> Dict[str, Any]:
    start_dt = _to_date_obj(start_date)
    end_dt = _to_date_obj(end_date)

    get_company_group_fn = _require("get_company_group")
    rows = _get_all_rows_fallback()

    strict = defaultdict(list)   # (party_code, invoice_ref, instrument_no)
    relaxed = defaultdict(list)  # (party_code, instrument_no)

    for r in rows:
        cg = _safe_company_group(get_company_group_fn, r)
        if company_group and company_group != "ALL" and cg != company_group:
            continue

        trn_dt = _get_trans_date(r)
        if not trn_dt:
            continue
        if start_dt and trn_dt < start_dt:
            continue
        if end_dt and trn_dt > end_dt:
            continue

        paid = _to_decimal(_get_paid_amount(r) or 0)
        if paid <= 0:
            continue

        party_code = _norm_key(r.get("Party Code") or r.get("party_code"))
        inst = _norm_inst(_get_erp_instrument_no(r))

        invref_raw = (
            r.get("Ref No")
            or r.get("RefNo")
            or r.get("Reference No")
            or r.get("Invoice No")
            or r.get("InvoiceNo")
            or r.get("Bill No")
            or r.get("BillNo")
            or ""
        )
        invref = _key_clean(invref_raw)

        if not party_code or not inst:
            continue

        receipt_no = str(
            r.get("Trans No")
            or r.get("trans_no")
            or r.get("Receipt No")
            or r.get("Voucher No")
            or ""
        ).strip()
        trans_date = str(r.get("Trans Date") or r.get("trans_date") or "").strip()

        item = {
            "company_name": r.get("Company Name") or r.get("company_name") or "",
            "party_name": r.get("Party Name") or r.get("party_name") or "",
            "party_code": party_code,
            "invoice_no": invref,
            "instrument_no": inst,
            "receipt_no": receipt_no,
            "trans_date": trans_date,
            "paid_amount": paid,
        }

        if invref:
            strict[(party_code, invref, inst)].append(item)

        relaxed[(party_code, inst)].append(item)

    return {"strict": strict, "relaxed": relaxed}


def attach_erp_receipts_to_pdc_entries(
    receivable_entries: List[Dict[str, Any]],
    erp_index: Dict[str, Any],
) -> List[Dict[str, Any]]:
    strict = erp_index.get("strict") or {}
    relaxed = erp_index.get("relaxed") or {}

    for r in receivable_entries:
        party_code = _norm_key(r.get("party_code"))
        invoice_no = _key_clean(r.get("invoice_no"))

        inst = _norm_inst(r.get("instrument_no") or r.get("cheque_no"))

        r["erp_matched"] = False
        r["erp_paid_total"] = Decimal("0")
        r["erp_receipts"] = []
        r.setdefault("receipt_no", "")
        r.setdefault("receipt_date", "")

        if not party_code or not inst:
            continue

        matches = []

        if invoice_no:
            matches = strict.get((party_code, invoice_no, inst)) or []

        if not matches:
            cand = relaxed.get((party_code, inst)) or []
            inv_set = {c.get("invoice_no") for c in cand if c.get("invoice_no")}
            if len(inv_set) <= 1:
                matches = cand

        if not matches:
            continue

        total = sum((m.get("paid_amount") or Decimal("0")) for m in matches)
        last_dt = ""
        for m in matches:
            td = str(m.get("trans_date") or "")
            if td and (not last_dt or td > last_dt):
                last_dt = td

        r["erp_matched"] = True
        r["erp_paid_total"] = total
        r["erp_receipts"] = matches

        r["receipt_no"] = matches[0].get("receipt_no") or ""
        r["receipt_date"] = last_dt or (matches[0].get("trans_date") or "")

    return receivable_entries