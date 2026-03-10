# ACCOUNTS/services/receivables_weekly_fast.py
from decimal import Decimal
from django.core.cache import cache
from django.db.models import Sum, Q
from django.db.models.functions import Coalesce

from ACCOUNTS.Receivable.models import ReceivableSnapshotRow
from ACCOUNTS.Receivable.services.receivables_sync import latest_snapshot_date
from ACCOUNTS.Receivable.services.company_groups import COMPANY_GROUPS  # ✅ single source


def _dstr(d):
    return d.isoformat() if hasattr(d, "isoformat") else (str(d or ""))


def _bill_key(pc, inv):
    return f"{(pc or '').strip().upper()}||{(inv or '').strip().upper()}"


def _company_group_qs(qs, company_group: str):
    group = (company_group or "ALL").strip().upper()
    if not group or group == "ALL":
        return qs

    if group == "OTHER":
        known = set()
        for names in COMPANY_GROUPS.values():
            known.update(names)
        return qs.exclude(company_name__in=list(known))

    names = COMPANY_GROUPS.get(group) or []
    if not names:
        return qs

    # exact match + safe fallback contains
    q = Q(company_name__in=names)
    if group == "OCSPL":
        q |= Q(company_name__icontains="OC Specialities Private Limited")
    elif group == "OCCHEM":
        q |= Q(company_name__icontains="OC Specialities Chemicals Private Limited")

    return qs.filter(q)


def build_paid_lookup_for_period(*, company_group="ALL", start_date=None, end_date=None, snapshot_date=None):
    snap = snapshot_date or latest_snapshot_date()
    if not snap:
        return {}

    cache_key = f"paid_lookup::{snap}::{company_group}::{_dstr(start_date)}::{_dstr(end_date)}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    qs = ReceivableSnapshotRow.objects.filter(snapshot_date=snap, paid_amt__gt=0)
    qs = _company_group_qs(qs, company_group)

    if start_date:
        qs = qs.filter(trans_date__gte=start_date)
    if end_date:
        qs = qs.filter(trans_date__lte=end_date)

    agg = qs.values("party_code", "trans_no").annotate(
        total_paid=Coalesce(Sum("paid_amt"), Decimal("0"))
    )

    out = {}
    for r in agg:
        pc = (r.get("party_code") or "").strip()
        inv = (r.get("trans_no") or "").strip()
        if pc and inv:
            out[_bill_key(pc, inv)] = r["total_paid"] or Decimal("0")

    cache.set(cache_key, out, 300)
    return out


def get_received_rows_for_period(*, company_group="ALL", start_date=None, end_date=None, snapshot_date=None, limit=5000):
    snap = snapshot_date or latest_snapshot_date()
    if not snap:
        return []

    cache_key = f"week_rows::{snap}::{company_group}::{_dstr(start_date)}::{_dstr(end_date)}::{int(limit or 0)}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    qs = ReceivableSnapshotRow.objects.filter(snapshot_date=snap, paid_amt__gt=0)
    qs = _company_group_qs(qs, company_group)

    if start_date:
        qs = qs.filter(trans_date__gte=start_date)
    if end_date:
        qs = qs.filter(trans_date__lte=end_date)

    qs = qs.values(
        "company_name", "party_code", "party_name",
        "trans_no", "trans_date_display", "paid_amt"
    ).order_by("party_name", "trans_no")

    if limit:
        qs = qs[:limit]

    out = [{
        "company_name": r.get("company_name") or "",
        "party_code": r.get("party_code") or "",
        "party_name": r.get("party_name") or "",
        "invoice_no": r.get("trans_no") or "",
        "trans_date": r.get("trans_date_display") or "",
        "paid_amount": r.get("paid_amt") or Decimal("0"),
    } for r in qs]

    cache.set(cache_key, out, 300)
    return out


def get_received_totals_for_period(*, company_group="ALL", start_date=None, end_date=None, snapshot_date=None):
    snap = snapshot_date or latest_snapshot_date()
    if not snap:
        return {"received_total": Decimal("0")}

    cache_key = f"week_total::{snap}::{company_group}::{_dstr(start_date)}::{_dstr(end_date)}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    qs = ReceivableSnapshotRow.objects.filter(snapshot_date=snap, paid_amt__gt=0)
    qs = _company_group_qs(qs, company_group)

    if start_date:
        qs = qs.filter(trans_date__gte=start_date)
    if end_date:
        qs = qs.filter(trans_date__lte=end_date)

    total = qs.aggregate(total=Coalesce(Sum("paid_amt"), Decimal("0")))["total"]
    out = {"received_total": total or Decimal("0")}
    cache.set(cache_key, out, 300)
    return out
# --------------------------------------------------------