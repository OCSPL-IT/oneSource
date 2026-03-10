# ACCOUNTS/Receivable/ajax_views.py
from decimal import Decimal
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.db.models import Q
from django.utils import timezone

from ACCOUNTS.Receivable.models import ReceivableSnapshotRow
from ACCOUNTS.Receivable.services.receivables_sync import latest_snapshot_date


@login_required
def ajax_customers(request):
    q = (request.GET.get("q") or "").strip()

    # IMPORTANT:
    # Use the table that contains the full customer list.
    # Best source in your flow is usually ReceivableSnapshotRow (latest snapshot).
    snap = latest_snapshot_date()
    if not snap:
        return JsonResponse({"results": []})

    qs = (
        ReceivableSnapshotRow.objects
        .filter(snapshot_date=snap)
        .exclude(party_code__isnull=True)
        .exclude(party_code__exact="")
        .exclude(party_name__isnull=True)
        .exclude(party_name__exact="")
    )

    # ✅ Show BOTH OCSPL + OCHEM automatically (no filter here)
    # If you previously filtered company_name by "Special"/"Chem", REMOVE it.

    if q:
        qs = qs.filter(
            Q(party_code__icontains=q) |
            Q(party_name__icontains=q) |
            Q(company_name__icontains=q)
        )

    # DISTINCT party_code list (SQL Server safe way: values + distinct)
    qs = qs.values("party_code", "party_name").distinct().order_by("party_code")

    # Increase limit (or implement paging); keep a sane cap
    qs = qs[:2000]

    results = []
    for r in qs:
        code = (r["party_code"] or "").strip()
        name = (r["party_name"] or "").strip()
        if not code:
            continue
        results.append({
            "id": code,                       # used as value in dropdown
            "text": f"{code} - {name}",
            "name": name,
        })

    return JsonResponse({"results": results})

@login_required
def ajax_customer_invoices(request):
    customer_code = (request.GET.get("customer_code") or "").strip()
    q = (request.GET.get("q") or "").strip()

    snap = latest_snapshot_date()
    if not snap or not customer_code:
        return JsonResponse({"results": []})

    qs = (
        ReceivableSnapshotRow.objects
        .filter(snapshot_date=snap, party_code__iexact=customer_code)
        .exclude(trans_no__isnull=True)
        .exclude(trans_no__exact="")
    )

    if q:
        qs = qs.filter(Q(trans_no__icontains=q))

    qs = qs.only(
        "trans_no", "trans_date", "due_date", "overdue_date",
        "bill_amt", "outstanding_amt"
    ).order_by("-trans_date", "trans_no")[:2000]

    results = []
    for r in qs:
        inv_no = (r.trans_no or "").strip()
        inv_date = r.trans_date
        due_date = r.due_date or r.overdue_date

        bill_amt = r.bill_amt or Decimal("0")
        os_amt = r.outstanding_amt or Decimal("0")

        results.append({
            "id": inv_no,
            "text": inv_no,
            "invoice_date": inv_date.isoformat() if inv_date else "",
            "due_date": due_date.isoformat() if due_date else "",
            "invoice_amount": str(bill_amt),          # ✅ NEW
            "outstanding_amount": str(os_amt),        # optional
        })

    return JsonResponse({"results": results})
