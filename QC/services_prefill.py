from __future__ import annotations
from datetime import date, timedelta
from typing import List, Dict

# Import directly from QC models
from QC.models import (
    IncomingGRNCache,
    DailyQAReport,
    IncomingMaterial,
    PDLSample,
)

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
def _rm_pm_from_item_type(item_type: str) -> str:
    """
    Map ERP item type text to our 2-letter choices.
    'Packing' -> 'PM'; everything else (Raw/Key Raw/etc.) -> 'RM'
    """
    it = (item_type or "").strip().lower()
    return "PM" if "pack" in it else "RM"


# ------------------------------------------------------------------------------
# Incoming Material [RM/PM]
# ------------------------------------------------------------------------------
def fetch_incoming_rm_pm(on_date: date) -> List[Dict]:
    """
    Build formset 'initial' rows for Incoming Material (RM/PM).

    Order of precedence:
      1) Cached GRNs for the SAME day (IncomingGRNCache.grn_date = on_date)
      2) Cached GRNs for the PREVIOUS day (on_date - 1)
      3) Already-saved IncomingMaterial rows for on_date

    Returned dict keys match the form fields:
      material_type, material, supplier, qty_mt, (optional status, remarks)
    """
    # 1) Same-day cache
    same_day_qs = (
        IncomingGRNCache.objects
        .filter(grn_date=on_date)
        .order_by("grn_no", "item_name")
        .values("item_type", "item_name", "supplier_name", "qty")
    )

    initial: List[Dict] = [
        {
            "material_type": _rm_pm_from_item_type(r["item_type"]),
            "material":      r["item_name"],
            "supplier":      r["supplier_name"],
            "qty_mt":        r["qty"],
        }
        for r in same_day_qs
    ]
    if initial:
        return initial

    # 2) Previous-day cache
    prev = on_date - timedelta(days=1)
    prev_qs = (
        IncomingGRNCache.objects
        .filter(grn_date=prev)
        .order_by("grn_no", "item_name")
        .values("item_type", "item_name", "supplier_name", "qty")
    )

    initial = [
        {
            "material_type": _rm_pm_from_item_type(r["item_type"]),
            "material":      r["item_name"],
            "supplier":      r["supplier_name"],
            "qty_mt":        r["qty"],
        }
        for r in prev_qs
    ]
    if initial:
        return initial

    # 3) Fallback to rows already saved on that date (if any)
    saved_qs = IncomingMaterial.objects.filter(
        report__report_date=on_date
    ).order_by("id")

    initial = [
        {
            "material_type": obj.material_type,
            "material": obj.material,
            "supplier": obj.supplier,
            "qty_mt": obj.qty_mt,
            "status": obj.status,
            "remarks": obj.remarks,
        }
        for obj in saved_qs
    ]

    # Ensure at least one empty row so the table is usable
    return initial or [{}]


# ------------------------------------------------------------------------------
# PDL Samples
# ------------------------------------------------------------------------------
def fetch_pdl_samples(on_date: date) -> List[Dict]:
    qs = PDLSample.objects.filter(report__report_date=on_date).order_by("id")
    return [
        {
            "sample_name": obj.sample_name,
            "pending": obj.pending,
            "remark": obj.remark,
        }
        for obj in qs
    ]


# ------------------------------------------------------------------------------
# Other Details block (header fields)
# ------------------------------------------------------------------------------
def fetch_other_details(on_date: date) -> Dict:
    """
    Returns other header details for given report date.
    Keys exactly match DailyQAReport form fields.
    """
    try:
        rpt = DailyQAReport.objects.get(report_date=on_date)
        return {
            "customer_complaints": rpt.customer_complaints,
            "analytical_mistakes": rpt.analytical_mistakes,
            "process_deviations": rpt.process_deviations,
            "incident_first_aid_injury": rpt.incident_first_aid_injury,
            "ftr_percent": rpt.ftr_percent,
            "analytical_downtime_hrs": rpt.analytical_downtime_hrs,
            "finished_goods_inspections": rpt.finished_goods_inspections,
            "any_other_abnormality": rpt.any_other_abnormality,
        }
    except DailyQAReport.DoesNotExist:
        return {
            "customer_complaints": 0,
            "analytical_mistakes": 0,
            "process_deviations": 0,
            "incident_first_aid_injury": 0,
            "ftr_percent": 0,
            "analytical_downtime_hrs": 0,
            "finished_goods_inspections": 0,
            "any_other_abnormality": "",
        }
