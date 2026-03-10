# ETP/storage_tank_reporting.py
from datetime import date, timedelta
from decimal import Decimal
from typing import List, Tuple, Optional
from django.db.models import Sum
from .models import EffluentTank, EffluentOpeningBalance
from ETP.models import EffluentQty, GeneralEffluent,PrimaryTreatmentEffluent  # adjust path if needed

def _days_between(fr: date, to: date):
    d = fr
    while d <= to:
        yield d
        d += timedelta(days=1)

def _month_first(d: date) -> date:
    return d.replace(day=1)

# ─────────── Inlet fetcher (already wired to your tables) ───────────
def fetch_inlet_for(tank: EffluentTank, d) -> Decimal:
    """
    Inlet by tank type (separate rules):
      - Cyanide Effluent Storage Tank  -> EffluentQty ['Sodium Cyanide Effluent']
      - Spent HCL Storage Tank         -> GeneralEffluent ['Spent HCL']
      - Residue Effluent Storage Tank  -> EffluentQty ['Residue']
      - Basic tanks                    -> EffluentQty ['Scrubber Basic Effluent','Basic']
                                          + GeneralEffluent ['Ejector effluent','Outside Drainage Water',
                                                             'PCO Cleaning / cleaning Effluent','Scrubber Basic Effluent',
                                                             'Basic','Dyke Effluent','QC effluent']
      - Acidic tanks                   -> EffluentQty ['Acidic','Acidic Aq. Layer']
                                          + GeneralEffluent ['Scrubber Acidic Effluent']
    """
    name = (tank.name or "").strip().lower()

    if "cyanide effluent" in name:
        eq_qs = EffluentQty.objects.filter(
            effluent_record__record_date=d,
            effluent_nature__in=["Sodium Cyanide Effluent"],
        )
        ge_qs = GeneralEffluent.objects.none()

    elif "spent hcl" in name:
        eq_qs = EffluentQty.objects.none()
        ge_qs = GeneralEffluent.objects.filter(
            record_date=d,
            effluent_nature__in=["Spent HCL"],
        )

    elif "residue effluent" in name:
        eq_qs = EffluentQty.objects.filter(
            effluent_record__record_date=d,
            effluent_nature__in=["Residue"],
        )
        ge_qs = GeneralEffluent.objects.none()
    # NEW: Spent Sulphuric Acid Storage Tank
    elif ("spent" in name and "sulphuric" in name):
        eq_qs = EffluentQty.objects.filter(
            effluent_record__record_date=d,
            effluent_nature__in=[
                "Sulphuric below 50 % effluent",
                "Sulphuric above 50% effluent",
            ],
        )
        ge_qs = GeneralEffluent.objects.none()
        
    elif "basic" in name:
        eq_qs = EffluentQty.objects.filter(
            effluent_record__record_date=d,
            effluent_nature__in=["Scrubber Basic Effluent", "Basic"],
        )
        ge_qs = GeneralEffluent.objects.filter(
            record_date=d,
            effluent_nature__in=[
                "Ejector effluent",
                "Outside Drainage Water",
                "PCO Cleaning / cleaning Effluent",
                "Scrubber Basic Effluent",
                "Basic",
                "Dyke Effluent",
                "QC effluent",
            ],
        )

    elif "acidic" in name:
        eq_qs = EffluentQty.objects.filter(
            effluent_record__record_date=d,
            effluent_nature__in=["Acidic", "Acidic Aq. Layer"],
        )
        ge_qs = GeneralEffluent.objects.filter(
            record_date=d,
            effluent_nature__in=["Scrubber Acidic Effluent"],
        )

    else:
        eq_qs = EffluentQty.objects.none()
        ge_qs = GeneralEffluent.objects.none()

    eq_total = eq_qs.aggregate(s=Sum("actual_quantity"))["s"] or 0.0
    ge_total = ge_qs.aggregate(s=Sum("actual_quantity"))["s"] or 0.0
    return Decimal(str(eq_total + ge_total))

def fetch_consume_for(tank: EffluentTank, d: date) -> Decimal:
    """
    Consume Effluent rules (sum of effluent_neutralized on the date):
      - Acidic tanks:  ['Scrubber Acidic Effluent','Acidic Aq. Layer']
      - Basic tanks:   ['Basic','Ejector effluent']
      - Cyanide tank:  ['Sodium Cyanide Effluent']
      - Others:        0 (until defined)
    """
    name = (tank.name or "").strip().lower()

    if "acidic" in name:
        total = PrimaryTreatmentEffluent.objects.filter(
            date=d,
            effluent_nature__in=["Scrubber Acidic Effluent", "Acidic Aq. Layer"],
        ).aggregate(s=Sum("effluent_neutralized"))["s"] or 0

    elif "basic" in name:
        total = PrimaryTreatmentEffluent.objects.filter(
            date=d,
            effluent_nature__in=["Basic", "Ejector effluent"],
        ).aggregate(s=Sum("effluent_neutralized"))["s"] or 0

    elif "cyanide effluent" in name:
        total = PrimaryTreatmentEffluent.objects.filter(
            date=d,
            effluent_nature__in=["Sodium Cyanide Effluent"],
        ).aggregate(s=Sum("effluent_neutralized"))["s"] or 0

    else:
        total = 0

    return Decimal(str(total))

def build_effluent_report_range(fr: date, to: date, tank_ids: Optional[List[int]] = None):
    days = list(_days_between(fr, to))
    tanks = EffluentTank.objects.all()
    if tank_ids:
        tanks = tanks.filter(id__in=tank_ids)

    rows = []
    for tank in tanks.order_by("name"):
        # Opening for the range = month opening on 1st of FROM month
        ob_rec = EffluentOpeningBalance.objects.filter(
            tank=tank, month=_month_first(fr)
        ).first()
        opening = ob_rec.opening_balance if ob_rec else Decimal("0")

        cells = []
        for d in days:
            inlet   = fetch_inlet_for(tank, d)
            consume = fetch_consume_for(tank, d)

            # Closing balance for the day
            closing = (opening + inlet) - consume

            # Available space = Capacity - Closing Balance
            avail_space = (tank.capacity or Decimal("0")) - closing

            cells.append({
                "date": d,
                "opening": opening,
                "inlet": inlet,
                "consume": consume,
                "closing": closing,
                "available_space": avail_space,
            })

            # carry forward
            opening = closing

        rows.append({"tank": tank, "capacity": tank.capacity, "cells": cells})

    return {"from": fr, "to": to, "rows": rows}
