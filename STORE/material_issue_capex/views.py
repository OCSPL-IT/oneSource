from decimal import Decimal
import logging
from io import BytesIO
from django.contrib import messages
from django.views.decorators.http import require_POST
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db.models import (Sum, Max, Value, DecimalField, OuterRef, Subquery, Exists,F,ExpressionWrapper,TextField, CharField,Case, When,)
from django.db.models.functions import Coalesce, Cast
from django.http import HttpResponse
from django.shortcuts import render, redirect
from django.utils.http import urlencode
from django.db.models import Case, When
import xlsxwriter
from .models import *
from .services import * 
import locale

logger = logging.getLogger("custom_logger")  


def _apply_filters(request, capex_qs, mi_qs, transfer_qs):
    """
    Apply common filters to CAPEX GRN, Material Issue, and Transfer querysets.
    Returns: (capex_qs, mi_qs, transfer_qs, filters_dict, base_qs_string)
    """
    batch_no = (request.GET.get("batch_no") or "").strip()
    virtual_location = (request.GET.get("virtual_location") or "").strip()
    item_code = (request.GET.get("item_code") or "").strip()
    item_name = (request.GET.get("item_name") or "").strip()
    location = (request.GET.get("location") or "").strip()

    if batch_no:
        capex_qs = capex_qs.filter(batch_no__icontains=batch_no)
        mi_qs = mi_qs.filter(batch_no__icontains=batch_no)
        transfer_qs = transfer_qs.filter(batch_no__icontains=batch_no)

    if virtual_location:
        capex_qs = capex_qs.filter(virtual_location__icontains=virtual_location)
        mi_qs = mi_qs.filter(virtual_location__icontains=virtual_location)
        transfer_qs = transfer_qs.filter(virtual_location__icontains=virtual_location)  # ? ADDED

    if item_code:
        capex_qs = capex_qs.filter(item_code__icontains=item_code)
        mi_qs = mi_qs.filter(item_code__icontains=item_code)
        transfer_qs = transfer_qs.filter(item_code__icontains=item_code)

    if item_name:
        capex_qs = capex_qs.filter(item_name__icontains=item_name)
        mi_qs = mi_qs.filter(item_name__icontains=item_name)
        transfer_qs = transfer_qs.filter(item_name__icontains=item_name)

    if location:
        capex_qs = capex_qs.filter(location__icontains=location)
        mi_qs = mi_qs.filter(location_from__icontains=location)
        transfer_qs = transfer_qs.filter(location__icontains=location)
  
    filters = {
        "batch_no": batch_no,
        "virtual_location": virtual_location,
        "item_code": item_code,
        "item_name": item_name,
        "location": location,
    }

    base_qs = urlencode({k: v for k, v in filters.items() if v})
    return capex_qs, mi_qs, transfer_qs, filters, base_qs



@login_required
def capex_mi_dashboard(request):
    """
    Dashboard view:
      - Cards: CAPEX GRN Qty, Location Transfer Qty, Material Issue Qty, Closing Qty, CAPEX GRN Total
      - Detail table on (batch_no, virtual_location, item_code),
        including rows that exist only in CAPEX or only in MI.

    ALSO pulls Transfer Stock data from LocationStockTransferCapex:
      - to_location
      - transfer_qty (sum)
      - transfer_virtual_location

    FIX:
      - Show transfer rows even when CAPEX/MI key doesn't match (transfer-only rows)
      - Card "Location Transfer Qty" should come directly from filtered transfer_qs
    """
    if not request.user.has_perm("material_issue_capex.view_capexgrnline"):
        messages.error(request, "You do not have permission to view CAPEX vs Material Issue dashboard.")
        logger.warning("User '%s' tried to access CAPEX vs MI dashboard without permission.", request.user.username)
        return redirect("indexpage")

    virtual_locations = (
        CapexGrnLine.objects
        .exclude(virtual_location__isnull=True)
        .exclude(virtual_location__in=["", "-", " "])
        .values_list("virtual_location", flat=True)
        .distinct()
        .order_by("virtual_location")
    )

    capex_qs = CapexGrnLine.objects.all()
    mi_qs = MaterialIssueLine.objects.all()
    transfer_qs = LocationStockTransferCapex.objects.all()

    # ? filters applied to all 3 querysets here
    capex_qs, mi_qs, transfer_qs, filters, base_qs = _apply_filters(request, capex_qs, mi_qs, transfer_qs)

    # ------------------------------------------------------------------
    # Subquery: total MI qty for each CAPEX row (join keys)
    # ------------------------------------------------------------------
    mi_sub = (
        mi_qs.filter(
            item_code=OuterRef("item_code"),
            batch_no=OuterRef("batch_no"),
            virtual_location=OuterRef("virtual_location"),
        )
        .values("item_code")
        .annotate(total_qty=Sum("quantity"))
        .values("total_qty")
    )

    # ------------------------------------------------------------------
    # Subquery: transfer info for each key (join keys)
    # ------------------------------------------------------------------
    transfer_base = (
        transfer_qs.filter(
            item_code=OuterRef("item_code"),
            batch_no=OuterRef("batch_no"),
            virtual_location=OuterRef("virtual_location"),
        )
        .values("item_code")
        .annotate(
            total_transfer_qty=Sum("transfer_quantity"),
            to_loc=Max("to_location"),
            from_loc=Max("location"),
            transfer_vloc=Max("virtual_location"),
        )
    )
    transfer_qty_sub = transfer_base.values("total_transfer_qty")
    transfer_to_loc_sub = transfer_base.values("to_loc")
    transfer_from_loc_sub = transfer_base.values("from_loc")
    transfer_vloc_sub = transfer_base.values("transfer_vloc")

    # ------------------------------------------------------------------
    # CAPEX side
    # ------------------------------------------------------------------
    capex_detail = (
        capex_qs.values("item_code", "item_name", "batch_no", "location", "virtual_location")
        .annotate(
            capex_doc_no=Max("doc_no"),
            capex_grn_qty=Coalesce(
                Sum("quantity"),
                Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
            ),
            rate=Max("rate"),
            total_amount=Coalesce(
                Sum("total_amount"),
                Value(Decimal("0.00"), output_field=DecimalField(max_digits=18, decimal_places=2)),
            ),
            material_issue_qty=Coalesce(
                Subquery(mi_sub[:1]),
                Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
            ),
            mi_doc_no=Value("-", output_field=CharField()),

            # Transfer columns (matched keys only)
            to_location=Coalesce(Subquery(transfer_to_loc_sub[:1]), Value("-", output_field=CharField())),
            transfer_qty=Coalesce(
                Subquery(transfer_qty_sub[:1]),
                Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
            ),
        )
        .annotate(
            closing_qty=Case(
                When(
                    capex_grn_qty__lt=F("material_issue_qty"),
                    then=Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
                ),
                default=ExpressionWrapper(
                    F("capex_grn_qty") - F("material_issue_qty"),
                    output_field=DecimalField(max_digits=18, decimal_places=3),
                ),
                output_field=DecimalField(max_digits=18, decimal_places=3),
            )
        )
    )

    # ------------------------------------------------------------------
    # MI-only keys
    # ------------------------------------------------------------------
    capex_exists_sub = CapexGrnLine.objects.filter(
        item_code=OuterRef("item_code"),
        batch_no=OuterRef("batch_no"),
        virtual_location=OuterRef("virtual_location"),
    )

    mi_only = (
        mi_qs.annotate(has_capex=Exists(capex_exists_sub))
        .filter(has_capex=False)
        .values("item_code", "item_name", "batch_no", "virtual_location")
        .annotate(
            location=Max("location_from"),
            capex_doc_no=Value("-", output_field=CharField()),
            mi_doc_no=Max("doc_no"),
            capex_grn_qty=Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
            rate=Value(Decimal("0.0000"), output_field=DecimalField(max_digits=18, decimal_places=4)),
            total_amount=Value(Decimal("0.00"), output_field=DecimalField(max_digits=18, decimal_places=2)),
            material_issue_qty=Coalesce(
                Sum("quantity"),
                Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
            ),

            # Transfer columns (matched keys only)
            to_location=Coalesce(Subquery(transfer_to_loc_sub[:1]), Value("-", output_field=CharField())),
            transfer_qty=Coalesce(
                Subquery(transfer_qty_sub[:1]),
                Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
            ),  
        )
        .annotate(
            closing_qty=Case(
                When(
                    capex_grn_qty__lt=F("material_issue_qty"),
                    then=Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
                ),
                default=ExpressionWrapper(
                    F("capex_grn_qty") - F("material_issue_qty"),
                    output_field=DecimalField(max_digits=18, decimal_places=3),
                ),
                output_field=DecimalField(max_digits=18, decimal_places=3),
            )
        )
    )

    # ------------------------------------------------------------------
    # ? Transfer-only rows (so transfer shows even when CAPEX/MI batch doesn't match)
    # join key for "exist" check: item_code + batch_no + virtual_location
    # ------------------------------------------------------------------
    capex_key_exists = CapexGrnLine.objects.filter(
        item_code=OuterRef("item_code"),
        batch_no=OuterRef("batch_no"),
        virtual_location=OuterRef("virtual_location"),
    )
    mi_key_exists = MaterialIssueLine.objects.filter(
        item_code=OuterRef("item_code"),
        batch_no=OuterRef("batch_no"),
        virtual_location=OuterRef("virtual_location"),
    )

    transfer_only = (
    transfer_qs
    .annotate(has_capex=Exists(capex_key_exists))
    .annotate(has_mi=Exists(mi_key_exists))
    .filter(has_capex=False, has_mi=False)
    .values("item_code", "item_name", "batch_no", "virtual_location")
    .annotate(
        location=Max("location"),
        to_location=Max("to_location"),
        transfer_qty=Coalesce(
            Sum("transfer_quantity"),
            Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
        ),

            capex_doc_no=Value("-", output_field=CharField()),
            mi_doc_no=Value("-", output_field=CharField()),
            capex_grn_qty=Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
            material_issue_qty=Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
            closing_qty=Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
            rate=Value(Decimal("0.0000"), output_field=DecimalField(max_digits=18, decimal_places=4)),
            total_amount=Value(Decimal("0.00"), output_field=DecimalField(max_digits=18, decimal_places=2)),
        )
    )

    base_fields = [
        "item_code", "item_name", "batch_no", "location", "virtual_location",
        "to_location", "transfer_qty",
        "capex_doc_no", "mi_doc_no",
        "capex_grn_qty", "material_issue_qty", "closing_qty",
        "rate", "total_amount",
    ]

    capex_detail = capex_detail.values(*base_fields)
    mi_only = mi_only.values(*base_fields)
    transfer_only = transfer_only.values(*base_fields)

    # ? FULL OUTER style union includes transfer-only now
    detail_qs = capex_detail.union(mi_only, transfer_only, all=True).order_by(
        "item_code", "batch_no", "location", "virtual_location"
    )

    # ------------------------------------------------------------------
    # Cards totals
    # ------------------------------------------------------------------
    capex_totals = capex_detail.aggregate(
        capex_qty=Coalesce(Sum("capex_grn_qty"), Value(Decimal("0.000"))),
        capex_total=Coalesce(Sum("total_amount"), Value(Decimal("0.00"))),
        mi_qty_capex_side=Coalesce(Sum("material_issue_qty"), Value(Decimal("0.000"))),
        closing_qty_capex_side=Coalesce(Sum("closing_qty"), Value(Decimal("0.000"))),
    )

    mi_only_totals = mi_only.aggregate(
        mi_qty_only=Coalesce(Sum("material_issue_qty"), Value(Decimal("0.000"))),
        closing_qty_only=Coalesce(Sum("closing_qty"), Value(Decimal("0.000"))),
    )

    capex_qty = capex_totals["capex_qty"]
    capex_total = capex_totals["capex_total"]
    mi_qty = capex_totals["mi_qty_capex_side"] + mi_only_totals["mi_qty_only"]
    closing_qty_total = capex_totals["closing_qty_capex_side"] + mi_only_totals["closing_qty_only"]

    # ? IMPORTANT: transfer card total from filtered transfer_qs directly
    transfer_card_total = transfer_qs.aggregate(
        total=Coalesce(Sum("transfer_quantity"), Value(Decimal("0.000")))
    )["total"]

    # Pagination
    paginator = Paginator(detail_qs, 50)
    page_number = request.GET.get("page") or 1
    page_obj = paginator.get_page(page_number)

    locale.setlocale(locale.LC_ALL, "English_India.1252")

    context = {
        "filters": filters,
        "base_qs": base_qs,
        "cards": {
            "capex_qty": locale.format_string("%.3f", capex_qty, grouping=True),
            "location_transfer_qty": locale.format_string("%.3f", transfer_card_total, grouping=True),
            "mi_qty": locale.format_string("%.3f", mi_qty, grouping=True),
            "closing_qty": locale.format_string("%.3f", closing_qty_total, grouping=True),
            "capex_total": locale.format_string("%.2f", capex_total, grouping=True),
        },
        "paginator": paginator,
        "page_obj": page_obj,
        "rows": page_obj.object_list,
        "virtual_locations": virtual_locations,
    }
    return render(request, "material_issue_capex/capex_mi_dashboard.html", context)




@login_required
def capex_mi_export_excel(request):
    """
    Excel download for the same detail table.

    Closing Qty rule:
      - Never show negative closing.
      - closing_qty = max(capex_grn_qty - material_issue_qty, 0)

    Includes Doc Nos:
      - capex_doc_no (MAX)
      - mi_doc_no (MAX) for MI-only rows, '-' elsewhere
    """
    # ---- Permission check ----
    if not request.user.has_perm("material_issue_capex.view_capexgrnline"):
        messages.error(
            request,
            "You do not have permission to export CAPEX vs Material Issue report.",
        )
        logger.warning(
            "User '%s' tried to export CAPEX vs MI Excel without permission.",
            request.user.username,
        )
        return redirect("indexpage")

    capex_qs = CapexGrnLine.objects.all()
    mi_qs = MaterialIssueLine.objects.all()
    transfer_qs = LocationStockTransferCapex.objects.all()
    capex_qs, mi_qs, transfer_qs, filters, base_qs = _apply_filters(
        request, capex_qs, mi_qs, transfer_qs
    )
    logger.info(
        "User=%s exporting CAPEX vs MI Excel | batch_no='%s' virtual_location='%s' item_code='%s' item_name='%s'",
        request.user.username,
        filters["batch_no"],
        filters["location"],
        filters["virtual_location"],
        filters["item_code"],
        filters["item_name"],
    )

    # ---- Build the same FULL OUTER JOIN detail_qs as dashboard ----

    mi_sub = (
        mi_qs.filter(
            item_code=OuterRef("item_code"),
            batch_no=OuterRef("batch_no"),
            virtual_location=OuterRef("virtual_location"),
        )
        .values("item_code")
        .annotate(total_qty=Sum("quantity"))
        .values("total_qty")
    )

    capex_detail = (
        capex_qs.values("item_code", "item_name", "batch_no","location","virtual_location",)
        .annotate(
            to_location=Value("-", output_field=CharField()),
            capex_doc_no=Max("doc_no"),
            mi_doc_no=Value("-", output_field=CharField()),
            capex_grn_qty=Coalesce(
                Sum("quantity"),
                Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
            ),
            rate=Max("rate"),
            total_amount=Coalesce(
                Sum("total_amount"),
                Value(Decimal("0.00"), output_field=DecimalField(max_digits=18, decimal_places=2)),
            ),
            material_issue_qty=Coalesce(
                Subquery(mi_sub[:1]),
                Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
            ),
        )
        .annotate(
            # ? closing_qty = max(capex - mi, 0)
            closing_qty=Case(
                When(
                    capex_grn_qty__lt=F("material_issue_qty"),
                    then=Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
                ),
                default=ExpressionWrapper(
                    F("capex_grn_qty") - F("material_issue_qty"),
                    output_field=DecimalField(max_digits=18, decimal_places=3),
                ),
                output_field=DecimalField(max_digits=18, decimal_places=3),
            )
        )
    )

    capex_exists_sub = CapexGrnLine.objects.filter(
        item_code=OuterRef("item_code"),
        batch_no=OuterRef("batch_no"),
        virtual_location=OuterRef("virtual_location"),
    )

    mi_only = (
        mi_qs.annotate(has_capex=Exists(capex_exists_sub))
        .filter(has_capex=False)
        .values("item_code", "item_name", "batch_no", "virtual_location")
        .annotate(
            location=Max("location_from"),
            to_location=Max("location_from"),
            capex_doc_no=Value("-", output_field=CharField()),
            mi_doc_no=Max("doc_no"),
            capex_grn_qty=Value(
                Decimal("0.000"),
                output_field=DecimalField(max_digits=18, decimal_places=3),
            ),
            rate=Value(
                Decimal("0.0000"),
                output_field=DecimalField(max_digits=18, decimal_places=4),
            ),
            total_amount=Value(
                Decimal("0.00"),
                output_field=DecimalField(max_digits=18, decimal_places=2),
            ),
            material_issue_qty=Coalesce(
                Sum("quantity"),
                Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
            ),
        )
        .annotate(
            # ? closing_qty = max(capex - mi, 0)  (capex=0 here so always 0)
            closing_qty=Case(
                When(
                    capex_grn_qty__lt=F("material_issue_qty"),
                    then=Value(Decimal("0.000"), output_field=DecimalField(max_digits=18, decimal_places=3)),
                ),
                default=ExpressionWrapper(
                    F("capex_grn_qty") - F("material_issue_qty"),
                    output_field=DecimalField(max_digits=18, decimal_places=3),
                ),
                output_field=DecimalField(max_digits=18, decimal_places=3),
            )
        )
    )

    base_fields = [
        "batch_no",
        "location",
        "virtual_location",
        "to_location",
        "item_code",
        "item_name",
        "capex_grn_qty",
        "material_issue_qty",
        "closing_qty",
        "rate",
        "total_amount",
        "capex_doc_no",
        "mi_doc_no",
    ]

    capex_detail = capex_detail.values(*base_fields)
    mi_only = mi_only.values(*base_fields)

    detail_qs = capex_detail.union(mi_only, all=True).order_by(
        "item_code", "batch_no","location","virtual_location"
    )

    total_rows = detail_qs.count()
    logger.info("CAPEX vs MI Excel rows | user=%s rows=%s", request.user.username, total_rows)

    # ---- Build Excel in-memory ----
    output = BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    ws = workbook.add_worksheet("CAPEX vs MI")

    title_format = workbook.add_format({"bold": True, "font_size": 14, "align": "left"})
    header_format = workbook.add_format({"bold": True, "bg_color": "#EEF2FF", "border": 1})
    num_format_qty = workbook.add_format({"num_format": "0.000"})
    num_format_rate = workbook.add_format({"num_format": "0.0000"})
    num_format_amt = workbook.add_format({"num_format": "0.00"})

    # ? Title (11 columns => A..K)
    ws.merge_range("A1:L1", "CAPEX GRN vs Material Issue", title_format)
    row = 2

    headers = [
        "Batch No",
        "location",
        "Virtual Location",
        "To Location",
        "Item Code",
        "Item Name",
        "Capex GRN Qty",
        "Material Issue Qty",
        "Closing Qty",
        "Rate",
        "Total Amount",
        "CAPEX Doc No",
        "MI Doc No",
    ]
    for col, h in enumerate(headers):
        ws.write(row, col, h, header_format)
    row += 1

    for r in detail_qs:
        for col, field in enumerate(base_fields):
            value = r.get(field)

            if field in ["capex_grn_qty", "material_issue_qty", "closing_qty"]:
                ws.write_number(row, col, float(value or 0), num_format_qty)

            elif field == "rate":
                ws.write_number(row, col, float(value or 0), num_format_rate)

            elif field == "total_amount":
                ws.write_number(row, col, float(value or 0), num_format_amt)

            else:
                ws.write(row, col, value or "")

        row += 1

    workbook.close()
    output.seek(0)

    filename = "capex_vs_material_issue.xlsx"
    resp = HttpResponse(
        output.read(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp





@require_POST
@login_required
def capex_mi_sync_erp(request):
    """
    Run syncs:
      - CAPEX GRN lines
      - Material Issue lines
      - Location Stock Transfer CAPEX lines
    and then redirect back to the dashboard.
    """
    # Optional restriction:
    # if not request.user.is_superuser:
    #     messages.error(request, "You are not allowed to run ERP sync.")
    #     return redirect("material_issue_capex:capex_mi_dashboard")

    try:
        inserted_capex = rebuild_capex_grn_lines()
        inserted_mi = rebuild_material_issue_lines()
        inserted_ltc = rebuild_location_stock_transfer_capex_lines()

        messages.success(
            request,
            "ERP sync completed. "
            f"CAPEX GRN rows: {inserted_capex}, "
            f"Material Issue rows: {inserted_mi}, "
            f"Location Transfer CAPEX rows: {inserted_ltc}."
        )

        logger.info(
            "ERP sync done by %s (CapexGRN=%s, MI=%s, LocTransferCapex=%s)",
            request.user.username,
            inserted_capex,
            inserted_mi,
            inserted_ltc,
        )
    except Exception as exc:
        logger.exception("Error during ERP sync (CAPEX GRN / Material Issue / Location Transfer CAPEX)")
        messages.error(request, f"ERP sync failed: {exc}")

    return redirect("material_issue_capex:capex_mi_dashboard")