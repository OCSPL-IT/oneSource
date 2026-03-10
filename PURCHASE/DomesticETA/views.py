# views.py

from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.http import  Http404, HttpResponse
from .forms import *
from .models import *
from django.core.paginator import Paginator
from django.contrib import messages
from django.db import connections
from decimal import Decimal, InvalidOperation
import logging
import io
import xlsxwriter
from django.db.models import Count, Q,Sum
from django.http import JsonResponse
from datetime import datetime
from django.db.models.functions import Coalesce
from django.db.models import DecimalField






logger = logging.getLogger("custom_logger")


def fetch_domestic_eta_po_data(po_number: str | None = None):
    """
    Fetch PO data from ERP for Domestic ETA.

    Mappings:
        PoNumber        = Purchase Order No (HDR.sDocNo)
        RequiredDate    = Required By Date  (txncf, header/line 'Required By Date')
        RawMaterial     = Item Name         (ITMMST.sName)
        Qty             = Quantity          (TXNDET.dQty)
        Supplier        = Supplier Name – Billing (BUSMST lAccId1)
        TransporterName = Transporter Name  (BUSMST lAccId3)
        Packing         = Item Narration    (TXNDET.sNarr)

    Returns a list of rows (one dict per PO line) for the given PO number.
    """

    sql = r"""
        SELECT
            LTRIM(RTRIM(HDR.sDocNo)) AS PoNumber,
            SUPP.sName               AS Supplier,
            TRANS.sName              AS TransporterName,
            ITM.sName                AS RawMaterial,
            DET.sNarr                AS Packing,          -- Item Narration
            CAST(DET.dQty AS decimal(18,2)) AS Qty,
            TRY_CONVERT(
                date,
                (
                    SELECT TOP (1) sValue
                    FROM txncf
                    WHERE lid   = HDR.lId
                      AND sName = 'Required By Date'
                      AND lLine = DET.lLine
                ),
                106
            ) AS RequiredDate
        FROM TXNHDR HDR
        INNER JOIN TXNTYP AS TYP       ON TYP.lTypId  = HDR.lTypId
        LEFT  JOIN BUSMST AS SUPP      ON HDR.lAccId1 = SUPP.lId
        LEFT  JOIN BUSMST AS SUPP_SHIP ON HDR.lAccId2 = SUPP_SHIP.lId
        LEFT  JOIN BUSMST AS TRANS     ON HDR.lAccId3 = TRANS.lId
        INNER JOIN TXNDET AS DET       ON HDR.lId     = DET.lId
        INNER JOIN ITMMST AS ITM       ON DET.lItmId  = ITM.lId
        LEFT  JOIN itmcf  AS icf       ON DET.lItmId  = icf.lId  AND icf.sname  = 'Inventory Category'
        LEFT  JOIN itmcf  AS icf1      ON DET.lItmId  = icf1.lId AND icf1.sname = 'Inventory Sub Category'
        INNER JOIN ITMTYP AS ITP       ON ITP.lTypid  = DET.lItmtyp
        INNER JOIN UNTMST AS UOM       ON DET.lUntId  = UOM.lId
        INNER JOIN UNTMST AS UOM2      ON DET.lUntId2 = UOM2.lId
        INNER JOIN CURMST AS CUR       ON CUR.lid     = HDR.lCurrId
        LEFT  OUTER JOIN HSNMST AS HSN ON HSN.lid     = DET.lHSNid
        WHERE
            HDR.lTypId IN (
                400, 509, 520, 524, 750, 751, 752, 753, 754, 755, 756, 757,
                758, 759, 760, 761, 762, 763, 764, 765, 766, 767, 768, 769, 956,547
            )
            AND HDR.sDocNo = %s
        ORDER BY
            HDR.sDocNo,
            DET.lLine;
    """

    with connections["readonly_db"].cursor() as cursor:
        cursor.execute(sql, [po_number])
        rows = cursor.fetchall()
        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, r)) for r in rows]



def get_transporter_names():
    """
    Fetch distinct transporter names from BUSMST in ERP (readonly_db).
    """
    sql = """
    SELECT DISTINCT sName
    FROM BUSMST WITH (NOLOCK)
    WHERE sName LIKE '%TRANSPORT%'
       OR sName LIKE '%ROADLINE%'
       OR sName LIKE '%TRANS%'
       OR sName LIKE '%CARGO%'
       OR sName LIKE '%LOGI%'
    ORDER BY sName;
    """
    with connections["readonly_db"].cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()
    # return simple list of strings, trimmed
    return [r[0].strip() for r in rows if r[0]]




@login_required
def domestic_eta_create(request):
    # ---- Permission check ----
    if not request.user.has_perm("DomesticETA.add_domesticetatracking"):
        messages.error(request, "You do not have permission to add Domestic ETA records.")
        logger.warning("User '%s' tried to create Domestic ETA without permission.",request.user.username,)
        return redirect("indexpage")

    transporter_names = get_transporter_names()   # ?? fetch once per request

    logger.info("User=%s accessed Domestic ETA CREATE | method=%s",request.user.username,request.method,)
    if request.method == "POST":
        action = request.POST.get("action", "save")
        logger.info("User=%s submitted Domestic ETA CREATE | action=%s",request.user.username,action,)

        if action == "fetch":
            po = (request.POST.get("PoNumber") or "").strip()
            form = DomesticETATrackingForm()
            logger.info("User=%s requested ERP fetch for Domestic ETA | PoNumber='%s'",request.user.username,po,)
            if not po:
                messages.error(request, "Please enter PO Number before fetching from ERP.")
                logger.warning( "User=%s ERP fetch blocked (missing PoNumber)",request.user.username,)
            else:
                rows = fetch_domestic_eta_po_data(po)
                if not rows:
                    messages.warning(request, f"No ERP data found for PO No. {po}.")
                    logger.info("User=%s ERP fetch returned no rows | PoNumber='%s'",request.user.username,po,)
                else:
                    row = rows[0]
                    qty_value = row.get("Qty")

                    initial = {
                        "PoNumber": row.get("PoNumber") or po,
                        "RequiredDate": (
                            row["RequiredDate"].isoformat()
                            if row.get("RequiredDate") else ""
                        ),
                        "RawMaterial": row.get("RawMaterial") or "",
                        "Packing": row.get("Packing") or "",
                        "Qty": qty_value or "",
                        "Supplier": row.get("Supplier") or "",
                        "TransporterName": row.get("TransporterName") or "",
                    }
                    form = DomesticETATrackingForm(initial=initial)
                    messages.success(request,f"ERP data fetched successfully for PO No. {initial['PoNumber']}.", )
                    #logger.info("User=%s ERP fetch success | PoNumber='%s' | Supplier='%s' | TransporterName='%s'",request.user.username,initial.get("PoNumber"),initial.get("Supplier"),initial.get("TransporterName"),)

            return render(request,"eta/domestic_eta_form.html",{"form": form, "obj": None, "transporter_names": transporter_names},)

        # normal save
        form = DomesticETATrackingForm(request.POST, request.FILES)
        if form.is_valid():
            obj = form.save()
            messages.success(request, f"Domestic ETA record saved successfully for PO No. {obj.PoNumber}.",)
            logger.info("User=%s saved Domestic ETA | id=%s | PoNumber='%s' | Status='%s'",request.user.username,
                getattr(obj, "id", None),getattr(obj, "PoNumber", None),getattr(obj, "Status", None),)
            return redirect("domestic_eta:domestic_eta_list")
        else:
            messages.error(request, "Please correct the errors below.")
            logger.error("DomesticETATrackingForm invalid for user=%s | errors=%s",request.user.username,form.errors.as_json(),)
    else:
        form = DomesticETATrackingForm()

    return render(request,"eta/domestic_eta_form.html",{"form": form, "obj": None, "transporter_names": transporter_names},)



@login_required
def domestic_eta_edit(request, pk):
    # ---- Permission check ----
    if not request.user.has_perm("DomesticETA.change_domesticetatracking"):
        messages.error(request, "You do not have permission to edit Domestic ETA records.")
        logger.warning("User '%s' tried to edit Domestic ETA (pk=%s) without permission.", request.user.username,pk,)
        return redirect("indexpage")

    obj = get_object_or_404(DomesticETATracking, pk=pk)
    transporter_names = get_transporter_names()
    logger.info("User=%s accessed Domestic ETA EDIT | pk=%s | method=%s",request.user.username, pk,request.method,)
    if request.method == "POST":
        action = request.POST.get("action", "save")
        logger.info("User=%s submitted Domestic ETA EDIT | pk=%s | action=%s",request.user.username, pk,action, )
        form = DomesticETATrackingForm(request.POST, request.FILES, instance=obj)
        if action == "fetch":
            po = (request.POST.get("PoNumber") or obj.PoNumber or "").strip()
            logger.info("User=%s requested ERP fetch (EDIT) | pk=%s | PoNumber='%s'",request.user.username,pk,po,)

            if not po:
                messages.error(request, "Please enter PO Number before fetching from ERP.")
                logger.warning("User=%s ERP fetch blocked (missing PoNumber) | pk=%s", request.user.username, pk, )
                return render(request, "eta/domestic_eta_form.html",{"form": form, "obj": obj, "transporter_names": transporter_names},)

            rows = fetch_domestic_eta_po_data(po)
            if not rows:
                messages.warning(request, f"No ERP data found for PO No. {po}.")
                logger.info("User=%s ERP fetch returned no rows (EDIT) | pk=%s | PoNumber='%s'",request.user.username,
                    pk, po,  )
                return render(request,"eta/domestic_eta_form.html",{"form": form, "obj": obj, "transporter_names": transporter_names},)

            row = rows[0]
            qty_value = row.get("Qty")

            initial = {
                "PoNumber": row.get("PoNumber") or po,
                "RequiredDate": (
                    row["RequiredDate"].isoformat()
                    if row.get("RequiredDate") else ""
                ),
                "RawMaterial": row.get("RawMaterial") or "",
                "Packing": row.get("Packing") or "",
                "Qty": qty_value or "",
                "Supplier": row.get("Supplier") or "",
                "TransporterName": row.get("TransporterName") or "",
            }

            form = DomesticETATrackingForm(instance=obj, initial=initial)
            messages.success( request,f"ERP data fetched successfully for PO No. {initial['PoNumber']}.", )
            logger.info("User=%s ERP fetch success (EDIT) | pk=%s | PoNumber='%s' | Supplier='%s' | TransporterName='%s'",
                request.user.username, pk,initial.get("PoNumber"),initial.get("Supplier"),initial.get("TransporterName"), )
            return render(request,"eta/domestic_eta_form.html",{"form": form, "obj": obj, "transporter_names": transporter_names},)

        # normal save
        if form.is_valid():
            obj = form.save()
            messages.success( request,f"Domestic ETA record updated successfully for PO No. {obj.PoNumber}.",)
            logger.info("User=%s updated Domestic ETA | pk=%s | PoNumber='%s' | Status='%s'",
                request.user.username,pk, getattr(obj, "PoNumber", None), getattr(obj, "Status", None),)
            return redirect("domestic_eta:domestic_eta_list")
        else:
            messages.error(request, "Please correct the errors below.")
            logger.error("DomesticETATrackingForm invalid (EDIT) for user=%s | pk=%s | errors=%s",
                request.user.username, pk,form.errors.as_json(), )
    else:
        form = DomesticETATrackingForm(instance=obj)
    return render(request,"eta/domestic_eta_form.html",{"form": form, "obj": obj, "transporter_names": transporter_names},)


@login_required
def domestic_eta_list(request):
    """
    List view for Domestic ETA Tracking with filters:
    PoNumber, Status, RawMaterial, ETDDate range, Supplier, TransporterName
    """
    # ---- Permission check ----
    if not request.user.has_perm("DomesticETA.view_domesticetatracking"):
        messages.error(request, "You do not have permission to view Domestic ETA records.")
        logger.warning(
            "User '%s' tried to view Domestic ETA list without permission.",
            request.user.username,
        )
        return redirect("indexpage")

    logger.info(
        "User=%s accessed Domestic ETA LIST | method=%s",
        request.user.username,
        request.method,
    )

    qs = DomesticETATracking.objects.all().order_by("-RequiredDate", "PoNumber")

    # ---- Filters ----
    po          = (request.GET.get("po") or "").strip()
    status      = (request.GET.get("status") or "").strip()
    material    = (request.GET.get("material") or "").strip()
    etd_from    = (request.GET.get("etd_from") or "").strip()
    etd_to      = (request.GET.get("etd_to") or "").strip()
    supplier    = (request.GET.get("supplier") or "").strip()
    transporter = (request.GET.get("transporter") or "").strip()

    logger.info(
        "Domestic ETA LIST filters by user=%s | po='%s' status='%s' material='%s' "
        "etd_from='%s' etd_to='%s' supplier='%s' transporter='%s'",
        request.user.username,
        po,
        status,
        material,
        etd_from,
        etd_to,
        supplier,
        transporter,
    )

    if po:
        qs = qs.filter(PoNumber__icontains=po)
    if status:
        qs = qs.filter(Status=status)
    if material:
        qs = qs.filter(RawMaterial__icontains=material)
    if etd_from:
        qs = qs.filter(ETDDate__gte=etd_from)
    if etd_to:
        qs = qs.filter(ETDDate__lte=etd_to)
    if supplier:
        qs = qs.filter(Supplier__icontains=supplier)
    if transporter:
        qs = qs.filter(TransporterName__icontains=transporter)

    # ---- Pagination ----
    paginator = Paginator(qs, 50)  # 50 rows per page
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    logger.info(
        "Domestic ETA LIST result by user=%s | rows_total=%s | page=%s | page_rows=%s",
        request.user.username,
        qs.count(),
        page_obj.number,
        len(page_obj.object_list),
    )

    context = {
        "page_obj": page_obj,
        "filters": {
            "po": po,
            "status": status,
            "material": material,
            "etd_from": etd_from,
            "etd_to": etd_to,
            "supplier": supplier,
            "transporter": transporter,
        },
        "status_choices": DomesticETATracking.STATUS_CHOICES,
    }
    return render(request, "eta/domestic_eta_list.html", context)


@login_required
def domestic_eta_detail(request, pk):
    # ---- Permission check ----
    if not request.user.has_perm("DomesticETA.view_domesticetatracking"):
        messages.error(request, "You do not have permission to view Domestic ETA details.")
        logger.warning("User '%s' tried to view Domestic ETA detail (pk=%s) without permission.",request.user.username, pk,)
        return redirect("indexpage")

    logger.info("User=%s accessed Domestic ETA DETAIL | pk=%s",request.user.username, pk, )
    obj = get_object_or_404(DomesticETATracking, pk=pk)
    return render(request, "eta/eta_detail.html", {"obj": obj})



@login_required
def domestic_eta_photo(request, pk):
    """
    Stream the binary photo stored in DomesticETATracking.Photos.
    Assumes uploaded file is an image (change content_type if needed).
    """
    # ---- Permission check ----
    if not request.user.has_perm("DomesticETA.view_domesticetatracking"):
        messages.error(request, "You do not have permission to view Domestic ETA photos.")
        logger.warning("User '%s' tried to view Domestic ETA photo (pk=%s) without permission.",request.user.username,pk,)
        return redirect("indexpage")
    logger.info("User=%s requested Domestic ETA PHOTO | pk=%s",request.user.username, pk,)
    obj = get_object_or_404(DomesticETATracking, pk=pk)

    if not obj.Photos:
        logger.info("Domestic ETA PHOTO not found | user=%s | pk=%s | PoNumber='%s'",request.user.username,
            pk,getattr(obj, "PoNumber", ""),)
        raise Http404("No photo uploaded for this record.")

    # If you may store PNG/PDF, switch this to application/octet-stream
    response = HttpResponse(obj.Photos, content_type="image/jpeg")
    response["Content-Disposition"] = (f'inline; filename="eta_photo_{obj.PoNumber}.jpg"')

    logger.info("Domestic ETA PHOTO served | user=%s | pk=%s | PoNumber='%s'", request.user.username,
        pk,getattr(obj, "PoNumber", ""),)
    return response


@login_required
def domestic_eta_delete(request, pk):
    """
    Delete a Domestic ETA record after confirmation.
    """
    obj = get_object_or_404(DomesticETATracking, pk=pk)

    # permission check
    if not request.user.has_perm("domestic_eta.delete_domesticetatracking"):
        messages.error(request, "You do not have permission to delete Domestic ETA records.")
        logger.warning(
            "User='%s' tried to DELETE Domestic ETA id=%s (PO=%s) without permission.",
            request.user.username,
            obj.pk,
            obj.PoNumber,
        )
        return redirect("domestic_eta:domestic_eta_list")

    if request.method == "POST":
        po_no = obj.PoNumber
        obj.delete()
        messages.success(
            request,
            f"Domestic ETA record for PO No. {po_no} deleted successfully.",
        )
        logger.info(
            "User='%s' deleted Domestic ETA id=%s (PO=%s).",
            request.user.username,
            pk,
            po_no,
        )
        return redirect("domestic_eta:domestic_eta_list")

    # GET => show confirm page
    return render(
        request,
        "eta/domestic_eta_confirm_delete.html",
        {"obj": obj},
    )




@login_required
def domestic_eta_export_excel(request):
    # ---- Permission check ----
    if not request.user.has_perm("DomesticETA.view_domesticetatracking"):
        messages.error(request, "You do not have permission to export Domestic ETA records.")
        logger.warning(
            "User='%s' tried to export Domestic ETA Excel without permission.",
            request.user.username,
        )
        return redirect("indexpage")
    qs = DomesticETATracking.objects.all().order_by("-RequiredDate", "PoNumber")
    # ---- Same filters as list page ----
    po          = (request.GET.get("po") or "").strip()
    status      = (request.GET.get("status") or "").strip()
    material    = (request.GET.get("material") or "").strip()
    etd_from    = (request.GET.get("etd_from") or "").strip()
    etd_to      = (request.GET.get("etd_to") or "").strip()
    supplier    = (request.GET.get("supplier") or "").strip()
    transporter = (request.GET.get("transporter") or "").strip()
    if po:
        qs = qs.filter(PoNumber__icontains=po)
    if status:
        qs = qs.filter(Status=status)
    if material:
        qs = qs.filter(RawMaterial__icontains=material)
    if etd_from:
        qs = qs.filter(ETDDate__gte=etd_from)
    if etd_to:
        qs = qs.filter(ETDDate__lte=etd_to)
    if supplier:
        qs = qs.filter(Supplier__icontains=supplier)
    if transporter:
        qs = qs.filter(TransporterName__icontains=transporter)
    logger.info(
        "User='%s' exported Domestic ETA Excel | po='%s' status='%s' material='%s' "
        "etd_from='%s' etd_to='%s' supplier='%s' transporter='%s' | rows=%s",
        request.user.username,
        po, status, material, etd_from, etd_to, supplier, transporter,
        qs.count(),
    )
    # ---- Excel creation ----
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    worksheet = workbook.add_worksheet("Domestic ETA")
    # ---- Styles ----
    title_fmt = workbook.add_format({
        "bold": True,
        "font_size": 14,
        "align": "center",
        "valign": "vcenter",
    })
    header_fmt = workbook.add_format({
        "bold": True,
        "bg_color": "#E5E7EB",  # light gray
        "border": 1,
        "align": "center",
        "valign": "vcenter",
        "text_wrap": True,
    })
    cell_fmt = workbook.add_format({
        "border": 1,
        "valign": "top",
    })
    date_fmt = workbook.add_format({
        "border": 1,
        "num_format": "dd-mm-yyyy",
    })
    # ---- Title Row ----
    headers = [
        "Status",
        "Raw Material",
        "Required Date At Plant",
        "ETD Date",
        "Revised ETA",
        "Qty",
        "Freight Charges",  # ? NEW
        "Packing",
        "Supplier",
        "Lifting Location",
        "Transporter",
        "Vehicle No",
        "Driver No",
        "LR No",
        "PO Number",
        "Evaluation",
        "Photo",
        "Invoice / Remark",
        "Invoice Date",
        "General Remark",
    ]
    worksheet.merge_range(0, 0, 0, len(headers) - 1,
                          "Domestic ETA Tracking Report", title_fmt)
    # ---- Header Row ----
    for col, h in enumerate(headers):
        worksheet.write(1, col, h, header_fmt)
        worksheet.set_column(col, col, 18)
    # ---- Data Rows ----
    row = 2
    for obj in qs:
        col = 0
        # Status
        worksheet.write(row, col, obj.Status, cell_fmt)
        col += 1
        # Raw Material
        worksheet.write(row, col, obj.RawMaterial, cell_fmt)
        col += 1
        # Required Date At Plant
        if obj.RequiredDate:
            worksheet.write_datetime(row, col, obj.RequiredDate, date_fmt)
        else:
            worksheet.write(row, col, "", cell_fmt)
        col += 1
        # ETD Date
        if obj.ETDDate:
            worksheet.write_datetime(row, col, obj.ETDDate, date_fmt)
        else:
            worksheet.write(row, col, "", cell_fmt)
        col += 1
        # Revised ETA
        if obj.RevisedETADate:
            worksheet.write_datetime(row, col, obj.RevisedETADate, date_fmt)
        else:
            worksheet.write(row, col, "", cell_fmt)
        col += 1
        # Qty
        worksheet.write(row, col, float(obj.Qty), cell_fmt)
        col += 1
        # Freight Charges
        worksheet.write(row, col, float(obj.FreightCharges or 0), cell_fmt)
        col += 1
        # Packing
        worksheet.write(row, col, obj.Packing or "", cell_fmt)
        col += 1
        # Supplier
        worksheet.write(row, col, obj.Supplier, cell_fmt)
        col += 1
        # Lifting Location
        worksheet.write(row, col, obj.LiftingLocation or "", cell_fmt)
        col += 1
        # Transporter
        worksheet.write(row, col, obj.TransporterName or "", cell_fmt)
        col += 1
        # Vehicle No
        worksheet.write(row, col, obj.VehicleNo or "", cell_fmt)
        col += 1
        # Driver No
        worksheet.write(row, col, obj.DriverNo or "", cell_fmt)
        col += 1
        # LR No
        worksheet.write(row, col, obj.LRNo or "", cell_fmt)
        col += 1
        # PO Number
        worksheet.write(row, col, obj.PoNumber, cell_fmt)
        col += 1
        # Evaluation
        worksheet.write(row, col, obj.Evaluation or "", cell_fmt)
        col += 1
        # Photo (Yes/No)
        photos_flag = "Yes" if obj.Photos else "No"
        worksheet.write(row, col, photos_flag, cell_fmt)
        col += 1
        # Invoice / Remark
        worksheet.write(row, col, obj.InvoiceNoRemark, cell_fmt)
        col += 1
        # Invoice Date
        if obj.InvoiceDate:
            worksheet.write_datetime(row, col, obj.InvoiceDate, date_fmt)
        else:
            worksheet.write(row, col, "", cell_fmt)
        col += 1
        # Remark (new field)
        worksheet.write(row, col, obj.Remark or "", cell_fmt)
        row += 1
    workbook.close()
    output.seek(0)
    filename = "Domestic_ETA_Tracking.xlsx"
    response = HttpResponse(output.getvalue(),content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",)
    response["Content-Disposition"] = f'attachment; filename=\"{filename}\"'
    return response



# ==========================================================
# MAIN DASHBOARD VIEW
# ==========================================================
def eta_dashboard(request):

    qs = DomesticETATracking.objects.all()

    # ------------------------------------------------------
    # SLICER OPTIONS
    # ------------------------------------------------------
    status_options = qs.values_list("Status", flat=True).distinct()
    supplier_options = qs.values_list("Supplier", flat=True).distinct()
    raw_material_options = qs.values_list("RawMaterial", flat=True).distinct()
    transporter_options = (
        qs.exclude(TransporterName__isnull=True)
          .exclude(TransporterName__exact="")
          .values_list("TransporterName", flat=True)
          .distinct()
    )

    # ------------------------------------------------------
    # STATUS COUNT
    # ------------------------------------------------------
    status_counts = (
        qs.values("Status")
          .annotate(count=Count("id"))
          .order_by()
    )

    # ------------------------------------------------------
    # PENDING INVOICE
    # ------------------------------------------------------
    pending_invoice_count = qs.filter(
        InvoiceNoRemark="Pending"
    ).count()

    # ------------------------------------------------------
    # RAW MATERIAL COUNT
    # ------------------------------------------------------
    raw_material_counts = (
        qs.values("RawMaterial")
          .annotate(count=Count("id"))
          .order_by("-count")
    )

    # ------------------------------------------------------
    # SUPPLIER COUNT
    # ------------------------------------------------------
    supplier_counts = (
        qs.values("Supplier")
          .annotate(count=Count("id"))
          .order_by("-count")
    )

    # ------------------------------------------------------
    # TRANSPORTER COUNT
    # ------------------------------------------------------
    transporter_counts = (
        qs.exclude(TransporterName__isnull=True)
          .exclude(TransporterName__exact="")
          .values("TransporterName")
          .annotate(count=Count("id"))
          .order_by("-count")
    )

    # ------------------------------------------------------
    # ? FIXED FREIGHT SUMMARY
    # ------------------------------------------------------

    freight_sum = qs.aggregate(
        total=Coalesce(
            Sum("FreightCharges"),
            Decimal("0.00"),
            output_field=DecimalField()
        )
    )["total"]

    
    freight_data = qs.exclude(FreightCharges__isnull=True)

    freight_count = freight_data.count()

    if freight_count > 0:
        freight_sum = freight_data.aggregate(
            total=Sum("FreightCharges")
        )["total"]
    else:
        freight_sum = None

    context = {
        "status_counts": status_counts,
        "pending_invoice_count": pending_invoice_count,
        "raw_material_counts": raw_material_counts,
        "supplier_counts": supplier_counts,
        "transporter_counts": transporter_counts,
        "status_options": status_options,
        "supplier_options": supplier_options,
        "raw_material_options": raw_material_options,
        "transporter_options": transporter_options,
        "freight_sum": freight_sum,
        "freight_count": freight_count,
    }

    return render(request, "dashboard/eta_dashboard.html", context)


# ==========================================================
# AJAX DATA ENDPOINT
# ==========================================================
def eta_dashboard_data(request):

    qs = DomesticETATracking.objects.all()

    status = request.GET.get("status")
    supplier = request.GET.get("supplier")
    raw_material = request.GET.get("raw_material")
    transporter = request.GET.get("transporter")
    invoice = request.GET.get("invoice")
    etd_from = request.GET.get("etd_from")
    etd_to = request.GET.get("etd_to")

    if status:
        qs = qs.filter(Status=status)

    if supplier:
        qs = qs.filter(Supplier__icontains=supplier)

    if raw_material:
        qs = qs.filter(RawMaterial__icontains=raw_material)

    if transporter:
        qs = qs.filter(TransporterName__icontains=transporter)

    if invoice == "pending":
        qs = qs.filter(InvoiceNoRemark="Pending")

    if etd_from:
        qs = qs.filter(ETDDate__gte=etd_from)

    if etd_to:
        qs = qs.filter(ETDDate__lte=etd_to)

    # ------------------------------------------------------
    # COUNTS
    # ------------------------------------------------------
    status_counts = list(
        qs.values("Status")
          .annotate(count=Count("id"))
          .order_by()
    )

    pending_invoice = qs.filter(
        InvoiceNoRemark="Pending"
    ).count()

    raw_material_counts = list(
        qs.values("RawMaterial")
          .annotate(count=Count("id"))
          .order_by("-count")
    )

    supplier_counts = list(
        qs.values("Supplier")
          .annotate(count=Count("id"))
          .order_by("-count")
    )

    transporter_counts = list(
        qs.exclude(TransporterName__isnull=True)
          .exclude(TransporterName__exact="")
          .values("TransporterName")
          .annotate(count=Count("id"))
          .order_by("-count")
    )

    # ------------------------------------------------------
    # ? FIXED FREIGHT FOR AJAX
    # ------------------------------------------------------
    freight_sum = qs.aggregate(
        total=Coalesce(
            Sum("FreightCharges"),
            Decimal("0.00"),
            output_field=DecimalField()
        )
    )["total"]

    freight_count = qs.exclude(FreightCharges__isnull=True).count()

    # ------------------------------------------------------
    # DETAIL TABLE
    # ------------------------------------------------------
    detail_qs = qs.order_by("-created_at")[:500]

    detail_table = list(detail_qs.values(
        "PoNumber",
        "Status",
        "RequiredDate",
        "ETDDate",
        "RevisedETADate",
        "RawMaterial",
        "Packing",
        "Qty",
        "FreightCharges",
        "Supplier",
        "LiftingLocation",
        "TransporterName",
        "VehicleNo",
        "LRNo",
        "DriverNo",
        "Evaluation",
        "InvoiceNoRemark",
        "InvoiceDate",
        "Remark",
        "created_at",
    ))

    return JsonResponse({
        "status_counts": status_counts,
        "pending_invoice": pending_invoice,
        "raw_material_counts": raw_material_counts,
        "supplier_counts": supplier_counts,
        "transporter_counts": transporter_counts,
        "freight_sum": float(freight_sum),
        "freight_count": freight_count,
        "detail_table": detail_table,
    })

@login_required 
def eta_dashboard_export_excel(request):
    # ---- Permission check ----
    if not request.user.has_perm("DomesticETA.view_domesticetatracking"):
        messages.error(request, "You do not have permission to export Domestic ETA records.")
        logger.warning("User='%s' tried to export Domestic ETA Excel without permission.", request.user.username)
        return redirect("indexpage")

    # ---- Base queryset ----
    qs = DomesticETATracking.objects.all().order_by("-created_at")

    # ---- Filters from GET ----
    status = (request.GET.get("status") or "").strip()
    supplier = (request.GET.get("supplier") or "").strip()
    raw_material = (request.GET.get("raw_material") or "").strip()
    transporter = (request.GET.get("transporter") or "").strip()
    invoice = (request.GET.get("invoice") or "").strip()
    etd_from = (request.GET.get("etd_from") or "").strip()
    etd_to = (request.GET.get("etd_to") or "").strip()

    if status:
        qs = qs.filter(Status=status)
    if supplier:
        qs = qs.filter(Supplier__icontains=supplier)
    if raw_material:
        qs = qs.filter(RawMaterial__icontains=raw_material)
    if transporter:
        qs = qs.filter(TransporterName__icontains=transporter)
    if invoice == "pending":
        qs = qs.filter(InvoiceNoRemark="Pending")
    if etd_from:
        qs = qs.filter(ETDDate__gte=etd_from)
    if etd_to:
        qs = qs.filter(ETDDate__lte=etd_to)

    logger.info(
        "User='%s' exported ETA Excel | status='%s' supplier='%s' raw_material='%s' transporter='%s' invoice='%s' etd_from='%s' etd_to='%s' | rows=%s",
        request.user.username,
        status, supplier, raw_material, transporter, invoice, etd_from, etd_to,
        qs.count(),
    )

    # ---- Create Excel ----
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    worksheet = workbook.add_worksheet("Domestic ETA")

    # ---- Formats ----
    title_fmt = workbook.add_format({"bold": True, "font_size": 14, "align": "center", "valign": "vcenter"})
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#E5E7EB", "border": 1, "align": "center", "valign": "vcenter", "text_wrap": True})
    cell_fmt = workbook.add_format({"border": 1, "valign": "top"})
    date_fmt = workbook.add_format({"border": 1, "num_format": "dd-mm-yyyy"})

    # ---- Headers (ETD at start) ----
    headers = [
        "ETD Date",
        "Status",
        "Raw Material",
        "Required Date",
        "Revised ETA",
        "Qty",
        "Freight Charges",  # ? NEW
        "Packing",
        "Supplier",
        "Lifting Location",
        "Transporter",
        "Vehicle No",
        "Driver No",
        "LR No",
        "PO Number",
        "Evaluation",
        "Invoice / Remark",
        "Invoice Date",
        "Remark"
    ]

    worksheet.merge_range(0, 0, 0, len(headers)-1, "Domestic ETA Dashboard Export", title_fmt)

    for col, h in enumerate(headers):
        worksheet.write(1, col, h, header_fmt)
        worksheet.set_column(col, col, 18)

    # ---- Write Data ----
    row = 2
    for obj in qs:
        col = 0
        # ETD Date
        if obj.ETDDate:
            worksheet.write_datetime(row, col, obj.ETDDate, date_fmt)
        else:
            worksheet.write(row, col, "", cell_fmt)
        col += 1
        # Status
        worksheet.write(row, col, obj.Status, cell_fmt)
        col += 1
        # Raw Material
        worksheet.write(row, col, obj.RawMaterial or "", cell_fmt)
        col += 1
        # Required Date
        if obj.RequiredDate:
            worksheet.write_datetime(row, col, obj.RequiredDate, date_fmt)
        else:
            worksheet.write(row, col, "", cell_fmt)
        col += 1
        # Revised ETA
        if obj.RevisedETADate:
            worksheet.write_datetime(row, col, obj.RevisedETADate, date_fmt)
        else:
            worksheet.write(row, col, "", cell_fmt)
        col += 1
        # Qty
        worksheet.write(row, col, float(obj.Qty or 0), cell_fmt)
        col += 1
        ## Freight Charges
        worksheet.write(row, col, float(obj.FreightCharges or 0), cell_fmt)
        col += 1
        worksheet.write(row, col, float(obj.Qty or 0), cell_fmt)
        col += 1
        # Packing
        worksheet.write(row, col, obj.Packing or "", cell_fmt)
        col += 1
        # Supplier
        worksheet.write(row, col, obj.Supplier or "", cell_fmt)
        col += 1
        # Lifting Location
        worksheet.write(row, col, obj.LiftingLocation or "", cell_fmt)
        col += 1
        # Transporter
        worksheet.write(row, col, obj.TransporterName or "", cell_fmt)
        col += 1
        # Vehicle No
        worksheet.write(row, col, obj.VehicleNo or "", cell_fmt)
        col += 1
        # Driver No
        worksheet.write(row, col, obj.DriverNo or "", cell_fmt)
        col += 1
        # LR No
        worksheet.write(row, col, obj.LRNo or "", cell_fmt)
        col += 1
        # PO Number
        worksheet.write(row, col, obj.PoNumber or "", cell_fmt)
        col += 1
        # Evaluation
        worksheet.write(row, col, obj.Evaluation or "", cell_fmt)
        col += 1
        # Invoice / Remark
        worksheet.write(row, col, obj.InvoiceNoRemark or "", cell_fmt)
        col += 1
        # Invoice Date
        if obj.InvoiceDate:
            worksheet.write_datetime(row, col, obj.InvoiceDate, date_fmt)
        else:
            worksheet.write(row, col, "", cell_fmt)
        col += 1
        # Remark
        worksheet.write(row, col, obj.Remark or "", cell_fmt)

        row += 1

    workbook.close()
    output.seek(0)
    filename = "ETA_Dashboard_Export.xlsx"
    response = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response
