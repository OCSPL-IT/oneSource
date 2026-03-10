# views.py
from django.shortcuts import render, redirect, get_object_or_404
from django.http import Http404, HttpResponse
from .forms import *
from .models import *
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.shortcuts import render, get_object_or_404, redirect
from django.contrib import messages
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_http_methods
from django.db.models import Q
from django.forms.models import model_to_dict
from django.db import connections
from .utils.amount_words import money_usd_to_words
from datetime import date
import logging


logger = logging.getLogger('custom_logger')


def fetch_erp_invoice_data(invoice_no: str):
    sql = r"""
    DECLARE @InvoiceNo nvarchar(200) = %s;

    ;WITH H AS (
        SELECT d.*
        FROM TXNHDR d (NOLOCK)
        WHERE d.lCompId = 27
          AND d.lTypId  = 651
          AND ISNULL(d.bDel,0)=0
          AND EXISTS (
                SELECT 1
                FROM TXNCF cf0 (NOLOCK)
                WHERE cf0.lId      = d.lId
                  AND cf0.lLine    = 0
                  AND cf0.lFieldNo = 1
                  AND LTRIM(RTRIM(CONVERT(nvarchar(200), cf0.sValue))) = @InvoiceNo
          )
    )
    SELECT
        tt.sName                                               AS transaction_type,
        cf.InvoiceNo                                           AS invoice_number,

        CONVERT(date, CONVERT(char(8), h.dtEntDate), 112)      AS invoice_date,

        cf.BuyersOrderNo                                       AS buyers_order_no,
        cf.BuyersOrderDate                                     AS buyers_order_date,

        bill.sName                                             AS consignee_name,
        h.sAccAdd2                                             AS consignee_address,

        cf.Notify1Name                                         AS notify_party1_name,
        cf.Notify1Address                                      AS notify_party1_address,
        cf.Notify2Name                                         AS notify_party2_name,
        cf.Notify2Address                                      AS notify_party2_address,

        cf.CountryOrigin                                       AS country_origin,
        cf.CountryDestination                                  AS country_destination,
        cf.DistrictOrigin                                      AS district_origin,
        cf.StateOrigin                                         AS state_origin,

        cf.Vessel                                              AS vessel_name_no,
        cf.PortLoading                                         AS port_loading,
        cf.PortDischarge                                       AS port_discharge,
        cf.FinalDestination                                    AS final_destination,

        N'NO'                                                  AS preferential_agreement,
        N'KGS'                                                 AS standard_unit_qty_code,

        cf.Delivery                                            AS delivery,
        cf.Shipment                                            AS shipment_mode,
        cf.PaymentTerms                                        AS payment_terms,

        CONVERT(varchar(11),
                CONVERT(date, CONVERT(char(8), h.dtDueDate), 112),
                106)                                           AS due_date,

        cf.BankName                                            AS bank_name,
        cf.BankAccNo                                           AS bank_account_no,
        cf.ADCode                                              AS ad_code,
        cf.SwiftCode                                           AS swift_code,
        cf.BankAddress                                         AS bank_address,

        itm.sName                                              AS product_name,
        lcf.ContainerNo                                        AS container_no,

        CONVERT(nvarchar(max), dline.sNarr)                    AS merks_and_container_no,
        CONVERT(nvarchar(max), nar.sValue)                     AS merks_and_container_no1,

        lcf.Packing                                            AS packing_details,
        lcf.GrossWt                                            AS gross_wt,

        LTRIM(RTRIM(
            COALESCE(NULLIF(dline.sValue8, N''), N'')
            + CASE
                WHEN NULLIF(dline.sValue8, N'') IS NOT NULL
                 AND NULLIF(lcf.GoodsDesc, N'') IS NOT NULL
                    THEN CHAR(13) + CHAR(10)      -- newline between both lines
                ELSE N''
              END
            + COALESCE(NULLIF(lcf.GoodsDesc, N''), N'')
        ))                                                      AS description_of_goods,
        lcf.ItemNo                                             AS item_no,
        hsn.sCode                                              AS hsn_no,

        CONVERT(decimal(12,2), dline.dQty)                     AS quantity,
        un.sName                                               AS quantity_unit,

        -- ✅ ERP Total + Conversion Rate (your highlighted columns)
        -- (optional) base total, not mapped to Django model but ok to keep
        CONVERT(decimal(18,5), h.dTotal)                       AS erp_total,

        -- 🔹 This will map directly to InvoicePostShipment.conversion_rate
        CONVERT(decimal(18,2), h.dCurrCnv)                    AS conversion_rate,

        -- ✅ Amount (US$) = Total / ConversionRate
        CAST(ROUND(
            CONVERT(decimal(38,10), h.dTotal) / NULLIF(CONVERT(decimal(38,10), h.dCurrCnv), 0),
            2
        ) AS decimal(12,2))                                    AS amount_usd,

        -- ✅ Rate (US$/Unit) = AmountUSD / Quantity
        CAST(ROUND(
            (
              CONVERT(decimal(38,10), h.dTotal) / NULLIF(CONVERT(decimal(38,10), h.dCurrCnv), 0)
            ) / NULLIF(CONVERT(decimal(38,10), dline.dQty), 0),
            4
        ) AS decimal(12,4))                                    AS rate_usd,

        cf.BLNo                                                AS bl_number,
        cf.BLDate                                              AS bl_date,
        cf.ShippingBillNo                                      AS shipping_bill_no,
        cf.ShippingBillDate                                    AS shipping_bill_date

    FROM H h
    JOIN TXNTYP tt (NOLOCK) ON tt.lTypId = h.lTypId
    LEFT JOIN BUSMST bill (NOLOCK) ON bill.lId = h.lAccId1

    OUTER APPLY (
        SELECT
            MAX(CASE WHEN cf0.lFieldNo=1  THEN LTRIM(RTRIM(CONVERT(nvarchar(200), cf0.sValue))) END) AS InvoiceNo,
            MAX(CASE WHEN cf0.lFieldNo=28 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS BuyersOrderNo,

            TRY_CONVERT(date, REPLACE(CONVERT(varchar(50),
                MAX(CASE WHEN cf0.lFieldNo=29 THEN CONVERT(nvarchar(50), cf0.sValue) END)
            ), '-', ' '), 106) AS BuyersOrderDate,

            MAX(CASE WHEN cf0.lFieldNo=32 THEN CONVERT(nvarchar(500), cf0.sValue) END) AS Notify1Name,
            MAX(CASE WHEN cf0.lFieldNo=33 THEN CONVERT(nvarchar(max), cf0.sValue) END) AS ConsigneeAddress,

            LTRIM(RTRIM(COALESCE(MAX(CASE WHEN cf0.lFieldNo=33 THEN CONVERT(nvarchar(max), cf0.sValue) END), N''))) +
              CASE
                WHEN NULLIF(LTRIM(RTRIM(COALESCE(MAX(CASE WHEN cf0.lFieldNo=34 THEN CONVERT(nvarchar(max), cf0.sValue) END), N''))), N'') IS NULL
                THEN N''
                ELSE CHAR(10) + LTRIM(RTRIM(MAX(CASE WHEN cf0.lFieldNo=34 THEN CONVERT(nvarchar(max), cf0.sValue) END)))
              END AS Notify1Address,

            MAX(CASE WHEN cf0.lFieldNo=35 THEN CONVERT(nvarchar(500), cf0.sValue) END) AS Notify2Name,

            LTRIM(RTRIM(COALESCE(MAX(CASE WHEN cf0.lFieldNo=36 THEN CONVERT(nvarchar(max), cf0.sValue) END), N''))) +
              CASE
                WHEN NULLIF(LTRIM(RTRIM(COALESCE(MAX(CASE WHEN cf0.lFieldNo=37 THEN CONVERT(nvarchar(max), cf0.sValue) END), N''))), N'') IS NULL
                THEN N''
                ELSE CHAR(10) + LTRIM(RTRIM(MAX(CASE WHEN cf0.lFieldNo=37 THEN CONVERT(nvarchar(max), cf0.sValue) END)))
              END AS Notify2Address,

            MAX(CASE WHEN cf0.lFieldNo=40 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS CountryOrigin,
            MAX(CASE WHEN cf0.lFieldNo=41 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS CountryDestination,
            MAX(CASE WHEN cf0.lFieldNo=19 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS DistrictOrigin,
            MAX(CASE WHEN cf0.lFieldNo=55 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS StateOrigin,

            MAX(CASE WHEN cf0.lFieldNo=42 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS Vessel,
            MAX(CASE WHEN cf0.lFieldNo=43 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS PortLoading,
            MAX(CASE WHEN cf0.lFieldNo=44 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS PortDischarge,
            MAX(CASE WHEN cf0.lFieldNo=45 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS FinalDestination,

            MAX(CASE WHEN cf0.lFieldNo=46 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS Delivery,
            MAX(CASE WHEN cf0.lFieldNo=47 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS Shipment,
            MAX(CASE WHEN cf0.lFieldNo=48 THEN CONVERT(nvarchar(500), cf0.sValue) END) AS PaymentTerms,

            MAX(CASE WHEN cf0.lFieldNo=69 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS BankName,
            MAX(CASE WHEN cf0.lFieldNo=51 THEN CONVERT(nvarchar(max), cf0.sValue) END) AS BankAddress,
            MAX(CASE WHEN cf0.lFieldNo=49 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS BankAccNo,
            MAX(CASE WHEN cf0.lFieldNo=50 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS ADCode,
            MAX(CASE WHEN cf0.lFieldNo=52 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS SwiftCode,

            MAX(CASE WHEN cf0.lFieldNo=59 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS BLNo,
            TRY_CONVERT(date, REPLACE(CONVERT(varchar(50),
                MAX(CASE WHEN cf0.lFieldNo=60 THEN CONVERT(nvarchar(50), cf0.sValue) END)
            ), '-', ' '), 106) AS BLDate,

            MAX(CASE WHEN cf0.lFieldNo=61 THEN CONVERT(nvarchar(200), cf0.sValue) END) AS ShippingBillNo,
            TRY_CONVERT(date, REPLACE(CONVERT(varchar(50),
                MAX(CASE WHEN cf0.lFieldNo=62 THEN CONVERT(nvarchar(50), cf0.sValue) END)
            ), '-', ' '), 106) AS ShippingBillDate
        FROM TXNCF cf0 (NOLOCK)
        WHERE cf0.lId = h.lId AND cf0.lLine = 0
    ) cf

    OUTER APPLY (
        SELECT TOP (1) dd.*
        FROM TXNDET dd (NOLOCK)
        WHERE dd.lId = h.lId AND ISNULL(dd.bDel,0)=0
        ORDER BY dd.lLine
    ) dline

    LEFT JOIN ITMMST itm (NOLOCK) ON itm.lId = dline.lItmId
    LEFT JOIN UNTMST un  (NOLOCK) ON un.lId  = dline.lUntId
    LEFT JOIN HSNMST hsn (NOLOCK) ON hsn.lId = dline.lHsnId AND hsn.cTyp='I'
    LEFT JOIN TXNNAR nar (NOLOCK) ON nar.lId = h.lId AND nar.lLine = dline.lLine AND nar.cTyp='N'

    OUTER APPLY (
        SELECT
            MAX(CASE WHEN cf1.lFieldNo=16 THEN CONVERT(nvarchar(max), cf1.sValue) END) AS Packing,
            NULLIF(LTRIM(RTRIM(MAX(CASE WHEN cf1.lFieldNo=18 THEN CONVERT(nvarchar(100), cf1.sValue) END))), N'') AS GrossWt,
            MAX(CASE WHEN cf1.lFieldNo=5  THEN CONVERT(nvarchar(max), cf1.sValue) END) AS GoodsDesc,
            MAX(CASE WHEN cf1.lFieldNo=2  THEN CONVERT(nvarchar(max), cf1.sValue) END) AS ItemNo,
            MAX(CASE WHEN cf1.lFieldNo=20 THEN CONVERT(nvarchar(max), cf1.sValue) END) AS ContainerNo
        FROM TXNCF cf1 (NOLOCK)
        WHERE cf1.lId = h.lId AND cf1.lLine = dline.lLine
    ) lcf;
    """

    with connections["readonly_db"].cursor() as cursor:
        cursor.execute(sql, [invoice_no])
        row = cursor.fetchone()
        if not row:
            return None
        cols = [c[0] for c in cursor.description]
        data = dict(zip(cols, row))

        # 🔹 Fix BL / Shipping dates: if NULL or 1900-xx-xx → use today
        today = date.today()
        for field in ("bl_date", "shipping_bill_date"):
            d = data.get(field)
            # d is a Python date from the DB cursor
            if not d or getattr(d, "year", None) == 1900:
                data[field] = today
        return data






def _clean_initial_from_post(post_data):
    """
    Take POST and build initial dict (remove csrf/action).
    """
    initial = post_data.copy().dict()
    initial.pop("csrfmiddlewaretoken", None)
    initial.pop("action", None)
    return initial


@login_required
@require_http_methods(["GET", "POST"])
def invoice_post_shipment_form_view(request):
    """
    NEW form (create).
    Supports:
      - action=fetch (fetch ERP data)
      - action=save  (save new record)
    """

    # ---- Permission check (Create/Add) ----
    if not request.user.has_perm("Export_Commercial_Invoice.add_invoicepostshipment"):
        messages.error(request, "You do not have permission to add Invoice Post-Shipment records.")
        logger.warning(
            "User '%s' tried to create Invoice Post-Shipment record without permission.",
            request.user.username,
        )
        return redirect("indexpage")

    logger.info("User=%s accessed Invoice Post-Shipment CREATE page", request.user.username)

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        logger.info(
            "User=%s Invoice Post-Shipment CREATE POST | action=%s | invoice_number=%s",
            request.user.username,
            action,
            (request.POST.get("invoice_number") or "").strip(),
        )

        # ---------------- FETCH ----------------
        if action == "fetch":
            invoice_no = (request.POST.get("invoice_number") or "").strip()
            initial = _clean_initial_from_post(request.POST)

            if not invoice_no:
                logger.warning(
                    "User=%s Invoice Post-Shipment CREATE fetch failed: invoice_number missing",
                    request.user.username,
                )
                messages.error(request, "Please enter Invoice No. then click Fetch from ERP.")
                form = InvoicePostShipmentForm(initial=initial)
                return render(request, "export/invoice_post_shipment_form.html", {"form": form})

            logger.info(
                "User=%s fetching ERP invoice data | invoice_no=%s",
                request.user.username,
                invoice_no,
            )
            erp_data = fetch_erp_invoice_data(invoice_no)
            if not erp_data:
                logger.warning(
                    "User=%s ERP invoice not found | invoice_no=%s",
                    request.user.username,
                    invoice_no,
                )
                messages.error(request, f"No ERP invoice found for '{invoice_no}'.")
                form = InvoicePostShipmentForm(initial=initial)
                return render(request, "export/invoice_post_shipment_form.html", {"form": form})

            # merge ERP -> initial (ERP wins if not None)
            for k, v in erp_data.items():
                if v is not None:
                    initial[k] = v

            initial["invoice_number"] = invoice_no
            initial.setdefault("preferential_agreement", "NO")
            initial.setdefault("standard_unit_qty_code", "KGS")

            logger.info(
                "User=%s ERP data fetched successfully | invoice_no=%s",
                request.user.username,
                invoice_no,
            )
            form = InvoicePostShipmentForm(initial=initial)
            messages.success(request, "ERP data fetched. Please review and fill remaining fields.")
            return render(request, "export/invoice_post_shipment_form.html", {"form": form,"is_edit": False,})

        # ---------------- SAVE ----------------
        form = InvoicePostShipmentForm(request.POST, request.FILES)
        if form.is_valid():
            obj = form.save()
            logger.info(
                "User=%s created InvoicePostShipment pk=%s | invoice_number=%s",
                request.user.username,
                obj.pk,
                getattr(obj, "invoice_number", None),
            )
            messages.success(request, "Invoice post-shipment record saved.")
            return redirect("invoice_post_shipment_detail", pk=obj.pk)

        logger.warning(
            "User=%s InvoicePostShipment CREATE form invalid | errors=%s",
            request.user.username,
            form.errors.as_json(),
        )
        messages.error(request, "Please correct the errors highlighted below.")
        return render(request, "export/invoice_post_shipment_form.html", {"form": form, "is_edit": False,})

    # GET (new)
    form = InvoicePostShipmentForm(initial={"preferential_agreement": "NO", "standard_unit_qty_code": "KGS"})
    return render(request, "export/invoice_post_shipment_form.html", {"form": form,"is_edit": False,})


@login_required
@require_http_methods(["GET", "POST"])
def invoice_post_shipment_edit_view(request, pk):
    """
    EDIT form (update).
    Uses SAME template: export/invoice_post_shipment_form.html
    Supports:
      - action=fetch (fetch ERP data and refill form)
      - action=save  (update existing record)
    """
    # ---- Permission check (Edit/Change) ----
    if not request.user.has_perm("Export_Commercial_Invoice.change_invoicepostshipment"):
        messages.error(request, "You do not have permission to update Invoice Post-Shipment records.")
        logger.warning(
            "User '%s' tried to update Invoice Post-Shipment pk=%s without permission.",
            request.user.username,
            pk,
        )
        return redirect("indexpage")

    obj = get_object_or_404(InvoicePostShipment, pk=pk)
    logger.info(
        "User=%s accessed Invoice Post-Shipment EDIT page | pk=%s | invoice_number=%s",
        request.user.username,
        obj.pk,
        getattr(obj, "invoice_number", None),
    )

    if request.method == "POST":
        action = (request.POST.get("action") or "").strip()
        logger.info(
            "User=%s Invoice Post-Shipment EDIT POST | pk=%s | action=%s | invoice_number=%s",
            request.user.username,
            obj.pk,
            action,
            (request.POST.get("invoice_number") or "").strip(),
        )
        # ---------------- FETCH (Edit) ----------------
        if action == "fetch":
            invoice_no = (request.POST.get("invoice_number") or "").strip()

            # Start from DB values (so nothing is lost)
            initial = model_to_dict(obj)

            # Overlay user's typed values from POST
            posted_initial = _clean_initial_from_post(request.POST)
            for k, v in posted_initial.items():
                initial[k] = v

            if not invoice_no:
                logger.warning(
                    "User=%s Invoice Post-Shipment EDIT fetch failed: invoice_number missing | pk=%s",
                    request.user.username,
                    obj.pk,
                )
                messages.error(request, "Please enter Invoice No. then click Fetch from ERP.")
                form = InvoicePostShipmentForm(initial=initial)
                return render(request, "export/invoice_post_shipment_form.html", {"form": form, "is_edit": True})

            logger.info(
                "User=%s fetching ERP invoice data (edit) | pk=%s | invoice_no=%s",
                request.user.username,
                obj.pk,
                invoice_no,
            )
            erp_data = fetch_erp_invoice_data(invoice_no)
            if not erp_data:
                logger.warning(
                    "User=%s ERP invoice not found (edit) | pk=%s | invoice_no=%s",
                    request.user.username,
                    obj.pk,
                    invoice_no,
                )
                messages.error(request, f"No ERP invoice found for '{invoice_no}'.")
                form = InvoicePostShipmentForm(initial=initial)
                return render(request, "export/invoice_post_shipment_form.html", {"form": form, "is_edit": True})

            # Merge ERP -> initial (ERP wins if not None)
            for k, v in erp_data.items():
                if v is not None:
                    initial[k] = v

            initial["invoice_number"] = invoice_no
            initial.setdefault("preferential_agreement", "NO")
            initial.setdefault("standard_unit_qty_code", "KGS")

            logger.info(
                "User=%s ERP data fetched successfully (edit) | pk=%s | invoice_no=%s",
                request.user.username,
                obj.pk,
                invoice_no,
            )
            form = InvoicePostShipmentForm(initial=initial)
            messages.success(request, "ERP data fetched. Please review and update fields, then Save.")
            return render(request, "export/invoice_post_shipment_form.html", {"form": form, "is_edit": True})

        # ---------------- SAVE (Edit) ----------------
        form = InvoicePostShipmentForm(request.POST, request.FILES, instance=obj)
        if form.is_valid():
            obj = form.save()
            logger.info(
                "User=%s updated InvoicePostShipment pk=%s | invoice_number=%s",
                request.user.username,
                obj.pk,
                getattr(obj, "invoice_number", None),
            )
            messages.success(request, "Invoice post-shipment record updated.")
            return redirect("invoice_post_shipment_detail", pk=obj.pk)

        logger.warning(
            "User=%s InvoicePostShipment EDIT form invalid | pk=%s | errors=%s",
            request.user.username,
            obj.pk,
            form.errors.as_json(),
        )
        messages.error(request, "Please correct the errors highlighted below.")
        return render(request, "export/invoice_post_shipment_form.html", {"form": form, "is_edit": True})
    # GET (edit)
    form = InvoicePostShipmentForm(instance=obj)
    return render(request, "export/invoice_post_shipment_form.html", {"form": form, "is_edit": True})




@login_required
def invoice_post_shipment_detail_view(request, pk):
    """
    Printable / PDF-like invoice view.
    Later you can wrap this with xhtml2pdf / weasyprint.
    """
    # ---- Permission check (View) ----
    if not request.user.has_perm("Export_Commercial_Invoice.view_invoicepostshipment"):
        messages.error(request, "You do not have permission to view Invoice Post-Shipment records.")
        logger.warning(
            "User '%s' tried to view Invoice Post-Shipment detail pk=%s without permission.",
            request.user.username,
            pk,
        )
        return redirect("indexpage")

    invoice = get_object_or_404(InvoicePostShipment, pk=pk)
    logger.info(
        "User=%s accessed Invoice Post-Shipment DETAIL | pk=%s | invoice_number=%s",
        request.user.username,
        invoice.pk,
        getattr(invoice, "invoice_number", None),
    )

    amount_usd_in_words = money_usd_to_words(invoice.amount_usd)

    context = {
        "invoice": invoice,
        # static company/shipper info – adjust as required
        "shipper_name": "OC SPECIALITIES PVT. LTD.",
        "shipper_address": (
            "PLOT NO. E-18, CHINCHOLI MIDC,\n"
            "TALUKA MOHOL, DIST. SOLAPUR - 413255,\n"
            "MAHARASHTRA, INDIA"
        ),
        "shipper_contact": (
            "E : solapur@ocspl.com\n"
            "W : www.ocspl.com \n"
            "Tel. Ph. :  +91 22 2626 9200"
        ),
        "shipper_gstin": "27AAACO7181P1ZT",
        "shipper_pan": "AAACO7181P",
        "shipper_cin": "U24100MH2005PTC150735",
        "shipper_iec": "0305003364",
        "amount_usd_in_words": amount_usd_in_words,
    }
    return render(request, "export/invoice_post_shipment_pdf.html", context)


@login_required
def invoice_post_shipment_list_view(request):
    """
    List page with 2 tabs:
      - tab=invoice  -> Invoice Post Shipment (Invoice print link)
      - tab=packing  -> Post Shipment Packing List (Packing list print link)
    """
    # ---- Permission check (View) ----
    if not request.user.has_perm("Export_Commercial_Invoice.view_invoicepostshipment"):
        messages.error(request, "You do not have permission to view Invoice Post-Shipment records.")
        logger.warning(
            "User '%s' tried to access Invoice Post-Shipment LIST without permission.",
            request.user.username,
        )
        return redirect("indexpage")

    tab = (request.GET.get("tab") or "invoice").strip().lower()
    if tab not in ("invoice", "packing"):
        tab = "invoice"

    qs = InvoicePostShipment.objects.all()

    search = (request.GET.get("search") or "").strip()
    if search:
        qs = qs.filter(
            Q(invoice_number__icontains=search)
            | Q(buyers_order_no__icontains=search)
            | Q(consignee_name__icontains=search)
        )

    qs = qs.order_by("-invoice_date", "-id")

    logger.info(
        "User=%s accessed Invoice Post-Shipment LIST | tab=%s | search='%s' | rows=%s",
        request.user.username,
        tab,
        search,
        qs.count(),
    )

    paginator = Paginator(qs, 25)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    context = {
        "page_obj": page_obj,
        "search": search,
        "tab": tab,  # ✅ important for tabs + action url switch
    }
    return render(request, "export/invoice_post_shipment_list.html", context)


@login_required
@require_http_methods(["GET", "POST"])
def invoice_post_shipment_delete_view(request, pk):
    # ---- Permission check (Delete) ----
    if not request.user.has_perm("Export_Commercial_Invoice.delete_invoicepostshipment"):
        messages.error(request, "You do not have permission to delete Invoice Post-Shipment records.")
        logger.warning(
            "User '%s' tried to DELETE Invoice Post-Shipment pk=%s without permission.",
            request.user.username,
            pk,
        )
        return redirect("indexpage")

    obj = get_object_or_404(InvoicePostShipment, pk=pk)

    if request.method == "POST":
        inv_no = obj.invoice_number
        try:
            obj.delete()
            logger.info(
                "User=%s deleted Invoice Post-Shipment | pk=%s | invoice_number=%s",
                request.user.username,
                pk,
                inv_no,
            )
            messages.success(request, f"Invoice '{inv_no}' deleted successfully.")
        except Exception as e:
            logger.error(
                "User=%s failed to delete Invoice Post-Shipment | pk=%s | invoice_number=%s | error=%s",
                request.user.username,
                pk,
                inv_no,
                str(e),
            )
            messages.error(request, "An unexpected error occurred while deleting the invoice.")
        return redirect("invoice_post_shipment_list")

    # GET -> confirm page
    logger.info(
        "User=%s opened Invoice Post-Shipment DELETE confirm | pk=%s | invoice_number=%s",
        request.user.username,
        obj.pk,
        getattr(obj, "invoice_number", None),
    )
    return render(request, "export/invoice_post_shipment_confirm_delete.html", {"obj": obj})


def _shipper_context():
    return {
        "shipper_name": "OC SPECIALITIES PVT. LTD.",
        "shipper_address": (
            "PLOT NO. E-18, CHINCHOLI MIDC,\n"
            "TALUKA MOHOL, DIST. SOLAPUR - 413255,\n"
            "MAHARASHTRA, INDIA"
        ),
        "shipper_contact": (
            "E : solapur@ocspl.com\n"
            "W : www.ocspl.com \n"
            "Tel. Ph. :  +91 22 2626 9200"
        ),
        "shipper_gstin": "27AAACO7181P1ZT",
        "shipper_pan": "AAACO7181P",
        "shipper_cin": "U24100MH2005PTC150735",
        "shipper_iec": "0305003364",
    }


@login_required
def invoice_post_shipment_packing_list_view(request, pk):
    """
    Packing List printable page (separate HTML)
    """
    # ---- Permission check (View) ----
    if not request.user.has_perm("Export_Commercial_Invoice.view_invoicepostshipment"):
        messages.error(request, "You do not have permission to view Invoice Post-Shipment records.")
        logger.warning(
            "User '%s' tried to view Packing List pk=%s without permission.",
            request.user.username,
            pk,
        )
        return redirect("indexpage")

    invoice = get_object_or_404(InvoicePostShipment, pk=pk)
    logger.info(
        "User=%s accessed Invoice Post-Shipment PACKING LIST | pk=%s | invoice_number=%s",
        request.user.username,
        invoice.pk,
        getattr(invoice, "invoice_number", None),
    )

    context = {"invoice": invoice}
    context.update(_shipper_context())

    return render(request, "export/invoice_post_shipment_packing_pdf.html", context)



@login_required
def invoice_post_shipment_detail_simple(request, pk):
    """
    Normal (non-PDF) detail page - shows data + attachment download link.
    """
    # ---- Permission check (View) ----
    if not request.user.has_perm("Export_Commercial_Invoice.view_invoicepostshipment"):
        messages.error(request, "You do not have permission to view Invoice Post-Shipment records.")
        return redirect("indexpage")

    obj = get_object_or_404(InvoicePostShipment, pk=pk)

    # BinaryField exists -> just check bytes
    has_attachment = bool(obj.attachment)

    # Optional: if you later add these metadata columns, it will auto show
    attachment_name = getattr(obj, "attachment_name", None)

    return render(
        request,
        "export/invoice_post_shipment_detail_simple.html",
        {
            "obj": obj,
            "has_attachment": has_attachment,
            "attachment_name": attachment_name,
        },
    )
    
    
    
@login_required
def invoice_post_shipment_download_attachment(request, pk):
    """
    Download binary attachment stored in DB (BinaryField).
    """
    if not request.user.has_perm("Export_Commercial_Invoice.view_invoicepostshipment"):
        raise Http404

    obj = get_object_or_404(InvoicePostShipment, pk=pk)

    if not obj.attachment:
        raise Http404("No attachment found")

    # If metadata exists, use it; else fallback
    filename = getattr(obj, "attachment_name", None) or f"invoice_post_shipment_{obj.pk}"
    content_type = getattr(obj, "attachment_content_type", None) or "application/octet-stream"

    resp = HttpResponse(bytes(obj.attachment), content_type=content_type)
    resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp
