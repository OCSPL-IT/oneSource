# views.py
from django.contrib import messages
import logging
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import  require_POST
from django.shortcuts import get_object_or_404, redirect, render
from .forms import *
from .models import *
from django.core.paginator import Paginator
from io import BytesIO
import xlsxwriter
from django.http import HttpResponse


logger = logging.getLogger("custom_logger")


@login_required
def bioreactor_daily_reading_form(request, pk=None):
    """
    Create or edit a BioreactorDailyReading with its chemicals (PAC / DAP).
    Uses checkboxes + quantity inputs similar to Primary Treatment Effluent.
    """
    # ---- Permission check ----
    # Change perm strings if your app_label/model differ
    if pk:
        perm_code = "BIOREACTOR.change_bioreactordailyreading"
        action_name = "edit"
    else:
        perm_code = "BIOREACTOR.add_bioreactordailyreading"
        action_name = "create"

    if not request.user.has_perm(perm_code):
        messages.error(request, "You do not have permission to add/update Bioreactor readings.")
        logger.warning("User '%s' tried to %s BioreactorDailyReading without permission. perm=%s pk=%s",
            request.user.username,  action_name, perm_code, pk, )
        return redirect("indexpage")
    logger.info("User='%s' opened BioreactorDailyReading form | mode=%s | method=%s | pk=%s",
        request.user.username, "edit" if pk else "create", request.method, pk,)
    if pk:
        reading = get_object_or_404(BioreactorDailyReading, pk=pk)
        is_edit = True
    else:
        reading = None
        is_edit = False
    if request.method == "POST":
        form = BioreactorDailyReadingForm(request.POST, instance=reading)

        if form.is_valid():
            reading = form.save()
            # ----- save chemicals (PAC / DAP) -----
            BioreactorChemical.objects.filter(reading=reading).delete()
            chem_names = request.POST.getlist("chemical_name")
            chem_qtys = request.POST.getlist("chemical_qty")

            saved_chems = 0
            invalid_qty = 0
            for name, qty in zip(chem_names, chem_qtys):
                qty = (qty or "").strip()
                if not qty:
                    continue
                try:
                    qty_val = Decimal(qty)
                except (InvalidOperation, TypeError):
                    invalid_qty += 1
                    continue
                BioreactorChemical.objects.create(
                    reading=reading,
                    chemical_name=name,
                    quantity=qty_val,
                )
                saved_chems += 1
            logger.info( "User='%s' %s BioreactorDailyReading saved | reading_pk=%s | chemicals_saved=%s | invalid_qty_skipped=%s",
                request.user.username,"updated" if is_edit else "created", reading.pk, saved_chems,  invalid_qty,  )
            if is_edit:
                messages.success(request, "Bioreactor reading updated successfully.")
            else:
                messages.success(request, "Bioreactor reading saved successfully.")

            return redirect("bioreactor_daily_reading_detail", pk=reading.pk)
        else:
            logger.error(
                "BioreactorDailyReadingForm invalid | user=%s | pk=%s | errors=%s",
                request.user.username,
                pk,
                form.errors.as_json(),
            )
            messages.error(request, "Please correct the errors below.")

    else:
        form = BioreactorDailyReadingForm(instance=reading)
    # ----- build chemicals list for template (for checkboxes) -----
    if reading:
        existing = {chem.chemical_name: chem.quantity for chem in reading.chemicals.all()}
    else:
        existing = {}
    all_chemicals = []
    for code, label in BIOREACTOR_CHEMICALS:
        all_chemicals.append(
            {
                "value": code,
                "name": label,
                "quantity": existing.get(code),  # None if not present
            }
        )
    context = { "form": form,"is_edit": is_edit, "all_chemicals": all_chemicals,  }
    return render(request, "bioreactor/bioreactor_daily_reading_form.html", context)


@login_required
def bioreactor_daily_reading_list(request):
    """
    List of bioreactor daily readings with simple pagination.
    """
    # ---- Permission check ----
    if not request.user.has_perm("BIOREACTOR.view_bioreactordailyreading"):
        messages.error(request, "You do not have permission to view Bioreactor readings.")
        logger.warning(
            "User '%s' tried to view BioreactorDailyReading list without permission.",
            request.user.username,
        )
        return redirect("indexpage")

    page_number = (request.GET.get("page") or "").strip()

    logger.info(
        "User='%s' opened BioreactorDailyReading LIST | method=%s | page=%s",
        request.user.username,
        request.method,
        page_number or "1",
    )

    qs = BioreactorDailyReading.objects.all().order_by("-date", "-id")

    paginator = Paginator(qs, 25)  # 25 rows per page
    page_obj = paginator.get_page(page_number)

    logger.info(
        "BioreactorDailyReading LIST result | user='%s' | total_rows=%s | page=%s | page_rows=%s",
        request.user.username,
        qs.count(),
        page_obj.number,
        len(page_obj.object_list),
    )
    context = {
        "page_obj": page_obj,
        "readings": page_obj.object_list,
    }
    return render(request, "bioreactor/bioreactor_daily_reading_list.html", context)



@login_required
def bioreactor_daily_reading_detail(request, pk):
    """
    Detail view for a single bioreactor daily reading, including chemicals used.
    """
    # ---- Permission check ----
    if not request.user.has_perm("BIOREACTOR.view_bioreactordailyreading"):
        messages.error(request, "You do not have permission to view Bioreactor reading details.")
        logger.warning(
            "User '%s' tried to view BioreactorDailyReading detail (pk=%s) without permission.",
            request.user.username,
            pk,
        )
        return redirect("indexpage")
    logger.info(
        "User='%s' opened BioreactorDailyReading DETAIL | pk=%s | method=%s",
        request.user.username,
        pk,
        request.method,
    )
    reading = get_object_or_404(
        BioreactorDailyReading.objects.prefetch_related("chemicals"),
        pk=pk,
    )
    chemicals = reading.chemicals.all().order_by("chemical_name")
    logger.info(
        "BioreactorDailyReading DETAIL loaded | user='%s' | pk=%s | chemicals=%s",
        request.user.username,
        pk,
        chemicals.count(),
    )
    context = { "reading": reading,"chemicals": chemicals, }
    return render(request, "bioreactor/bioreactor_daily_reading_detail.html", context)




@login_required
@require_POST
def bioreactor_daily_reading_delete(request, pk):
    """
    Delete a single BioreactorDailyReading (and its chemicals – FK CASCADE).
    Triggered via POST from the list/detail screen.
    """
    # ---- Permission check ----
    if not request.user.has_perm("BIOREACTOR.delete_bioreactordailyreading"):
        messages.error(request, "You do not have permission to delete Bioreactor readings.")
        logger.warning(
            "User '%s' tried to DELETE BioreactorDailyReading (pk=%s) without permission.",
            request.user.username,
            pk,
        )
        return redirect("indexpage")

    reading = get_object_or_404(BioreactorDailyReading, pk=pk)

    try:
        ref = f"{getattr(reading, 'date', None) or ''}".strip() or f"pk={pk}"
        logger.info(
            "User='%s' deleting BioreactorDailyReading | pk=%s | ref='%s'",
            request.user.username,
            pk,
            ref,
        )

        reading.delete()

        logger.info(
            "User='%s' deleted BioreactorDailyReading successfully | pk=%s",
            request.user.username,
            pk,
        )
        messages.success(request, "Bioreactor reading deleted successfully.")

    except Exception:
        logger.exception(
            "Error deleting BioreactorDailyReading | pk=%s | user='%s'",
            pk,
            request.user.username,
        )
        messages.error(request, "An unexpected error occurred while deleting the record.")
    return redirect("bioreactor_daily_reading_list")


@login_required
def bioreactor_daily_reading_excel(request):
    """
    Download all BioreactorDailyReading rows as an Excel file (using xlsxwriter)
    in the same layout as the provided Excel sample (group headers, colors, etc.).
    """

    # ---- Permission check ----
    if not request.user.has_perm("BIOREACTOR.view_bioreactordailyreading"):
        messages.error(request, "You do not have permission to export Bioreactor readings.")
        logger.warning(
            "User '%s' tried to EXPORT BioreactorDailyReading Excel without permission.",
            request.user.username,
        )
        return redirect("indexpage")

    logger.info("User='%s' started BioreactorDailyReading Excel export", request.user.username)

    try:
        # Query data (with chemicals)
        qs = (
            BioreactorDailyReading.objects
            .prefetch_related("chemicals")
            .order_by("date", "id")
        )

        total_rows = qs.count()
        logger.info("User='%s' Bioreactor export rows=%s", request.user.username, total_rows)

        # In-memory workbook
        output = BytesIO()
        workbook = xlsxwriter.Workbook(output, {"in_memory": True})
        ws = workbook.add_worksheet("Bioreactor Readings")

        # ===== Formats =====
        title_fmt = workbook.add_format({
            "bold": True,
            "font_size": 14,
            "align": "center",
            "valign": "vcenter",
        })

        generic_group_fmt = workbook.add_format({
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#F9FAFB",
        })
        br1_group_fmt = workbook.add_format({
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#FEF3C7",   # amber-ish
        })
        br2_group_fmt = workbook.add_format({
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#E5E7EB",   # slate-ish
        })
        pt_group_fmt = workbook.add_format({
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#DBEAFE",   # sky-ish
        })
        feed_group_fmt = workbook.add_format({
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#DCFCE7",   # emerald-ish
        })

        sub_header_fmt_br1 = workbook.add_format({
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#FEF9C3",
            "font_size": 9,
        })
        sub_header_fmt_br2 = workbook.add_format({
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#F3F4F6",
            "font_size": 9,
        })
        sub_header_fmt_pt = workbook.add_format({
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#E0F2FE",
            "font_size": 9,
        })
        sub_header_fmt_feed = workbook.add_format({
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#DCFCE7",
            "font_size": 9,
        })
        sub_header_fmt_generic = workbook.add_format({
            "bold": True,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "bg_color": "#F1F5F9",
            "font_size": 9,
        })

        num_int_fmt = workbook.add_format({"num_format": "0", "border": 1})
        num_two_dec_fmt = workbook.add_format({"num_format": "0.00", "border": 1})
        text_fmt = workbook.add_format({"border": 1})

        # ===== Column layout =====
        # 0: Date
        # 1: Bioreactor Feed (KLD)
        # 2-8:  Bioreactor – 1 (pH, COD, MLSS, MLVSS, SVI, DO, F:M)
        # 9-15: Bioreactor – 2 (pH, COD, MLSS, MLVSS, SVI, DO, F:M)
        # 16-19: Polishing Tank (pH, TSS, TDS, COD)
        # 20-24: Feed effluent quality (pH, Temp, TDS, TSS, COD)
        # 25-26: Chemicals (PAC, DAP)

        # ===== Title row (row 0) =====
        last_col = 26
        ws.merge_range(0, 0, 0, last_col, "Bioreactor Daily Readings", title_fmt)

        # ===== Group header row (row 1) =====
        ws.merge_range(1, 0, 2, 0, "Date", generic_group_fmt)
        ws.merge_range(1, 1, 2, 1, "Bioreactor Feed\n(KLD)", generic_group_fmt)

        ws.merge_range(1, 2, 1, 8, "Bioreactor – 1", br1_group_fmt)
        ws.merge_range(1, 9, 1, 15, "Bioreactor – 2", br2_group_fmt)
        ws.merge_range(1, 16, 1, 19, "Polishing Tank", pt_group_fmt)
        ws.merge_range(1, 20, 1, 24, "Bioreactor Feed effluent quality", feed_group_fmt)
        ws.merge_range(1, 25, 1, 26, "Chemical Used", generic_group_fmt)

        # ===== Sub-header row (row 2) =====
        br1_sub_headers = ["pH", "COD\n(ppm)", "MLSS\n(ppm)", "MLVSS\n(ppm)", "SVI\n(ml)", "DO\n(ppm)", "F:M ratio"]
        br2_sub_headers = ["pH", "COD\n(ppm)", "MLSS\n(ppm)", "MLVSS\n(ppm)", "SVI\n(ml)", "DO\n(ppm)", "F:M ratio"]
        pt_sub_headers = ["pH", "TSS\n(ppm)", "TDS\n(ppm)", "COD\n(ppm)"]
        feed_sub_headers = ["pH", "Temp\n(°C)", "TDS\n(ppm)", "TSS\n(ppm)", "COD\n(ppm)"]

        col = 2
        for title in br1_sub_headers:
            ws.write(2, col, title, sub_header_fmt_br1)
            col += 1

        for title in br2_sub_headers:
            ws.write(2, col, title, sub_header_fmt_br2)
            col += 1

        for title in pt_sub_headers:
            ws.write(2, col, title, sub_header_fmt_pt)
            col += 1

        for title in feed_sub_headers:
            ws.write(2, col, title, sub_header_fmt_feed)
            col += 1

        ws.write(2, 25, "PAC (Kg)", sub_header_fmt_generic)
        ws.write(2, 26, "DAP (Kg)", sub_header_fmt_generic)

        # Column widths + freeze
        ws.set_column(0, 0, 11)
        ws.set_column(1, 1, 14)
        ws.set_column(2, last_col, 11)
        ws.freeze_panes(3, 2)

        # ===== Helpers =====
        def write_num(row, col, value):
            if value is None or value == "":
                ws.write(row, col, "", text_fmt)
                return
            v = float(value)
            if v.is_integer():
                ws.write_number(row, col, v, num_int_fmt)
            else:
                ws.write_number(row, col, round(v, 2), num_two_dec_fmt)

        def write_text(row, col, value):
            ws.write(row, col, "" if value is None else value, text_fmt)

        # ===== Data rows start at row 3 =====
        row_idx = 3

        # important: avoid N+1 by iterating; prefetch_related already done
        for r in qs:
            chem_map = {c.chemical_name: c.quantity for c in r.chemicals.all()}
            pac_qty = chem_map.get("PAC")
            dap_qty = chem_map.get("DAP")

            col = 0
            write_text(row_idx, col, r.date.strftime("%d-%m-%Y")); col += 1
            write_num(row_idx, col, r.bioreactor_feed); col += 1

            # --- Bioreactor 1 ---
            write_num(row_idx, col, r.bioreactor_1_ph);       col += 1
            write_num(row_idx, col, r.bioreactor_1_cod);      col += 1
            write_num(row_idx, col, r.bioreactor_1_mlss);     col += 1
            write_num(row_idx, col, r.bioreactor_1_mlvss);    col += 1
            write_num(row_idx, col, r.bioreactor_1_svi);      col += 1
            write_num(row_idx, col, r.bioreactor_1_do);       col += 1
            write_num(row_idx, col, r.bioreactor_1_fm_ratio); col += 1

            # --- Bioreactor 2 ---
            write_num(row_idx, col, r.bioreactor_2_ph);       col += 1
            write_num(row_idx, col, r.bioreactor_2_cod);      col += 1
            write_num(row_idx, col, r.bioreactor_2_mlss);     col += 1
            write_num(row_idx, col, r.bioreactor_2_mlvss);    col += 1
            write_num(row_idx, col, r.bioreactor_2_svi);      col += 1
            write_num(row_idx, col, r.bioreactor_2_do);       col += 1
            write_num(row_idx, col, r.bioreactor_2_fm_ratio); col += 1

            # --- Polishing Tank ---
            write_num(row_idx, col, r.polishing_tank_ph);  col += 1
            write_num(row_idx, col, r.polishing_tank_tss); col += 1
            write_num(row_idx, col, r.polishing_tank_tds); col += 1
            write_num(row_idx, col, r.polishing_tank_cod); col += 1

            # --- Feed effluent quality ---
            write_num(row_idx, col, r.bioreactor_feed_ph);   col += 1
            write_num(row_idx, col, r.bioreactor_feed_temp); col += 1
            write_num(row_idx, col, r.bioreactor_feed_tds);  col += 1
            write_num(row_idx, col, r.bioreactor_feed_tss);  col += 1
            write_num(row_idx, col, r.bioreactor_feed_cod);  col += 1

            # --- Chemicals ---
            write_num(row_idx, col, pac_qty); col += 1
            write_num(row_idx, col, dap_qty); col += 1

            row_idx += 1

        workbook.close()
        output.seek(0)

        filename = "bioreactor_readings.xlsx"
        logger.info(
            "User='%s' completed BioreactorDailyReading Excel export | rows=%s | file='%s'",
            request.user.username,
            total_rows,
            filename,
        )

        response = HttpResponse(
            output.getvalue(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = f'attachment; filename="{filename}"'
        return response

    except Exception:
        logger.exception(
            "BioreactorDailyReading Excel export failed | user='%s'",
            request.user.username,
        )
        messages.error(request, "Failed to export Bioreactor readings. Please check logs.")
        return redirect("indexpage")