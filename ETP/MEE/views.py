from django.shortcuts import render, redirect, get_object_or_404, HttpResponse
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from .models import MEEReadingCategory, MEEReadingSubCategory, MEEDailyReading
from django.db import transaction
from django.db.models import Prefetch
from datetime import date
from datetime import datetime, timedelta
from django.http import Http404
from collections import OrderedDict
from .models import *
from .forms import *
import logging
import io
import xlsxwriter
from decimal import Decimal, InvalidOperation
from django.db.models import Sum,Value,DecimalField
from django.utils.dateparse import parse_date
from django.db.models.functions import Coalesce
from itertools import zip_longest
from django.utils.dateparse import parse_date, parse_datetime
from django.urls import reverse
from io import BytesIO
from decimal import Decimal




logger = logging.getLogger('custom_logger')



def _parse_dt_local(value: str):
    """
    Parse 'YYYY-MM-DDTHH:MM' coming from <input type="datetime-local">.
    Returns naive datetime or None.
    """
    if not value:
        return None
    value = value.strip()
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M")
    except ValueError:
        return None


@login_required
def mee_reading_form(request):
    """
    Dynamic form showing categories and subcategories with Tailwind styling.
    Handles:
      - MEEDailyReading (values per subcategory)
      - MEEDowntime rows for that reading date
    """
    # ---- Permission check ----
    if not request.user.has_perm("MEE.add_meedailyreading"):
        messages.error(request, "You do not have permission to add MEE readings.")
        logger.warning(
            "User '%s' tried to access MEE reading form without permission.",
            request.user.username,
        )
        return redirect("indexpage")
    # --------------------------

    logger.info("User=%s accessed MEE reading form", request.user.username)

    categories = (
        MEEReadingCategory.objects
        .prefetch_related("subcategories")
        .order_by("order", "id")
    )

    if request.method == "POST":
        # raw string from <input type="date">
        reading_date_str = (request.POST.get("reading_date") or "").strip()
        try:
            reading_date = datetime.strptime(reading_date_str, "%Y-%m-%d").date()
        except ValueError:
            reading_date = None

        logger.info(
            "User=%s submitting MEE readings for date=%s",
            request.user.username,
            reading_date_str,
        )

        # ---------- DUPLICATE DATE CHECK ----------
        if reading_date and MEEDailyReading.objects.filter(
            reading_date=reading_date
        ).exists():
            # Attach current input values back to subcategories so user doesn't lose data
            posted_values = {}
            for key, val in request.POST.items():
                if key.startswith("value_"):
                    sub_id = key.split("_", 1)[1]
                    posted_values[sub_id] = val

            for cat in categories:
                for sub in cat.subcategories.all():
                    sub.initial_value = posted_values.get(str(sub.id), "")

            messages.error(request, "Reading date already present.")
            logger.info(
                "User=%s tried to create MEE readings for existing date=%s",
                request.user.username,
                reading_date_str,
            )
            ctx = {
                "categories": categories,
                "selected_date": reading_date,
            }
            return render(request, "MEE/mee_reading_form.html", ctx)
        # ------------------------------------------

        # fallback if parsing failed
        if not reading_date:
            reading_date = date.today()

        # Downtime arrays from the table
        dt_starts  = request.POST.getlist("dt_start[]")
        dt_ends    = request.POST.getlist("dt_end[]")
        dt_reasons = request.POST.getlist("dt_reason[]")

        with transaction.atomic():
            anchor_reading = None  # first MEEDailyReading created

            # 1) Save all reading values
            for key, val in request.POST.items():
                if not key.startswith("value_"):
                    continue
                val = val.strip()
                if not val:
                    continue

                subcat_id = key.split("_", 1)[1]

                reading_obj, _ = MEEDailyReading.objects.update_or_create(
                    reading_date=reading_date,
                    subcategory_id=subcat_id,
                    defaults={
                        "value": val,
                        "entered_by": request.user,
                    },
                )

                if anchor_reading is None:
                    anchor_reading = reading_obj

            # 2) Save downtime rows (attach to anchor_reading)
            if anchor_reading:
                for start_str, end_str, reason_str in zip(dt_starts, dt_ends, dt_reasons):
                    start_dt = _parse_dt_local(start_str)
                    end_dt   = _parse_dt_local(end_str)
                    reason   = (reason_str or "").strip() or None

                    # skip completely empty rows
                    if not start_dt and not end_dt and not reason:
                        continue
                    # require valid start/end
                    if not start_dt or not end_dt or end_dt <= start_dt:
                        continue

                    MEEDowntime.objects.create(
                        reading=anchor_reading,
                        downtime_start=start_dt,
                        downtime_end=end_dt,
                        reason=reason,
                    )

        messages.success(request, "MEE readings and downtimes saved successfully!")
        logger.info(
            "User=%s successfully saved MEE readings for date=%s",
            request.user.username,
            reading_date,
        )
        return redirect("mee:mee_reading_form")
    # GET
    ctx = {
        "categories": categories,
        "selected_date": None,
        "edit_mode": False,
    }
    return render(request, "MEE/mee_reading_form.html", ctx)



def _fmt_mee_value(val):
    """
    For frontend display:
      • if val is blank -> ""
      • if val is numeric and fractional part is 0 -> show integer, e.g. 32
      • else -> show up to 3 decimals, without trailing zeros, e.g. 1.097, 2.5
    """
    if val in (None, ""):
        return ""

    try:
        d = Decimal(str(val))
    except (InvalidOperation, TypeError, ValueError):
        return str(val)

    # pure integer?
    if d == d.to_integral():
        return str(int(d))

    # non-zero fractional part → up to 3 decimals, strip trailing zeros
    d = d.quantize(Decimal("0.001"))
    s = format(d, "f").rstrip("0").rstrip(".")
    return s


def _parse_date_param(value: str):
    """Accept 'YYYY-MM-DD' (HTML date) or 'dd-mm-YYYY'. Return date or None."""
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


@login_required
def mee_reading_list(request):
    # ---- Permission check ----
    if not request.user.has_perm("MEE.view_meedailyreading"):
        messages.error(request, "You do not have permission to view MEE readings.")
        logger.warning(
            "User '%s' tried to access MEE reading list without permission.",
            request.user.username,
        )
        return redirect("indexpage")
    # --------------------------

    # ── read filter params from query ───────────────────────────────────
    from_str = (request.GET.get("from_date") or "").strip()
    to_str   = (request.GET.get("to_date") or "").strip()

    # ── default: current month if no filter given ───────────────────────
    if not from_str and not to_str:
        today = date.today()
        first_day = today.replace(day=1)
        if today.month == 12:
            next_month_first = today.replace(year=today.year + 1, month=1, day=1)
        else:
            next_month_first = today.replace(month=today.month + 1, day=1)
        last_day = next_month_first - timedelta(days=1)

        from_date = first_day
        to_date = last_day

        from_str = from_date.strftime("%Y-%m-%d")
        to_str = to_date.strftime("%Y-%m-%d")
    else:
        from_date = _parse_date_param(from_str)
        to_date = _parse_date_param(to_str)

    logger.info(
        "User=%s accessed MEE reading list with from_date=%s to_date=%s",
        request.user.username,
        from_str,
        to_str,
    )

    # ── categories + subcategories in order ─────────────────────────────
    categories = (
        MEEReadingCategory.objects
        .prefetch_related(
            Prefetch(
                "subcategories",
                queryset=MEEReadingSubCategory.objects.order_by("order", "id"),
            )
        )
        .order_by("order", "id")
    )

    subcats = []
    for cat in categories:
        for sub in cat.subcategories.all():
            subcats.append(sub)

    # ── readings with date range filter ─────────────────────────────────
    qs = (
        MEEDailyReading.objects
        .select_related("subcategory", "subcategory__category")
        .order_by("-reading_date")
    )
    if from_date:
        qs = qs.filter(reading_date__gte=from_date)
    if to_date:
        qs = qs.filter(reading_date__lte=to_date)

    # rows_dict: date -> dict with sub-values
    rows_dict: OrderedDict = OrderedDict()
    for r in qs:
        d = r.reading_date
        if d not in rows_dict:
            rows_dict[d] = {"by_sub": {}}
        rows_dict[d]["by_sub"][r.subcategory_id] = r.value

    # ── aggregate downtimes per *start date* (earliest start, latest end, sum hours, merged reasons) ──
    dt_qs = MEEDowntime.objects.select_related("reading").order_by(
        "reading__reading_date", "downtime_start"
    )
    if from_date:
        dt_qs = dt_qs.filter(reading__reading_date__gte=from_date)
    if to_date:
        dt_qs = dt_qs.filter(reading__reading_date__lte=to_date)

    dt_summary = {}  # date -> aggregate info

    for dt in dt_qs:
        # Key by the calendar day of downtime_start; fallback to reading_date
        if dt.downtime_start:
            key_date = dt.downtime_start.date()
        else:
            key_date = dt.reading.reading_date

        if key_date not in dt_summary:
            dt_summary[key_date] = {
                "start": None,
                "end": None,
                "hours": Decimal("0.00"),
                "reasons": [],
            }
        agg = dt_summary[key_date]

        # earliest start
        if dt.downtime_start and (agg["start"] is None or dt.downtime_start < agg["start"]):
            agg["start"] = dt.downtime_start

        # latest end
        if dt.downtime_end and (agg["end"] is None or dt.downtime_end > agg["end"]):
            agg["end"] = dt.downtime_end

        # total hours (use stored downtime_hours or compute on the fly)
        if dt.downtime_hours is not None:
            try:
                agg["hours"] += Decimal(str(dt.downtime_hours))
            except (InvalidOperation, TypeError, ValueError):
                pass
        elif dt.downtime_start and dt.downtime_end and dt.downtime_end > dt.downtime_start:
            secs = (dt.downtime_end - dt.downtime_start).total_seconds()
            agg["hours"] += Decimal(secs) / Decimal("3600")

        # collect reasons (unique)
        if dt.reason:
            r = dt.reason.strip()
            if r and r not in agg["reasons"]:
                agg["reasons"].append(r)

    # ── build table rows for template ───────────────────────────────────
    table_rows = []
    for d, info in rows_dict.items():
        by_sub = info["by_sub"]

        row_vals = [_fmt_mee_value(by_sub.get(sub.id, "")) for sub in subcats]

        dt_info = dt_summary.get(d, None)
        if dt_info:
            dt_start = dt_info["start"]
            dt_end   = dt_info["end"]
            total_hours = (
                dt_info["hours"].quantize(Decimal("0.01"))
                if dt_info["hours"]
                else None
            )
            reason_text = "; ".join(dt_info["reasons"]) if dt_info["reasons"] else None
        else:
            dt_start = dt_end = total_hours = reason_text = None

        table_rows.append({
            "date": d,
            "values": row_vals,
            "downtime_start": dt_start,
            "downtime_end": dt_end,
            "total_downtime": total_hours,
            "reason": reason_text,
        })

    context = {
        "categories": categories,
        "subcats": subcats,
        "rows": table_rows,
        "from_date": from_str,
        "to_date": to_str,
    }
    return render(request, "MEE/mee_reading_list.html", context)





@login_required
def mee_reading_detail(request, reading_date):
    """
    Detail page for a single date:
    - shows ALL categories/subcategories values
    - shows ALL downtime rows for that date
    """
    if not request.user.has_perm("MEE.view_meedailyreading"):
        messages.error(request, "You do not have permission to view MEE readings.")
        return redirect("indexpage")

    try:
        target_date = datetime.strptime(reading_date, "%Y-%m-%d").date()
    except ValueError:
        raise Http404("Invalid date")

    # categories + ALL subcats
    categories = (
        MEEReadingCategory.objects
        .prefetch_related(
            Prefetch(
                "subcategories",
                queryset=MEEReadingSubCategory.objects.order_by("order", "id"),
            )
        )
        .order_by("order", "id")
    )

    all_subcats = []
    for cat in categories:
        for sub in cat.subcategories.all():
            all_subcats.append(sub)

    # readings for that date
    qs = (
        MEEDailyReading.objects
        .filter(reading_date=target_date)
        .select_related("subcategory", "subcategory__category")
    )

    by_sub = {}
    for r in qs:
        by_sub[r.subcategory_id] = r.value

    row_vals_all = [_fmt_mee_value(by_sub.get(sub.id, "")) for sub in all_subcats]

    # all downtime rows for that date (based on reading_date)
    dt_rows = (
        MEEDowntime.objects
        .select_related("reading")
        .filter(reading__reading_date=target_date)
        .order_by("downtime_start")
    )

    # 👉 total downtime (sum of hours) for the highlighted box
    total_dt_hours = (
        dt_rows.aggregate(total=Sum("downtime_hours"))["total"]
        if dt_rows.exists()
        else None )

    context = {
        "date": target_date,
        "categories": categories,
        "subcats": all_subcats,
        "values": row_vals_all,
        "downtimes": dt_rows,
        "total_downtime": total_dt_hours,   # <–– send to template
    }
    return render(request, "MEE/mee_reading_detail.html", context)






@login_required
def mee_reading_export_xlsx(request):
    logger.info("User=%s Download Mee Reading", request.user.username)
    from_str = (request.GET.get("from_date") or "").strip()
    to_str   = (request.GET.get("to_date") or "").strip()

    # ── default = current month if not specified ────────────────────────
    if not from_str and not to_str:
        today = date.today()
        first_day = today.replace(day=1)
        if today.month == 12:
            next_month_first = today.replace(year=today.year + 1, month=1, day=1)
        else:
            next_month_first = today.replace(month=today.month + 1, day=1)
        last_day = next_month_first - timedelta(days=1)

        from_date = first_day
        to_date   = last_day
    else:
        from_date = _parse_date_param(from_str)
        to_date   = _parse_date_param(to_str)

    # ── categories + ordered subcats ────────────────────────────────────
    categories = (
        MEEReadingCategory.objects
        .prefetch_related(
            Prefetch(
                "subcategories",
                queryset=MEEReadingSubCategory.objects.order_by("order", "id"),
            )
        )
        .order_by("order", "id")
    )

    subcats = []
    for cat in categories:
        for sub in cat.subcategories.all():
            subcats.append(sub)

    # ── readings with date range filter → dict[date] -> {sub_id: value} ─
    qs = (
        MEEDailyReading.objects
        .select_related("subcategory", "subcategory__category")
        .order_by("reading_date")
    )
    if from_date:
        qs = qs.filter(reading_date__gte=from_date)
    if to_date:
        qs = qs.filter(reading_date__lte=to_date)

    readings_by_date: "OrderedDict[date, dict]" = OrderedDict()
    for r in qs:
        d = r.reading_date
        if d not in readings_by_date:
            readings_by_date[d] = {}
        readings_by_date[d][r.subcategory_id] = r.value

    # ── downtimes per date (list of rows for each date) ─────────────────
    dt_qs = (
        MEEDowntime.objects
        .select_related("reading")
        .order_by("reading__reading_date", "downtime_start")
    )
    if from_date:
        dt_qs = dt_qs.filter(reading__reading_date__gte=from_date)
    if to_date:
        dt_qs = dt_qs.filter(reading__reading_date__lte=to_date)

    from collections import defaultdict
    downtimes_by_date = defaultdict(list)  # date -> [MEEDowntime,...]
    for dt in dt_qs:
        d = dt.reading.reading_date
        downtimes_by_date[d].append(dt)

    # ── Excel build ─────────────────────────────────────────────────────
    output   = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    ws       = workbook.add_worksheet("MEE Readings")

    # Formats
    title_fmt = workbook.add_format({
        "bold": True, "font_size": 14, "align": "center", "valign": "vcenter"
    })
    cat_fmt = workbook.add_format({
        "bold": True, "bg_color": "#D9EAD3", "border": 1,
        "align": "center", "valign": "vcenter"
    })
    sub_fmt = workbook.add_format({
        "bold": True, "bg_color": "#F0F0F0", "border": 1,
        "align": "center"
    })
    date_fmt = workbook.add_format({"num_format": "dd-mm-yyyy", "border": 1})
    cell_fmt = workbook.add_format({"border": 1})

    # 🔹 separate formats for int vs decimal
    num_int_fmt = workbook.add_format({
        "border": 1,
        "num_format": "0",
    })
    num_dec_fmt = workbook.add_format({
        "border": 1,
        "num_format": "0.###",
    })
    dt_hours_fmt = workbook.add_format({
        "border": 1,
        "num_format": "0.00",
    })

    # ── header rows ─────────────────────────────────────────────────────
    total_cols = 1 + len(subcats) + 4  # 1 Date + subcats + 4 downtime cols
    title = "MEE Readings"
    if from_date or to_date:
        fr = from_date.strftime("%d-%m-%Y") if from_date else "..."
        to = to_date.strftime("%d-%m-%Y") if to_date else "..."
        title += f" (From {fr} To {to})"

    row_title      = 0
    row_cat        = 2
    row_sub        = 3
    data_row_start = 4

    ws.merge_range(row_title, 0, row_title, total_cols, title, title_fmt)

    # Date column
    ws.merge_range(row_cat, 0, row_sub, 0, "Date", cat_fmt)
    ws.set_column(0, 0, 12)

    # Category + subcategory headers
    col = 1
    for cat in categories:
        sub_list = list(cat.subcategories.all())
        if not sub_list:
            continue
        start_col = col
        end_col   = col + len(sub_list) - 1
        cat_label = cat.name
        if cat.unit:
            cat_label += f" ({cat.unit})"
        ws.merge_range(row_cat, start_col, row_cat, end_col, cat_label, cat_fmt)

        for sub in sub_list:
            ws.write(row_sub, col, sub.name, sub_fmt)
            ws.set_column(col, col, 10)
            col += 1

    # Downtime columns
    ws.merge_range(row_cat, col, row_sub, col, "Downtime Start", cat_fmt)
    ws.set_column(col, col, 20)
    col += 1

    ws.merge_range(row_cat, col, row_sub, col, "Downtime End", cat_fmt)
    ws.set_column(col, col, 20)
    col += 1

    ws.merge_range(row_cat, col, row_sub, col, "Downtime (hrs)", cat_fmt)
    ws.set_column(col, col, 14)
    col += 1

    ws.merge_range(row_cat, col, row_sub, col, "Reason", cat_fmt)
    ws.set_column(col, col, 30)

    # ── helper to write a numeric cell with correct int/decimal format ──
    def write_number_dynamic(sheet, row, col_idx, value, merge_rows=None):
        """
        value -> Decimal or str/float
        if merge_rows is not None: (row, col) .. (merge_rows, col)
        """
        try:
            dval = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            # fallback as plain text
            if merge_rows is not None and merge_rows > row:
                sheet.merge_range(row, col_idx, merge_rows, col_idx, str(value), cell_fmt)
            else:
                sheet.write(row, col_idx, str(value), cell_fmt)
            return

        # integer or decimal?
        if dval == dval.to_integral():
            num = float(dval)
            fmt = num_int_fmt
        else:
            dval = dval.quantize(Decimal("0.001"))
            num  = float(dval)
            fmt  = num_dec_fmt

        if merge_rows is not None and merge_rows > row:
            sheet.merge_range(row, col_idx, merge_rows, col_idx, num, fmt)
        else:
            sheet.write_number(row, col_idx, num, fmt)

    # ── data rows (blocks per date, merged like earlier) ────────────────
    r = data_row_start

    for d, by_sub in readings_by_date.items():
        row_vals   = [by_sub.get(sub.id, "") for sub in subcats]
        dt_list    = downtimes_by_date.get(d, [])
        block_h    = max(1, len(dt_list))
        last_row   = r + block_h - 1

        # Date column
        dt_value = datetime.combine(d, datetime.min.time())
        if block_h > 1:
            ws.merge_range(r, 0, last_row, 0, dt_value, date_fmt)
        else:
            ws.write_datetime(r, 0, dt_value, date_fmt)

        # Reading columns
        c = 1
        for val in row_vals:
            if val in ("", None):
                if block_h > 1:
                    ws.merge_range(r, c, last_row, c, "", cell_fmt)
                else:
                    ws.write(r, c, "", cell_fmt)
            else:
                if block_h > 1:
                    write_number_dynamic(ws, r, c, val, merge_rows=last_row)
                else:
                    write_number_dynamic(ws, r, c, val)
            c += 1

        # Downtime rows
        if dt_list:
            row_idx = r
            for dt in dt_list:
                c_dt = 1 + len(subcats)

                # Start
                if dt.downtime_start:
                    ws.write(row_idx, c_dt,
                             dt.downtime_start.strftime("%d-%m-%Y %H:%M"), cell_fmt)
                else:
                    ws.write(row_idx, c_dt, "", cell_fmt)
                c_dt += 1

                # End
                if dt.downtime_end:
                    ws.write(row_idx, c_dt,
                             dt.downtime_end.strftime("%d-%m-%Y %H:%M"), cell_fmt)
                else:
                    ws.write(row_idx, c_dt, "", cell_fmt)
                c_dt += 1

                # Hours
                if dt.downtime_hours is not None:
                    try:
                        ws.write_number(
                            row_idx, c_dt, float(dt.downtime_hours), dt_hours_fmt
                        )
                    except (TypeError, ValueError):
                        ws.write(row_idx, c_dt, str(dt.downtime_hours), cell_fmt)
                else:
                    ws.write(row_idx, c_dt, "", cell_fmt)
                c_dt += 1

                # Reason
                ws.write(row_idx, c_dt, dt.reason or "", cell_fmt)

                row_idx += 1
        else:
            # no downtime
            c_dt = 1 + len(subcats)
            ws.write(r, c_dt,     "", cell_fmt)
            ws.write(r, c_dt + 1, "", cell_fmt)
            ws.write(r, c_dt + 2, "", cell_fmt)
            ws.write(r, c_dt + 3, "", cell_fmt)

        r += block_h

    workbook.close()
    output.seek(0)

    response = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="MEE_Readings.xlsx"'
    return response




def _parse_dt_or_time_local(base_date, value: str):
    if not value:
        return None
    value = value.strip()
    try:
        # Case 1: full datetime-local
        if "T" in value:
            return datetime.strptime(value, "%Y-%m-%dT%H:%M")
        # Case 2: time-only
        t = datetime.strptime(value, "%H:%M").time()
        return datetime.combine(base_date, t)
    except ValueError:
        return None



@login_required
def mee_reading_edit(request, reading_date):
    """
    Edit MEE readings (values) and downtime rows for a given reading_date.
    Uses MEEDailyReading + MEEDowntime (multiple rows).
    """
    if not request.user.has_perm("MEE.change_meedailyreading"):
        messages.error(request, "You do not have permission to edit MEE readings.")
        logger.warning(
            "User '%s' tried to edit MEE readings for date %s without permission.",
            request.user.username,
            reading_date,
        )
        return redirect("indexpage")

    # --- Parse URL date ---
    try:
        original_date = datetime.strptime(reading_date, "%Y-%m-%d").date()
    except ValueError:
        logger.error(
            "Invalid date '%s' passed to mee_reading_edit by user=%s",
            reading_date,
            request.user.username,
        )
        raise Http404("Invalid date")

    logger.info(
        "User=%s accessed MEE reading edit for date=%s",
        request.user.username,
        original_date,
    )

    # --- Categories + existing values ---
    categories = (
        MEEReadingCategory.objects
        .prefetch_related("subcategories")
        .order_by("order", "id")
    )

    qs_existing = MEEDailyReading.objects.filter(reading_date=original_date)
    existing_values = {str(r.subcategory_id): r.value for r in qs_existing}

    # Pre-fill value inputs
    for cat in categories:
        for sub in cat.subcategories.all():
            sub.initial_value = existing_values.get(str(sub.id), "")

    # Existing downtimes for this date (may be 0..n)
    existing_downtimes = MEEDowntime.objects.filter(
        reading__reading_date=original_date
    ).order_by("downtime_start")

    if request.method == "POST":
        # New date (user may change it), else keep original
        new_date_str = request.POST.get("reading_date") or reading_date
        try:
            new_date = datetime.strptime(new_date_str, "%Y-%m-%d").date()
        except ValueError:
            logger.warning(
                "Invalid new_date '%s' in MEE edit; falling back to original_date=%s, user=%s",
                new_date_str,
                original_date,
                request.user.username,
            )
            new_date = original_date

        # Downtime arrays from the form
        dt_starts  = request.POST.getlist("dt_start[]")
        dt_ends    = request.POST.getlist("dt_end[]")
        dt_reasons = request.POST.getlist("dt_reason[]")

        with transaction.atomic():
            # Delete old readings (their downtimes are cascaded)
            MEEDailyReading.objects.filter(reading_date=original_date).delete()

            anchor_reading = None
            created_count = 0

            # Recreate readings for the (possibly new) date
            for key, val in request.POST.items():
                if not key.startswith("value_"):
                    continue
                val = val.strip()
                if not val:
                    continue

                subcat_id = key.split("_", 1)[1]

                reading_obj = MEEDailyReading.objects.create(
                    reading_date=new_date,
                    subcategory_id=subcat_id,
                    value=val,
                    entered_by=request.user,
                )
                created_count += 1

                if anchor_reading is None:
                    anchor_reading = reading_obj

            # Recreate downtime rows, attached to anchor_reading
            if anchor_reading:
                for start_str, end_str, reason_str in zip(dt_starts, dt_ends, dt_reasons):
                    start_dt = _parse_dt_local(start_str)
                    end_dt   = _parse_dt_local(end_str)
                    reason   = (reason_str or "").strip() or None

                    # Skip completely empty rows
                    if not start_dt and not end_dt and not reason:
                        continue
                    # Require valid start/end
                    if not start_dt or not end_dt or end_dt <= start_dt:
                        continue

                    MEEDowntime.objects.create(
                        reading=anchor_reading,
                        downtime_start=start_dt,
                        downtime_end=end_dt,
                        reason=reason,
                    )

        logger.info(
            "User=%s updated MEE readings for original_date=%s -> new_date=%s (rows=%d)",
            request.user.username,
            original_date,
            new_date,
            created_count,
        )
        messages.success(
            request,
            f"MEE readings updated successfully for {new_date.strftime('%d-%m-%Y')}."
        )
        return redirect("mee:mee_reading_list")

    # GET
    ctx = {
        "categories": categories,
        "selected_date": original_date,
        "edit_mode": True,
        # you can use this in template later to pre-fill downtime rows
        "existing_downtimes": existing_downtimes,
    }
    return render(request, "MEE/mee_reading_form.html", ctx)







@login_required
def mee_reading_delete(request, reading_date):
    # ---- Permission check ----
    if not request.user.has_perm('MEE.delete_meedailyreading'):
        messages.error(request, "You do not have permission to delete MEE readings.")
        logger.warning("User '%s' tried to delete MEE readings for date %s without permission.",
            request.user.username, reading_date,)
        return redirect('indexpage')
    # --------------------------
    try:
        d = datetime.strptime(reading_date, "%Y-%m-%d").date()
    except ValueError:
        logger.error( "Invalid date format '%s' in mee_reading_delete by user=%s", reading_date,
            request.user.username, )
        messages.error(request, "Invalid date format.")
        return redirect("mee:mee_reading_list")
    qs = MEEDailyReading.objects.filter(reading_date=d)
    if qs.exists():
        deleted_count = qs.count()
        qs.delete()
        logger.info("User=%s deleted %d MEE readings for date=%s",request.user.username,
            deleted_count, d, )
        messages.success(request, f"MEE readings deleted for {d.strftime('%d-%m-%Y')}." )
    else:
        logger.info( "User=%s attempted delete but no MEE readings found for date=%s",request.user.username,
            d,)
        messages.info( request, f"No MEE readings found for {d.strftime('%d-%m-%Y')}." )
    return redirect("mee:mee_reading_list")


















# =========================BELOW CODE FOR ATFD READING ========================================================================


def _compute_atfd_qty(effluent_feed, effluent_feed_spgr, atfd_salt):
    """
    atfd_qty = effluent_feed * effluent_feed_spgr - atfd_salt
    """
    try:
        if effluent_feed is None or effluent_feed_spgr is None or atfd_salt is None:
            return None
        val = (Decimal(effluent_feed) * Decimal(effluent_feed_spgr)) - Decimal(atfd_salt)
        return val.quantize(Decimal("0.01"))
    except (InvalidOperation, TypeError):
        return None


@login_required
def atfd_entry(request):
    """
    Create/Update ATFDReading by reading_date (unique).
    Handles multiple ATFDDowntime rows from dt_start[]/dt_end[]/dt_reason[].
    """

    # ---- Permission check ----
    # (Adjust permission codename to your actual app/model)
    if not request.user.has_perm("MEE.add_atfdreading") and not request.user.has_perm("MEE.change_atfdreading"):
        messages.error(request, "You do not have permission to add/update ATFD readings.")
        logger.warning(
            "User='%s' tried to access ATFD entry without permission | method=%s",
            request.user.username,
            request.method,
        )
        return redirect("indexpage")

    selected_date = None
    if request.method == "GET":
        qd = request.GET.get("date")
        selected_date = parse_date(qd) if qd else None

        logger.info(
            "User='%s' opened ATFD entry page | date_param='%s' parsed_date='%s'",
            request.user.username,
            qd,
            selected_date,
        )

    instance = None
    edit_mode = False
    existing_downtimes = []

    if selected_date:
        instance = ATFDReading.objects.filter(reading_date=selected_date).first()
        if instance:
            edit_mode = True
            existing_downtimes = list(instance.downtimes.all())

        logger.info(
            "User='%s' ATFD load | selected_date='%s' | edit_mode=%s | existing_downtimes=%s",
            request.user.username,
            selected_date,
            edit_mode,
            len(existing_downtimes),
        )

    if request.method == "POST":
        post_date_raw = request.POST.get("reading_date") or ""
        post_date = parse_date(post_date_raw)

        logger.info(
            "User='%s' submitted ATFD entry | reading_date_raw='%s' parsed='%s'",
            request.user.username,
            post_date_raw,
            post_date,
        )

        if not post_date:
            messages.error(request, "Reading Date is required.")
            logger.warning(
                "User='%s' ATFD submit failed: missing/invalid reading_date | raw='%s'",
                request.user.username,
                post_date_raw,
            )
            return redirect(request.path)

        instance = ATFDReading.objects.filter(reading_date=post_date).first()
        form = ATFDReadingForm(request.POST, instance=instance)

        # Build downtime objects from POST first (validate before saving)
        starts = request.POST.getlist("dt_start[]")
        ends = request.POST.getlist("dt_end[]")
        reasons = request.POST.getlist("dt_reason[]")

        logger.info(
            "User='%s' ATFD downtimes received | rows=%s",
            request.user.username,
            max(len(starts), len(ends), len(reasons)),
        )

        dt_to_create = []
        dt_errors = []

        for idx, (s, e, r) in enumerate(zip_longest(starts, ends, reasons, fillvalue=""), start=1):
            s = (s or "").strip()
            e = (e or "").strip()
            r = (r or "").strip()

            # allow fully empty row
            if not s and not e and not r:
                continue

            if not s or not e:
                dt_errors.append(f"Row {idx}: Please fill both Start and End time.")
                continue

            sdt = parse_datetime(s)
            edt = parse_datetime(e)
            if not sdt or not edt:
                dt_errors.append(f"Row {idx}: Invalid date/time format.")
                continue
            if edt <= sdt:
                dt_errors.append(f"Row {idx}: Start must be less than End.")
                continue

            dt_to_create.append(ATFDDowntime(downtime_start=sdt, downtime_end=edt, reason=r))

        if not form.is_valid():
            messages.error(request, "Please correct the highlighted errors.")
            logger.error(
                "ATFDReadingForm invalid | user=%s | date=%s | errors=%s",
                request.user.username,
                post_date,
                form.errors.as_json(),
            )
        elif dt_errors:
            messages.error(request, " | ".join(dt_errors))
            logger.warning(
                "User='%s' ATFD downtime validation failed | date=%s | errors=%s",
                request.user.username,
                post_date,
                dt_errors,
            )
        else:
            try:
                with transaction.atomic():
                    reading = form.save(commit=False)

                    # calculate atfd_qty here
                    reading.atfd_qty = _compute_atfd_qty(
                        reading.effluent_feed,
                        reading.effluent_feed_spgr,
                        reading.atfd_salt,
                    )

                    reading.save()

                    # Replace downtimes
                    deleted_count = ATFDDowntime.objects.filter(reading=reading).count()
                    ATFDDowntime.objects.filter(reading=reading).delete()
                    for dt in dt_to_create:
                        dt.reading = reading
                        dt.save()

                logger.info(
                    "User='%s' saved ATFD reading | reading_date=%s | edit=%s | "
                    "atfd_qty=%s | downtimes_saved=%s | downtimes_deleted=%s",
                    request.user.username,
                    reading.reading_date,
                    bool(instance),
                    reading.atfd_qty,
                    len(dt_to_create),
                    deleted_count,
                )

                messages.success(request, "ATFD Reading saved successfully.")
                return redirect(reverse("mee:atfd_reading_form") + f"?date={reading.reading_date}")

            except Exception as e:
                logger.exception(
                    "ATFD save failed | user=%s | date=%s | error=%s",
                    request.user.username,
                    post_date,
                    str(e),
                )
                messages.error(request, "An unexpected error occurred while saving. Please try again.")

        # re-render with current form (and current db downtimes)
        instance = ATFDReading.objects.filter(reading_date=post_date).first()
        edit_mode = bool(instance)
        existing_downtimes = list(instance.downtimes.all()) if instance else []

        logger.info(
            "User='%s' ATFD re-render after POST | date=%s | edit_mode=%s | existing_downtimes=%s",
            request.user.username,
            post_date,
            edit_mode,
            len(existing_downtimes),
        )

    else:
        form = ATFDReadingForm(instance=instance)

    context = {
        "form": form,
        "edit_mode": edit_mode,
        "existing_downtimes": existing_downtimes,
        "selected_date": selected_date,
    }
    return render(request, "ATFD/atfd_entry.html", context)



@login_required
def atfd_reading_list(request):
    from_date_str = (request.GET.get("from_date") or "").strip()
    to_date_str   = (request.GET.get("to_date") or "").strip()

    from_date = parse_date(from_date_str) if from_date_str else None
    to_date   = parse_date(to_date_str) if to_date_str else None

    qs = ATFDReading.objects.all()

    if from_date:
        qs = qs.filter(reading_date__gte=from_date)
    if to_date:
        qs = qs.filter(reading_date__lte=to_date)

    qs = qs.annotate(
        total_downtime=Coalesce(
            Sum("downtimes__downtime_hours"),
            Value(Decimal("0.00")),
            output_field=DecimalField(max_digits=7, decimal_places=2),
        )
    ).order_by("-reading_date")[:200]

    return render(
        request,
        "ATFD/atfd_reading_list.html",
        {
            "rows": qs,
            "from_date": from_date_str,
            "to_date": to_date_str,
            "export_enabled": True,
        },
    )


@login_required
def atfd_reading_detail(request, reading_date):
    """
    Detail page for one ATFD reading day + all downtime rows + total downtime.
    reading_date format: 'YYYY-MM-DD'
    """
    d = parse_date(reading_date)
    if not d:
        # fallback: 404 if date invalid
        obj = get_object_or_404(ATFDReading, reading_date="0001-01-01")  # will 404
        # (won't reach here)

    obj = get_object_or_404(ATFDReading, reading_date=d)

    downtimes = obj.downtimes.all().order_by("downtime_start")

    total_downtime = (
        downtimes.aggregate(total=Sum("downtime_hours"))["total"]
        or Decimal("0.00")
    )
    # Keep as 2 decimal string/Decimal
    total_downtime = total_downtime.quantize(Decimal("0.01"))

    context = {
        "obj": obj,
        "date": obj.reading_date,
        "downtimes": downtimes,
        "total_downtime": total_downtime,
    }
    return render(request, "atfd/atfd_reading_detail.html", context)



@login_required
def atfd_reading_edit(request, reading_date):
    """
    Edit ATFDReading by reading_date (YYYY-MM-DD) using the SAME template as entry.
    """
    d = parse_date(reading_date)
    if not d:
        # force 404 for invalid date
        get_object_or_404(ATFDReading, reading_date="0001-01-01")

    instance = get_object_or_404(ATFDReading, reading_date=d)

    if request.method == "POST":
        form = ATFDReadingForm(request.POST, instance=instance)

        # Downtime rows from POST
        starts = request.POST.getlist("dt_start[]")
        ends = request.POST.getlist("dt_end[]")
        reasons = request.POST.getlist("dt_reason[]")

        dt_to_create = []
        dt_errors = []

        for idx, (s, e, r) in enumerate(zip_longest(starts, ends, reasons, fillvalue=""), start=1):
            s = (s or "").strip()
            e = (e or "").strip()
            r = (r or "").strip()

            # allow fully empty row
            if not s and not e and not r:
                continue

            # must have both start and end
            if not s or not e:
                dt_errors.append(f"Row {idx}: Please fill both Start and End time.")
                continue

            sdt = parse_datetime(s)
            edt = parse_datetime(e)

            if not sdt or not edt:
                dt_errors.append(f"Row {idx}: Invalid date/time format.")
                continue
            if edt <= sdt:
                dt_errors.append(f"Row {idx}: Start must be less than End.")
                continue

            dt_to_create.append(
                ATFDDowntime(downtime_start=sdt, downtime_end=edt, reason=r)
            )

        if not form.is_valid():
            messages.error(request, "Please correct the highlighted errors.")
        elif dt_errors:
            messages.error(request, " | ".join(dt_errors))
        else:
            with transaction.atomic():
                reading = form.save()  # steam_economy + atfd_qty auto in model save

                # Replace downtimes
                ATFDDowntime.objects.filter(reading=reading).delete()
                for dt in dt_to_create:
                    dt.reading = reading
                    dt.save()

            messages.success(request, "ATFD Reading updated successfully.")
            return redirect(
                reverse("mee:atfd_reading_edit", args=[reading.reading_date.strftime("%Y-%m-%d")])
            )

    else:
        form = ATFDReadingForm(instance=instance)

    context = {
        "form": form,
        "edit_mode": True,
        "existing_downtimes": list(instance.downtimes.all().order_by("downtime_start")),
        "selected_date": instance.reading_date,
    }
    return render(request, "ATFD/atfd_entry.html", context)



@login_required
def atfd_reading_export_xlsx(request):
    from_date_str = (request.GET.get("from_date") or "").strip()
    to_date_str   = (request.GET.get("to_date") or "").strip()

    from_date = parse_date(from_date_str) if from_date_str else None
    to_date   = parse_date(to_date_str) if to_date_str else None

    qs = ATFDReading.objects.all()
    if from_date:
        qs = qs.filter(reading_date__gte=from_date)
    if to_date:
        qs = qs.filter(reading_date__lte=to_date)

    qs = qs.order_by("-reading_date")[:5000]

    # ---- downtimes map: {reading_date: [dt, dt, ...]} ----
    dt_qs = ATFDDowntime.objects.select_related("reading").order_by(
        "reading__reading_date", "downtime_start"
    )
    if from_date:
        dt_qs = dt_qs.filter(reading__reading_date__gte=from_date)
    if to_date:
        dt_qs = dt_qs.filter(reading__reading_date__lte=to_date)

    dt_map = {}
    for dt in dt_qs:
        dt_map.setdefault(dt.reading.reading_date, []).append(dt)

    # ---- timezone-safe datetime for Excel ----
    from django.utils import timezone
    from datetime import datetime
    def _naive_dt(dt):
        if not dt:
            return None
        if timezone.is_aware(dt):
            dt = timezone.localtime(dt).replace(tzinfo=None)
        return dt

    output = BytesIO()
    wb = xlsxwriter.Workbook(output, {"in_memory": True, "remove_timezone": True})
    ws = wb.add_worksheet("ATFD Readings")

    # ---------- Colors ----------
    C_EMERALD_100 = "#D1FAE5"
    C_SKY_200     = "#BAE6FD"
    C_SKY_100     = "#E0F2FE"
    C_ORANGE_200  = "#FED7AA"
    C_ORANGE_100  = "#FFEDD5"
    C_YELLOW_100  = "#FEF08A"
    C_GRAY_BORDER = "#6B7280"

    # ---------- Formats ----------
    title_fmt = wb.add_format({"bold": True, "font_size": 14, "align": "center", "valign": "vcenter"})
    subtitle_fmt = wb.add_format({"font_size": 10, "align": "center", "valign": "vcenter", "italic": True})

    hdr_main = wb.add_format({
        "bold": True, "bg_color": C_EMERALD_100,
        "border": 1, "border_color": C_GRAY_BORDER,
        "align": "center", "valign": "vcenter", "text_wrap": True
    })
    hdr_group_feed = wb.add_format({
        "bold": True, "bg_color": C_SKY_200,
        "border": 1, "border_color": C_GRAY_BORDER,
        "align": "center", "valign": "vcenter"
    })
    hdr_group_vapor = wb.add_format({
        "bold": True, "bg_color": C_ORANGE_200,
        "border": 1, "border_color": C_GRAY_BORDER,
        "align": "center", "valign": "vcenter"
    })
    hdr_sub_feed = wb.add_format({
        "bold": True, "bg_color": C_SKY_100,
        "border": 1, "border_color": C_GRAY_BORDER,
        "align": "center", "valign": "vcenter"
    })
    hdr_sub_vapor = wb.add_format({
        "bold": True, "bg_color": C_ORANGE_100,
        "border": 1, "border_color": C_GRAY_BORDER,
        "align": "center", "valign": "vcenter"
    })
    hdr_date = wb.add_format({
        "bold": True, "bg_color": "#FFFFFF",
        "border": 1, "border_color": C_GRAY_BORDER,
        "align": "center", "valign": "vcenter"
    })

    date_fmt = wb.add_format({"num_format": "dd-mm-yyyy", "border": 1, "border_color": "#D1D5DB", "align": "center"})
    dt_dt_fmt = wb.add_format({"num_format": "dd-mm-yyyy hh:mm", "border": 1, "border_color": "#D1D5DB", "align": "center"})
    dt_hours_fmt = wb.add_format({"num_format": "0.00", "border": 1, "border_color": "#D1D5DB", "align": "right"})

    num_3 = wb.add_format({"num_format": "0.000", "border": 1, "border_color": "#D1D5DB", "align": "right"})
    num_4 = wb.add_format({"num_format": "0.0000", "border": 1, "border_color": "#D1D5DB", "align": "right"})
    num_2 = wb.add_format({"num_format": "0.00", "border": 1, "border_color": "#D1D5DB", "align": "right"})
    num_0 = wb.add_format({"num_format": "0", "border": 1, "border_color": "#D1D5DB", "align": "right"})
    txt = wb.add_format({"border": 1, "border_color": "#D1D5DB"})
    txt_center = wb.add_format({"border": 1, "border_color": "#D1D5DB", "align": "center"})

    hi_yellow_4 = wb.add_format({"num_format": "0.0000", "bg_color": C_YELLOW_100, "bold": True, "border": 1, "border_color": "#D1D5DB", "align": "right"})
    hi_yellow_2 = wb.add_format({"num_format": "0.00", "bg_color": C_YELLOW_100, "bold": True, "border": 1, "border_color": "#D1D5DB", "align": "right"})

    # ---------- Columns (same sheet includes downtime columns at right) ----------
    # 0..15 = readings, 16..19 = downtime start/end/hrs/reason
    col_defs = [
        ("Date", 12),
        ("Effluent Feed (KL)", 16),
        ("Steam Consume (MT)", 18),
        ("Steam Economy (F/S ratio)", 20),
        ("Blower Draft (mmWC)", 18),
        ("Steam Inlet Pressure (kg/cm2)", 22),
        ("ATFD RPM", 10),
        ("ATFD Salt (MT)", 14),

        ("pH", 8),
        ("TDS (%)", 10),
        ("COD (%)", 10),
        ("SpGr.", 10),

        ("Quantity (KL)", 14),
        ("pH", 8),
        ("TDS (%)", 10),
        ("COD (%)", 10),

        ("Downtime Start", 20),
        ("Downtime End", 20),
        ("Downtime (hrs)", 14),
        ("Reason", 24),
    ]
    for i, (_, w) in enumerate(col_defs):
        ws.set_column(i, i, w)

    last_col = len(col_defs) - 1  # 19

    # ---------- Title ----------
    ws.merge_range(0, 0, 0, last_col, "ATFD Readings Report", title_fmt)
    subtitle = f"From: {from_date_str or '-'}    To: {to_date_str or '-'}" if (from_date_str or to_date_str) else "All Dates"
    ws.merge_range(1, 0, 1, last_col, subtitle, subtitle_fmt)
    ws.set_row(0, 22)
    ws.set_row(1, 16)

    # ---------- Headers (2 rows grouped like your image) ----------
    group_row = 3
    sub_row = 4
    ws.set_row(group_row, 22)
    ws.set_row(sub_row, 20)

    # Date (rowspan)
    ws.merge_range(group_row, 0, sub_row, 0, "Date", hdr_date)

    # Main green columns (rowspan)
    ws.merge_range(group_row, 1, sub_row, 1, "Effluent Feed\n(KL)", hdr_main)
    ws.merge_range(group_row, 2, sub_row, 2, "Steam\nConsume (MT)", hdr_main)
    ws.merge_range(group_row, 3, sub_row, 3, "Steam Economy\n(F/S ratio)", hdr_main)
    ws.merge_range(group_row, 4, sub_row, 4, "Blower Draft\n(mmWC)", hdr_main)
    ws.merge_range(group_row, 5, sub_row, 5, "Steam Inlet Pressure\n(kg/cm2)", hdr_main)
    ws.merge_range(group_row, 6, sub_row, 6, "ATFD\nRPM", hdr_main)
    ws.merge_range(group_row, 7, sub_row, 7, "ATFD Salt\n(MT)", hdr_main)

    # Groups
    ws.merge_range(group_row, 8,  group_row, 11, "Feed Effluent", hdr_group_feed)
    ws.merge_range(group_row, 12, group_row, 15, "ATFD Vapor Condensate", hdr_group_vapor)

    # Downtime columns (rowspan headers on same sheet)
    ws.merge_range(group_row, 16, sub_row, 16, "Downtime Start", hdr_main)
    ws.merge_range(group_row, 17, sub_row, 17, "Downtime End", hdr_main)
    ws.merge_range(group_row, 18, sub_row, 18, "Downtime (hrs)", hdr_main)
    ws.merge_range(group_row, 19, sub_row, 19, "Reason", hdr_main)

    # Sub headers
    ws.write(sub_row, 8,  "pH",      hdr_sub_feed)
    ws.write(sub_row, 9,  "TDS (%)", hdr_sub_feed)
    ws.write(sub_row, 10, "COD (%)", hdr_sub_feed)
    ws.write(sub_row, 11, "SpGr.",   hdr_sub_feed)

    ws.write(sub_row, 12, "Quantity (KL)", hdr_sub_vapor)
    ws.write(sub_row, 13, "pH",            hdr_sub_vapor)
    ws.write(sub_row, 14, "TDS (%)",       hdr_sub_vapor)
    ws.write(sub_row, 15, "COD (%)",       hdr_sub_vapor)

    # ---------- helpers to write merged blocks ----------
    def _write_merged_text(r0, c, r1, text, fmt):
        if r1 > r0:
            ws.merge_range(r0, c, r1, c, text, fmt)
        else:
            ws.write(r0, c, text, fmt)

    def _write_merged_number(r0, c, r1, val, fmt_num, fmt_txt=txt, none_text="-"):
        if val is None:
            _write_merged_text(r0, c, r1, none_text, fmt_txt)
            return
        try:
            num = float(val)
        except (TypeError, ValueError):
            _write_merged_text(r0, c, r1, str(val), fmt_txt)
            return

        if r1 > r0:
            ws.merge_range(r0, c, r1, c, num, fmt_num)
        else:
            ws.write_number(r0, c, num, fmt_num)

    # ---------- Data (blocks per date with multiple downtime rows) ----------
    start_row = 5
    ws.freeze_panes(start_row, 1)

    r = start_row
    for rec in qs:
        d = rec.reading_date
        dt_list = dt_map.get(d, [])
        block_h = max(1, len(dt_list))
        last_row = r + block_h - 1

        # Date (merge across block)
        if d:
            dt_value = datetime.combine(d, datetime.min.time())
            if block_h > 1:
                ws.merge_range(r, 0, last_row, 0, dt_value, date_fmt)
            else:
                ws.write_datetime(r, 0, dt_value, date_fmt)
        else:
            _write_merged_text(r, 0, last_row, "-", txt_center)

        # Reading columns merged across block_h
        _write_merged_number(r, 1,  last_row, rec.effluent_feed,       num_3)
        _write_merged_number(r, 2,  last_row, rec.steam_consume,       num_3)
        _write_merged_number(r, 3,  last_row, rec.steam_economy,       hi_yellow_4)
        _write_merged_number(r, 4,  last_row, rec.blower_draft,        num_3)
        _write_merged_number(r, 5,  last_row, rec.steam_inlet_pressure,num_3)
        _write_merged_number(r, 6,  last_row, rec.atfd_rpm,            num_0)
        _write_merged_number(r, 7,  last_row, rec.atfd_salt,           num_3)

        _write_merged_number(r, 8,  last_row, rec.effluent_feed_ph,    num_2)
        _write_merged_number(r, 9,  last_row, rec.effluent_feed_TDS,   num_2)
        _write_merged_number(r, 10, last_row, rec.effluent_feed_cod,   num_2)
        _write_merged_number(r, 11, last_row, rec.effluent_feed_spgr,  num_3)

        _write_merged_number(r, 12, last_row, rec.atfd_qty,            hi_yellow_2)
        _write_merged_number(r, 13, last_row, rec.vapor_contensate_ph, num_2)
        _write_merged_number(r, 14, last_row, rec.vapor_contensate_tds,num_2)
        _write_merged_number(r, 15, last_row, rec.vapor_contensate_cod,num_2)

        # Downtime rows (not merged)
        for i in range(block_h):
            rr = r + i
            dt = dt_list[i] if i < len(dt_list) else None

            if dt:
                sdt = _naive_dt(dt.downtime_start)
                edt = _naive_dt(dt.downtime_end)

                if sdt:
                    ws.write_datetime(rr, 16, sdt, dt_dt_fmt)
                else:
                    ws.write(rr, 16, "-", txt)

                if edt:
                    ws.write_datetime(rr, 17, edt, dt_dt_fmt)
                else:
                    ws.write(rr, 17, "-", txt)

                if dt.downtime_hours is not None:
                    try:
                        ws.write_number(rr, 18, float(dt.downtime_hours), dt_hours_fmt)
                    except (TypeError, ValueError):
                        ws.write(rr, 18, str(dt.downtime_hours), txt)
                else:
                    ws.write(rr, 18, "-", txt)

                ws.write(rr, 19, dt.reason or "", txt)
            else:
                ws.write(rr, 16, "-", txt)
                ws.write(rr, 17, "-", txt)
                ws.write(rr, 18, "-", txt)
                ws.write(rr, 19, "", txt)

        r += block_h

    wb.close()
    output.seek(0)

    filename = "ATFD_Readings.xlsx"
    resp = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp





@login_required
def atfd_reading_delete(request, reading_date):
    d = parse_date(reading_date)
    if not d:
        raise Http404("Invalid reading date")
    # Use reading_date lookup (safe even if pk is not reading_date)
    obj = get_object_or_404(ATFDReading, reading_date=d)
    if request.method == "POST":
        obj.delete()
        messages.success(request, "Deleted successfully.")
        return redirect("mee:atfd_reading_list")
    # If someone opens URL directly, just go back
    return redirect("mee:atfd_reading_list")