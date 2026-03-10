from itertools import groupby
from operator import attrgetter
from django.shortcuts import render, redirect, get_object_or_404, get_list_or_404
from django.forms import formset_factory
from django.contrib import messages
from django.db.models import Max
from django.http import Http404,HttpResponse
from decimal import Decimal
from .forms import *
from .models import *
from django.shortcuts import render
import sys # Optional: For debugging print statements
from decimal import Decimal, InvalidOperation
from collections import defaultdict
from datetime import datetime,date
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
import xlsxwriter
from io import BytesIO
from django.utils import timezone
import io
from datetime import datetime
import logging
from django.db.models import F, Sum, Value
from django.db.models.functions import Coalesce
import json


logger = logging.getLogger('custom_logger')

#Add entry function


@login_required
def entry_view(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('UTILITY.add_utilityrecord'):
        messages.error(request, "You do not have permission to Add Utility records.")
        return redirect('indexpage')

    UtilityFormSet = formset_factory(UtilityRecordForm, extra=0)

    if request.method == "POST":
        date = request.POST.get("date")
        formset = UtilityFormSet(request.POST)
        if formset.is_valid():
            # Remove any existing for that date
            UtilityRecord.objects.filter(reading_date=date).delete()
            for form in formset:
                rec = form.save(commit=False)
                rec.reading_date = date
                rec.save()
            messages.success(request, f"✅utility Readings saved!")
            return redirect("utility_readings_report")
    else:
        # One blank form per TYPE_CHOICES
        initial = [{"reading_type": t} for t, _ in TYPE_CHOICES]
        formset = UtilityFormSet(initial=initial)

    return render(request, "utility/boiler_steam/utility_entry.html", {
        "formset": formset,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
    })
    
import re
 
@login_required
def edit_utility_date(request, date_str):
    user_groups  = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
 
    if not request.user.has_perm('UTILITY.change_utilityrecord'):
        messages.error(request, "You do not have permission to update Utility records.")
        return redirect('indexpage')
 
    UtilityFormSet = formset_factory(UtilityRecordForm, extra=0)
 
    # Clean & parse strictly as ISO (YYYY-MM-DD)
    date_str = re.sub(r"[^\d-]", "", (date_str or "").strip())
    try:
        date_obj = date.fromisoformat(date_str)  # <-- robust ISO parse
    except ValueError:
        # print("BAD DATE ->", repr(date_str))
        messages.error(request, f"Invalid date format: {date_str}. Use YYYY-MM-DD.")
        return redirect("utility_readings_report")
 
    batch_qs = UtilityRecord.objects.filter(reading_date=date_obj).order_by('reading_type')
    if not batch_qs.exists():
        messages.error(request, f"No records found for {date_obj}.")
        return redirect("utility_readings_report")
 
    if request.method == "POST":
        formset = UtilityFormSet(request.POST)
        if formset.is_valid():
            UtilityRecord.objects.filter(reading_date=date_obj).delete()
            for form in formset:
                rec = form.save(commit=False)
                rec.reading_date = date_obj
                rec.save()
            messages.success(request, f"✅ Readings updated for {date_obj}!")
            return redirect("utility_readings_report")
    else:
        type_map = {rec.reading_type: rec for rec in batch_qs}
        initial = []
        for t, _ in TYPE_CHOICES:
            rec = type_map.get(t)
            if rec:
                data = {"reading_type": rec.reading_type}
                for fld in UtilityRecord._meta.fields:
                    name = fld.name
                    if name not in ("id", "reading_date", "reading_type"):
                        data[name] = getattr(rec, name)
                initial.append(data)
            else:
                initial.append({"reading_type": t})
        formset = UtilityFormSet(initial=initial)
 
    return render(request, "utility/boiler_steam/utility_entry.html", {
        "formset": formset,
        "edit_date": date_obj,
        "user_groups": user_groups,
        "is_superuser": is_superuser,
    })

#Delete reading date
@login_required
def delete_utility_date(request, date_str):
    user_groups = request.user.groups.values_list('name', flat=True)  # Check if the user is in HR group
    is_superuser = request.user.is_superuser

    """ View UTILITY details (Permission Required: UTILITY.delete_utilityrecord) """
    if not request.user.has_perm('UTILITY.delete_utilityrecord'):
        messages.error(request, "You do not have permission to Delete Utility records.")
        return redirect('indexpage')
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        messages.error(request, "Invalid date format.")
        return redirect("utility_readings_report")

    records = UtilityRecord.objects.filter(reading_date=date_obj)
    if not records.exists():
        messages.error(request, "No records found for the selected date.")
        return redirect('utility_readings_report')

    if request.method == "POST":
        count, _ = records.delete()
    messages.success(request, f"Deleted record(s) for {date_str}.")
    return redirect('utility_readings_report')

   

# -----------------------------------------------------------------------------------------------------

ACTUAL_TYPE_FIELDS_OWNERSHIP = {
    "STEAM GENERATION READING": ["sb_3_e_22_main_fm_fv", "sb_3_sub_fm_oc"],
    "STEAM CONSUMPTION READING": [
        "block_a_reading", "block_b_reading", "mee_total_reading", "stripper_reading",
        "old_atfd", "mps_d_block_reading", "lps_e_17", "mps_e_17",
        "jet_ejector_atfd_c", "deareator", "new_atfd"
    ],
    "Boiler Water meter Reading": ["boiler_water_meter"],
    "MIDC reading": ["midc_water_e_18", "midc_water_e_17", "midc_water_e_22", "midc_water_e_16", "midc_water_e_20"],
    "BRIQUETTE": ["briquette_sb_3","briquette_tfh"],
    "DM Water consumed for boiler": ["dm_water_for_boiler"], # Not in Excel, but in model
}


DISPLAY_HEADER_STRUCTURE = [
    {
        'group_label': 'STEAM GENERATION READING',
        'fields': [
            ('sb_3_e_22_main_fm_fv', 'SB-3 (E-22)<br>(Main FM FV)'),
            ('sb_3_sub_fm_oc', 'SB-3 (Sub FM OC)'),
        ],
        'group_bg_color': 'bg-green-200',
        'cell_bg_color': 'bg-green-50'
    },
    {
        'group_label': 'STEAM CONSUMPTION READING',
        'fields': [
            ('block_a_reading', 'Block-A<br>Reading'),
            ('block_b_reading', 'Block_B<br>Reading'),
            ('mee_total_reading', 'MEE<br>Total<br>Reading'),
            ('stripper_reading', 'Stripper<br>Reading'),
            ('old_atfd', 'Old ATFD'),
            ('mps_d_block_reading', 'MPS D-<br>block<br>reading'),
            ('lps_e_17', 'LPS E-17'),
            ('mps_e_17', 'MPS E-17'),            
            ('new_atfd', 'New ATFD'),
        ],
        'group_bg_color': 'bg-blue-200',
        'cell_bg_color': 'bg-blue-50'
    },
    { # Grouping Boiler Water alone or with other general water readings
        'group_label': ' ', # Empty group label as it's a single main column in Excel
        'fields': [
            ('boiler_water_meter', 'Boiler<br>Water<br>meter<br>Reading'),
        ],
        'group_bg_color': 'bg-purple-200', # No group header in excel, so cell color dominates
        'cell_bg_color': 'bg-purple-50'
    },
    {
        'group_label': 'MIDC reading',
        'fields': [
            ('midc_water_e_18', 'MIDC<br>Water<br>Reading<br>E-18'),
            ('midc_water_e_17', 'MIDC<br>Water<br>Reading<br>E-17'),
            ('midc_water_e_22', 'MIDC<br>Water<br>Reading<br>E-22'),
            ('midc_water_e_16', 'MIDC<br>Water<br>Reading<br>E-16'),
            ('midc_water_e_20', 'MIDC<br>Water<br>Reading<br>E-20'), # In model, not Excel. Uncomment to display.
        ],
        'group_bg_color': 'bg-sky-200',
        'cell_bg_color': 'bg-sky-50'
    },
]

# This flat list determines the order of data cells in each row.
ORDERED_DISPLAY_FIELDS = []
for group in DISPLAY_HEADER_STRUCTURE:
    for field_name, _ in group['fields']:
        ORDERED_DISPLAY_FIELDS.append(field_name)


@login_required
def utility_readings_report(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('UTILITY.view_utilityrecord'):
        messages.error(request, "You do not have permission to View Utility records.")
        return redirect('indexpage')

    # ----- Add date filters -----
    from_date = request.GET.get('from_date')
    to_date   = request.GET.get('to_date')
    date_filter = {}

    if from_date:
        date_filter['reading_date__gte'] = from_date
    if to_date:
        date_filter['reading_date__lte'] = to_date

    # Filter distinct_dates based on selected period
    distinct_dates = UtilityRecord.objects.filter(**date_filter) \
                        .values_list('reading_date', flat=True) \
                        .distinct().order_by('-reading_date')

    # ------------- PAGINATION -------------
    page_number = request.GET.get('page', 1)
    per_page = 10  # Change this value as needed
    paginator = Paginator(distinct_dates, per_page)
    page_obj = paginator.get_page(page_number)
    paged_dates = page_obj.object_list
    # --------------------------------------

    report_data_rows = []

    all_records_for_dates = UtilityRecord.objects.filter(reading_date__in=list(paged_dates)) \
                                                .order_by('reading_date', 'reading_type', 'id')

    records_by_date_then_type = defaultdict(dict)
    for record in all_records_for_dates:
        records_by_date_then_type[record.reading_date][record.reading_type] = record

    for r_date in paged_dates:
        consolidated_readings_for_date = {field_name: None for field_name in ORDERED_DISPLAY_FIELDS}
        records_for_type_on_this_date = records_by_date_then_type.get(r_date, {})
        for reading_type_key, owned_fields in ACTUAL_TYPE_FIELDS_OWNERSHIP.items():
            record_for_type = records_for_type_on_this_date.get(reading_type_key)
            if record_for_type:
                for model_field in owned_fields:
                    if model_field in consolidated_readings_for_date:
                        consolidated_readings_for_date[model_field] = getattr(record_for_type, model_field, None)
        ordered_values_for_row = []
        for field_name in ORDERED_DISPLAY_FIELDS:
            raw_value = consolidated_readings_for_date.get(field_name)
            processed_value = None
            if isinstance(raw_value, Decimal):
                processed_value = raw_value
            elif raw_value is not None:
                try:
                    processed_value = Decimal(str(raw_value))
                except InvalidOperation:
                    processed_value = None
            ordered_values_for_row.append({
                'name': field_name,
                'value': processed_value
            })
        report_data_rows.append({
            'date': r_date,
            'values': ordered_values_for_row
        })

    context = {
        'header_structure': DISPLAY_HEADER_STRUCTURE,
        'report_rows': report_data_rows,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'from_date': from_date or '',
        'to_date': to_date or '',
        'page_obj': page_obj,  # Pass to template
    }
    return render(request, 'utility/boiler_steam/utility_readings_report.html', context)




#-----------Excel Download  ----------------------------
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse
from django.utils.timezone import now # For default dates if needed, though your code handles it
from collections import defaultdict
from io import BytesIO
import xlsxwriter
from decimal import Decimal

# Assuming UtilityRecord, DISPLAY_HEADER_STRUCTURE, ORDERED_DISPLAY_FIELDS,
# and ACTUAL_TYPE_FIELDS_OWNERSHIP are defined elsewhere and imported correctly.
# from .models import UtilityRecord
# from .your_constants_module import DISPLAY_HEADER_STRUCTURE, ORDERED_DISPLAY_FIELDS, ACTUAL_TYPE_FIELDS_OWNERSHIP

@login_required
def utility_readings_excel(request):
    if not request.user.has_perm('UTILITY.view_utilityrecord'):
        return HttpResponse("Unauthorized", status=403)

    from_date_str = request.GET.get('from_date')
    to_date_str = request.GET.get('to_date')
    # The 'page' parameter is no longer needed for Excel export logic
    # page = request.GET.get('page', 1)

    date_filter = {}
    if from_date_str:
        date_filter['reading_date__gte'] = from_date_str
    if to_date_str:
        date_filter['reading_date__lte'] = to_date_str

    # Get ALL distinct dates within the filter range for the Excel report
    # No pagination here.
    report_dates = UtilityRecord.objects.filter(**date_filter) \
        .values_list('reading_date', flat=True).distinct().order_by('-reading_date')

    # If there are no dates, we can return an empty Excel or a message.
    # For now, it will proceed and create an Excel with only headers.
    if not report_dates:
        # Optionally, handle the case of no data differently, e.g.,
        # return HttpResponse("No data found for the selected date range.", status=200)
        pass # Or let it generate an empty report

    # Fetch all records for ALL the distinct dates found
    all_records_for_report_dates = UtilityRecord.objects.filter(reading_date__in=list(report_dates)) \
        .order_by('reading_date', 'reading_type', 'id')

    records_by_date_then_type = defaultdict(dict)
    for record in all_records_for_report_dates:
        records_by_date_then_type[record.reading_date][record.reading_type] = record

    # --- Prepare Excel file in memory ---
    output = BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet('Utility Readings')

    # Styles
    bold = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter'})
    center = workbook.add_format({'align': 'center', 'valign': 'vcenter'})
    border = workbook.add_format({'border': 1, 'align': 'center', 'valign': 'vcenter'})
    group_header_fmt = workbook.add_format({'bold': True, 'align': 'center', 'bg_color': '#dbeafe', 'border': 1, 'valign': 'vcenter'})
    sub_header_fmt = workbook.add_format({'bold': True, 'align': 'center', 'bg_color': '#f1f5f9', 'border': 1, 'valign': 'vcenter'})

    # -- Header row 1: Group labels (merged cells) --
    current_col_header1 = 0
    worksheet.merge_range(0, 0, 1, 0, "DATE", group_header_fmt)  # Date header
    current_col_header1 = 1 # Start after DATE column
    for group in DISPLAY_HEADER_STRUCTURE:
        group_span = len(group['fields'])
        if group_span == 1:
            worksheet.write(0, current_col_header1, group['group_label'], group_header_fmt)
            current_col_header1 += 1
        elif group_span > 1: # Ensure span is positive
            worksheet.merge_range(0, current_col_header1, 0, current_col_header1 + group_span - 1, group['group_label'], group_header_fmt)
            current_col_header1 += group_span
        # If group_span is 0 or less, it might indicate an issue with DISPLAY_HEADER_STRUCTURE

    # -- Header row 2: Field labels --
    current_col_header2 = 1 # Start after DATE column
    for group in DISPLAY_HEADER_STRUCTURE:
        for field_name, label in group['fields']:
            clean_label = label.replace('<br>', ' ').replace(' ', ' ').strip()
            worksheet.write(1, current_col_header2, clean_label, sub_header_fmt)
            current_col_header2 += 1

    # --- Data rows ---
    # Iterate over all 'report_dates' instead of 'paged_dates'
    for row_idx, r_date in enumerate(report_dates, start=2): # Start at row 2 (0-indexed) for data
        worksheet.write(row_idx, 0, r_date.strftime("%d/%m/%Y"), border) # Date in first column

        consolidated_readings_for_date = {field_name: None for field_name in ORDERED_DISPLAY_FIELDS}
        records_for_type_on_this_date = records_by_date_then_type.get(r_date, {})

        for reading_type_key, owned_fields in ACTUAL_TYPE_FIELDS_OWNERSHIP.items():
            record_for_type = records_for_type_on_this_date.get(reading_type_key)
            if record_for_type:
                for model_field in owned_fields:
                    if model_field in consolidated_readings_for_date: # Check if field is expected in output
                        consolidated_readings_for_date[model_field] = getattr(record_for_type, model_field, None)

        # Write all values for this date according to ORDERED_DISPLAY_FIELDS
        for col_idx_offset, field_name in enumerate(ORDERED_DISPLAY_FIELDS):
            actual_col_idx = col_idx_offset + 1 # Data columns start after the DATE column
            val = consolidated_readings_for_date.get(field_name)
            try:
                if isinstance(val, Decimal):
                    # xlsxwriter prefers float for numbers
                    worksheet.write_number(row_idx, actual_col_idx, float(val), border)
                elif isinstance(val, (int, float)): # Handle existing floats/ints
                    worksheet.write_number(row_idx, actual_col_idx, float(val), border)
                elif val is not None:
                    # Attempt to convert to float if it looks like a number, otherwise write as string
                    try:
                        num_val = float(val)
                        worksheet.write_number(row_idx, actual_col_idx, num_val, border)
                    except (ValueError, TypeError):
                        worksheet.write_string(row_idx, actual_col_idx, str(val), border)
                else: # val is None
                    worksheet.write_blank(row_idx, actual_col_idx, None, border) # Explicitly write blank
            except Exception: # Catch-all for any other conversion errors
                worksheet.write_string(row_idx, actual_col_idx, str(val) if val is not None else '', border)


    # Set column width for better visibility
    worksheet.set_column(0, 0, 12) # Date column width
    if ORDERED_DISPLAY_FIELDS: # Check if there are other fields
        worksheet.set_column(1, len(ORDERED_DISPLAY_FIELDS), 15) # Default width for data columns

    workbook.close()
    output.seek(0)

    filename = f"Utility_Readings_{now().strftime('%Y%m%d')}.xlsx" # Add date to filename
    response = HttpResponse(
        output,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response['Content-Disposition'] = f'attachment; filename="{filename}"' # Ensure filename is quoted
    return response





#---------------------------------------------------------------------------------------------



# TYPE_FIELDS: Maps reading_type to model fields it contains.
TYPE_FIELDS = {
    "STEAM GENERATION READING": ["sb_3_e_22_main_fm_fv", "sb_3_sub_fm_oc"],
    "STEAM CONSUMPTION READING": [
        "block_a_reading", "block_b_reading", "mee_total_reading", "stripper_reading",
        "old_atfd", "mps_d_block_reading", "lps_e_17", "mps_e_17",
        "jet_ejector_atfd_c", "deareator", "new_atfd"
    ],
    "Boiler Water meter Reading": ["boiler_water_meter"],
    "MIDC reading": ["midc_water_e_17", "midc_water_e_16", "midc_water_e_22", "midc_water_e_18", "midc_water_e_20"],
    "BRIQUETTE": ["briquette_sb_3","briquette_tfh"],
    "DM Water consumed for boiler": ["dm_water_for_boiler"],
}

HEADER_STRUCTURE = [
    {
        'group_label': 'STEAM GENERATION',
        'fields': [
            ('sb_3_e_22_main_fm_fv', 'SB-3 (E-22) Main FM/FV', 'bg-orange-50'),
            ('sb_3_sub_fm_oc', 'SB-3 Sub FM/OC', 'bg-green-50'),
        ]
    },
    {
        'group_label': 'STEAM CONSUMPTION',
        'fields': [
            ('block_a_reading', 'Block-A', 'bg-blue-100'),
            ('block_b_reading', 'Block-B', 'bg-blue-100'),
            ('mee_total_reading', 'MEE', 'bg-blue-100'),
            ('stripper_reading', 'Stripper', 'bg-blue-100'),
            ('old_atfd', 'Old ATFD', 'bg-blue-100'),
            ('mps_d_block_reading', 'D-Block MPS', 'bg-blue-100'),
            ('lps_e_17', 'LPS E-17', 'bg-blue-100'),
            ('mps_e_17', 'MPS E-17', 'bg-blue-100'),
            ('jet_ejector_atfd_c', '4 JET Ejector + ATFD-C', 'bg-blue-100'),
            ('deareator', 'Deareator', 'bg-blue-100'),
            ('new_atfd', 'New ATFD', 'bg-blue-100'),
        ]
    },
    {
        'group_label': 'BRIQUETTE',
        'fields': [
            ('briquette_sb_3', 'SB-3', 'bg-yellow-50'),
            ('briquette_tfh', 'TFH', 'bg-yellow-50'),
        ]
    },
    {
        'group_label': 'Boiler Water meter Consumption',
        'fields': [
            ('boiler_water_meter', 'Boiler Water Meter', 'bg-violet-100'),
        ]
    },
    {
        'group_label': 'DM Water consumption for boiler',
        'fields': [
            ('dm_water_for_boiler', 'DM Water consumed for boiler', 'bg-sky-100'),
        ]
    },
    {
        'group_label': 'WATER',
        'fields': [
            ('midc_water_e_17', 'MIDC water E-17', 'bg-sky-100'),
            ('midc_water_e_16', 'MIDC water E-16', 'bg-sky-100'),
            ('midc_water_e_22', 'MIDC water E-22', 'bg-sky-100'),
            ('midc_water_e_18', 'MIDC water E-18', 'bg-sky-100'),
            ('midc_water_e_20', 'MIDC water E-20', 'bg-sky-100'),
        ]
    }
]





# Defines which unique fields need their consumption calculated
UNIQUE_MODEL_FIELDS_FOR_CALCULATION = []
_seen_for_calc = set()
for group in HEADER_STRUCTURE:
    for field_name, _, _ in group['fields']:
        if field_name not in _seen_for_calc:
            UNIQUE_MODEL_FIELDS_FOR_CALCULATION.append(field_name)
            _seen_for_calc.add(field_name)

# Defines the order and repetition of fields as they appear in the report columns
ALL_DISPLAY_FIELDS_IN_ORDER = []
for group in HEADER_STRUCTURE:
    for field_name, _, _ in group['fields']:
        ALL_DISPLAY_FIELDS_IN_ORDER.append(field_name)


def get_today_value(records_for_types, field_name):
    for r_type, flds_in_type in TYPE_FIELDS.items():
        if field_name in flds_in_type:
            today_record = records_for_types.get(r_type)
            if today_record:
                val = getattr(today_record, field_name, None)
                if val is None:
                    return Decimal('0.00')
                if not isinstance(val, Decimal):
                    return Decimal(str(val))
                return val
    return Decimal('0.00')

@login_required
def utility_consumption_report(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('UTILITY.view_utilityrecord'):
        messages.error(request, "You do not have permission to View Utility records.")
        return redirect('indexpage')

    from_date_str = request.GET.get("from_date", "")
    to_date_str = request.GET.get("to_date", "")

    if from_date_str and to_date_str:
        try:
            from_date = date.fromisoformat(from_date_str)
            to_date = date.fromisoformat(to_date_str)
        except Exception:
            from_date = None
            to_date = None
    else:
        from_date = None
        to_date = None

    qs = UtilityRecord.objects.all()
    if from_date and to_date:
        qs = qs.filter(reading_date__range=[from_date, to_date])

    all_distinct_dates = list(
        qs.values_list("reading_date", flat=True)
          .distinct()
          .order_by("-reading_date")
    )

    if len(all_distinct_dates) < 2:
        return render(request, "utility/boiler_steam/utility_consumption_report.html", {
            "header_structure": HEADER_STRUCTURE,
            "report_rows": [],
            "from_date": from_date or "",
            "to_date": to_date or "",
            "error": "Need at least two days of readings to compute consumption.",
        })

    report_rows_data = []
    records_by_date_and_type = {}
    relevant_dates = set(all_distinct_dates)
    all_db_records = UtilityRecord.objects.filter(reading_date__in=relevant_dates).order_by('reading_date', 'reading_type', 'id')

    for record in all_db_records:
        date_key = record.reading_date
        type_key = record.reading_type
        if date_key not in records_by_date_and_type:
            records_by_date_and_type[date_key] = {}
        records_by_date_and_type[date_key][type_key] = record

    SHOW_AS_IS_FIELDS = ["briquette_sb_3","briquette_tfh", "deareator", "jet_ejector_atfd_c","dm_water_for_boiler"]

    for i in range(len(all_distinct_dates) - 1):
        today_date = all_distinct_dates[i]
        yesterday_date = all_distinct_dates[i + 1]

        today_records_for_all_types = records_by_date_and_type.get(today_date, {})
        yesterday_records_for_all_types = records_by_date_and_type.get(yesterday_date, {})

        calculated_deltas_map = {}
        total_steam_consumption_for_day = Decimal('0.00')

        for model_field_name in UNIQUE_MODEL_FIELDS_FOR_CALCULATION:
            delta = Decimal('0.00')
            field_reading_type = None
            for r_type, flds_in_type in TYPE_FIELDS.items():
                if model_field_name in flds_in_type:
                    field_reading_type = r_type
                    break

            if field_reading_type:
                today_record_for_type = today_records_for_all_types.get(field_reading_type)
                yesterday_record_for_type = yesterday_records_for_all_types.get(field_reading_type)

                if today_record_for_type and yesterday_record_for_type:
                    today_val_raw = getattr(today_record_for_type, model_field_name, None)
                    yesterday_val_raw = getattr(yesterday_record_for_type, model_field_name, None)

                    if today_val_raw is None: today_val = Decimal('0.00')
                    elif not isinstance(today_val_raw, Decimal): today_val = Decimal(str(today_val_raw))
                    else: today_val = today_val_raw

                    if yesterday_val_raw is None: yesterday_val = Decimal('0.00')
                    elif not isinstance(yesterday_val_raw, Decimal): yesterday_val = Decimal(str(yesterday_val_raw))
                    else: yesterday_val = yesterday_val_raw

                    delta = today_val - yesterday_val

                if model_field_name == 'mps_e_17':
                    delta = delta * Decimal('1000')

            calculated_deltas_map[model_field_name] = delta

            if field_reading_type == "STEAM CONSUMPTION READING":
                total_steam_consumption_for_day += delta

        mee = (
            calculated_deltas_map.get('mee_total_reading', Decimal('0.00')) +
            calculated_deltas_map.get('stripper_reading', Decimal('0.00')) +
            calculated_deltas_map.get('old_atfd', Decimal('0.00')) +
            calculated_deltas_map.get('new_atfd', Decimal('0.00'))
        )
        plant = (
            calculated_deltas_map.get('block_a_reading', Decimal('0.00')) +
            calculated_deltas_map.get('block_b_reading', Decimal('0.00')) +
            calculated_deltas_map.get('mps_d_block_reading', Decimal('0.00')) +
            calculated_deltas_map.get('lps_e_17', Decimal('0.00')) +
            calculated_deltas_map.get('mps_e_17', Decimal('0.00')) +
            get_today_value(today_records_for_all_types, 'jet_ejector_atfd_c') +
            get_today_value(today_records_for_all_types, 'deareator')
        )
        total = mee + plant

        current_day_display_values_list = []
        for display_field_name in ALL_DISPLAY_FIELDS_IN_ORDER:
            if display_field_name in SHOW_AS_IS_FIELDS:
                today_value = None
                for r_type, flds_in_type in TYPE_FIELDS.items():
                    if display_field_name in flds_in_type:
                        today_record = today_records_for_all_types.get(r_type)
                        if today_record:
                            today_value = getattr(today_record, display_field_name, None)
                        break
                if today_value is None:
                    value_to_display = Decimal('0.00')
                else:
                    value_to_display = today_value
            else:
                value_to_display = calculated_deltas_map.get(display_field_name, Decimal('0.00'))
            current_day_display_values_list.append(value_to_display)

        report_rows_data.append({
            'date': today_date,
            'values': current_day_display_values_list,
            'steam_calc': {
                'mee': mee,
                'plant': plant,
                'total': total,
            }
        })

    # PAGINATION
    per_page = 10  # Set your page size here
    paginator = Paginator(report_rows_data, per_page)
    page_number = request.GET.get("page") or 1

    try:
        page_obj = paginator.page(page_number)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    context = {
        "title": "Daily Consumption Report",
        "header_structure": HEADER_STRUCTURE,
        "report_rows": page_obj.object_list,
        "from_date": from_date or "",
        "to_date": to_date or "",
        "error": None,
        "page_obj": page_obj,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
    }
    return render(request, "utility/boiler_steam/utility_consumption_report.html", context)






@login_required
def utility_consumption_excel(request):
    # Match new filter logic: show all dates if no filter is applied
    from_date_str = request.GET.get("from_date", "")
    to_date_str = request.GET.get("to_date", "")

    if from_date_str and to_date_str:
        try:
            from_date = date.fromisoformat(from_date_str)
            to_date = date.fromisoformat(to_date_str)
        except Exception:
            from_date = None
            to_date = None
    else:
        from_date = None
        to_date = None

    qs = UtilityRecord.objects.all()
    if from_date and to_date:
        qs = qs.filter(reading_date__range=[from_date, to_date])

    all_distinct_dates = list(
        qs.values_list("reading_date", flat=True)
          .distinct()
          .order_by("-reading_date")
    )

    records_by_date_and_type = {}
    relevant_dates = set(all_distinct_dates)
    all_db_records = UtilityRecord.objects.filter(reading_date__in=relevant_dates).order_by('reading_date', 'reading_type', 'id')

    for record in all_db_records:
        date_key = record.reading_date
        type_key = record.reading_type
        if date_key not in records_by_date_and_type:
            records_by_date_and_type[date_key] = {}
        records_by_date_and_type[date_key][type_key] = record

    SHOW_AS_IS_FIELDS = ["briquette_sb_3","briquette_tfh", "deareator", "jet_ejector_atfd_c","dm_water_for_boiler"]

    # Prepare Excel workbook in-memory
    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {'in_memory': True})
    ws = wb.add_worksheet('Utility Consumption')

    # Styles
    header_fmt = wb.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#e0edfa', 'border': 1})
    group_fmt  = wb.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#d1fae5', 'border': 1})
    th_fmt     = wb.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'border': 1})
    date_fmt   = wb.add_format({'num_format': 'dd/mm/yyyy', 'border': 1, 'align': 'center'})
    val_fmt    = wb.add_format({'num_format': '#,##0.00', 'border': 1, 'align': 'right'})
    val_bold   = wb.add_format({'num_format': '#,##0.00', 'border': 1, 'align': 'right', 'bold': True})
    
    # ---- Write Headers ----

    # 1st header row: group labels
    row, col = 0, 0
    ws.write(row, col, "DATE", group_fmt)
    col += 1
    for group in HEADER_STRUCTURE:
        ws.merge_range(row, col, row, col+len(group['fields'])-1, group['group_label'], group_fmt)
        col += len(group['fields'])
    ws.merge_range(row, col, row, col+2, "STEAM", group_fmt)

    # 2nd header row: field labels
    row, col = 1, 0
    ws.write(row, col, "", th_fmt)
    col += 1
    for group in HEADER_STRUCTURE:
        for _, label, _ in group['fields']:
            ws.write(row, col, label, th_fmt)
            col += 1
    ws.write(row, col, "MEE", th_fmt); col += 1
    ws.write(row, col, "PLANT", th_fmt); col += 1
    ws.write(row, col, "TOTAL", th_fmt)

    # ---- Write Data Rows ----

    for i in range(len(all_distinct_dates) - 1):
        today_date = all_distinct_dates[i]
        yesterday_date = all_distinct_dates[i + 1]
        today_records_for_all_types = records_by_date_and_type.get(today_date, {})
        yesterday_records_for_all_types = records_by_date_and_type.get(yesterday_date, {})

        calculated_deltas_map = {}
        for model_field_name in UNIQUE_MODEL_FIELDS_FOR_CALCULATION:
            delta = Decimal('0.00')
            field_reading_type = None
            for r_type, flds_in_type in TYPE_FIELDS.items():
                if model_field_name in flds_in_type:
                    field_reading_type = r_type
                    break

            if field_reading_type:
                today_record_for_type = today_records_for_all_types.get(field_reading_type)
                yesterday_record_for_type = yesterday_records_for_all_types.get(field_reading_type)

                if today_record_for_type and yesterday_record_for_type:
                    today_val_raw = getattr(today_record_for_type, model_field_name, None)
                    yesterday_val_raw = getattr(yesterday_record_for_type, model_field_name, None)
                    if today_val_raw is None: today_val = Decimal('0.00')
                    elif not isinstance(today_val_raw, Decimal): today_val = Decimal(str(today_val_raw))
                    else: today_val = today_val_raw
                    if yesterday_val_raw is None: yesterday_val = Decimal('0.00')
                    elif not isinstance(yesterday_val_raw, Decimal): yesterday_val = Decimal(str(yesterday_val_raw))
                    else: yesterday_val = yesterday_val_raw
                    delta = today_val - yesterday_val

                if model_field_name == 'mps_e_17':
                    delta = delta * Decimal('1000')

            calculated_deltas_map[model_field_name] = delta

        mee = (
            calculated_deltas_map.get('mee_total_reading', Decimal('0.00')) +
            calculated_deltas_map.get('stripper_reading', Decimal('0.00')) +
            calculated_deltas_map.get('old_atfd', Decimal('0.00')) +
            calculated_deltas_map.get('new_atfd', Decimal('0.00'))
        )
        plant = (
            calculated_deltas_map.get('block_a_reading', Decimal('0.00')) +
            calculated_deltas_map.get('block_b_reading', Decimal('0.00')) +
            calculated_deltas_map.get('mps_d_block_reading', Decimal('0.00')) +
            calculated_deltas_map.get('lps_e_17', Decimal('0.00')) +
            calculated_deltas_map.get('mps_e_17', Decimal('0.00')) +
            get_today_value(today_records_for_all_types, 'jet_ejector_atfd_c') +
            get_today_value(today_records_for_all_types, 'deareator')
        )
        total = mee + plant

        current_day_display_values_list = []
        for display_field_name in ALL_DISPLAY_FIELDS_IN_ORDER:
            if display_field_name in SHOW_AS_IS_FIELDS:
                today_value = None
                for r_type, flds_in_type in TYPE_FIELDS.items():
                    if display_field_name in flds_in_type:
                        today_record = today_records_for_all_types.get(r_type)
                        if today_record:
                            today_value = getattr(today_record, display_field_name, None)
                        break
                if today_value is None:
                    value_to_display = Decimal('0.00')
                else:
                    value_to_display = today_value
            else:
                value_to_display = calculated_deltas_map.get(display_field_name, Decimal('0.00'))
            current_day_display_values_list.append(value_to_display)

        row += 1
        col = 0
        ws.write(row, col, today_date, date_fmt)
        col += 1
        for v in current_day_display_values_list:
            ws.write(row, col, float(v), val_fmt)
            col += 1
        ws.write(row, col, float(mee), val_bold); col += 1
        ws.write(row, col, float(plant), val_bold); col += 1
        ws.write(row, col, float(total), val_bold)

    ws.autofilter(0, 0, row, col)  # Apply filter for user
    ws.freeze_panes(2, 1)          # Freeze headers

    wb.close()
    output.seek(0)

    # Handle filename for all/filtered cases
    if from_date_str and to_date_str:
        filename = f"Utility_Consumption_{from_date_str}_to_{to_date_str}.xlsx"
    else:
        filename = f"Utility_Consumption_All.xlsx"

    response = HttpResponse(
        output.read(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response


# =================================== Below code is for Power =============================================================




@login_required
def power_entry_view(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    if not request.user.has_perm('UTILITY.add_utilitypowerreading'):
        messages.error(request, "You do not have permission to add Power records.")
        return redirect('indexpage')

    PowerFormSet = formset_factory(UtilityPowerReadingForm, formset=BasePowerReadingFormSet, extra=0)

    if request.method == "POST":
        date = request.POST.get("date")
        formset = PowerFormSet(request.POST)
        # 1. Check if records for this date already exist
        if date and UtilityPowerReading.objects.filter(reading_date=date).exists():
            messages.error(request, f"❌ A power reading for the date {date} already exists. Please choose a different date or edit the existing record.")
        elif formset.is_valid():
            # 2. Save each form as a separate model instance (ONE PER READING TYPE)
            records_created = 0
            for form in formset:
                if not form.has_changed():
                    continue
                rec = form.save(commit=False)
                rec.reading_date = date
                rec.save()
                records_created += 1
            if records_created:
                logger.info(f"Power readings saved for date {date} by {request.user} ({records_created} records)")
                messages.success(request, "✅ Power Readings saved successfully!")
                return redirect("power_readings_report")
            else:
                messages.warning(request, "No data was entered to save.")
        else:
            logger.warning(f"Power readings formset errors: {formset.errors}")
            messages.error(request, "❌ Please correct the errors below and resubmit.")

    else:  # GET
        initial = [{"reading_type": t[0]} for t in POWER_TYPE_CHOICES]
        formset = PowerFormSet(initial=initial)

    return render(request, "utility/power/power_entry.html", { 
        "formset": formset,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
    })



@login_required
def edit_power_date(request, date_str):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('UTILITY.change_utilitypowerreading'):
        messages.error(request, "You do not have permission to update Power records.")
        return redirect('indexpage')

    PowerFormSet = formset_factory(UtilityPowerReadingForm, extra=0)

    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        messages.error(request, "Invalid date format.")
        return redirect("power_readings_report")

    batch_qs = UtilityPowerReading.objects.filter(reading_date=date_obj).order_by('reading_type')
    if not batch_qs.exists():
        messages.error(request, "No records found for that date.")
        return redirect("power_readings_report")

    if request.method == "POST":
        formset = PowerFormSet(request.POST)
        new_date_str = request.POST.get("date")
        try:
            new_date_obj = datetime.strptime(new_date_str, "%Y-%m-%d").date()
        except Exception:
            new_date_obj = date_obj  # fallback to old date

        if formset.is_valid():
            UtilityPowerReading.objects.filter(reading_date=date_obj).delete()
            for form in formset:
                rec = form.save(commit=False)
                rec.reading_date = new_date_obj  # use new date here
                for fld in CALC_FIELDS:
                    if fld in form.cleaned_data:
                        setattr(rec, fld, form.cleaned_data[fld])
                rec.save()

            if new_date_obj != date_obj:
                messages.success(request, f"✅ Power Readings updated and moved to {new_date_obj}!")
            else:
                messages.success(request, f"✅ Power Readings updated for {date_obj}!")
            return redirect("power_readings_report")

    else:
        # GET: prepare initial data for each reading type
        type_map = {rec.reading_type: rec for rec in batch_qs}
        initial = []
        for t, _ in POWER_TYPE_CHOICES:
            rec = type_map.get(t)
            if rec:
                data = {"reading_type": rec.reading_type}
                for fld in UtilityPowerReading._meta.fields:
                    name = fld.name
                    if name not in ("id", "reading_date", "reading_type"):
                        data[name] = getattr(rec, name)
                initial.append(data)
            else:
                initial.append({"reading_type": t})
        formset = PowerFormSet(initial=initial)
        new_date_obj = date_obj  # on GET, we still use the existing date

    return render(request, "utility/power/power_entry.html", {
        "formset": formset,
        "edit_date": new_date_obj,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
    })


# This is derived directly from the provided image and model structure.
ACTUAL_TYPE_FIELDS_OWNERSHIP_POWER = {
    "E-18 POWER CONSUMPTION": [
        "block_a1", "block_a2", "block_b1", "block_b2", "block_d1", "block_c1", 
        "block_d2", "block_b_all_ejector", "utility_2", "utility_3", 
        "block_b3_ut04", "utility_05_block_b_anfd", "tf_unit", "ct_75hp_pump2", 
        "stabilizer", "etp_e17", "mee_e17", "c03_air_compressor_40hp", 
        "trane1_brine_comp_110tr", "chiller_02_trane2", "voltas_chiller_02", 
        "block_c2_d04", "ct_75hp_pump1", "new_ro", "new_atfd", "admin", 
        "etp_press_filter", "others_e18_fire", "mcc_total"
    ],
    "E-17 POWER CONSUMPTION": [
        "imcc_panel_01_utility", "imcc_panel_02_utility", "imcc_panel_03", 
        "imcc_panel_04", "imcc_panel_05", "row_power_panel", "lighting_panel", 
        "brine_chiller_1_5f_30", "water_chiller_2_4r_440", "others_e17", 
        "imcc_total"  # Assumes the first 'imcc_total' in the model
    ],
    "E-22 POWER CONSUMPTION": [
        "e22_mseb", "e22_pcc", "e22_boiler", "e22_aircom_tf_boiler_other", 
        
    ],
    "TOTAL POWER CONSUMPTION": [
        "mcc_imcc_total",
        "pcc_main_e17", "mseb_e18", "pcc_01", "pcc_02", "tr_losses_e18", 
        "tr_losses_e22", # Assumes the second 'others_e18_fire'
        "e16_mseb", "dg_total_e18", "dg_total_e22", "dg_pcc_e18", "dg_pcc_e22", 
        "total_kwh_e18_e22_e16"
    ],
}

# Defines the entire header structure for the report template, including labels,
# grouping, and color-coding based on the image.
DISPLAY_HEADER_STRUCTURE_POWER = [
    {
        'group_label': 'E-18 POWER CONSUMPTION',
        'fields': [
            ('block_a1', 'BLOCK A1'), ('block_a2', 'BLOCK A2'), ('block_b1', 'BLOCK B1'),
            ('block_b2', 'BLOCK B2'), ('block_d1', 'BLOCK D1'), ('block_c1', 'BLOCK C1'),
            ('block_d2', 'BLOCK D2'), ('block_b_all_ejector', 'Block B +<br>All Ejector'),
            ('utility_2', 'UTILITY 2'), ('utility_3', 'UTILITY 3'),
            ('block_b3_ut04', 'BLOCK B3<br>(UT-04)'),
            ('utility_05_block_b_anfd', 'UTILITY-05<br>(BLOCK B ANFD)'),
            ('tf_unit', 'TF UNIT'), ('ct_75hp_pump2', 'CT 75 HP<br>(PUMP.2)'),
            ('stabilizer', 'STABILIZER'), ('etp_e17', 'ETP (E.17)'), ('mee_e17', 'MEE (E.17)'),
            ('c03_air_compressor_40hp', '(C-03)Air<br>Compressor<br>(40HP)'),
            ('trane1_brine_comp_110tr', 'TRANE 1<br>(BRINE COMP<br>110TR)'),
            ('chiller_02_trane2', 'Chiller-02<br>(Trane 2)'),
            ('voltas_chiller_02', 'Voltas<br>Chiller-02'),
            ('block_c2_d04', 'Block C2<br>(D-04)'),
            ('ct_75hp_pump1', 'CT 75 HP<br>(PUMP.1)'), ('new_ro', 'NEW RO'),
            ('new_atfd', 'NEW ATFD'), ('admin', 'ADMIN'),
            ('etp_press_filter', 'ETP<br>PRESS<br>FILTER'),
            ('others_e18_fire', 'OTHERS<br>E-18 (Fire)'), ('mcc_total', 'MCC<br>TOTAL'),
        ],
        'group_bg_color': 'bg-green-200', 'cell_bg_color': 'bg-green-50'
    },
    {
        'group_label': 'E-17 POWER CONSUMPTION',
        'fields': [
            ('imcc_panel_01_utility', 'IMCC<br>PANEL 01<br>UTILITY'),
            ('imcc_panel_02_utility', 'IMCC<br>PANEL 02<br>UTILITY'),
            ('imcc_panel_03', 'IMCC<br>PANEL 03'), ('imcc_panel_04', 'IMCC<br>PANEL 04'),
            ('imcc_panel_05', 'IMCC<br>PANEL 05'), ('row_power_panel', 'Row power<br>panel'),
            ('lighting_panel', 'Lighting<br>Panel'),
            ('brine_chiller_1_5f_30', 'BRINE<br>CHILLER<br>(1/5F-30)'),
            ('water_chiller_2_4r_440', 'WATER<br>CHILLER<br>2/4R-440'),
            ('others_e17', 'OTHERS<br>E-17'), ('imcc_total', 'IMCC<br>TOTAL'),
        ],
        'group_bg_color': 'bg-yellow-200', 'cell_bg_color': 'bg-yellow-50'
    },
    {
        'group_label': 'E-22 POWER CONSUMPTION',
        'fields': [
            ('e22_mseb', 'E-22<br>MSEB'), ('e22_pcc', 'E-22<br>PCC'),
            ('e22_boiler', 'E-22<br>BOILER'),
            ('e22_aircom_tf_boiler_other', 'E-22<br>Air com, TF<br>Boiler & Other'),
            
        ],
        'group_bg_color': 'bg-sky-200', 'cell_bg_color': 'bg-sky-50'
    },
    {
        'group_label': 'TOTAL POWER CONSUMPTION',
        'fields': [
            ('mcc_imcc_total', 'MCC &<br>IMCC<br>TOTAL'),
            ('pcc_main_e17', 'PCC<br>MAIN E-17'), ('mseb_e18', 'MSEB<br>E-18'),
            ('pcc_01', 'PCC-01'), ('pcc_02', 'PCC-02'),
            ('tr_losses_e18', 'TR.<br>LOSSES<br>E-18'),
            ('tr_losses_e22', 'TR.<br>LOSSES<br>E-22'),
             ('e16_mseb', 'E-16<br>MSEB'),
            ('dg_total_e18', 'DG TOTAL<br>E-18'), ('dg_total_e22', 'DG TOTAL<br>E-22'),
            ('dg_pcc_e18', 'DG + PCC<br>E-18'), ('dg_pcc_e22', 'DG + PCC<br>E-22'),
            ('total_kwh_e18_e22_e16', 'TOTAL (KWH)<br>E-18,E-22 & E-16'),
        ],
        'group_bg_color': 'bg-purple-200', 'cell_bg_color': 'bg-purple-50'
    },
]

# A flat list of all field names in the desired display order.
ORDERED_DISPLAY_FIELDS_POWER = []
for group in DISPLAY_HEADER_STRUCTURE_POWER:
    for field_name, _ in group['fields']:
        ORDERED_DISPLAY_FIELDS_POWER.append(field_name)


@login_required
def power_readings_report(request):
    user_groups = request.user.groups.values_list('name', flat=True)  # Check if the user is in HR group
    is_superuser = request.user.is_superuser
    """
    This view generates a consolidated daily power consumption report.
    It handles date filtering, pagination, and organizes data from multiple
    reading types into a single cohesive view based on the defined structures.
    """
    if not request.user.has_perm('UTILITY.view_utilitypowerreading'): # Replace APP_NAME
        messages.error(request, "You do not have permission to View Power records.")
        return redirect('indexpage')

    # Standard date filtering logic
    from_date = request.GET.get('from_date')
    to_date = request.GET.get('to_date')
    date_filter = {}

    if from_date:
        date_filter['reading_date__gte'] = from_date
    if to_date:
        date_filter['reading_date__lte'] = to_date

    distinct_dates = UtilityPowerReading.objects.filter(**date_filter) \
        .values_list('reading_date', flat=True) \
        .distinct().order_by('-reading_date')

    # Standard pagination logic
    paginator = Paginator(distinct_dates, 10) # 10 dates per page
    page_number = request.GET.get('page', 1)
    page_obj = paginator.get_page(page_number)
    paged_dates = page_obj.object_list

    report_data_rows = []

    # Fetch all records for the dates on the current page in one query
    all_records_for_dates = UtilityPowerReading.objects.filter(reading_date__in=list(paged_dates)) \
        .order_by('reading_date', 'reading_type', 'id')

    # Group records by date and then by type for efficient lookup
    records_by_date_then_type = defaultdict(dict)
    for record in all_records_for_dates:
        records_by_date_then_type[record.reading_date][record.reading_type] = record

    # Process each day to build a consolidated row for the report
    for r_date in paged_dates:
        consolidated_readings = {field: None for field in ORDERED_DISPLAY_FIELDS_POWER}
        
        records_for_this_date = records_by_date_then_type.get(r_date, {})
        for r_type, owned_fields in ACTUAL_TYPE_FIELDS_OWNERSHIP_POWER.items():
            record_for_type = records_for_this_date.get(r_type)
            if record_for_type:
                for field in owned_fields:
                    if field in consolidated_readings:
                        consolidated_readings[field] = getattr(record_for_type, field, None)
        
        ordered_values = []
        for field_name in ORDERED_DISPLAY_FIELDS_POWER:
            raw_val = consolidated_readings.get(field_name)
            proc_val = None
            if isinstance(raw_val, Decimal):
                proc_val = raw_val
            elif raw_val is not None:
                try:
                    proc_val = Decimal(str(raw_val))
                except InvalidOperation:
                    proc_val = None
            
            ordered_values.append({'name': field_name, 'value': proc_val})
            
        report_data_rows.append({'date': r_date, 'values': ordered_values})

    context = {
        'header_structure': DISPLAY_HEADER_STRUCTURE_POWER,
        'report_rows': report_data_rows,
        'page_obj': page_obj,
        'from_date': from_date or '',
        'to_date': to_date or '',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
    }
    return render(request, 'utility/power/power_readings_report.html', context)


@login_required
def delete_power_readings_for_date(request, date_str):
    """
    Deletes all UtilityPowerReading records for a given date (format: YYYY-MM-DD).
    Only accessible to users with delete permission.
    """
    if not request.user.has_perm('UTILITY.delete_utilitypowerreading'):
        messages.error(request, "You do not have permission to delete Power records.")
        return redirect('power_readings_report')

    try:
        from datetime import datetime
        reading_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except Exception:
        messages.error(request, "Invalid date provided for deletion.")
        return redirect('power_readings_report')

    deleted_count, _ = UtilityPowerReading.objects.filter(reading_date=reading_date).delete()
    if deleted_count > 0:
        messages.success(request, f"✅ Deleted {deleted_count} Power reading(s) for {reading_date.strftime('%d-%m-%Y')}.")
    else:
        messages.warning(request, f"No records found for {reading_date.strftime('%d-%m-%Y')}.")

    return redirect('power_readings_report')



@login_required
def power_readings_excel(request):
    """
    Generates a multi-header, color-coded Excel report of power readings
    that matches the provided screenshot format.
    """
    if not request.user.has_perm('utility.view_utilitypowerreading'): # Use your actual permission
        messages.error(request, "You do not have permission to download Power records.")
        return redirect('indexpage')

    # 1. DATA FETCHING (Same as your existing code)
    from_date = request.GET.get('from_date')
    to_date = request.GET.get('to_date')
    date_filter = {}
    if from_date: date_filter['reading_date__gte'] = from_date
    if to_date: date_filter['reading_date__lte'] = to_date

    dates = UtilityPowerReading.objects.filter(**date_filter)\
        .values_list('reading_date', flat=True).distinct().order_by('reading_date')

    all_records = UtilityPowerReading.objects.filter(reading_date__in=list(dates))\
        .order_by('reading_date', 'reading_type', 'id')
        
    records_by_date_then_type = defaultdict(dict)
    for record in all_records:
        records_by_date_then_type[record.reading_date][record.reading_type] = record

    # 2. EXCEL WORKBOOK SETUP
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet('Power Consumption Report')

    # --- Cell Formats ---
    # These formats define the colors and styles for all parts of the report.
    # Main title format
    title_format = workbook.add_format({'bold': True, 'font_size': 18, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#FFF2CC'})
    
    # Header formats based on the screenshot's colors
    group_header_formats = {
        'E-18 POWER CONSUMPTION': workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#FFC000'}),
        'E-17 POWER CONSUMPTION': workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#9BC2E6'}),
        'E-22 POWER CONSUMPTION': workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#00B0F0'}),
        'TOTAL POWER CONSUMPTION': workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#C5E0B4'}),
    }
    
    # Format for the individual column headers (the dark green cells)
    field_header_format = workbook.add_format({
        'bold': True, 'text_wrap': True, 'align': 'center', 'valign': 'vcenter', 
        'bg_color': '#00B050', 'font_color': 'white', 'border': 1
    })
    
    # Format for the 'DATE' header cell
    date_header_format = workbook.add_format({
        'bold': True, 'text_wrap': True, 'align': 'center', 'valign': 'vcenter',
        'bg_color': '#00B050', 'font_color': 'white', 'border': 1
    })
    
    # Formats for the data cells
    date_cell_format = workbook.add_format({'num_format': 'dd/mm/yyyy', 'align': 'left', 'border': 1})
    data_cell_format = workbook.add_format({'align': 'right', 'border': 1})

    # 3. WRITE HEADERS
    
    # --- Write Main Title (merged across all columns) ---
    num_data_columns = len(ORDERED_DISPLAY_FIELDS_POWER)
    worksheet.merge_range(0, 0, 0, num_data_columns, 'POWER CONSUMPTION REPORT', title_format)
    worksheet.set_row(0, 30) # Set height for title row

    # --- Write the two-tiered, merged, and colored headers ---
    # First, write the DATE header, merged across two rows
    worksheet.merge_range('A2:A3', 'DATE', date_header_format)
    worksheet.set_column('A:A', 12) # Set width for date column
    
    current_col = 1 # Start from the second column (index 1)
    
    for group in DISPLAY_HEADER_STRUCTURE_POWER:
        num_fields = len(group['fields'])
        group_format = group_header_formats.get(group['group_label'], field_header_format)
        
        # Merge and write the top-level group header (e.g., "E-18 POWER CONSUMPTION")
        if num_fields > 0:
            worksheet.merge_range(1, current_col, 1, current_col + num_fields - 1, group['group_label'], group_format)

        # Write the second-level individual field headers below the group header
        for field_name, field_label in group['fields']:
            # Replace <br> with newline for Excel and write the header
            clean_label = field_label.replace('<br>', '\n').replace('<BR>', '\n')
            worksheet.write(2, current_col, clean_label, field_header_format)
            worksheet.set_column(current_col, current_col, 15) # Set default width for data columns
            current_col += 1
    
    # Set height for the header rows to accommodate wrapped text
    worksheet.set_row(1, 20)
    worksheet.set_row(2, 45)

    # 4. WRITE DATA ROWS
    start_row = 3 # Data starts from the 4th row (index 3)
    for row_num, r_date in enumerate(dates, start=start_row):
        # Get the consolidated data for the date
        consolidated = {field: None for field in ORDERED_DISPLAY_FIELDS_POWER}
        recs = records_by_date_then_type.get(r_date, {})
        for r_type, owned_fields in ACTUAL_TYPE_FIELDS_OWNERSHIP_POWER.items():
            rec = recs.get(r_type)
            if rec:
                for field in owned_fields:
                    if field in consolidated:
                        consolidated[field] = getattr(rec, field, None)
        
        # Write the data to the worksheet row
        worksheet.write_datetime(row_num, 0, r_date, date_cell_format)
        
        current_data_col = 1
        for field_name in ORDERED_DISPLAY_FIELDS_POWER:
            val = consolidated.get(field_name)
            if isinstance(val, decimal.Decimal):
                worksheet.write_number(row_num, current_data_col, float(val), data_cell_format)
            elif val is None:
                worksheet.write_blank(row_num, current_data_col, None, data_cell_format)
            else:
                worksheet.write(row_num, current_data_col, val, data_cell_format)
            current_data_col += 1

    # 5. FINALIZE AND RETURN RESPONSE
    workbook.close()
    output.seek(0)
    filename = f"Power_Consumption_Report_{from_date}_to_{to_date}.xlsx"
    response = HttpResponse(output.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response



@login_required
def power_consumption_report(request):
    import datetime
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    """
    View to aggregate and display the power consumption report based on date filters,
    grouping all readings for a single day into one row.
    """
    ZERO = Value(Decimal('0.0'))

    # --- Date Filtering ---
    from_date_str = request.GET.get('from_date')
    to_date_str = request.GET.get('to_date')
    today = datetime.date.today()
    if not from_date_str:
        from_date_str = today.replace(day=1).strftime('%Y-%m-%d')
    if not to_date_str:
        next_month = today.replace(day=28) + datetime.timedelta(days=4)
        last_day = next_month - datetime.timedelta(days=next_month.day)
        to_date_str = last_day.strftime('%Y-%m-%d')
    
    # --- Group by date, SUM all relevant fields, and then calculate totals ---
    daily_data = UtilityPowerReading.objects.filter(
        reading_date__gte=from_date_str,
        reading_date__lte=to_date_str
    ).values('reading_date').annotate(
        # --- Sum all individual fields needed for calculations ---
        s_utility_2=Coalesce(Sum('utility_2'), ZERO),
        s_utility_3=Coalesce(Sum('utility_3'), ZERO),
        s_tf_unit=Coalesce(Sum('tf_unit'), ZERO),
        s_ct_75hp_pump2=Coalesce(Sum('ct_75hp_pump2'), ZERO),
        s_trane1_brine_comp_110tr=Coalesce(Sum('trane1_brine_comp_110tr'), ZERO),
        s_chiller_02_trane2=Coalesce(Sum('chiller_02_trane2'), ZERO),
        s_voltas_chiller_02=Coalesce(Sum('voltas_chiller_02'), ZERO),
        s_block_c2_d04=Coalesce(Sum('block_c2_d04'), ZERO),
        s_ct_75hp_pump1=Coalesce(Sum('ct_75hp_pump1'), ZERO),
        s_new_ro=Coalesce(Sum('new_ro'), ZERO),
        s_block_a1=Coalesce(Sum('block_a1'), ZERO),
        s_block_a2=Coalesce(Sum('block_a2'), ZERO),
        s_block_b1=Coalesce(Sum('block_b1'), ZERO),
        s_block_b2=Coalesce(Sum('block_b2'), ZERO),
        s_block_b3_ut04=Coalesce(Sum('block_b3_ut04'), ZERO),
        s_utility_05_block_b_anfd=Coalesce(Sum('utility_05_block_b_anfd'), ZERO),
        s_block_d1=Coalesce(Sum('block_d1'), ZERO),
        s_block_c1=Coalesce(Sum('block_c1'), ZERO),
        s_block_d2=Coalesce(Sum('block_d2'), ZERO),
        s_c03_air_compressor_40hp=Coalesce(Sum('c03_air_compressor_40hp'), ZERO),
        s_block_b_all_ejector=Coalesce(Sum('block_b_all_ejector'), ZERO),
        s_etp_e17=Coalesce(Sum('etp_e17'), ZERO),
        s_mee_e17=Coalesce(Sum('mee_e17'), ZERO),
        s_new_atfd=Coalesce(Sum('new_atfd'), ZERO),
        s_etp_press_filter=Coalesce(Sum('etp_press_filter'), ZERO),
        s_stabilizer=Coalesce(Sum('stabilizer'), ZERO),
        s_admin=Coalesce(Sum('admin'), ZERO),
        s_others_e18_fire=Coalesce(Sum('others_e18_fire'), ZERO),
        s_imcc_panel_01_utility=Coalesce(Sum('imcc_panel_01_utility'), ZERO),
        s_imcc_panel_02_utility=Coalesce(Sum('imcc_panel_02_utility'), ZERO),
        s_brine_chiller_1_5f_30=Coalesce(Sum('brine_chiller_1_5f_30'), ZERO),
        s_water_chiller_2_4r_440=Coalesce(Sum('water_chiller_2_4r_440'), ZERO),
        s_imcc_panel_03=Coalesce(Sum('imcc_panel_03'), ZERO),
        s_imcc_panel_04=Coalesce(Sum('imcc_panel_04'), ZERO),
        s_imcc_panel_05=Coalesce(Sum('imcc_panel_05'), ZERO),
        s_row_power_panel=Coalesce(Sum('row_power_panel'), ZERO),
        s_lighting_panel=Coalesce(Sum('lighting_panel'), ZERO),
        s_others_e17=Coalesce(Sum('others_e17'), ZERO),
        s_e22_pcc=Coalesce(Sum('e22_pcc'), ZERO),
        s_e16_mseb=Coalesce(Sum('e16_mseb'), ZERO),
    ).annotate(
        # --- Perform calculations on the summed fields ---
        
        # --- E-18 Calculations ---
        e18_utility=(
            F('s_utility_2') + F('s_utility_3') + F('s_tf_unit') + F('s_ct_75hp_pump2') +
            F('s_trane1_brine_comp_110tr') + F('s_chiller_02_trane2') + F('s_voltas_chiller_02') + F('s_ct_75hp_pump1') + F('s_new_ro')
        ),
        e18_a_block=F('s_block_a1') + F('s_block_a2'),
        e18_b_block=F('s_block_b1') + F('s_block_b2') + F('s_block_b3_ut04') + F('s_utility_05_block_b_anfd'),
        e18_dc_block=F('s_block_d1') + F('s_block_c1') + F('s_block_d2') + F('s_c03_air_compressor_40hp') + F('s_block_c2_d04'),
        e18_ejector=F('s_block_b_all_ejector'),
        e18_mee_etp=F('s_etp_e17') + F('s_mee_e17') + F('s_new_atfd') + F('s_etp_press_filter'),
        e18_other=F('s_stabilizer') + F('s_admin') + F('s_others_e18_fire'),
        
        # --- E-17 Calculations ---
        e17_utility=F('s_imcc_panel_01_utility') + F('s_imcc_panel_02_utility') + F('s_brine_chiller_1_5f_30') + F('s_water_chiller_2_4r_440'),
        e17_e_block=F('s_imcc_panel_03') + F('s_imcc_panel_04') + F('s_imcc_panel_05') + F('s_row_power_panel') + F('s_lighting_panel'),
        e17_other=F('s_others_e17'),
        
        # --- E-22 & E-16 Calculations ---
        e22_total=F('s_e22_pcc'),
        e16_total=F('s_e16_mseb'),
    ).order_by('reading_date')

    # --- Calculate Grand Totals in Python for efficiency ---
    totals = {
        'total_e18_utility': sum(item['e18_utility'] for item in daily_data),
        'total_e18_a_block': sum(item['e18_a_block'] for item in daily_data),
        'total_e18_b_block': sum(item['e18_b_block'] for item in daily_data),
        'total_e18_dc_block': sum(item['e18_dc_block'] for item in daily_data),
        'total_e18_ejector': sum(item['e18_ejector'] for item in daily_data),
        'total_e18_mee_etp': sum(item['e18_mee_etp'] for item in daily_data),
        'total_e18_other': sum(item['e18_other'] for item in daily_data),
        'total_e17_utility': sum(item['e17_utility'] for item in daily_data),
        'total_e17_e_block': sum(item['e17_e_block'] for item in daily_data),
        'total_e17_other': sum(item['e17_other'] for item in daily_data),
        'total_e22': sum(item['e22_total'] for item in daily_data),
        'total_e16': sum(item['e16_total'] for item in daily_data),
    }

    # Header structure for the template
    REPORT_HEADER_STRUCTURE = [
        { 'group_label': 'E-18', 'group_bg_color': 'bg-green-200', 'fields': [
            ('e18_utility', 'Utility'), ('e18_a_block', 'A Block'), ('e18_b_block', 'B Block'),
            ('e18_dc_block', 'D & C Block'), ('e18_ejector', 'Ejector'), ('e18_mee_etp', 'MEE & ETP'),
            ('e18_other', 'Other E-18')
        ]},
        { 'group_label': 'E-17', 'group_bg_color': 'bg-yellow-200', 'fields': [
            ('e17_utility', 'Utility E-17'), ('e17_e_block', 'E Block'), ('e17_other', 'Other E-17')
        ]},
        { 'group_label': 'E-22', 'group_bg_color': 'bg-red-200', 'fields': [('e22_total', 'E-22')] },
        { 'group_label': 'E-16', 'group_bg_color': 'bg-orange-200', 'fields': [('e16_total', 'E-16')] },
    ]

    # --- Prepare data for the bar chart ---
    grand_total = sum(totals.values())
    chart_data = []

    if grand_total > 0:
        chart_data = [
            {'label': 'E18 Utility', 'value': totals['total_e18_utility'], 'percentage': (totals['total_e18_utility'] / grand_total) * 100},
            {'label': 'E18 A Block', 'value': totals['total_e18_a_block'], 'percentage': (totals['total_e18_a_block'] / grand_total) * 100},
            {'label': 'E18 B Block', 'value': totals['total_e18_b_block'], 'percentage': (totals['total_e18_b_block'] / grand_total) * 100},
            {'label': 'E18 D&C Block', 'value': totals['total_e18_dc_block'], 'percentage': (totals['total_e18_dc_block'] / grand_total) * 100},
            {'label': 'E18 Ejector', 'value': totals['total_e18_ejector'], 'percentage': (totals['total_e18_ejector'] / grand_total) * 100},
            {'label': 'E18 MEE & ETP', 'value': totals['total_e18_mee_etp'], 'percentage': (totals['total_e18_mee_etp'] / grand_total) * 100},
            {'label': 'E18 Other', 'value': totals['total_e18_other'], 'percentage': (totals['total_e18_other'] / grand_total) * 100},
            {'label': 'E17 Utility', 'value': totals['total_e17_utility'], 'percentage': (totals['total_e17_utility'] / grand_total) * 100},
            {'label': 'E17 E Block', 'value': totals['total_e17_e_block'], 'percentage': (totals['total_e17_e_block'] / grand_total) * 100},
            {'label': 'E17 Other', 'value': totals['total_e17_other'], 'percentage': (totals['total_e17_other'] / grand_total) * 100},
            {'label': 'E22 Total', 'value': totals['total_e22'], 'percentage': (totals['total_e22'] / grand_total) * 100},
            {'label': 'E16 Total', 'value': totals['total_e16'], 'percentage': (totals['total_e16'] / grand_total) * 100},
        ]

    context = {
        'daily_readings': daily_data,
        'totals': totals,
        'header_structure': REPORT_HEADER_STRUCTURE,
        'from_date': from_date_str,
        'to_date': to_date_str,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'chart_data': json.dumps(chart_data, default=str),  # Convert Decimal to string for JSON
    }
    return render(request, 'utility/power/power_consumption.html', context)



def _get_power_consumption_data(from_date_str, to_date_str):
    """
    Fetches and processes power consumption data from the database.
    This helper function is used by both the HTML view and the Excel download view.
    """
    ZERO = Value(Decimal('0.0'))

    # --- Database query remains the same ---
    daily_data = UtilityPowerReading.objects.filter(
        reading_date__gte=from_date_str,
        reading_date__lte=to_date_str
    ).values('reading_date').annotate(
        # ... all your s_... fields are correct and unchanged ...
        s_utility_2=Coalesce(Sum('utility_2'), ZERO),
        s_utility_3=Coalesce(Sum('utility_3'), ZERO),
        s_tf_unit=Coalesce(Sum('tf_unit'), ZERO),
        s_ct_75hp_pump2=Coalesce(Sum('ct_75hp_pump2'), ZERO),
        s_trane1_brine_comp_110tr=Coalesce(Sum('trane1_brine_comp_110tr'), ZERO),
        s_chiller_02_trane2=Coalesce(Sum('chiller_02_trane2'), ZERO),
        s_voltas_chiller_02=Coalesce(Sum('voltas_chiller_02'), ZERO),
        s_block_c2_d04=Coalesce(Sum('block_c2_d04'), ZERO),
        s_ct_75hp_pump1=Coalesce(Sum('ct_75hp_pump1'), ZERO),
        s_new_ro=Coalesce(Sum('new_ro'), ZERO),
        s_block_a1=Coalesce(Sum('block_a1'), ZERO),
        s_block_a2=Coalesce(Sum('block_a2'), ZERO),
        s_block_b1=Coalesce(Sum('block_b1'), ZERO),
        s_block_b2=Coalesce(Sum('block_b2'), ZERO),
        s_block_b3_ut04=Coalesce(Sum('block_b3_ut04'), ZERO),
        s_utility_05_block_b_anfd=Coalesce(Sum('utility_05_block_b_anfd'), ZERO),
        s_block_d1=Coalesce(Sum('block_d1'), ZERO),
        s_block_c1=Coalesce(Sum('block_c1'), ZERO),
        s_block_d2=Coalesce(Sum('block_d2'), ZERO),
        s_c03_air_compressor_40hp=Coalesce(Sum('c03_air_compressor_40hp'), ZERO),
        s_block_b_all_ejector=Coalesce(Sum('block_b_all_ejector'), ZERO),
        s_etp_e17=Coalesce(Sum('etp_e17'), ZERO),
        s_mee_e17=Coalesce(Sum('mee_e17'), ZERO),
        s_new_atfd=Coalesce(Sum('new_atfd'), ZERO),
        s_etp_press_filter=Coalesce(Sum('etp_press_filter'), ZERO),
        s_stabilizer=Coalesce(Sum('stabilizer'), ZERO),
        s_admin=Coalesce(Sum('admin'), ZERO),
        s_others_e18_fire=Coalesce(Sum('others_e18_fire'), ZERO),
        s_imcc_panel_01_utility=Coalesce(Sum('imcc_panel_01_utility'), ZERO),
        s_imcc_panel_02_utility=Coalesce(Sum('imcc_panel_02_utility'), ZERO),
        s_brine_chiller_1_5f_30=Coalesce(Sum('brine_chiller_1_5f_30'), ZERO),
        s_water_chiller_2_4r_440=Coalesce(Sum('water_chiller_2_4r_440'), ZERO),
        s_imcc_panel_03=Coalesce(Sum('imcc_panel_03'), ZERO),
        s_imcc_panel_04=Coalesce(Sum('imcc_panel_04'), ZERO),
        s_imcc_panel_05=Coalesce(Sum('imcc_panel_05'), ZERO),
        s_row_power_panel=Coalesce(Sum('row_power_panel'), ZERO),
        s_lighting_panel=Coalesce(Sum('lighting_panel'), ZERO),
        s_others_e17=Coalesce(Sum('others_e17'), ZERO),
        s_e22_pcc=Coalesce(Sum('e22_pcc'), ZERO),
        s_e16_mseb=Coalesce(Sum('e16_mseb'), ZERO),
    ).annotate(
        # ... all your calculation annotations are correct and unchanged ...
        e18_utility=(
            F('s_utility_2') + F('s_utility_3') + F('s_tf_unit') + F('s_ct_75hp_pump2') +
            F('s_trane1_brine_comp_110tr') + F('s_chiller_02_trane2') + F('s_voltas_chiller_02') + F('s_ct_75hp_pump1') + F('s_new_ro')
        ),
        e18_a_block=F('s_block_a1') + F('s_block_a2'),
        e18_b_block=F('s_block_b1') + F('s_block_b2') + F('s_block_b3_ut04') + F('s_utility_05_block_b_anfd'),
        e18_dc_block=F('s_block_d1') + F('s_block_c1') + F('s_block_d2') + F('s_c03_air_compressor_40hp') + F('s_block_c2_d04'),
        e18_ejector=F('s_block_b_all_ejector'),
        e18_mee_etp=F('s_etp_e17') + F('s_mee_e17') + F('s_new_atfd') + F('s_etp_press_filter'),
        e18_other=F('s_stabilizer') + F('s_admin') + F('s_others_e18_fire'),
        e17_utility=F('s_imcc_panel_01_utility') + F('s_imcc_panel_02_utility') + F('s_brine_chiller_1_5f_30') + F('s_water_chiller_2_4r_440'),
        e17_e_block=F('s_imcc_panel_03') + F('s_imcc_panel_04') + F('s_imcc_panel_05') + F('s_row_power_panel') + F('s_lighting_panel'),
        e17_other=F('s_others_e17'),
        e22_total=F('s_e22_pcc'),
        e16_total=F('s_e16_mseb'),
    ).order_by('reading_date')

    # --- Calculate Grand Totals in Python for efficiency (CORRECTED KEYS) ---
    totals = {
        'total_e18_utility': sum(item['e18_utility'] for item in daily_data),
        'total_e18_a_block': sum(item['e18_a_block'] for item in daily_data),
        'total_e18_b_block': sum(item['e18_b_block'] for item in daily_data),
        'total_e18_dc_block': sum(item['e18_dc_block'] for item in daily_data),
        'total_e18_ejector': sum(item['e18_ejector'] for item in daily_data),
        'total_e18_mee_etp': sum(item['e18_mee_etp'] for item in daily_data),
        'total_e18_other': sum(item['e18_other'] for item in daily_data),
        'total_e17_utility': sum(item['e17_utility'] for item in daily_data),
        'total_e17_e_block': sum(item['e17_e_block'] for item in daily_data),
        'total_e17_other': sum(item['e17_other'] for item in daily_data),
        # CORRECTED: The keys now consistently match the pattern 'total_' + field_key
        'total_e22_total': sum(item['e22_total'] for item in daily_data),
        'total_e16_total': sum(item['e16_total'] for item in daily_data),
    }

    # --- Header structure remains the same ---
    header_structure = [
        {'group_label': 'E-18', 'bg_color': "#B6EF93", 'fields': [
            ('e18_utility', 'UTILITY'), ('e18_a_block', 'A BLOCK'), ('e18_b_block', 'B BLOCK'),
            ('e18_dc_block', 'D & C BLOCK'), ('e18_ejector', 'EJECTOR'), ('e18_mee_etp', 'MEE & ETP'),
            ('e18_other', 'OTHER E-18')
        ]},
        {'group_label': 'E-17', 'bg_color': "#F9D055", 'fields': [
            ('e17_utility', 'UTILITY E-17'), ('e17_e_block', 'E BLOCK'), ('e17_other', 'OTHER E-17')
        ]},
        {'group_label': 'E-22', 'bg_color': "#B46EB3", 'fields': [('e22_total', 'E-22')]},
        {'group_label': 'E-16', 'bg_color': "#F06B12", 'fields': [('e16_total', 'E-16')]},
    ]
    
    return daily_data, totals, header_structure



@login_required
def power_consumption_excel(request):
    """
    Generates and downloads an Excel report of power consumption
    with a title and styling similar to the provided screenshot.
    """
    # --- Date Filtering ---
    today = datetime.date.today()
    default_from_date = today.replace(day=1).strftime('%Y-%m-%d')
    next_month = (today.replace(day=28) + datetime.timedelta(days=4))
    last_day_of_month = next_month.replace(day=1) - datetime.timedelta(days=1)
    default_to_date = last_day_of_month.strftime('%Y-%m-%d')
    from_date_str = request.GET.get('from_date', default_from_date)
    to_date_str = request.GET.get('to_date', default_to_date)
    
    daily_data, totals, header_structure = _get_power_consumption_data(from_date_str, to_date_str)

    # --- Create an in-memory Excel file ---
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet("Power Consumption")

    # --- Define Formats ---
    # Format for the main title
    title_format = workbook.add_format({'bold': True, 'font_size': 16, 'align': 'center', 'valign': 'vcenter'})
    
    # Base format for all header cells
    header_base_format = {'bold': True, 'border': 1, 'align': 'center', 'valign': 'vcenter'}

    # Specific format for the "DATE" header
    date_header_format = workbook.add_format({**header_base_format, 'bg_color': "#9FCEF7"}) # Light Blue

    # Data and Total formats
    date_format = workbook.add_format({'num_format': 'dd-mm-yyyy', 'border': 1})
    data_format = workbook.add_format({'border': 1})
    total_label_format = workbook.add_format({'bold': True, 'bg_color': '#FFC000', 'border': 1, 'align': 'right'})
    total_value_format = workbook.add_format({'bold': True, 'bg_color': '#FFC000', 'border': 1})
    
    # --- Set Column Widths ---
    worksheet.set_column('A:A', 12)
    worksheet.set_column('B:N', 15)

    # --- Write Title (RE-ADDED) ---
    # The merged title will span across all data columns (A to N, which is 14 columns).
    worksheet.merge_range('A1:N1', 'Power Consumption Report', title_format)

    # --- Write Headers ---
    # Start headers on the 3rd row (index 2) to leave space for the title and a small gap.
    row, col = 2, 0
    
    # Write the merged "DATE" header
    worksheet.merge_range(row, col, row + 1, col, 'DATE', date_header_format)
    col += 1

    # Loop to write the colored group and sub-headers
    sub_header_row = row + 1
    for group in header_structure:
        num_fields = len(group['fields'])
        
        # Create a specific format for this group using its defined background color
        group_specific_format = workbook.add_format({**header_base_format, 'bg_color': group['bg_color']})
        
        # Merge and write the top-level group header (e.g., E-18)
        if num_fields > 1:
            worksheet.merge_range(row, col, row, col + num_fields - 1, group['group_label'], group_specific_format)
        else:
            worksheet.write(row, col, group['group_label'], group_specific_format)
        
        # Write the sub-headers below using the same colored format
        for field_key, field_label in group['fields']:
             worksheet.write(sub_header_row, col, field_label, group_specific_format)
             col += 1
    
    # --- Write Data Rows ---
    current_row = sub_header_row + 1
    for item in daily_data:
        col = 0
        worksheet.write_datetime(current_row, col, item['reading_date'], date_format)
        col += 1
        for group in header_structure:
            for field_key, _ in group['fields']:
                worksheet.write_number(current_row, col, item[field_key], data_format)
                col += 1
        current_row += 1

    # --- Write Total Row ---
    col = 0
    worksheet.write(current_row, col, 'Total', total_label_format)
    col += 1
    for group in header_structure:
        for field_key, _ in group['fields']:
            total_key = f"total_{field_key}"
            worksheet.write_number(current_row, col, totals[total_key], total_value_format)
            col += 1

    # --- Finalize and Return ---
    workbook.close()
    output.seek(0)

    filename = f"Power_Consumption_Report_{from_date_str}_to_{to_date_str}.xlsx"
    response = HttpResponse(
        output,
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{filename}"'

    return response








