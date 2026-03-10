from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from .models import * 
from .forms import RAndDMoistureForm
from django.shortcuts import get_object_or_404
from django.db.models import Q
import io
import xlsxwriter
from django.http import HttpResponse
from .models import KFFactorEntry, KFFactorEntryLine, RDMaster,MeltingPointRecord
from .forms import KFFactorEntryForm, KFFactorEntryLineFormSet,KFFactorEntryLineForm,MeltingPointRecordForm
from django.utils import timezone
from django.db.models import Prefetch
from django.forms import inlineformset_factory
from django.utils.dateparse import parse_date
import re,datetime


@login_required
def add_r_and_d_moisture(request):
    user_groups      = request.user.groups.values_list('name', flat=True)
    is_superuser     = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)

    if not request.user.has_perm('R_and_D.add_r_and_d_moisture'):
        messages.error(request, "You do not have permission to add R&D Moisture records.")
        return redirect('indexpage')

    if request.method == 'POST':
        form = RAndDMoistureForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Moisture record added successfully!")
            return redirect('r_and_d_moisture_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        now = datetime.datetime.now()
        today = now.date()
        current_time = now.strftime('%H:%M')  # HH:MM format for <input type="time">
        form = RAndDMoistureForm(initial={
            'entry_date': today,
            'entry_time': current_time,
            'completed_date': today,
            'completed_time': current_time,
        })

    return render(request, 'r_and_d/moisture_form.html', {
        'form': form,
        'active_link': 'r_and_d_moisture',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'show_admin_panel': show_admin_panel,
    })

@login_required
def r_and_d_moisture_list(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)

    records = R_and_D_Moisture.objects.all()

    # FILTERS
    entry_date_from = request.GET.get('entry_date_from')
    entry_date_to = request.GET.get('entry_date_to')
    product_name = request.GET.get('product_name', '').strip()
    batch_no = request.GET.get('batch_no', '').strip()
    unit = request.GET.get('unit', '').strip()
    instrument = request.GET.get('instrument', '').strip()
    analysed_by = request.GET.get('analysed_by', '').strip()

    if entry_date_from:
        records = records.filter(entry_date__gte=entry_date_from)
    if entry_date_to:
        records = records.filter(entry_date__lte=entry_date_to)
    if product_name:
        records = records.filter(product_name__icontains=product_name)
    if batch_no:
        records = records.filter(batch_no__icontains=batch_no)
    if unit:
        records = records.filter(unit__name__icontains=unit)
    if instrument:
        records = records.filter(instrument__name__icontains=instrument)
    if analysed_by:
        records = records.filter(analysed_by__name__icontains=analysed_by)

    records = records.order_by('-entry_date', '-id')
    paginator = Paginator(records, 10)  # Show 10 per page, change as needed
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # Build the querystring for filter persistence
    get_dict = request.GET.copy()
    if 'page' in get_dict:
        get_dict.pop('page')
    querystring = get_dict.urlencode()

    return render(request, 'r_and_d/moisture_list.html', {
        'page_obj': page_obj,
        'active_link': 'r_and_d_moisture',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'show_admin_panel': show_admin_panel,
        'filters': {
            'entry_date_from': entry_date_from or '',
            'entry_date_to': entry_date_to or '',
            'product_name': product_name,
            'batch_no': batch_no,
            'unit': unit,
            'instrument': instrument,
            'analysed_by': analysed_by,
        },
        'querystring': querystring,  # <-- pass this to template
    })


@login_required
def edit_r_and_d_moisture(request, pk):
    user_groups      = request.user.groups.values_list('name', flat=True)
    is_superuser     = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)
    obj = get_object_or_404(R_and_D_Moisture, pk=pk)
    if request.method == 'POST':
        form = RAndDMoistureForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Moisture record updated successfully!")
            return redirect('r_and_d_moisture_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = RAndDMoistureForm(instance=obj)

    return render(request, 'r_and_d/moisture_form.html', {
        'form': form,
        'active_link': 'r_and_d_moisture',
        'user_groups':       user_groups,
        'is_superuser':      is_superuser,
        'show_admin_panel':show_admin_panel,
    })



@login_required
def delete_r_and_d_moisture(request, pk):
    obj = get_object_or_404(R_and_D_Moisture, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Moisture record deleted successfully!")
        return redirect('r_and_d_moisture_list')
    return redirect('r_and_d_moisture_list')



@login_required
def r_and_d_moisture_download_xlsx(request):
    records = R_and_D_Moisture.objects.all()

    # Apply filters (same as your list view)
    entry_date_from = request.GET.get('entry_date_from')
    entry_date_to = request.GET.get('entry_date_to')
    product_name = request.GET.get('product_name', '').strip()
    batch_no = request.GET.get('batch_no', '').strip()
    unit = request.GET.get('unit', '').strip()
    instrument = request.GET.get('instrument', '').strip()
    analysed_by = request.GET.get('analysed_by', '').strip()

    if entry_date_from:
        records = records.filter(entry_date__gte=entry_date_from)
    if entry_date_to:
        records = records.filter(entry_date__lte=entry_date_to)
    if product_name:
        records = records.filter(product_name__icontains=product_name)
    if batch_no:
        records = records.filter(batch_no__icontains=batch_no)
    if unit:
        records = records.filter(unit__name__icontains=unit)
    if instrument:
        records = records.filter(instrument__name__icontains=instrument)
    if analysed_by:
        records = records.filter(analysed_by__name__icontains=analysed_by)

    records = records.order_by('-entry_date', '-id')

    # Create the in-memory file
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet("Moisture Records")

    big_title = "R&D Moisture Records"
    worksheet.merge_range('A1:P1', big_title, workbook.add_format({
        'bold': True,
        'font_size': 14,
        'align': 'center',
        'valign': 'vcenter',
        'bg_color': '#C6EFCE',
        'font_color': '#006100',
    }))

    # Define the header format
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#4682B4',  # Deep blue
        'font_color': '#FFFFFF',
        'border': 1,
        'align': 'center'
    })

    # Headers including Sr. No
    headers = [
        "Sr. No", "Entry Date", "Entry Time", "ELN ID", "Product", "Batch No", "Sample Description", "Unit",
        "Instrument", "Factor (mg/mL)", "Sample Weight (gm)", "Burette Reading (mL)", "Moisture (%)",
        "Analysed By", "Completed Date", "Completed Time"
    ]

    # Write headers with format (second row)
    for col_num, header in enumerate(headers):
        worksheet.write(1, col_num, header, header_format)

    # Write data rows starting from third row (row_num + 2)
    for row_num, obj in enumerate(records, 1):
        worksheet.write(row_num + 1, 0, row_num)  # Sr. No
        worksheet.write(row_num + 1, 1, obj.entry_date.strftime('%d-%m-%Y') if obj.entry_date else '')
        worksheet.write(row_num + 1, 2, obj.entry_time.strftime('%H:%M') if obj.entry_time else '')
        worksheet.write(row_num + 1, 3, obj.eln_id or '')
        worksheet.write(row_num + 1, 4, obj.product_name if obj.product_name else '')
        worksheet.write(row_num + 1, 5, obj.batch_no)
        worksheet.write(row_num + 1, 6, obj.sample_description)
        worksheet.write(row_num + 1, 7, obj.unit.name if obj.unit else '')
        worksheet.write(row_num + 1, 8, obj.instrument.name if obj.instrument else '')
        worksheet.write(row_num + 1, 9, float(obj.factor_mg_per_ml) if obj.factor_mg_per_ml else '')
        worksheet.write(row_num + 1, 10, float(obj.sample_weight_gm) if obj.sample_weight_gm else '')
        worksheet.write(row_num + 1, 11, float(obj.burette_reading_ml) if obj.burette_reading_ml else '')
        worksheet.write(row_num + 1, 12, float(obj.moisture_percent) if obj.moisture_percent else '')
        worksheet.write(row_num + 1, 13, obj.analysed_by.name if obj.analysed_by else '')
        worksheet.write(row_num + 1, 14, obj.completed_date.strftime('%d-%m-%Y') if obj.completed_date else '')
        worksheet.write(row_num + 1, 15, obj.completed_time.strftime('%H:%M') if obj.completed_time else '')

    worksheet.freeze_panes(2, 0)  # Freeze after the big header and column header

    workbook.close()
    output.seek(0)

    response = HttpResponse(
        output.read(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = 'attachment; filename=R_and_D_Moisture.xlsx'
    return response




@login_required
def add_kf_factor_entry(request):
    user_groups      = request.user.groups.values_list('name', flat=True)
    is_superuser     = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)

    if not request.user.has_perm('R_and_D.add_kffactorentry'):
        messages.error(request, "You do not have permission to add R&D Moisture records.")
        return redirect('indexpage')
    
    if request.method == 'POST':
        form = KFFactorEntryForm(request.POST)
        formset = KFFactorEntryLineFormSet(request.POST)
        if form.is_valid() and formset.is_valid():
            entry = form.save(commit=False)
            entry.date = timezone.now().date()  # Automatically set the date
            entry.save()
            formset.instance = entry
            formset.save()
            messages.success(request, "KF Factor Entry added successfully.")
            # return redirect('kf_factor_entry_list')  # Change to your list/detail url
        else:
            messages.error(request, "Please fix the errors below.")
    else:
        form = KFFactorEntryForm()
        formset = KFFactorEntryLineFormSet()

    return render(request, 'r_and_d/add_kf_factor_entry.html', {
        'form': form,
        'formset': formset,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'show_admin_panel':show_admin_panel,
    })



@login_required
def kf_factor_entry_edit(request, pk=None):
    user_groups      = request.user.groups.values_list('name', flat=True)
    is_superuser     = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)

    if pk:
        entry = get_object_or_404(KFFactorEntry, pk=pk)
        is_edit = True
    else:
        entry = None
        is_edit = False

    # Use extra=0 for edit, extra=3 for add
    if is_edit:
        KFFactorEntryLineFormSetCls = inlineformset_factory(
            KFFactorEntry, KFFactorEntryLine,
            form=KFFactorEntryLineForm,
            extra=0, can_delete=False,
            fk_name='entry'   # <-- THIS IS THE FIX!
        )
    else:
        KFFactorEntryLineFormSetCls = inlineformset_factory(
            KFFactorEntry, KFFactorEntryLine,
            form=KFFactorEntryLineForm,
            extra=3, can_delete=False,
            fk_name='entry'   # <-- THIS IS THE FIX!
        )

    if request.method == 'POST':
        form = KFFactorEntryForm(request.POST, instance=entry)
        formset = KFFactorEntryLineFormSetCls(request.POST, instance=entry)
        if form.is_valid() and formset.is_valid():
            entry = form.save(commit=False)
            if not is_edit:
                entry.created_at = timezone.now()
            entry.save()
            formset.instance = entry
            formset.save()
            messages.success(request, f"KF Factor Entry {'updated' if is_edit else 'added'} successfully.")
            return redirect('kf_factor_entry_list')
        else:
            # print("FORM ERRORS:", form.errors)
            # print("FORM CLEANED DATA:", getattr(form, "cleaned_data", None))
            # print("FORMSET ERRORS:", formset.errors)
            # print("FORMSET NON FORM ERRORS:", formset.non_form_errors())
            messages.error(request, "Please fix the errors below.")
    else:
        form = KFFactorEntryForm(instance=entry)
        formset = KFFactorEntryLineFormSetCls(instance=entry)
    # Debugging: Print lines count
    # print(f"Loaded {formset.total_form_count()} line(s) for entry id={pk}")
    return render(request, 'r_and_d/add_kf_factor_entry.html', {
        'form': form,
        'formset': formset,
        'is_edit': is_edit,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'show_admin_panel':show_admin_panel,
    })


@login_required
def kf_factor_entry_list(request):
    user_groups      = request.user.groups.values_list('name', flat=True)
    is_superuser     = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)

    # Get filter params
    date_from = request.GET.get('date_from')
    date_to = request.GET.get('date_to')
    instrument_id = request.GET.get('instrument')
    analysed_by_id = request.GET.get('analysed_by')

    # Build queryset
    entries = KFFactorEntry.objects.select_related('instrument', 'analysed_by').prefetch_related('lines')

    # Date filter
    if date_from:
        entries = entries.filter(created_at__date__gte=date_from)
    if date_to:
        entries = entries.filter(created_at__date__lte=date_to)

    # Instrument filter
    if instrument_id:
        entries = entries.filter(instrument_id=instrument_id)
    # Analysed By filter
    if analysed_by_id:
        entries = entries.filter(analysed_by_id=analysed_by_id)

    entries = entries.order_by('-created_at')

    # Pagination
    paginator = Paginator(entries, 10)  # 10 entries per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    # Prepare lines for each entry
    table_data = []
    for entry in page_obj:
        lines = list(entry.lines.all())
        while len(lines) < 3:
            lines.append(None)
        avg_kf = (
            round(
                sum([l.kf_factor for l in lines if l and l.kf_factor]) / len([l for l in lines if l and l.kf_factor]), 4
            ) if any(l and l.kf_factor for l in lines) else None
        )
        table_data.append({
            'entry': entry,
            'lines': lines,
            'avg_kf': avg_kf,
        })

    # Fetch choices for dropdowns
    from .models import RDMaster
    instruments = RDMaster.objects.filter(category='Instrument')
    analysts = RDMaster.objects.filter(category='Analyst')

    context = {
        'table_data': table_data,
        'page_obj': page_obj,
        'date_from': date_from,
        'date_to': date_to,
        'instrument_id': instrument_id,
        'analysed_by_id': analysed_by_id,
        'instruments': instruments,
        'analysts': analysts,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'show_admin_panel':show_admin_panel,
    }
    return render(request, 'r_and_d/kf_factor_entry_list.html', context)



@login_required
def kf_factor_entry_delete(request, pk):
    entry = get_object_or_404(KFFactorEntry, pk=pk)
    if request.method == "POST":
        entry.delete()
        messages.success(request, "KF Factor Entry deleted successfully.")
        return redirect('kf_factor_entry_list')  # Change to your list view name

@login_required
def kf_factor_entry_download_excel(request):
    date_from = request.GET.get('date_from')
    date_to = request.GET.get('date_to')
    instrument_id = request.GET.get('instrument')
    analysed_by_id = request.GET.get('analysed_by')

    if not date_from or date_from == "None":
        date_from = None
    if not date_to or date_to == "None":
        date_to = None
    if not instrument_id or instrument_id == "None":
        instrument_id = None
    if not analysed_by_id or analysed_by_id == "None":
        analysed_by_id = None

    entries = KFFactorEntry.objects.select_related('instrument', 'analysed_by').prefetch_related('lines')
    if date_from:
        entries = entries.filter(created_at__date__gte=date_from)
    if date_to:
        entries = entries.filter(created_at__date__lte=date_to)
    if instrument_id:
        entries = entries.filter(instrument_id=instrument_id)
    if analysed_by_id:
        entries = entries.filter(analysed_by_id=analysed_by_id)
    entries = entries.order_by('-created_at')

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet('KF Factor Entries')

    # Big merged title/header
    big_header = "KF Factor Entry Register"
    worksheet.merge_range('A1:N1', big_header, workbook.add_format({
        'bold': True,
        'font_size': 14,
        'align': 'center',
        'valign': 'vcenter',
        'bg_color': '#C6EFCE',
        'font_color': '#006100',
    }))

    # Create a header format with background color
    header_format = workbook.add_format({
        'bold': True,
        'bg_color': '#CCE5FF',  # Light blue, change as needed
        'font_color': '#000000',
        'border': 1,
        'align': 'center',
        'valign': 'vcenter'
    })

    header = [
        'Sr No', 'Entry Date', 'Instrument', 'Analysed By',
        'Sample Weight 01 (gm)', 'Sample Weight 02 (gm)', 'Sample Weight 03 (gm)',
        'Burette Reading 01 (mL)', 'Burette Reading 02 (mL)', 'Burette Reading 03 (mL)',
        'KF Factor 01', 'KF Factor 02', 'KF Factor 03', 'Average KF Factor'
    ]

    # Write header with formatting (second row)
    worksheet.write_row(1, 0, header, header_format)

    for row_num, entry in enumerate(entries, start=2):  # Data starts from row 2 (row 3 in Excel)
        lines = list(entry.lines.all())
        while len(lines) < 3:
            lines.append(None)
        avg_kf = (
            round(
                sum([l.kf_factor for l in lines if l and l.kf_factor]) / len([l for l in lines if l and l.kf_factor]), 4
            ) if any(l and l.kf_factor for l in lines) else None
        )

        row = [
            row_num - 1,
            entry.created_at.strftime("%d-%m-%Y"),
            entry.instrument.name if entry.instrument else '',
            entry.analysed_by.name if entry.analysed_by else '',
        ]
        row.extend([l.sample_weight_mg if l else '' for l in lines])
        row.extend([l.burette_reading_ml if l else '' for l in lines])
        row.extend([round(l.kf_factor, 4) if l and l.kf_factor else '' for l in lines])
        row.append(avg_kf if avg_kf is not None else '')
        worksheet.write_row(row_num, 0, row)

    worksheet.freeze_panes(2, 0)  # Freeze below the header

    workbook.close()
    output.seek(0)
    filename = f'KF_Factor_Entries_{datetime.date.today().strftime("%Y%m%d")}.xlsx'
    response = HttpResponse(
        output,
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename={filename}'
    return response



@login_required
def add_melting_point_record(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)

    if not request.user.has_perm('R_and_D.add_meltingpointrecord'):  # Replace YOURAPP!
        messages.error(request, "You do not have permission to add Melting Point records.")
        return redirect('indexpage')

    if request.method == 'POST':
        form = MeltingPointRecordForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, "Melting Point record added successfully!")
            return redirect('melting_point_record_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        now = datetime.datetime.now()
        today = now.date()
        current_time = now.strftime('%H:%M')
        form = MeltingPointRecordForm(initial={
            'entry_date': today,
            'entry_time': current_time,
            'completed_date': today,
            'completed_time': current_time,
        })

    return render(request, 'r_and_d/melting_point_form.html', {
        'form': form,
        'active_link': 'melting_point_record',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'show_admin_panel': show_admin_panel,
    })

@login_required
def melting_point_record_list(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)

    records = MeltingPointRecord.objects.all()

    # FILTERS
    entry_date_from = request.GET.get('entry_date_from')
    entry_date_to = request.GET.get('entry_date_to')
    product_name = request.GET.get('product_name', '').strip()
    batch_no = request.GET.get('batch_no', '').strip()
    unit = request.GET.get('unit', '').strip()
    instrument = request.GET.get('instrument', '').strip()
    analysed_by = request.GET.get('analysed_by', '').strip()

    if entry_date_from:
        records = records.filter(entry_date__gte=entry_date_from)
    if entry_date_to:
        records = records.filter(entry_date__lte=entry_date_to)
    if product_name:
        records = records.filter(product_name__icontains=product_name)
    if batch_no:
        records = records.filter(batch_no__icontains=batch_no)
    if unit:
        records = records.filter(unit__icontains=unit)
    if instrument:
        records = records.filter(instrument__icontains=instrument)
    if analysed_by:
        records = records.filter(analysed_by__icontains=analysed_by)

    records = records.order_by('-entry_date', '-id')
    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    get_dict = request.GET.copy()
    if 'page' in get_dict:
        get_dict.pop('page')
    querystring = get_dict.urlencode()

    return render(request, 'r_and_d/melting_point_list.html', {
        'page_obj': page_obj,
        'active_link': 'melting_point_record',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'show_admin_panel': show_admin_panel,
        'filters': {
            'entry_date_from': entry_date_from or '',
            'entry_date_to': entry_date_to or '',
            'product_name': product_name,
            'batch_no': batch_no,
            'unit': unit,
            'instrument': instrument,
            'analysed_by': analysed_by,
        },
        'querystring': querystring,
    })

@login_required
def edit_melting_point_record(request, pk):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)
    if not request.user.has_perm('R_and_D.change_meltingpointrecord'):  # Replace YOURAPP!
        messages.error(request, "You do not have permission to Update Melting Point records.")
        return redirect('indexpage')
    
    obj = get_object_or_404(MeltingPointRecord, pk=pk)
    if request.method == 'POST':
        form = MeltingPointRecordForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Melting Point record updated successfully!")
            return redirect('melting_point_record_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = MeltingPointRecordForm(instance=obj)

    return render(request, 'r_and_d/melting_point_form.html', {
        'form': form,
        'active_link': 'melting_point_record',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'show_admin_panel': show_admin_panel,
    })

@login_required
def delete_melting_point_record(request, pk):
    obj = get_object_or_404(MeltingPointRecord, pk=pk)
    if request.method == 'POST':
        obj.delete()
        messages.success(request, "Melting Point record deleted successfully!")
        return redirect('melting_point_record_list')
    return redirect('melting_point_record_list')



# --- helper (same logic as the template filter) ---
_num_re = re.compile(r"^\s*(\d+(?:\.\d+)?)(?:\s*[-–—]\s*(\d+(?:\.\d+)?))?\s*$")
def format_melting_point(mp: str | None) -> str:
    if not mp:
        return ""
    s = str(mp).replace("–", "-").replace("—", "-").strip()
    m = _num_re.match(s)
    if not m:
        return s  # fallback
    a, b = m.group(1), m.group(2)
    # tidy trailing zeros
    if "." in a: a = a.rstrip("0").rstrip(".")
    if b and "." in b: b = b.rstrip("0").rstrip(".")
    return f"{a}°C - {b}°C" if b else f"{a}°C"

@login_required
def melting_point_record_download_excel(request):
    entry_date_from = request.GET.get('entry_date_from')
    entry_date_to = request.GET.get('entry_date_to')
    product_name = request.GET.get('product_name', '').strip()
    batch_no = request.GET.get('batch_no', '').strip()
    unit = request.GET.get('unit', '').strip()
    instrument = request.GET.get('instrument', '').strip()
    analysed_by = request.GET.get('analysed_by', '').strip()

    qs = MeltingPointRecord.objects.all()
    if entry_date_from:
        qs = qs.filter(entry_date__gte=entry_date_from)
    if entry_date_to:
        qs = qs.filter(entry_date__lte=entry_date_to)
    if product_name:
        qs = qs.filter(product_name__icontains=product_name)
    if batch_no:
        qs = qs.filter(batch_no__icontains=batch_no)
    if unit:
        qs = qs.filter(unit__icontains=unit)
    if instrument:
        qs = qs.filter(instrument__icontains=instrument)
    if analysed_by:
        qs = qs.filter(analysed_by__icontains=analysed_by)
    qs = qs.order_by('-entry_date', '-id')

    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {'in_memory': True})
    ws = wb.add_worksheet('Melting Point Records')

    # Title
    title_fmt = wb.add_format({
        'bold': True, 'font_size': 14, 'align': 'center', 'valign': 'vcenter',
        'bg_color': '#C6EFCE', 'font_color': '#006100'
    })
    ws.merge_range('A1:M1', "Analytical R&D / Entry Register: Melting point", title_fmt)

    # Headers
    header_fmt = wb.add_format({
        'bold': True, 'bg_color': '#CCE5FF', 'font_color': '#000000',
        'border': 1, 'align': 'center', 'valign': 'vcenter'
    })
    right_fmt = wb.add_format({'align': 'right'})  # for MP column if desired

    headers = [
        'Sr No', 'Entry Date', 'Entry Time', 'ELN ID', 'Product Name',
        'Batch No', 'Sample Description', 'Unit', 'Instrument',
        'Melting Point', 'Analysed By', 'Completed Date', 'Completed Time'
    ]
    ws.write_row(1, 0, headers, header_fmt)

    row = 2
    for idx, obj in enumerate(qs, start=1):
        ws.write(row, 0, idx)
        ws.write(row, 1, obj.entry_date.strftime("%d-%m-%Y") if obj.entry_date else '')
        ws.write(row, 2, obj.entry_time.strftime("%H:%M") if obj.entry_time else '')
        ws.write(row, 3, obj.eln_id or '')
        ws.write(row, 4, obj.product_name or '')
        ws.write(row, 5, obj.batch_no or '')
        ws.write(row, 6, obj.sample_description or '')
        ws.write(row, 7, obj.unit or '')
        ws.write(row, 8, obj.instrument or '')

        # 🔥 formatted melting point (e.g., "101°C - 103°C")
        ws.write(row, 9, format_melting_point(obj.melting_point), right_fmt)

        ws.write(row,10, obj.analysed_by or '')
        ws.write(row,11, obj.completed_date.strftime("%d-%m-%Y") if obj.completed_date else '')
        ws.write(row,12, obj.completed_time.strftime("%H:%M") if obj.completed_time else '')
        row += 1

    ws.freeze_panes(2, 0)
    # (optional) set a comfy width for MP column
    ws.set_column(9, 9, 16)

    wb.close()
    output.seek(0)
    filename = f'Melting_Point_Records_{datetime.date.today().strftime("%Y%m%d")}.xlsx'
    resp = HttpResponse(
        output,
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    resp['Content-Disposition'] = f'attachment; filename={filename}'
    return resp