import logging
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.shortcuts import render, redirect,get_object_or_404
from django.contrib import messages
from EHS.forms import *
from .models import *
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse,JsonResponse
import pandas as pd
from datetime import datetime,time,date
import io
import xlsxwriter
from django.db.models import Count
from django.views.decorators.csrf import csrf_exempt
from django.utils.dateparse import parse_date
from django.db.models import Q
from django.utils import timezone 
from django.utils.http import url_has_allowed_host_and_scheme


logger = logging.getLogger('custom_logger')





# @csrf_exempt  # If you use JS fetch(), otherwise with jQuery setup, you may not need this.
@login_required
def add_physical_location(request):
    if request.method == "POST":
        logger.info(f"User '{request.user.username}' ({request.user.id}) attempting to add physical location")
        form = PhysicalLocationForm(request.POST)
        if form.is_valid():
            loc = form.save()
            logger.info(f"Physical location '{loc.name}' (ID: {loc.id}) created by user '{request.user.username}' ({request.user.id})")
            return JsonResponse({
                "success": True,
                "location": {"id": loc.id, "name": loc.name},
                "message": "Location added successfully!"
            })
        else:
            error = form.errors.as_json()
            logger.warning(f"User '{request.user.username}' ({request.user.id}) submitted invalid physical location data: {error}")
            return JsonResponse({"success": False, "error": error})
    logger.warning(f"User '{request.user.username}' ({request.user.id}) made invalid request method to add_physical_location: {request.method}")
    return JsonResponse({"success": False, "error": "Invalid request"})




@login_required
def add_leading_record(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('EHS.add_leadingrecords'):
        messages.error(request, "You do not have permission to add Leading records.")
        logger.warning(f"User '{request.user.username}' tried to add a leading record without permission.")
        return redirect('indexpage')

    if request.method == 'POST':
        form = LeadingRecordForm(request.POST)
        if form.is_valid():
            record = form.save(commit=False)
            record.created_by = request.user  # <-- capture who added
            record.save()
            logger.info(f"Leading record (ID: {record.id}) created by '{request.user.username}' ({request.user.id})")
            messages.success(request, "Leading record created successfully!")
            return redirect('add_leading_record')
        else:
            messages.error(request, "Please correct the errors below.")
            logger.warning(f"User '{request.user.username}' submitted invalid leading record data: {form.errors}")
    else:
        form = LeadingRecordForm()
    return render(request, 'leading/add_leading_record.html', 
                  {'form': form, 'user_groups': user_groups, 'is_superuser': is_superuser})


@login_required
def view_leading_records(request):
    logger.info("User=%s accessed Leading records", request.user.username)
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('EHS.view_leadingrecords'):
        messages.error(request, "You do not have permission to View Leading records.")
        logger.warning(f"User '{request.user.username}' tried to view leading records without permission.")
        return redirect('indexpage')
    
    can_edit_leading_records = request.user.has_perm('EHS.change_leadingrecords')
    can_delete_leading_records = request.user.has_perm('EHS.delete_leadingrecords')
    can_add_leading_records = request.user.has_perm('EHS.add_leadingrecords')
    can_view_leading_records = request.user.has_perm('EHS.view_leadingrecords')

    # Filters
    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    department_filter = request.GET.get('department_filter', '')
    status_filter = request.GET.get('status_filter', '')
    physical_location_filter = request.GET.get('physical_location_filter', '')
    risk_factor_filter = request.GET.get('risk_factor_filter', '')

    records = LeadingRecords.objects.all().order_by('-id')

    filter_data = {}
    if from_date:
        records = records.filter(observation_date__gte=from_date)
        filter_data['from_date'] = from_date
    if to_date:
        records = records.filter(observation_date__lte=to_date)
        filter_data['to_date'] = to_date
    if department_filter:
        records = records.filter(department__icontains=department_filter)
        filter_data['department_filter'] = department_filter
    if status_filter:
        records = records.filter(status=status_filter)
        filter_data['status_filter'] = status_filter
    if physical_location_filter:
        records = records.filter(physical_location__name__icontains=physical_location_filter)
        filter_data['physical_location_filter'] = physical_location_filter
    if risk_factor_filter:
        records = records.filter(risk_factor__icontains=risk_factor_filter)
        filter_data['risk_factor_filter'] = risk_factor_filter

    if filter_data:
        logger.info(
            f"User '{request.user.username}' filtered leading records with {filter_data}. "
            f"Result count: {records.count()}"
        )

    # Pagination
    paginator = Paginator(records, 10)  # 10 per page
    page = request.GET.get('page')
    try:
        paginated_records = paginator.page(page)
    except PageNotAnInteger:
        paginated_records = paginator.page(1)
    except EmptyPage:
        paginated_records = paginator.page(paginator.num_pages)

    logger.info(
        f"User '{request.user.username}' viewed leading records page: {page or 1} "
        f"(records this page: {paginated_records.object_list.count()})"
    )

    # Build URLs for preserving state
    list_url = request.get_full_path()          # includes current page & filters
    qs = request.GET.copy()
    qs.pop('page', None)                        # filters only (no page) for clean pagination links
    querystring_no_page = qs.urlencode()

    context = {
        'STATUS_CHOICES': STATUS_CHOICES,
        'leading_records': paginated_records,
        'from_date': from_date,
        'to_date': to_date,
        'department_filter': department_filter,
        'status_filter': status_filter,
        'physical_location_filter': physical_location_filter,
        'risk_factor_filter': risk_factor_filter,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'can_edit_leading_records': can_edit_leading_records,
        'can_delete_leading_records': can_delete_leading_records,
        'can_add_leading_records': can_add_leading_records,
        'can_view_leading_records': can_view_leading_records,

        # NEW: use these in templates
        'list_url': list_url,                         # for ?next=...
        'querystring_no_page': querystring_no_page,   # for pagination links
    }

    return render(request, 'leading/view_leading.html', context)


@login_required
def leading_record_detail(request, pk):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('EHS.view_leadingrecords'):
        messages.error(request, "You do not have permission to View Leading records deatail.")
        return redirect('indexpage')

    record = get_object_or_404(LeadingRecords, pk=pk)

    # carry list state (page + filters) into template
    next_url = request.GET.get('next')
    # optional: only keep it if it points back to this host
    if next_url and not url_has_allowed_host_and_scheme(next_url, {request.get_host()}):
        next_url = None

    return render(
        request,
        'leading/view_leading_detail.html',
        {
            'record': record,
            'user_groups': user_groups,
            'is_superuser': is_superuser,
            'next': next_url,  # <-- use this in the template for Back/Edit links
        },
    )


@login_required
def edit_leading_record(request, pk):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('EHS.change_leadingrecords'):
        messages.error(request, "You do not have permission to edit Leading records.")
        logger.warning(f"User '{request.user.username}' tried to edit Leading record (ID: {pk}) without permission.")
        return redirect('indexpage')

    leading_obj = get_object_or_404(LeadingRecords, pk=pk)

    # carry the pagination/filter URL through GET->POST->redirect
    next_url = request.GET.get('next') or request.POST.get('next')

    if request.method == 'POST':
        form = LeadingRecordForm(request.POST, instance=leading_obj)
        if form.is_valid():
            old_data = {field.name: getattr(leading_obj, field.name) for field in leading_obj._meta.fields}
            record = form.save()
            logger.info(
                f"Leading record (ID: {pk}) edited by '{request.user.username}' ({request.user.id}). "
                f"Old data: {old_data}. Updated data: "
                f"{ {field: getattr(record, field) for field in old_data.keys()} }"
            )
            messages.success(request, "Leading record updated successfully!")

            # redirect back to the exact page+filters, if safe; else fallback
            if next_url and url_has_allowed_host_and_scheme(next_url, allowed_hosts={request.get_host()}):
                return redirect(next_url)
            return redirect('view_leading')
        else:
            logger.warning(
                f"User '{request.user.username}' submitted invalid data for Leading record (ID: {pk}): {form.errors}"
            )
            messages.error(request, "Please correct the errors below.")
    else:
        form = LeadingRecordForm(instance=leading_obj)

    logger.info(f"User '{request.user.username}' accessed edit page for Leading record (ID: {pk})")

    return render(request, 'leading/edit_leading_record.html', {
        'form': form,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'leading_obj': leading_obj,
        'next': next_url,  # optional, if you want to use it directly in the template
    })

@login_required
def delete_leading_record(request, pk):
    # Permission check
    if not request.user.has_perm('EHS.delete_leadingrecords'):
        messages.error(request, "You do not have permission to delete Leading records.")
        logger.warning(f"User '{request.user.username}' tried to delete Leading record (ID: {pk}) without permission.")
        return redirect('indexpage')

    leading_obj = get_object_or_404(LeadingRecords, pk=pk)

    # carry the pagination/filter URL through GET->POST->redirect
    next_url = request.GET.get('next') or request.POST.get('next')

    if request.method == 'POST':
        # Optional: capture record info before deletion for audit
        record_data = {field.name: getattr(leading_obj, field.name) for field in leading_obj._meta.fields}
        leading_obj.delete()
        logger.info(
            f"Leading record (ID: {pk}) deleted by '{request.user.username}' ({request.user.id}). "
            f"Deleted record data: {record_data}"
        )
        messages.success(request, "Leading record deleted successfully!")

        # redirect back to the exact page+filters, if safe; else fallback
        if next_url and url_has_allowed_host_and_scheme(next_url, allowed_hosts={request.get_host()}):
            return redirect(next_url)
        return redirect('view_leading')

    logger.info(f"User '{request.user.username}' accessed delete confirmation page for Leading record (ID: {pk})")

    return render(
        request,
        'leading/delete_leading_record_confirm.html',
        {
            'leading_obj': leading_obj,
            'next': next_url,  # so the template can keep forwarding it
        }
    )


@login_required
def export_leading_excel(request):
    if not request.user.has_perm('EHS.view_leadingrecords'):
        messages.error(request, "You do not have permission to download leading records.")
        logger.warning(f"User '{request.user.username}' tried to export leading records without permission.")
        return redirect('indexpage')

    # Get filters
    from_date = request.GET.get('from_date', '')
    to_date = request.GET.get('to_date', '')
    department_filter = request.GET.get('department_filter', '')
    status_filter = request.GET.get('status_filter', '')
    physical_location_filter = request.GET.get('physical_location_filter', '')
    risk_factor_filter = request.GET.get('risk_factor_filter', '')

    filter_info = {
        'from_date': from_date,
        'to_date': to_date,
        'department_filter': department_filter,
        'status_filter': status_filter,
        'physical_location_filter': physical_location_filter,
        'risk_factor_filter': risk_factor_filter,
    }

    # Apply filters (descending order)
    records = LeadingRecords.objects.select_related('physical_location').all().order_by('-id')

    if from_date:
        records = records.filter(observation_date__gte=from_date)
    if to_date:
        records = records.filter(observation_date__lte=to_date)
    if department_filter:
        records = records.filter(department__icontains=department_filter)
    if status_filter:
        records = records.filter(status=status_filter)
    if physical_location_filter:
        records = records.filter(physical_location__name__icontains=physical_location_filter)
    if risk_factor_filter:
        records = records.filter(risk_factor__icontains=risk_factor_filter)

    data = records.values(
        'observation_date',
        'department',
        'physical_location__name',
        'leading_abnormality',
        'initiated_by',
        'severity',
        'likelihood',
        'risk_factor',
        'observation_description',
        'corrective_action',
        'psl_member_name',
        'responsible_person',
        'root_cause',
        'preventive_action',
        'target_date',
        'status',
        'remark'
    )

    df = pd.DataFrame(data)
    df.insert(0, 'Serial No.', range(1, len(df) + 1))
    df.rename(columns={
        'observation_date': 'Observation Date',
        'department': 'Function/Department',
        'physical_location__name': 'Physical Location',
        'leading_abnormality': 'Leading Abnormality',
        'initiated_by': 'Initiated By',
        'severity': 'Severity',
        'likelihood': 'Likelihood',
        'risk_factor': 'Risk Factor',
        'observation_description': 'Observation Description',
        'corrective_action': 'Corrective Action',
        'psl_member_name': 'PSL Member Name',
        'responsible_person': 'Responsible Person',
        'root_cause': 'Root Cause',
        'preventive_action': 'Preventive Action',
        'target_date': 'Target Date',
        'status': 'Status',
        'remark': 'Remark'
    }, inplace=True)

    logger.info(
        f"User '{request.user.username}' exported leading records. "
        f"Filters used: {filter_info}. Records exported: {len(df)}"
    )

    response = HttpResponse(
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = 'attachment; filename=leading_records_filtered.xlsx'

    with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Leading Records', index=False)

    return response
    
    

@login_required
def lagging_chart_summary(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    today = date.today()
    first_day_year = today.replace(month=1, day=1)
    from_date_str = request.GET.get('from_date', '')
    to_date_str = request.GET.get('to_date', '')

    department = request.GET.get('department', '').strip()
    physical_location = request.GET.get('physical_location', '').strip()
    hse_lag_indicator = request.GET.get('hse_lag_indicator', '').strip()
    risk_factor = request.GET.get('risk_factor', '').strip()

    # Filters logic
    if 'clear' in request.GET:
        from_date = first_day_year
        to_date = today
        department = ''
        physical_location = ''
        hse_lag_indicator = ''
        risk_factor = ''
    elif from_date_str and to_date_str:
        try:
            from_date = datetime.strptime(from_date_str, '%Y-%m-%d').date()
            to_date = datetime.strptime(to_date_str, '%Y-%m-%d').date()
        except ValueError:
            from_date = first_day_year
            to_date = today
    else:
        from_date = first_day_year
        to_date = today

    qs = Lagging_Indicator.objects.filter(incident_date__range=(from_date, to_date))

    if department:
        qs = qs.filter(department__icontains=department)
    if physical_location:
        qs = qs.filter(physical_location__name__icontains=physical_location)
    if hse_lag_indicator:
        qs = qs.filter(hse_lag_indicator__icontains=hse_lag_indicator)
    if risk_factor:
        qs = qs.filter(risk_factor__icontains=risk_factor)

    # Compliance status count cards
    status_counts = (
        qs.values('complience_status')
        .annotate(count=Count('id'))
        .order_by('complience_status')
    )

    # Bar chart: Department wise
    department_counts = (
        qs.values('department')
        .annotate(count=Count('id'))
        .order_by('department')
    )
    department_labels = [x['department'] or 'Unknown' for x in department_counts]
    department_data = [x['count'] for x in department_counts]

    # Bar chart: Physical location wise
    location_counts = (
        qs.values('physical_location__name')
        .annotate(count=Count('id'))
        .order_by('physical_location__name')
    )
    location_labels = [x['physical_location__name'] or 'Unknown' for x in location_counts]
    location_data = [x['count'] for x in location_counts]

    # Pie chart: risk_factor (Low, Medium, High) - grouping
    risk_factors = qs.values_list('risk_factor', flat=True)
    bucket = {'Low': 0, 'Medium': 0, 'High': 0}
    for rf in risk_factors:
        val = (rf or '').lower()
        if "low" in val:
            bucket['Low'] += 1
        elif "med" in val:
            bucket['Medium'] += 1
        elif "high" in val:
            bucket['High'] += 1
        # Optional: you can add 'Unknown' here if needed

    risk_factor_labels = list(bucket.keys())
    risk_factor_data = list(bucket.values())

    # Distinct for dropdowns
    all_departments = Lagging_Indicator.objects.values_list('department', flat=True).distinct().order_by('department')
    all_locations = all_locations = (Lagging_Indicator.objects.filter(physical_location__isnull=False).values_list('physical_location__name', flat=True).distinct().order_by('physical_location__name'))
    all_hse_lag = Lagging_Indicator.objects.values_list('hse_lag_indicator', flat=True).distinct().order_by('hse_lag_indicator')
    all_risk_factors = Lagging_Indicator.objects.values_list('risk_factor', flat=True).distinct().order_by('risk_factor')

    context = {
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'status_counts': status_counts,
        'department_labels': department_labels,
        'department_data': department_data,
        'location_labels': location_labels,
        'location_data': location_data,
        'risk_factor_labels': risk_factor_labels,
        'risk_factor_data': risk_factor_data,
        'from_date': from_date.strftime('%Y-%m-%d'),
        'to_date': to_date.strftime('%Y-%m-%d'),
        'department_filter': department,
        'physical_location_filter': physical_location,
        'hse_lag_indicator_filter': hse_lag_indicator,
        'risk_factor_filter': risk_factor,
        'all_departments': all_departments,
        'all_locations': all_locations,
        'all_hse_lag': all_hse_lag,
        'all_risk_factors': all_risk_factors,
    }
    return render(request, 'lagging/lagging_chart_summary.html', context)




#=================================================================================================================



@login_required
def leading_chart_summary(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    today = date.today()
    first_day_year = today.replace(month=1, day=1)
    from_date_str = request.GET.get('from_date', '')
    to_date_str = request.GET.get('to_date', '')

    # Additional filters
    status_filter = request.GET.get('status', '').strip()
    department_filter = request.GET.get('department', '').strip()
    physical_location_filter = request.GET.get('physical_location', '').strip()
    risk_factor_filter = request.GET.get('risk_factor', '').strip()

    # If user provided both dates, use them. If 'Clear', ignore GET params.
    if 'clear' in request.GET:
        from_date = first_day_year
        to_date = today
        status_filter = ''
        department_filter = ''
        physical_location_filter = ''
        risk_factor_filter = ''
    elif from_date_str and to_date_str:
        try:
            from_date = datetime.strptime(from_date_str, '%Y-%m-%d').date()
            to_date = datetime.strptime(to_date_str, '%Y-%m-%d').date()
        except ValueError:
            from_date = first_day_year
            to_date = today
    else:
        from_date = first_day_year
        to_date = today

    qs = LeadingRecords.objects.filter(observation_date__range=(from_date, to_date))

    if status_filter:
        qs = qs.filter(status__icontains=status_filter)
    if department_filter:
        qs = qs.filter(department__icontains=department_filter)
    if physical_location_filter:
        qs = qs.filter(physical_location__name__icontains=physical_location_filter)
    if risk_factor_filter:
        qs = qs.filter(risk_factor__icontains=risk_factor_filter)

    # Status count cards
    status_counts = (
        qs.values('status')
        .annotate(count=Count('id'))
        .order_by('status')
    )

    # Physical location bar chart data
    location_counts = (
        qs.values('physical_location__name')
        .annotate(count=Count('id'))
        .order_by('physical_location__name')
    )
    location_labels = [x['physical_location__name'] or 'Unknown' for x in location_counts]
    location_data = [x['count'] for x in location_counts]

    # Department bar chart data
    department_counts = (
        qs.values('department')
        .annotate(count=Count('id'))
        .order_by('department')
    )
    department_labels = [x['department'] or 'Unknown' for x in department_counts]
    department_data = [x['count'] for x in department_counts]

    # Bar chart for PSL Member (status = "Open/Due" only)
    psl_open_counts = (
        qs.filter(status="Open")
        .values('psl_member_name')
        .annotate(count=Count('id'))
        .order_by('psl_member_name')
    )
    psl_member_labels = [x['psl_member_name'] or 'Unknown' for x in psl_open_counts]
    psl_member_data = [x['count'] for x in psl_open_counts]

    logger.info(
        f"User '{request.user.username}' accessed Leading Chart Summary. "
        f"Dates: {from_date} to {to_date} | Filters: status='{status_filter}', "
        f"department='{department_filter}', physical_location='{physical_location_filter}', "
        f"risk_factor='{risk_factor_filter}' | Statuses: {list(status_counts)} | "
        f"Locations: {location_labels} | Departments: {department_labels}"
    )

    context = {
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'status_counts': status_counts,
        'location_labels': location_labels,
        'location_data': location_data,
        'department_labels': department_labels,
        'department_data': department_data,
        'from_date': from_date.strftime('%Y-%m-%d'),
        'to_date': to_date.strftime('%Y-%m-%d'),
        'status_filter': status_filter,
        'department_filter': department_filter,
        'physical_location_filter': physical_location_filter,
        'risk_factor_filter': risk_factor_filter,
        'psl_member_labels': psl_member_labels,
        'psl_member_data': psl_member_data,
    }
    return render(request, 'leading/leading_chart_summary.html', context)
    
    

# --------------------------------------------------------------------------------------------------------------------------------


#Below All code related Lagging Module

@login_required
def add_lagging_indicator(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('EHS.add_lagging_indicator'):
        messages.error(request, "You do not have permission to add Lagging records.")
        logger.warning(f"User '{request.user.username}' tried to add a Lagging record without permission.")
        return redirect('indexpage')

    if request.method == 'POST':
        # print("Received POST request with data:", request.POST)

        form = LaggingIndicatorForm(request.POST)
        formset = LaggingCapaEntryFormSet(request.POST)

        # print("Form valid:", form.is_valid())
        # print("Formset valid:", formset.is_valid())

        if form.is_valid() and formset.is_valid():
            # 1. Save main Lagging Indicator
            lagging_obj = form.save()  # psm_failure now stored as a comma-separated string

            # 2. Save CAPA Entries
            capa_entries = formset.save(commit=False)
            for entry in capa_entries:
                entry.lagging_indicator = lagging_obj
                entry.save()
            
            # Handle any deleted forms
            for deleted_form in formset.deleted_forms:
                if deleted_form.instance.pk:
                    deleted_form.instance.delete()

            # 3. Update compliance status based on newly saved CAPAs
            lagging_obj.update_compliance_status()

            messages.success(request, "Lagging Indicator record saved successfully!")
            return redirect('view_lagging')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = LaggingIndicatorForm()
        formset = LaggingCapaEntryFormSet()

    return render(request,'lagging/add_lagging_form.html',
                  {'form': form,'formset': formset,'user_groups': user_groups,'is_superuser': is_superuser})




@login_required
def view_lagging_records(request):
    logger.debug("Lagging view accessed by user %s (%s)", request.user, request.user.id)

    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    
    if not request.user.has_perm('EHS.view_lagging_indicator'):
        messages.error(request, "You do not have permission to view Lagging records.")
        logger.warning(f"User '{request.user.username}' tried to view a Lagging record without permission.")
        return redirect('indexpage')

    can_edit_lagging_records = request.user.has_perm('EHS.change_lagging_indicator')
    can_delete_lagging_records = request.user.has_perm('EHS.delete_lagging_indicator')
    can_add_lagging_records = request.user.has_perm('EHS.add_lagging_indicator')
    can_view_lagging_records = request.user.has_perm('EHS.view_lagging_indicator')

    from_incident_date = request.GET.get('from_incident_date', '')
    to_incident_date = request.GET.get('to_incident_date', '')
    hse_lag_filter = request.GET.get('hse_lag_filter', 'All')
    risk_factor_filter = request.GET.get('risk_factor_filter', 'All')
    complience_status_filter = request.GET.get('complience_status_filter', 'All')

    logger.info(
        "Filters: from_incident_date=%s, to_incident_date=%s, hse_lag_filter=%s, risk_factor_filter=%s, complience_status_filter=%s",
        from_incident_date, to_incident_date, hse_lag_filter, risk_factor_filter, complience_status_filter
    )

    try:
        hse_lag_choices = ['All'] + list(
            Lagging_Indicator.objects.values_list('hse_lag_indicator', flat=True)
            .exclude(hse_lag_indicator__isnull=True)
            .exclude(hse_lag_indicator__exact='')
            .distinct()
        )
        logger.debug("Loaded hse_lag_choices: %s", hse_lag_choices)
    except Exception as e:
        logger.error("Error loading hse_lag_choices: %s", e, exc_info=True)
        hse_lag_choices = ['All']

    risk_factor_choices = ['All', 'Low', 'Medium', 'High']
    complience_status_choices = ['All', 'Closed', 'Open/Due']

    records = Lagging_Indicator.objects.all().order_by('-id', '-incident_date')
    logger.debug("Initial records count: %d", records.count())

    if from_incident_date:
        records = records.filter(incident_date__gte=parse_date(from_incident_date))
        logger.debug("Filtered by from_incident_date=%s, count=%d", from_incident_date, records.count())
    if to_incident_date:
        records = records.filter(incident_date__lte=parse_date(to_incident_date))
        logger.debug("Filtered by to_incident_date=%s, count=%d", to_incident_date, records.count())
    if hse_lag_filter and hse_lag_filter != 'All':
        records = records.filter(hse_lag_indicator=hse_lag_filter)
        logger.debug("Filtered by hse_lag_filter=%s, count=%d", hse_lag_filter, records.count())
    if risk_factor_filter and risk_factor_filter != 'All':
        records = records.filter(risk_factor__icontains=risk_factor_filter)
        logger.debug("Filtered by risk_factor_filter=%s, count=%d", risk_factor_filter, records.count())
    if complience_status_filter and complience_status_filter != "All":
        records = records.filter(complience_status=complience_status_filter)
        logger.debug("Filtered by complience_status_filter=%s, count=%d", complience_status_filter, records.count())

    # Get dropdown values (distinct, sorted)
    try:
        hse_lag_choices = ["All"] + list(
            Lagging_Indicator.objects.values_list('hse_lag_indicator', flat=True).distinct().order_by('hse_lag_indicator')
        )
        logger.debug("Dropdown hse_lag_choices (distinct): %s", hse_lag_choices)
    except Exception as e:
        logger.error("Error loading dropdown hse_lag_choices: %s", e, exc_info=True)
        hse_lag_choices = ["All"]
    risk_factor_choices = ["All", "Low", "Medium", "High"]
    complience_status_choices = ["All"] + list(
        Lagging_Indicator.objects.values_list('complience_status', flat=True).distinct().order_by('complience_status')
    )

    paginator = Paginator(records, 10)
    page = request.GET.get('page')
    try:
        records_paginated = paginator.page(page)
        logger.debug("Paginator on page %s, object count=%d", page, len(records_paginated))
    except PageNotAnInteger:
        records_paginated = paginator.page(1)
        logger.warning("PageNotAnInteger: set to page 1")
    except EmptyPage:
        records_paginated = paginator.page(paginator.num_pages)
        logger.warning("EmptyPage: set to last page %d", paginator.num_pages)

    logger.info(
        "Returning page %d of %d with %d records",
        records_paginated.number, paginator.num_pages, len(records_paginated)
    )

    context = {
        'lagging_records': records_paginated,
        'from_incident_date': from_incident_date,
        'to_incident_date': to_incident_date,
        'hse_lag_choices': hse_lag_choices,
        'hse_lag_filter': hse_lag_filter,
        'risk_factor_choices': risk_factor_choices,
        'risk_factor_filter': risk_factor_filter,
        'complience_status_choices': complience_status_choices,
        'complience_status_filter': complience_status_filter,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'can_edit_lagging_records': can_edit_lagging_records,
        'can_delete_lagging_records': can_delete_lagging_records,
        'can_add_lagging_records': can_add_lagging_records,
        'can_view_lagging_records': can_view_lagging_records,
    }
    logger.debug("Context prepared: keys=%s", list(context.keys()))
    return render(request, 'lagging/view_lagging.html', context)



@login_required
def lagging_record_detail(request, record_id):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('EHS.view_lagging_indicator'):
        messages.error(request, "You do not have permission to view Lagging records.")
        logger.warning(f"User '{request.user.username}' tried to View a Lagging record without permission.")
        return redirect('indexpage')
    
    # Fetch the main Lagging Indicator record
    record = get_object_or_404(Lagging_Indicator, id=record_id)
    
    # Fetch all associated CAPA entries for this Lagging Indicator
    capa_entries = LaggingCapaEntry.objects.filter(lagging_indicator=record)

    context = {
        'record': record,
        'capa_entries': capa_entries,
        'user_groups': user_groups,
        'is_superuser': is_superuser
    }
    return render(request, 'lagging/lagging_record_detail.html', context)




@login_required
def edit_lagging_indicator(request, record_id):
    if not request.user.has_perm('EHS.change_lagging_indicator'):
        messages.error(request, "You do not have permission to edit Lagging records.")
        logger.warning(f"User '{request.user.username}' tried to Update a Lagging record without permission.")
        return redirect('indexpage')
    # Get the instance of Lagging Indicator to be edited
    record = get_object_or_404(Lagging_Indicator, id=record_id)
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if request.method == 'POST':
        # print("Received POST request with data:", request.POST)

        form = LaggingIndicatorForm(request.POST, instance=record)
        formset = LaggingCapaEntryFormSet(request.POST, instance=record)

        # Set empty_permitted for new forms (those without a primary key)
        for form_in_formset in formset.forms:
            if not form_in_formset.instance.pk:
                form_in_formset.empty_permitted = True  # Allow empty forms

        # print("Main form valid:", form.is_valid())
        # print("Formset valid:", formset.is_valid())
        # print("Formset errors:", formset.errors)  # Additional debugging print

        if form.is_valid() and formset.is_valid():
            try:
                # Save the main record (Lagging Indicator)
                lagging_obj = form.save()  # psm_failure is stored as a comma-separated string

                # Save CAPA entries
                capa_entries = formset.save(commit=False)
                for entry in capa_entries:
                    entry.lagging_indicator = lagging_obj
                    entry.save()

                # Handle any deleted CAPA forms
                for deleted_form in formset.deleted_forms:
                    if deleted_form.instance.pk:
                        deleted_form.instance.delete()

                # Debug: log compliance status update
                # print("Updating compliance status for record ID:", lagging_obj.id)
                # lagging_obj.update_compliance_status()
                # print("Compliance status updated to:", lagging_obj.complience_status)

                messages.success(request, "Lagging Indicator record updated successfully!")
                return redirect('view_lagging')
            except Exception as e:
                # print("Error while saving record:", e)
                import traceback
                traceback.print_exc()
                messages.error(request, f"An error occurred while saving: {e}")
        else:
            # Debug: print out the form and formset errors
            # print("Form errors:", form.errors)
            # print("Formset errors:", formset.errors)
            messages.error(request, "Please correct the errors below.")
    else:
        form = LaggingIndicatorForm(instance=record)
        formset = LaggingCapaEntryFormSet(instance=record)
        # Set empty_permitted=True for any extra forms on GET as well
        for f in formset.forms:
            if f.instance.pk is None:
                f.empty_permitted = True  # Allow empty forms to be submitted on GET

    return render(request, 'lagging/edit_lagging_form.html', 
                  {'form': form, 'formset': formset, 'user_groups': user_groups, 'is_superuser': is_superuser})




@login_required
def delete_lagging_record(request, record_id):
    record = get_object_or_404(Lagging_Indicator, id=record_id)
    
    # Check permission to delete
    if not request.user.has_perm('EHS.delete_lagging_indicator'):
        messages.error(request, "You do not have permission to delete this record.")
        return redirect('view_lagging')
        logger.warning(f"User '{request.user.username}' tried to delete a Lagging record without permission.")
        return redirect('indexpage')

    if request.method == 'POST':
        record.delete()
        messages.success(request, "Lagging Indicator record deleted successfully!")
        return redirect('view_lagging')



@login_required
def download_lagging_excel(request):
    records = Lagging_Indicator.objects.all().order_by('-id','-incident_date')
    from_incident_date = request.GET.get('from_incident_date', '')
    to_incident_date = request.GET.get('to_incident_date', '')
    hse_lag_filter = request.GET.get('hse_lag_filter', 'All')
    risk_factor_filter = request.GET.get('risk_factor_filter', 'All')
    complience_status_filter = request.GET.get('complience_status_filter', 'All')

    if from_incident_date:
        records = records.filter(incident_date__gte=parse_date(from_incident_date))
    if to_incident_date:
        records = records.filter(incident_date__lte=parse_date(to_incident_date))
    if hse_lag_filter and hse_lag_filter != 'All':
        records = records.filter(hse_lag_indicator=hse_lag_filter)
    if risk_factor_filter and risk_factor_filter != 'All':
        records = records.filter(risk_factor__icontains=risk_factor_filter)
    if complience_status_filter and complience_status_filter != "All":
        records = records.filter(complience_status=complience_status_filter)

    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {'in_memory': True})
    ws = wb.add_worksheet("Lagging Indicators")

    header_fmt = wb.add_format({'bold': True, 'bg_color': '#F0F0F0', 'align': 'center'})
    date_fmt = wb.add_format({'num_format': 'dd/mm/yyyy', 'align': 'center'})
    wrap_fmt = wb.add_format({'text_wrap': True, 'valign': 'top'})
    merge_fmt = wb.add_format({'valign': 'vcenter'})

    headers = [
        "Sr.No", "Record Date", "Incident Date", "Incident Time", "Employee Type",
        "Contractor Name", "Department", "Physical Location", "HSE Lag Indicator",
        "Type of Injury", "Injured Body Part", "Name of Injured Person", "Severity",
        "Likelihood", "Risk Factor", "Incident", "Immediate Action", "Investigation Method",
        "Fact About Men", "Fact About Machine", "Fact About Mother Nature",
        "Fact About Measurement", "Fact About Method", "Fact About Material",
        "Fact About History", "Why One", "Why Two", "Why Three", "Why Four", "Why Five",
        "Direct Root Cause", "Indirect Root Cause", "PSM Failure", "Date Resume Duty",
        "Mandays Lost", "Compliance Status", "Compliance Status Date",
        "CAPA No", "CAPA", "CAPA Department", "FPR", "Target Date", "CAPA Compliance Status"
    ]
    for c, h in enumerate(headers):
        ws.write(0, c, h, header_fmt)

    ws.freeze_panes(1, 0)
    ws.set_column(0, 36, 16)
    ws.set_column(37, 42, 18)

    row = 1
    srno = 1
    for rec in records:
        capalist = list(rec.lagging_capa_entry.all())
        n_capa = max(1, len(capalist))
        capalist = capalist or [None]
        start_row = row

        # Store master values (in order)
        master_row_values = [
            srno,
            rec.record_date.strftime("%d/%m/%Y") if rec.record_date else "",
            rec.incident_date.strftime("%d/%m/%Y") if rec.incident_date else "",
            rec.incident_time.strftime("%H:%M") if rec.incident_time else "",
            rec.employee_type or "",
            rec.Contractor_name or "",
            rec.department or "",
            str(rec.physical_location) if rec.physical_location else "",
            rec.hse_lag_indicator or "",
            rec.type_of_injury or "",
            rec.injured_body_part or "",
            rec.name_of_injured_person or "",
            rec.severity if rec.severity is not None else "",
            rec.likelihood if rec.likelihood is not None else "",
            rec.risk_factor or "",
            rec.incident or "",
            rec.immediate_action or "",
            rec.investigation_method or "",
            rec.fact_about_men or "",
            rec.fact_about_machine or "",
            rec.fact_about_mother_nature or "",
            rec.fact_about_measurement or "",
            rec.fact_about_method or "",
            rec.fact_about_material or "",
            rec.fact_about_history or "",
            rec.why_one or "",
            rec.why_two or "",
            rec.why_three or "",
            rec.why_four or "",
            rec.why_five or "",
            rec.direct_root_cause or "",
            rec.indirect_root_cause or "",
            rec.psm_failure or "",
            rec.date_resume_duty.strftime("%d/%m/%Y") if rec.date_resume_duty else "",
            rec.mandays_lost if rec.mandays_lost is not None else "",
            rec.complience_status or "",
            rec.complience_status_date.strftime("%d/%m/%Y") if rec.complience_status_date else ""
        ]

        for idx, capa in enumerate(capalist, start=1):
            if idx == 1:
                for c, v in enumerate(master_row_values):
                    ws.write(row, c, v, merge_fmt if c != 15 and c != 16 and c != 17 else wrap_fmt)
            # CAPA columns
            ws.write_number(row, 37, idx)
            if capa:
                ws.write(row, 38, capa.capa or "", wrap_fmt)
                ws.write(row, 39, capa.department or "", wrap_fmt)
                ws.write(row, 40, capa.frp or "", wrap_fmt)
                ws.write(row, 41, capa.target_date.strftime("%d/%m/%Y") if capa.target_date else "", merge_fmt)
                ws.write(row, 42, capa.compliance_status or "", wrap_fmt)
            else:
                ws.write(row, 38, "", wrap_fmt)
                ws.write(row, 39, "", wrap_fmt)
                ws.write(row, 40, "", wrap_fmt)
                ws.write(row, 41, "", merge_fmt)
                ws.write(row, 42, "", wrap_fmt)
            row += 1

        # Merge master columns vertically across all CAPA rows (if multiple)
        if n_capa > 1:
            for col in range(0, 37):
                ws.merge_range(start_row, col, row - 1, col, master_row_values[col], merge_fmt if col != 15 and col != 16 and col != 17 else wrap_fmt)

        srno += 1

    wb.close()
    output.seek(0)
    response = HttpResponse(
        output.read(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response['Content-Disposition'] = 'attachment; filename="lagging_indicator_records.xlsx"'
    return response





#=======================================================================================        
        


# -------------------- ADD PSSR RECORD --------------------
@login_required
def add_pssr_record(request):
    if not request.user.has_perm('EHS.add_pssrjobrecord'):
        messages.error(request, "You do not have permission to Add this record.")
        return redirect('indexpage')

    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if request.method == 'POST':
        job_form = PSSRJobRecordForm(request.POST)
        observation_formset = PSSRObservationFormSet(request.POST)

        if job_form.is_valid() and observation_formset.is_valid():
            job_record = job_form.save()
            for obs in observation_formset.save(commit=False):
                obs.job_record = job_record
                # Only keep remark if status is Pending or Omitted
                if obs.compliance_status not in ['Pending', 'Omitted']:
                    obs.remark = ''
                obs.save()
            for dform in observation_formset.deleted_forms:
                if dform.instance.pk:
                    dform.instance.delete()

            messages.success(request, "PSSR Job Record saved successfully.")
            return redirect('pssr_record_list')
        else:
            if not job_form.is_valid():
                logger.error("Job form errors: %s", job_form.errors.as_json())
                messages.error(request, f"Job form errors: {job_form.errors.as_text()}")
            if not observation_formset.is_valid():
                fs_errors = [f"{i}: {f.errors.as_text()}" for i, f in enumerate(observation_formset.forms) if f.errors]
                non_form  = observation_formset.non_form_errors()
                logger.error("Observation formset errors: %s | non-form: %s", fs_errors, non_form)
                messages.error(request, f"Observation errors: {fs_errors or non_form}")
    else:
        job_form = PSSRJobRecordForm()
        observation_formset = PSSRObservationFormSet()

    return render(request, 'pssr/add_pssr_record.html', {
        'job_form': job_form,
        'observation_formset': observation_formset,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
    })


# -------------------- PSSR RECORD LIST --------------------
@login_required
def pssr_record_list(request):
    if not request.user.has_perm("EHS.view_pssrjobrecord"):
        messages.error(request, "You do not have permission to view PSSR Job Records.")
        return redirect("indexpage")

    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    date_filter = request.GET.get('date_filter', '').strip()
    from_date   = request.GET.get('from_date', '').strip()
    to_date     = request.GET.get('to_date', '').strip()
    moc_filter  = request.GET.get('moc_filter', '').strip()
    job_filter  = request.GET.get('job_description', '').strip()

    qs = PSSRJobRecord.objects.prefetch_related('observations').order_by('-date','-id')

    if date_filter:
        qs = qs.filter(date=date_filter)
    if from_date and to_date:
        try:
            from_date_parsed = datetime.strptime(from_date,'%Y-%m-%d').date()
            to_date_parsed   = datetime.strptime(to_date,'%Y-%m-%d').date()
            qs = qs.filter(date__range=(from_date_parsed, to_date_parsed))
        except ValueError:
            pass
    if moc_filter:
        qs = qs.filter(moc_no__icontains=moc_filter)
    if job_filter:
        qs = qs.filter(job_description__icontains=job_filter)

    paginator = Paginator(qs, 10)
    page_number = request.GET.get('page', 1)
    try:
        page_obj = paginator.page(page_number)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    return render(request, 'pssr/pssr_record_list.html', {
        'jobs': page_obj,
        'date_filter': date_filter,
        'from_date': from_date,
        'to_date': to_date,
        'moc_filter': moc_filter,
        'job_filter': job_filter,
        'is_paginated': paginator.num_pages > 1,
        'paginator': paginator,
        'page_obj': page_obj,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
    })


# -------------------- EDIT PSSR RECORD --------------------
from django.forms import inlineformset_factory

EditObservationFormSet = inlineformset_factory(
    PSSRJobRecord,
    PSSRObservation,
    form=PSSRObservationInlineForm,
    extra=0,
    can_delete=True
)

@login_required
def edit_pssr_record(request, record_id):
    if not request.user.has_perm('EHS.change_pssrjobrecord'):
        messages.error(request, "You do not have permission to update this record.")
        return redirect('indexpage')

    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    job_record = get_object_or_404(PSSRJobRecord, pk=record_id)

    if request.method == "POST":
        job_form = PSSRJobRecordForm(request.POST, instance=job_record)
        observation_formset = EditObservationFormSet(request.POST, instance=job_record)

        if job_form.is_valid() and observation_formset.is_valid():
            job_form.save()
            for obs in observation_formset.save(commit=False):
                if obs.compliance_status not in ['Pending','Omitted']:
                    obs.remark = ''
                obs.save()
            observation_formset.save_m2m()
            messages.success(request, "PSSR Job Record updated successfully.")
            return redirect("pssr_record_list")
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        job_form = PSSRJobRecordForm(instance=job_record)
        observation_formset = EditObservationFormSet(instance=job_record)

    return render(request, "pssr/edit_pssr_record.html", {
        "job_form": job_form,
        "observation_formset": observation_formset,
        "job_record": job_record,
        "user_groups": user_groups,
        "is_superuser": is_superuser,
    })


# -------------------- DETAIL VIEW --------------------
@login_required
def pssr_record_detail(request, pk):
    if not request.user.has_perm('EHS.view_pssrjobrecord'):
        messages.error(request, "You do not have permission to View this record.")
        return redirect('indexpage')

    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    job = get_object_or_404(PSSRJobRecord.objects.prefetch_related('observations'), pk=pk)
    observations = job.observations.order_by('id')

    return render(request, "pssr/pssr_record_detail.html", {
        "job": job,
        "observations": observations,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
    })


# -------------------- DELETE RECORD --------------------
@login_required
def delete_pssr_record(request, record_id):
    job = get_object_or_404(PSSRJobRecord, pk=record_id)
    if not request.user.has_perm('EHS.delete_pssrjobrecord'):
        messages.error(request, "You do not have permission to delete PSSR records.")
        return redirect('indexpage')

    if request.method == 'POST':
        try:
            job.delete()
            messages.success(request, "PSSR Record deleted successfully.")
        except Exception as e:
            messages.error(request,"Error deleting record: "+str(e))
        return redirect('pssr_record_list')

    return redirect('pssr_record_list')


# -------------------- DELETE OBSERVATION --------------------
@login_required
def delete_pssr_observation(request, pk):
    if not request.user.has_perm('EHS.delete_pssrobservation'):
        messages.error(request, "You do not have permission to delete PSSR observation records.")
        return redirect('indexpage')

    obs = get_object_or_404(PSSRObservation, pk=pk)
    parent_id = obs.job_record_id

    if request.method == "POST":
        obs.delete()

    return redirect("pssr_record_detail", pk=parent_id)


# -------------------- DOWNLOAD EXCEL --------------------
@login_required
@login_required
def download_pssr_excel(request):
    import io
    from datetime import datetime, time
    import xlsxwriter
    from django.http import HttpResponse

    qs = (PSSRJobRecord.objects
          .prefetch_related('observations')
          .order_by('-date','-id'))

    # apply filters
    date_filter = request.GET.get('date_filter','').strip()
    from_date   = request.GET.get('from_date','').strip()
    to_date     = request.GET.get('to_date','').strip()
    moc_filter  = request.GET.get('moc_filter','').strip()
    job_filter  = request.GET.get('job_description','').strip()

    if date_filter:
        qs = qs.filter(date=date_filter)

    if from_date and to_date:
        try:
            dt1 = datetime.strptime(from_date,'%Y-%m-%d').date()
            dt2 = datetime.strptime(to_date,  '%Y-%m-%d').date()
            qs = qs.filter(date__range=(dt1, dt2))
        except ValueError:
            pass

    if moc_filter:
        qs = qs.filter(moc_no__icontains=moc_filter)

    if job_filter:
        qs = qs.filter(job_description__icontains=job_filter)

    # workbook
    output = io.BytesIO()
    wb     = xlsxwriter.Workbook(output, {'in_memory': True})
    ws     = wb.add_worksheet("PSSR Records")

    # ===== FORMATS =====
    title_fmt  = wb.add_format({
        'bold': True,
        'font_size': 16,
        'align': 'center',
        'valign': 'vcenter'
    })

    subtitle_fmt = wb.add_format({
        'align': 'center',
        'valign': 'vcenter',
        'italic': True
    })

    header_fmt = wb.add_format({
        'bold':True,'bg_color':'#F0F0F0',
        'align':'center','valign':'vcenter','border':1
    })
    date_fmt   = wb.add_format({'num_format':'dd/mm/yyyy','align':'center','border':1})
    merge_fmt  = wb.add_format({'valign':'vcenter','align':'left','border':1})
    center_fmt = wb.add_format({'align':'center','valign':'vcenter','border':1})
    text_fmt   = wb.add_format({'border':1,'text_wrap':True})

    # ===== ? TITLE AT TOP (NEW) =====
    # Merge across all 13 columns (0 to 12)
    ws.merge_range(0, 0, 0, 12, "PSSR RECORDS REPORT", title_fmt)

    # Optional: show filters below title
    filter_text = f"From: {from_date or 'All'}   To: {to_date or 'All'}"
    ws.merge_range(1, 0, 1, 12, filter_text, subtitle_fmt)

    # Empty spacing row
    header_row = 3  # headers will start from row 3

    # ===== HEADERS =====
    headers = [
        "Sr.No", "Performar Date", "MOC No", "Job Description",
        "No.", "Observar", "Observation", "FPR",
        "Target Date", "Compliance Date", "RPN Category", "Compliance Status",
        "Remark"
    ]

    for c, h in enumerate(headers):
        ws.write(header_row, c, h, header_fmt)

    # column widths
    ws.set_column(0,  0, 6)
    ws.set_column(1,  1, 12)
    ws.set_column(2,  2, 20)
    ws.set_column(3,  3, 40)
    ws.set_column(4,  4, 6)
    ws.set_column(5,  5, 15)
    ws.set_column(6,  6, 50)
    ws.set_column(7,  7, 12)
    ws.set_column(8,  8, 14)
    ws.set_column(9,  9, 16)
    ws.set_column(10,10,14)
    ws.set_column(11,11,20)
    ws.set_column(12,12,40)

    # ===== DATA START AFTER HEADER =====
    row  = header_row + 1
    srno = 1

    for job in qs:
        obs_qs = job.observations.all()
        count  = obs_qs.count() or 1
        start  = row

        if obs_qs:
            for idx, o in enumerate(obs_qs, start=1):
                ws.write_number(row, 4, idx, center_fmt)
                ws.write_string(row, 5, o.observar or "", center_fmt)
                ws.write_string(row, 6, o.observation or "", text_fmt)
                ws.write_string(row, 7, o.fpr or "", center_fmt)

                if o.target_date:
                    dt = datetime.combine(o.target_date, time())
                    ws.write_datetime(row, 8, dt, date_fmt)
                else:
                    ws.write_blank(row, 8, None, center_fmt)

                if o.compliance_date:
                    dtc = datetime.combine(o.compliance_date, time())
                    ws.write_datetime(row, 9, dtc, date_fmt)
                else:
                    ws.write_blank(row, 9, None, center_fmt)

                ws.write_string(row, 10, o.rpn_category or "", center_fmt)
                ws.write_string(row, 11, o.compliance_status or "", center_fmt)
                ws.write_string(row, 12, o.remark or "", text_fmt)

                row += 1
        else:
            ws.write_number(row, 4, 1, center_fmt)
            for col in range(5, 13):
                ws.write_blank(row, col, None, center_fmt)
            row += 1

        # merge job-level cells
        if count > 1:
            ws.merge_range(start, 0, row-1, 0, srno, center_fmt)
            ws.merge_range(start, 1, row-1, 1, job.date.strftime("%d/%m/%Y"), center_fmt)
            ws.merge_range(start, 2, row-1, 2, job.moc_no or "", merge_fmt)
            ws.merge_range(start, 3, row-1, 3, job.job_description or "", merge_fmt)
        else:
            dt0 = datetime.combine(job.date, time())
            ws.write_number(start, 0, srno, center_fmt)
            ws.write_datetime(start, 1, dt0, date_fmt)
            ws.write_string(start, 2, job.moc_no or "", merge_fmt)
            ws.write_string(start, 3, job.job_description or "", merge_fmt)

        srno += 1

    wb.close()
    output.seek(0)

    response = HttpResponse(
        output.read(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    response['Content-Disposition'] = 'attachment; filename="PSSR_Records_Report.xlsx"'
    return response
    
    


@login_required
def pssr_chart_summary(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    # --- parse or default the date‐range ---
    today = date.today()
    first_of_year = today.replace(month=1, day=1)

    # Default to current year if no GET params
    from_str = request.GET.get('from_date') or first_of_year.isoformat()
    to_str   = request.GET.get('to_date')   or today.isoformat()

    try:
        from_date = datetime.strptime(from_str, '%Y-%m-%d').date()
        to_date   = datetime.strptime(to_str,   '%Y-%m-%d').date()
    except ValueError:
        # fallback
        from_date, to_date = first_of_year, today

    # base queryset: only observations in that job_record.date range
    base_qs = PSSRObservation.objects.filter(
        job_record__date__range=(from_date, to_date)
    )

    # RPN aggregate
    rpn_data = (
        base_qs
        .values('rpn_category')
        .annotate(count=Count('id'))
        .order_by('rpn_category')
    )

    # Compliance aggregate
    comp_data = (
        base_qs
        .values('compliance_status')
        .annotate(count=Count('id'))
        .order_by('compliance_status')
    )

    # totals
    total_obs       = base_qs.count()
    total_completed = base_qs.filter(compliance_status='Completed').count()
    total_pending   = base_qs.filter(compliance_status='Pending').count()
    total_omitted   = base_qs.filter(compliance_status='Omitted').count()

    context = {
        'from_date':       from_date.isoformat(),
        'to_date':         to_date.isoformat(),
        'rpn_labels':      [x['rpn_category'] or '—' for x in rpn_data],
        'rpn_counts':      [x['count'] for x in rpn_data],
        'comp_labels':     [x['compliance_status'] or '—' for x in comp_data],
        'comp_counts':     [x['count'] for x in comp_data],
        'total_obs':       total_obs,
        'total_completed': total_completed,
        'total_pending':   total_pending,
        'total_omitted':   total_omitted,
        'user_groups':         user_groups,
        'is_superuser':        is_superuser,
    }
    return render(request, 'pssr/pssr_chart_summary.html', context)





# -----------------------EHS Dashboard  ---------------------------------------------------------------------

from datetime import date as _date
from django.db.models import (
    Count, Q, F, IntegerField, CharField, Value, Case, When,
    ExpressionWrapper, Min
)
from django.db.models.functions import Coalesce
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from django.shortcuts import render

from .models import Lagging_Indicator, LaggingCapaEntry, PSSRJobRecord, PSSRObservation
from .utils import _parse_date


# ================= DASHBOARD PAGE (KEEPING PSSR SUPPORT) =================
@login_required
def hse_dashboard_page(request):
    """
    Unified Dashboard Page
    Default = Lagging
    ?type=pssr will load PSSR without changing its logic
    """
    dashboard_type = request.GET.get("type", "lagging").lower()
    if dashboard_type not in ["lagging", "pssr"]:
        dashboard_type = "lagging"

    today = _date.today()
    first_of_year = today.replace(month=1, day=1)

    ctx = {
        "default_from": first_of_year.isoformat(),
        "default_to": today.isoformat(),
        "dashboard_type": dashboard_type,
    }
    return render(request, "ehs_dashboard.html", ctx)


# ================= RISK CALCULATION =================
def _annotate_risk(qs):
    return (
        qs.annotate(
            risk_score=ExpressionWrapper(
                Coalesce(F("severity"), 0) * Coalesce(F("likelihood"), 0),
                output_field=IntegerField(),
            )
        )
        .annotate(
            risk_bucket=Case(
                When(Q(severity__isnull=True) | Q(likelihood__isnull=True), then=Value(None)),
                When(risk_score__lte=2, then=Value("Low")),
                When(risk_score__lte=6, then=Value("Medium")),
                default=Value("High"),
                output_field=CharField(),
            )
        )
    )


# ================= MAIN DASHBOARD DATA API =================
@login_required
def hse_dashboard_data(request):
    dfrom = _parse_date(request.GET.get("from"))
    dto = _parse_date(request.GET.get("to"))

    # fallback ? current year
    if not dfrom or not dto:
        today = _date.today()
        first_of_year = today.replace(month=1, day=1)
        dfrom = dfrom or first_of_year
        dto = dto or today

    # Filters (for clickable KPIs & tables)
    q = (request.GET.get("q") or "").strip()
    f_indicator = (request.GET.get("indicator") or "").strip()
    f_department = (request.GET.get("department") or "").strip()
    f_risk = (request.GET.get("risk_bucket") or "").strip()
    f_frp = (request.GET.get("frp") or "").strip()

    # ================= BASE INCIDENT QUERYSET =================
    qs = Lagging_Indicator.objects.filter(incident_date__range=[dfrom, dto])

    # Search (global)
    if q:
        qs = qs.filter(
            Q(hse_lag_indicator__icontains=q) |
            Q(department__icontains=q) |
            Q(physical_location__name__icontains=q) |
            Q(type_of_injury__icontains=q) |
            Q(injured_body_part__icontains=q) |
            Q(name_of_injured_person__icontains=q) |
            Q(Contractor_name__icontains=q) |
            Q(lagging_capa_entry__frp__icontains=q)
        ).distinct()

    # Risk annotation
    qs = _annotate_risk(qs)

    # Clickable filters
    if f_indicator:
        qs = qs.filter(hse_lag_indicator=f_indicator)

    if f_department:
        qs = qs.filter(department=f_department)

    if f_risk:
        qs = qs.filter(risk_bucket=f_risk)

    # IMPORTANT: FRP filter should filter incidents via CAPA
    if f_frp:
        qs = qs.filter(lagging_capa_entry__frp=f_frp).distinct()

    # ================= CAPA BASE =================
    capa_base = LaggingCapaEntry.objects.filter(lagging_indicator__in=qs)

    if f_frp:
        capa_base = capa_base.filter(frp=f_frp)

    # ================= KPI TOTALS =================
    totals = {
        "incidents": qs.count(),
        "open_capa": capa_base.filter(
            Q(compliance_status__isnull=True) |
            Q(compliance_status="") |
            Q(compliance_status="Open")
        ).count(),
        "closed_capa": capa_base.filter(compliance_status="Closed").count(),
    }

    # ================= TABLE: BY INDICATOR =================
    by_indicator_raw = (
        qs.values("hse_lag_indicator")
        .annotate(count=Count("id"))
        .order_by("-count", "hse_lag_indicator")
    )

    by_indicator = [
        {
            "indicator": r["hse_lag_indicator"] or "(blank)",
            "count": r["count"],
        }
        for r in by_indicator_raw
    ]

    # ================= TABLE: BY DEPARTMENT =================
    by_department_raw = (
        qs.values("department")
        .annotate(count=Count("id"))
        .order_by("-count", "department")
    )

    by_department = [
        {
            "department": r["department"] or "(blank)",
            "count": r["count"],
        }
        for r in by_department_raw
    ]

    # ================= TABLE: BY RISK =================
    by_risk_raw = (
        qs.values("risk_bucket")
        .annotate(count=Count("id"))
        .order_by("-count")
    )

    by_risk = [
        {
            "risk_bucket": r["risk_bucket"] or "(blank)",
            "count": r["count"],
        }
        for r in by_risk_raw
    ]

    # ================= TABLE: BY FRP =================
    frp_qs = LaggingCapaEntry.objects.filter(lagging_indicator__in=qs)

    if f_frp:
        frp_qs = frp_qs.filter(frp=f_frp)

    by_frp_raw = (
        frp_qs.values("frp", "department")
        .annotate(count=Count("id"))
        .order_by("-count", "frp", "department")
    )

    by_frp = [
        {
            "frp": r["frp"] or "(blank)",
            "department": r["department"] or "(blank)",
            "count": r["count"],
        }
        for r in by_frp_raw
    ]

    # ================= FRP OVERDUE (SEPARATE TABLE - AS REQUESTED) =================
    today = _date.today()

    overdue_raw = (
        frp_qs.filter(target_date__lt=today)
        .filter(
            Q(compliance_status__isnull=True) |
            Q(compliance_status="") |
            Q(compliance_status="Open")
        )
        .values("frp", "department")
        .annotate(
            overdue_count=Count("id"),
            min_target_date=Min("target_date"),
        )
        .order_by("-overdue_count", "frp", "department")
    )

    frp_overdue_table = [
        {
            "frp": r["frp"] or "(blank)",
            "department": r["department"] or "(blank)",
            "target_date": r["min_target_date"].isoformat() if r["min_target_date"] else "",
            "overdue_count": r["overdue_count"],
        }
        for r in overdue_raw
    ]

    # ================= MAIN DETAIL TABLE (WITH REMAINING FIELDS) =================
    detail_qs = (
        qs.select_related("physical_location")
        .order_by("-incident_date", "-id")
        .values(
            "incident_date",
            "incident_time",
            "hse_lag_indicator",
            "department",
            "employee_type",
            "Contractor_name",
            "type_of_injury",
            "injured_body_part",
            "name_of_injured_person",
            "risk_bucket",
            "risk_factor",
            "physical_location__name",
        )[:500]
    )

    table = [
        {
            "incident_date": r["incident_date"].isoformat() if r["incident_date"] else "",
            "incident_time": r["incident_time"].strftime("%H:%M") if r["incident_time"] else "",
            "indicator": r["hse_lag_indicator"] or "",
            "department": r["department"] or "",
            "employee_type": r["employee_type"] or "",
            "contractor": r["Contractor_name"] or "",
            "injury": r["type_of_injury"] or "",
            "body_part": r["injured_body_part"] or "",
            "injured_person": r["name_of_injured_person"] or "",
            "location": r["physical_location__name"] or "",
            "risk_bucket": r["risk_bucket"] or "",
            "risk_factor": r["risk_factor"] or "",
        }
        for r in detail_qs
    ]

    # ================= FINAL JSON RESPONSE =================
    return JsonResponse({
        "totals": totals,
        "by_indicator": by_indicator,
        "by_department": by_department,
        "by_risk": by_risk,
        "by_frp": by_frp,
        "frp_overdue_table": frp_overdue_table,  # separate overdue table (NOT KPI)
        "table": table,
    })


# ----------------- PSSR DASHBOARD DATA (CLICKABLE KPI) -----------------
@login_required
def pssr_dashboard_data(request):
    dfrom = _parse_date(request.GET.get("from"))
    dto   = _parse_date(request.GET.get("to"))
    status_filter = request.GET.get("status")
    rpn_filter = request.GET.get("rpn")

    qs = PSSRJobRecord.objects.prefetch_related("observations")

    if dfrom and dto:
        qs = qs.filter(date__range=[dfrom, dto])
    elif dfrom:
        qs = qs.filter(date__gte=dfrom)
    elif dto:
        qs = qs.filter(date__lte=dto)

    observations = PSSRObservation.objects.filter(job_record__in=qs)
    if status_filter:
        observations = observations.filter(compliance_status=status_filter)
    if rpn_filter:
        observations = observations.filter(rpn_category=rpn_filter)

    total_jobs = qs.count()
    total_obs  = observations.count()
    pending    = observations.filter(compliance_status="Pending").count()
    completed  = observations.filter(compliance_status="Completed").count()
    omitted    = observations.filter(compliance_status="Omitted").count()

    rpn_pending_a = PSSRObservation.objects.filter(job_record__in=qs, compliance_status="Pending", rpn_category="A").count()
    rpn_pending_b = PSSRObservation.objects.filter(job_record__in=qs, compliance_status="Pending", rpn_category="B").count()

    by_rpn = list(PSSRObservation.objects.filter(job_record__in=qs).values("rpn_category").annotate(count=Count("id")).order_by("-count"))
    by_status = list(PSSRObservation.objects.filter(job_record__in=qs).values("compliance_status").annotate(count=Count("id")).order_by("-count"))

    # Table
    table = []
    jobs = qs.order_by("-date")
    for job in jobs:
        for obs in job.observations.all():
            if status_filter and obs.compliance_status != status_filter:
                continue
            if rpn_filter and obs.rpn_category != rpn_filter:
                continue
            table.append({
                "date": job.date.isoformat() if job.date else "",
                "moc_no": job.moc_no or "",
                "job_description": job.job_description or "",
                "observator": obs.observar or "",
                "observation": obs.observation or "",
                "fpr": obs.fpr or "",
                "target_date": obs.target_date.isoformat() if obs.target_date else "",
                "rpn": obs.rpn_category or "",
                "status": obs.compliance_status or "",
                "remark": obs.remark or "",
            })

    return JsonResponse({
        "totals": {
            "jobs": total_jobs,
            "observations": total_obs,
            "pending": pending,
            "completed": completed,
            "omitted": omitted,
            "rpn_pending_a": rpn_pending_a,
            "rpn_pending_b": rpn_pending_b,
        },
        "by_rpn": by_rpn,
        "by_status": by_status,
        "table": table[:500]
    })






##======================================Daily  Work Permit =========================================


def create_daily_permit(request):

    if not request.user.has_perm("EHS.add_dailyworkpermit"):
        logger.warning(
            "User '%s' tried to create permit without permission.",
            request.user.username
        )
        messages.error(request, "You do not have permission to add permits.")
        return redirect("permit_list")

    if request.method == 'POST':
        form = DailyWorkPermitForm(request.POST)
        if form.is_valid():
            permit = form.save()

            logger.info(
                "User '%s' created permit ID %s.",
                request.user.username,
                permit.id
            )

            messages.success(request, "Permit created successfully.")
            return redirect('permit_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = DailyWorkPermitForm()

    return render(request, 'daily_permit/daily_permit_form.html', {'form': form})

def permit_list(request):
    permits = Dailyworkpermit.objects.all().order_by('-created_at')

    work_location = request.GET.get('work_location')
    permit_status = request.GET.get('status')
    initiated_by = request.GET.get('initiated_by')  

    if work_location:
        permits = permits.filter(work_location=work_location)

    if permit_status:
        permits = permits.filter(permit_open_closed=permit_status)

    if initiated_by: 
        permits = permits.filter(initiated_by__iexact=initiated_by.strip())

    # For dropdown values
    locations = Dailyworkpermit.objects.values_list('work_location', flat=True).distinct()
    initiated_users = Dailyworkpermit.objects.values_list('initiated_by', flat=True).distinct()

    context = {
        'permits': permits,
        'locations': locations,
        'initiated_users': initiated_users,
    }

    return render(request, 'daily_permit/permit_list.html', context)



def edit_permit(request, pk):

    if not request.user.has_perm("EHS.change_dailyworkpermit"):
        logger.warning(
            "User '%s' tried to edit permit ID %s without permission.",
            request.user.username,
            pk
        )
        messages.error(request, "You do not have permission to edit permits.")
        return redirect("permit_list")

    permit = get_object_or_404(Dailyworkpermit, pk=pk)

    if request.method == 'POST':
        form = DailyWorkPermitForm(request.POST, instance=permit)
        if form.is_valid():
            form.save()

            logger.info(
                "User '%s' edited permit ID %s.",
                request.user.username,
                permit.id
            )

            messages.success(request, "Permit updated successfully.")
            return redirect('permit_list')
        else:
            messages.error(request, "Please correct the errors below.")
    else:
        form = DailyWorkPermitForm(instance=permit)

    return render(request, 'daily_permit/daily_permit_form.html', {'form': form})


def delete_permit(request, pk):

    if not request.user.has_perm("EHS.delete_dailyworkpermit"):
        logger.warning(
            "User '%s' tried to delete permit ID %s without permission.",
            request.user.username,
            pk
        )
        messages.error(request, "You do not have permission to delete permits.")
        return redirect("permit_list")

    permit = get_object_or_404(Dailyworkpermit, pk=pk)

    if request.method == 'POST':
        permit_id = permit.id
        permit.delete()

        logger.info(
            "User '%s' deleted permit ID %s.",
            request.user.username,
            permit_id
        )

        messages.success(request, "Permit deleted successfully.")

    return redirect('permit_list')




def export_permits_excel(request):

    if not request.user.has_perm("EHS.view_dailyworkpermit"):
        logger.warning(
            "User '%s' tried to download Excel without permission.",
            request.user.username
        )
        messages.error(request, "You do not have permission to download Excel.")
        return redirect("permit_list")

    permits = Dailyworkpermit.objects.all()

    # Get filter values from URL
    work_location = request.GET.get('work_location')
    initiated_by = request.GET.get('initiated_by')
    status = request.GET.get('status')

    # Apply filters
    if work_location:
        permits = permits.filter(work_location=work_location)

    if initiated_by:
        permits = permits.filter(initiated_by=initiated_by)

    if status:
        permits = permits.filter(permit_open_closed=status)

    permits = permits.order_by('-created_at')

    logger.info(
        "User '%s' downloaded Excel. Filters - Location: %s, Initiated By: %s, Status: %s",
        request.user.username,
        work_location,
        initiated_by,
        status
    )

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output)
    worksheet = workbook.add_worksheet("Daily Work Permit")

    # -----------------------
    # FORMATS
    # -----------------------

    title_format = workbook.add_format({
        'bold': True,
        'font_size': 14,
        'align': 'center',
        'valign': 'vcenter',
        'bg_color': "#00B7FF"
    })

    header_format = workbook.add_format({
        'bold': True,
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'text_wrap': True
    })

    yellow_format = workbook.add_format({
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'bg_color': '#FFFF00'
    })

    normal_format = workbook.add_format({
        'border': 1,
        'align': 'center',
        'valign': 'vcenter'
    })

    green_format = workbook.add_format({
        'border': 1,
        'align': 'center',
        'valign': 'vcenter',
        'bg_color': '#C6EFCE'
    })

    # -----------------------
    # SET COLUMN WIDTH
    # -----------------------
    worksheet.set_column('A:A', 5)
    worksheet.set_column('B:B', 12)
    worksheet.set_column('B:C', 10)
    worksheet.set_column('D:D', 20)
    worksheet.set_column('E:E', 30)
    worksheet.set_column('F:F', 12)
    worksheet.set_column('G:G', 10)
    worksheet.set_column('H:I', 20)
    worksheet.set_column('J:O', 14)  # right side columns

    # -----------------------
    # TITLE ROW
    # -----------------------

    title = "Daily Work Permit"
    worksheet.merge_range('A1:Q1', f'{title}', title_format)

    # -----------------------
    # HEADER ROWS
    # -----------------------

# Merge vertical headers (Row 2 & 3)
    worksheet.merge_range('A2:A3', 'Sr. No.', header_format)
    worksheet.merge_range('B2:B3', 'Permit Date', header_format)
    worksheet.merge_range('C2:C3', 'Site Time', header_format)
    worksheet.merge_range('D2:D3', 'Permit Approval Time', header_format)
    worksheet.merge_range('E2:E3', 'Reason For Delay Permit', header_format)
    worksheet.merge_range('F2:F3', 'Work Description', header_format)
    worksheet.merge_range('G2:G3', 'Work Location', header_format)
    worksheet.merge_range('H2:H3', 'Permit No.', header_format)

    worksheet.merge_range('I2:I3', 'Work Type', header_format)
    worksheet.merge_range('J2:J3', 'Work Type Value', header_format)
    worksheet.merge_range('K2:K3', 'Initiated By', header_format)
    worksheet.merge_range('L2:L3', 'Permit Open / Closed', header_format)
    worksheet.merge_range('M2:M3', 'permit_status_reason', header_format)
    worksheet.merge_range('N2:N3', 'Validity Of Permit', header_format)
    worksheet.merge_range('O2:O3', 'Extenction Of Permit', header_format)



    # -----------------------
    # DATA START FROM ROW 4
    # -----------------------
    row = 3

    for index, permit in enumerate(permits, start=1):

        worksheet.write(row, 0, index, normal_format)  # Sr No

        worksheet.write(
            row, 1,
            permit.permit_date.strftime("%d-%m-%Y") if permit.permit_date else "",
            normal_format
        )

        worksheet.write(
            row, 2,
            permit.site_time.strftime("%H:%M") if permit.site_time else "",
            normal_format
        )

        worksheet.write(
            row, 3,
            permit.permit_approval_time.strftime("%H:%M") if permit.permit_approval_time else "",
            normal_format
        )

        worksheet.write(row, 4, permit.reason_for_delay or "", normal_format)
        worksheet.write(row, 5, permit.work_description or "", normal_format)
        worksheet.write(row, 6, permit.work_location or "", normal_format)
        worksheet.write(row, 7, permit.permit_no or "", normal_format)
        worksheet.write(row, 8, permit.work_type or "", normal_format)
        worksheet.write(row, 9, permit.work_type_value or "", normal_format)

        worksheet.write(row, 10, permit.initiated_by or "", normal_format)
        worksheet.write(row, 11, permit.permit_open_closed or "", green_format)

        worksheet.write(row, 12, permit.permit_status_reason or "", normal_format)


        # Validity
        validity = ""
        if permit.valid_from and permit.valid_to:
            validity = f"{permit.valid_from.strftime('%H:%M')} to {permit.valid_to.strftime('%H:%M')}"
        worksheet.write(row, 13, validity, normal_format)

        # Extenction
        extension = ""
        if permit.Extenction_valid_from and permit.Extenction_valid_to:
            extension = f"{permit.Extenction_valid_from.strftime('%H:%M')} to {permit.Extenction_valid_to.strftime('%H:%M')}"
        worksheet.write(row, 14, extension, normal_format)

        

        row += 1

    workbook.close()
    output.seek(0)

    response = HttpResponse(
        output,
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = 'attachment; filename=Daily_Work_Permit_Styled.xlsx'

    return response

