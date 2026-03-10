import os
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib import messages
from django.db.models import Q
from HR.models import *
from django.contrib.auth.decorators import  permission_required
from .forms import *
from django.db.models import Count,Sum
from datetime import date,datetime
from django.utils.timezone import now
import calendar
import requests
import io
import xlsxwriter
from django.http import HttpResponse,JsonResponse
import pandas as pd
from collections import defaultdict
from django.db.models import Value as V
from django.db.models.functions import Coalesce
from .utils import *
from django.utils import timezone
from .tasks import *
from django.views.decorators.http import require_POST
import logging


logger = logging.getLogger("custom_logger")



@login_required
def create_hr(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('HR.add_hr'):
        messages.error(request, "You do not have permission to add HR records.")
        return redirect('indexpage')

    if request.method == 'POST':
        form = HRForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, 'HR record submitted successfully!')
            return redirect('view_hr_records')
        else:
            messages.error(request, 'There was an error with your submission.')
    else:
        form = HRForm()

    return render(request, 'hr/create_hr.html', 
                  {'form': form,'user_groups':user_groups,'is_superuser':is_superuser})


@login_required
def view_hr_records(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('HR.view_hr'):
        messages.error(request, "You do not have permission to add HR records.")
        return redirect('indexpage')

    can_edit_hr = request.user.has_perm('HR.change_hr')
    can_delete_hr = request.user.has_perm('HR.delete_hr')
    can_add_hr = request.user.has_perm('HR.add_hr')

    # Get filter inputs
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')

    # If "None", treat as empty
    if start_date == "None":
        start_date = ""
    if end_date == "None":
        end_date = ""

    # Fetch all HR records
    records = HR.objects.all()

    # Apply date filters if provided
    if start_date and end_date:
        records = records.filter(date__range=[start_date, end_date])
    elif start_date:
        records = records.filter(date__gte=start_date)
    elif end_date:
        records = records.filter(date__lte=end_date)

    # Sort descending
    records = records.order_by('-date')

    # Pagination
    paginator = Paginator(records, 10)
    page = request.GET.get('page')
    try:
        records = paginator.page(page)
    except PageNotAnInteger:
        records = paginator.page(1)
    except EmptyPage:
        records = paginator.page(paginator.num_pages)

    context = {
        'records': records,
        'start_date': start_date,
        'end_date': end_date,
        'can_edit_hr': can_edit_hr,
        'can_delete_hr': can_delete_hr,
        'can_add_hr': can_add_hr,
        'user_groups': user_groups,
        'is_superuser': is_superuser
    }
    return render(request, 'hr/view_hr.html', context)




@login_required
def edit_hr(request, pk):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    # Check if the user has permission to edit
    if not request.user.has_perm('HR.change_hr'):
        messages.error(request, "You do not have permission to edit HR records.")
        return redirect('indexpage')
    
    record = get_object_or_404(HR, pk=pk)

    if request.method == 'POST':
        # Bind the form to the POST data and the existing instance
        form = HRForm(request.POST, instance=record)
        if form.is_valid():
            form.save()
            messages.success(request, 'HR record updated successfully!')
            return redirect('view_hr_records')
        else:
            messages.error(request, 'There was an error updating the HR record.')
    else:
        # Initialize the form with the current record
        form = HRForm(instance=record)

    context = {
        'form': form,
        'user_groups':user_groups,
        'is_superuser':is_superuser
    }
    return render(request, 'hr/edit_hr.html', context)


@login_required
def delete_hr(request, pk):
    if not request.user.has_perm('HR.delete_hr'):
        # Add a message if the user lacks permission
        messages.error(request, "You do not have permission to Delete HR records.")
        return redirect('indexpage')
    
    record = get_object_or_404(HR, pk=pk)
    record.delete()
    messages.success(request, 'HR record deleted successfully!')
    return redirect('view_hr_records')


# Mapping status_in_out to readable names
STATUS_LABELS = {
    'APL | APL': 'All Purpose Leave (APL)',
    'COM | COM': 'Compensation (COM)',
    'CO | CO': 'Compensatory Off (CO)',
    'APL | P': 'APL + Present',
    'P | COM': 'Present + Compensation',
    'P | P': 'Present',
    'CO | P': 'CO + Present',
    'P | CO': 'Present',
    'A | P': '2nd Half Present',
    'HO | HO': 'Holiday (HO)',
    'APL | A': 'APL + Absent',
    'A | A': 'Absent',
    'P | A': '1st Half Present',
    'WO | WO': 'Week Off (WO)',
    'P | APL': 'Present + APL',
    'ML | ML' : 'Maternity Leave',
    'COM | APL': 'COM + APL',
}


@login_required
def attendance_summary(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)

    today = date.today().isoformat()
    start_date = request.GET.get('start_date') or today
    end_date = request.GET.get('end_date') or today
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')
    selected_department = request.GET.get('department', '')
    selected_sub_department = request.GET.get('sub_department', '') # Added sub_department
    selected_shift_code = request.GET.get('shift_code', '')

    queryset = DailyAttendance.objects.all()

    if start_date and end_date:
        if start_date == end_date:
            queryset = queryset.filter(attendance_date=start_date)
        else:
            queryset = queryset.filter(attendance_date__gte=start_date, attendance_date__lte=end_date)

    if selected_company:
        queryset = queryset.filter(company=selected_company)

    if selected_branch:
        queryset = queryset.filter(branch=selected_branch)

    if selected_department:
        queryset = queryset.filter(department=selected_department)

    if selected_sub_department: # Added filter logic for sub_department
        queryset = queryset.filter(sub_department=selected_sub_department)

    if selected_shift_code:
        queryset = queryset.filter(shift_code=selected_shift_code)

    stats = queryset.values('status_in_out').annotate(count=Count('id'))

    for stat in stats:
        stat['label'] = STATUS_LABELS.get(stat['status_in_out'], stat['status_in_out'])

    total_count = queryset.count()

    companies = DailyAttendance.objects.values_list('company', flat=True).distinct()
    branches = DailyAttendance.objects.values_list('branch', flat=True).distinct()
    departments = DailyAttendance.objects.values_list('department', flat=True).distinct()
    sub_departments = DailyAttendance.objects.values_list('sub_department', flat=True).distinct() # Added sub_departments
    shift_codes = DailyAttendance.objects.values_list('shift_code', flat=True).distinct()

    return render(request, 'hr/attendance_summary.html', {
        'active_link': 'attendance_summary',
        'stats': stats,
        'start_date': start_date,
        'end_date': end_date,
        'total_count': total_count,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'show_admin_panel': show_admin_panel,
        'companies': companies,
        'branches': branches,
        'departments': departments,
        'sub_departments': sub_departments, # Added to context
        'shift_codes': shift_codes,
        'selected_company': selected_company,
        'selected_branch': selected_branch,
        'selected_department': selected_department,
        'selected_sub_department': selected_sub_department, # Added to context
        'selected_shift_code': selected_shift_code,
    })


@login_required
def attendance_by_status(request, status_code):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    # Existing filters
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')
    search_name = request.GET.get('search_name', '').strip()

    # Filters for company, branch, department, sub_department, and shift_code
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')
    selected_department = request.GET.get('department', '')
    selected_sub_department = request.GET.get('sub_department', '') # Added sub_department
    selected_shift_code = request.GET.get('shift_code', '')

    # Initial queryset
    queryset = DailyAttendance.objects.all()

    # Filter by status code if not 'all'
    if status_code != 'all':
        queryset = queryset.filter(status_in_out=status_code)

    # Apply date filters
    if start_date and end_date:
        if start_date == end_date:
            queryset = queryset.filter(attendance_date=start_date)
        else:
            queryset = queryset.filter(attendance_date__range=(start_date, end_date))
    elif start_date:
        queryset = queryset.filter(attendance_date__gte=start_date)
    elif end_date:
        queryset = queryset.filter(attendance_date__lte=end_date)

    # Apply name filter
    if search_name:
        queryset = queryset.filter(full_name__icontains=search_name)

    # Apply additional filters
    if selected_company:
        queryset = queryset.filter(company=selected_company)
    if selected_branch:
        queryset = queryset.filter(branch=selected_branch)
    if selected_department:
        queryset = queryset.filter(department=selected_department)
    if selected_sub_department: # Added filter logic for sub_department
        queryset = queryset.filter(sub_department=selected_sub_department)
    if selected_shift_code:
        queryset = queryset.filter(shift_code=selected_shift_code)

    # Retrieve the friendly label for the status code
    label = STATUS_LABELS.get(status_code, 'All Records' if status_code == 'all' else status_code)

    # Distinct filter options
    companies = DailyAttendance.objects.values_list('company', flat=True).distinct()
    branches = DailyAttendance.objects.values_list('branch', flat=True).distinct()
    departments = DailyAttendance.objects.values_list('department', flat=True).distinct()
    sub_departments = DailyAttendance.objects.values_list('sub_department', flat=True).distinct() # Added sub_departments
    shift_codes = DailyAttendance.objects.values_list('shift_code', flat=True).distinct()

    return render(request, 'hr/attendance_detail.html', {
        'active_link': 'attendance_summary',
        'records': queryset,
        'label': label,
        'start_date': start_date,
        'end_date': end_date,
        'search_name': search_name,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'companies': companies,
        'branches': branches,
        'departments': departments,
        'sub_departments': sub_departments, # Added to context
        'shift_codes': shift_codes,
        'status_code': status_code,
        'selected_company': selected_company,
        'selected_branch': selected_branch,
        'selected_department': selected_department,
        'selected_sub_department': selected_sub_department, # Added to context
        'selected_shift_code': selected_shift_code,
    })


@login_required
def attendance_download_excel(request, status_code):
    """
    Generates an Excel file of filtered DailyAttendance records,
    including handling of the 'all' status code to fetch all records.
    """
    # Retrieve filter parameters from query string
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')
    search_name = request.GET.get('search_name', '').strip()
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')
    selected_department = request.GET.get('department', '')
    selected_sub_department = request.GET.get('sub_department', '') # Added sub_department
    selected_shift_code = request.GET.get('shift_code', '')

    # Initial queryset
    queryset = DailyAttendance.objects.all()

    # Apply status filter if status_code is not 'all'
    if status_code != 'all':
        queryset = queryset.filter(status_in_out=status_code)

    # Apply date filters
    if start_date and end_date:
        queryset = queryset.filter(attendance_date__range=(start_date, end_date))
    elif start_date:
        queryset = queryset.filter(attendance_date__gte=start_date)
    elif end_date:
        queryset = queryset.filter(attendance_date__lte=end_date)

    # Apply search filters
    if search_name:
        queryset = queryset.filter(full_name__icontains=search_name)
    if selected_company:
        queryset = queryset.filter(company=selected_company)
    if selected_branch:
        queryset = queryset.filter(branch=selected_branch)
    if selected_department:
        queryset = queryset.filter(department=selected_department)
    if selected_sub_department: # Added filter logic for sub_department
        queryset = queryset.filter(sub_department=selected_sub_department)
    if selected_shift_code:
        queryset = queryset.filter(shift_code=selected_shift_code)

    # Create an in-memory Excel file
    output = io.BytesIO()

    # Create Excel workbook using xlsxwriter
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet("Attendance")

    # Define headers
    headers = [
        "Attendance Date", "Employee Code", "Full Name", "Status In/Out", "Punch In/Out Time",
        "Reporting Manager", "Company", "Business Unit", "Department", "Sub Department", # Added Sub Department
        "Designation", "Branch", "Sub Branch", "Employment Status",
        "Shift Code", "Shift Timing", "Late or Early", "Working Hours",
        "Total Office Hours", "Source", "Date of Joining", "Employment Type",
        "Grade", "Lat/Long", "Level", "Location", "Mobile", "Region", "Work Email"
    ]

    # Write headers
    header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC'})
    worksheet.set_row(0, None, header_format)

    for col_num, header in enumerate(headers):
        worksheet.write(0, col_num, header)

    # Populate data rows
    for row_num, record in enumerate(queryset, start=1):
        worksheet.write(row_num, 0, record.attendance_date.strftime('%Y-%m-%d') if record.attendance_date else '')
        worksheet.write(row_num, 1, record.employee_code)
        worksheet.write(row_num, 2, record.full_name)
        worksheet.write(row_num, 3, record.status_in_out)
        worksheet.write(row_num, 4, record.punch_in_punch_out_time)
        worksheet.write(row_num, 5, record.reporting_manager)
        worksheet.write(row_num, 6, record.company)
        worksheet.write(row_num, 7, record.business_unit)
        worksheet.write(row_num, 8, record.department)
        worksheet.write(row_num, 9, record.sub_department) # Added sub_department data
        worksheet.write(row_num, 10, record.designation)
        worksheet.write(row_num, 11, record.branch)
        worksheet.write(row_num, 12, record.sub_branch)
        worksheet.write(row_num, 13, record.employment_status)
        worksheet.write(row_num, 14, record.shift_code)
        worksheet.write(row_num, 15, record.shift_timing)
        worksheet.write(row_num, 16, record.Late_or_early)
        worksheet.write(row_num, 17, record.working_hours)
        worksheet.write(row_num, 18, record.total_office_hours)
        worksheet.write(row_num, 19, record.source)
        worksheet.write(row_num, 20, record.date_of_joining.strftime('%Y-%m-%d') if record.date_of_joining else '')
        worksheet.write(row_num, 21, record.employment_type)
        worksheet.write(row_num, 22, record.grade)
        worksheet.write(row_num, 23, record.lattitude_longitude)
        worksheet.write(row_num, 24, record.level)
        worksheet.write(row_num, 25, record.location)
        worksheet.write(row_num, 26, record.mobile)
        worksheet.write(row_num, 27, record.region)
        worksheet.write(row_num, 28, record.work_email)

    workbook.close()
    output.seek(0)

    # Prepare Excel response
    filename = f"attendance_{status_code}.xlsx" if status_code != 'all' else "attendance_all_records.xlsx"
    response = HttpResponse(
        output.read(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response




@login_required
def attendance_regulation_summary(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    today = now().date()

    # Extract filters from request
    selected_year = int(request.GET.get('year', today.year))
    selected_month = int(request.GET.get('month', today.month))
    selected_date = request.GET.get('date','')  # Optional: specific day filter
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')
    selected_department = request.GET.get('department', '')

    # Base queryset
    queryset = AttendanceRegulation.objects.filter(
        attendance_date__year=selected_year,
        attendance_date__month=selected_month
    )
    
    # Apply specific day filter if provided
    if selected_date:
        queryset = queryset.filter(attendance_date=selected_date)

    # Apply company, branch, and department filters
    if selected_company:
        queryset = queryset.filter(company=selected_company)
    if selected_branch:
        queryset = queryset.filter(branch=selected_branch)
    if selected_department:
        queryset = queryset.filter(department=selected_department)

    total_count = queryset.count()
    # Aggregation for status counts
    status_counts = queryset.values('request_status').annotate(count=Count('id'))
    
    # Dropdown values for filters
    years = list(range(2023, today.year + 1))
    months = [(i, calendar.month_name[i]) for i in range(1, 13)]
    company_list = AttendanceRegulation.objects.values_list('company', flat=True).distinct()
    branch_list = AttendanceRegulation.objects.values_list('branch', flat=True).distinct()
    department_list = AttendanceRegulation.objects.values_list('department', flat=True).distinct()

    context = {
        'active_link': 'attendance_regulation',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'status_counts': status_counts,
        'selected_month': selected_month,
        'selected_year': selected_year,
        'selected_date': selected_date,  # Add to context if used in template
        'selected_company': selected_company,
        'selected_branch': selected_branch,
        'selected_department': selected_department,
        'years': years,
        'months': months,
        'company_list': company_list,
        'branch_list': branch_list,
        'department_list': department_list,
        'total_count': total_count,
    }

    return render(request, 'hr/attendance_regulation_summary.html', context)


@login_required
def attendance_regulation_by_status(request, status):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    today = now().date()

    # Extract filters from the request
    selected_year = int(request.GET.get('year', today.year))
    selected_month = int(request.GET.get('month', today.month))
    selected_date = request.GET.get('date', '')  # New: specific day filter
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')
    selected_department = request.GET.get('department', '')
    full_name_query = request.GET.get('full_name', '').strip()

    # Base queryset for filtering by status, year, and month
    queryset = AttendanceRegulation.objects.filter(
        attendance_date__year=selected_year,
        attendance_date__month=selected_month
    )

    # Apply specific day filter if provided
    if selected_date:
        queryset = queryset.filter(attendance_date=selected_date)

    # Apply filters for company, branch, department, and full name
    if selected_company:
        queryset = queryset.filter(company=selected_company)
    if selected_branch:
        queryset = queryset.filter(branch=selected_branch)
    if selected_department:
        queryset = queryset.filter(department=selected_department)
    if full_name_query:
        queryset = queryset.filter(full_name__icontains=full_name_query)
    if status != 'all':
        queryset = queryset.filter(request_status=status)

    # Prepare data for dropdowns and context
    years = list(range(2023, today.year + 1))
    months = [(i, calendar.month_name[i]) for i in range(1, 13)]
    company_list = AttendanceRegulation.objects.values_list('company', flat=True).distinct()
    branch_list = AttendanceRegulation.objects.values_list('branch', flat=True).distinct()
    department_list = AttendanceRegulation.objects.values_list('department', flat=True).distinct()

    return render(request, 'hr/attendance_regulation_detail.html', {
        'active_link': 'attendance_regulation',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'records': queryset,
        'status': status,
        'selected_month': selected_month,
        'selected_year': selected_year,
        'selected_date': selected_date,
        'selected_company': selected_company,
        'selected_branch': selected_branch,
        'selected_department': selected_department,
        'years': years,
        'months': months,
        'company_list': company_list,
        'branch_list': branch_list,
        'department_list': department_list,
        'full_name_query': full_name_query,
    })


@login_required
def download_attendance_regulation_excel(request, status):
    today = now().date()
    selected_year = int(request.GET.get('year', today.year))
    selected_month = int(request.GET.get('month', today.month))
    selected_date = request.GET.get('date', '')
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')
    selected_department = request.GET.get('department', '')
    full_name_query = request.GET.get('full_name', '').strip()

    # Base queryset with date filters
    queryset = AttendanceRegulation.objects.filter(
        attendance_date__year=selected_year,
        attendance_date__month=selected_month
    )

    # Apply additional filters
    if selected_date:
        queryset = queryset.filter(attendance_date=selected_date)
    if selected_company:
        queryset = queryset.filter(company=selected_company)
    if selected_branch:
        queryset = queryset.filter(branch=selected_branch)
    if selected_department:
        queryset = queryset.filter(department=selected_department)
    if full_name_query:
        queryset = queryset.filter(full_name__icontains=full_name_query)
    
    # Only filter by status if not "all"
    if status != "all":
        queryset = queryset.filter(request_status=status)

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet("Attendance Regulation")

    headers = [
        "Employee Code", "Full Name", "Employment Status", "Company", "Business Unit", "Department",
        "Designation", "Branch", "Sub Branch", "Request Type", "Attendance Date", "Attendance Day",
        "Reason", "Shift Code", "Shift Timings", "Actual In/Out", "Punch In Date", "Punch In Time",
        "Punch Out Date", "Punch Out Time", "Remarks", "Request Status", "Requested By",
        "Requested On", "Approved By", "Approved On", "Approver Remark"
    ]

    # Write headers
    header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC'})
    worksheet.set_row(0, None, header_format)

    for col, header in enumerate(headers):
        worksheet.write(0, col, header)

    for row, r in enumerate(queryset, start=1):
        worksheet.write(row, 0, r.employee_code)
        worksheet.write(row, 1, r.full_name)
        worksheet.write(row, 2, r.employment_status)
        worksheet.write(row, 3, r.company)
        worksheet.write(row, 4, r.business_unit)
        worksheet.write(row, 5, r.department)
        worksheet.write(row, 6, r.designation)
        worksheet.write(row, 7, r.branch)
        worksheet.write(row, 8, r.sub_branch)
        worksheet.write(row, 9, r.request_type)
        worksheet.write(row, 10, r.attendance_date.strftime('%Y-%m-%d'))
        worksheet.write(row, 11, r.attendance_day)
        worksheet.write(row, 12, r.reason)
        worksheet.write(row, 13, r.shift_code)
        worksheet.write(row, 14, r.shift_timings)
        worksheet.write(row, 15, r.actual_punch_in_out or '')
        worksheet.write(row, 16, r.punch_in_date.strftime('%Y-%m-%d') if r.punch_in_date else '')
        worksheet.write(row, 17, r.punch_in_time or '')
        worksheet.write(row, 18, r.punch_out_date.strftime('%Y-%m-%d') if r.punch_out_date else '')
        worksheet.write(row, 19, r.punch_out_time or '')
        worksheet.write(row, 20, r.remarks or '')
        worksheet.write(row, 21, r.request_status)
        worksheet.write(row, 22, r.requested_by)
        worksheet.write(row, 23, r.requested_on.strftime('%Y-%m-%d'))
        worksheet.write(row, 24, r.approved_by or '')
        worksheet.write(row, 25, r.approved_on.strftime('%Y-%m-%d') if r.approved_on else '')
        worksheet.write(row, 26, r.approver_remark or '')

    workbook.close()
    output.seek(0)

    filename = f"attendance_regulation_{status.lower()}.xlsx"
    return HttpResponse(
        output.read(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )




@login_required
def daily_check_in_summary(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    today = now().date()


    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')
    selected_department = request.GET.get('department', '')
    selected_shift = request.GET.get('shift', '')

    from_date = request.GET.get('from_date')
    to_date = request.GET.get('to_date')

    if not from_date:
        from_date = today.strftime("%Y-%m-%d")
    if not to_date:
        to_date = today.strftime("%Y-%m-%d")

    # Filtering
    queryset = DailyCheckIn.objects.filter(attendance_date__range=[from_date, to_date])

    total_count = queryset.count()

    if selected_company:
        queryset = queryset.filter(company=selected_company)
    if selected_branch:
        queryset = queryset.filter(branch=selected_branch)
    if selected_department:
        queryset = queryset.filter(department=selected_department)
    if selected_shift:
        queryset = queryset.filter(shift=selected_shift)

    status_counts = queryset.values('check_in').annotate(count=Count('id'))

    company_list = DailyCheckIn.objects.values_list('company', flat=True).distinct()
    branch_list = DailyCheckIn.objects.values_list('branch', flat=True).distinct()
    department_list = DailyCheckIn.objects.values_list('department', flat=True).distinct()
    shift_list = DailyCheckIn.objects.values_list('shift', flat=True).distinct()

    return render(request, 'hr/daily_check_in_summary.html', {
        'active_link': 'daily_check_in',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'total_count': total_count,
        'status_counts': status_counts,
        'from_date': from_date,
        'to_date': to_date,
        'selected_company': selected_company,
        'selected_branch': selected_branch,
        'selected_department': selected_department,
        'selected_shift': selected_shift,
        'company_list': company_list,
        'branch_list': branch_list,
        'department_list': department_list,
        'shift_list': shift_list,
    })


@login_required
def check_in_status_detail(request, check_in_status):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    today = now().date()

    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')
    selected_department = request.GET.get('department', '')
    selected_shift = request.GET.get('shift', '')
    full_name_query = request.GET.get('full_name', '').strip()

    # Date filters
    from_date = request.GET.get('from_date')
    to_date = request.GET.get('to_date')

    if not from_date:
        from_date = today.strftime('%Y-%m-%d')
    if not to_date:
        to_date = today.strftime('%Y-%m-%d')

    # Base queryset filtered by date
    records = DailyCheckIn.objects.filter(attendance_date__range=[from_date, to_date])

    # Apply check_in filter if not 'all'
    if check_in_status != 'all':
        records = records.filter(check_in=check_in_status)

    # Apply other filters
    if selected_company:
        records = records.filter(company=selected_company)
    if selected_branch:
        records = records.filter(branch=selected_branch)
    if selected_department:
        records = records.filter(department=selected_department)
    if selected_shift:
        records = records.filter(shift=selected_shift)
    if full_name_query:
        records = records.filter(full_name__icontains=full_name_query)

    # Dropdown data
    company_list = DailyCheckIn.objects.values_list('company', flat=True).distinct()
    branch_list = DailyCheckIn.objects.values_list('branch', flat=True).distinct()
    department_list = DailyCheckIn.objects.values_list('department', flat=True).distinct()
    shift_list = DailyCheckIn.objects.values_list('shift', flat=True).distinct()

    return render(request, 'hr/daily_check_in_status_detail.html', {
        'active_link': 'daily_check_in',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'records': records,
        'check_in_status': check_in_status,
        'from_date': from_date,
        'to_date': to_date,
        'selected_company': selected_company,
        'selected_branch': selected_branch,
        'selected_department': selected_department,
        'selected_shift': selected_shift,
        'company_list': company_list,
        'branch_list': branch_list,
        'department_list': department_list,
        'shift_list': shift_list,
        'full_name_query': full_name_query,
    })



@login_required
def check_in_status_excel_download(request, check_in_status):
    # Retrieve filter parameters from GET request
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')
    selected_department = request.GET.get('department', '')
    selected_shift = request.GET.get('shift', '')
    full_name_query = request.GET.get('full_name', '').strip()
    from_date = request.GET.get('from_date')
    to_date = request.GET.get('to_date')

    today = now().date()

    # Default date handling
    if not from_date:
        from_date = today.strftime('%Y-%m-%d')
    if not to_date:
        to_date = today.strftime('%Y-%m-%d')

    # Initial queryset filtered by date
    records = DailyCheckIn.objects.filter(attendance_date__range=[from_date, to_date])

    # Apply status filter unless 'all'
    if check_in_status != 'all':
        records = records.filter(check_in=check_in_status)

    # Additional filters
    if selected_company:
        records = records.filter(company=selected_company)
    if selected_branch:
        records = records.filter(branch=selected_branch)
    if selected_department:
        records = records.filter(department=selected_department)
    if selected_shift:
        records = records.filter(shift=selected_shift)
    if full_name_query:
        records = records.filter(full_name__icontains=full_name_query)

    # Create Excel file in-memory
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet("Daily Check-In")

    # Define Excel header
    headers = [
        "Employee Code", "Full Name", "Employment Status", "Company",
        "Business Unit", "Department", "Designation", "Branch",
        "Sub Branch", "Attendance Date", "Shift", "Check-In Status",
        "First Punch", "Last Punch", "Raw Punch", "Source"
    ]

    # Write header row with formatting
    header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC'})
    worksheet.set_row(0, None, header_format)

    for col_num, header in enumerate(headers):
        worksheet.write(0, col_num, header)

    # Write data rows
    for row_num, record in enumerate(records, start=1):
        worksheet.write(row_num, 0, record.employee_code)
        worksheet.write(row_num, 1, record.full_name)
        worksheet.write(row_num, 2, record.employment_status)
        worksheet.write(row_num, 3, record.company)
        worksheet.write(row_num, 4, record.business_unit)
        worksheet.write(row_num, 5, record.department)
        worksheet.write(row_num, 6, record.designation)
        worksheet.write(row_num, 7, record.branch)
        worksheet.write(row_num, 8, record.sub_branch)
        worksheet.write(row_num, 9, record.attendance_date.strftime('%Y-%m-%d'))
        worksheet.write(row_num, 10, record.shift)
        worksheet.write(row_num, 11, record.check_in)
        worksheet.write(row_num, 12, record.first_punch.strftime('%H:%M:%S') if record.first_punch else '')
        worksheet.write(row_num, 13, record.last_punch.strftime('%H:%M:%S') if record.last_punch else '')
        worksheet.write(row_num, 14, record.raw_punch)
        worksheet.write(row_num, 15, record.source)

    workbook.close()
    output.seek(0)

    # Prepare HttpResponse with Excel headers
    filename = f"check_in_{check_in_status}_{from_date}_to_{to_date}.xlsx"
    response = HttpResponse(
        output.read(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response






@login_required
def late_early_go_summary(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    today = now().date()

    # Get filters from request
    from_date = request.GET.get('from_date')
    to_date = request.GET.get('to_date')
    department = request.GET.get('department', '')
    branch = request.GET.get('branch', '')
    shift_code = request.GET.get('shift_code', '')
    company = request.GET.get('company', '')

    # Default to today's date if filters are not provided
    if not from_date:
        from_date = today.strftime('%Y-%m-%d')
    if not to_date:
        to_date = today.strftime('%Y-%m-%d')

    # Base queryset
    queryset = Late_Early_Go.objects.filter(attendance_date__range=[from_date, to_date])

    # Apply other filters
    if department:
        queryset = queryset.filter(department__icontains=department)
    if branch:
        queryset = queryset.filter(branch__icontains=branch)
    if shift_code:
        queryset = queryset.filter(shift_code__icontains=shift_code)
    if company:
        queryset = queryset.filter(company__icontains=company)
    
    # Count by Late/Early
    status_counts = queryset.values('late_early').annotate(count=Count('id'))
    total_count = queryset.count()
    # Dropdown filter values
    department_list = Late_Early_Go.objects.values_list('department', flat=True).distinct()
    branch_list = Late_Early_Go.objects.values_list('branch', flat=True).distinct()
    shift_list = Late_Early_Go.objects.values_list('shift_code', flat=True).distinct()
    company_list = Late_Early_Go.objects.values_list('company', flat=True).distinct()

    return render(request, 'hr/late_early_go_summary.html', {
    'active_link': 'late_early_go',
    'user_groups': user_groups,
    'is_superuser': is_superuser,
    'status_counts': status_counts,
    'from_date': from_date,
    'to_date': to_date,
    'selected_department': department,
    'selected_branch': branch,
    'selected_shift_code': shift_code,
    'selected_company': company,
    'department_list': department_list,
    'branch_list': branch_list,
    'shift_list': shift_list,
    'company_list': company_list,
    'total_count': total_count,
})



@login_required
def late_early_go_detail(request, late_early):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    today = now().date()

    # Extract filters from GET parameters
    from_date = request.GET.get('from_date')
    to_date = request.GET.get('to_date')
    department = request.GET.get('department', '')
    branch = request.GET.get('branch', '')
    shift_code = request.GET.get('shift_code', '')
    company = request.GET.get('company', '')

    # Use today's date if none provided
    if not from_date:
        from_date = today.strftime('%Y-%m-%d')
    if not to_date:
        to_date = today.strftime('%Y-%m-%d')

    # Base queryset for the given late/early status and date range
    queryset = Late_Early_Go.objects.filter(attendance_date__range=[from_date, to_date])

    # Apply filters
    if department:
        queryset = queryset.filter(department__icontains=department)
    if branch:
        queryset = queryset.filter(branch__icontains=branch)
    if shift_code:
        queryset = queryset.filter(shift_code__icontains=shift_code)
    if company:
        queryset = queryset.filter(company__icontains=company)
    # Only filter by late_early if it's not 'all'
    if late_early != 'all':
        queryset = queryset.filter(late_early=late_early)

    # Dropdown values
    department_list = Late_Early_Go.objects.values_list('department', flat=True).distinct()
    branch_list = Late_Early_Go.objects.values_list('branch', flat=True).distinct()
    shift_list = Late_Early_Go.objects.values_list('shift_code', flat=True).distinct()
    company_list = Late_Early_Go.objects.values_list('company', flat=True).distinct()

    return render(request, 'hr/late_early_go_detail.html', {
        'active_link': 'late_early_go',
        'records': queryset,
        'late_early': late_early,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'from_date': from_date,
        'to_date': to_date,
        'selected_department': department,
        'selected_branch': branch,
        'selected_shift_code': shift_code,
        'selected_company': company,
        'department_list': department_list,
        'branch_list': branch_list,
        'shift_list': shift_list,
        'company_list': company_list,
    })



def late_early_go_download_excel(request, late_early):
    today = now().date()

    # Filters
    from_date = request.GET.get('from_date') or today.strftime('%Y-%m-%d')
    to_date = request.GET.get('to_date') or today.strftime('%Y-%m-%d')
    department = request.GET.get('department', '')
    branch = request.GET.get('branch', '')
    shift_code = request.GET.get('shift_code', '')
    company = request.GET.get('company', '')

    queryset = Late_Early_Go.objects.filter(attendance_date__range=[from_date, to_date])

    if department:
        queryset = queryset.filter(department__icontains=department)
    if branch:
        queryset = queryset.filter(branch__icontains=branch)
    if shift_code:
        queryset = queryset.filter(shift_code__icontains=shift_code)
    if company:
        queryset = queryset.filter(company__icontains=company)
    if late_early != 'all':
        queryset = queryset.filter(late_early=late_early)

    # Excel file generation
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet("Late Early Detail")

    headers = [
        "Attendance Date", "Employee Code", "Full Name", "Shift Timings", "Late/Early Minutes",
        "Employment Status", "Company", "Business Unit", "Department", "Designation",
        "Branch", "Sub Branch", "Shift Code", "Late/Early"
    ]

    # Write header row with formatting
    header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC'})
    worksheet.set_row(0, None, header_format)

    for col_num, header in enumerate(headers):
        worksheet.write(0, col_num, header)

    for row_num, record in enumerate(queryset, start=1):
        worksheet.write(row_num, 0, record.attendance_date.strftime('%Y-%m-%d') if record.attendance_date else '')
        worksheet.write(row_num, 1, record.employee_code)
        worksheet.write(row_num, 2, record.full_name)
        worksheet.write(row_num, 3, record.shift_timings)
        worksheet.write(row_num, 4, record.late_early_by_min)
        worksheet.write(row_num, 5, record.employment_status)
        worksheet.write(row_num, 6, record.company)
        worksheet.write(row_num, 7, record.business_unit)
        worksheet.write(row_num, 8, record.department)
        worksheet.write(row_num, 9, record.designation)
        worksheet.write(row_num, 10, record.branch)
        worksheet.write(row_num, 11, record.sub_branch)
        worksheet.write(row_num, 12, record.shift_code)
        worksheet.write(row_num, 13, record.late_early)

    workbook.close()
    output.seek(0)

    filename = f"late_early_detail_{late_early}_{from_date}_to_{to_date}.xlsx"
    response = HttpResponse(output.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response




@login_required
def on_duty_request_summary(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    today = now().date()

    # Filters from request
    selected_year = int(request.GET.get('year', today.year))
    selected_month = int(request.GET.get('month', today.month))
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')
    selected_status = request.GET.get('request_status', '')

    # Base queryset
    queryset = On_Duty_Request.objects.filter(
        attendance_date__year=selected_year,
        attendance_date__month=selected_month
    )

    # Apply filters
    if selected_company:
        queryset = queryset.filter(company__icontains=selected_company)
    if selected_branch:
        queryset = queryset.filter(branch__icontains=selected_branch)
    if selected_status:
        queryset = queryset.filter(request_status__icontains=selected_status)

    # Count per status
    status_counts = queryset.values('request_status').annotate(count=Count('id'))
    total_count = queryset.count()

    # Dropdown options
    company_list = On_Duty_Request.objects.values_list('company', flat=True).distinct()
    branch_list = On_Duty_Request.objects.values_list('branch', flat=True).distinct()
    status_list = On_Duty_Request.objects.values_list('request_status', flat=True).distinct()
    years = list(range(2023, today.year + 1))
    months = [(i, datetime(2000, i, 1).strftime('%B')) for i in range(1, 13)]

    return render(request, 'hr/on_duty_request_summary.html', {
        'active_link': 'on_duty_request',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'status_counts': status_counts,
        'selected_year': selected_year,
        'selected_month': selected_month,
        'selected_company': selected_company,
        'selected_branch': selected_branch,
        'selected_status': selected_status,
        'company_list': company_list,
        'branch_list': branch_list,
        'status_list': status_list,
        'years': years,
        'months': months,
        'total_count': total_count,  
    })



@login_required
def on_duty_request_detail(request, request_status):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    today = now().date()

    selected_year = int(request.GET.get('year', today.year))
    selected_month = int(request.GET.get('month', today.month))
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')

    queryset = On_Duty_Request.objects.filter(attendance_date__year=selected_year,attendance_date__month=selected_month)

    if selected_company:
        queryset = queryset.filter(company__icontains=selected_company)
    if selected_branch:
        queryset = queryset.filter(branch__icontains=selected_branch)
    if request_status != 'all':
        queryset = queryset.filter(request_status=request_status)

    company_list = On_Duty_Request.objects.values_list('company', flat=True).distinct()
    branch_list = On_Duty_Request.objects.values_list('branch', flat=True).distinct()
    status_list = On_Duty_Request.objects.values_list('request_status', flat=True).distinct()
    years = list(range(2023, today.year + 1))
    months = [(i, datetime(2000, i, 1).strftime('%B')) for i in range(1, 13)]

    return render(request, 'hr/on_duty_request_detail.html', {
        'records': queryset,
        'active_link': 'on_duty_request',
        'late_status': request_status,
        'selected_company': selected_company,
        'selected_branch': selected_branch,
        'selected_year': selected_year,
        'selected_month': selected_month,
        'company_list': company_list,
        'branch_list': branch_list,
        'status_list': status_list,
        'months': months,
        'years': years,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
    })


@login_required
def on_duty_request_download_excel(request, request_status):
    today = now().date()
    selected_year = int(request.GET.get('year', today.year))
    selected_month = int(request.GET.get('month', today.month))
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')

    queryset = On_Duty_Request.objects.filter(
        attendance_date__year=selected_year,
        attendance_date__month=selected_month
    )

    if selected_company:
        queryset = queryset.filter(company__icontains=selected_company)
    if selected_branch:
        queryset = queryset.filter(branch__icontains=selected_branch)
    if request_status != 'all':
        queryset = queryset.filter(request_status=request_status)

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet("OnDutyRequests")

    headers = [
        "Emp Code", "Full Name", "Status", "Company", "Business Unit", "Department", "Designation", "Branch", "Sub Branch",
        "Request Type", "Attendance Date", "Day", "OD Type", "Shift Code", "Timings", "Actual IO", "Punch In", "Punch Out",
        "Remarks", "Request By", "Request On", "Pending With", "Approved By", "Approved On", "Approver Remark",
        "Billable", "In Timing", "Out Timing"
    ]

    # Write header row with formatting
    header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC'})
    worksheet.set_row(0, None, header_format)

    for col, header in enumerate(headers):
        worksheet.write(0, col, header)

    for row_num, r in enumerate(queryset, start=1):
        worksheet.write(row_num, 0, r.employee_code)
        worksheet.write(row_num, 1, r.full_name)
        worksheet.write(row_num, 2, r.employment_status)
        worksheet.write(row_num, 3, r.company)
        worksheet.write(row_num, 4, r.business_unit)
        worksheet.write(row_num, 5, r.department)
        worksheet.write(row_num, 6, r.designation)
        worksheet.write(row_num, 7, r.branch)
        worksheet.write(row_num, 8, r.sub_branch)
        worksheet.write(row_num, 9, r.request_type)
        worksheet.write(row_num, 10, r.attendance_date.strftime('%d-%m-%Y') if r.attendance_date else '')
        worksheet.write(row_num, 11, r.attendance_day)
        worksheet.write(row_num, 12, r.on_duty_type)
        worksheet.write(row_num, 13, r.shift_code)
        worksheet.write(row_num, 14, r.shift_timings)
        worksheet.write(row_num, 15, r.actual_punch_in_out)
        worksheet.write(row_num, 16, str(r.punch_in_date) if r.punch_in_date else '')
        worksheet.write(row_num, 17, str(r.punch_out_date) if r.punch_out_date else '')
        worksheet.write(row_num, 18, r.remarks)
        worksheet.write(row_num, 19, r.request_by)
        worksheet.write(row_num, 20, str(r.request_on) if r.request_on else '')
        worksheet.write(row_num, 21, r.pending_with)
        worksheet.write(row_num, 22, r.approved_by)
        worksheet.write(row_num, 23, str(r.approved_on) if r.approved_on else '')
        worksheet.write(row_num, 24, r.approver_remark)
        worksheet.write(row_num, 25, r.billable_type)
        worksheet.write(row_num, 26, r.punch_in_timing)
        worksheet.write(row_num, 27, r.punch_out_timing)

    workbook.close()
    output.seek(0)

    response = HttpResponse(
        output.read(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="On_Duty_Requests_{request_status}.xlsx"'
    return response




@login_required
def overtime_report_summary(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    today = now().date()

    # Filter input from request
    from_date = request.GET.get('from_date', today.strftime('%Y-%m-%d'))
    to_date = request.GET.get('to_date', today.strftime('%Y-%m-%d'))
    company = request.GET.get('company', '')
    branch = request.GET.get('branch', '')
    shift_code = request.GET.get('shift_code', '')

    # Base queryset
    queryset = OvertimeReport.objects.filter(attendance_date__range=[from_date, to_date])

    if company:
        queryset = queryset.filter(company__icontains=company)
    if branch:
        queryset = queryset.filter(branch__icontains=branch)
    if shift_code:
        queryset = queryset.filter(shift_code__icontains=shift_code)

    # Total count and grouped count
    total_count = queryset.count()
    status_counts = queryset.values('request_status').annotate(count=Count('id'))

    # Dropdown options
    company_list = OvertimeReport.objects.values_list('company', flat=True).distinct()
    branch_list = OvertimeReport.objects.values_list('branch', flat=True).distinct()
    shift_list = OvertimeReport.objects.values_list('shift_code', flat=True).distinct()

    return render(request, 'hr/overtime_report_summary.html', {
        'active_link': 'overtime_report',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'from_date': from_date,
        'to_date': to_date,
        'selected_company': company,
        'selected_branch': branch,
        'selected_shift_code': shift_code,
        'company_list': company_list,
        'branch_list': branch_list,
        'shift_list': shift_list,
        'status_counts': status_counts,
        'total_count': total_count,
    })


@login_required
def overtime_report_detail(request, request_status):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    today = now().date()

    from_date = request.GET.get('from_date', today.strftime('%Y-%m-%d'))
    to_date = request.GET.get('to_date', today.strftime('%Y-%m-%d'))
    company = request.GET.get('company', '')
    branch = request.GET.get('branch', '')
    shift_code = request.GET.get('shift_code', '')
    full_name = request.GET.get('full_name', '').strip()

    queryset = OvertimeReport.objects.filter(
        attendance_date__range=[from_date, to_date],
    )

    if company:
        queryset = queryset.filter(company__icontains=company)
    if branch:
        queryset = queryset.filter(branch__icontains=branch)
    if shift_code:
        queryset = queryset.filter(shift_code__icontains=shift_code)
    if full_name:
        queryset = queryset.filter(full_name__icontains=full_name)
    if request_status != 'all':
        queryset = queryset.filter(request_status=request_status)

    # Dropdown values for filters
    company_list = OvertimeReport.objects.values_list('company', flat=True).distinct()
    branch_list = OvertimeReport.objects.values_list('branch', flat=True).distinct()
    shift_list = OvertimeReport.objects.values_list('shift_code', flat=True).distinct()

    return render(request, 'hr/overtime_report_detail.html', {
        'active_link': 'overtime_report',
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'records': queryset,
        'request_status': request_status,
        'from_date': from_date,
        'to_date': to_date,
        'selected_company': company,
        'selected_branch': branch,
        'selected_shift': shift_code,
        'selected_full_name': full_name,
        'company_list': company_list,
        'branch_list': branch_list,
        'shift_list': shift_list,
    })


@login_required
def download_overtime_report_excel(request, request_status):
    today = now().date()

    from_date = request.GET.get('from_date', today.strftime('%Y-%m-%d'))
    to_date = request.GET.get('to_date', today.strftime('%Y-%m-%d'))
    company = request.GET.get('company', '')
    branch = request.GET.get('branch', '')
    shift_code = request.GET.get('shift_code', '')
    full_name = request.GET.get('full_name', '').strip()

    queryset = OvertimeReport.objects.filter(attendance_date__range=[from_date, to_date])

    if company:
        queryset = queryset.filter(company__icontains=company)
    if branch:
        queryset = queryset.filter(branch__icontains=branch)
    if shift_code:
        queryset = queryset.filter(shift_code__icontains=shift_code)
    if full_name:
        queryset = queryset.filter(full_name__icontains=full_name)
    if request_status != 'all':
        queryset = queryset.filter(request_status=request_status)

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet("Overtime Report")

    headers = [
        "Attendance Date", "Employee Code", "Full Name", "Employment Status", "Company", "Business Unit", "Department",
        "Designation", "Branch", "Sub Branch", "Attendance Day", "Shift Code", "Shift", "Shift Timings",
        "Punch In", "Punch Out", "Working Hours", "Overtime Hours", "Overtime Minutes",
        "Request Status", "Request By", "Request On", "Pending With", "Approved By", "Approved On", "Approver Remark"
    ]

    # Write header row with formatting
    header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC'})
    worksheet.set_row(0, None, header_format)

    for col, header in enumerate(headers):
        worksheet.write(0, col, header)

    for row, r in enumerate(queryset, start=1):
        worksheet.write(row, 0, r.attendance_date.strftime('%Y-%m-%d') if r.attendance_date else '')
        worksheet.write(row, 1, r.employee_code)
        worksheet.write(row, 2, r.full_name)
        worksheet.write(row, 3, r.employment_status)
        worksheet.write(row, 4, r.company)
        worksheet.write(row, 5, r.business_unit)
        worksheet.write(row, 6, r.department)
        worksheet.write(row, 7, r.designation)
        worksheet.write(row, 8, r.branch)
        worksheet.write(row, 9, r.sub_branch)
        worksheet.write(row, 10, r.attendance_day)
        worksheet.write(row, 11, r.shift_code)
        worksheet.write(row, 12, r.shift)
        worksheet.write(row, 13, r.shift_timings)
        worksheet.write(row, 14, r.punch_in_time)
        worksheet.write(row, 15, r.punch_out_time)
        worksheet.write(row, 16, r.working_hours)
        worksheet.write(row, 17, r.overtime_hours)
        worksheet.write(row, 18, r.overtime_minutes)
        worksheet.write(row, 19, r.request_status)
        worksheet.write(row, 20, r.request_by)
        worksheet.write(row, 21, r.request_on.strftime('%Y-%m-%d') if r.request_on else '')
        worksheet.write(row, 22, r.pending_with)
        worksheet.write(row, 23, r.approved_by)
        worksheet.write(row, 24, r.approved_on.strftime('%Y-%m-%d') if r.approved_on else '')
        worksheet.write(row, 25, r.approver_remark)

    workbook.close()
    output.seek(0)

    response = HttpResponse(output.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = f'attachment; filename="Overtime_Report_{request_status}.xlsx"'
    return response



def short_leave_summary(request):
    today = now().date()

    # Default to current year and month if not provided
    selected_year = int(request.GET.get('year', today.year))
    selected_month = int(request.GET.get('month', today.month))
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')

    short_leaves = ShortLeave.objects.all()

    # Apply filters
    if selected_month:
        short_leaves = short_leaves.filter(attendance_date__month=selected_month)
    if selected_year:
        short_leaves = short_leaves.filter(attendance_date__year=selected_year)
    if selected_company:
        short_leaves = short_leaves.filter(company=selected_company)
    if selected_branch:
        short_leaves = short_leaves.filter(branch=selected_branch)

    # Group by request_status and count
    status_counts = short_leaves.values('request_status').annotate(count=Count('id'))
    # Total count
    total_count = short_leaves.count()
    # Distinct dropdown values
    company_list = ShortLeave.objects.values_list('company', flat=True).distinct().order_by('company')
    branch_list = ShortLeave.objects.values_list('branch', flat=True).distinct().order_by('branch')

    # Month name mapping for template
    months = [
        (1, "January"), (2, "February"), (3, "March"), (4, "April"),
        (5, "May"), (6, "June"), (7, "July"), (8, "August"),
        (9, "September"), (10, "October"), (11, "November"), (12, "December")
    ]

    years = [2023, 2024, 2025]

    context = {
        'active_link': 'short_leave',
        'status_counts': status_counts,
        'total_count': total_count,
        'selected_month': selected_month,
        'selected_year': selected_year,
        'selected_company': selected_company,
        'selected_branch': selected_branch,
        'months': months,
        'years': years,
        'company_list': company_list,
        'branch_list': branch_list,
    }
    return render(request, 'hr/short_leave_summary.html', context)



def short_leave_detail(request, status):
    today = now().date()
    selected_year = int(request.GET.get('year', today.year))
    selected_month = int(request.GET.get('month', today.month))
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')

    queryset = ShortLeave.objects.all()

    # Apply filters
    if selected_month:
        queryset = queryset.filter(attendance_date__month=selected_month)
    if selected_year:
        queryset = queryset.filter(attendance_date__year=selected_year)
    if selected_company:
        queryset = queryset.filter(company=selected_company)
    if selected_branch:
        queryset = queryset.filter(branch=selected_branch)
    if status != "all":
        queryset = queryset.filter(request_status=status)
    # Distinct dropdown values
    company_list = ShortLeave.objects.values_list('company', flat=True).distinct().order_by('company')
    branch_list = ShortLeave.objects.values_list('branch', flat=True).distinct().order_by('branch')

    months = [
        (1, "January"), (2, "February"), (3, "March"), (4, "April"),
        (5, "May"), (6, "June"), (7, "July"), (8, "August"),
        (9, "September"), (10, "October"), (11, "November"), (12, "December")
    ]
    years = [2023, 2024, 2025]

    context = {
        'active_link': 'short_leave',
        'status': status,
        'short_leaves': queryset,
        'selected_month': selected_month,
        'selected_year': selected_year,
        'selected_company': selected_company,
        'selected_branch': selected_branch,
        'months': months,
        'years': years,
        'company_list': company_list,
        'branch_list': branch_list,
    }
    return render(request, 'hr/short_leave_detail.html', context)



def download_short_leave_excel(request, status):
    today = now().date()
    selected_year = int(request.GET.get('year', today.year))
    selected_month = int(request.GET.get('month', today.month))
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')

    queryset = ShortLeave.objects.all()

    if selected_month:
        queryset = queryset.filter(attendance_date__month=selected_month)
    if selected_year:
        queryset = queryset.filter(attendance_date__year=selected_year)
    if selected_company:
        queryset = queryset.filter(company=selected_company)
    if selected_branch:
        queryset = queryset.filter(branch=selected_branch)
    if status != "all":
        queryset = queryset.filter(request_status=status)

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet("Short Leave Report")

    headers = [
        "Employee Code", "Full Name", "Employment Status", "Company", "Business Unit", "Department", "Designation",
        "Branch", "Sub Branch", "Request Type", "Attendance Date", "Attendance Day", "Shift Code", "Shift Timings",
        "Actual In/Out", "Punch In Date", "Punch In Time", "Punch Out Date", "Punch Out Time",
        "Remarks", "Request Status", "Requested By", "Request On", "Pending With", "Approved By",
        "Approved On", "Approver Remark"
    ]

    # Write header row with formatting
    header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC'})
    worksheet.set_row(0, None, header_format)

        # Write header row with formatting
    header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC'})
    worksheet.set_row(0, None, header_format)

    for col, header in enumerate(headers):
        worksheet.write(0, col, header)

    for row, r in enumerate(queryset, start=1):
        worksheet.write(row, 0, r.employee_code)
        worksheet.write(row, 1, r.full_name)
        worksheet.write(row, 2, r.employment_status)
        worksheet.write(row, 3, r.company)
        worksheet.write(row, 4, r.business_unit)
        worksheet.write(row, 5, r.department)
        worksheet.write(row, 6, r.designation)
        worksheet.write(row, 7, r.branch)
        worksheet.write(row, 8, r.sub_branch)
        worksheet.write(row, 9, r.request_type)
        worksheet.write(row, 10, r.attendance_date.strftime('%Y-%m-%d'))
        worksheet.write(row, 11, r.attendance_day)
        worksheet.write(row, 12, r.shift_code)
        worksheet.write(row, 13, r.shift_timings)
        worksheet.write(row, 14, r.actual_punch_in_out or '')
        worksheet.write(row, 15, r.punch_in_date.strftime('%Y-%m-%d') if r.punch_in_date else '')
        worksheet.write(row, 16, r.punch_in_timing)
        worksheet.write(row, 17, r.punch_out_date.strftime('%Y-%m-%d') if r.punch_out_date else '')
        worksheet.write(row, 18, r.punch_out_timing)
        worksheet.write(row, 19, r.remarks or '')
        worksheet.write(row, 20, r.request_status)
        worksheet.write(row, 21, r.request_by)
        worksheet.write(row, 22, r.request_on.strftime('%Y-%m-%d') if r.request_on else '')
        worksheet.write(row, 23, r.pending_with or '')
        worksheet.write(row, 24, r.approved_by or '')
        worksheet.write(row, 25, r.approved_on.strftime('%Y-%m-%d') if r.approved_on else '')
        worksheet.write(row, 26, r.approver_remark or '')

    workbook.close()
    output.seek(0)

    response = HttpResponse(
        output.read(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = 'attachment; filename=short_leave_report.xlsx'
    return response




def helpdesk_ticket_summary(request):
    today = now().date()
    
    # Get filter values from query parameters
    department = request.GET.get('department', '')
    sub_category = request.GET.get('sub_category', '')
    priority = request.GET.get('priority', '')
    selected_month = int(request.GET.get('month', today.month))
    selected_year = int(request.GET.get('year', today.year))

    # Base queryset
    tickets = Helpdesk_Ticket.objects.all()

    # Apply filters
    if department:
        tickets = tickets.filter(department=department)
    if sub_category:
        tickets = tickets.filter(sub_category=sub_category)
    if priority:
        tickets = tickets.filter(priority=priority)
    if selected_month:
        tickets = tickets.filter(raised_on__month=selected_month)
    if selected_year:
        tickets = tickets.filter(raised_on__year=selected_year)

    # Group by status and count
    status_counts = tickets.values('status').annotate(count=Count('id')).order_by('status')
    total_count = tickets.count()

    # Dropdown data
    department_list = Helpdesk_Ticket.objects.values_list('department', flat=True).distinct().order_by('department')
    sub_category_list = Helpdesk_Ticket.objects.values_list('sub_category', flat=True).distinct().order_by('sub_category')
    priority_list = Helpdesk_Ticket.objects.values_list('priority', flat=True).distinct().order_by('priority')

    months = [
        (1, "January"), (2, "February"), (3, "March"), (4, "April"),
        (5, "May"), (6, "June"), (7, "July"), (8, "August"),
        (9, "September"), (10, "October"), (11, "November"), (12, "December")
    ]
    years = [2023, 2024, 2025]

    context = {
        'active_link': 'helpdesk_ticket',
        'status_counts': status_counts,
        'total_count': total_count,
        'department_list': department_list,
        'sub_category_list': sub_category_list,
        'priority_list': priority_list,
        'months': months,
        'years': years,
        'selected_department': department,
        'selected_sub_category': sub_category,
        'selected_priority': priority,
        'selected_month': selected_month,
        'selected_year': selected_year,
    }

    return render(request, 'hr/helpdesk_ticket_summary.html', context)



def helpdesk_ticket_detail(request, status=None):
    today = now().date()

    # Filters from GET
    department = request.GET.get('department', '')
    sub_category = request.GET.get('sub_category', '')
    priority = request.GET.get('priority', '')
    full_name = request.GET.get('full_name', '')
    selected_month = int(request.GET.get('month', today.month))
    selected_year = int(request.GET.get('year', today.year))

    queryset = Helpdesk_Ticket.objects.all()

    # Apply status if passed
    if status and status != 'All':
        queryset = queryset.filter(status=status)

    # Apply filters
    if department:
        queryset = queryset.filter(department=department)
    if sub_category:
        queryset = queryset.filter(sub_category=sub_category)
    if priority:
        queryset = queryset.filter(priority=priority)
    if full_name:
        queryset = queryset.filter(full_name__icontains=full_name)
    if selected_month:
        queryset = queryset.filter(raised_on__month=selected_month)
    if selected_year:
        queryset = queryset.filter(raised_on__year=selected_year)

    # Dropdown data
    department_list = Helpdesk_Ticket.objects.values_list('department', flat=True).distinct().order_by('department')
    sub_category_list = Helpdesk_Ticket.objects.values_list('sub_category', flat=True).distinct().order_by('sub_category')
    priority_list = Helpdesk_Ticket.objects.values_list('priority', flat=True).distinct().order_by('priority')
    months = [
        (1, "January"), (2, "February"), (3, "March"), (4, "April"),
        (5, "May"), (6, "June"), (7, "July"), (8, "August"),
        (9, "September"), (10, "October"), (11, "November"), (12, "December")
    ]
    years = [2023, 2024, 2025]

    context = {
        'active_link': 'helpdesk_ticket',
        'tickets': queryset,
        'status': status,
        'department_list': department_list,
        'sub_category_list': sub_category_list,
        'priority_list': priority_list,
        'months': months,
        'years': years,
        'selected_department': department,
        'selected_sub_category': sub_category,
        'selected_priority': priority,
        'selected_month': selected_month,
        'selected_year': selected_year,
        'selected_full_name': full_name,
    }
    return render(request, 'hr/helpdesk_ticket_detail.html', context)


@login_required
def download_helpdesk_ticket_excel(request, status):
    today = now().date()
    department = request.GET.get('department', '')
    sub_category = request.GET.get('sub_category', '')
    priority = request.GET.get('priority', '')
    full_name = request.GET.get('full_name', '')
    selected_month = int(request.GET.get('month', today.month))
    selected_year = int(request.GET.get('year', today.year))

    queryset = Helpdesk_Ticket.objects.all()

    if status and status != 'All':
        queryset = queryset.filter(status=status)
    if department:
        queryset = queryset.filter(department=department)
    if sub_category:
        queryset = queryset.filter(sub_category=sub_category)
    if priority:
        queryset = queryset.filter(priority=priority)
    if full_name:
        queryset = queryset.filter(full_name__icontains=full_name)
    if selected_month:
        queryset = queryset.filter(raised_on__month=selected_month)
    if selected_year:
        queryset = queryset.filter(raised_on__year=selected_year)

    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet("Helpdesk Tickets")

    headers = [
        "Ticket ID", "Raised On", "Employee Code", "Full Name", "Employment Status", "Company",
        "Business Unit", "Department", "Designation", "Branch", "Sub Branch", "Ticket Details",
        "Category", "Sub Category", "Priority", "Status", "Assigned To", "Pending With",
        "Closed By", "Closed On", "Closed On Time?", "Feedback Rating",
        "Escalated", "Escalated To", "RCA", "Time to Close"
    ]

    for col_num, header in enumerate(headers):
        worksheet.write(0, col_num, header)

    for row_num, t in enumerate(queryset, start=1):
        worksheet.write(row_num, 0, t.ticket_id)
        worksheet.write(row_num, 1, t.raised_on.strftime('%Y-%m-%d %H:%M') if t.raised_on else '')
        worksheet.write(row_num, 2, t.employee_code)
        worksheet.write(row_num, 3, t.full_name)
        worksheet.write(row_num, 4, t.employment_status)
        worksheet.write(row_num, 5, t.company)
        worksheet.write(row_num, 6, t.business_unit)
        worksheet.write(row_num, 7, t.department)
        worksheet.write(row_num, 8, t.designation)
        worksheet.write(row_num, 9, t.branch)
        worksheet.write(row_num, 10, t.sub_branch)
        worksheet.write(row_num, 11, t.ticket_details)
        worksheet.write(row_num, 12, t.category)
        worksheet.write(row_num, 13, t.sub_category)
        worksheet.write(row_num, 14, t.priority)
        worksheet.write(row_num, 15, t.status)
        worksheet.write(row_num, 16, t.assigned_to)
        worksheet.write(row_num, 17, t.pending_with)
        worksheet.write(row_num, 18, t.closed_by)
        worksheet.write(row_num, 19, t.closed_on.strftime('%Y-%m-%d %H:%M') if t.closed_on else '')
        worksheet.write(row_num, 20, t.is_closed_on_time)
        worksheet.write(row_num, 21, t.feedback_rating)
        worksheet.write(row_num, 22, t.was_ticket_escalated)
        worksheet.write(row_num, 23, t.escalated_to)
        worksheet.write(row_num, 24, t.rca)
        worksheet.write(row_num, 25, t.time_to_close)

    workbook.close()
    output.seek(0)

    filename = f"helpdesk_tickets_{status.lower()}.xlsx"
    response = HttpResponse(
        output.read(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename={filename}'
    return response



STATUS_LABELS = {
    'APL | APL': 'All Purpose Leave (APL)',
    'COM | COM': 'Compensation (COM)',
    'CO | CO': 'Compensatory Off (CO)',
    'P | P': 'Present',
    'A | P': '2nd Half Present',
    'HO | HO': 'Holiday (HO)',
    'A | A': 'Absent',
    'WO | WO': 'Week Off (WO)',
    'ML | ML': 'Maternity Leave',
}

@login_required
def mis_report(request):
    today = date.today()
    selected_month = int(request.GET.get('month', today.month))
    selected_year = int(request.GET.get('year', today.year))
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')
    selected_full_name = request.GET.get('full_name', '')

    def safe_df(queryset, columns):
        data = list(queryset)
        return pd.DataFrame(data, columns=columns) if data else pd.DataFrame(columns=columns)

    def load_count_df(model, alias, all_employee_codes, extra_filter=None):
        qs = model.objects.filter(
            attendance_date__month=selected_month,
            attendance_date__year=selected_year,
            employee_code__in=all_employee_codes
        )
        if extra_filter:
            qs = qs.filter(**extra_filter)
        qs = qs.values('employee_code').annotate(**{alias: Count('id')})
        return safe_df(qs, ['employee_code', alias])

    def load_sum_df(model, field_name, alias, all_employee_codes, extra_filter=None):
        qs = model.objects.filter(
            attendance_date__month=selected_month,
            attendance_date__year=selected_year,
            employee_code__in=all_employee_codes
        )
        if extra_filter:
            qs = qs.filter(**extra_filter)
        qs = qs.values('employee_code').annotate(**{alias: Sum(field_name)})
        return safe_df(qs, ['employee_code', alias])

    # ─── Step 1: Build union of all employee_codes across models ─────────────────────
    all_codes = set()
    all_codes.update(AttendanceRegulation.objects.values_list('employee_code', flat=True).distinct())
    all_codes.update(DailyAttendance.objects.values_list('employee_code', flat=True).distinct())
    all_codes.update(DailyCheckIn.objects.values_list('employee_code', flat=True).distinct())
    all_codes.update(OvertimeReport.objects.values_list('employee_code', flat=True).distinct())
    all_codes.update(ShortLeave.objects.values_list('employee_code', flat=True).distinct())
    all_codes.update(Late_Early_Go.objects.values_list('employee_code', flat=True).distinct())
    all_employee_codes = sorted(all_codes)
    base_df = pd.DataFrame(all_employee_codes, columns=['employee_code'])

    # ─── ✅ Step 2: Load model counts using filtered employee codes and month/year ───
    df_att = load_count_df(AttendanceRegulation, 'attendance_regulation', all_employee_codes,{'request_status': 'Approved'})
    df_early = load_count_df(Late_Early_Go, 'late_early_go', all_employee_codes)
    df_duty = load_count_df(On_Duty_Request, 'on_duty_request', all_employee_codes, {'request_status': 'Approved'})
    df_short = load_count_df(ShortLeave, 'short_leave', all_employee_codes, {'request_status': 'Approved'})
    df_ot = load_sum_df(OvertimeReport, 'overtime_minutes', 'overtime_report', all_employee_codes, {'request_status': 'Approved'})

    # ─── Step 3: Merge each onto base_df ─────────────────────────────────────────────
    for df, alias in [(df_att, 'attendance_regulation'), (df_early, 'late_early_go'),
                      (df_duty, 'on_duty_request'), (df_short, 'short_leave'), (df_ot, 'overtime_report')]:
        base_df = base_df.merge(df, on='employee_code', how='left')

    for col in ['attendance_regulation', 'late_early_go', 'on_duty_request', 'short_leave', 'overtime_report']:
        if col in base_df.columns:
            base_df[col] = pd.to_numeric(base_df[col].fillna(0), downcast='integer')

    # ─── Step 4: Metadata from DailyAttendance ────────────────────────────────────────
    meta_fields = ['employee_code', 'full_name', 'employment_status', 'company', 'department', 'designation', 'branch']
    qs_meta = DailyAttendance.objects.filter(employee_code__in=all_employee_codes)
    df_meta = safe_df(qs_meta.values(*meta_fields).distinct(), meta_fields)
    df_meta.drop_duplicates(subset='employee_code', keep='last', inplace=True)

    final = base_df.merge(df_meta, on='employee_code', how='left')
    
    # ─── Step 5: Apply dropdown filters ───────────────────────────────────────────────
    companies = qs_meta.values_list('company', flat=True).distinct().order_by('company')
    branches = qs_meta.values_list('branch', flat=True).distinct().order_by('branch')

    if selected_company:
        final = final[final['company'] == selected_company]
    if selected_branch:
        final = final[final['branch'] == selected_branch]
    if selected_full_name:
        final = final[final['full_name'].str.contains(selected_full_name, case=False, na=False)]

    summary_data = final.fillna('').sort_values('employee_code').to_dict('records')
    months = [(i, datetime(2025, i, 1).strftime('%B')) for i in range(1, 13)]
    years = [2023, 2024, 2025]

    return render(request, 'hr/mis_report.html', {
        'summary_data': summary_data,
        'months': months,
        'years': years,
        'selected_month': selected_month,
        'selected_year': selected_year,
        'selected_company': selected_company,
        'selected_branch': selected_branch,
        'selected_full_name': selected_full_name,
        'companies': companies,
        'branches': branches,
    })



@login_required
def download_mis_report_excel(request):
    today = date.today()
    selected_month = int(request.GET.get('month', today.month))
    selected_year = int(request.GET.get('year', today.year))
    selected_company = request.GET.get('company', '')
    selected_branch = request.GET.get('branch', '')
    selected_full_name = request.GET.get('full_name', '')

    # --- Replicate logic from `mis_report` to get final DataFrame ---
    # Build union of all employee codes
    all_codes = set()
    all_codes.update(AttendanceRegulation.objects.values_list('employee_code', flat=True).distinct())
    all_codes.update(DailyAttendance.objects.values_list('employee_code', flat=True).distinct())
    all_codes.update(DailyCheckIn.objects.values_list('employee_code', flat=True).distinct())
    all_codes.update(OvertimeReport.objects.values_list('employee_code', flat=True).distinct())
    all_codes.update(ShortLeave.objects.values_list('employee_code', flat=True).distinct())
    all_codes.update(Late_Early_Go.objects.values_list('employee_code', flat=True).distinct())
    all_employee_codes = sorted(all_codes)
    base_df = pd.DataFrame(all_employee_codes, columns=['employee_code'])

    def safe_df(queryset, columns):
        data = list(queryset)
        return pd.DataFrame(data, columns=columns) if data else pd.DataFrame(columns=columns)

    def load_count_df(model, alias, extra_filter=None):
        qs = model.objects.filter(
            attendance_date__month=selected_month,
            attendance_date__year=selected_year,
            employee_code__in=all_employee_codes
        )
        if extra_filter:
            qs = qs.filter(**extra_filter)
        qs = qs.values('employee_code').annotate(**{alias: Count('id')})
        return safe_df(qs, ['employee_code', alias])

    def load_sum_df(model, field_name, alias, extra_filter=None):
        qs = model.objects.filter(
            attendance_date__month=selected_month,
            attendance_date__year=selected_year,
            employee_code__in=all_employee_codes
        )
        if extra_filter:
            qs = qs.filter(**extra_filter)
        qs = qs.values('employee_code').annotate(**{alias: Sum(field_name)})
        return safe_df(qs, ['employee_code', alias])

    # Load all required counts and sums
    df_att = load_count_df(AttendanceRegulation, 'attendance_regulation',{'request_status': 'Approved'})
    df_early = load_count_df(Late_Early_Go, 'late_early_go')
    df_duty = load_count_df(On_Duty_Request, 'on_duty_request', {'request_status': 'Approved'})
    df_short = load_count_df(ShortLeave, 'short_leave', {'request_status': 'Approved'})
    df_ot = load_sum_df(OvertimeReport, 'overtime_minutes', 'overtime_report', {'request_status': 'Approved'})

    # Merge all into base_df
    for df, alias in [(df_att, 'attendance_regulation'), (df_early, 'late_early_go'),
                      (df_duty, 'on_duty_request'), (df_short, 'short_leave'), (df_ot, 'overtime_report')]:
        base_df = base_df.merge(df, on='employee_code', how='left')

    for col in ['attendance_regulation', 'late_early_go', 'on_duty_request', 'short_leave', 'overtime_report']:
        base_df[col] = pd.to_numeric(base_df[col].fillna(0), downcast='integer')

    # Metadata from DailyAttendance
    meta_fields = ['employee_code', 'full_name', 'employment_status', 'company', 'department', 'designation', 'branch']
    qs_meta = DailyAttendance.objects.filter(employee_code__in=all_employee_codes)
    df_meta = safe_df(qs_meta.values(*meta_fields).distinct(), meta_fields)
    df_meta.drop_duplicates(subset='employee_code', keep='last', inplace=True)

    final = base_df.merge(df_meta, on='employee_code', how='left')

    # Apply filters
    if selected_company:
        final = final[final['company'] == selected_company]
    if selected_branch:
        final = final[final['branch'] == selected_branch]
    if selected_full_name:
        final = final[final['full_name'].str.contains(selected_full_name, case=False, na=False)]

    # Add summary row
    summary_row = {
        'employee_code': 'TOTAL',
        'full_name': '',
        'employment_status': '',
        'company': '',
        'department': '',
        'designation': '',
        'branch': '',
        'attendance_regulation': final['attendance_regulation'].sum(),
        'late_early_go': final['late_early_go'].sum(),
        'on_duty_request': final['on_duty_request'].sum(),
        'overtime_report': final['overtime_report'].sum(),
        'short_leave': final['short_leave'].sum(),
    }
    final = pd.concat([final, pd.DataFrame([summary_row])], ignore_index=True)

    # --- Convert overtime_report to overtime_hours (and drop overtime_report) ---
    def format_minutes_to_hhmm(mins):
        try:
            mins = int(mins)
            if mins <= 0 or pd.isna(mins):
                return "0 hr 0 min"
            return f"{mins // 60} hr {mins % 60} min"
        except Exception:
            return "0 hr 0 min"

    final['overtime_hours'] = final['overtime_report'].apply(format_minutes_to_hhmm)
    final.drop(columns=['overtime_report'], inplace=True)

    # Optionally: reorder columns for better Excel output (use your preferred order)
    cols = ['employee_code', 'full_name', 'department', 'designation', 'company', 'branch', 'employment_status',
            'attendance_regulation', 'late_early_go', 'on_duty_request', 'overtime_hours', 'short_leave']
    final = final[[c for c in cols if c in final.columns]]

    # Export to Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        final.to_excel(writer, index=False, sheet_name='MIS Report')

    output.seek(0)
    response = HttpResponse(output.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename=MIS_Report.xlsx'
    return response





@login_required
def attendance_pivot_report(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)
    today = date.today()
    first_day = today.replace(day=1)
    last_day = date(today.year, today.month, calendar.monthrange(today.year, today.month)[1])

    # GET values and defaults for filters
    from_str = request.GET.get('from_date', first_day.strftime('%Y-%m-%d'))
    to_str = request.GET.get('to_date', last_day.strftime('%Y-%m-%d'))
    selected_company = request.GET.get('company', 'All')
    selected_branch = request.GET.get('branch', 'Solapur')
    selected_department = request.GET.get('department', 'All')
    selected_sub_department = request.GET.get('sub_department', 'All')
    selected_shift_code = request.GET.get('shift_code', 'All')
    month = request.GET.get('month', '')
    year = request.GET.get('year', '')

    # --- Main QuerySet for the report data ---
    qs = DailyAttendance.objects.all()
    date_label = ""

    # Date Filtering
    if from_str and to_str:
        try:
            from_date = datetime.strptime(from_str, '%Y-%m-%d').date()
            to_date = datetime.strptime(to_str, '%Y-%m-%d').date()
            qs = qs.filter(attendance_date__range=(from_date, to_date))
            date_label = f"{from_date.strftime('%d-%b-%Y')} to {to_date.strftime('%d-%b-%Y')}"
        except Exception:
            from_date = first_day
            to_date = last_day
            qs = qs.filter(attendance_date__month=today.month, attendance_date__year=today.year)
            date_label = today.strftime('%B %Y')
    elif month and year:
        try:
            month = int(month)
            year = int(year)
            qs = qs.filter(attendance_date__month=month, attendance_date__year=year)
            from_date = date(year, month, 1)
            to_date = date(year, month, calendar.monthrange(year, month)[1])
            date_label = f"{from_date.strftime('%B %Y')}"
        except Exception:
            from_date = first_day
            to_date = last_day
            qs = qs.filter(attendance_date__month=today.month, attendance_date__year=today.year)
            date_label = today.strftime('%B %Y')
    else:
        from_date = first_day
        to_date = last_day
        qs = qs.filter(attendance_date__month=today.month, attendance_date__year=today.year)
        date_label = today.strftime('%B %Y')

    # Apply other filters to the main queryset
    if selected_branch and selected_branch != 'All':
        qs = qs.filter(branch=selected_branch)
    if selected_company and selected_company != 'All':
        qs = qs.filter(company=selected_company)
    if selected_department and selected_department != 'All':
        qs = qs.filter(department=selected_department)
    if selected_sub_department and selected_sub_department != 'All':
        qs = qs.filter(sub_department=selected_sub_department)
    if selected_shift_code and selected_shift_code != 'All':
        qs = qs.filter(shift_code=selected_shift_code)

    # Aggregate data for the pivot table
    pivot_qs = qs.values('status_in_out', 'attendance_date').annotate(count=Count('id')).order_by('status_in_out', 'attendance_date')

    counts = defaultdict(dict)
    all_dates = set()
    for row in pivot_qs:
        status = row['status_in_out']
        day_obj = row['attendance_date']
        all_dates.add(day_obj)
        counts[status][day_obj] = row['count']

    # ========================== KEY FIX IS HERE ==========================
    # This helper function checks if a model has a field before trying to filter on it.
    # This prevents the 500 Internal Server Error if a model is missing a field.
    def apply_safe_filters(queryset):
        model_fields = [f.name for f in queryset.model._meta.get_fields()]
        
        if 'company' in model_fields and selected_company and selected_company != 'All':
            queryset = queryset.filter(company=selected_company)
        if 'branch' in model_fields and selected_branch and selected_branch != 'All':
            queryset = queryset.filter(branch=selected_branch)
        if 'department' in model_fields and selected_department and selected_department != 'All':
            queryset = queryset.filter(department=selected_department)
        if 'sub_department' in model_fields and selected_sub_department and selected_sub_department != 'All':
            queryset = queryset.filter(sub_department=selected_sub_department)
        if 'shift_code' in model_fields and selected_shift_code and selected_shift_code != 'All':
            queryset = queryset.filter(shift_code=selected_shift_code)
            
        return queryset
    # ========================== END OF FIX ==========================

    # ---- On Duty Approved (now uses the safe filtering function)
    od_qs = On_Duty_Request.objects.filter(request_status="Approved", attendance_date__range=(from_date, to_date))
    od_qs = apply_safe_filters(od_qs)
    od_counts_by_date = dict(od_qs.values('attendance_date').annotate(count=Count('id')).values_list('attendance_date', 'count'))
    for dt, c in od_counts_by_date.items():
        all_dates.add(dt); counts["ON DUTY"][dt] = c

    # ---- Attendance Regulation Approved (now uses the safe filtering function)
    reg_qs = AttendanceRegulation.objects.filter(request_status="Approved", attendance_date__range=(from_date, to_date))
    reg_qs = apply_safe_filters(reg_qs)
    reg_counts_by_date = dict(reg_qs.values('attendance_date').annotate(count=Count('id')).values_list('attendance_date', 'count'))
    for dt, c in reg_counts_by_date.items():
        all_dates.add(dt); counts["ATT REG"][dt] = c

    # ---- Short Leave Approved (now uses the safe filtering function)
    sl_qs = ShortLeave.objects.filter(request_status="Approved", attendance_date__range=(from_date, to_date))
    sl_qs = apply_safe_filters(sl_qs)
    sl_counts_by_date = dict(sl_qs.values('attendance_date').annotate(count=Count('id')).values_list('attendance_date', 'count'))
    for dt, c in sl_counts_by_date.items():
        all_dates.add(dt); counts["SL APPROVED"][dt] = c

    # Prepare data for rendering
    sorted_dates = sorted(all_dates)
    date_labels = [f"{d.day}-{d.strftime('%b')}" for d in sorted_dates]
    custom_order = [
        "P | P", "A | A", "WO | WO", "APL | APL", "COM | COM", "CO | CO", "P | A", "A | P", "P | APL", "APL | P",
        "A | APL", "APL | A", "P | CO", "CO | P", "SPL | SPL", "HO | HO", "ON DUTY", "ATT REG", "SL APPROVED",
    ]
    status_labels = {
        "P | P": "Present", "A | A": "Absent", "WO | WO": "Week Off", "APL | APL": "All Purpose Leave", "COM | COM": "Comp Off",
        "CO | CO": "Comp Off", "HO | HO": "Holiday", "ON DUTY": "On Duty Approved", "ATT REG": "Attendance Regularization Approved",
        "SL APPROVED": "Short Leave Approved", "P | A": "First half Present", "A | P": "Second half Present",
        "APL | A": "First half All Purpose Leave", "A | APL": "Second half All Purpose Leave",
    }
    all_statuses = set(counts.keys())
    ordered_statuses = [status for status in custom_order if status in all_statuses]
    remaining_statuses = sorted(list(all_statuses - set(custom_order)))
    final_statuses = ordered_statuses + remaining_statuses

    table_data = []
    for status in final_statuses:
        row = [counts[status].get(dt, "") for dt in sorted_dates]
        table_data.append({'status': status, 'status_label': status_labels.get(status, ""), 'counts': row})

    exclude_statuses = {"ON DUTY", "ATT REG", "SL APPROVED"}
    grand_total = []
    for dt in sorted_dates:
        total = sum(counts[status].get(dt, 0) or 0 for status in final_statuses if status not in exclude_statuses and status in counts)
        grand_total.append(total)

    # --- DYNAMIC FILTER OPTIONS FOR DROPDOWNS (for initial page load) ---
    companies = list(DailyAttendance.objects.order_by('company').values_list('company', flat=True).distinct())
    branches = list(DailyAttendance.objects.order_by('branch').values_list('branch', flat=True).distinct())

    dept_qs = DailyAttendance.objects.all()
    if selected_branch and selected_branch != 'All':
        dept_qs = dept_qs.filter(branch=selected_branch)
    departments = list(dept_qs.order_by('department').values_list('department', flat=True).distinct())

    sub_dept_qs = DailyAttendance.objects.all()
    if selected_branch and selected_branch != 'All':
        sub_dept_qs = sub_dept_qs.filter(branch=selected_branch)
    if selected_department and selected_department != 'All':
        sub_dept_qs = sub_dept_qs.filter(department=selected_department)
    sub_departments = list(sub_dept_qs.order_by('sub_department').values_list('sub_department', flat=True).distinct())

    shift_qs = DailyAttendance.objects.all()
    if selected_branch and selected_branch != 'All':
        shift_qs = shift_qs.filter(branch=selected_branch)
    if selected_department and selected_department != 'All':
        shift_qs = shift_qs.filter(department=selected_department)
    if selected_sub_department and selected_sub_department != 'All':
        shift_qs = shift_qs.filter(sub_department=selected_sub_department)
    shift_codes = list(shift_qs.order_by('shift_code').values_list('shift_code', flat=True).distinct())
    
    months_list = [(i, date(1900, i, 1).strftime('%B')) for i in range(1, 13)]
    years_list = list(range(today.year - 3, today.year + 2))

    context = {
        'table_data': table_data,
        'days': date_labels,
        'date_label': date_label,
        'grand_total': grand_total,
        'selected_month': month or today.month,
        'selected_year': year or today.year,
        'from_date': from_str,
        'to_date': to_str,
        'months_list': months_list,
        'years_list': years_list,
        'companies': ['All'] + companies,
        'branches': ['All'] + branches,
        'departments': ['All'] + departments,
        'sub_departments': ['All'] + sub_departments,
        'shift_codes': ['All'] + shift_codes,
        'selected_branch': selected_branch,
        'selected_company': selected_company,
        'selected_department': selected_department,
        'selected_sub_department': selected_sub_department,
        'selected_shift_code': selected_shift_code,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'show_admin_panel': show_admin_panel,
    }
    return render(request, 'hr/attendance_pivot_report.html', context)


# ===================================================================
#               NEW API VIEWS FOR DYNAMIC FILTERS
# ===================================================================

@login_required
def get_departments(request):
    branch = request.GET.get('branch')
    qs = DailyAttendance.objects.all()
    if branch and branch != 'All':
        qs = qs.filter(branch=branch)
    departments = list(qs.order_by('department').values_list('department', flat=True).distinct())
    return JsonResponse({'departments': departments})

@login_required
def get_sub_departments(request):
    branch = request.GET.get('branch')
    department = request.GET.get('department')
    
    qs = DailyAttendance.objects.all()
    if branch and branch != 'All':
        qs = qs.filter(branch=branch)
    if department and department != 'All':
        qs = qs.filter(department=department)
    
    sub_departments = list(qs.order_by('sub_department').values_list('sub_department', flat=True).distinct())
    return JsonResponse({'sub_departments': sub_departments})

@login_required
def get_shift_codes(request):
    branch = request.GET.get('branch')
    department = request.GET.get('department')
    sub_department = request.GET.get('sub_department')

    qs = DailyAttendance.objects.all()
    if branch and branch != 'All':
        qs = qs.filter(branch=branch)
    if department and department != 'All':
        qs = qs.filter(department=department)
    if sub_department and sub_department != 'All':
        qs = qs.filter(sub_department=sub_department)
        
    shift_codes = list(qs.order_by('shift_code').values_list('shift_code', flat=True).distinct())
    return JsonResponse({'shift_codes': shift_codes})


@login_required
def attendance_pivot_excel(request):
    today = date.today()
    first_day = today.replace(day=1)
    last_day = today.replace(day=calendar.monthrange(today.year, today.month)[1])
    
    # Get values from request, including all filters
    from_str = request.GET.get('from_date', first_day.strftime('%Y-%m-%d'))
    to_str = request.GET.get('to_date', last_day.strftime('%Y-%m-%d'))
    company = request.GET.get('company', 'All')
    branch = request.GET.get('branch', 'Solapur')
    department = request.GET.get('department', 'All')
    sub_department = request.GET.get('sub_department', 'All')
    shift_code = request.GET.get('shift_code', 'All')
    month = request.GET.get('month', '')
    year = request.GET.get('year', '')

    # --- Main QuerySet for the report data ---
    qs = DailyAttendance.objects.all()

    # Date Filtering
    if from_str and to_str:
        try:
            from_date = datetime.strptime(from_str, '%Y-%m-%d').date()
            to_date = datetime.strptime(to_str, '%Y-%m-%d').date()
            qs = qs.filter(attendance_date__range=(from_date, to_date))
        except Exception:
            from_date = first_day
            to_date = last_day
            qs = qs.filter(attendance_date__month=today.month, attendance_date__year=today.year)
    elif month and year:
        try:
            month = int(month)
            year = int(year)
            qs = qs.filter(attendance_date__month=month, attendance_date__year=year)
            from_date = date(year, month, 1)
            to_date = date(year, month, calendar.monthrange(year, month)[1])
        except Exception:
            from_date = first_day
            to_date = last_day
            qs = qs.filter(attendance_date__month=today.month, attendance_date__year=today.year)
    else:
        from_date = first_day
        to_date = last_day
        qs = qs.filter(attendance_date__month=today.month, attendance_date__year=today.year)

    # Apply all other filters to the main queryset
    if company and company != 'All':
        qs = qs.filter(company=company)
    if branch and branch != 'All':
        qs = qs.filter(branch=branch)
    if department and department != 'All':
        qs = qs.filter(department=department)
    if sub_department and sub_department != 'All':
        qs = qs.filter(sub_department=sub_department)
    if shift_code and shift_code != 'All':
        qs = qs.filter(shift_code=shift_code)

    # Aggregate data for the pivot table
    pivot_qs = qs.values('status_in_out', 'attendance_date').annotate(count=Count('id')).order_by('status_in_out', 'attendance_date')
    counts = defaultdict(dict)
    all_dates = set()
    for row in pivot_qs:
        status = row['status_in_out']
        day_obj = row['attendance_date']
        all_dates.add(day_obj)
        counts[status][day_obj] = row['count']

    # ========================== KEY FIX IS HERE ==========================
    # Helper function to safely apply filters to related models, preventing crashes.
    def apply_safe_filters(queryset):
        model_fields = [f.name for f in queryset.model._meta.get_fields()]
        
        if 'company' in model_fields and company and company != 'All':
            queryset = queryset.filter(company=company)
        if 'branch' in model_fields and branch and branch != 'All':
            queryset = queryset.filter(branch=branch)
        if 'department' in model_fields and department and department != 'All':
            queryset = queryset.filter(department=department)
        if 'sub_department' in model_fields and sub_department and sub_department != 'All':
            queryset = queryset.filter(sub_department=sub_department)
        if 'shift_code' in model_fields and shift_code and shift_code != 'All':
            queryset = queryset.filter(shift_code=shift_code)
            
        return queryset
    # ========================== END OF FIX ==========================

    # --- On Duty Approved (with safe filters applied) ---
    od_qs = On_Duty_Request.objects.filter(request_status="Approved", attendance_date__range=(from_date, to_date))
    od_qs = apply_safe_filters(od_qs)
    od_counts_by_date = dict(od_qs.values('attendance_date').annotate(count=Count('id')).values_list('attendance_date', 'count'))
    for dt, c in od_counts_by_date.items():
        all_dates.add(dt)
        counts["ON DUTY"][dt] = c

    # --- Attendance Regulation Approved (with safe filters applied) ---
    reg_qs = AttendanceRegulation.objects.filter(request_status="Approved", attendance_date__range=(from_date, to_date))
    reg_qs = apply_safe_filters(reg_qs)
    reg_counts_by_date = dict(reg_qs.values('attendance_date').annotate(count=Count('id')).values_list('attendance_date', 'count'))
    for dt, c in reg_counts_by_date.items():
        all_dates.add(dt)
        counts["ATT REG"][dt] = c

    # --- Short Leave Approved (with safe filters applied) ---
    sl_qs = ShortLeave.objects.filter(request_status="Approved", attendance_date__range=(from_date, to_date))
    sl_qs = apply_safe_filters(sl_qs)
    sl_counts_by_date = dict(sl_qs.values('attendance_date').annotate(count=Count('id')).values_list('attendance_date', 'count'))
    for dt, c in sl_counts_by_date.items():
        all_dates.add(dt)
        counts["SL APPROVED"][dt] = c

    # --- Prepare data for Excel rendering ---
    sorted_dates = sorted(list(all_dates))
    date_labels = [f"{d.day}-{d.strftime('%b')}" for d in sorted_dates]
    custom_order = [
        "P | P","A | A","WO | WO","APL | APL","COM | COM","CO | CO","P | A","A | P","P | APL","APL | P","A | APL","APL | A","P | CO","CO | P",
        "SPL | SPL","HO | HO","ON DUTY","ATT REG","SL APPROVED"
    ]
    status_labels = {
        "P | P": "Present","A | A": "Absent","WO | WO": "Week Off","APL | APL": "All Purpose Leave","COM | COM": "Comp Off",
        "CO | CO": "Comp Off","HO | HO": "Holiday","ON DUTY": "On Duty Approved","ATT REG": "Attendance Regularization Approved",
        "SL APPROVED": "Short Leave Approved","P | A": "First half Present","A | P": "Second half Present","APL | A": "First half All Purpose Leave",
        "A | APL": "Second half All Purpose Leave",
    }
    all_statuses = set(counts.keys())
    ordered_statuses = [status for status in custom_order if status in all_statuses]
    remaining_statuses = sorted(list(all_statuses - set(custom_order)))
    final_statuses = ordered_statuses + remaining_statuses

    # --- Create Excel file in memory ---
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet('Attendance Pivot')

    # Define formats
    bold = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter'})
    center = workbook.add_format({'align': 'center', 'valign': 'vcenter'})
    total_format = workbook.add_format({'bold': True, 'align': 'center', 'valign': 'vcenter', 'bg_color': '#D9EAD3'})

    # Write Header
    worksheet.write(0, 0, "Status (Label)", bold)
    for col, day in enumerate(date_labels, 1):
        worksheet.write(0, col, day, bold)

    # Write data rows
    for row_idx, status in enumerate(final_statuses, 1):
        label = status_labels.get(status, "")
        status_combined = f"{status} ({label})" if label else status
        worksheet.write(row_idx, 0, status_combined, center)
        for col_idx, dt in enumerate(sorted_dates, 1):
            worksheet.write(row_idx, col_idx, counts[status].get(dt, ""))

    # Write Grand Total row
    total_row = len(final_statuses) + 1
    worksheet.write(total_row, 0, "Grand Total", total_format)
    exclude_statuses = {"ON DUTY", "ATT REG", "SL APPROVED"}
    for col_idx, dt in enumerate(sorted_dates, 1):
        total = sum(
            counts[status].get(dt, 0) or 0
            for status in final_statuses
            if status not in exclude_statuses and status in counts
        )
        worksheet.write(total_row, col_idx, total, total_format)

    # Finalize workbook
    worksheet.set_column('A:A', 35) # Increased width for label
    worksheet.freeze_panes(1, 1)
    workbook.close()
    output.seek(0)

    # Create HTTP response
    filename = f"Attendance_Pivot_{from_str}_to_{to_str}.xlsx"
    response = HttpResponse(
        output.getvalue(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response

















































# http://192.168.1.156:56/data

def fetch_api_data(request):
    base_api_url = "http://192.168.1.156:56/data"
    query_params = {}

    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    # Collect filter parameters from GET request
    start_date = request.GET.get('start_date')
    end_date = request.GET.get('end_date')
    employee_name = request.GET.get('employee_name')
    company_name = request.GET.get('company_name')
    shift = request.GET.get('shift')

    if start_date and end_date:
        query_params['start_date'] = start_date
        query_params['end_date'] = end_date
    if employee_name:
        query_params['employee_name'] = employee_name
    if company_name:
        query_params['company_name'] = company_name
    if shift:
        query_params['shift'] = shift

    # Convert query_params to a query string and make API request
    response = requests.get(base_api_url, params=query_params)
    try:
        response.raise_for_status()  # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        data = response.json().get('data', [])
        distinct_shifts = set(item.get('Shift') for item in data if item.get('Shift'))
        total_records = len(data)
        total_ot_time = sum(item.get('OT', 0) for item in data)
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        data = []
        distinct_shifts = set()
        total_records = 0
        total_ot_time = 0

    return render(request, 'hr/contract_employee_data.html', {
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'data': data,
        'total_records': total_records,
        'total_ot_time': total_ot_time,
        'filters': {
            'start_date': start_date,
            'end_date': end_date,
            'employee_name': employee_name,
            'company_name': company_name,
            'shift': shift
        },
        'distinct_shifts': distinct_shifts
    })









# views.py
import plotly.express as px
import pandas as pd
from django.shortcuts import render
from django.db.models import Count, Q
from .models import DailyAttendance
from datetime import datetime

def attendance_report(request):
    # Default to today's date for filtering if no date is provided
    today = datetime.today().date()
    start_date = request.GET.get('start_date', str(today))
    end_date = request.GET.get('end_date', str(today))

    # Filter logic as before
    query = DailyAttendance.objects.filter(
        attendance_date__range=[start_date, end_date]
    )

    # Fetching distinct companies, departments, and branches
    companies = query.order_by('company').values_list('company', flat=True).distinct()
    departments = query.order_by('department').values_list('department', flat=True).distinct()
    branches = query.order_by('branch').values_list('branch', flat=True).distinct()

    # Generating dummy data for pie and bar charts
    df = pd.DataFrame(list(query.values('branch', 'status_in_out')))
    if not df.empty:
        pie_chart = generate_chart(df, 'pie', names='branch', values='status_in_out', title='Attendance by Branch and Status')
        bar_chart = generate_chart(df, 'bar', x='branch', y='status_in_out', title='Total Employees by Status and Branch')
    else:
        pie_chart = "No data available"
        bar_chart = "No data available"

    return render(request, 'hr/attendance_report.html', {
        'pie_chart': pie_chart,
        'bar_chart': bar_chart,
        'start_date': start_date,
        'end_date': end_date,
        'companies': list(companies),
        'departments': list(departments),
        'branches': list(branches)
    })

def generate_chart(df, chart_type, **kwargs):
    if chart_type == 'pie':
        fig = px.pie(df, names=kwargs['names'], values=kwargs['values'], title=kwargs['title'])
    elif chart_type == 'bar':
        fig = px.bar(df, x=kwargs['x'], y=kwargs['y'], title=kwargs['title'])
    return fig.to_html(full_html=False)








# ===================================================================
#               EMPLOYEE JOINING 
# ===================================================================


@login_required
def employee_joining_create(request):
    if not (is_hr(request.user) or request.user.is_superuser):
        messages.error(request, "You do not have permission to create Employee Joining records.")
        return redirect("indexpage")

    if request.method == "POST":
        form = EmployeeJoiningForm(request.POST)
        if form.is_valid():
            obj = form.save(commit=False)
            obj.hr_user = request.user
            obj.status = "hr_submitted"
            obj.hr_submitted_at = timezone.now()
            obj.save()
            # 🔔 send email to all IT_User group members
            try:
                sent_count = send_employee_joining_to_it_users(obj, request)
                logger.info( "EmployeeJoining (id=%s) email sent to %s IT users.", obj.pk, sent_count,   )
            except Exception as e:
                logger.exception("Failed to send IT notification email for EmployeeJoining id=%s: %s", obj.pk,  e, )

            logger.info("EmployeeJoining (id=%s, emp_id=%s) created by HR user '%s'.", obj.pk, obj.employee_id,request.user.username,)
            messages.success(request,f"Employee joining record created for {obj.employee_name} ({obj.employee_id}), and an email notification has been sent to the IT team.")
            return redirect("employee_joining_list")
        else:
            logger.error("EmployeeJoiningForm invalid for HR user='%s' | errors=%s", request.user.username,
                form.errors.as_json(), )
            messages.error(request, "Please correct the errors below.")
    else:
        form = EmployeeJoiningForm()
        logger.info("HR user '%s' opened Employee Joining create form.", request.user.username)
    return render( request, "new_joining/employee_joining_form.html",{"form": form, "is_edit": False, "object": None}, )




@login_required
def employee_joining_list(request):
    # ---- Permission check ----
    if not request.user.has_perm("HR.view_employeejoining"):
        messages.error(request, "You do not have permission to view Employee Joining records.")
        logger.warning( "User='%s' tried to access Employee Joining list without permission.", request.user.username, )
        return redirect("indexpage")

    search     = (request.GET.get("search") or "").strip()
    company    = request.GET.get("company") or ""
    location   = request.GET.get("location") or ""
    department = request.GET.get("department") or ""
    status     = request.GET.get("status") or ""   # new (optional) filter

    qs = EmployeeJoining.objects.all().order_by("-date_of_joining", "employee_name")

    if search:
        qs = qs.filter(
            Q(employee_name__icontains=search)
            | Q(employee_id__icontains=search)
            | Q(email_id__icontains=search)
        )
    if company:
        qs = qs.filter(company=company)
    if location:
        qs = qs.filter(location=location)
    if department:
        qs = qs.filter(department=department)
    if status:
        qs = qs.filter(status=status)

    # ---- Logging filters + result count ----
    logger.info(
        "[EmployeeJoiningList] user='%s' search='%s' company='%s' location='%s' "
        "department='%s' status='%s' result_count=%s",
        request.user.username,
        search,
        company,
        location,
        department,
        status,
        qs.count(),
    )

    paginator   = Paginator(qs, 25)  # 25 rows per page
    page_number = request.GET.get("page")
    page_obj    = paginator.get_page(page_number)

    context = {
        "page_obj": page_obj,
        "search": search,
        "company": company,
        "location": location,
        "department": department,
        "status": status,
        "company_choices": EmployeeJoining.COMPANY_CHOICES,
        "location_choices": EmployeeJoining.LOCATION_CHOICES,
        "department_choices": EmployeeJoining.DEPARTMENT_CHOICES,
        "status_choices": EmployeeJoining.STATUS_CHOICES,  # for dropdown if needed

        # role flags for template (show/hide action buttons)
        "is_hr": is_hr(request.user),
        "is_it_user": is_it_user(request.user),
        "is_it_approver": is_it_approver(request.user),
    }
    return render(request, "new_joining/employee_joining_list.html", context)


@login_required
def employee_joining_it_update(request, pk):
    obj = get_object_or_404(EmployeeJoining, pk=pk)
    if not (is_it_user(request.user) or request.user.is_superuser):
        messages.error(request, "You are not allowed to update IT infra details.")
        return redirect("employee_joining_list")

    if obj.status not in ("hr_submitted", "it_in_progress", "pending_approval"):
        messages.error(request, "This record is not in a stage where IT can update it.")
        return redirect("employee_joining_detail", pk=obj.pk)

    if request.method == "POST":
        form = EmployeeJoiningITForm(request.POST, instance=obj)
        if form.is_valid():
            obj = form.save(commit=False)
            obj.it_user = request.user
            obj.it_completed_at = timezone.now()
            obj.status = "pending_approval"
            obj.save()
            # 🔔 email approvers
            try:
                sent_count = send_employee_joining_to_approvers(obj, request)
                logger.info("EmployeeJoining (id=%s) approval email sent to %s IT approvers.", obj.pk, sent_count, )
            except Exception as e:
                logger.exception("Failed to send approval email for EmployeeJoining id=%s: %s", obj.pk,  e, )
            logger.info("EmployeeJoining (id=%s) IT details updated by '%s'; status -> pending_approval.",
                obj.pk, request.user.username,  )
            messages.success(request, "IT infra details updated and sent for approval.")
            return redirect("employee_joining_detail", pk=obj.pk)
        else:
            logger.error("EmployeeJoiningITForm invalid for IT user='%s' | errors=%s",request.user.username,
                         form.errors.as_json(), )
            messages.error(request, "Please correct the errors below.")
    else:
        form = EmployeeJoiningITForm(instance=obj)
        logger.info("IT user '%s' opened IT update form for EmployeeJoining id=%s.",request.user.username, obj.pk, )
    return render(request,"new_joining/employee_joining_it_form.html", {"form": form, "object": obj},)
    
    

@login_required
def employee_joining_approve(request, pk):
    obj = get_object_or_404(EmployeeJoining, pk=pk)
    if not (request.user.is_superuser or is_it_approver(request.user)):
        messages.error(request, "You are not allowed to approve Employee Joining records.")
        return redirect("employee_joining_list")

    if obj.status != "pending_approval":
        messages.error(request, "This record is not pending approval.")
        return redirect("employee_joining_detail", pk=obj.pk)

    if request.method == "POST":
        form = EmployeeJoiningApprovalForm(request.POST, instance=obj)
        action = request.POST.get("action")  # expected 'approve' or 'reject'

        if form.is_valid() and action in ("approve", "reject"):
            obj = form.save(commit=False)
            obj.it_approver = request.user
            obj.approved_at = timezone.now()
            obj.status = "approved" if action == "approve" else "rejected"
            obj.save()
            # 🔔 send mail to Emp_joining_HR + IT_User groups
            try:
                sent_count = send_employee_joining_status_to_hr_and_it(obj, request)
                logger.info("EmployeeJoining (id=%s) final status '%s' emailed to %s HR/IT recipients.", obj.pk,obj.status,
                    sent_count,  )
            except Exception as e:
                logger.exception("Failed to send final status email for EmployeeJoining id=%s: %s",obj.pk, e, )
            logger.info("EmployeeJoining (id=%s) %s by '%s'.", obj.pk, obj.status,request.user.username,   )
            messages.success(request,f"Employee joining has been {obj.status}, and an email has been sent to the IT and HR teams.")
            return redirect("employee_joining_detail", pk=obj.pk)
        else:
            logger.error("EmployeeJoiningApprovalForm invalid or bad action for approver '%s' | errors=%s | action=%s",
                request.user.username,form.errors.as_json(), action, )
            messages.error(request, "Please correct the errors below.")
    else:
        form = EmployeeJoiningApprovalForm(instance=obj)
        logger.info( "Approver '%s' opened approval form for EmployeeJoining id=%s.", request.user.username,
            obj.pk, )
    return render(request,"new_joining/employee_joining_approve_form.html",  {"form": form, "obj": obj},)





@login_required
def employee_joining_edit(request, pk):
    # ---- Permission check ----
    if not request.user.has_perm('HR.change_employeejoining'):
        messages.error(request, "You do not have permission to edit Employee Joining records.")
        logger.warning("User='%s' tried to edit Employee Joining record pk=%s without permission.",request.user.username, pk,)
        return redirect('indexpage')
    obj = get_object_or_404(EmployeeJoining, pk=pk)
    if request.method == "POST":
        form = EmployeeJoiningForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            logger.info("EmployeeJoining record pk=%s updated by user='%s'.",
                obj.pk, request.user.username, )
            messages.success(request, "Employee joining record updated successfully.")
            return redirect("employee_joining_list")
        else:
            logger.error("EmployeeJoiningForm invalid for pk=%s by user='%s' | errors=%s",
                pk, request.user.username,form.errors.as_json(), )
            messages.error(request, "Please correct the errors below.")
    else:
        logger.info("User='%s' opened edit form for EmployeeJoining pk=%s.",request.user.username, pk, )
        form = EmployeeJoiningForm(instance=obj)
    return render(request, "new_joining/employee_joining_form.html", {"form": form, "is_edit": True,   "object": obj,   }, )
    

@login_required
def employee_joining_detail(request, pk):
    # ---- Permission check ----
    if not request.user.has_perm('HR.view_employeejoining'):
        messages.error(request, "You do not have permission to view Employee Joining records.")
        logger.warning("User='%s' tried to view Employee Joining detail pk=%s without permission.", request.user.username,pk, )
        return redirect('indexpage')
    obj = get_object_or_404(EmployeeJoining, pk=pk)
    logger.info("User='%s' viewed Employee Joining detail pk=%s.", request.user.username, pk,)
    return render(request,"new_joining/employee_joining_detail.html",{"object": obj},)


@login_required
def employee_joining_delete(request, pk):
    # ---- Permission check ----
    if not request.user.has_perm('HR.delete_employeejoining'):
        messages.error(request, "You do not have permission to delete Employee Joining records.")
        logger.warning("User='%s' tried to delete EmployeeJoining pk=%s without permission.",request.user.username, pk, )
        return redirect("indexpage")
    obj = get_object_or_404(EmployeeJoining, pk=pk)
    if request.method == "POST":
        emp_str = f"{obj.employee_id} - {obj.employee_name}"
        obj.delete()
        logger.info("User='%s' deleted EmployeeJoining pk=%s (%s).",request.user.username, pk, emp_str,)
        messages.success(request, f"Employee joining record '{emp_str}' deleted successfully.")
        return redirect("employee_joining_list")
    # GET → show confirm page
    logger.info("User='%s' opened delete confirmation for EmployeeJoining pk=%s.", request.user.username, pk,)
    return render(request,"new_joining/employee_joining_confirm_delete.html",{"object": obj},)




@login_required
def employee_joining_export_excel(request):
    """
    Export EmployeeJoining records (with same filters as list) to Excel.
    """

    # ---- Permission check ----
    if not request.user.has_perm('HR.view_employeejoining'):
        messages.error(request, "You do not have permission to export Employee Joining records.")
        logger.warning(
            "User='%s' tried to export EmployeeJoining records without permission.",
            request.user.username,
        )
        return redirect("indexpage")

    qs = EmployeeJoining.objects.all().order_by("-date_of_joining", "employee_name")

    search     = (request.GET.get("search") or "").strip()
    company    = request.GET.get("company") or ""
    location   = request.GET.get("location") or ""
    department = request.GET.get("department") or ""

    if search:
        qs = qs.filter(
            Q(employee_name__icontains=search)
            | Q(employee_id__icontains=search)
            | Q(email_id__icontains=search)
        )

    if company:
        qs = qs.filter(company=company)

    if location:
        qs = qs.filter(location=location)

    if department:
        qs = qs.filter(department=department)

    logger.info(
        "User='%s' exported EmployeeJoining Excel | search='%s' company='%s' "
        "location='%s' department='%s' | rows=%s",
        request.user.username,
        search,
        company,
        location,
        department,
        qs.count(),
    )

    # -------- Build DataFrame (ALL COLUMNS) --------
    data = qs.values(
        # Basic
        "employee_id",
        "employee_name",
        "gender",
        "date_of_birth",
        "company",
        "location",
        "department",
        "designation",
        "date_of_joining",
        "date_of_confirmation",
        # Access / infra
        "Biomatric_enrollment",
        "attendance_card",
        "smart_office_entry",
        "mobile_phone",
        "sim_card",
        "telephone_extension",
        "computer",
        "erp_login",
        "office_365_id",
        "email_id",
        "sharepoint_site",
        "specific_folder_rights",
        "remark",
        # Workflow
        "status",
        "hr_user__username",
        "it_user__username",
        "it_approver__username",
        "hr_submitted_at",
        "it_completed_at",
        "approved_at",
        "approval_remark",
        # Audit
        "created_at",
        "updated_at",
    )

    df = pd.DataFrame(list(data))

    # ---- NEW: make timezone-aware datetimes naive so Excel is happy ----
    tz_cols = [
        "hr_submitted_at",
        "it_completed_at",
        "approved_at",
        "created_at",
        "updated_at",
    ]
    for c in tz_cols:
        if c in df.columns:
            # to_datetime handles None values; tz_localize(None) drops timezone info
            df[c] = pd.to_datetime(df[c]).dt.tz_localize(None)

    # Friendly column names / order
    df.rename(
        columns={
            "employee_id": "Employee ID",
            "employee_name": "Employee Name",
            "gender": "Gender",
            "date_of_birth": "Date of Birth",
            "company": "Company",
            "location": "Location",
            "department": "Department",
            "designation": "Designation",
            "date_of_joining": "Date of Joining",
            "date_of_confirmation": "Date of Confirmation",
            "Biomatric_enrollment": "Biometric Enrollment",
            "attendance_card": "Attendance Card",
            "smart_office_entry": "Smart Office Entry",
            "mobile_phone": "Mobile Phone",
            "sim_card": "SIM Card",
            "telephone_extension": "Telephone Extension",
            "computer": "Computer",
            "erp_login": "ERP Login",
            "office_365_id": "Office 365 ID",
            "email_id": "Email",
            "sharepoint_site": "SharePoint Site",
            "specific_folder_rights": "Specific Folder Rights",
            "remark": "Remark",
            # workflow
            "status": "Status",
            "hr_user__username": "HR User",
            "it_user__username": "IT User",
            "it_approver__username": "IT Approver",
            "hr_submitted_at": "HR Submitted At",
            "it_completed_at": "IT Completed At",
            "approved_at": "Approved At",
            "approval_remark": "Approval Remark",
            # audit
            "created_at": "Created At",
            "updated_at": "Updated At",
        },
        inplace=True,
    )

    # -------- Write to Excel --------
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="EmployeeJoining", startrow=2)

        workbook  = writer.book
        worksheet = writer.sheets["EmployeeJoining"]

        # Title
        title_fmt = workbook.add_format({
            "bold": True,
            "font_size": 14,
        })
        worksheet.write(0, 0, "Employee Joining Detail", title_fmt)

        # Header formatting
        header_fmt = workbook.add_format({
            "bold": True,
            "bg_color": "#EAF2FF",
            "border": 1,
        })
        for col_num, col_name in enumerate(df.columns):
            worksheet.write(2, col_num, col_name, header_fmt)

        # Column width
        worksheet.set_column(0, len(df.columns) - 1, 20)

    output.seek(0)
    filename = "EmployeeJoining.xlsx"
    response = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


#--------------------------------------------------------------------------------------------
### Manual Upload using celery

@require_POST
@login_required
def attendance_manual_upload_excel(request):
    excel_file = request.FILES.get("attendance_file")

    if not excel_file:
        messages.error(request, "Please select an Excel file to upload.")
        return redirect("attendance_summary")

    name_lower = excel_file.name.lower()
    if not (name_lower.endswith(".xls") or name_lower.endswith(".xlsx")):
        messages.error(request, "Invalid file type. Please upload an .xls or .xlsx file.")
        return redirect("attendance_summary")

    # ✅ MANUAL_ATT_DIR is already a Path; this is our *directory*
    upload_dir = MANUAL_ATT_DIR
    upload_dir.mkdir(parents=True, exist_ok=True)

    # build a unique filename inside that directory
    timestamp = timezone.now().strftime("%Y%m%d_%H%M%S")
    suffix = Path(excel_file.name).suffix
    safe_name = f"daily_attendance_{timestamp}{suffix}"
    saved_path = upload_dir / safe_name

    # save the uploaded file
    with saved_path.open("wb+") as destination:
        for chunk in excel_file.chunks():
            destination.write(chunk)

    # call Celery task in background
    upload_manual_daily_attendance_task.delay(str(saved_path))

    messages.success(
        request,
        "Attendance file uploaded successfully. Import has started in the background.",
    )
    return redirect("attendance_summary")
