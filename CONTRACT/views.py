from django.shortcuts import render, redirect
from datetime import date,datetime, time
from django.shortcuts import render
from django.contrib.auth.decorators import login_required
import xlsxwriter
import io
from django.http import HttpResponse
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from CONTRACT.hr_contract_etl import run_hr_contract_etl
from .models import *
from django.db import transaction, connection
from django.db.models import Count
import logging
from datetime import datetime, timedelta, date, time
import pytz
from zk import ZK
from django.utils import timezone
from django.utils.timezone import make_aware
from django.db.models import Q
from django.utils.dateparse import parse_date
from django.http import FileResponse
from pytz import timezone
from django.core.exceptions import ObjectDoesNotExist
from django.views.decorators.http import require_POST


logger = logging.getLogger('custom_logger')    
#==========================Below code is for contract punch in punch out =========================================================


from .models import ContractEmpDepartment,ContractEmployee
from django.db import connection
from django.utils.encoding import escape_uri_path





def get_matched_contract_employees(start_date, end_date, dept_id=None, emp_name=None, shift=None):
    query = """
        SELECT 
            hr.employee_id,
            hr.work_date,
            hr.in_date,
            hr.in_time,
            hr.out_date,
            hr.out_time,
            hr.shift,
            hr.work_hhmm,
            hr.ot_hours,
            hr.double_ot_hours,
            ce.name AS employee_name,
            dept.name AS company
        FROM hr_contract hr
        INNER JOIN contract_employee ce ON hr.employee_id = ce.id
        LEFT JOIN contract_employee_dept dept ON ce.department_id = dept.id
        WHERE hr.work_date >= %s AND hr.work_date <= %s
    """
    params = [start_date, end_date]

    # Only add filter if not 'all' and not blank
    if dept_id and dept_id != 'all' and dept_id != '':
        query += " AND ce.department_id = %s"
        params.append(dept_id)
    if emp_name:
        query += " AND ce.name LIKE %s"
        params.append(f"%{emp_name}%")
    if shift and shift != 'all' and shift != '':
        query += " AND hr.shift = %s"
        params.append(shift)
    query += " ORDER BY hr.employee_id"

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]



def get_unregistered_contract_employees(start_date, end_date, shift=None):
    """
    Rows present in hr_contract but NOT present in contract_employee.
    Name / company are intentionally NULL (hidden in template).
    """
    query = """
        SELECT 
            hr.employee_id,
            hr.work_date,
            hr.in_date,
            hr.in_time,
            hr.out_date,
            hr.out_time,
            hr.shift,
            hr.work_hhmm,
            hr.ot_hours,
            hr.double_ot_hours,
            NULL AS employee_name,
            NULL AS company
        FROM hr_contract hr
        LEFT JOIN contract_employee ce ON hr.employee_id = ce.id
        WHERE ce.id IS NULL
          AND hr.work_date >= %s AND hr.work_date <= %s
    """
    params = [start_date, end_date]
    if shift and shift != 'all' and shift != '':
        query += " AND hr.shift = %s"
        params.append(shift)
    query += " ORDER BY hr.employee_id"

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        cols = [c[0] for c in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]


def count_unregistered(start_date, end_date, shift=None):
    query = """
        SELECT COUNT(*)
        FROM hr_contract hr
        LEFT JOIN contract_employee ce ON hr.employee_id = ce.id
        WHERE ce.id IS NULL
          AND hr.work_date >= %s AND hr.work_date <= %s
    """
    params = [start_date, end_date]
    if shift and shift != 'all' and shift != '':
        query += " AND hr.shift = %s"
        params.append(shift)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchone()[0]


SHIFT_CHOICES = [
        "1st Shift (07:00AM-15:00PM)",
        "General (09:00AM-18:00PM)",
        "2nd Shift (15:00PM-23:00PM)",
        "4th Shift (19:00PM-07:00AM)",
        "Night (23:00PM-07:00AM)",
        "3rd Shift (07:00AM-19:00PM)",  
    ]

def contract_employee_matched_report(request):
    logger.info("User=%s access casual sync page", request.user.username)
    today_str = datetime.now().strftime('%Y-%m-%d')
    start_date = request.GET.get('start_date', today_str)
    end_date   = request.GET.get('end_date', today_str)
    dept_id    = request.GET.get('department', '')
    emp_name   = request.GET.get('employee_name', '').strip()
    shift      = request.GET.get('shift', 'all')

    # new toggle: when "Unregistered" card is clicked
    show_unregistered = request.GET.get('unregistered') == '1'

    if show_unregistered:
        records = get_unregistered_contract_employees(start_date, end_date, shift)
    else:
        records = get_matched_contract_employees(start_date, end_date, dept_id, emp_name, shift)

    total = len(records)
    total_ot = sum(int(rec.get('ot_hours') or 0) for rec in records)
    # double_ot still computed but not shown in a card anymore; safe to keep in table/export if you want
    double_ot = sum(int(rec.get('double_ot_hours') or 0) for rec in records)

    total_work_minutes = 0
    for rec in records:
        hhmm = rec.get('work_hhmm')
        if hhmm and ':' in hhmm:
            h, m = [int(x) for x in hhmm.split(':')]
            total_work_minutes += h * 60 + m
    total_work_hhmm = f"{total_work_minutes // 60:02d}:{total_work_minutes % 60:02d}"

    departments = ContractEmpDepartment.objects.order_by('name')

    # show a live count on the new card
    unregistered_count = count_unregistered(start_date, end_date, shift)

    return render(request, 'contract/contract_attend_report.html', {
        'records': records,
        'start_date': start_date,
        'end_date': end_date,
        'total': total,
        'total_ot': total_ot,
        'double_ot': double_ot,  # kept for table; card removed
        'total_work_hhmm': total_work_hhmm,
        'departments': departments,
        'selected_dept': str(dept_id),
        'employee_name': emp_name,
        'shift_choices': SHIFT_CHOICES,
        'selected_shift': shift,
        'show_unregistered': show_unregistered,
        'unregistered_count': unregistered_count,
    })


def contract_employee_attend_report_excel(request):
    logger.info("User=%s download Casual sync report", request.user.username)
    import io
    from datetime import datetime, timedelta
    import xlsxwriter
    from django.http import HttpResponse
    from django.utils.encoding import escape_uri_path

    SHIFT_CHOICES = [
        "1st Shift (07:00AM-15:00PM)",
        "General (09:00AM-18:00PM)",
        "2nd Shift (15:00PM-23:00PM)",
        "4th Shift (19:00PM-07:00AM)",
        "Night (23:00PM-07:00AM)",
        "3rd Shift (07:00AM-19:00PM)",
    ]

    today_str = datetime.now().strftime('%Y-%m-%d')
    start_date = request.GET.get('start_date', today_str)
    end_date   = request.GET.get('end_date', today_str)
    dept_id    = request.GET.get('department', '')
    emp_name   = request.GET.get('employee_name', '').strip()
    shift      = request.GET.get('shift', 'all')

    if dept_id == 'all' or not dept_id:
        dept_id = ''
    if shift == 'all' or not shift:
        shift = None

    records = get_matched_contract_employees(start_date, end_date, dept_id, emp_name, shift)

    # ----- date range (inclusive) -----
    def _d(s: str) -> datetime.date:
        return datetime.strptime(s, "%Y-%m-%d").date()

    d0 = _d(start_date)
    d1 = _d(end_date)
    if d1 < d0:
        d0, d1 = d1, d0

    date_list = []
    cur = d0
    while cur <= d1:
        date_list.append(cur)
        cur += timedelta(days=1)

    # ----- presence matrix prep -----
    from collections import OrderedDict
    employees = OrderedDict()  # emp_code -> (name, company)
    punched   = set()          # (emp_code, date)

    for rec in records:
        emp_code = str(rec.get('employee_id') or '').strip()
        emp_nm   = str(rec.get('employee_name') or '').strip()
        company  = str(rec.get('company') or '').strip()
        if emp_code:
            if emp_code not in employees:
                employees[emp_code] = (emp_nm, company)
            # normalize work_date
            wdt = rec.get('work_date')
            if wdt:
                if isinstance(wdt, str):
                    try:
                        wdt = datetime.strptime(wdt[:10], "%Y-%m-%d").date()
                    except Exception:
                        wdt = None
                elif isinstance(wdt, datetime):
                    wdt = wdt.date()
                if wdt and d0 <= wdt <= d1:
                    punched.add((emp_code, wdt))

    # ----- workbook -----
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})

    fmt_header = workbook.add_format({
        'bold': True, 'align': 'center', 'valign': 'vcenter',
        'border': 1, 'bg_color': '#FFE5CC'
    })
    fmt_cell_center = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1})
    fmt_cell_center_bold = workbook.add_format({'align': 'center', 'valign': 'vcenter', 'border': 1, 'bold': True})
    fmt_text = workbook.add_format({'border': 1})
    fmt_head_left = workbook.add_format({'bold': True, 'border': 1, 'align': 'left', 'bg_color': '#FFE5CC'})

    # Sheet 1: Attendance
    ws = workbook.add_worksheet('Attendance')
    headers = [
        "Emp Code", "Name", "Company", "Work Date", "In Date", "In Time",
        "Out Date", "Out Time", "Shift", "Work HHMM", "OT Hours", "Double OT"
    ]
    ws.write_row(0, 0, headers, fmt_header)
    for rowidx, rec in enumerate(records, 1):
        ws.write(rowidx, 0, rec.get('employee_id'))
        ws.write(rowidx, 1, rec.get('employee_name'))
        ws.write(rowidx, 2, rec.get('company'))
        ws.write(rowidx, 3, str(rec.get('work_date')))
        ws.write(rowidx, 4, str(rec.get('in_date')))
        ws.write(rowidx, 5, rec.get('in_time'))
        ws.write(rowidx, 6, str(rec.get('out_date')))
        ws.write(rowidx, 7, rec.get('out_time'))
        ws.write(rowidx, 8, rec.get('shift'))
        ws.write(rowidx, 9, rec.get('work_hhmm'))
        ws.write(rowidx, 10, rec.get('ot_hours'))
        ws.write(rowidx, 11, rec.get('double_ot_hours'))
    ws.set_column(0, 0, 12)
    ws.set_column(1, 1, 28)
    ws.set_column(2, 2, 22)
    ws.set_column(3, 8, 14)
    ws.set_column(9, 11, 10)
    ws.freeze_panes(1, 0)

    # Sheet 2: Presence Matrix (Company + 'A' for absences + per-day totals)
    wsm = workbook.add_worksheet('Presence Matrix')

    # headers
    wsm.write(0, 0, "Emp Code", fmt_head_left)
    wsm.write(0, 1, "Name", fmt_head_left)
    wsm.write(0, 2, "Company", fmt_head_left)
    for j, d in enumerate(date_list, start=3):
        wsm.write(0, j, d.strftime("%Y-%m-%d"), fmt_header)

    # rows with P/A
    for i, (emp_code, (emp_name, company)) in enumerate(employees.items(), start=1):
        wsm.write(i, 0, emp_code, fmt_text)
        wsm.write(i, 1, emp_name, fmt_text)
        wsm.write(i, 2, company, fmt_text)
        for j, d in enumerate(date_list, start=3):
            wsm.write(i, j, 'P' if (emp_code, d) in punched else 'A', fmt_cell_center)

    # totals row (count of P for each day)
    total_row = len(employees) + 1
    # Label spanning first three columns
    wsm.merge_range(total_row, 0, total_row, 2, "Total P", fmt_header)
    for j, d in enumerate(date_list, start=3):
        present_count = sum(1 for emp in employees.keys() if (emp, d) in punched)
        wsm.write(total_row, j, present_count, fmt_cell_center_bold)

    # sizing & filter
    wsm.set_column(0, 0, 12)              # Emp Code
    wsm.set_column(1, 1, 34)              # Name
    wsm.set_column(2, 2, 22)              # Company
    wsm.set_column(3, 2 + len(date_list), 12)  # Dates
    wsm.freeze_panes(1, 3)                # freeze header row + first 3 cols
    wsm.autofilter(0, 0, total_row, 2 + len(date_list))

    workbook.close()
    output.seek(0)

    filename = f"Contract_Attendance_{start_date}_to_{end_date}.xlsx"
    response = HttpResponse(
        output.read(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{escape_uri_path(filename)}"'
    return response



@login_required
def sync_hr_contract_view(request):
    logger.info("User=%s run casual report sync", request.user.username)
    if request.method == "POST" and request.headers.get("x-requested-with") == "XMLHttpRequest":
        try:
            run_hr_contract_etl()
            return JsonResponse({"success": True, "message": "HR Contract data synced successfully."})
        except Exception as e:
            return JsonResponse({"success": False, "error": str(e)})
    return JsonResponse({"success": False, "error": "Invalid request"})

@login_required
def contract_employee_names(request):
    # 1) get the requested date (YYYY-MM-DD), default to today
    date_str = request.GET.get('date')
    try:
        selected_date = datetime.strptime(date_str, "%Y-%m-%d").date() if date_str else date.today()
    except ValueError:
        selected_date = date.today()

    # 2) fetch all records for that single day
    records = get_matched_contract_employees(
        start_date=selected_date,
        end_date=selected_date,
        dept_id=None,
        emp_name=None,
        shift=None
    )

    # 3) build a list of employees with their times
    employees = [
        {
            'name':   r['employee_name'],
            'in_time':  r['in_time'],
            'out_time': r['out_time'],
        }
        for r in records
    ]

    return render(request, 'contract/contract_employee_names.html', {
        'date':      selected_date,
        'employees': employees,
    })





def get_dashboard_counts(selected_date):
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(DISTINCT employee_id) FROM hr_contract WHERE work_date = %s", [selected_date])
        total_punched_in = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(id) FROM contract_employee_assignment WHERE punch_date = %s", [selected_date])
        total_assigned = cursor.fetchone()[0]
    return total_punched_in, total_assigned


def get_unassigned_employees(punch_date):
    """
    MODIFIED: An employee is now considered "unavailable" if they have any assignment
    for the day that has NOT been marked for reassignment.
    """
    # KEY CHANGE: Find employees who are "busy". A busy employee has an assignment
    # for the day that is not yet marked as 'reassigned'.
    unavailable_employee_ids = list(EmployeeAssignment.objects.filter(
        punch_date=punch_date,
        is_reassigned=False
    ).values_list('employee_id', flat=True).distinct())

    query = """
        SELECT
            hr.employee_id, hr.in_time, hr.shift, ce.name AS employee_name,
            ce.department_id AS contractor_id
        FROM hr_contract hr
        INNER JOIN contract_employee ce ON hr.employee_id = ce.id
        WHERE hr.work_date = %s
    """
    params = [punch_date]
    if unavailable_employee_ids:
        placeholders = ', '.join(['%s'] * len(unavailable_employee_ids))
        query += f" AND hr.employee_id NOT IN ({placeholders})"
        params.extend(unavailable_employee_ids)
    query += " ORDER BY ce.name"
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


@transaction.atomic
def daily_assignment_view(request):
    logger.info("User=%s access casual assign page", request.user.username)
    # This view remains correct and does not need changes.
    if request.method == 'POST':
        try:
            assignment = EmployeeAssignment(
                punch_date=request.POST.get('punch_date'),
                employee_id=request.POST.get('employee'),
                contractor_id=request.POST.get('contractor') or None,
                department=request.POST.get('department') or None,
                block_location=request.POST.get('block_location'),
                shift=request.POST.get('shift') or None,
                punch_in=request.POST.get('punch_in') or None,
                punch_out=request.POST.get('punch_out') or None,
            )
            assignment.full_clean(exclude=['shift', 'punch_in', 'punch_out', 'contractor', 'department'])
            assignment.save()
            return JsonResponse({'success': True, 'message': 'Assignment saved!'})
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)}, status=400)

    selected_date_str = request.GET.get('punch_date', date.today().strftime('%Y-%m-%d'))
    try:
        selected_date = date.fromisoformat(selected_date_str)
    except (ValueError, TypeError):
        selected_date = date.today()

    unassigned_records = get_unassigned_employees(selected_date)
    total_punched_in, total_assigned = get_dashboard_counts(selected_date)
    
    contractors = ContractorName.objects.all().order_by('name')
    department_choices = DEPARTMENT_CHOICES
    block_locations = BLOCK_LOCATIONS
    shift_choices = EmployeeAssignment._meta.get_field('shift').choices

    context = {
        'records': unassigned_records,
        'contractors': contractors,
        'department_choices': department_choices,
        'block_locations': block_locations,
        'shift_choices': shift_choices,
        'selected_date': selected_date.strftime('%Y-%m-%d'),
        'total_punched_in': total_punched_in,
        'total_assigned': total_assigned,
    }
    return render(request, 'contract/daily_assignment.html', context)


@require_POST
def update_punch_out_view(request):
    # This view remains correct and does not need changes.
    try:
        assignment_id = request.POST.get('assignment_id')
        punch_out_time = request.POST.get('punch_out_time')
        assignment = EmployeeAssignment.objects.get(pk=assignment_id)
        assignment.punch_out = punch_out_time or None
        assignment.save(update_fields=['punch_out'])
        return JsonResponse({'success': True, 'message': 'Punch out time updated successfully.'})
    except ObjectDoesNotExist:
        return JsonResponse({'success': False, 'error': 'Assignment record not found.'}, status=404)
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@require_POST
def reassign_employee_view(request):
    """
    MODIFIED: This view now sets the 'is_reassigned' flag to True, which makes
    the employee available for a new task.
    """
    try:
        assignment_id = request.POST.get('assignment_id')
        if not assignment_id:
            return JsonResponse({'success': False, 'error': 'Assignment ID is missing.'}, status=400)

        assignment = EmployeeAssignment.objects.get(pk=assignment_id)
        
        if not assignment.punch_out:
            return JsonResponse({'success': False, 'error': 'Cannot reassign without a Punch Out time.'}, status=400)
        
        # KEY CHANGE: Mark the assignment as reassigned instead of just checking.
        assignment.is_reassigned = True
        assignment.save(update_fields=['is_reassigned'])
        
        return JsonResponse({'success': True, 'message': 'Employee is now available for a new assignment.'})

    except ObjectDoesNotExist:
        return JsonResponse({'success': False, 'error': 'Assignment record not found.'}, status=404)
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@require_POST
@login_required
def update_shift_view(request):
    """
    Update shift for an assignment row (AJAX).
    """
    try:
        assignment_id = request.POST.get("assignment_id")
        new_shift = (request.POST.get("shift") or "").strip()

        if not assignment_id:
            return JsonResponse({"success": False, "error": "Assignment ID is missing."}, status=400)

        if not new_shift:
            return JsonResponse({"success": False, "error": "Shift is required."}, status=400)

        if new_shift not in SHIFT_CHOICES:
            return JsonResponse({"success": False, "error": "Invalid shift value."}, status=400)

        assignment = EmployeeAssignment.objects.get(pk=assignment_id)
        # Optional: If you want to block shift change after reassigned, uncomment
        # if assignment.is_reassigned:
        #     return JsonResponse({"success": False, "error": "Cannot update shift after reassigned."}, status=400)
        assignment.shift = new_shift
        assignment.save(update_fields=["shift"])

        return JsonResponse({"success": True, "message": "Shift updated successfully.", "shift": new_shift})

    except ObjectDoesNotExist:
        return JsonResponse({"success": False, "error": "Assignment record not found."}, status=404)
    except Exception as e:
        logger.exception("update_shift_view error")
        return JsonResponse({"success": False, "error": str(e)}, status=500)



@login_required
def assignment_dashboard_page(request):
    logger.info("User=%s access Casual worker assigned dashboard", request.user.username)
    today_iso = date.today().isoformat()
    context = {
        "default_from": today_iso,
        "default_to": today_iso,
        # ✅ Send shift list to template for dropdown
        "SHIFT_CHOICES": SHIFT_CHOICES,
    }
    return render(request, "contract/assignment_dashboard.html", context)


def assignment_dashboard_data(request):
    """
    MODIFIED: Add the 'is_reassigned' field to the data sent to the frontend.
    """
    # ... (filtering logic is the same) ...
    dfrom_str, dto_str, q, contractor_id, department, block, shift = (
        request.GET.get("from"), request.GET.get("to"), (request.GET.get("q") or "").strip(),
        request.GET.get("contractor"), request.GET.get("department"),
        request.GET.get("block_location"), request.GET.get("shift")
    )
    qs = EmployeeAssignment.objects.select_related('employee').all()
    if dfrom_str and dto_str: qs = qs.filter(punch_date__range=[dfrom_str, dto_str])
    if q: qs = qs.filter(employee__name__icontains=q)
    if contractor_id: qs = qs.filter(contractor_id=contractor_id)
    if department: qs = qs.filter(department=department)
    if block: qs = qs.filter(block_location=block)
    if shift: qs = qs.filter(shift=shift)

    # ... (KPI calculations are the same) ...
    total_assignments = qs.count()
    by_contractor = list(qs.values('contractor__name').annotate(count=Count('id')).order_by('-count'))
    by_department = list(qs.values('department').annotate(count=Count('id')).order_by('-count'))
    by_block = list(qs.values('block_location').annotate(count=Count('id')).order_by('-count'))
    by_shift = list(qs.values('shift').annotate(count=Count('id')).order_by('-count'))
    
    # KEY CHANGE: Add 'is_reassigned' to the values list.
    detail_rows = list(qs.order_by('-punch_date', '-assigned_date')[:500].values(
        'id', 'punch_date', 'employee__name', 'contractor__name', 'department',
        'block_location', 'shift', 'punch_in', 'punch_out', 'is_reassigned'
    ))

    return JsonResponse({
        "total_assignments": total_assignments,
        "by_contractor": by_contractor,
        "by_department": by_department,
        "by_block": by_block,
        "by_shift": by_shift,
        "table_rows": detail_rows,
    }, safe=False, json_dumps_params={'default': str})



def assignment_dashboard_export(request):
    logger.info("User=%s download casual assigned report", request.user.username)
    import io
    from collections import defaultdict
    from datetime import date, datetime, timedelta, time as dtime

    import xlsxwriter
    from django.http import HttpResponse
    from django.db.models import Count
    from .models import EmployeeAssignment

    # --- 1) Read filters (unchanged) -----------------------------------------
    dfrom_str = request.GET.get("from")
    dto_str   = request.GET.get("to")
    q         = (request.GET.get("q") or "").strip()
    contractor_id = request.GET.get("contractor")
    department    = request.GET.get("department")
    block         = request.GET.get("block_location")
    shift         = request.GET.get("shift")

    if not dfrom_str or not dto_str:
        today_iso = date.today().isoformat()
        dfrom_str = dfrom_str or today_iso
        dto_str   = dto_str   or today_iso
    dfrom = datetime.strptime(dfrom_str, "%Y-%m-%d").date()
    dto   = datetime.strptime(dto_str,   "%Y-%m-%d").date()
    if dto < dfrom:
        dfrom, dto = dto, dfrom

    # --- 2) Base queryset (unchanged filters) --------------------------------
    qs = EmployeeAssignment.objects.select_related('employee', 'contractor').all()
    qs = qs.filter(punch_date__range=[dfrom_str, dto_str])
    if q:
        qs = qs.filter(employee__name__icontains=q)
    if contractor_id:
        qs = qs.filter(contractor_id=contractor_id)
    if department:
        qs = qs.filter(department=department)
    if block:
        qs = qs.filter(block_location=block)
    if shift:
        qs = qs.filter(shift=shift)

    # ------- Sheet 1 & 2: your existing code (UNCHANGED) ---------------------
    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {"in_memory": True})

    fmt_title   = wb.add_format({"bold": True, "font_size": 16, "align": "center", "valign": "vcenter"})
    fmt_head    = wb.add_format({"bold": True, "bg_color": "#E5F0FF", "border": 1, "align": "center", "valign": "vcenter"})
    fmt_head_l  = wb.add_format({"bold": True, "bg_color": "#E5F0FF", "border": 1})
    fmt_text    = wb.add_format({"border": 1})
    fmt_center  = wb.add_format({"border": 1, "align": "center"})
    fmt_date    = wb.add_format({"border": 1, "num_format": "dd-mm-yyyy"})
    fmt_time    = wb.add_format({"border": 1, "num_format": "hh:mm"})
    fmt_duration= wb.add_format({"border": 1, "num_format": "[h]:mm"})
    fmt_note    = wb.add_format({"italic": True, "font_color": "#666"})
    fmt_total   = wb.add_format({"bold": True, "border": 1, "bg_color": "#F1F5F9", "align": "center"})

    # ---------------- Sheet 1: Assignments (DETAIL) --------------------------
    ws = wb.add_worksheet("Assignments")
    headers = ["Punch Date","Employee","Contractor","Department","Block","Shift",
               "Punch In","Punch Out","Working Hours","OT Hours"]
    ws.merge_range(0, 0, 0, len(headers) - 1, "Casual worker Attendance", fmt_title)
    for c, h in enumerate(headers):
        ws.write(1, c, h, fmt_head)

    rows = qs.order_by('-punch_date', '-assigned_date').values(
        'punch_date','employee__name','contractor__name','department',
        'block_location','shift','punch_in','punch_out'
    )

    # ---------------- SHIFT → OT MAP (UPDATED RULE) --------------------------
    def get_ot_threshold(shift):
        """
        1st & 2nd shift → 9 hrs
        3rd & 4th shift → 8 hrs
        """
        if not shift:
            return timedelta(hours=9)
        s = shift.lower()
        if "3rd" in s or "4th" in s:
            return timedelta(hours=8)
        return timedelta(hours=9)

    # ---------------- Populate Assignment Sheet ------------------------------
    for r_idx, row in enumerate(rows, 2):
        ws.write_datetime(r_idx, 0, datetime.combine(row['punch_date'], dtime.min), fmt_date)
        ws.write(r_idx, 1, row['employee__name'] or '', fmt_text)
        ws.write(r_idx, 2, row['contractor__name'] or '', fmt_text)
        ws.write(r_idx, 3, row['department'] or '', fmt_text)
        ws.write(r_idx, 4, row['block_location'] or '', fmt_text)
        ws.write(r_idx, 5, row['shift'] or '', fmt_text)

        if row['punch_in']:
            ws.write_datetime(r_idx, 6, datetime.combine(date.today(), row['punch_in']), fmt_time)
        else:
            ws.write(r_idx, 6, '', fmt_text)

        if row['punch_out']:
            ws.write_datetime(r_idx, 7, datetime.combine(date.today(), row['punch_out']), fmt_time)
        else:
            ws.write(r_idx, 7, '', fmt_text)

        wh = ot = None
        if row['punch_in'] and row['punch_out']:
            d0 = date.today()
            pin  = datetime.combine(d0, row['punch_in'])
            pout = datetime.combine(d0, row['punch_out'])
            # Night shift (cross midnight)
            if pout < pin:
                pout += timedelta(days=1)

            wh = pout - pin
            ot_start = get_ot_threshold(row['shift'])
            ot = wh - ot_start if wh > ot_start else timedelta(0)

        if wh is not None:
            ws.write_number(r_idx, 8, wh.total_seconds()/86400.0, fmt_duration)
        else:
            ws.write(r_idx, 8, '', fmt_text)

        if ot is not None:
            ws.write_number(r_idx, 9, ot.total_seconds()/86400.0, fmt_duration)
        else:
            ws.write(r_idx, 9, '', fmt_text)

    ws.autofit()

    # ---------------- Sheet 2: Summary ---------------------------------------
    ws2 = wb.add_worksheet("Summary")
    total_assignments = qs.count()
    ws2.merge_range(0, 0, 0, 5, "Casual Employee Dashboard – Summary", fmt_title)
    ws2.write(
        1, 0,
        f"Date range: {dfrom_str or '-'} to {dto_str or '-'}   |   Total assigned: {total_assignments}",
        fmt_note
    )

    def write_table(start_row, title, col_label, data, key_field):
        ws2.write(start_row, 0, title, fmt_head_l)
        ws2.write(start_row+1, 0, col_label, fmt_head)
        ws2.write(start_row+1, 1, "Count", fmt_head)
        r = start_row+2
        for item in data:
            ws2.write(r, 0, item.get(key_field) or "", fmt_text)
            ws2.write_number(r, 1, item["count"], fmt_center)
            r += 1
        ws2.write(r, 0, "Total", fmt_head)
        ws2.write_formula(r, 1, f"=SUM(B{start_row+3}:B{r})", fmt_head)
        return r + 2

    by_contractor = list(qs.values('contractor__name').annotate(count=Count('id')).order_by('-count','contractor__name'))
    by_department = list(qs.values('department').annotate(count=Count('id')).order_by('-count','department'))
    by_block      = list(qs.values('block_location').annotate(count=Count('id')).order_by('-count','block_location'))
    by_shift      = list(qs.values('shift').annotate(count=Count('id')).order_by('-count','shift'))

    rp = 3
    rp = write_table(rp, "Contractor-wise", "Contractor", by_contractor, "contractor__name")
    rp = write_table(rp, "Department-wise", "Department", by_department, "department")
    rp = write_table(rp, "Block-wise", "Block", by_block, "block_location")
    rp = write_table(rp, "Shift-wise", "Shift", by_shift, "shift")
    ws2.set_column(0, 0, 35)
    ws2.set_column(1, 1, 12)
    ws2.autofit()

    # ---------------- Sheet 3: Attendance Pivot -----------------------------
    ws3 = wb.add_worksheet("Attendance Pivot")
    date_cols = []
    cur = dfrom
    while cur <= dto:
        date_cols.append(cur)
        cur += timedelta(days=1)

    # Header
    fixed_headers = ["Emp Code", "Name", "Company"]
    last_col = len(fixed_headers) + len(date_cols)
    ws3.merge_range(0, 0, 0, last_col, "Casual worker Attendance", fmt_title)
    for c, h in enumerate(fixed_headers):
        ws3.write(1, c, h, fmt_head)
    for idx, d in enumerate(date_cols):
        ws3.write(1, len(fixed_headers) + idx, d.strftime("%Y-%m-%d"), fmt_head)
    ws3.write(1, len(fixed_headers) + len(date_cols), "Total OT Hours", fmt_head)
    ws3.freeze_panes(2, 3)

    # Aggregate per employee
    emp_map = defaultdict(lambda: {"dates": {}, "total_ot": timedelta(0), "name": "", "company": "", "emp_code": ""})
    for row in qs:
        emp_code = getattr(row.employee, "emp_code", row.employee.id if row.employee else "")
        key = emp_code
        emp_map[key]["name"] = row.employee.name if row.employee else ""
        emp_map[key]["company"] = row.contractor.name if row.contractor else ""
        emp_map[key]["emp_code"] = emp_code

        pin, pout = row.punch_in, row.punch_out
        wh = ot = timedelta(0)
        if pin and pout:
            pin_dt = datetime.combine(row.punch_date, pin)
            pout_dt = datetime.combine(row.punch_date, pout)
            if pout_dt < pin_dt:
                pout_dt += timedelta(days=1)
            wh = pout_dt - pin_dt
            ot_start = get_ot_threshold(row.shift)
            ot = wh - ot_start if wh > ot_start else timedelta(0)

        # Store status
        if row.punch_date in emp_map[key]["dates"]:
            prev = emp_map[key]["dates"][row.punch_date]
            # combine multiple punches
            emp_map[key]["dates"][row.punch_date] = prev + " | P"
        else:
            emp_map[key]["dates"][row.punch_date] = "P" if pin and pout else "A"

        emp_map[key]["total_ot"] += ot

    # Write Pivot Body
    r = 2
    for key, data in sorted(emp_map.items(), key=lambda x: x[1]["name"]):
        ws3.write(r, 0, data["emp_code"], fmt_text)
        ws3.write(r, 1, data["name"], fmt_text)
        ws3.write(r, 2, data["company"], fmt_text)
        for ci, dcol in enumerate(date_cols):
            status = data["dates"].get(dcol, "A")
            ws3.write(r, 3 + ci, status, fmt_center)
        ws3.write_number(r, 3 + len(date_cols), data["total_ot"].total_seconds()/86400.0, fmt_duration)
        r += 1

    # Column widths
    ws3.set_column(0, 0, 12)
    ws3.set_column(1, 1, 28)
    ws3.set_column(2, 2, 24)
    for col in range(3, 3 + len(date_cols)):
        ws3.set_column(col, col, 12)
    ws3.set_column(3 + len(date_cols), 3 + len(date_cols), 14)
    ws3.autofit()

    # Finish
    wb.close()
    output.seek(0)
    filename = f"Casual_worker_Attendance_{date.today().strftime('%Y%m%d')}.xlsx"
    return HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )
