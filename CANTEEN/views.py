from django.views.decorators.http import require_GET
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from datetime import time,datetime, timedelta
from .models import *
from .utils import determine_shift
from django.utils import timezone
from django.db import transaction
from django.http import JsonResponse, HttpResponse
from datetime import timedelta, datetime, time
import pytz
from collections import Counter
from django.views.decorators.csrf import csrf_exempt
import json
from django.db.models import Count
from django.utils.timezone import make_aware, is_naive
from collections import defaultdict
import xlsxwriter
import io, logging
from .tasks import *
from celery.result import AsyncResult
from django.views.decorators.http import require_http_methods
from django.contrib import messages
import pandas as pd
from django.db.models import Q
# Set your local timezone
LOCAL_TIMEZONE = pytz.timezone("Asia/Kolkata")

logger = logging.getLogger('custom_logger')


def get_sync_status(request):
    task_id = request.GET.get('task_id')
    if task_id:
        task_result = AsyncResult(task_id)
        result = {
            "task_id": task_id,
            "status": task_result.status,
            "result": task_result.result if task_result.ready() else None
        }
        return JsonResponse(result)
    return JsonResponse({"status": "error", "message": "No task_id provided"}, status=400)


def fetch_attendance_from_device(request):
    """
    API endpoint to trigger the attendance sync task.
    """
    task = sync_attendance_from_device_task.delay()
    return JsonResponse({"task_id": task.id})


def canteen_dashboard(request):
    # logger.info("User=%s Access Canteen Dashboard", request.user.username)
    # User info
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)

    # Read & parse filter dates
    from_str = request.GET.get("from_date", "").strip()
    to_str = request.GET.get("to_date", "").strip()

    now_local = datetime.now(LOCAL_TIMEZONE)

    start_local = None
    end_local = None

    if from_str:
        try:
            dt = datetime.strptime(from_str, "%Y-%m-%d")
            start_local = LOCAL_TIMEZONE.localize(dt.replace(hour=0, minute=0, second=0, microsecond=0))
        except ValueError:
            pass

    if not start_local:
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    if to_str:
        try:
            dt = datetime.strptime(to_str, "%Y-%m-%d")
            end_local = LOCAL_TIMEZONE.localize(dt.replace(hour=23, minute=59, second=59, microsecond=999999))
        except ValueError:
            pass

    if not end_local:
        end_local = now_local.replace(hour=23, minute=59, second=59, microsecond=999999)

    start_utc = start_local.astimezone(pytz.utc)
    end_utc = end_local.astimezone(pytz.utc)
    
    attendances_qs = Attendance.objects.filter(
        punched_at__gte=start_utc,
        punched_at__lte=end_utc
    ).select_related('employee__department', 'shift').order_by('-punched_at')

    # --- FIX: Pre-format the Employee ID for the template ---
    attendances_formatted = []
    for a in attendances_qs:
        emp_id_str = str(a.employee.id)
        # Add a new attribute to the object for the template to use
        if emp_id_str.upper().startswith('T'):
            a.formatted_id = emp_id_str
        else:
            a.formatted_id = emp_id_str.zfill(5)
        attendances_formatted.append(a)

    shift_counts = Counter(a.shift.name if a.shift else "Unknown" for a in attendances_qs)
    # ... (rest of the data processing is the same)
    sorted_names = sorted(shift_counts)
    shift_chart_data = {'labels': sorted_names, 'data': [shift_counts[n] for n in sorted_names]}
    all_shifts = Shift.objects.all().order_by('start_time')
    shift_card_data = [{'id': s.id, 'name': s.name, 'start_time': s.start_time, 'end_time': s.end_time, 'count': shift_counts.get(s.name, 0)} for s in all_shifts]
    
    

    return render(request, 'canteen/canteen_dashboard.html', {
        'attendances': attendances_formatted,  # Pass the formatted list
        'shift_card_data': shift_card_data,
        'shift_chart_data': json.dumps(shift_chart_data),
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'show_admin_panel': show_admin_panel,
        'from_date': from_str,
        'to_date': to_str,
    })


@csrf_exempt
def update_attendance(request):
    """
    API endpoint to receive attendance data via POST.
    Checks for valid punch time and prevents duplicates.
    """
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'Only POST method allowed.'}, status=405)

    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'status': 'error', 'message': 'Invalid JSON payload.'}, status=400)

    employee_id = data.get('employee_id')
    punched_at_str = data.get('punched_at')
    meal_type = data.get('meal_type', 'Meal')

    if not employee_id or not punched_at_str:
        return JsonResponse({'status': 'error', 'message': 'Missing employee_id or punched_at.'}, status=400)

    try:
        if punched_at_str.endswith('Z'):
            punched_at_str = punched_at_str[:-1] + '+00:00'

        punched_at = datetime.fromisoformat(punched_at_str)

        if punched_at.tzinfo is None:
            punched_at_local = LOCAL_TIMEZONE.localize(punched_at)
        else:
            punched_at_local = punched_at.astimezone(LOCAL_TIMEZONE)

        punched_at_utc = punched_at_local.astimezone(pytz.utc)
        shift = determine_shift(punched_at_local)
        if not shift:
            return JsonResponse({'status': 'error', 'message': 'Punch time outside allowed schedule.'}, status=400)

        employee = Employee.objects.filter(id=employee_id).first()
        if not employee:
            return JsonResponse({'status': 'error', 'message': f'Employee with id {employee_id} not found.'}, status=404)

        if Attendance.objects.filter(employee=employee, punched_at=punched_at_utc).exists():
            return JsonResponse({'status': 'error', 'message': 'Duplicate record.'}, status=400)

        punch_date = punched_at_local.date()
        start_local = datetime.combine(punch_date, time.min)
        end_local = start_local + timedelta(days=1)
        start_utc = LOCAL_TIMEZONE.localize(start_local).astimezone(pytz.utc)
        end_utc = LOCAL_TIMEZONE.localize(end_local).astimezone(pytz.utc)

        if Attendance.objects.filter(
            employee=employee,
            shift=shift,
            punched_at__gte=start_utc,
            punched_at__lt=end_utc
        ).exists():
            return JsonResponse({'status': 'error', 'message': 'Already punched for this shift today.'}, status=400)

        new_attendance = Attendance.objects.create(
            employee=employee,
            punched_at=punched_at_utc,
            meal_type=meal_type,
            shift=shift
        )

        return JsonResponse({'status': 'success', 'attendance_id': new_attendance.id})

    except Exception as e:
        return JsonResponse({'status': 'error', 'message': f'Internal error: {str(e)}'}, status=500)


@csrf_exempt
def device_push(request):
    """
    API endpoint to push attendance from device.
    Prevents duplicate shift punch and returns employee name if successful.
    """
    if request.method != 'POST':
        return JsonResponse({"status": "error", "message": "Only POST method allowed."}, status=405)

    try:
        data = json.loads(request.body)
        user_id = data.get("UserID")
        punch_time_str = data.get("Timestamp")

        if not user_id or not punch_time_str:
            return JsonResponse({"status": "error", "message": "Missing UserID or Timestamp."}, status=400)

        punch_time = datetime.strptime(punch_time_str, "%Y-%m-%d %H:%M:%S")

        if punch_time.tzinfo is None:
            punch_time_local = LOCAL_TIMEZONE.localize(punch_time)
        else:
            punch_time_local = punch_time.astimezone(LOCAL_TIMEZONE)

        punch_time_utc = punch_time_local.astimezone(pytz.utc)
        shift = determine_shift(punch_time_local)
        if not shift:
            return JsonResponse({"status": "error", "message": "Punch time outside allowed schedule."}, status=400)

        employee = Employee.objects.filter(id=user_id).first()
        if not employee:
            return JsonResponse({"status": "error", "message": "Employee not found"}, status=404)

        punch_date = punch_time_local.date()
        start_local = datetime.combine(punch_date, time.min)
        end_local = start_local + timedelta(days=1)
        start_utc = LOCAL_TIMEZONE.localize(start_local).astimezone(pytz.utc)
        end_utc = LOCAL_TIMEZONE.localize(end_local).astimezone(pytz.utc)

        if Attendance.objects.filter(
            employee=employee,
            shift=shift,
            punched_at__gte=start_utc,
            punched_at__lt=end_utc
        ).exists():
            return JsonResponse({
                "status": "duplicate",
                "message": f"{employee.name} already punched for shift '{shift.name}' on {punch_date}."
            }, status=200)

        Attendance.objects.create(
            employee=employee,
            punched_at=punch_time_utc,
            meal_type="Meal",
            shift=shift
        )

        return JsonResponse({
            "status": "ok",
            "employee": employee.name,
            "message": f"Attendance saved for {employee.name} at {punch_time_local.strftime('%I:%M %p')}"
        }, status=200)

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)


@require_GET
def dashboard_api_data(request):
    # ... (date parsing logic is the same)
    from_str = request.GET.get("from_date", "").strip()
    to_str = request.GET.get("to_date", "").strip()
    now_local = datetime.now(LOCAL_TIMEZONE)
    start_local = None
    end_local = None
    if from_str:
        try:
            dt = datetime.strptime(from_str, "%Y-%m-%d")
            start_local = LOCAL_TIMEZONE.localize(dt.replace(hour=0, minute=0, second=0, microsecond=0))
        except ValueError: pass
    if not start_local:
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    if to_str:
        try:
            dt = datetime.strptime(to_str, "%Y-%m-%d")
            end_local = LOCAL_TIMEZONE.localize(dt.replace(hour=23, minute=59, second=59, microsecond=999999))
        except ValueError: pass
    if not end_local:
        end_local = now_local.replace(hour=23, minute=59, second=59, microsecond=999999)
    start_utc = start_local.astimezone(pytz.utc)
    end_utc = end_local.astimezone(pytz.utc)

    attendances = Attendance.objects.filter(
        punched_at__gte=start_utc,
        punched_at__lte=end_utc
    ).select_related('employee__department', 'shift').order_by('-punched_at')

    # ... (chart and card data logic is the same)
    shift_counts = Counter(a.shift.name if a.shift else "Unknown" for a in attendances)
    sorted_names = sorted(shift_counts)
    shift_chart_data = {'labels': sorted_names, 'data': [shift_counts[n] for n in sorted_names]}
    all_shifts = Shift.objects.all().order_by('start_time')
    shift_card_data = [{'name': s.name, 'start_time': s.start_time.strftime("%H:%M"), 'end_time': s.end_time.strftime("%H:%M"), 'count': shift_counts.get(s.name, 0)} for s in all_shifts]
    

    # --- FIX: Format the Employee ID in the JSON response ---
    attendance_list = []
    for a in attendances:
        emp_id_str = str(a.employee.id)
        formatted_id = emp_id_str.zfill(5) if not emp_id_str.upper().startswith('T') else emp_id_str
        
        attendance_list.append({
            'employee_id': formatted_id, # Use the formatted ID
            'employee_name': a.employee.name,
            'department_name': a.employee.department.name if a.employee.department else "N/A",
            'shift_name': a.shift.name if a.shift else "Unknown",
            'punched_at_local': a.punched_at.astimezone(LOCAL_TIMEZONE).strftime("%d %b %Y, %I:%M %p")
        })

    return JsonResponse({
        'attendances': attendance_list,
        'shift_card_data': shift_card_data,
        'shift_chart_data': shift_chart_data,
    })


@login_required
def canteen_attendance_summary_report(request):
    logger.info("User=%s accessed Canteen Bill report", request.user.username)
    user_groups  = request.user.groups.values_list("name", flat=True)
    is_superuser = request.user.is_superuser

    today           = datetime.today()
    selected_month  = int(request.GET.get("month",  today.month))
    selected_year   = int(request.GET.get("year",   today.year))
    filter_type     = request.GET.get("filter_type", "monthly")     # daily / monthly
    emp_group       = request.GET.get("emp_group",   "all")         # all / regular / casual

    # ---- date range --------------------------------------------------------
    start_date = datetime(selected_year, selected_month, 1)
    next_month = start_date.replace(day=28) + timedelta(days=4)
    end_date   = next_month - timedelta(days=next_month.day)

    # ---- day headers for daily view ----------------------------------------
    date_headers, cur = [], start_date
    while cur <= end_date:
        date_headers.append(cur.date())
        cur += timedelta(days=1)

    # ---- attendance rows ---------------------------------------------------
    valid_shift_ids = (
        Shift.objects.filter(name__in=["Lunch F", "Lunch G", "Dinner", "Evening Dinner"])
        .values_list("id", flat=True)
    )

    attendances = (
        Attendance.objects.filter(
            punched_at__date__range=(start_date.date(), end_date.date()),
            shift_id__in=valid_shift_ids,
        )
        .values(
            "employee__id",
            "employee__name",
            "employee__employee_type",
            "punched_at__date",
        )
        .annotate(count=Count("id"))
    )

    # ---- arrange counts per employee ---------------------------------------
    data = defaultdict(
        lambda: {
            "employee_name": "",
            "employee_type": "",
            "date_counts": defaultdict(int),
        }
    )

    for row in attendances:
        emp_id                    = row["employee__id"]
        d                         = data[emp_id]
        d["employee_name"]        = row["employee__name"]
        d["employee_type"]        = row["employee__employee_type"] or "Company"
        d["date_counts"][row["punched_at__date"]] += row["count"]

    # ---- helper for rupee values -------------------------------------------
    RATE_COMPANY = 73
    RATE_CASUAL  = 49

    def compute_amounts(emp_type, days_with_meal, extra_meals, total_meals):
        """Return dict with keys: emp, comp, total"""
        if emp_type == "Casual":
            total = total_meals * RATE_CASUAL
            return {"emp": 0, "comp": total, "total": total}
        
        if emp_type == "Guest":
            total = total_meals * RATE_COMPANY
            return {"emp": 0, "comp": total, "total": total}

        emp_share  = round(days_with_meal * RATE_COMPANY * 0.40, 2)
        comp_share = round(days_with_meal * RATE_COMPANY * 0.60 +
                           extra_meals  * RATE_COMPANY, 2)
        total      = total_meals * RATE_COMPANY
        return {"emp": emp_share, "comp": comp_share, "total": total}

    # ---- DAILY table --------------------------------------------------------
    final_data = []
    for emp_id, info in data.items():
        # employee-type filter
        if emp_group == "regular" and info["employee_type"] == "Casual":
            continue
        if emp_group == "casual"  and info["employee_type"] != "Casual":
            continue

        counts_by_day   = [info["date_counts"].get(d, 0) for d in date_headers]
        total_meals     = sum(counts_by_day)
        days_with_meal  = sum(1 for c in counts_by_day if c)
        extra_meals     = total_meals - days_with_meal
        amounts         = compute_amounts(info["employee_type"],
                                          days_with_meal, extra_meals, total_meals)

        final_data.append({
            "id":            emp_id,
            "employee_name": info["employee_name"],
            "employee_type": info["employee_type"],
            "date_strings":  [str(c or 0) for c in counts_by_day],
            "meal_count":    total_meals,
            "total":         amounts["total"],
            "contrib_40":    amounts["emp"],
            "contrib_60":    amounts["comp"],
        })

    # ---- DAILY grand totals -------------------------------------------------
    grand_meals      = sum(r["meal_count"] for r in final_data)
    grand_total      = sum(r["total"]      for r in final_data)
    grand_contrib_40 = sum(r["contrib_40"] for r in final_data)
    grand_contrib_60 = sum(r["contrib_60"] for r in final_data)

    grand_day_counts = [
        sum(int(r["date_strings"][i]) for r in final_data)
        for i in range(len(date_headers))
    ] if final_data else [0] * len(date_headers)

    # ---- MONTHLY summary ----------------------------------------------------
    monthly_data = []
    if filter_type == "monthly":
        for emp_id, info in data.items():
            # apply same employee-type filter
            if emp_group == "regular" and info["employee_type"] == "Casual":
                continue
            if emp_group == "casual"  and info["employee_type"] != "Casual":
                continue

            total_meals   = sum(info["date_counts"].values())
            days_with_meal = sum(1 for v in info["date_counts"].values() if v)
            extra_meals   = total_meals - days_with_meal
            amounts       = compute_amounts(info["employee_type"],
                                            days_with_meal, extra_meals, total_meals)

            monthly_data.append({
                "id":            emp_id,
                "employee_name": info["employee_name"],
                "employee_type": info["employee_type"],
                "meal_count":    total_meals,
                "total":         amounts["total"],
                "contrib_40":    amounts["emp"],
                "contrib_60":    amounts["comp"],
            })

        monthly_grand_meals      = sum(r["meal_count"] for r in monthly_data)
        monthly_grand_total      = sum(r["total"]      for r in monthly_data)
        monthly_grand_contrib_40 = sum(r["contrib_40"] for r in monthly_data)
        monthly_grand_contrib_60 = sum(r["contrib_60"] for r in monthly_data)
    else:
        monthly_data               = []
        monthly_grand_meals        = grand_meals
        monthly_grand_total        = grand_total
        monthly_grand_contrib_40   = grand_contrib_40
        monthly_grand_contrib_60   = grand_contrib_60

    # ---- render -------------------------------------------------------------
    months = [(i, datetime(2025, i, 1).strftime("%B")) for i in range(1, 13)]
    years  = [2024, 2025, 2026]

    return render(request, "canteen/canteen_attendance_summary.html", {
        "user_groups":               user_groups,
        "is_superuser":              is_superuser,
        "filter_type":               filter_type,
        "emp_group":                 emp_group,          # ▶︎ for template
        "report_data":               final_data,
        "monthly_data":              monthly_data,
        "date_headers":              date_headers,
        "months":                    months,
        "years":                     years,
        "selected_month":            selected_month,
        "selected_year":             selected_year,
        "grand_meals":               grand_meals,
        "grand_total":               grand_total,
        "grand_contrib_40":          grand_contrib_40,
        "grand_contrib_60":          grand_contrib_60,
        "grand_day_counts":          grand_day_counts,
        "monthly_grand_meals":       monthly_grand_meals,
        "monthly_grand_total":       monthly_grand_total,
        "monthly_grand_contrib_40":  monthly_grand_contrib_40,
        "monthly_grand_contrib_60":  monthly_grand_contrib_60,
    })


@login_required
def download_canteen_excel(request):
    logger.info("User=%s download Canteen Bill report", request.user.username)
    selected_month = int(request.GET.get('month', datetime.today().month))
    selected_year = int(request.GET.get('year', datetime.today().year))
    filter_type = request.GET.get('filter_type', 'monthly')  # default to monthly
    emp_group = request.GET.get('emp_group', 'all')

    start_date = datetime(selected_year, selected_month, 1)
    next_month = start_date.replace(day=28) + timedelta(days=4)
    end_date = next_month - timedelta(days=next_month.day)

    date_headers = []
    current_date = start_date
    while current_date <= end_date:
        date_headers.append(current_date.date())
        current_date += timedelta(days=1)

    valid_shifts = Shift.objects.filter(
        name__in=["Lunch F", "Lunch G", "Dinner", "Evening Dinner"]
    ).values_list('id', flat=True)

    # Also fetch employee_type
    attendances = Attendance.objects.filter(
        punched_at__date__range=(start_date.date(), end_date.date()),
        shift_id__in=valid_shifts
    ).values(
        'employee__id', 'employee__name', 'employee__employee_type', 'punched_at__date'
    ).annotate(count=Count('id'))

    data = defaultdict(lambda: {
        'employee_name': '',
        'employee_type': '',
        'date_counts': defaultdict(int)
    })

    for item in attendances:
        emp_id = item['employee__id']
        emp_name = item['employee__name']
        emp_type = item['employee__employee_type'] or 'Company'
        punch_date = item['punched_at__date']
        data[emp_id]['employee_name'] = emp_name
        data[emp_id]['employee_type'] = emp_type
        data[emp_id]['date_counts'][punch_date] += item['count']

    # === FILTER BY emp_group ===
    def include_row(emp_type):
        emp_type = (emp_type or 'Company').lower()
        if emp_group == 'all':
            return True
        elif emp_group == 'regular':
            return emp_type in ['company', 'trainee', 'guest']
        elif emp_group == 'casual':
            return emp_type == 'casual'
        return True

    RATE_COMPANY = 73
    RATE_CASUAL = 49

    def compute_amounts(emp_type, days_with_meal, extra_meals, total_meals):
        if emp_type == "Casual":
            total = total_meals * RATE_CASUAL
            # Place in company contribution, emp_contrib=0
            return {"emp": 0, "comp": total, "total": total}
        if emp_type == "Guest":
            total = total_meals * RATE_COMPANY
            return {"emp": 0, "comp": total, "total": total}
            
        emp_share = round(days_with_meal * RATE_COMPANY * 0.40, 2)
        comp_share = round(days_with_meal * RATE_COMPANY * 0.60 +
                           extra_meals * RATE_COMPANY, 2)
        total = total_meals * RATE_COMPANY
        return {"emp": emp_share, "comp": comp_share, "total": total}

    # =========================
    # MONTHLY SUMMARY (default)
    # =========================
    if filter_type == 'monthly':
        excel_data = []
        for emp_id, info in data.items():
            if not include_row(info['employee_type']):
                continue
            total_meals = sum(info['date_counts'].values())
            days_with_meal = sum(1 for v in info['date_counts'].values() if v > 0)
            extra_meals = total_meals - days_with_meal
            amounts = compute_amounts(info['employee_type'], days_with_meal, extra_meals, total_meals)
            excel_data.append({
                'id': emp_id,
                'employee_name': info['employee_name'],
                'employee_type': info['employee_type'],
                'meal_count': total_meals,
                'contrib_40': amounts['emp'],
                'contrib_60': amounts['comp'],
                'total': amounts['total'],
            })

        grand_meals = sum(row['meal_count'] for row in excel_data)
        grand_total = sum(row['total'] for row in excel_data)
        grand_contrib_40 = sum(row['contrib_40'] for row in excel_data)
        grand_contrib_60 = sum(row['contrib_60'] for row in excel_data)

        # Excel
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet("Canteen Summary")

        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#C6E0B4', 'align': 'center', 'border': 1})
        total_fmt = workbook.add_format({'bold': True, 'bg_color': '#FFF2CC', 'align': 'center', 'border': 1})

        headers = ['ID', 'Employee Name', 'Employee Type', 'Total Meals', 'Employee Contribution ₹', 'Company Contribution ₹', 'Total ₹']
        for col_num, header in enumerate(headers):
            worksheet.write(0, col_num, header, header_fmt)

        for row_num, row in enumerate(excel_data, start=1):
            padded_id = str(row['id']).zfill(5)
            worksheet.write(row_num, 0, padded_id)
            worksheet.write(row_num, 1, row['employee_name'])
            worksheet.write(row_num, 2, row['employee_type'])
            worksheet.write(row_num, 3, row['meal_count'])
            worksheet.write(row_num, 4, row['contrib_40'] if row['employee_type'] != "Casual" else '—')
            worksheet.write(row_num, 5, row['contrib_60'])
            worksheet.write(row_num, 6, row['total'])

        # Grand total row
        total_row = len(excel_data) + 1
        worksheet.write(total_row, 0, '', total_fmt)
        worksheet.write(total_row, 1, 'Grand Total', total_fmt)
        worksheet.write(total_row, 2, '', total_fmt)
        worksheet.write(total_row, 3, grand_meals, total_fmt)
        worksheet.write(total_row, 4, grand_contrib_40, total_fmt)
        worksheet.write(total_row, 5, grand_contrib_60, total_fmt)
        worksheet.write(total_row, 6, grand_total, total_fmt)

        workbook.close()
        output.seek(0)

        filename = f"Canteen_Monthly_Summary_{selected_month:02d}_{selected_year}.xlsx"
        response = HttpResponse(
            output.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response

    # =====================
    # DAILY DETAILED EXPORT
    # =====================
    else:
        final_data = []
        for emp_id, info in data.items():
            if not include_row(info['employee_type']):
                continue
            date_values = [info['date_counts'].get(d, 0) for d in date_headers]
            date_strings = [str(v) if v > 0 else "0" for v in date_values]
            meal_count = sum(date_values)
            days_with_meal = sum(1 for v in date_values if v > 0)
            extra_meals = meal_count - days_with_meal
            amounts = compute_amounts(info['employee_type'], days_with_meal, extra_meals, meal_count)
            final_data.append({
                'id': emp_id,
                'employee_name': info['employee_name'],
                'employee_type': info['employee_type'],
                'date_strings': date_strings,
                'meal_count': meal_count,
                'contrib_40': amounts['emp'],
                'contrib_60': amounts['comp'],
                'total': amounts['total'],
            })

        grand_total = sum(row['total'] for row in final_data)
        grand_contrib_40 = sum(row['contrib_40'] for row in final_data)
        grand_contrib_60 = sum(row['contrib_60'] for row in final_data)
        grand_day_counts = []
        for idx in range(len(date_headers)):
            count = sum(int(row['date_strings'][idx]) for row in final_data)
            grand_day_counts.append(count)

        # Excel export
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet("Attendance Summary")

        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#C6E0B4', 'align': 'center', 'border': 1})
        total_fmt = workbook.add_format({'bold': True, 'bg_color': '#FFF2CC', 'align': 'center', 'border': 1})

        headers = (
            ['ID', 'Employee Name', 'Type'] +
            [d.strftime("%d-%b") for d in date_headers] +
            ['Total Meals', 'Employee Contribution ₹', 'Company Contribution ₹', 'Total ₹']
        )

        for col_num, header in enumerate(headers):
            worksheet.write(0, col_num, header, header_fmt)

        for row_num, row in enumerate(final_data, start=1):
            padded_id = str(row['id']).zfill(5)
            worksheet.write(row_num, 0, padded_id)
            worksheet.write(row_num, 1, row['employee_name'])
            worksheet.write(row_num, 2, row['employee_type'])
            for col_offset, val in enumerate(row['date_strings'], start=3):
                worksheet.write(row_num, col_offset, val)
            meals_col = 3 + len(date_headers)
            worksheet.write(row_num, meals_col, row['meal_count'])
            worksheet.write(row_num, meals_col + 1, row['contrib_40'] if row['employee_type'] != "Casual" else '—')
            worksheet.write(row_num, meals_col + 2, row['contrib_60'])
            worksheet.write(row_num, meals_col + 3, row['total'])

        # Grand total row
        total_row = len(final_data) + 1
        worksheet.write(total_row, 0, '', total_fmt)
        worksheet.write(total_row, 1, 'Grand Total', total_fmt)
        worksheet.write(total_row, 2, '', total_fmt)
        for c, count in enumerate(grand_day_counts, start=3):
            worksheet.write(total_row, c, count, total_fmt)
        worksheet.write(total_row, meals_col, sum(r['meal_count'] for r in final_data), total_fmt)
        worksheet.write(total_row, meals_col + 1, grand_contrib_40, total_fmt)
        worksheet.write(total_row, meals_col + 2, grand_contrib_60, total_fmt)
        worksheet.write(total_row, meals_col + 3, grand_total, total_fmt)

        workbook.close()
        output.seek(0)

        filename = f"Canteen_Daily_Report_{selected_month:02d}_{selected_year}.xlsx"
        response = HttpResponse(
            output.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response





@login_required
def attendance_list(request):
    logger.info("User=%s accessed Canteen Records", request.user.username)
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    # --- Get all filter parameters ---
    from_str = request.GET.get('from_date', '').strip()
    to_str = request.GET.get('to_date', '').strip()
    name_q = request.GET.get('name', '').strip()
    dept_q = request.GET.get('department', '').strip()
    employee_type_q = request.GET.get('employee_type', '').strip()   # ✅ now expects: "", "CTG", "Casual"
    shift_type_q = request.GET.get('shift_type', '').strip()

    # --- Timezone-aware date parsing ---
    today = datetime.now(LOCAL_TIMEZONE).date()
    try:
        from_date = datetime.strptime(from_str, '%Y-%m-%d').date() if from_str else today
        to_date = datetime.strptime(to_str, '%Y-%m-%d').date() if to_str else today
    except ValueError:
        from_date = to_date = today

    start_local = LOCAL_TIMEZONE.localize(datetime.combine(from_date, time.min))
    end_local   = LOCAL_TIMEZONE.localize(datetime.combine(to_date, time.max))
    start_utc   = start_local.astimezone(pytz.utc)
    end_utc     = end_local.astimezone(pytz.utc)

    # --- Base queryset ---
    qs = Attendance.objects.filter(punched_at__range=(start_utc, end_utc))

    # --- Apply filters ---
    food_shifts = ["Lunch F", "Lunch G", "Dinner", "Evening Dinner"]
    tea_shifts  = ["Morning Tea", "Evening Tea", "Night Tea"]

    if shift_type_q == 'food':
        qs = qs.filter(shift__name__in=food_shifts)
    elif shift_type_q == 'tea':
        qs = qs.filter(shift__name__in=tea_shifts)

    if name_q:
        qs = qs.filter(employee__name__icontains=name_q)

    if dept_q:
        qs = qs.filter(employee__department_id=dept_q)

    # ✅ Employee Type filter grouping
    if employee_type_q == "Casual":
        qs = qs.filter(employee__employee_type="Casual")
    elif employee_type_q == "CTG":
        qs = qs.filter(
            Q(employee__employee_type__in=["Company", "Trainee", "Guest"])
            | Q(employee__employee_type__isnull=True)
            | Q(employee__employee_type__exact="")
        )

    attendances = qs.select_related('employee__department', 'shift').order_by('-punched_at')

    # --- Counters for Cards ---
    def norm_type(v):
        v = (v or "").strip()
        return v if v else "Company"

    # group Company + Trainee + Guest into one bucket
    group_count = 0
    casual_count = 0
    for att in attendances:
        t = norm_type(att.employee.employee_type)
        if t == "Casual":
            casual_count += 1
        elif t in ("Company", "Trainee", "Guest"):
            group_count += 1
        else:
            # if any other type appears, treat it in the group (optional)
            group_count += 1

    shift_counter = Counter(att.shift.name for att in attendances)
    total_attendance = attendances.count()

    # --- Meta info for Employee Type Cards ---
    EMP_TYPE_META = {
        "CTG":    {"label": "Company / Trainee / Guest", "icon": "fa-user-group", "bg": "bg-green-50", "txt": "text-green-700"},
        "Casual": {"label": "Casual",                   "icon": "fa-user-clock", "bg": "bg-pink-50",  "txt": "text-pink-600"},
    }

    # --- Meta info for Shift Cards ---
    SHIFT_META = {
        "Evening Dinner": {"icon": "fa-utensils", "bg": "bg-orange-50", "txt": "text-orange-700"},
        "Dinner":         {"icon": "fa-utensils", "bg": "bg-red-50",    "txt": "text-red-700"},
        "Lunch F":        {"icon": "fa-utensils", "bg": "bg-cyan-50",   "txt": "text-cyan-700"},
        "Lunch G":        {"icon": "fa-utensils", "bg": "bg-teal-50",   "txt": "text-teal-700"},
        "Morning Tea":    {"icon": "fa-coffee",   "bg": "bg-lime-50",   "txt": "text-lime-700"},
        "Evening Tea":    {"icon": "fa-coffee",   "bg": "bg-amber-50",  "txt": "text-amber-700"},
        "Night Tea":      {"icon": "fa-coffee",   "bg": "bg-indigo-50", "txt": "text-indigo-700"},
    }

    # --- Build Employee Cards ---
    employee_cards = [
        {"type": "Total", "count": total_attendance, "label": "Total", "icon": "fa-users", "bg": "bg-blue-50", "txt": "text-blue-800"}
    ]
    if group_count:
        employee_cards.append({"type": "CTG", "count": group_count, **EMP_TYPE_META["CTG"]})
    if casual_count:
        employee_cards.append({"type": "Casual", "count": casual_count, **EMP_TYPE_META["Casual"]})

    # --- Build Shift Cards ---
    shift_cards = []
    for shift_name, count in sorted(shift_counter.items()):
        meta = SHIFT_META.get(shift_name, {"icon": "fa-clock", "bg": "bg-gray-50", "txt": "text-gray-700"})
        if count:
            shift_cards.append({"type": shift_name, "count": count, "label": shift_name, **meta})

    department_choices = Department.objects.all()

    # ✅ Updated choices for dropdown
    employee_type_choices = [
        ("", "All"),
        ("CTG", "Company / Trainee / Guest"),
        ("Casual", "Casual"),
    ]

    return render(request, 'canteen/attendance_list.html', {
        'attendances': attendances,
        'from_date': from_date,
        'to_date': to_date,
        'name_q': name_q,
        'dept_q': dept_q,
        'employee_type_q': employee_type_q,
        'shift_type_q': shift_type_q,
        'department_choices': department_choices,
        'employee_type_choices': employee_type_choices,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'employee_cards': employee_cards,
        'shift_cards': shift_cards,
    })



@login_required
def attendance_xlsx(request):
    logger.info("User=%s Download Canteen Records", request.user.username)
    from_str = request.GET.get('from_date', '').strip()
    to_str = request.GET.get('to_date', '').strip()
    name_q = request.GET.get('name', '').strip()
    dept_q = request.GET.get('department', '').strip()
    employee_type_q = request.GET.get('employee_type', '').strip()   # ✅ "", "CTG", "Casual"
    shift_type_q = request.GET.get('shift_type', '').strip()

    today = datetime.now(LOCAL_TIMEZONE).date()
    try:
        from_date = datetime.strptime(from_str, '%Y-%m-%d').date() if from_str else today
        to_date = datetime.strptime(to_str, '%Y-%m-%d').date() if to_str else today
    except ValueError:
        from_date = to_date = today

    start_local = LOCAL_TIMEZONE.localize(datetime.combine(from_date, time.min))
    end_local = LOCAL_TIMEZONE.localize(datetime.combine(to_date, time.max))
    start_utc = start_local.astimezone(pytz.utc)
    end_utc = end_local.astimezone(pytz.utc)

    qs = Attendance.objects.filter(punched_at__range=(start_utc, end_utc))

    food_shifts = ["Lunch F", "Lunch G", "Dinner", "Evening Dinner"]
    tea_shifts  = ["Morning Tea", "Evening Tea", "Night Tea"]
    if shift_type_q == 'food':
        qs = qs.filter(shift__name__in=food_shifts)
    elif shift_type_q == 'tea':
        qs = qs.filter(shift__name__in=tea_shifts)

    if name_q:
        qs = qs.filter(employee__name__icontains=name_q)
    if dept_q:
        qs = qs.filter(employee__department_id=dept_q)

    # ✅ same grouping logic
    if employee_type_q == "Casual":
        qs = qs.filter(employee__employee_type="Casual")
    elif employee_type_q == "CTG":
        qs = qs.filter(
            Q(employee__employee_type__in=["Company", "Trainee", "Guest"])
            | Q(employee__employee_type__isnull=True)
            | Q(employee__employee_type__exact="")
        )

    attendances = qs.select_related('employee__department', 'shift').order_by('punched_at')

    # --- Excel generation unchanged ---
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {'in_memory': True, 'remove_timezone': True})
    sheet = workbook.add_worksheet("Attendance")

    hdr_fmt = workbook.add_format({'bold': True, 'bg_color': '#F0F0F0'})
    headers = ['Employee ID', 'Employee Name', 'Department', 'Employee Type', 'Punched At', 'Shift', 'Meal Type']
    for col, title in enumerate(headers):
        sheet.write(0, col, title, hdr_fmt)

    dt_fmt = workbook.add_format({'num_format': 'yyyy-mm-dd hh:mm'})
    for row_idx, att in enumerate(attendances, start=1):
        emp_id = str(att.employee.id).zfill(5)
        sheet.write_string(row_idx, 0, emp_id)
        sheet.write_string(row_idx, 1, att.employee.name)
        sheet.write_string(row_idx, 2, att.employee.department.name)
        sheet.write_string(row_idx, 3, att.employee.employee_type or 'Company')
        sheet.write_datetime(row_idx, 4, att.punched_at.astimezone(LOCAL_TIMEZONE), dt_fmt)
        sheet.write_string(row_idx, 5, att.shift.name if att.shift else '-')
        sheet.write_string(row_idx, 6, att.meal_type or '-')

    workbook.close()
    output.seek(0)

    fname = f"canteen_attendance_{from_date:%Y%m%d}_{to_date:%Y%m%d}.xlsx"
    response = HttpResponse(
        output.read(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="{fname}"'
    return response




















'''===========      Canteen Head count Machine ========================='''


@login_required
def fetch_canteen_headcount_from_device(request):
    """
    API endpoint to trigger the canteen headcount sync task.
    """
    logger.info("User='%s' triggered Canteen HeadCount Sync task.", request.user.username)
    task = Canteen_Head_Count_sync.delay()
    logger.info(
        "Canteen HeadCount Sync TASK queued | task_id=%s | user=%s",
        task.id, request.user.username
    )
    return JsonResponse({"ok": True, "task_id": task.id})


@login_required
def ch_count_dashboard(request):
    # ---- Permission check ----
    if not request.user.has_perm("CANTEEN.view_canteenheadcount"):
        messages.error(request, "You do not have permission to view Canteen Headcount dashboard.")
        logger.warning(
            "User '%s' tried to access Canteen Headcount dashboard without permission.",
            request.user.username,
        )
        return redirect("indexpage")

    user_groups = request.user.groups.values_list("name", flat=True)
    is_superuser = request.user.is_superuser
    show_admin_panel = is_superuser or (request.user.is_staff and request.user.is_active)

    from_str = (request.GET.get("from_date") or "").strip()
    to_str = (request.GET.get("to_date") or "").strip()

    logger.info(
        "User='%s' opened Canteen Headcount Dashboard | from_date='%s' to_date='%s'",
        request.user.username, from_str, to_str
    )

    now_local = datetime.now(LOCAL_TIMEZONE)

    start_local = None
    end_local = None
    if from_str:
        try:
            dt = datetime.strptime(from_str, "%Y-%m-%d")
            start_local = LOCAL_TIMEZONE.localize(
                dt.replace(hour=0, minute=0, second=0, microsecond=0)
            )
        except ValueError:
            logger.warning(
                "Invalid from_date in dashboard | user=%s | from_date='%s'",
                request.user.username, from_str
            )

    if not start_local:
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    if to_str:
        try:
            dt = datetime.strptime(to_str, "%Y-%m-%d")
            end_local = LOCAL_TIMEZONE.localize(
                dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            )
        except ValueError:
            logger.warning(
                "Invalid to_date in dashboard | user=%s | to_date='%s'",
                request.user.username, to_str
            )

    if not end_local:
        end_local = now_local.replace(hour=23, minute=59, second=59, microsecond=999999)

    start_utc = start_local.astimezone(pytz.utc)
    end_utc = end_local.astimezone(pytz.utc)

    attendances_qs = (
        CanteenHeadCount.objects
        .filter(punched_at__gte=start_utc, punched_at__lte=end_utc)
        .select_related("employee__department")
        .order_by("-punched_at")
    )

    total_rows = attendances_qs.count()
    logger.info(
        "Canteen Dashboard Query | user=%s | start_utc=%s end_utc=%s | rows=%s",
        request.user.username,
        start_utc.isoformat(),
        end_utc.isoformat(),
        total_rows,
    )

    # format employee ID
    attendances_formatted = []
    for a in attendances_qs:
        emp_id_str = str(a.employee.id)
        if emp_id_str.upper().startswith("T"):
            a.formatted_id = emp_id_str
        else:
            a.formatted_id = emp_id_str.zfill(5)
        attendances_formatted.append(a)

    return render(
        request,
        "canteen/ch_canteen_dashboard.html",
        {
            "attendances": attendances_formatted,
            "user_groups": user_groups,
            "is_superuser": is_superuser,
            "show_admin_panel": show_admin_panel,
            "from_date": from_str,
            "to_date": to_str,
        },
    )


@csrf_exempt
def ch_update_attendance(request):
    """
    API endpoint to receive attendance data via POST.
    Rules:
      - no shift
      - skip if same punched_at UTC exists
      - skip if any punch already exists for that employee on that local day
    """
    logger.info(
        "Canteen API ch_update_attendance HIT | method=%s | path=%s | remote=%s",
        request.method,
        getattr(request, "path", ""),
        request.META.get("REMOTE_ADDR"),
    )

    if request.method != "POST":
        logger.warning(
            "Canteen API ch_update_attendance rejected (non-POST) | method=%s | remote=%s",
            request.method,
            request.META.get("REMOTE_ADDR"),
        )
        return JsonResponse(
            {"status": "error", "message": "Only POST method allowed."},
            status=405,
        )

    try:
        raw_body = (request.body or b"")[:2000]  # avoid dumping huge payload
        data = json.loads(request.body)
    except json.JSONDecodeError:
        logger.error(
            "Canteen API invalid JSON payload | remote=%s | body_preview=%s",
            request.META.get("REMOTE_ADDR"),
            raw_body,
        )
        return JsonResponse(
            {"status": "error", "message": "Invalid JSON payload."},
            status=400,
        )

    employee_id = data.get("employee_id")
    punched_at_str = data.get("punched_at")
    meal_type = data.get("meal_type", "Meal")

    if not employee_id or not punched_at_str:
        logger.warning(
            "Canteen API missing required fields | employee_id=%s punched_at=%s remote=%s",
            employee_id,
            punched_at_str,
            request.META.get("REMOTE_ADDR"),
        )
        return JsonResponse(
            {"status": "error", "message": "Missing employee_id or punched_at."},
            status=400,
        )

    try:
        # normalise ISO string with 'Z'
        orig_punched_str = punched_at_str
        if punched_at_str.endswith("Z"):
            punched_at_str = punched_at_str[:-1] + "+00:00"

        punched_at = datetime.fromisoformat(punched_at_str)

        if punched_at.tzinfo is None:
            punched_at_local = LOCAL_TIMEZONE.localize(punched_at)
        else:
            punched_at_local = punched_at.astimezone(LOCAL_TIMEZONE)

        punched_at_utc = punched_at_local.astimezone(pytz.utc)

        logger.info(
            "Canteen API parsed payload | employee_id=%s meal_type=%s punched_at_in=%s punched_local=%s punched_utc=%s",
            employee_id,
            meal_type,
            orig_punched_str,
            punched_at_local.isoformat(),
            punched_at_utc.isoformat(),
        )

        employee = Employee.objects.filter(id=employee_id).first()
        if not employee:
            logger.warning(
                "Canteen API employee not found | employee_id=%s",
                employee_id,
            )
            return JsonResponse(
                {"status": "error", "message": f"Employee with id {employee_id} not found."},
                status=404,
            )

        # exact duplicate
        if CanteenHeadCount.objects.filter(employee=employee, punched_at=punched_at_utc).exists():
            logger.info(
                "Canteen API skipped exact duplicate | employee_id=%s punched_utc=%s",
                employee_id,
                punched_at_utc.isoformat(),
            )
            return JsonResponse(
                {"status": "error", "message": "Duplicate record (same time)."},
                status=400,
            )

        # same-day duplicate
        punch_date = punched_at_local.date()
        start_local = datetime.combine(punch_date, time.min)
        end_local = start_local + timedelta(days=1)

        start_utc = LOCAL_TIMEZONE.localize(start_local).astimezone(pytz.utc)
        end_utc = LOCAL_TIMEZONE.localize(end_local).astimezone(pytz.utc)

        if CanteenHeadCount.objects.filter(
            employee=employee,
            punched_at__gte=start_utc,
            punched_at__lt=end_utc,
        ).exists():
            logger.info(
                "Canteen API skipped same-day punch | employee_id=%s punch_date=%s window_utc=%s..%s",
                employee_id,
                str(punch_date),
                start_utc.isoformat(),
                end_utc.isoformat(),
            )
            return JsonResponse(
                {"status": "error", "message": "Already punched for this day."},
                status=400,
            )

        new_attendance = CanteenHeadCount.objects.create(
            employee=employee,
            punched_at=punched_at_utc,
            meal_type=meal_type,
        )

        logger.info(
            "Canteen API saved attendance | attendance_id=%s employee_id=%s punched_utc=%s meal_type=%s",
            new_attendance.id,
            employee_id,
            punched_at_utc.isoformat(),
            meal_type,
        )

        return JsonResponse({"status": "success", "attendance_id": new_attendance.id})

    except Exception as e:
        logger.exception( "Canteen API ERROR | employee_id=%s punched_at=%s meal_type=%s err=%s",
            employee_id,  punched_at_str,  meal_type,  str(e),  )
        return JsonResponse( {"status": "error", "message": f"Internal error: {str(e)}"},  status=500,  )


@csrf_exempt
def ch_device_push(request):
    """
    API endpoint to push attendance from device.
    Rules:
      - no shift
      - at most one punch per employee per day
    """
    if request.method != 'POST':
        return JsonResponse({"status": "error", "message": "Only POST method allowed."}, status=405)

    try:
        data = json.loads(request.body)
        user_id = data.get("UserID")
        punch_time_str = data.get("Timestamp")

        if not user_id or not punch_time_str:
            return JsonResponse({"status": "error", "message": "Missing UserID or Timestamp."}, status=400)

        punch_time = datetime.strptime(punch_time_str, "%Y-%m-%d %H:%M:%S")

        if punch_time.tzinfo is None:
            punch_time_local = LOCAL_TIMEZONE.localize(punch_time)
        else:
            punch_time_local = punch_time.astimezone(LOCAL_TIMEZONE)

        punch_time_utc = punch_time_local.astimezone(pytz.utc)

        employee = Employee.objects.filter(id=user_id).first()
        if not employee:
            return JsonResponse({"status": "error", "message": "Employee not found"}, status=404)

        punch_date = punch_time_local.date()
        start_local = datetime.combine(punch_date, time.min)
        end_local = start_local + timedelta(days=1)
        start_utc = LOCAL_TIMEZONE.localize(start_local).astimezone(pytz.utc)
        end_utc = LOCAL_TIMEZONE.localize(end_local).astimezone(pytz.utc)

        # skip if already punched that day
        if CanteenHeadCount.objects.filter(
            employee=employee,
            punched_at__gte=start_utc,
            punched_at__lt=end_utc
        ).exists():
            return JsonResponse({
                "status": "duplicate",
                "message": f"{employee.name} already punched on {punch_date}.",
            }, status=200)

        CanteenHeadCount.objects.create(
            employee=employee,
            punched_at=punch_time_utc,
        )
        return JsonResponse({
            "status": "ok",
            "employee": employee.name,
            "message": f"Attendance saved for {employee.name} at {punch_time_local.strftime('%I:%M %p')}",
        }, status=200)

    except Exception as e:
        return JsonResponse({"status": "error", "message": str(e)}, status=500)



@require_GET
def ch_dashboard_api_data(request):
    from_str = request.GET.get("from_date", "").strip()
    to_str = request.GET.get("to_date", "").strip()
    now_local = datetime.now(LOCAL_TIMEZONE)
    start_local = None
    end_local = None

    if from_str:
        try:
            dt = datetime.strptime(from_str, "%Y-%m-%d")
            start_local = LOCAL_TIMEZONE.localize(
                dt.replace(hour=0, minute=0, second=0, microsecond=0)
            )
        except ValueError:
            pass
    if not start_local:
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    if to_str:
        try:
            dt = datetime.strptime(to_str, "%Y-%m-%d")
            end_local = LOCAL_TIMEZONE.localize(
                dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            )
        except ValueError:
            pass
    if not end_local:
        end_local = now_local.replace(hour=23, minute=59, second=59, microsecond=999999)

    start_utc = start_local.astimezone(pytz.utc)
    end_utc = end_local.astimezone(pytz.utc)

    attendances = (
        CanteenHeadCount.objects
        .filter(punched_at__gte=start_utc, punched_at__lte=end_utc)
        .select_related('employee__department')
        .order_by('-punched_at')
    )

    attendance_list = []
    for a in attendances:
        emp_id_str = str(a.employee.id)
        formatted_id = emp_id_str.zfill(5) if not emp_id_str.upper().startswith('T') else emp_id_str

        attendance_list.append({
            'employee_id': formatted_id,
            'employee_name': a.employee.name,
            'department_name': a.employee.department.name if a.employee.department else "N/A",
            'employee_type': a.employee.employee_type or "",
            'meal_type': a.meal_type or "",
            'punched_at_local': a.punched_at.astimezone(LOCAL_TIMEZONE).strftime("%d %b %Y, %I:%M %p"),
        })

    return JsonResponse({
        'attendances': attendance_list,
    })



@login_required
def ch_export_excel(request):
    """
    Export canteen headcount data to Excel for a given date range.
    If from/to not provided, defaults to today's date (same logic as dashboard).
    Uses xlsxwriter.
    """
    from_str = (request.GET.get("from_date") or "").strip()
    to_str = (request.GET.get("to_date") or "").strip()

    now_local = datetime.now(LOCAL_TIMEZONE)

    start_local = None
    end_local = None

    # FROM date
    if from_str:
        try:
            dt = datetime.strptime(from_str, "%Y-%m-%d")
            start_local = LOCAL_TIMEZONE.localize(
                dt.replace(hour=0, minute=0, second=0, microsecond=0)
            )
        except ValueError:
            start_local = None

    if not start_local:
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    # TO date
    if to_str:
        try:
            dt = datetime.strptime(to_str, "%Y-%m-%d")
            end_local = LOCAL_TIMEZONE.localize(
                dt.replace(hour=23, minute=59, second=59, microsecond=999999)
            )
        except ValueError:
            end_local = None

    if not end_local:
        end_local = now_local.replace(hour=23, minute=59, second=59, microsecond=999999)

    start_utc = start_local.astimezone(pytz.utc)
    end_utc = end_local.astimezone(pytz.utc)

    qs = (
        CanteenHeadCount.objects
        .filter(punched_at__gte=start_utc, punched_at__lte=end_utc)
        .select_related('employee__department')
        .order_by('punched_at')
    )

    # ---- Build Excel in memory with xlsxwriter ----
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    worksheet = workbook.add_worksheet("Canteen Headcount")

    # Formats
    title_format = workbook.add_format({
        "bold": True,
        "font_size": 14,
        "align": "center",
        "valign": "vcenter",
    })
    header_format = workbook.add_format({
        "bold": True,
        "bg_color": "#D9E1F2",
        "border": 1,
        "align": "center",
        "valign": "vcenter",
    })
    text_format = workbook.add_format({"border": 1})
    date_format = workbook.add_format({
        "num_format": "dd-mm-yyyy hh:mm",
        "border": 1,
    })

    # ===== Title row (row 0) =====
    title_text = (
        f"Canteen Headcount "
        f"({start_local.strftime('%d-%m-%Y')} to {end_local.strftime('%d-%m-%Y')})"
    )
    worksheet.merge_range(0, 0, 0, 5, title_text, title_format)

    # ===== Header row (row 2) =====
    header_row = 2
    headers = [
        "Employee ID",
        "Employee Name",
        "Department",
        "Employee Type",
        "Meal Type",
        "Punched At (IST)",
    ]
    for col, h in enumerate(headers):
        worksheet.write(header_row, col, h, header_format)

    # ===== Data rows start from row 3 =====
    row = header_row + 1
    for rec in qs:
        emp_id = str(rec.employee.id)
        if not emp_id.upper().startswith("T"):
            emp_id = emp_id.zfill(5)

        dept = rec.employee.department.name if rec.employee.department else ""
        emp_type = rec.employee.employee_type or ""
        meal_type = rec.meal_type or "Meal"

        # timezone-aware -> naive (Excel limitation)
        punched_local = rec.punched_at.astimezone(LOCAL_TIMEZONE)
        punched_local_naive = punched_local.replace(tzinfo=None)

        worksheet.write(row, 0, emp_id, text_format)
        worksheet.write(row, 1, rec.employee.name, text_format)
        worksheet.write(row, 2, dept, text_format)
        worksheet.write(row, 3, emp_type, text_format)
        worksheet.write(row, 4, meal_type, text_format)
        worksheet.write_datetime(row, 5, punched_local_naive, date_format)
        row += 1

    worksheet.set_column(0, 0, 12)
    worksheet.set_column(1, 1, 25)
    worksheet.set_column(2, 2, 20)
    worksheet.set_column(3, 3, 15)
    worksheet.set_column(4, 4, 12)
    worksheet.set_column(5, 5, 22)
    worksheet.freeze_panes(header_row + 1, 0)

    workbook.close()
    output.seek(0)

    filename = f"canteen_headcount_{start_local.date()}_{end_local.date()}.xlsx"
    response = HttpResponse(
        output.getvalue(),
        content_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml."
            "sheet"
        ),
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response







































































'''===========      punching Device  related code ========================='''

from .utils import add_user_to_device,delete_user_from_device,get_all_users_from_device



@login_required
def add_user_page(request):
    return render(request, "add_device_user.html")



@csrf_exempt
def add_device_user(request):
    if request.method != 'POST':
        # print("Request is not POST.")
        return JsonResponse({'status': 'error', 'message': 'Only POST allowed'}, status=405)
    import json
    data = json.loads(request.body)
    # print("Received data:", data)

    ip_address = data.get("ip_address")  # Default for safety
    user_id = data.get("user_id")
    name = data.get("name", "Unknown")
    card_raw = data.get("card")

    # print("Raw values - ip_address:", ip_address, "user_id:", user_id, "name:", name, "card:", card_raw)

    if not ip_address:
        return JsonResponse({'status': 'error', 'message': 'Please select a machine.'}, status=400)

    if not user_id:
        # print("User ID missing.")
        return JsonResponse({'status': 'error', 'message': 'User ID is required.'}, status=400)
    card = None
    if card_raw not in [None, ""]:
        try:
            card = int(card_raw)
        except (TypeError, ValueError):
            # print("Card conversion failed:", card_raw)
            return JsonResponse({'status': 'error', 'message': 'Card number must be an integer.'}, status=400)

    # print("Calling add_user_to_device with:", user_id, name, card, ip_address)

    success, msg = add_user_to_device(
        user_id=user_id,
        name=name,
        card=card,
        ip_address=ip_address
    )
    # print("add_user_to_device result:", success, msg)

    if success:
        return JsonResponse({'status': 'success', 'message': msg})
    else:
        return JsonResponse({'status': 'error', 'message': msg}, status=500)


@csrf_exempt
def delete_device_user(request):
    if request.method != 'POST':
        return JsonResponse({'status': 'error', 'message': 'Only POST allowed'}, status=405)
    import json
    data = json.loads(request.body)
    user_id = data.get("user_id")
    ip_address = data.get("ip_address")
    if not user_id:
        return JsonResponse({'status': 'error', 'message': 'user_id is required'}, status=400)
    if not ip_address:
        return JsonResponse({'status': 'error', 'message': 'ip_address is required'}, status=400)
    success, msg = delete_user_from_device(user_id, ip_address=ip_address)
    return JsonResponse({'status': 'success' if success else 'error', 'message': msg})



def download_device_users_excel(request):
    ip = request.GET.get('ip', '192.168.0.30')  # Default to .30 if not set
    # print("[VIEW] Downloading users for IP:", ip)
    df = get_all_users_from_device(ip_address=ip)
    if df is None or df.empty:
        return HttpResponse("No user data found or unable to connect to device.", status=500)
    import io
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)
    response = HttpResponse(
        buffer,
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = f'attachment; filename="device_users_{ip.replace(".", "_")}.xlsx"'
    return response



@login_required
@require_http_methods(["POST"])
def bulk_upload_users_from_excel(request):
    """
    Handle Excel upload and push users to devices in bulk.
    Expected columns in Excel:
      - IP          (device IP, e.g. 192.168.0.30)
      - UserID      (user_id to set on device)
      - Name        (user name)
      - Card        (optional, card number)
    """
    file = request.FILES.get("excel_file")
    if not file:
        messages.error(request, "Please select an Excel file to upload.")
        return redirect("device_user_add")

    try:
        # Reads the first sheet by default
        df = pd.read_excel(file)
    except Exception as e:
        messages.error(request, f"Could not read Excel file: {e}")
        return redirect("device_user_add")

    # Normalise column names (strip spaces)
    df.columns = [str(c).strip() for c in df.columns]

    required_cols = ["IP", "UserID", "Name"]  # Card is optional
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        messages.error(
            request,
            f"Missing required columns in Excel: {', '.join(missing)}. "
            f"Expected columns: IP, UserID, Name, Card(optional).",
        )
        return redirect("device_user_add")

    success_count = 0
    error_rows = []

    for idx, row in df.iterrows():
        row_num = idx + 2  # Excel row number (assuming row 1 = headers)

        ip = str(row["IP"]).strip()
        user_id = str(row["UserID"]).strip()
        name = str(row["Name"]).strip()

        # Card is optional
        card = None
        if "Card" in df.columns:
            val = row.get("Card")
            if pd.notna(val) and str(val).strip() != "":
                try:
                    card = int(val)
                except Exception:
                    error_rows.append((row_num, "Invalid Card number"))
                    continue

        # Basic validation
        if not ip or not user_id or not name:
            error_rows.append((row_num, "IP / UserID / Name cannot be blank"))
            continue

        ok, msg = add_user_to_device(
            user_id=user_id,
            name=name,
            card=card,
            ip_address=ip,
        )

        if ok:
            success_count += 1
        else:
            error_rows.append((row_num, msg))

    # Messages for user
    if success_count:
        messages.success(request, f"Successfully added {success_count} users to devices.")

    if error_rows:
        # Show only first few error rows in flash message (rest can be logged if needed)
        preview = ", ".join([f"Row {r}: {m}" for r, m in error_rows[:10]])
        if len(error_rows) > 10:
            preview += f" ... and {len(error_rows) - 10} more rows."
        messages.warning(
            request,
            f"Some rows failed while adding users: {preview}",
        )

    if not success_count and not error_rows:
        messages.info(request, "No valid rows found in the file.")

    return redirect("device_user_add")