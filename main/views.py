import logging
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required
from django.contrib.auth import login, logout
from main.forms import LoginForm  # Import your custom LoginForm
from django.contrib import messages
from django.shortcuts import render, redirect
from django.contrib.auth.decorators import login_required, permission_required
from django.contrib.auth import login, logout
from main.forms import LoginForm  # Import your custom LoginForm
from .models import *
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.http import HttpResponse
from django.db.models import Q
from io import BytesIO
import xlsxwriter
from datetime import datetime

# Initialize logger
logger = logging.getLogger('custom_logger')

# This is your main dashboard view after a user logs in.
@login_required
def indexpage(request):
    # ---- TEMPORARILY COMMENT OUT THE TRY/EXCEPT TO DEBUG ----
    # try:
    context = {
        'user_groups': list(request.user.groups.values_list('name', flat=True)),
        'is_superuser': request.user.is_superuser,
        'show_admin_panel': request.user.is_superuser or (request.user.is_staff and request.user.is_active)
    }
    logger.info(f"User '{request.user.username}' trying to access index page.")
    return render(request, 'main/index.html', context)

def LoginPage(request):
    """
    Handles user login.
    If a user is already authenticated, it redirects them to the dashboard.
    It now uses your custom LoginForm.
    """
    # If the user is already logged in, send them to the main page.
    if request.user.is_authenticated:
        logger.info(f"User '{request.user.username}' is already authenticated, redirecting to index.")
        # Use named URL for redirection. Assumes name='indexpage' in urls.py
        return redirect('indexpage')

    if request.method == "POST":
        # Use your custom LoginForm from forms.py
        form = LoginForm(request, data=request.POST)
        if form.is_valid():
            # The form handles the authentication check.
            user = form.get_user()
            login(request, user)
            logger.info(f"User '{user.username}' logged in successfully.")
            messages.success(request, f'Welcome back, {user.username}!')
            return redirect('indexpage') # Use named URL
        else:
            logger.warning("Failed login attempt.")
            messages.error(request, 'Invalid username or password!')
    else:
        form = LoginForm()

    return render(request, 'main/login.html', {'form': form})


@login_required
def User_logout(request):
    """
    Handles user logout.
    """
    username = request.user.username
    logout(request)
    messages.success(request, 'You have been successfully logged out.')
    logger.info(f"User '{username}' logged out.")
    # Redirect to the login page. Assumes name='userlogin' in urls.py
    return redirect('userlogin')

def Signup_Page(request):
    """
    Redirects any signup attempts back to the login page with a message.
    """
    logger.info(f"User '{request.user.username or 'Anonymous'}' attempted to access signup page.")
    messages.info(request, 'Please contact your IT Administrator for account creation.')
    # Redirect to the login page. Assumes name='userlogin' in urls.py
    return redirect('userlogin')


    

# main/views.py
from datetime import timedelta
import csv

from django.contrib.auth.decorators import login_required, user_passes_test
from django.db import models
from django.db.models import F, ExpressionWrapper, DurationField, Q
from django.http import HttpResponse
from django.shortcuts import render
from django.utils import timezone

from .models import UserStatus, LoginActivity


ONLINE_WINDOW_MINUTES = 10  # consider "online" if active within last 10 minutes


def staff_or_superuser(u):
    return u.is_authenticated and (u.is_staff or u.is_superuser)


from django.core.paginator import Paginator

@login_required
@user_passes_test(staff_or_superuser)
def online_users(request):
    """
    Show UserStatus rows.
    Default: only active in last ONLINE_WINDOW_MINUTES.
    Toggle: pass ?all=1 to show ALL users.
    Supports: q (search), from/to (date), CSV export, pagination.
    """
    qs = UserStatus.objects.using('default').select_related("user")

    # Search
    qstr = (request.GET.get("q") or "").strip()
    if qstr:
        qs = qs.filter(
            Q(user__username__icontains=qstr) |
            Q(user__email__icontains=qstr)
        )

    # Toggle: all vs last N minutes
    is_all = (request.GET.get("all") == "1")
    if not is_all:
        window = timezone.now() - timedelta(minutes=ONLINE_WINDOW_MINUTES)
        qs = qs.filter(last_seen__gte=window)

    # Optional date range (applies on top of all/lastN)
    date_from = (request.GET.get("from") or "").strip()
    date_to   = (request.GET.get("to") or "").strip()
    if date_from:
        qs = qs.filter(last_seen__date__gte=date_from)
    if date_to:
        qs = qs.filter(last_seen__date__lte=date_to)

    qs = qs.order_by("-last_seen")

    # CSV export of the filtered set
    if (request.GET.get("export") or "").lower() == "csv":
        import csv
        from django.http import HttpResponse
        resp = HttpResponse(content_type="text/csv")
        fname = "user_status_all.csv" if is_all else f"user_status_last_{ONLINE_WINDOW_MINUTES}m.csv"
        resp["Content-Disposition"] = f'attachment; filename="{fname}"'
        w = csv.writer(resp)
        w.writerow(["Username", "Email", "Last seen (ISO)"])
        for row in qs.iterator():
            u = getattr(row, "user", None)
            w.writerow([
                getattr(u, "username", "") if u else "",
                getattr(u, "email", "") if u else "",
                row.last_seen.isoformat(sep=" ")
            ])
        return resp

    # Pagination (keep lists snappy)
    page_num = request.GET.get("page") or 1
    paginator = Paginator(qs, 100)   # 100 per page
    page_obj = paginator.get_page(page_num)

    context = {
        "title": f"Online Users ({'all' if is_all else f'active in last {ONLINE_WINDOW_MINUTES} min'})",
        "users": page_obj,             # iterable in template
        "page_obj": page_obj,
        "is_all": is_all,
        "q": qstr,
        "date_from": date_from,
        "date_to": date_to,
        "window_minutes": ONLINE_WINDOW_MINUTES,
    }
    return render(request, "main/online_users.html", context)


@login_required
@user_passes_test(staff_or_superuser)
def login_activity(request):
    """
    Login activity browser with optional filters and CSV export.
    Pinned to 'default' DB and materialized before rendering to avoid router re-routing.
    """
    qs = LoginActivity.objects.using('default').select_related("user").all()

    # Text search
    qstr = (request.GET.get("q") or "").strip()
    if qstr:
        qs = qs.filter(
            Q(user__username__icontains=qstr)
            | Q(user__email__icontains=qstr)
            | Q(ip_address__icontains=qstr)
            | Q(user_agent__icontains=qstr)
        )

    # Date range (inclusive)
    date_from = (request.GET.get("from") or "").strip()
    date_to = (request.GET.get("to") or "").strip()
    if date_from:
        qs = qs.filter(login_time__date__gte=date_from)
    if date_to:
        qs = qs.filter(login_time__date__lte=date_to)

    # Failed / Success filter
    show_failed = request.GET.get("failed") or "0"
    if show_failed == "1":
        qs = qs.filter(login_failed=True)
    else:
        qs = qs.filter(login_failed=False)

    # CSV export
    if (request.GET.get("export") or "").lower() == "csv":
        response = HttpResponse(content_type="text/csv")
        response["Content-Disposition"] = 'attachment; filename="login_activity.csv"'
        writer = csv.writer(response)
        writer.writerow(
            ["Username", "Login Time", "Logout Time", "Duration (sec)", "IP", "User Agent", "Failed?"]
        )
        for row in qs.order_by("-login_time").iterator():
            writer.writerow([
                getattr(row.user, "username", "") if row.user_id else "",
                row.login_time.isoformat(sep=" "),
                row.logout_time.isoformat(sep=" ") if row.logout_time else "",
                row.duration_seconds,
                row.ip_address or "",
                (row.user_agent or "")[:500],
                "Yes" if row.login_failed else "No",
            ])
        return response

    # Optional example annotation (not strictly required for template)
    # Keeping the import of DurationField above if you later want to show hh:mm:ss
    qs = qs.annotate(
        end_time=ExpressionWrapper(F("logout_time"), output_field=models.DateTimeField())
    )

    # Materialize before template to pin to 'default'
    activities = list(qs.order_by("-login_time")[:1000])

    context = {
        "activities": activities,
        "q": qstr,
        "date_from": date_from,
        "date_to": date_to,
        "show_failed": show_failed,
    }
    return render(request, "main/login_activity.html", context)





########################  Audit Log  in DB  ################################################


def _filtered_audit_queryset(request):
    """
    Common filter logic for list + Excel export.
    Filters by username (icontains) and app_label (exact or icontains).
    """
    qs = AuditLog.objects.select_related("user").all()

    username = (request.GET.get("username") or "").strip()
    app_label = (request.GET.get("app_label") or "").strip()

    created_from = (request.GET.get("created_from") or "").strip()
    created_to = (request.GET.get("created_to") or "").strip()

    if username:
        qs = qs.filter(user__username__icontains=username)

    if app_label:
        qs = qs.filter(app_label__icontains=app_label)
    if created_from:
        qs = qs.filter(created_at__date__gte=created_from)
    if created_to:
        qs = qs.filter(created_at__date__lte=created_to)

    return qs.order_by("-created_at"), username, app_label, created_from, created_to



@login_required
def audit_log_list(request):
    """
    HTML view – shows audit logs with filters & pagination (10 per page).
    """
    qs, username, app_label, created_from, created_to = _filtered_audit_queryset(request)


    paginator = Paginator(qs, 10)  # 10 rows per page
    page_number = request.GET.get("page", 1)

    try:
        page_obj = paginator.page(page_number)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)
    context = {
    "page_obj": page_obj,
    "username": username,
    "app_label": app_label,
    "created_from": created_from,
    "created_to": created_to,
    }
    return render(request, "main/audit_log_list.html", context)


@login_required
def audit_log_export_excel(request):
    """
    Export current filtered audit logs to Excel (.xlsx) using xlsxwriter.
    Includes a title row and keeps the same filters as the list view,
    with Created At date range filter added.
    """
    # -------------------- Filters --------------------
    qs, username, app_label = AuditLog.objects.select_related("user").all(), "", ""
    username = (request.GET.get("username") or "").strip()
    app_label = (request.GET.get("app_label") or "").strip()
    created_from = (request.GET.get("created_from") or "").strip()
    created_to = (request.GET.get("created_to") or "").strip()

    qs = AuditLog.objects.select_related("user").all()
    if username:
        qs = qs.filter(user__username__icontains=username)
    if app_label:
        qs = qs.filter(app_label__icontains=app_label)
    if created_from:
        qs = qs.filter(created_at__date__gte=created_from)
    if created_to:
        qs = qs.filter(created_at__date__lte=created_to)

    qs = qs.order_by("-created_at")

    # -------------------- Excel Setup --------------------
    output = BytesIO()
    wb = xlsxwriter.Workbook(output, {"in_memory": True})
    ws = wb.add_worksheet("Audit Log")

    # ---------- Formats ----------
    title_fmt = wb.add_format(
        {"bold": True, "font_size": 14, "align": "center", "valign": "vcenter", "font_color": "white", "bg_color": "#1E293B"}
    )
    subtitle_fmt = wb.add_format(
        {"font_size": 9, "align": "left", "valign": "vcenter", "font_color": "#475569"}
    )
    header_fmt = wb.add_format(
        {"bold": True, "font_size": 10, "align": "center", "valign": "vcenter", "border": 1, "bg_color": "#E5E7EB"}
    )
    text_fmt = wb.add_format(
        {"font_size": 10, "align": "left", "valign": "top", "border": 1, "text_wrap": True}
    )

    # ---------- Column Widths ----------
    ws.set_column("A:A", 20)  # Created At
    ws.set_column("B:B", 18)  # User
    ws.set_column("C:C", 10)  # Action
    ws.set_column("D:D", 14)  # App
    ws.set_column("E:E", 16)  # Model
    ws.set_column("F:F", 12)  # Object ID

    # ---------- Title + filters ----------
    ws.merge_range("A1:F1", "Audit Log", title_fmt)

    filters_text = []
    if created_from:
        filters_text.append(f"Created From: {created_from}")
    if created_to:
        filters_text.append(f"Created To: {created_to}")
    if username:
        filters_text.append(f"User: {username}")
    if app_label:
        filters_text.append(f"App: {app_label}")

    filter_line = " | ".join(filters_text) if filters_text else "No filters (all records)"

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws.write("A2", f"{filter_line}   —   Generated at: {generated_at}", subtitle_fmt)
    ws.set_row(0, 24)
    ws.set_row(1, 16)

    # ---------- Header Row ----------
    headers = ["Created At", "User", "Action", "App Label", "Model Name", "Object ID"]
    header_row = 2
    for col_idx, col_name in enumerate(headers):
        ws.write(header_row, col_idx, col_name, header_fmt)

    # ---------- Data Rows ----------
    row = header_row + 1
    for log in qs:
        ws.write(row, 0, log.created_at.strftime("%Y-%m-%d %H:%M:%S"), text_fmt)
        ws.write(row, 1, log.user.username if log.user else "", text_fmt)
        ws.write(row, 2, log.action, text_fmt)
        ws.write(row, 3, log.app_label, text_fmt)
        ws.write(row, 4, log.model_name, text_fmt)
        ws.write(row, 5, log.object_id, text_fmt)
        row += 1

    wb.close()
    output.seek(0)

    response = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = 'attachment; filename="audit_log.xlsx"'
    return response
