from django.shortcuts import render, redirect,get_object_or_404
from django.contrib import messages
from .forms import CredentialApplicationForm,ExtentionListForm, DirectorySearchForm
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from .models import Credentials,ExtentionList
from django.db.models import Q
from django.http import JsonResponse, HttpResponse
from django.views.decorators.http import require_GET
import io
import xlsxwriter
from django.utils.timezone import now
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
import logging, socket

logger = logging.getLogger('custom_logger')

@login_required
def credential_add(request):
    if request.method == 'POST':
        form = CredentialApplicationForm(request.POST)
        if form.is_valid():
            form.save()
            messages.success(request, 'Credential record added successfully.')
            return redirect('credential_add')  # redirect back to form or change as needed
    else:
        form = CredentialApplicationForm()

    return render(request, 'credentials/credential_add.html', {'form': form})


@login_required
def credential_list(request):
    qs = Credentials.objects.all().order_by('-id')

    # Get filter params
    location = request.GET.get('location')
    device = request.GET.get('device')
    lan_ip = request.GET.get('lan_ip')
    wan_ip = request.GET.get('wan_ip')
    url = request.GET.get('url')

    if location:
        qs = qs.filter(location=location)
    if device:
        qs = qs.filter(device__icontains=device)
    if lan_ip:
        qs = qs.filter(lan_ip__icontains=lan_ip)
    if wan_ip:
        qs = qs.filter(wan_ip__icontains=wan_ip)
    if url:
        qs = qs.filter(url__icontains=url)

    paginator = Paginator(qs, 10)  # 10 records per page
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    context = {
        'credentials': page_obj,
        'filter_location': location or '',
        'filter_device': device or '',
        'filter_lan_ip': lan_ip or '',
        'filter_wan_ip': wan_ip or '',
        'filter_url': url or '',
        'location_choices': Credentials.LOCATION_CHOICES,
    }
    return render(request, 'credentials/credential_list.html', context)


@login_required
def credential_edit(request, pk=None):
    if pk:
        instance = get_object_or_404(Credentials, pk=pk)
        heading = "Edit Credential Record"
    else:
        instance = None
        heading = "Add Credential Record"

    if request.method == "POST":
        form = CredentialApplicationForm(request.POST, instance=instance)
        if form.is_valid():
            form.save()
            if pk:
                messages.success(request, "Credential record updated successfully.")
            else:
                messages.success(request, "Credential record added successfully.")
            return redirect('credential_list')  # Redirect to list or change as needed
    else:
        form = CredentialApplicationForm(instance=instance)

    return render(request, "credentials/credential_add.html", {
        "form": form,
        "heading": heading,
    })


@login_required
def credential_delete(request, pk):
    credential = get_object_or_404(Credentials, pk=pk)
    if request.method == "POST":
        credential.delete()
        messages.success(request, "Credential record deleted successfully.")
        return redirect('credential_list')
    # Optional: You can add confirmation template or redirect directly
    return redirect('credential_list')




# ── Below code for Extension List ────────────────────────────────────────────────────────────────

def get_system_ip():
    try:
        return socket.gethostbyname(socket.gethostname())
    except Exception:
        return "Unknown"


@require_GET
def extension_find_by_name(request):
    """AJAX: return entry by exact name (case-insensitive) if it exists."""
    name = (request.GET.get("name") or "").strip()
    if not name:
        return JsonResponse({"found": False})

    try:
        obj = ExtentionList.objects.get(name__iexact=name)
        data = {
            "id": obj.id,
            "name": obj.name,
            "department": obj.department,
            "designation": obj.designation or "",
            "extension_no": obj.extension_no or "",
            "mobile": obj.mobile or "",
            "location": obj.location,
        }
        return JsonResponse({"found": True, "data": data})
    except ExtentionList.DoesNotExist:
        return JsonResponse({"found": False})


def extension_create(request):
    ip = get_system_ip()
    user = request.user.username if request.user.is_authenticated else "anonymous"
    instance = None
    if request.method == "POST":
        instance_id = (request.POST.get("instance_id") or "").strip()
        if instance_id:
            instance = get_object_or_404(ExtentionList, pk=instance_id)
        form = ExtentionListForm(request.POST, instance=instance)
        if form.is_valid():
            obj = form.save()
            logger.info("[Directory][ExtensionCreate][SUCCESS] user=%s ip=%s id=%s name=%s dept=%s",
                user, ip, obj.pk, getattr(obj, "name", None), getattr(obj, "department", None))
            messages.success(request, "Extension updated." if instance else "Extension saved.")
            return redirect("extension_list")
        else:
            logger.warning("[Directory][ExtensionCreate][INVALID] user=%s ip=%s errors=%s",
                user, ip, dict(form.errors) )
    else:
        preload_name = (request.GET.get("name") or "").strip()
        if preload_name:
            try:
                instance = ExtentionList.objects.get(name__iexact=preload_name)
                logger.info("[Directory][ExtensionCreate][GET] user=%s ip=%s preload existing name=%s", user, ip, preload_name)
            except ExtentionList.DoesNotExist:
                instance = None
                logger.info("[Directory][ExtensionCreate][GET] user=%s ip=%s preload new name=%s (not found)", user, ip, preload_name)
        form = ExtentionListForm(instance=instance)
    title = "Edit Entry" if instance else "Add Entry"
    logger.info("[Directory][ExtensionCreate][RENDER] user=%s ip=%s title=%s", user, ip, title)
    # Pass instance so template can set hidden instance_id
    return render(
        request,
        "extensions/extension_form.html",
        {"form": form, "title": title, "instance": instance},
    )


def extension_update(request, pk):
    ip = get_system_ip()
    user = request.user.username if request.user.is_authenticated else "anonymous"
    logger.info("[Directory][ExtensionUpdate][OPEN] user=%s ip=%s pk=%s method=%s", user, ip, pk, request.method)

    obj = get_object_or_404(ExtentionList, pk=pk)

    if request.method == "POST":
        form = ExtentionListForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            logger.info("[Directory][ExtensionUpdate][SUCCESS] user=%s ip=%s pk=%s name=%s dept=%s",
                        user, ip, pk, getattr(obj, "name", None), getattr(obj, "department", None))
            messages.success(request, "Extension updated.")
            return redirect("extension_list")
        else:
            logger.warning("[Directory][ExtensionUpdate][INVALID] user=%s ip=%s pk=%s errors=%s",
                           user, ip, pk, dict(form.errors))
    else:
        form = ExtentionListForm(instance=obj)
    logger.info("[Directory][ExtensionUpdate][RENDER] user=%s ip=%s pk=%s", user, ip, pk)
    # Pass obj as 'instance' so hidden field is set
    return render(
        request,
        "extensions/extension_form.html",
        {"form": form, "title": "Edit Entry", "instance": obj},
    )



def extension_list(request):
    ip = get_system_ip()
    user = request.user.username if request.user.is_authenticated else "anonymous"
    logger.info("[ExtensionList] user=%s ip=%s accessed extension list", user, ip)
    form = DirectorySearchForm(request.GET or None)
    qs = ExtentionList.objects.prefetch_related().order_by("id")
    if form.is_valid():
        q    = form.cleaned_data.get("q") or ""
        dept = form.cleaned_data.get("department") or ""
        desg = form.cleaned_data.get("designation") or ""   # safe even if field removed from form
        loc  = form.cleaned_data.get("location") or ""
        logger.info(
            "[ExtensionList][Filters] user=%s ip=%s q='%s' dept='%s' desg='%s' loc='%s'",
            user, ip, q, dept, desg, loc  )
        if q:
            qs = qs.filter(
                Q(name__icontains=q)
                | Q(mobile__icontains=q)
                | Q(extension_no__icontains=q)
                | Q(department__icontains=q)
                | Q(designation__icontains=q)
            )
        if dept:
            qs = qs.filter(department=dept)
        if desg:
            qs = qs.filter(designation=desg)
        if loc:
            qs = qs.filter(location=loc)
    # ---- Pagination (12 per page) ----
    paginator = Paginator(qs, 12)
    page_number = request.GET.get("page") or 1
    try:
        rows = paginator.page(page_number)
    except PageNotAnInteger:
        rows = paginator.page(1)
    except EmptyPage:
        rows = paginator.page(paginator.num_pages)
    # keep current filters (without page) for page links
    querydict = request.GET.copy()
    querydict.pop("page", None)
    query_string = querydict.urlencode()

    return render(
        request,
        "extensions/extension_list.html",
        {
            "form": form,
            "rows": rows,
            "paginator": paginator,
            "query_string": query_string,
            "request": request,
        },
    )




def extension_export_xlsx(request):
    ip = get_system_ip()
    user = request.user.username if request.user.is_authenticated else "anonymous"
    logger.info("[ExtensionList] user=%s ip=%s download extension list", user, ip)
    form = DirectorySearchForm(request.GET or None)
    qs = ExtentionList.objects.order_by("id")

    if form.is_valid():
        q    = form.cleaned_data.get("q") or ""
        dept = form.cleaned_data.get("department") or ""
        loc  = form.cleaned_data.get("location") or ""
        desg = form.cleaned_data.get("designation") if "designation" in form.fields else None

        if q:
            qs = qs.filter(
                Q(name__icontains=q)
                | Q(mobile__icontains=q)
                | Q(extension_no__icontains=q)
                | Q(department__icontains=q)
                | Q(designation__icontains=q)
            )
        if dept:
            qs = qs.filter(department=dept)
        if desg:
            qs = qs.filter(designation=desg)
        if loc:
            qs = qs.filter(location=loc)

    # Build workbook in memory
    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {"in_memory": True})
    ws = wb.add_worksheet("Extensions")

    # Formats
    title_fmt = wb.add_format({
        "bold": True, "font_size": 16, "align": "center", "valign": "vcenter"
    })
    subtitle_fmt = wb.add_format({
        "italic": True, "font_color": "#6B7280", "align": "center", "valign": "vcenter"
    })
    header = wb.add_format({"bold": True, "bg_color": "#E5E7EB", "border": 1})
    cell   = wb.add_format({"border": 1})
    text   = wb.add_format({"border": 1, "num_format": "@"})  # keep numbers/plus as text

    # Columns & headers
    columns = [
        ("Name",         28),
        ("Department",   22),
        ("Designation",  22),
        ("Extension",    14),
        ("Mobile",       18),
        ("Location",     14),
    ]
    for col_idx, (title, width) in enumerate(columns):
        ws.set_column(col_idx, col_idx, width)

    # Title + subtitle
    last_col = len(columns) - 1
    ws.merge_range(0, 0, 0, last_col, "Extension List", title_fmt)
    ws.merge_range(1, 0, 1, last_col, f"Generated: {now().strftime('%Y-%m-%d %H:%M')}", subtitle_fmt)
    ws.set_row(0, 24)  # taller row for title
    ws.set_row(1, 18)

    # Header row (row index 2)
    header_row = 2
    for col_idx, (title, _) in enumerate(columns):
        ws.write(header_row, col_idx, title, header)

    # Data rows start at row index 3
    row = header_row + 1
    for obj in qs:
        ws.write(row, 0, obj.name or "", text)
        ws.write(row, 1, obj.department or "", cell)
        ws.write(row, 2, obj.designation or "", cell)
        ws.write(row, 3, (obj.extension_no or ""), text)
        ws.write(row, 4, (obj.mobile or ""), text)
        ws.write(row, 5, obj.location or "", cell)
        row += 1

    # Freeze panes below header; add autofilter covering data range
    ws.freeze_panes(header_row + 1, 0)  # freeze top 3 rows
    ws.autofilter(header_row, 0, max(header_row, row - 1), last_col)

    wb.close()
    output.seek(0)

    filename = f"extensions_{now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    resp = HttpResponse(
        output.read(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    resp["Content-Disposition"] = f'attachment; filename="{filename}"'
    return resp

