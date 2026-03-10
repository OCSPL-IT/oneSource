# PERSONAL_CARE/views.py

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect, get_object_or_404
from django.views.decorators.http import require_POST
from django.http import HttpResponseNotAllowed,Http404
from .forms import *
from .models import *
from io import BytesIO
import uuid
from tablib import Dataset
import io
import xlsxwriter
from django.http import HttpResponse,JsonResponse
from django.core.paginator import Paginator
from django.utils.dateparse import parse_date
from django.db.models import Count, OuterRef, Subquery
from django.db.models import Count, Q, F
import datetime as _dt
from datetime import datetime
from django.utils import timezone
from collections import Counter
from django.core.files.storage import default_storage
from .admin import CustomerFollowupResource
import logging


logger = logging.getLogger("custom_logger")

def _options(category):
    """Return distinct subcategory values for a given category."""
    return (
        PersonalCareMaster.objects.filter(category=category)
        .order_by("subcategory")
        .values_list("subcategory", flat=True)
        .distinct()
    )


@login_required
def pc_customer_create(request, pk=None):
    instance = None
    if pk is not None:
        base_qs = restrict_to_executive(
            PC_CustomerMaster.objects.select_related(
                "customer_name",
                "customer_profile",
                "sub_profile",
                "place",
                "city",
                "state",
                "zone",
                "executive_name",
                "source",
            ),
            request.user,
        )
        instance = get_object_or_404(base_qs, pk=pk)

    # ---- Permission checks (add vs edit) ----
    if instance is None:
        # Creating new customer
        if not request.user.has_perm("PERSONAL_CARE.add_pc_customermaster"):
            messages.error(request, "You do not have permission to add Personal Care customers.")
            logger.warning(
                "User '%s' tried to create PC Customer without permission.",
                request.user.username,
            )
            return redirect("indexpage")
    else:
        # Editing existing customer
        if not request.user.has_perm("PERSONAL_CARE.change_pc_customermaster"):
            messages.error(request, "You do not have permission to edit Personal Care customers.")
            logger.warning(
                "User '%s' tried to edit PC Customer (id=%s) without permission.",
                request.user.username,
                instance.pk,
            )
            return redirect("indexpage")
    # ----------------------------------------

    if request.method == "POST":
        form = PCCustomerMasterForm(request.POST, instance=instance)
        if form.is_valid():
            obj = form.save()

            if instance is None:
                messages.success(request, "Customer saved successfully.")
                return redirect("pc_customer_create")
            else:
                messages.success(request, "Customer updated successfully.")
                return redirect("pc_customer_list")
    else:
        form = PCCustomerMasterForm(instance=instance)

    context = {
        "form": form,
        "customer_name_options": _options("Customer Name"),
        "customer_profile_options": _options("Customer Profile"),
        "sub_profile_options": _options("Sub Profile"),
        "designation_options": _options("Designation"),
        "place_options": _options("Place"),
        "city_options": _options("City"),
        "state_options": _options("State"),
        "zone_options": _options("Zone"),
        "executive_options": _options("Executive Name"),
        "source_options": _options("Source"),
        "is_edit": instance is not None,   # <- tells template Add vs Edit
    }
    return render(request, "PERSONAL_CARE/pc_customer_form.html", context)



@login_required
def pc_customer_delete(request, pk):
    # ---- Permission check ----
    if not request.user.has_perm("PERSONAL_CARE.delete_pc_customermaster"):
        messages.error(request, "You do not have permission to delete Personal Care customers.")
        logger.warning(
            "User '%s' tried to delete PC Customer (id=%s) without permission.",
            request.user.username,
            pk,
        )
        return redirect("indexpage")
    # --------------------------
    qs = restrict_to_executive(PC_CustomerMaster.objects.all(), request.user)
    obj = get_object_or_404(qs, pk=pk)

    if request.method == "POST":
        customer_str = str(obj)
        obj.delete()
        logger.info(
            "PC Customer deleted by user '%s' | customer=%s (id=%s)",
            request.user.username,
            customer_str,
            pk,
        )
        messages.success(request, "Customer deleted successfully.")
        return redirect("pc_customer_list")
    # Do not allow GET deletes
    return HttpResponseNotAllowed(["POST"])




def restrict_to_executive(qs, user):
    """
    Normal executive → only their data.
    Management group / superuser / staff → all executives.
    """

    if not user.is_authenticated:
        return qs.none()

    # FULL ACCESS RULE
    if (
        user.is_superuser
        or user.is_staff
        or user.groups.filter(name="PERSONAL_CARE_Management").exists()
    ):
        return qs
    # EXECUTIVE-SPECIFIC ACCESS
    exec_link = getattr(user, "pc_executive", None)
    if exec_link and exec_link.executive_id:
        return qs.filter(executive_name=exec_link.executive)
    # no mapping => no customers
    return qs.none()



@login_required
def pc_customer_list(request):
    qs = PC_CustomerMaster.objects.select_related(
        "customer_name",
        "customer_profile",
        "sub_profile",
        "place",
        "city",
        "state",
        "zone",
        "executive_name",
        "source",
    )

    qs = restrict_to_executive(qs, request.user)

    filters = {
        "followup_from": request.GET.get("followup_from", "").strip(),
        "followup_to": request.GET.get("followup_to", "").strip(),
        "customer_name": request.GET.get("customer_name", "").strip(),
        "customer_profile": request.GET.get("customer_profile", "").strip(),
        "sub_profile": request.GET.get("sub_profile", "").strip(),
        "contact_person": request.GET.get("contact_person", "").strip(),
        "city": request.GET.get("city", "").strip(),
        "state": request.GET.get("state", "").strip(),
        "zone": request.GET.get("zone", "").strip(),
        "executive_name": request.GET.get("executive_name", "").strip(),
        "source": request.GET.get("source", "").strip(),
    }

    # ---- follow-up date FROM–TO range ----
    if filters["followup_from"]:
        d = parse_date(filters["followup_from"])
        if d:
            qs = qs.filter(followup_date__gte=d)   # ✅ change field if your model uses different name

    if filters["followup_to"]:
        d = parse_date(filters["followup_to"])
        if d:
            qs = qs.filter(followup_date__lte=d)   # ✅ change field if your model uses different name

    # ---- other filters ----
    if filters["customer_name"]:
        qs = qs.filter(customer_name__subcategory__icontains=filters["customer_name"])

    if filters["customer_profile"]:
        qs = qs.filter(customer_profile__subcategory__icontains=filters["customer_profile"])

    if filters["sub_profile"]:
        qs = qs.filter(sub_profile__subcategory__icontains=filters["sub_profile"])

    if filters["contact_person"]:
        qs = qs.filter(contact_person__icontains=filters["contact_person"])

    if filters["city"]:
        qs = qs.filter(city__subcategory__icontains=filters["city"])

    if filters["state"]:
        qs = qs.filter(state__subcategory__icontains=filters["state"])

    if filters["zone"]:
        qs = qs.filter(zone__subcategory__icontains=filters["zone"])

    if filters["executive_name"]:
        qs = qs.filter(executive_name__subcategory__icontains=filters["executive_name"])

    if filters["source"]:
        qs = qs.filter(source__subcategory__icontains=filters["source"])

    qs = qs.order_by("-created_at")

    # ✅ counts (after all filters, before pagination)
    total_records = qs.count()
    distinct_customers = qs.values("customer_name_id").distinct().count()  # ✅ distinct customer

    # ---- Pagination (50 per page) ----
    paginator = Paginator(qs, 50)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    # Preserve filters in pagination links
    querydict = request.GET.copy()
    querydict.pop("page", None)
    querystring = querydict.urlencode()

    context = {
        "customers": page_obj,
        "page_obj": page_obj,
        "paginator": paginator,
        "is_paginated": page_obj.has_other_pages(),
        "querystring": querystring,
        "filters": filters,

        # ✅ send counts to template
        "total_records": total_records,
        "distinct_customers": distinct_customers,
        "customer_name_options": _options("Customer Name"),
        "customer_profile_options": _options("Customer Profile"),
        "sub_profile_options": _options("Sub Profile"),
        "city_options": _options("City"),
        "state_options": _options("State"),
        "zone_options": _options("Zone"),
        "executive_options": _options("Executive Name"),
        "source_options": _options("Source"),
    }
    return render(request, "PERSONAL_CARE/pc_customer_list.html", context)


def _safe(obj, attr=None):
    if obj is None:
        return ""
    if attr:
        val = getattr(obj, attr, "")
    else:
        val = obj
    if val is None:
        return ""
    return str(val)


def _filtered_customers(request):
    """Apply all filters and return queryset + filter dict."""
    qs = PC_CustomerMaster.objects.select_related(
        "customer_name",        # ← add this
        "customer_profile",
        "sub_profile",
        "place",
        "city",
        "state",
        "zone",
        "executive_name",
        "source",
    )
    qs = restrict_to_executive(qs, request.user)

    filters = {
        "created_from": request.GET.get("created_from", "").strip(),
        "created_to": request.GET.get("created_to", "").strip(),
        "customer_name": request.GET.get("customer_name", "").strip(),
        "sub_profile": request.GET.get("sub_profile", "").strip(),
        "contact_person": request.GET.get("contact_person", "").strip(),
        "city": request.GET.get("city", "").strip(),
        "state": request.GET.get("state", "").strip(),
        "zone": request.GET.get("zone", "").strip(),
        "executive_name": request.GET.get("executive_name", "").strip(),
        "source": request.GET.get("source", "").strip(),
    }
    # created_date FROM–TO
    if filters["created_from"]:
        d = parse_date(filters["created_from"])
        if d:
            qs = qs.filter(created_date__gte=d)

    if filters["created_to"]:
        d = parse_date(filters["created_to"])
        if d:
            qs = qs.filter(created_date__lte=d)

    if filters["customer_name"]:
        qs = qs.filter(customer_name__subcategory__icontains=filters["customer_name"])
    if filters["sub_profile"]:
        qs = qs.filter(sub_profile__subcategory__icontains=filters["sub_profile"])
    if filters["contact_person"]:
        qs = qs.filter(contact_person__icontains=filters["contact_person"])
    if filters["city"]:
        qs = qs.filter(city__subcategory__icontains=filters["city"])
    if filters["state"]:
        qs = qs.filter(state__subcategory__icontains=filters["state"])
    if filters["zone"]:
        qs = qs.filter(zone__subcategory__icontains=filters["zone"])
    if filters["executive_name"]:
        qs = qs.filter(executive_name__subcategory__icontains=filters["executive_name"])
    if filters["source"]:
        qs = qs.filter(source__subcategory__icontains=filters["source"])

    qs = qs.order_by("customer_name__subcategory")
    return qs, filters


@login_required
def pc_customer_export(request):
    """Download the filtered customer list as Excel."""
    qs, _ = _filtered_customers(request)

    output = BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    ws = workbook.add_worksheet("Customers")

    # === Formats ===
    title_fmt = workbook.add_format({
        "bold": True,
        "font_size": 14,
        "align": "center",
        "valign": "vcenter",
    })
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#D9E1F2"})

    headers = [
        "Created Date",
        "Customer Name",
        "Customer Profile",
        "Sub Profile",
        "Contact Person",
        "Contact No",
        "Email ID",
        "Address",
        "Place",
        "City",
        "State",
        "Zone",
        "Executive Name",
        "Source",
        "Updated Date",
    ]
    # === Title row (row 0) ===
    last_col = len(headers) - 1
    ws.merge_range(0, 0, 0, last_col, "Personal Care Customer Master", title_fmt)

    # leave row 1 blank, put headers in row 2
    header_row = 2
    for col, h in enumerate(headers):
        ws.write(header_row, col, h, header_fmt)
    # === Data starting from row 3 ===
    row = header_row + 1
    for obj in qs:
        col = 0

        # Created date
        created_str = obj.created_at.strftime("%d-%m-%Y") if obj.created_at else ""
        ws.write(row, col, created_str); col += 1
        # FK/text fields
        ws.write(row, col, _safe(obj.customer_name, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.customer_profile, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.sub_profile, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.contact_person)); col += 1
        ws.write(row, col, _safe(obj.contact_no)); col += 1
        ws.write(row, col, _safe(obj.email_id)); col += 1
        ws.write(row, col, _safe(obj.address)); col += 1
        ws.write(row, col, _safe(obj.place, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.city, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.state, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.zone, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.executive_name, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.source, "subcategory")); col += 1
        # Updated date
        updated_str = obj.updated_at.strftime("%d-%m-%Y") if obj.updated_at else ""
        ws.write(row, col, updated_str); col += 1
        row += 1
    workbook.close()
    output.seek(0)

    resp = HttpResponse(
        output.getvalue(),
        content_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
    resp["Content-Disposition"] = 'attachment; filename="pc_customers.xlsx"'
    return resp





def get_PCcustomer_contacts(request):
    """
    API endpoint to fetch all contacts associated with a customer name.
    """
    customer_name_str = request.GET.get('customer_name')
    if not customer_name_str:
        return JsonResponse({'contacts': []})
    try:
        customer_name_obj = PersonalCareMaster.objects.get(
            category="Customer Name",
            subcategory=customer_name_str
        )
    except PersonalCareMaster.DoesNotExist:
        return JsonResponse({'contacts': []})
    contacts = PC_CustomerMaster.objects.filter(
        customer_name=customer_name_obj
    ).values(
        'contact_person',
        'contact_no',
        'email_id',
        'address'
    ).distinct()
    return JsonResponse({'contacts': list(contacts)})




@login_required
def pc_sample_request_create(request, pk=None):
    """
    Add / Edit PC Sample Request (same view, like pc_customer_create).
    If pk is provided → edit, otherwise → new.
    """
    instance = None
    is_edit = pk is not None
    # ---- Permission check (add vs change) ----
    if is_edit:
        perm_code = "PERSONAL_CARE.change_pc_samplerequest"
        action = "update"
    else:
        perm_code = "PERSONAL_CARE.add_pc_samplerequest"
        action = "add"
    if not request.user.has_perm(perm_code):
        messages.error(request, f"You do not have permission to {action} PC Sample Requests.")
        logger.warning(
            "User '%s' tried to %s PC Sample Request (pk=%s) without permission.",
            request.user.username,
            action,
            pk,
        )
        return redirect("indexpage")
    # ------------------------------------------
    # Log access
    logger.info(
        "User='%s' opened PC Sample Request %s view (pk=%s)",
        request.user.username,
        "edit" if is_edit else "create",
        pk,
    )
    if is_edit:
        base_qs = restrict_to_executive(
            PC_SampleRequest.objects.select_related(
                "customer_name", "product_name", "project_name", "supplier_name",
                "remarks_master", "stage", "executive_name"
            ),
            request.user,
        )
        instance = get_object_or_404(base_qs, pk=pk)
    if request.method == "POST":
        form = PCSampleRequestForm(request.POST, instance=instance)
        if form.is_valid():
            obj = form.save()
            if instance is None:
                logger.info(
                    "PC Sample Request created by user='%s' (id=%s)",
                    request.user.username,
                    obj.pk,
                )
                messages.success(request, "Sample request saved successfully.")
            else:
                logger.info(
                    "PC Sample Request updated by user='%s' (id=%s)",
                    request.user.username,
                    obj.pk,
                )
                messages.success(request, "Sample request updated successfully.")
            return redirect("pc_sample_request_list")
        else:
            logger.error(
                "PCSampleRequestForm invalid for user='%s' | errors=%s",
                request.user.username,
                form.errors.as_json(),
            )
            messages.error(
                request,
                "There were errors in the Sample Request form. Please correct them and try again.",
            )
    else:
        form = PCSampleRequestForm(instance=instance)
    context = {
        "form": form,
        "customer_name_options": _options("Customer Name"),
        "product_name_options": _options("Product Name"),
        "project_name_options": _options("Project Name"),
        "supplier_name_options": _options("Supplier Name"),
        "remarks_options": _options("Remarks"),
        "stage_options": _options("Stage"),
        "executive_options": _options("Executive Name"),
        "is_edit": instance is not None,
    }
    return render(request, "PERSONAL_CARE/pc_sample_request_form.html", context)



# ---------- shared filters for Sample Request list + export ----------

def _filtered_sample_requests(request):
    """
    Apply all filters for Sample Requests and return (qs, filters_dict).
    """
    qs = PC_SampleRequest.objects.select_related(
        "customer_name",
        "product_name",
        "project_name",
        "supplier_name",
        "remarks_master",
        "stage",
        "executive_name",
    )

    qs = restrict_to_executive(qs, request.user)

    filters = {
        "inquiry_from": (request.GET.get("inquiry_from") or "").strip(),
        "inquiry_to": (request.GET.get("inquiry_to") or "").strip(),
        "customer_name": (request.GET.get("customer_name") or "").strip(),
        "product_name": (request.GET.get("product_name") or "").strip(),
        "project_name": (request.GET.get("project_name") or "").strip(),
        "contact_person": (request.GET.get("contact_person") or "").strip(),
        "stage": (request.GET.get("stage") or "").strip(),
        "supplier_name": (request.GET.get("supplier_name") or "").strip(),
        "executive_name": (request.GET.get("executive_name") or "").strip(),
        "approval_by_nmp": (request.GET.get("approval_by_nmp") or "").strip(),
    }

    # ---- date range filters on inquiry_date ----
    if filters["inquiry_from"]:
        d = parse_date(filters["inquiry_from"])
        if d:
            qs = qs.filter(inquiry_date__gte=d)

    if filters["inquiry_to"]:
        d = parse_date(filters["inquiry_to"])
        if d:
            qs = qs.filter(inquiry_date__lte=d)

    # ---- text / FK filters ----
    if filters["customer_name"]:
        qs = qs.filter(
            customer_name__subcategory__icontains=filters["customer_name"]
        )

    if filters["product_name"]:
        qs = qs.filter(
            product_name__subcategory__icontains=filters["product_name"]
        )

    if filters["project_name"]:
        qs = qs.filter(
            project_name__subcategory__icontains=filters["project_name"]
        )

    if filters["contact_person"]:
        qs = qs.filter(contact_person__icontains=filters["contact_person"])

    if filters["stage"]:
        qs = qs.filter(stage__subcategory__icontains=filters["stage"])

    if filters["supplier_name"]:
        qs = qs.filter(
            supplier_name__subcategory__icontains=filters["supplier_name"]
        )

    if filters["executive_name"]:
        qs = qs.filter(
            executive_name__subcategory__icontains=filters["executive_name"]
        )

    if filters["approval_by_nmp"]:
        qs = qs.filter(
            approval_by_nmp__iexact=filters["approval_by_nmp"]
        )

    qs = qs.order_by("-inquiry_date", "customer_name__subcategory")
    return qs, filters


# ---------- LIST VIEW ----------

@login_required
def pc_sample_request_list(request):
    qs, filters = _filtered_sample_requests(request)

    paginator = Paginator(qs, 50)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    # Preserve filters in pagination links
    querydict = request.GET.copy()
    if "page" in querydict:
        del querydict["page"]
    querystring = querydict.urlencode()

    context = {
        "sample_requests": page_obj,
        "page_obj": page_obj,
        "paginator": paginator,
        "is_paginated": page_obj.has_other_pages(),
        "querystring": querystring,
        "filters": filters,
        # datalist options
        "customer_name_options": _options("Customer Name"),
        "product_name_options": _options("Product Name"),
        "project_name_options": _options("Project Name"),
        "supplier_name_options": _options("Supplier Name"),
        "remarks_options": _options("Remarks"),
        "stage_options": _options("Stage"),
        "executive_options": _options("Executive Name"),
    }
    return render(request, "PERSONAL_CARE/pc_sample_request_list.html", context)

# ---------- EXPORT VIEW ----------

@login_required
def pc_sample_request_export(request):
    """
    Download filtered Sample Request list as Excel.
    Uses same filters as pc_sample_request_list.
    """
    qs, _ = _filtered_sample_requests(request)
    output = BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    ws = workbook.add_worksheet("Sample Requests")
    # Formats
    title_fmt = workbook.add_format(
        {"bold": True, "font_size": 14, "align": "center", "valign": "vcenter"}
    )
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#D9E1F2"})
    headers = [
        "Inquiry Date",
        "Sample Dispatch Date",
        "Year",
        "Project Close Date",
        "Customer Name",
        "Contact Person",
        "Contact No",
        "Email",
        "Address",
        "Product Name",
        "Sample Quantity (gm/ml)",
        "Project Name",
        "Project Type",
        "Price Indication Given",
        "Supplier Name",
        "Last Follow-up Month",
        "Stage",
        "Remarks",
        "Executive Name",
        "Approval",
        "Approved Quantity",
    ]
    # Title row
    last_col = len(headers) - 1
    ws.merge_range(
        0, 0, 0, last_col, "Personal Care Sample Requests", title_fmt
    )
    # Header row
    header_row = 2
    for col, h in enumerate(headers):
        ws.write(header_row, col, h, header_fmt)
    # Data rows
    row = header_row + 1
    for obj in qs:
        col = 0
        inq_str = (
            obj.inquiry_date.strftime("%d-%m-%Y") if obj.inquiry_date else ""
        )
        samp_str = (
            obj.sample_dispatch_date.strftime("%d-%m-%Y")
            if obj.sample_dispatch_date
            else ""
        )
        close_str = (
            obj.project_close_date.strftime("%d-%m-%Y")
            if obj.project_close_date
            else ""
        )
        followup_str = (
            obj.followup_date.strftime("%b-%Y")  # e.g. "Dec, 2025"
            if obj.followup_date
            else ""
        )
        # 🔹 Year column – prefer model field, else derive from sample_dispatch_date
        year_val = (
            obj.year
            if getattr(obj, "year", None) is not None
            else (obj.sample_dispatch_date.year if obj.sample_dispatch_date else "")
        )
        ws.write(row, col, inq_str); col += 1
        ws.write(row, col, samp_str); col += 1
        ws.write(row, col, year_val); col += 1
        ws.write(row, col, close_str); col += 1
        ws.write(row, col, _safe(obj.customer_name, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.contact_person)); col += 1
        ws.write(row, col, _safe(obj.contact_no)); col += 1
        ws.write(row, col, _safe(obj.email)); col += 1
        ws.write(row, col, _safe(obj.address)); col += 1
        ws.write(row, col, _safe(obj.product_name, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.sample_quantity)); col += 1
        ws.write(row, col, _safe(obj.project_name, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.project_type)); col += 1
        ws.write(row, col, _safe(obj.price_indication_given)); col += 1
        ws.write(row, col, _safe(obj.supplier_name, "subcategory")); col += 1
        ws.write(row, col, followup_str); col += 1
        ws.write(row, col, _safe(obj.stage, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.remarks_master, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.executive_name, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.approval_by_nmp)); col += 1
        ws.write(row, col, _safe(obj.approved_quantity)); col += 1
        row += 1

    workbook.close()
    output.seek(0)

    resp = HttpResponse(
        output.getvalue(),
        content_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
    )
    resp["Content-Disposition"] = 'attachment; filename="pc_sample_requests.xlsx"'
    return resp




@login_required
def pc_sample_request_history(request, pk):
    """
    Show all sample requests for the same customer as the given record (pk).
    Used inside the modal on the list page.
    """
    base = PC_SampleRequest.objects.select_related(
        "customer_name",
        "product_name",
        "project_name",
        "supplier_name",
        "remarks_master",
        "stage",
        "executive_name",
    )
    base = restrict_to_executive(base, request.user)

    current = get_object_or_404(base, pk=pk)

    # all rows for this customer
    rows_qs = base.filter(customer_name=current.customer_name)

    # stage-wise counts
    stage_summary = (
        rows_qs
        .values("stage__subcategory")
        .annotate(total=Count("id"))
        .order_by("stage__subcategory")
    )

    # rows (DESC)
    rows = list(rows_qs.order_by("-inquiry_date", "-id"))

    # ✅ highlight: same customer + same product more than one record
    # (customer is same in this modal, so we check only product repeat)
    prod_ids = [r.product_name_id for r in rows if r.product_name_id]
    counts = Counter(prod_ids)
    dup_prod_ids = [pid for pid, c in counts.items() if c > 1]

    # ✅ yellow + other color (cycle if multiple groups)
    palette = [
        "bg-yellow-100",  # 1st duplicate group
        "bg-indigo-100",  # 2nd duplicate group
        "bg-emerald-100", # 3rd duplicate group
        "bg-rose-100",    # 4th duplicate group
    ]
    prod_color_map = {}
    for i, pid in enumerate(sorted(dup_prod_ids)):
        prod_color_map[pid] = palette[i % len(palette)]

    # attach a temp attribute on each row for template usage
    for r in rows:
        r.dup_prod_cls = prod_color_map.get(r.product_name_id, "")
    customer_label = current.customer_name.subcategory if current.customer_name else "Customer"

    context = {
        "customer_label": customer_label,
        "rows": rows,
        "stage_summary": stage_summary,
        "approval_choices": PC_SampleRequest.APPROVAL_NMP_CHOICES,
    }
    return render(request, "PERSONAL_CARE/pc_sample_request_history.html", context)


@login_required
def pc_sample_request_update_approval(request, pk):
    # ---- Permission check ----
    if not request.user.has_perm("PERSONAL_CARE.can_approve_sample_request"):
        logger.warning("User='%s' tried to update PC_SampleRequest approval_by_nmp without permission | pk=%s",
            request.user.username, pk, )
        return JsonResponse({"ok": False, "error": "Permission denied"}, status=403)

    if request.method != "POST":
        logger.warning("User='%s' called pc_sample_request_update_approval with non-POST | pk=%s | method=%s",
            request.user.username,  pk,  request.method, )
        raise Http404("Only POST allowed")

    obj = get_object_or_404(PC_SampleRequest, pk=pk)
    new_status = (request.POST.get("approval_by_nmp") or "").strip()

    valid_values = {value for value, _ in PC_SampleRequest.APPROVAL_NMP_CHOICES}
    if new_status not in valid_values:
        logger.warning("User='%s' sent invalid approval_by_nmp | pk=%s | value='%s' | valid=%s",
            request.user.username,pk,new_status,sorted(valid_values),)
        return JsonResponse({"ok": False, "error": "Invalid status"}, status=400)

    old_status = obj.approval_by_nmp
    obj.approval_by_nmp = new_status
    obj.save(update_fields=["approval_by_nmp", "updated_at"])
    logger.info("User='%s' updated PC_SampleRequest approval_by_nmp | pk=%s | '%s' -> '%s'",
        request.user.username,pk, old_status,new_status, )
    return JsonResponse({"ok": True, "approval_by_nmp": obj.approval_by_nmp})



@login_required
@require_POST
def pc_sample_request_delete(request, pk):
    """
    Delete a sample request (only within the executive's scope).
    """
    # start from same base restriction used in list
    qs = PC_SampleRequest.objects.all()
    qs = restrict_to_executive(qs, request.user)

    obj = get_object_or_404(qs, pk=pk)

    obj.delete()
    messages.success(request, "Sample request deleted successfully.")

    return redirect("pc_sample_request_list")








@login_required
def pc_customer_dashboard(request):
    context = {
        # ── Customers tab options ──────────────────────────
        "customer_name_options": _options("Customer Name"),
        "sub_profile_options": _options("Sub Profile"),
        "city_options": _options("City"),
        "state_options": _options("State"),
        "zone_options": _options("Zone"),
        "executive_options": _options("Executive Name"),
        "source_options": _options("Source"),

        # ── Sample Requests tab options ────────────────────
        "sample_customer_options": _options("Customer Name"),
        "sample_product_options": _options("Product Name"),
        "sample_project_options": _options("Project Name"),
        "sample_supplier_options": _options("Supplier Name"),
        "sample_remarks_options": _options("Remarks"),
        "sample_stage_options": _options("Stage"),
        "sample_executive_options": _options("Executive Name"),
        "sample_approval_options": [code for code, _ in PC_SampleRequest.APPROVAL_NMP_CHOICES],
    }
    return render(request, "PERSONAL_CARE/pc_customer_dashboard.html", context)



@login_required
def pc_customer_dashboard_data(request):
    q           = (request.GET.get("q") or "").strip()
    executive   = (request.GET.get("executive") or "").strip()
    state_val   = (request.GET.get("state") or "").strip()
    city_val    = (request.GET.get("city") or "").strip()
    sub_profile = (request.GET.get("sub_profile") or "").strip()
    customer    = (request.GET.get("customer") or "").strip()

    # 🔄 now using created_at range instead of followup_date
    created_from = (request.GET.get("created_from") or "").strip()
    created_to   = (request.GET.get("created_to") or "").strip()

    contact_person = (request.GET.get("contact_person") or "").strip()
    zone_val       = (request.GET.get("zone") or "").strip()
    source_val     = (request.GET.get("source") or "").strip()

    qs = PC_CustomerMaster.objects.select_related(
        "customer_profile",
        "sub_profile",
        "place",
        "city",
        "state",
        "zone",
        "executive_name",
        "source",
    )

    qs = restrict_to_executive(qs, request.user)

    if q:
        qs = qs.filter(
            Q(customer_name__subcategory__icontains=q)
            | Q(contact_person__icontains=q)
            | Q(contact_no__icontains=q)
            | Q(email_id__icontains=q)
            | Q(address__icontains=q)
            | Q(city__subcategory__icontains=q)
            | Q(state__subcategory__icontains=q)
            | Q(zone__subcategory__icontains=q)
            | Q(sub_profile__subcategory__icontains=q)
            | Q(executive_name__subcategory__icontains=q)
        )

    def _blank_or_exact(field, value, lookup="iexact"):
        if not value:
            return {}
        if value == "(blank)":
            return {f"{field}__isnull": True}
        return {f"{field}__subcategory__{lookup}": value}

    # 🎯 dropdown filters
    if executive:
        qs = qs.filter(**_blank_or_exact("executive_name", executive))
    if state_val:
        qs = qs.filter(**_blank_or_exact("state", state_val))
    if city_val:
        qs = qs.filter(**_blank_or_exact("city", city_val))
    if sub_profile:
        qs = qs.filter(**_blank_or_exact("sub_profile", sub_profile))
    if customer:
        qs = qs.filter(**_blank_or_exact("customer_name", customer))

    # 🔹 text / field filters
    if contact_person:
        qs = qs.filter(contact_person__icontains=contact_person)
    if zone_val:
        qs = qs.filter(**_blank_or_exact("zone", zone_val))
    if source_val:
        qs = qs.filter(**_blank_or_exact("source", source_val))

    # 📅 CREATED range (new)
    if created_from:
        try:
            dt_from = _dt.datetime.strptime(created_from, "%Y-%m-%d").date()
            qs = qs.filter(created_at__gte=dt_from)
        except ValueError:
            pass

    if created_to:
        try:
            dt_to = _dt.datetime.strptime(created_to, "%Y-%m-%d").date()
            qs = qs.filter(created_at__lte=dt_to)
        except ValueError:
            pass

    # 🧮 Totals (KPI cards) – distinct customers
    totals = {
        "customers": (
            qs.filter(customer_name__isnull=False)
              .values("customer_name")
              .distinct()
              .count()
        ),
        "states": (
            qs.filter(state__isnull=False)
              .values("state")
              .distinct()
              .count()
        ),
        "cities": (
            qs.filter(city__isnull=False)
              .values("city")
              .distinct()
              .count()
        ),
        "executives": (
            qs.filter(executive_name__isnull=False)
              .values("executive_name")
              .distinct()
              .count()
        ),
    }
    # 🧩 Summary tables
    by_executive = (
        qs.values(label=F("executive_name__subcategory"))
          .annotate(count=Count("customer_name", distinct=True))
          .order_by("-count", "label")
    )
    by_state = (
        qs.values(label=F("state__subcategory"))
          .annotate(count=Count("customer_name", distinct=True))
          .order_by("-count", "label")
    )
    by_city = (
        qs.values(label=F("city__subcategory"))
          .annotate(count=Count("customer_name", distinct=True))
          .order_by("-count", "label")
    )
    by_sub_profile = (
        qs.values(label=F("sub_profile__subcategory"))
          .annotate(count=Count("customer_name", distinct=True))
          .order_by("-count", "label")
    )

    # 🔁 customer table = entries count
    by_customer = (
        qs.values(label=F("customer_name__subcategory"))
          .annotate(count=Count("id"))
          .order_by("-count", "label")
    )
    # 🧾 Detail table
    qs_detail = qs.order_by("created_at")[:500]
    detail_rows = []
    for obj in qs_detail:
        detail_rows.append(
            {
                "created_at": obj.created_at.strftime("%d-%m-%Y") if obj.created_at else "",
                "customer_name": obj.customer_name.subcategory if obj.customer_name else "",
                "customer_profile": obj.customer_profile.subcategory if obj.customer_profile else "",
                "sub_profile": obj.sub_profile.subcategory if obj.sub_profile else "",
                "contact_person": obj.contact_person or "",
                "contact_no": obj.contact_no or "",
                "email_id": obj.email_id or "",
                "address": obj.address or "",
                "place": obj.place.subcategory if obj.place else "",
                "city": obj.city.subcategory if obj.city else "",
                "state": obj.state.subcategory if obj.state else "",
                "zone": obj.zone.subcategory if obj.zone else "",
                "executive": obj.executive_name.subcategory if obj.executive_name else "",
                "source": obj.source.subcategory if obj.source else "",
            }
        )
    return JsonResponse(
        {
            "totals": totals,
            "by_executive": list(by_executive),
            "by_state": list(by_state),
            "by_city": list(by_city),
            "by_sub_profile": list(by_sub_profile),
            "by_customer": list(by_customer),
            "table": detail_rows,
        }
    )




@login_required
def pc_sample_dashboard_data(request):
    """
    JSON data for Sample Request dashboard (tab 2).
    Grouped by: executive, customer, product, project, project_type,
    supplier, remarks, approval_by_nmp, stage.
    """
    q             = (request.GET.get("q") or "").strip()
    executive     = (request.GET.get("executive") or "").strip()
    customer      = (request.GET.get("customer") or "").strip()
    product       = (request.GET.get("product") or "").strip()
    project       = (request.GET.get("project") or "").strip()
    project_type  = (request.GET.get("project_type") or "").strip()
    supplier      = (request.GET.get("supplier") or "").strip()
    remarks       = (request.GET.get("remarks") or "").strip()
    stage_val     = (request.GET.get("stage") or "").strip()
    approval      = (request.GET.get("approval") or "").strip()

    # 🔄 new: sample dispatch date range
    sample_from   = (request.GET.get("sample_from") or "").strip()
    sample_to     = (request.GET.get("sample_to") or "").strip()

    qs = PC_SampleRequest.objects.select_related(
        "customer_name",
        "product_name",
        "project_name",
        "supplier_name",
        "remarks_master",
        "stage",
        "executive_name",
    )

    qs = restrict_to_executive(qs, request.user)

    # ---- global search ----
    if q:
        qs = qs.filter(
            Q(customer_name__subcategory__icontains=q)
            | Q(product_name__subcategory__icontains=q)
            | Q(project_name__subcategory__icontains=q)
            | Q(contact_person__icontains=q)
            | Q(contact_no__icontains=q)
            | Q(email__icontains=q)
            | Q(address__icontains=q)
            | Q(executive_name__subcategory__icontains=q)
            | Q(supplier_name__subcategory__icontains=q)
            | Q(remarks_master__subcategory__icontains=q)
        )

    def _blank_or_exact(field, value, lookup="iexact", raw=False):
        if not value:
            return {}
        if value == "(blank)":
            return {f"{field}__isnull": True}
        if raw:
            return {f"{field}__{lookup}": value}
        return {f"{field}__subcategory__{lookup}": value}

    # ---- clickable / text filters ----
    if executive:
        qs = qs.filter(**_blank_or_exact("executive_name", executive))
    if customer:
        qs = qs.filter(**_blank_or_exact("customer_name", customer))
    if product:
        qs = qs.filter(**_blank_or_exact("product_name", product))
    if project:
        qs = qs.filter(**_blank_or_exact("project_name", project))
    if project_type:
        qs = qs.filter(**_blank_or_exact("project_type", project_type, raw=True))
    if supplier:
        qs = qs.filter(**_blank_or_exact("supplier_name", supplier))
    if remarks:
        qs = qs.filter(**_blank_or_exact("remarks_master", remarks))
    if stage_val:
        qs = qs.filter(**_blank_or_exact("stage", stage_val))
    if approval:
        qs = qs.filter(**_blank_or_exact("approval_by_nmp", approval, raw=True))

    # ---- sample_dispatch_date range (new) ----
    if sample_from:
        try:
            dt_from = _dt.datetime.strptime(sample_from, "%Y-%m-%d").date()
            qs = qs.filter(sample_dispatch_date__gte=dt_from)
        except ValueError:
            pass

    if sample_to:
        try:
            dt_to = _dt.datetime.strptime(sample_to, "%Y-%m-%d").date()
            qs = qs.filter(sample_dispatch_date__lte=dt_to)
        except ValueError:
            pass

    # ---- totals (cards) ----
    totals = {
        "requests": qs.count(),
        "executives": (
            qs.filter(executive_name__isnull=False)
              .values("executive_name")
              .distinct()
              .count()
        ),
        "customers": (
            qs.filter(customer_name__isnull=False)
              .values("customer_name")
              .distinct()
              .count()
        ),
        "products": (
            qs.filter(product_name__isnull=False)
              .values("product_name")
              .distinct()
              .count()
        ),
    }

    def _group(field_expr, label_key):
        agg = (
            qs.annotate(label=F(field_expr))
              .values("label")
              .annotate(count=Count("id"))
              .order_by("-count", "label")
        )
        rows = []
        for r in agg:
            label = r["label"] or "(blank)"
            rows.append({label_key: label, "count": r["count"]})
        return rows

    by_executive     = _group("executive_name__subcategory", "executive")
    by_customer      = _group("customer_name__subcategory", "customer")
    by_product       = _group("product_name__subcategory", "product")
    by_project       = _group("project_name__subcategory", "project")
    by_project_type  = _group("project_type", "project_type")
    by_supplier      = _group("supplier_name__subcategory", "supplier")
    by_remarks       = _group("remarks_master__subcategory", "remarks")
    by_stage         = _group("stage__subcategory", "stage")
    by_approval      = _group("approval_by_nmp", "approval")

    # ---- detail table ----
    qs_detail = qs.order_by("inquiry_date", "customer_name__subcategory")[:500]

    detail_rows = []
    for obj in qs_detail:
        def dstr(dt):
            return dt.strftime("%d-%m-%Y") if dt else ""

        detail_rows.append(
            {
                "inquiry_date": dstr(obj.inquiry_date),
                "sample_dispatch_date": dstr(obj.sample_dispatch_date),
                "project_close_date": dstr(obj.project_close_date),
                "customer_name": obj.customer_name.subcategory if obj.customer_name else "",
                "product_name": obj.product_name.subcategory if obj.product_name else "",
                "project_name": obj.project_name.subcategory if obj.project_name else "",
                "project_type": obj.get_project_type_display() if obj.project_type else "",
                "supplier_name": obj.supplier_name.subcategory if obj.supplier_name else "",
                "remarks_master": obj.remarks_master.subcategory if obj.remarks_master else "",
                "stage": obj.stage.subcategory if obj.stage else "",
                "executive_name": obj.executive_name.subcategory if obj.executive_name else "",
                "contact_person": obj.contact_person or "",
                "contact_no": obj.contact_no or "",
                "email": obj.email or "",
                "address": obj.address or "",
                "sample_quantity": str(obj.sample_quantity or ""),
                "price_indication_given": obj.price_indication_given or "",
                "followup_date": dstr(obj.followup_date),
                "approval_by_nmp": obj.get_approval_by_nmp_display() if obj.approval_by_nmp else "",
                "approved_quantity": str(obj.approved_quantity or ""),
                "created_at": obj.created_at.strftime("%d-%m-%Y") if obj.created_at else "",
                "updated_at": obj.updated_at.strftime("%d-%m-%Y") if obj.updated_at else "",
            }
        )

    return JsonResponse(
        {
            "totals": totals,
            "by_executive": by_executive,
            "by_customer": by_customer,
            "by_product": by_product,
            "by_project": by_project,
            "by_project_type": by_project_type,
            "by_supplier": by_supplier,
            "by_remarks": by_remarks,
            "by_stage": by_stage,
            "by_approval": by_approval,
            "table": detail_rows,
        }
    )


@login_required
def pc_followup_dashboard_data(request):
    """
    JSON data for Customer Follow-up dashboard (tab 3).
    Shows counts by executive, mode of follow-up, status, customer, etc.
    """
    executive = (request.GET.get("executive") or "").strip()
    mode      = (request.GET.get("mode") or "").strip()
    status    = (request.GET.get("status") or "").strip()
    date_from = (request.GET.get("date_from") or "").strip()
    date_to   = (request.GET.get("date_to") or "").strip()

    qs = Customer_Followup.objects.select_related(
        "customer_name", "executive_name", "followup_status"
    )

    qs = restrict_to_executive(qs, request.user)

    if executive:
        qs = qs.filter(executive_name__subcategory__iexact=executive)
    if mode:
        qs = qs.filter(mode_of_followup=mode)
    if status:
        qs = qs.filter(followup_status__subcategory__iexact=status)
    if date_from:
        try:
            qs = qs.filter(date__gte=_dt.datetime.strptime(date_from, "%Y-%m-%d").date())
        except ValueError:
            pass
    if date_to:
        try:
            qs = qs.filter(date__lte=_dt.datetime.strptime(date_to, "%Y-%m-%d").date())
        except ValueError:
            pass

    totals = {
        "total_followups": qs.count(),
        "executives": qs.values("executive_name").distinct().count(),
        "customers": qs.values("customer_name").distinct().count(),
        "statuses": qs.values("followup_status").distinct().count(),
    }

    by_executive = (
        qs.values(label=F("executive_name__subcategory"))
          .annotate(count=Count("id"))
          .order_by("-count")
    )
    by_status = (
        qs.values(label=F("followup_status__subcategory"))
          .annotate(count=Count("id"))
          .order_by("-count")
    )
    by_mode = (
        qs.values(label=F("mode_of_followup"))
          .annotate(count=Count("id"))
          .order_by("-count")
    )

    # ✅ NEW: customer-wise
    by_customer = (
        qs.values(label=F("customer_name__subcategory"))
          .annotate(count=Count("id"))
          .order_by("-count")
    )

    details = [
        {
            "date": obj.date.strftime("%d-%m-%Y") if obj.date else "",
            "customer": obj.customer_name.subcategory if obj.customer_name else "",
            "executive": obj.executive_name.subcategory if obj.executive_name else "",
            "status": obj.followup_status.subcategory if obj.followup_status else "",
            "mode": obj.get_mode_of_followup_display(),
            "description": obj.description or "",
        }
        for obj in qs.order_by("-date")[:500]
    ]

    return JsonResponse({
        "totals": totals,
        "by_executive": list(by_executive),
        "by_status": list(by_status),
        "by_mode": list(by_mode),
        "by_customer": list(by_customer),   # ✅ NEW
        "table": details,
    })




@login_required
def pc_missing_customers_dashboard_data(request):
    executive = (request.GET.get("executive") or "").strip()

    # 1) Base master queryset (restricted)
    cm_qs = PC_CustomerMaster.objects.select_related("customer_name", "executive_name")
    cm_qs = restrict_to_executive(cm_qs, request.user)

    # optional executive filter (by master executive_name)
    if executive:
        cm_qs = cm_qs.filter(executive_name__subcategory__iexact=executive)

    # --- executive-wise summary (always based on master) ---
    by_executive = (
        PC_CustomerMaster.objects
        .select_related("executive_name", "customer_name")
    )
    by_executive = restrict_to_executive(by_executive, request.user)
    by_executive = (
        by_executive.filter(customer_name__isnull=False, executive_name__isnull=False)
        .values(label=F("executive_name__subcategory"))
        .annotate(count=Count("customer_name", distinct=True))
        .order_by("-count", "label")
    )

    master_customer_ids = (
        cm_qs.filter(customer_name__isnull=False)
             .values("customer_name_id")
             .distinct()
    )

    # 2) Sample request queryset (restricted)
    sr_qs = PC_SampleRequest.objects.select_related("customer_name", "executive_name")
    sr_qs = restrict_to_executive(sr_qs, request.user)

    # We want missing vs selected master executive, so filter SR by those customers only
    sample_customer_ids = (
        sr_qs.filter(customer_name__isnull=False)
             .values("customer_name_id")
             .distinct()
    )

    # 3) Followup queryset (restricted)
    fu_qs = Customer_Followup.objects.select_related("customer_name", "executive_name")
    fu_qs = restrict_to_executive(fu_qs, request.user)

    follow_customer_ids = (
        fu_qs.filter(customer_name__isnull=False)
             .values("customer_name_id")
             .distinct()
    )

    total_customers_qs = (
        PersonalCareMaster.objects
        .filter(id__in=Subquery(master_customer_ids.values("customer_name_id")))
        .order_by("subcategory")
    )

    missing_in_sample_qs = (
        PersonalCareMaster.objects
        .filter(id__in=Subquery(master_customer_ids.values("customer_name_id")))
        .exclude(id__in=Subquery(sample_customer_ids.values("customer_name_id")))
        .order_by("subcategory")
    )

    missing_in_followup_qs = (
        PersonalCareMaster.objects
        .filter(id__in=Subquery(master_customer_ids.values("customer_name_id")))
        .exclude(id__in=Subquery(follow_customer_ids.values("customer_name_id")))
        .order_by("subcategory")
    )

    missing_sample_list = list(missing_in_sample_qs.values_list("subcategory", flat=True)[:2000])
    missing_followup_list = list(missing_in_followup_qs.values_list("subcategory", flat=True)[:2000])

    return JsonResponse({
        "selected": {"executive": executive or ""},
        "totals": {
            "total_customers": total_customers_qs.count(),
            "missing_sample": len(missing_sample_list),
            "missing_followup": len(missing_followup_list),
        },
        "by_executive": list(by_executive),
        "missing_sample_customers": missing_sample_list,
        "missing_followup_customers": missing_followup_list,
    })



@login_required
def pc_other_customer_dashboard_data(request):
    """
    JSON data for Other Customers dashboard (tab).
    Similar to pc_customer_dashboard_data but uses PC_Other_CustomerMaster.
    """

    q           = (request.GET.get("q") or "").strip()
    executive   = (request.GET.get("executive") or "").strip()
    state_val   = (request.GET.get("state") or "").strip()
    city_val    = (request.GET.get("city") or "").strip()
    sub_profile = (request.GET.get("sub_profile") or "").strip()
    customer    = (request.GET.get("customer") or "").strip()

    created_from = (request.GET.get("created_from") or "").strip()
    created_to   = (request.GET.get("created_to") or "").strip()

    contact_person = (request.GET.get("contact_person") or "").strip()
    zone_val       = (request.GET.get("zone") or "").strip()
    source_val     = (request.GET.get("source") or "").strip()

    qs = PC_Other_CustomerMaster.objects.select_related(
        "customer_name",
        "sub_profile",
        "city",
        "state",
        "zone",
        "executive_name",
        "source",
    )

    qs = restrict_to_executive(qs, request.user)

    # ---- global search ----
    if q:
        qs = qs.filter(
            Q(customer_name__subcategory__icontains=q)
            | Q(contact_person__icontains=q)
            | Q(contact_no__icontains=q)
            | Q(email_id__icontains=q)
            | Q(address__icontains=q)
            | Q(city__subcategory__icontains=q)
            | Q(state__subcategory__icontains=q)
            | Q(zone__subcategory__icontains=q)
            | Q(sub_profile__subcategory__icontains=q)
            | Q(executive_name__subcategory__icontains=q)
            | Q(source__subcategory__icontains=q)
        )

    def _blank_or_exact(field, value, lookup="iexact"):
        if not value:
            return {}
        if value == "(blank)":
            return {f"{field}__isnull": True}
        return {f"{field}__subcategory__{lookup}": value}

    # ---- dropdown filters ----
    if executive:
        qs = qs.filter(**_blank_or_exact("executive_name", executive))
    if state_val:
        qs = qs.filter(**_blank_or_exact("state", state_val))
    if city_val:
        qs = qs.filter(**_blank_or_exact("city", city_val))
    if sub_profile:
        qs = qs.filter(**_blank_or_exact("sub_profile", sub_profile))
    if customer:
        qs = qs.filter(**_blank_or_exact("customer_name", customer))
    if zone_val:
        qs = qs.filter(**_blank_or_exact("zone", zone_val))
    if source_val:
        qs = qs.filter(**_blank_or_exact("source", source_val))

    # ---- text filter ----
    if contact_person:
        qs = qs.filter(contact_person__icontains=contact_person)

    # ---- created_at range ----
    if created_from:
        try:
            dt_from = _dt.datetime.strptime(created_from, "%Y-%m-%d").date()
            qs = qs.filter(created_at__gte=dt_from)
        except ValueError:
            pass

    if created_to:
        try:
            dt_to = _dt.datetime.strptime(created_to, "%Y-%m-%d").date()
            qs = qs.filter(created_at__lte=dt_to)
        except ValueError:
            pass

    # ---- totals (distinct customers) ----
    totals = {
        "customers": (
            qs.filter(customer_name__isnull=False)
              .values("customer_name")
              .distinct()
              .count()
        ),
        "states": (
            qs.filter(state__isnull=False)
              .values("state")
              .distinct()
              .count()
        ),
        "cities": (
            qs.filter(city__isnull=False)
              .values("city")
              .distinct()
              .count()
        ),
        "executives": (
            qs.filter(executive_name__isnull=False)
              .values("executive_name")
              .distinct()
              .count()
        ),
    }

    # ---- summary tables (distinct customers) ----
    by_executive = (
        qs.values(label=F("executive_name__subcategory"))
          .annotate(count=Count("customer_name", distinct=True))
          .order_by("-count", "label")
    )
    by_state = (
        qs.values(label=F("state__subcategory"))
          .annotate(count=Count("customer_name", distinct=True))
          .order_by("-count", "label")
    )
    by_city = (
        qs.values(label=F("city__subcategory"))
          .annotate(count=Count("customer_name", distinct=True))
          .order_by("-count", "label")
    )
    by_sub_profile = (
        qs.values(label=F("sub_profile__subcategory"))
          .annotate(count=Count("customer_name", distinct=True))
          .order_by("-count", "label")
    )

    # ---- customer table (entries count) ----
    by_customer = (
        qs.values(label=F("customer_name__subcategory"))
          .annotate(count=Count("id"))
          .order_by("-count", "label")
    )

    # ---- detail table ----
    qs_detail = qs.order_by("created_at")[:500]
    detail_rows = []
    for obj in qs_detail:
        detail_rows.append(
            {
                "created_at": obj.created_at.strftime("%d-%m-%Y") if obj.created_at else "",
                "customer_name": obj.customer_name.subcategory if obj.customer_name else "",
                "sub_profile": obj.sub_profile.subcategory if obj.sub_profile else "",
                "contact_person": obj.contact_person or "",
                "contact_no": obj.contact_no or "",
                "email_id": obj.email_id or "",
                "address": obj.address or "",
                "city": obj.city.subcategory if obj.city else "",
                "state": obj.state.subcategory if obj.state else "",
                "zone": obj.zone.subcategory if obj.zone else "",
                "executive": obj.executive_name.subcategory if obj.executive_name else "",
                "source": obj.source.subcategory if obj.source else "",
            }
        )

    return JsonResponse(
        {
            "totals": totals,
            "by_executive": list(by_executive),
            "by_state": list(by_state),
            "by_city": list(by_city),
            "by_sub_profile": list(by_sub_profile),
            "by_customer": list(by_customer),
            "table": detail_rows,
        }
    )













@login_required
def pc_task_create(request):
    """
    Create a single Customer_Followup record.
    """
    if not request.user.has_perm("PERSONAL_CARE.add_customer_followup"):
        messages.error(request, "You do not have permission to add Customer Follow-ups.")
        logger.warning("User '%s' tried to create Customer_Followup without permission.", request.user.username)
        return redirect("indexpage")

    if request.method == "POST":
        logger.info("Customer_Followup create POST by user=%s | data=%s", request.user.username, dict(request.POST))
        form = CustomerFollowupForm(request.POST)
        if form.is_valid():
            followup = form.save(commit=False)
            followup.created_by = request.user
            followup.save()
            logger.info("Customer_Followup created successfully: id=%s by user=%s", followup.pk, request.user.username)
            messages.success(request, "Customer follow-up saved successfully.")
            return redirect("pc_task_list")

        logger.error("CustomerFollowupForm (create) invalid for user=%s | errors=%s",
                     request.user.username, form.errors.as_json())
        messages.error(request, "There were errors in the form. Please correct them and try again.")
    else:
        logger.info("Customer_Followup create GET accessed by user=%s", request.user.username)
        form = CustomerFollowupForm()

    customers = (
        PC_CustomerMaster.objects
        .select_related("customer_name")
        .order_by("customer_name__subcategory")
    )
    executives = (
        PersonalCareMaster.objects
        .filter(category="Executive Name")
        .order_by("subcategory")
    )
    followup_status_options = (
        PersonalCareMaster.objects
        .filter(category="Followup Status")
        .order_by("subcategory")
    )

    # ✅ NEW (for Customer Profile datalist)
    customer_profile_options = (
        PersonalCareMaster.objects
        .filter(category="Sub Profile")
        .order_by("subcategory")
    )

    context = {
        "form": form,
        "customers": customers,
        "executives": executives,
        "followup_status_options": followup_status_options,
        "customer_profile_options": customer_profile_options,  # ✅ NEW
        "is_edit": False,
        "page_title": "Create Customer Follow-up",
        "submit_label": "Save Follow-up",
    }
    return render(request, "PERSONAL_CARE/pc_task_form.html", context)


@login_required
def pc_task_edit(request, pk):
    """
    Edit an existing Customer_Followup using the same HTML form.
    """
    if not request.user.has_perm("PERSONAL_CARE.change_customer_followup"):
        messages.error(request, "You do not have permission to edit Customer Follow-ups.")
        logger.warning("User '%s' tried to edit Customer_Followup id=%s without permission.", request.user.username, pk)
        return redirect("pc_task_list")

    followup = get_object_or_404(Customer_Followup, pk=pk)

    if request.method == "POST":
        logger.info("Customer_Followup edit POST by user=%s | id=%s | data=%s",
                    request.user.username, followup.pk, dict(request.POST))
        form = CustomerFollowupForm(request.POST, instance=followup)
        if form.is_valid():
            form.save()
            logger.info("Customer_Followup updated successfully: id=%s by user=%s", followup.pk, request.user.username)
            messages.success(request, "Customer follow-up updated successfully.")
            return redirect("pc_task_list")

        logger.error("CustomerFollowupForm (edit) invalid for user=%s | errors=%s",
                     request.user.username, form.errors.as_json())
        messages.error(request, "There were errors in the form. Please correct them and try again.")
    else:
        logger.info("Customer_Followup edit GET accessed by user=%s | id=%s", request.user.username, followup.pk)
        form = CustomerFollowupForm(instance=followup)

    customers = (
        PC_CustomerMaster.objects
        .select_related("customer_name")
        .order_by("customer_name__subcategory")
    )
    executives = (
        PersonalCareMaster.objects
        .filter(category="Executive Name")
        .order_by("subcategory")
    )
    followup_status_options = (
        PersonalCareMaster.objects
        .filter(category="Followup Status")
        .order_by("subcategory")
    )

    # ✅ NEW (for Customer Profile datalist)
    customer_profile_options = (
        PersonalCareMaster.objects
        .filter(category="Sub Profile")
        .order_by("subcategory")
    )

    context = {
        "form": form,
        "customers": customers,
        "executives": executives,
        "followup_status_options": followup_status_options,
        "customer_profile_options": customer_profile_options,  # ✅ NEW
        "is_edit": True,
        "page_title": "Edit Customer Follow-up",
        "submit_label": "Update Follow-up",
    }
    return render(request, "PERSONAL_CARE/pc_task_form.html", context)


def _parse_date_param(value: str):
    """
    Accept HTML date 'YYYY-MM-DD'. Return date or None.
    """
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


@login_required
def pc_task_list(request):
    """
    List + filter Customer_Followup records.
    (URL/view name kept as pc_task_list for backward compatibility.)
    """

    # ---- Base queryset ----
    qs = (
        Customer_Followup.objects
        .select_related(
            "customer_name",
            "executive_name",
            "followup_status",
        )
        .order_by("-date", "-id")
    )

    # 🔒 Apply executive restriction
    qs = restrict_to_executive(qs, request.user)

    # ---- Read filters from GET ----
    customer_name = (request.GET.get("customer_name") or "").strip()
    executive_name = (request.GET.get("executive_name") or "").strip()
    mode_of_followup = (request.GET.get("mode_of_followup") or "").strip()
    followup_status = (request.GET.get("followup_status") or "").strip()
    date_from_raw = (request.GET.get("date_from") or "").strip()
    date_to_raw = (request.GET.get("date_to") or "").strip()

    date_from = _parse_date_param(date_from_raw)
    date_to = _parse_date_param(date_to_raw)

    # ---- Apply filters ----
    if customer_name:
        qs = qs.filter(customer_name__subcategory__icontains=customer_name)

    if executive_name:
        qs = qs.filter(executive_name__subcategory__icontains=executive_name)

    if mode_of_followup:
        qs = qs.filter(mode_of_followup=mode_of_followup)

    if followup_status:
        qs = qs.filter(followup_status_id=followup_status)

    if date_from:
        qs = qs.filter(date__gte=date_from)

    if date_to:
        qs = qs.filter(date__lte=date_to)

    # ---- Datalist / select options ----
    customer_name_options = (
        PC_CustomerMaster.objects
        .select_related("customer_name")
        .exclude(customer_name__isnull=True)
        .values_list("customer_name__subcategory", flat=True)
        .order_by("customer_name__subcategory")
        .distinct()
    )

    executive_options = (
        PersonalCareMaster.objects
        .filter(category="Executive Name")
        .values_list("subcategory", flat=True)
        .order_by("subcategory")
        .distinct()
    )

    mode_of_followup_options = Customer_Followup.MODE_OF_FOLLOWUP_CHOICES

    followup_status_options = (
        PersonalCareMaster.objects
        .filter(category="Followup Status")
        .order_by("subcategory")
    )

    # ---- Pagination ----
    paginator = Paginator(qs, 25)
    page_number = request.GET.get("page") or 1
    page_obj = paginator.get_page(page_number)
    followups = page_obj.object_list

    querydict = request.GET.copy()
    if "page" in querydict:
        querydict.pop("page")
    querystring = querydict.urlencode()

    context = {
        "tasks": followups,          # keeping 'tasks' name so template still works
        "page_obj": page_obj,
        "paginator": paginator,
        "is_paginated": page_obj.has_other_pages(),
        "filters": {
            "customer_name": customer_name,
            "executive_name": executive_name,
            "mode_of_followup": mode_of_followup,
            "followup_status": followup_status,
            "date_from": date_from_raw,
            "date_to": date_to_raw,
        },
        "customer_name_options": customer_name_options,
        "executive_options": executive_options,
        "mode_of_followup_options": mode_of_followup_options,
        "followup_status_options": followup_status_options,
        "querystring": querystring,
    }
    return render(request, "PERSONAL_CARE/pc_task_list.html", context)








@login_required
def pc_task_export_excel(request):
    if not request.user.has_perm("PERSONAL_CARE.view_customer_followup"):
        messages.error(request, "You do not have permission to export follow-ups.")
        return redirect("pc_task_list")

    # ---------- Base queryset ----------
    qs = (
        Customer_Followup.objects
        .select_related(
            "customer_name",
            "customer_profile",
            "executive_name",
            "followup_status",
        )
        .order_by("-date", "-id")
    )

    # 🔒 Apply executive restriction (same rule as list view)
    qs = restrict_to_executive(qs, request.user)

    # ---------- Read filters (same as list view) ----------
    customer_name = (request.GET.get("customer_name") or "").strip()
    executive_name = (request.GET.get("executive_name") or "").strip()
    mode_of_followup = (request.GET.get("mode_of_followup") or "").strip()
    followup_status = (request.GET.get("followup_status") or "").strip()
    date_from_raw = (request.GET.get("date_from") or "").strip()
    date_to_raw = (request.GET.get("date_to") or "").strip()

    date_from = _parse_date_param(date_from_raw)
    date_to = _parse_date_param(date_to_raw)

    if customer_name:
        qs = qs.filter(customer_name__subcategory__icontains=customer_name)
    if executive_name:
        qs = qs.filter(executive_name__subcategory__icontains=executive_name)
    if mode_of_followup:
        qs = qs.filter(mode_of_followup=mode_of_followup)
    if followup_status:
        qs = qs.filter(followup_status_id=followup_status)  # keep same as your list view
    if date_from:
        qs = qs.filter(date__gte=date_from)
    if date_to:
        qs = qs.filter(date__lte=date_to)

    # ---------- Build Excel in memory ----------
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True, "remove_timezone": True})
    ws = workbook.add_worksheet("Customer Follow-ups")

    title_format = workbook.add_format({
        "bold": True, "font_size": 14, "align": "center", "valign": "vcenter"
    })
    header_format = workbook.add_format({
        "bold": True, "bg_color": "#E5E7EB", "border": 1,
        "align": "center", "valign": "vcenter"
    })
    cell_format = workbook.add_format({"border": 1, "valign": "top"})
    date_format = workbook.add_format({"border": 1, "num_format": "dd-mm-yyyy", "valign": "top"})

    headers = [
        "Date",
        "Customer Name",
        "Customer Profile",
        "Mode of Follow-up",
        "Stage",
        "Follow-up Status",
        "Executive Name",
    ]

    col_count = len(headers)
    ws.merge_range(0, 0, 0, col_count - 1, "Customer Follow-ups", title_format)

    for col, h in enumerate(headers):
        ws.write(1, col, h, header_format)

    row = 2
    for obj in qs:
        # 0 Date
        if obj.date:
            ws.write_datetime(
                row, 0,
                datetime(obj.date.year, obj.date.month, obj.date.day),
                date_format
            )
        else:
            ws.write(row, 0, "", cell_format)

        # 1 Customer Name
        ws.write(row, 1, obj.customer_name.subcategory if obj.customer_name else "", cell_format)

        # 2 Customer Profile
        ws.write(row, 2, obj.customer_profile.subcategory if obj.customer_profile else "", cell_format)

        # 3 Mode of Follow-up
        ws.write(row, 3, obj.get_mode_of_followup_display(), cell_format)
        
                # 4 Follow-up Status
        ws.write(row, 4, obj.followup_status.subcategory if obj.followup_status else "", cell_format)

        # 5 Remark
        ws.write(row, 5, obj.description or "", cell_format)

        # 6 Executive Name
        ws.write(row, 6, obj.executive_name.subcategory if obj.executive_name else "", cell_format)

        row += 1

    widths = [12, 25, 22, 22, 18, 50, 22]
    for i, w in enumerate(widths):
        ws.set_column(i, i, w)

    workbook.close()
    output.seek(0)

    filename = f"customer_followups_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    response = HttpResponse(
        output.read(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response




@login_required
def pc_followup_upload(request):
    """
    Step-1: Upload file -> preview (dry_run)
    """
    if not request.user.has_perm("PERSONAL_CARE.add_customer_followup"):
        messages.error(request, "You do not have permission to upload follow-ups.")
        return redirect("pc_task_list")

    if request.method == "GET":
        return render(request, "PERSONAL_CARE/pc_followup_upload.html")

    # POST (file upload)
    f = request.FILES.get("file")
    if not f:
        messages.error(request, "Please choose an Excel file.")
        return redirect("pc_followup_upload")

    if not f.name.lower().endswith((".xlsx", ".xls")):
        messages.error(request, "Only Excel files (.xlsx/.xls) are allowed.")
        return redirect("pc_followup_upload")

    # Save temp file
    tmp_name = f"tmp/pc_followup_{uuid.uuid4().hex}_{f.name}"
    path = default_storage.save(tmp_name, f)

    # Read into Dataset
    with default_storage.open(path, "rb") as fh:
        data = fh.read()

    dataset = Dataset()
    dataset.load(data, format="xlsx")

    resource = CustomerFollowupResource()

    # Optional: force executive for non-superuser
    force_exec = None
    if not request.user.is_superuser:
        try:
            force_exec = PC_Executive.objects.get(user=request.user, active=True).executive
        except PC_Executive.DoesNotExist:
            force_exec = None

    # Dry run (preview)
    result = resource.import_data(
        dataset,
        dry_run=True,
        raise_errors=False,
        user=request.user,
        force_exec=force_exec,
    )

    # store temp file path in session for confirm step
    request.session["pc_followup_upload_tmp"] = path

    # show first 50 rows in preview
    headers = dataset.headers or []
    preview_rows = []

    for i in range(min(50, len(dataset))):
        # dataset[i] is a list in same order as headers
        preview_rows.append(list(dataset[i]))

    # row errors
    row_errors = []
    try:
        # import-export provides this
        row_errors = result.row_errors()
    except Exception:
        row_errors = []

    context = {
        "filename": f.name,
        "headers": headers,
        "preview_rows": preview_rows,
        "result": result,
        "row_errors": row_errors,
    }
    return render(request, "PERSONAL_CARE/pc_followup_upload_preview.html", context)


@login_required
def pc_followup_upload_confirm(request):
    """
    Step-2: Confirm -> actual import
    """
    if not request.user.has_perm("PERSONAL_CARE.add_customer_followup"):
        messages.error(request, "You do not have permission to upload follow-ups.")
        return redirect("pc_task_list")

    path = request.session.get("pc_followup_upload_tmp")
    if not path or not default_storage.exists(path):
        messages.error(request, "Upload session expired. Please upload again.")
        return redirect("pc_followup_upload")

    with default_storage.open(path, "rb") as fh:
        data = fh.read()

    dataset = Dataset()
    dataset.load(data, format="xlsx")

    resource = CustomerFollowupResource()

    force_exec = None
    if not request.user.is_superuser:
        try:
            force_exec = PC_Executive.objects.get(user=request.user, active=True).executive
        except PC_Executive.DoesNotExist:
            force_exec = None

    result = resource.import_data(
        dataset,
        dry_run=False,
        raise_errors=False,
        user=request.user,
        force_exec=force_exec,
    )

    # cleanup
    default_storage.delete(path)
    request.session.pop("pc_followup_upload_tmp", None)

    # message summary
    messages.success(
        request,
        f"Follow-up import completed. Created: {result.totals.get('new', 0)} | "
        f"Updated: {result.totals.get('update', 0)} | Errors: {len(getattr(result, 'invalid_rows', []) or [])}"
    )
    return redirect("pc_task_list")




@login_required
def pc_task_delete(request, pk):
    if not request.user.has_perm("PERSONAL_CARE.delete_customer_followup"):
        messages.error(request, "You do not have permission to delete Customer Follow-ups.")
        logger.warning( "User '%s' tried to delete Customer_Followup id=%s without permission.",
            request.user.username,  pk, )
        return redirect("pc_task_list")
    # 🔒 apply executive restriction before fetching object
    base_qs = Customer_Followup.objects.all()
    qs = restrict_to_executive(base_qs, request.user)
    followup = get_object_or_404(qs, pk=pk)
    if request.method == "POST":
        logger.info("Customer_Followup delete POST by user=%s | id=%s",
            request.user.username, followup.pk, )
        followup.delete()
        messages.success(request, "Customer follow-up deleted successfully.")
        return redirect("pc_task_list")
    # If someone hits the URL with GET, just go back to list
    messages.warning(request, "Deletion must be confirmed from the list page.")
    return redirect("pc_task_list")




# =====================================================================================



def _options(category):
    """Return distinct subcategory values for a given category."""
    return (
        PersonalCareMaster.objects.filter(category=category)
        .order_by("subcategory")
        .values_list("subcategory", flat=True)
        .distinct()
    )


@login_required
def pc_other_customer_create(request, pk=None):
    instance = None
    if pk is not None:
        base_qs = restrict_to_executive(
            PC_Other_CustomerMaster.objects.select_related(
                "customer_name",
                "customer_profile",
                "sub_profile",
                "place",
                "city",
                "state",
                "zone",
                "executive_name",
                "source",
            ),
            request.user,
        )
        instance = get_object_or_404(base_qs, pk=pk)

    # ---- Permission checks (add vs edit) ----
    if instance is None:
        if not request.user.has_perm("PERSONAL_CARE.add_pc_other_customermaster"):
            messages.error(request, "You do not have permission to add Personal Care other customers.")
            logger.warning("User '%s' tried to create PC Other Customer without permission.", request.user.username)
            return redirect("indexpage")
    else:
        if not request.user.has_perm("PERSONAL_CARE.change_pc_other_customermaster"):
            messages.error(request, "You do not have permission to edit Personal Care other customers.")
            logger.warning("User '%s' tried to edit PC Other Customer (id=%s) without permission.", request.user.username, instance.pk)
            return redirect("indexpage")
    # ----------------------------------------

    if request.method == "POST":
        form = PCOtherCustomerMasterForm(request.POST, instance=instance)
        if form.is_valid():
            form.save()
            if instance is None:
                messages.success(request, "Other customer saved successfully.")
                return redirect("pc_other_customer_create")
            else:
                messages.success(request, "Other customer updated successfully.")
                return redirect("pc_other_customer_list")
    else:
        form = PCOtherCustomerMasterForm(instance=instance)

    context = {
        "form": form,
        "customer_name_options": _options("Customer Name"),
        "customer_profile_options": _options("Customer Profile"),
        "sub_profile_options": _options("Sub Profile"),
        "designation_options": _options("Designation"),
        "place_options": _options("Place"),
        "city_options": _options("City"),
        "state_options": _options("State"),
        "zone_options": _options("Zone"),
        "executive_options": _options("Executive Name"),
        "source_options": _options("Source"),
        "is_edit": instance is not None,
    }
    # you can reuse same template or create new one:
    return render(request, "PERSONAL_CARE/pc_other_customer_form.html", context)



@login_required
def pc_other_customer_delete(request, pk):
    # ---- Permission check ----
    if not request.user.has_perm("PERSONAL_CARE.delete_pc_other_customermaster"):
        messages.error(request, "You do not have permission to delete Personal Care other customers.")
        # logger.warning("User '%s' tried to delete PC Other Customer (id=%s) without permission.", request.user.username, pk)
        return redirect("indexpage")
    # --------------------------

    qs = restrict_to_executive(PC_Other_CustomerMaster.objects.all(), request.user)
    obj = get_object_or_404(qs, pk=pk)

    if request.method == "POST":
        obj.delete()
        # logger.info("PC Other Customer deleted by user '%s' | id=%s", request.user.username, pk)
        messages.success(request, "Other customer deleted successfully.")
        return redirect("pc_other_customer_list")

    return HttpResponseNotAllowed(["POST"])


@login_required
def pc_other_customer_list(request):
    qs = PC_Other_CustomerMaster.objects.select_related(
        "customer_name",
        "customer_profile",
        "sub_profile",
        "place",
        "city",
        "state",
        "zone",
        "executive_name",
        "source",
    )
    qs = restrict_to_executive(qs, request.user)

    filters = {
        "created_from": request.GET.get("created_from", "").strip(),
        "created_to": request.GET.get("created_to", "").strip(),
        "customer_name": request.GET.get("customer_name", "").strip(),
        "customer_profile": request.GET.get("customer_profile", "").strip(),
        "sub_profile": request.GET.get("sub_profile", "").strip(),
        "contact_person": request.GET.get("contact_person", "").strip(),
        "city": request.GET.get("city", "").strip(),
        "state": request.GET.get("state", "").strip(),
        "zone": request.GET.get("zone", "").strip(),
        "executive_name": request.GET.get("executive_name", "").strip(),
        "source": request.GET.get("source", "").strip(),
    }

    # ---- created date FROM–TO range ----
    if filters["created_from"]:
        d = parse_date(filters["created_from"])
        if d:
            qs = qs.filter(created_at__gte=d)

    if filters["created_to"]:
        d = parse_date(filters["created_to"])
        if d:
            qs = qs.filter(created_at__lte=d)

    # ---- other filters ----
    if filters["customer_name"]:
        qs = qs.filter(customer_name__subcategory__icontains=filters["customer_name"])

    if filters["customer_profile"]:
        qs = qs.filter(customer_profile__subcategory__icontains=filters["customer_profile"])

    if filters["sub_profile"]:
        qs = qs.filter(sub_profile__subcategory__icontains=filters["sub_profile"])

    if filters["contact_person"]:
        qs = qs.filter(contact_person__icontains=filters["contact_person"])

    if filters["city"]:
        qs = qs.filter(city__subcategory__icontains=filters["city"])

    if filters["state"]:
        qs = qs.filter(state__subcategory__icontains=filters["state"])

    if filters["zone"]:
        qs = qs.filter(zone__subcategory__icontains=filters["zone"])

    if filters["executive_name"]:
        qs = qs.filter(executive_name__subcategory__icontains=filters["executive_name"])

    if filters["source"]:
        qs = qs.filter(source__subcategory__icontains=filters["source"])

    qs = qs.order_by("-created_at")

    # ✅ counts (after filters, before pagination)
    total_records = qs.count()
    distinct_customers = qs.values("customer_name_id").distinct().count()

    paginator = Paginator(qs, 50)
    page_number = request.GET.get("page")
    page_obj = paginator.get_page(page_number)

    querydict = request.GET.copy()
    querydict.pop("page", None)
    querystring = querydict.urlencode()

    context = {
        "customers": page_obj,
        "page_obj": page_obj,
        "paginator": paginator,
        "is_paginated": page_obj.has_other_pages(),
        "querystring": querystring,
        "filters": filters,

        "total_records": total_records,
        "distinct_customers": distinct_customers,

        "customer_name_options": _options("Customer Name"),
        "customer_profile_options": _options("Customer Profile"),
        "sub_profile_options": _options("Sub Profile"),
        "city_options": _options("City"),
        "state_options": _options("State"),
        "zone_options": _options("Zone"),
        "executive_options": _options("Executive Name"),
        "source_options": _options("Source"),
    }
    return render(request, "PERSONAL_CARE/pc_other_customer_list.html", context)



def _safe(obj, attr=None):
    if obj is None:
        return ""
    if attr:
        val = getattr(obj, attr, "")
    else:
        val = obj
    if val is None:
        return ""
    return str(val)


def _filtered_other_customers(request):
    """Apply all filters and return queryset + filter dict."""
    qs = PC_Other_CustomerMaster.objects.select_related(
        "customer_name",
        "customer_profile",
        "sub_profile",
        "place",
        "city",
        "state",
        "zone",
        "executive_name",
        "source",
    )
    qs = restrict_to_executive(qs, request.user)

    filters = {
        "created_from": request.GET.get("created_from", "").strip(),
        "created_to": request.GET.get("created_to", "").strip(),
        "customer_name": request.GET.get("customer_name", "").strip(),
        "customer_profile": request.GET.get("customer_profile", "").strip(),
        "sub_profile": request.GET.get("sub_profile", "").strip(),
        "contact_person": request.GET.get("contact_person", "").strip(),
        "city": request.GET.get("city", "").strip(),
        "state": request.GET.get("state", "").strip(),
        "zone": request.GET.get("zone", "").strip(),
        "executive_name": request.GET.get("executive_name", "").strip(),
        "source": request.GET.get("source", "").strip(),
    }

    if filters["created_from"]:
        d = parse_date(filters["created_from"])
        if d:
            qs = qs.filter(created_at__gte=d)

    if filters["created_to"]:
        d = parse_date(filters["created_to"])
        if d:
            qs = qs.filter(created_at__lte=d)

    if filters["customer_name"]:
        qs = qs.filter(customer_name__subcategory__icontains=filters["customer_name"])
    if filters["customer_profile"]:
        qs = qs.filter(customer_profile__subcategory__icontains=filters["customer_profile"])
    if filters["sub_profile"]:
        qs = qs.filter(sub_profile__subcategory__icontains=filters["sub_profile"])
    if filters["contact_person"]:
        qs = qs.filter(contact_person__icontains=filters["contact_person"])
    if filters["city"]:
        qs = qs.filter(city__subcategory__icontains=filters["city"])
    if filters["state"]:
        qs = qs.filter(state__subcategory__icontains=filters["state"])
    if filters["zone"]:
        qs = qs.filter(zone__subcategory__icontains=filters["zone"])
    if filters["executive_name"]:
        qs = qs.filter(executive_name__subcategory__icontains=filters["executive_name"])
    if filters["source"]:
        qs = qs.filter(source__subcategory__icontains=filters["source"])

    qs = qs.order_by("customer_name__subcategory")
    return qs, filters


@login_required
def pc_other_customer_export(request):
    """Download the filtered other customer list as Excel."""
    qs, _ = _filtered_other_customers(request)

    output = BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    ws = workbook.add_worksheet("Other Customers")

    title_fmt = workbook.add_format({
        "bold": True,
        "font_size": 14,
        "align": "center",
        "valign": "vcenter",
    })
    header_fmt = workbook.add_format({"bold": True, "bg_color": "#D9E1F2"})

    headers = [
        "Created Date",
        "Customer Name",
        "Customer Profile",
        "Sub Profile",
        "Core Business",
        "Contact Person",
        "Contact No",
        "Email ID",
        "Address",
        "Place",
        "City",
        "State",
        "Zone",
        "Executive Name",
        "Source",
        "Updated Date",
    ]

    last_col = len(headers) - 1
    ws.merge_range(0, 0, 0, last_col, "Personal Care Other Customer Master", title_fmt)

    header_row = 2
    for col, h in enumerate(headers):
        ws.write(header_row, col, h, header_fmt)

    row = header_row + 1
    for obj in qs:
        col = 0

        created_str = obj.created_at.strftime("%d-%m-%Y") if obj.created_at else ""
        ws.write(row, col, created_str); col += 1

        ws.write(row, col, _safe(obj.customer_name, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.customer_profile, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.sub_profile, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.core_business)); col += 1
        ws.write(row, col, _safe(obj.contact_person)); col += 1
        ws.write(row, col, _safe(obj.contact_no)); col += 1
        ws.write(row, col, _safe(obj.email_id)); col += 1
        ws.write(row, col, _safe(obj.address)); col += 1
        ws.write(row, col, _safe(obj.place, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.city, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.state, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.zone, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.executive_name, "subcategory")); col += 1
        ws.write(row, col, _safe(obj.source, "subcategory")); col += 1

        updated_str = obj.updated_at.strftime("%d-%m-%Y") if obj.updated_at else ""
        ws.write(row, col, updated_str); col += 1

        row += 1

    workbook.close()
    output.seek(0)

    resp = HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    resp["Content-Disposition"] = 'attachment; filename="pc_other_customers.xlsx"'
    return resp