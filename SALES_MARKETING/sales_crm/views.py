from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db.models import Q, Count
from django.shortcuts import get_object_or_404, redirect, render

from .forms import *
from .models import *
import logging
logger = logging.getLogger(__name__)


@login_required
def crm_home(request):
    # ✅ SQL Server FIX:
    # Clear default Meta.ordering before aggregation (values/annotate)
    counts = (
        SalesLead.objects
        .order_by()                # ✅ removes Meta.ordering (updated_at, id)
        .values("status")
        .annotate(c=Count("id"))
    )
    map_counts = {row["status"]: row["c"] for row in counts}

    # KPI totals (keep count() separate)
    kpis = {
        "total": SalesLead.objects.count(),
        "new": map_counts.get(LeadStatus.NEW, 0),
        "in_progress": map_counts.get(LeadStatus.IN_PROGRESS, 0),
        "qualified": map_counts.get(LeadStatus.QUALIFIED, 0),
        "won": map_counts.get(LeadStatus.WON, 0),
        "lost": map_counts.get(LeadStatus.LOST, 0),
    }

    # ✅ Make "recent" deterministic (don’t rely on slicing without explicit order)
    recent = (
        SalesLead.objects
        .select_related("assigned_to")
        .order_by("-updated_at", "-id")[:10]
    )

    return render(request, "sales_crm/home.html", {
        "kpis": kpis,
        "recent": recent,
    })


@login_required
def lead_list(request):
    q = (request.GET.get("q") or "").strip()
    status = (request.GET.get("status") or "").strip()
    mine = (request.GET.get("mine") or "").strip()  # "1" means assigned_to == user

    qs = SalesLead.objects.select_related("assigned_to")

    if q:
        qs = qs.filter(
            Q(name__icontains=q) |
            Q(company__icontains=q) |
            Q(phone__icontains=q) |
            Q(email__icontains=q) |
            Q(city__icontains=q) |
            Q(state__icontains=q)
        )

    if status:
        qs = qs.filter(status=status)

    if mine == "1":
        qs = qs.filter(assigned_to=request.user)

    # ✅ explicit ordering for consistent list
    qs = qs.order_by("-updated_at", "-id")

    return render(request, "sales_crm/lead_list.html", {
        "rows": qs,
        "q": q,
        "status": status,
        "mine": mine,
        "status_choices": LeadStatus.choices,
    })


@login_required
def lead_create(request):
    if request.method == "POST":
        form = SalesLeadForm(request.POST)
        if form.is_valid():
            obj = form.save(commit=False)
            obj.created_by = request.user
            if not obj.assigned_to:
                obj.assigned_to = request.user
            obj.save()
            messages.success(request, "Lead created.")
            return redirect("sales_crm:lead_detail", pk=obj.pk)
        messages.error(request, "Please correct the errors below.")
    else:
        form = SalesLeadForm()

    return render(request, "sales_crm/lead_form.html", {
        "form": form,
        "mode": "create",
    })


@login_required
def lead_edit(request, pk: int):
    obj = get_object_or_404(SalesLead, pk=pk)

    if request.method == "POST":
        form = SalesLeadForm(request.POST, instance=obj)
        if form.is_valid():
            form.save()
            messages.success(request, "Lead updated.")
            return redirect("sales_crm:lead_detail", pk=obj.pk)
        messages.error(request, "Please correct the errors below.")
    else:
        form = SalesLeadForm(instance=obj)

    return render(request, "sales_crm/lead_form.html", {
        "form": form,
        "mode": "edit",
        "obj": obj,
    })


@login_required
def lead_detail(request, pk: int):
    obj = get_object_or_404(
        SalesLead.objects
        .select_related("assigned_to", "created_by")
        .prefetch_related("followups"),
        pk=pk
    )

    followup_form = LeadFollowUpForm()

    return render(request, "sales_crm/lead_detail.html", {
        "obj": obj,
        "followup_form": followup_form,
    })


@login_required
def lead_followup_add(request, pk: int):
    obj = get_object_or_404(SalesLead, pk=pk)

    if request.method != "POST":
        return redirect("sales_crm:lead_detail", pk=obj.pk)

    form = LeadFollowUpForm(request.POST)
    if form.is_valid():
        fu = form.save(commit=False)
        fu.lead = obj
        fu.created_by = request.user
        fu.save()
        messages.success(request, "Follow-up added.")
    else:
        messages.error(request, "Follow-up not saved. Please check note/date.")

    return redirect("sales_crm:lead_detail", pk=obj.pk)



def create_customer_visit(request):

    if not request.user.has_perm("sales_crm.add_customervisit"):
        logger.warning(
            "User '%s' tried to create customer visit without permission.",
            request.user.username
        )
        messages.error(request, "You do not have permission to add visits.")
        return redirect("sales_crm:customer_visit_list")

    if request.method == "POST":
        form = CustomerVisitForm(request.POST)

        if form.is_valid():
            visit = form.save(commit=False)

            logger.info(
                "User '%s' is creating a new customer visit.",
                request.user.username
            )

            new_customer = form.cleaned_data.get("new_customer")
            if new_customer:
                customer_obj, _ = Customer.objects.get_or_create(name=new_customer)
                visit.customer = customer_obj

            new_product = form.cleaned_data.get("new_product")
            if new_product:
                product_obj, _ = Product.objects.get_or_create(name=new_product)
                visit.product = product_obj

            new_industry = form.cleaned_data.get("new_industry")
            if new_industry:
                industry_obj, _ = Industry.objects.get_or_create(name=new_industry)
                visit.industry = industry_obj

            new_sales_person = form.cleaned_data.get("new_sales_person")
            if new_sales_person:
                sales_obj, _ = SalesPerson.objects.get_or_create(name=new_sales_person)
                visit.sales_person = sales_obj

            visit.save()

            logger.info(
                "Customer visit created by user '%s' for customer '%s'.",
                request.user.username,
                visit.customer
            )

            return redirect("sales_crm:customer_visit_list")

    else:
        form = CustomerVisitForm()

    return render(request, "sales_crm/customer_visit_form.html", {"form": form})





def customer_visit_list(request):

    if not request.user.has_perm("sales_crm.view_customervisit"):
        logger.warning(
            "User '%s' tried to access customer visit list without permission.",
            request.user.username
        )
        messages.error(request, "You do not have permission to view customer visits.")
        return redirect("home")   # or any page you want

    visits = CustomerVisit.objects.all().order_by("-visit_date")

    return render(request, "sales_crm/customer_visit_list.html", {
        "visits": visits
    })


def update_customer_visit(request, pk):

    if not request.user.has_perm("sales_crm.change_customervisit"):
        logger.warning(
            "User '%s' tried to edit customer visit without permission.",
            request.user.username
        )
        messages.error(request, "You do not have permission to edit.")
        return redirect("sales_crm:customer_visit_list")

    visit = get_object_or_404(CustomerVisit, pk=pk)

    if request.method == "POST":
        form = CustomerVisitForm(request.POST, instance=visit)

        if form.is_valid():
            logger.info(
                "User '%s' updated customer visit ID %s.",
                request.user.username,
                pk
            )

            visit = form.save()
            return redirect("sales_crm:customer_visit_list")

    else:
        form = CustomerVisitForm(instance=visit)

    return render(request, "sales_crm/customer_visit_form.html", {"form": form})



def delete_customer_visit(request, pk):

    if not request.user.has_perm("sales_crm.delete_customervisit"):
        logger.warning(
            "User '%s' tried to delete customer visit without permission.",
            request.user.username
        )
        messages.error(request, "You do not have permission to delete")
        return redirect("sales_crm:customer_visit_list")

    visit = get_object_or_404(CustomerVisit, pk=pk)

    logger.info(
        "User '%s' deleted customer visit ID %s.",
        request.user.username,
        pk
    )

    visit.delete()

    return redirect("sales_crm:customer_visit_list")



def followup_list(request):
    followups = FollowUp.objects.all().order_by("-followup_date")
    return render(request, "sales_crm/followup_list.html", {"followups": followups})


def followup_create(request, visit_id):

    if not request.user.has_perm("sales_crm.add_followup"):
        logger.warning(
            "User '%s' tried to create follow-up without permission.",
            request.user.username
        )
        messages.error(request, "You do not have permission to add follow-up.")
        return redirect("sales_crm:customer_visit_list")

    visit = CustomerVisit.objects.get(id=visit_id)

    if request.method == "POST":
        form = FollowUpForm(request.POST)

        if form.is_valid():
            followup = form.save(commit=False)
            followup.visit = visit
            followup.save()

            logger.info(
                "User '%s' created follow-up for visit ID %s.",
                request.user.username,
                visit_id
            )

            return redirect("sales_crm:followup_list")

    else:
        form = FollowUpForm()

    return render(request, "sales_crm/followup_form.html", {"form": form, "visit": visit})






def task_list(request):
    tasks = Task.objects.all().order_by("due_date")
    return render(request, "sales_crm/task_list.html", {"tasks": tasks})


def task_create(request, visit_id):

    if not request.user.has_perm("sales_crm.add_task"):
        logger.warning(
            "User '%s' tried to create task without permission.",
            request.user.username
        )
        messages.error(request, "You do not have permission to create tasks.")
        return redirect("sales_crm:customer_visit_list")

    visit = get_object_or_404(CustomerVisit, id=visit_id)

    if request.method == "POST":
        form = TaskForm(request.POST)

        if form.is_valid():
            task = form.save(commit=False)
            task.visit = visit
            task.save()

            logger.info(
                "User '%s' created task for visit ID %s.",
                request.user.username,
                visit_id
            )

            return redirect("sales_crm:task_list")

    else:
        form = TaskForm()

    return render(request, "sales_crm/task_form.html", {"form": form, "visit": visit})



def task_update(request, pk):
    if not request.user.has_perm("sales_crm.change_task"):
        messages.error(request, "You do not have permission to edit tasks.")
        return redirect("sales_crm:task_list")
    task = get_object_or_404(Task, pk=pk)

    if request.method == "POST":
        form = TaskForm(request.POST, instance=task)
        if form.is_valid():
            form.save()
            return redirect("sales_crm:task_list")
    else:
        form = TaskForm(instance=task)

    return render(request, "sales_crm/task_form.html", {
        "form": form,
        "visit": task.visit
    })


def task_delete(request, pk):
    if not request.user.has_perm("sales_crm.delete_task"):
        messages.error(request, "You do not have permission to delete tasks.")
        return redirect("sales_crm:task_list")
    task = get_object_or_404(Task, pk=pk)
    task.delete()
    return redirect("sales_crm:task_list")



def followup_update(request, pk):
    if not request.user.has_perm("sales_crm.change_followup"):
        messages.error(request, "You do not have permission to edit follow-up.")
        return redirect("sales_crm:followup_list")
    followup = get_object_or_404(FollowUp, pk=pk)

    if request.method == "POST":
        form = FollowUpForm(request.POST, instance=followup)
        if form.is_valid():
            form.save()
            return redirect("sales_crm:followup_list")
    else:
        form = FollowUpForm(instance=followup)

    return render(request, "sales_crm/followup_form.html", {
        "form": form,
        "visit": followup.visit
    })



def followup_delete(request, pk):

    if not request.user.has_perm("sales_crm.delete_followup"):
        logger.warning(
            "User '%s' tried to delete follow-up without permission.",
            request.user.username
        )
        messages.error(request, "You do not have permission to delete follow-up.")
        return redirect("sales_crm:followup_list")

    followup = get_object_or_404(FollowUp, pk=pk)

    logger.info(
        "User '%s' deleted follow-up ID %s.",
        request.user.username,
        pk
    )

    followup.delete()

    return redirect("sales_crm:followup_list")

