# accounts_budget/admin.py
from __future__ import annotations

from django.contrib import admin
from django.db import models
from django.utils.timezone import localtime

from .models import *
from django.contrib import admin
from django.contrib.auth import get_user_model
from django.db.models import Q, Sum

# If your project uses services/maker_checker.py (as in your views), we can reuse mc_get safely.
try:
    from .services.maker_checker import mc_get
except Exception:
    mc_get = None


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _has_checked_status() -> bool:
    try:
        return "CHECKED" in {c[0] for c in MCStatus.choices}
    except Exception:
        return False


def _mc_locked(mc_obj) -> bool:
    """
    Your UI logic: lock after SUBMITTED/CHECKED/APPROVED.
    In admin we treat locked as immutable (non-superuser).
    """
    if not mc_obj:
        return False
    status = (getattr(mc_obj, "status", "") or "").upper()
    locked = {MCStatus.SUBMITTED, MCStatus.APPROVED}
    if _has_checked_status():
        locked.add("CHECKED")
    return status in {str(x).upper() for x in locked}


def _model_has_field(Model: type[models.Model], name: str) -> bool:
    try:
        Model._meta.get_field(name)
        return True
    except Exception:
        return False


def _pick_existing_fields(Model: type[models.Model], names: list[str]) -> list[str]:
    return [n for n in names if _model_has_field(Model, n)]


def _fmt_dt(dt):
    if not dt:
        return "-"
    try:
        return localtime(dt).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return str(dt)


def _user_label(u):
    if not u:
        return "-"
    try:
        full = getattr(u, "get_full_name", lambda: "")() or ""
        return full.strip() or getattr(u, "username", None) or str(u)
    except Exception:
        return str(u)


def _find_fk_name(child_model: type[models.Model], parent_model: type[models.Model], preferred: list[str] | None = None) -> str | None:
    """
    Find FK field name on child_model that points to parent_model.
    - If preferred list provided, checks those first.
    """
    preferred = preferred or []
    for name in preferred:
        try:
            f = child_model._meta.get_field(name)
            if isinstance(f, models.ForeignKey) and getattr(f.remote_field, "model", None) is parent_model:
                return name
        except Exception:
            pass

    for f in child_model._meta.get_fields():
        if isinstance(f, models.ForeignKey) and getattr(f.remote_field, "model", None) is parent_model:
            return f.name
    return None


def _safe_ordering(Model: type[models.Model], candidates: list[str], fallback: str = "id") -> tuple[str, ...]:
    for c in candidates:
        if _model_has_field(Model, c):
            return (c, fallback)
    return (fallback,)


# -----------------------------------------------------------------------------
# Base Mixins
# -----------------------------------------------------------------------------
class BudgetReadOnlyAdminMixin:
    """
    Enforces: all non-superusers can ONLY VIEW budget transactional data in admin.
    Prevents bypassing Maker-Checker (editing in admin).
    """

    def has_add_permission(self, request):
        return bool(request.user and request.user.is_superuser)

    def has_change_permission(self, request, obj=None):
        return bool(request.user and request.user.is_superuser)

    def has_delete_permission(self, request, obj=None):
        return bool(request.user and request.user.is_superuser)

    def has_view_permission(self, request, obj=None):
        if not request.user or not request.user.is_active or not request.user.is_staff:
            return False
        if request.user.is_superuser:
            return True
        return super().has_view_permission(request, obj=obj)


class MCStateReadOnlyAdmin(admin.ModelAdmin):
    """
    MakerCheckerState should NOT be manually edited except superuser.
    Checkers/Approvers can view it.
    """

    # IMPORTANT: use admin methods for submitted_by/checked_by/approved_by
    # so admin does NOT crash if fields are absent/renamed in the model.
    list_display = (
        "id",
        "content_type",
        "object_id",
        "scope",
        "status",
        "submitted_by",
        "submitted_at_fmt",
        "checked_by",
        "checked_at_fmt",
        "approved_by",
        "approved_at_fmt",
        "updated_at_fmt",
    )
    list_filter = ("status", "scope", "content_type")
    search_fields = ("scope", "object_id", "remarks")

    _mc_order_field = "updated_at" if _model_has_field(MakerCheckerState, "updated_at") else ("modified_at" if _model_has_field(MakerCheckerState, "modified_at") else None)
    ordering = (f"-{_mc_order_field}", "-id") if _mc_order_field else ("-id",)

    readonly_fields = [f.name for f in MakerCheckerState._meta.fields]

    # ---- SAFE “who” columns ----
    def submitted_by(self, obj):
        for k in ("submitted_by", "maker", "created_by", "requested_by"):
            if hasattr(obj, k):
                return _user_label(getattr(obj, k))
        return "-"

    def checked_by(self, obj):
        for k in ("checked_by", "checker", "reviewed_by"):
            if hasattr(obj, k):
                return _user_label(getattr(obj, k))
        return "-"

    def approved_by(self, obj):
        for k in ("approved_by", "approver", "approved_user", "approvedby"):
            if hasattr(obj, k):
                return _user_label(getattr(obj, k))
        return "-"

    submitted_by.short_description = "Submitted By"
    checked_by.short_description = "Checked By"
    approved_by.short_description = "Approved By"

    # ---- SAFE “when” columns ----
    def submitted_at_fmt(self, obj): return _fmt_dt(getattr(obj, "submitted_at", None))
    def checked_at_fmt(self, obj): return _fmt_dt(getattr(obj, "checked_at", None))
    def approved_at_fmt(self, obj): return _fmt_dt(getattr(obj, "approved_at", None))
    def updated_at_fmt(self, obj):
        # try updated_at then modified_at then "-"
        return _fmt_dt(getattr(obj, "updated_at", None) or getattr(obj, "modified_at", None))

    submitted_at_fmt.short_description = "Submitted At"
    checked_at_fmt.short_description = "Checked At"
    approved_at_fmt.short_description = "Approved At"
    updated_at_fmt.short_description = "Updated At"

    def has_add_permission(self, request):
        return bool(request.user and request.user.is_superuser)

    def has_change_permission(self, request, obj=None):
        return bool(request.user and request.user.is_superuser)

    def has_delete_permission(self, request, obj=None):
        return bool(request.user and request.user.is_superuser)

    def has_view_permission(self, request, obj=None):
        if not request.user or not request.user.is_active or not request.user.is_staff:
            return False
        if request.user.is_superuser:
            return True

        app_label = MakerCheckerState._meta.app_label
        can_check = request.user.has_perm(f"{app_label}.can_check_budgets")
        can_approve = request.user.has_perm(f"{app_label}.can_approve_budgets")
        return can_check or can_approve or super().has_view_permission(request, obj=obj)


# -----------------------------------------------------------------------------
# Inlines (read-only style for budgets)
# -----------------------------------------------------------------------------
class ReadOnlyInline(admin.TabularInline):
    extra = 0
    can_delete = False
    show_change_link = True

    def has_add_permission(self, request, obj=None): return False
    def has_change_permission(self, request, obj=None): return False
    def has_delete_permission(self, request, obj=None): return False


class BudgetLineInline(ReadOnlyInline):
    model = BudgetLine
    fields = _pick_existing_fields(BudgetLine, [
        "sr_no", "particulars", "remarks",
        "apr","may","jun","jul","aug","sep","oct","nov","dec","jan","feb","mar",
        "total",
        "category",
        "gl_account",
    ])
    readonly_fields = fields
    ordering = ("sr_no", "id")


class BudgetAttachmentInline(ReadOnlyInline):
    model = BudgetLineAttachment
    fields = _pick_existing_fields(BudgetLineAttachment, [
        "line", "file", "uploaded_by", "uploaded_at"
    ])
    readonly_fields = fields
    ordering = ("-id",)


class BOMInputInline(ReadOnlyInline):
    model = ProductionBOMInputLine
    fields = _pick_existing_fields(ProductionBOMInputLine, [
        "material_code", "bom_item_code",
        "material_category", "type",
        "material_name", "bom_item_name",
        "unit",
        "budget_norm", "target_norm",
    ])
    readonly_fields = fields


class BOMEffluentInline(ReadOnlyInline):
    model = ProductionBOMEffluentLine
    fields = _pick_existing_fields(ProductionBOMEffluentLine, [
        "effluent_type", "effluent_name", "unit",
        "budget_norm", "target_norm",
    ])
    readonly_fields = fields


class SalesLineInline(ReadOnlyInline):
    model = SalesBudgetLine
    fields = _pick_existing_fields(SalesBudgetLine, [
        "sale_type", "product_name",
        "apr","may","jun","jul","aug","sep","oct","nov","dec","jan","feb","mar",
        "annual_qty_mt",
        "rate_inr", "rate_usd",
        "amt_inr",
    ])
    readonly_fields = fields


class RMCLineInline(ReadOnlyInline):
    model = RMCBudgetLine
    fields = _pick_existing_fields(RMCBudgetLine, [
        "rm_code", "rm_name", "unit", "purchase_type",
        "apr","may","jun","jul","aug","sep","oct","nov","dec","jan","feb","mar",
        "annual_qty_mt", "annual_qty", "qty",
        "required_qty", "required_qty_mt", "req_qty", "req_qty_mt",
        "local_rate_inr", "import_rate_usd", "duty_percent",
        "freight_inr", "clearance_inr",
    ])
    readonly_fields = fields
    ordering = ("rm_name", "rm_code", "id")


class CaptiveLineInline(ReadOnlyInline):
    model = CaptiveConsumptionLine
    fields = _pick_existing_fields(CaptiveConsumptionLine, [
        "item_name", "qty", "rate", "amount", "remarks"
    ])
    readonly_fields = fields
    ordering = ("item_name", "id")


# ✅ Correct FK detection: ProductionBudgetLine must FK to ProductionBudgetFG for inline.
_PROD_LINE_FK = _find_fk_name(
    ProductionBudgetLine,
    ProductionBudgetFG,
    preferred=["fg_budget", "fg", "fg_plan", "fg_header", "production_fg", "budget_fg"],
)

class ProductionLineInline(ReadOnlyInline):
    model = ProductionBudgetLine
    if _PROD_LINE_FK:
        fk_name = _PROD_LINE_FK

    fields = _pick_existing_fields(ProductionBudgetLine, [
        "bom_item_code", "material_code", "item_code",
        "material_name", "item_name",
        "unit",
        "target_norm", "budget_norm",
        "remarks",
    ])
    readonly_fields = fields

    # ✅ Safe ordering (won't crash if bom_item_code doesn't exist)
    ordering = _safe_ordering(ProductionBudgetLine, [
        "bom_item_code", "material_code", "item_code", "material_name", "item_name"
    ])


# -----------------------------------------------------------------------------
# Admin registrations
# -----------------------------------------------------------------------------

# --- Masters (editable using normal Django model permissions) -----------------
@admin.register(DepartmentBudgetHead)
class DepartmentBudgetHeadAdmin(admin.ModelAdmin):
    """
    Fix: 'gl_name' is not a model field in your DB.
    We expose gl_name as an admin method safely.
    """
    list_display = ("id", "unit", "department", "budget_head", "gl_name", "is_active")
    list_filter = ("department", "unit", "is_active")
    ordering = ("department", "unit", "budget_head")

    def gl_name(self, obj):
        # If you have a FK like gl_account -> show its name
        if hasattr(obj, "gl_account") and getattr(obj, "gl_account", None):
            ga = getattr(obj, "gl_account")
            return getattr(ga, "name", None) or str(ga)

        # If the model has a direct field named gl_name (future-proof)
        if hasattr(obj, "gl_name"):
            return getattr(obj, "gl_name") or "-"

        # Try common alternatives
        for k in ("gl", "gl_no", "gl_code", "account_name"):
            if hasattr(obj, k):
                v = getattr(obj, k)
                return v or "-"
        return "-"

    gl_name.short_description = "GL Name"

    # search_fields must be real fields/lookups; do not include admin method name.
    _sf = ["department", "unit", "budget_head"]
    if _model_has_field(DepartmentBudgetHead, "gl_account"):
        _sf += ["gl_account__no", "gl_account__name"]
    elif _model_has_field(DepartmentBudgetHead, "gl_name"):
        _sf += ["gl_name"]
    search_fields = tuple(_sf)


@admin.register(GLAccount)
class GLAccountAdmin(admin.ModelAdmin):
    list_display = ("id", "no", "name", "unit", "department", "budget_head", "blocked", "is_active")
    list_filter = ("department", "unit", "blocked", "is_active")
    search_fields = ("no", "name", "budget_head", "department", "unit")
    ordering = ("department", "unit", "budget_head", "no")


# --- Maker-Checker (viewable only to checker/approver; editable only superuser)
@admin.register(MakerCheckerState)
class MakerCheckerStateAdmin(MCStateReadOnlyAdmin):
    pass


# --- Budget transactional models (VIEW-ONLY in admin) ------------------------
@admin.register(BudgetPlan)
class BudgetPlanAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "fy", "company_group", "is_active", "created_by", "created_at", "updated_at")
    list_filter = ("fy", "company_group", "is_active")
    search_fields = ("fy", "company_group")
    ordering = ("-id",)
    inlines = [BudgetLineInline]
    date_hierarchy = "created_at"


@admin.register(BudgetLine)
class BudgetLineAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "plan", "category", "sr_no", "particulars", "total")
    list_filter = ("category", "plan__fy", "plan__company_group")
    search_fields = ("particulars", "remarks", "plan__fy", "plan__company_group")
    ordering = ("plan_id", "category", "sr_no", "id")
    inlines = [BudgetAttachmentInline]


@admin.register(BudgetLineAttachment)
class BudgetLineAttachmentAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "line", "uploaded_by", "uploaded_at")
    search_fields = ("line__particulars", "uploaded_by__username")
    ordering = ("-id",)


@admin.register(ProductionBOM)
class ProductionBOMAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "bom_code", "fg_name", "fg_alpha_name", "is_active", "created_by", "updated_at")
    list_filter = ("is_active",)
    search_fields = ("fg_name", "fg_alpha_name", "bom_code")
    ordering = ("fg_name",)
    inlines = [BOMInputInline, BOMEffluentInline]


@admin.register(ProductionBudgetFG)
class ProductionBudgetFGAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "plan", "fg_name", "created_by", "updated_at")
    list_filter = ("plan__fy", "plan__company_group")
    search_fields = ("fg_name", "plan__fy", "plan__company_group")
    ordering = ("plan_id", "fg_name")
    # ✅ only add inline if correct FK exists
    inlines = ([ProductionLineInline] if _PROD_LINE_FK else [])


@admin.register(ProductionBudget)
class ProductionBudgetAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "plan", "created_by", "updated_at")
    list_filter = ("plan__fy", "plan__company_group")
    search_fields = ("plan__fy", "plan__company_group")
    ordering = ("-id",)


@admin.register(ProductionNorm)
class ProductionNormAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "fg_name", "bom_item_code", "bom_item_name", "unit", "norm")
    list_filter = ("fg_name", "unit")
    search_fields = ("fg_name", "bom_item_code", "bom_item_name")
    ordering = ("fg_name", "bom_item_code")


@admin.register(SalesBudget)
class SalesBudgetAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "plan", "inr_usd", "created_by", "updated_at")
    list_filter = ("plan__fy", "plan__company_group")
    search_fields = ("plan__fy", "plan__company_group")
    ordering = ("-id",)
    inlines = [SalesLineInline]


@admin.register(SalesBudgetLine)
class SalesBudgetLineAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "budget", "sale_type", "product_name", "annual_qty_mt", "amt_inr")
    list_filter = ("sale_type", "budget__plan__fy", "budget__plan__company_group")
    search_fields = ("product_name", "budget__plan__fy", "budget__plan__company_group")
    ordering = ("budget_id", "sale_type", "product_name", "id")


@admin.register(RMCBudget)
class RMCBudgetAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "plan", "usd_inr", "created_by", "updated_at")
    list_filter = ("plan__fy", "plan__company_group")
    search_fields = ("plan__fy", "plan__company_group")
    ordering = ("-id",)
    inlines = [RMCLineInline]


@admin.register(RMCBudgetLine)
class RMCBudgetLineAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "budget", "rm_code", "rm_name", "purchase_type")
    list_filter = ("purchase_type", "budget__plan__fy", "budget__plan__company_group")
    search_fields = ("rm_code", "rm_name")
    ordering = ("rm_name", "rm_code", "id")


@admin.register(CaptiveConsumptionBudget)
class CaptiveConsumptionBudgetAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "plan", "created_by", "updated_at")
    list_filter = ("plan__fy", "plan__company_group")
    search_fields = ("plan__fy", "plan__company_group")
    ordering = ("-id",)
    inlines = [CaptiveLineInline]


@admin.register(CaptiveConsumptionLine)
class CaptiveConsumptionLineAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    list_display = ("id", "budget", "item_name", "qty", "rate")
    list_filter = ("budget__plan__fy", "budget__plan__company_group")
    search_fields = ("item_name",)
    ordering = ("item_name", "id")


# Optional admin branding
admin.site.site_header = "OneSource Budget Admin"
admin.site.site_title = "Budget Admin"
admin.site.index_title = "Budget Administration"



@admin.register(UserBudgetCategoryAccess)
class UserBudgetCategoryAccessAdmin(admin.ModelAdmin):
    list_display = ("user", "category", "can_view", "can_edit")
    list_filter = ("category", "can_view", "can_edit")
    search_fields = ("user__username", "user__first_name", "user__last_name", "user__email")
    ordering = ("user__username", "category")
    autocomplete_fields = ("user",)
    list_per_page = 50

    # nicer dropdown label + keeps category sorted by label
    def formfield_for_choice_field(self, db_field, request, **kwargs):
        if db_field.name == "category":
            choices = list(BudgetCategory.choices)
            # sort by label (2nd item)
            choices.sort(key=lambda x: (x[1] or "").lower())
            kwargs["choices"] = choices
        return super().formfield_for_choice_field(db_field, request, **kwargs)

# -----------------------------------------------------------------------------
# PACKING: Inlines (read-only) + Admin (master editable, budgets view-only)
# -----------------------------------------------------------------------------

class PackingLineInline(ReadOnlyInline):
    model = PackingBudgetLine

    fields = _pick_existing_fields(PackingBudgetLine, [
        "product_name",
        "packing_material",
        "packing_code", "packing_name", "unit",
        "packing_size", "rate", "wastage_percent",
        "req_apr","req_may","req_jun","req_jul","req_aug","req_sep","req_oct","req_nov","req_dec","req_jan","req_feb","req_mar",
        "req_total",
        "val_apr","val_may","val_jun","val_jul","val_aug","val_sep","val_oct","val_nov","val_dec","val_jan","val_feb","val_mar",
        "val_total",
        "remarks",
        "updated_at",
    ])
    readonly_fields = fields

    ordering = _safe_ordering(PackingBudgetLine, ["product_name", "packing_name", "packing_code"], fallback="id")


@admin.register(PackingMaterialMaster)
class PackingMaterialMasterAdmin(admin.ModelAdmin):
    """
    Packing master SHOULD be editable in admin (size + rate maintained here).
    Uses standard Django model permissions.
    """
    list_display = ("id", "item_code", "item_name", "unit", "packing_size", "rate", "is_active", "updated_by", "updated_at")
    list_filter = ("is_active", "unit")
    search_fields = ("item_code", "item_name")
    ordering = ("item_name", "item_code")
    list_per_page = 50

    readonly_fields = ("updated_at",)

    fieldsets = (
        ("Packing Material", {"fields": ("item_code", "item_name", "unit", "is_active")}),
        ("Commercial", {"fields": ("packing_size", "rate")}),
        ("Audit", {"fields": ("updated_by", "updated_at")}),
    )

    def save_model(self, request, obj, form, change):
        # keep updated_by updated
        if hasattr(obj, "updated_by"):
            obj.updated_by = request.user
        super().save_model(request, obj, form, change)


@admin.register(PackingBudget)
class PackingBudgetAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    """
    PackingBudget is transactional => VIEW-ONLY for non-superuser (Maker-Checker safety).
    """
    list_display = ("id", "plan", "wastage_percent_default", "selected_products_count", "total_req", "total_val", "created_by", "updated_at")
    list_filter = ("plan__fy", "plan__company_group")
    search_fields = ("plan__fy", "plan__company_group")
    ordering = ("-updated_at", "-id")
    inlines = [PackingLineInline]
    list_per_page = 25

    def selected_products_count(self, obj):
        try:
            return len(obj.selected_products or [])
        except Exception:
            return 0
    selected_products_count.short_description = "Selected Products"

    def total_req(self, obj):
        try:
            return obj.lines.aggregate(s=Sum("req_total")).get("s") or Decimal("0.000000")
        except Exception:
            return "-"
    total_req.short_description = "Req Total"

    def total_val(self, obj):
        try:
            return obj.lines.aggregate(s=Sum("val_total")).get("s") or Decimal("0.00")
        except Exception:
            return "-"
    total_val.short_description = "Value Total (₹)"


@admin.register(PackingBudgetLine)
class PackingBudgetLineAdmin(BudgetReadOnlyAdminMixin, admin.ModelAdmin):
    """
    PackingBudgetLine is transactional => VIEW-ONLY for non-superuser.
    """
    list_display = ("id", "budget", "product_name", "packing_code", "packing_name", "unit", "packing_size", "rate", "req_total", "val_total", "updated_at")
    list_filter = ("unit", "budget__plan__fy", "budget__plan__company_group")
    search_fields = ("product_name", "packing_code", "packing_name", "budget__plan__fy")
    ordering = ("-updated_at", "-id")
    list_per_page = 50