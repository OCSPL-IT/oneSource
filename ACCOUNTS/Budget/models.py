from decimal import Decimal
from django.db import models
from django.contrib.auth import get_user_model
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes.fields import GenericForeignKey
from django.conf import settings
import re
from django.core.exceptions import ValidationError
from django.db import models, transaction, IntegrityError
from django.db.models.expressions import RawSQL
from django.db.models import Max
from django.db import models, transaction


User = get_user_model()

# =============================================================================
# Approval Models
# ============================================================================

class MCStatus(models.TextChoices):
    DRAFT = "DRAFT", "Draft"
    SUBMITTED = "SUBMITTED", "Submitted"
    CHECKED = "CHECKED", "Checked"   # ✅ add
    APPROVED = "APPROVED", "Approved"
    REJECTED = "REJECTED", "Rejected"


class MakerCheckerState(models.Model):
    """
    Generic state for any model instance.
    `scope` lets you have multiple approvals per same object (e.g., BudgetPlan category pages).
    """
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveBigIntegerField()
    content_object = GenericForeignKey("content_type", "object_id")

    scope = models.CharField(max_length=80, blank=True, default="")  # e.g. "RMC", "SALES", "PROD", "BUDGET:safety"

    status = models.CharField(max_length=12, choices=MCStatus.choices, default=MCStatus.DRAFT)

    submitted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="mc_submitted_by")
    submitted_at = models.DateTimeField(null=True, blank=True)

    checked_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="mc_checked_by")
    checked_at = models.DateTimeField(null=True, blank=True)

    checker_remarks = models.TextField(blank=True, default="")
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["content_type", "object_id", "scope"], name="uq_mc_state_object_scope"),
        ]
        permissions = [
            ("can_check_budgets", "Can check budgets"), 
            ("can_approve_budgets", "Can approve budgets"),
        ]
        indexes = [
            models.Index(fields=["scope", "status"]),
        ]

    def is_locked(self) -> bool:
        # locked for maker
        return self.status in (MCStatus.SUBMITTED, MCStatus.APPROVED)

    def __str__(self):
        return f"{self.content_type_id}:{self.object_id} [{self.scope}] {self.status}"

# =============================================================================
# BUDGET PLAN AND CATEGORY MODELS
# =============================================================================

class BudgetPlan(models.Model):
    """
    One plan per FY (and optionally per company_group).
    FY is stored as '2025-26' format.
    """
    fy = models.CharField(max_length=7)  # e.g. '2025-26'
    company_group = models.CharField(max_length=50, blank=True, default="")  # optional
    is_active = models.BooleanField(default=True)

    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="budget_plans")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("fy", "company_group")
        ordering = ("-fy", "-updated_at")

    def __str__(self) -> str:
        cg = f" ({self.company_group})" if self.company_group else ""
        return f"Budget FY {self.fy}{cg}"

class BudgetCategory(models.TextChoices):
    # ✅ Active Departments (NEW LIST)
    PRODUCTION   = "production", "Production"
    SAFETY       = "safety",       "Safety & Health"
    ENVIRONMENT  = "environment",  "Environment"
    QAQC         = "qaqc",         "QA& QC"              # label updated to match your Ledger Master
    ENGINEERING  = "engineering",  "Engineering"
    UTILITY      = "utility",      "Utility"
    ADMIN        = "admin",        "Admin"               # label updated (was Admin Cost)
    HR           = "hr",           "HR"
    RD           = "rd",           "R&D"                 # label updated (was R & D)
    LOGISTIC     = "logistic",     "Logistic"
    FIN_ACCTS    = "fin_accounts", "Finance & Accounts"
    SALES        = "sales", "Sales"
    # ✅ Legacy categories (keep to avoid breaking existing rows/links)
    STEAM        = "steam",        "Steam (Legacy)"
    ELECTRICITY  = "electricity",  "Electricity (Legacy)"
    RM           = "rm",           "R & M (Legacy)"
    EMP_OC       = "emp_oc",       "Employee Cost (OC) (Legacy)"
    EMP_CONTRACT = "emp_contract", "Employee Cost (Contract Employees) (Legacy)"

    @classmethod
    def active(cls):
        return [
            cls.PRODUCTION,
            cls.SALES,
            cls.SAFETY,
            cls.ENVIRONMENT,
            cls.QAQC,
            cls.ENGINEERING,
            cls.UTILITY,
            cls.ADMIN,
            cls.HR,
            cls.RD,
            cls.LOGISTIC,
            cls.FIN_ACCTS,
        ]


class UserBudgetCategoryAccess(models.Model):
    """
    Controls which department budget pages a user can see/edit.
    """
    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="budget_category_access",
    )
    category = models.CharField(max_length=30, choices=BudgetCategory.choices, db_index=True)
    can_view = models.BooleanField(default=True)
    can_edit = models.BooleanField(default=True)

    class Meta:
        unique_together = ("user", "category")
        indexes = [
            models.Index(fields=["user", "category"]),
        ]

    def __str__(self):
        return f"{self.user} -> {self.category} (view={self.can_view}, edit={self.can_edit})"
    
    
    
class BudgetLine(models.Model):
    """
    Monthly budget line item (Apr..Mar) with a computed total.
    Kept generic so each department has its own page but data model remains consistent.
    """
    plan = models.ForeignKey(BudgetPlan, on_delete=models.CASCADE, related_name="lines")
    category = models.CharField(max_length=30, choices=BudgetCategory.choices)

    sr_no = models.PositiveIntegerField(default=0)
    particulars = models.CharField(max_length=255)

    # -----------------------------
    # ✅ NEW: COA linkage (optional)
    # -----------------------------
    account_category = models.ForeignKey(
        "AccountCategory",
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="budget_lines",
    )
    gl_account = models.ForeignKey(
        "GLAccount",
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="budget_lines",
    )
    account_group = models.ForeignKey(
        "AccountGroup",
        null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="budget_lines",
    )

    prev_budget = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)
    prev_actual = models.DecimalField(max_digits=14, decimal_places=2, null=True, blank=True)

    # FY months (Apr..Mar)
    apr = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    may = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    jun = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    jul = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    aug = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    sep = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    oct = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    nov = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    dec = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    jan = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    feb = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))
    mar = models.DecimalField(max_digits=14, decimal_places=2, default=Decimal("0.00"))

    total = models.DecimalField(max_digits=16, decimal_places=2, default=Decimal("0.00"))
    remarks = models.CharField(max_length=255, blank=True, default="")

    # ✅ NEW: exactly one attachment per row
    attachment = models.FileField(
        upload_to="budgets/line_attachments/",
        null=True,
        blank=True
    )

    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["plan", "category"]),
            # ✅ NEW: helps filtering on pages/reports
            models.Index(fields=["plan", "category", "account_group"]),
            models.Index(fields=["gl_account"]),
        ]
        ordering = ("sr_no", "id")

    def compute_total(self) -> Decimal:
        vals = [
            self.apr, self.may, self.jun, self.jul, self.aug, self.sep,
            self.oct, self.nov, self.dec, self.jan, self.feb, self.mar,
        ]
        s = Decimal("0.00")
        for v in vals:
            s += (v or Decimal("0.00"))
        return s
    
    @property
    def attachment_count(self) -> int:
        # safe even if no attachments exist
        return getattr(self, "attachments", None).count() if self.pk else 0


    def save(self, *args, **kwargs):
        # ✅ keep existing behavior
        self.total = self.compute_total()

        # ✅ NEW: auto-assign group from selected ledger (no flow change)
        if self.gl_account_id:
            ga = self.gl_account

            # ✅ assign group only if we actually resolved an AccountGroup instance
            grp = getattr(ga, "group", None)
            if grp:
                self.account_group = grp
            else:
                # optional: clear if no mapping; prevents stale group
                self.account_group = None

            # ✅ assign category only if GLAccount has account_category field (it currently does NOT)
            ga_cat = getattr(ga, "account_category", None)
            if not self.account_category_id and ga_cat:
                self.account_category = ga_cat

        super().save(*args, **kwargs)

def budget_line_attachment_path(instance, filename):
    # organized path: budgets/<fy>/<category>/line_<id>/<filename>
    fy = (instance.line.plan.fy or "NA").replace("/", "-")
    cat = (instance.line.category or "dept").lower()
    return f"budgets/{fy}/{cat}/line_{instance.line_id}/{filename}"


class BudgetLineAttachment(models.Model):
    line = models.ForeignKey(
        "BudgetLine",
        on_delete=models.CASCADE,
        related_name="attachments",
        db_index=True,
    )

    file = models.FileField(upload_to=budget_line_attachment_path)
    title = models.CharField(max_length=150, blank=True, default="")

    uploaded_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="budget_line_attachments",
    )
    uploaded_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ("-uploaded_at",)
        indexes = [
            models.Index(fields=["line", "uploaded_at"]),
        ]

    def __str__(self):
        return self.title or self.file.name

class DepartmentBudgetHead(models.Model):
    """
    Master rows uploaded from Excel:
      Unit | Department | Budget Head | GL
    """
    unit = models.CharField(max_length=50, blank=True, default="")
    department = models.CharField(max_length=120)
    budget_head = models.CharField(max_length=255)
    gl = models.CharField(max_length=255, blank=True, default="")

    is_active = models.BooleanField(default=True)

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True,
        on_delete=models.SET_NULL, related_name="dept_budget_heads_created"
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True,
        on_delete=models.SET_NULL, related_name="dept_budget_heads_updated"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["unit", "department", "budget_head"],
                name="uq_dept_budget_head_unit_dept_head",
            ),
        ]
        indexes = [
            models.Index(fields=["department", "unit"]),
            models.Index(fields=["budget_head"]),
            models.Index(fields=["gl"]),
        ]

    def __str__(self):
        u = f"{self.unit} | " if self.unit else ""
        return f"{u}{self.department} | {self.budget_head}"


# class GLAccount(models.Model):
#     """
#     Ledger master created ONLY from Excel-driven Unit/Department/Budget Head.
#     GL Code (no) is auto-generated if empty.
#     """

#     no = models.CharField(max_length=20, unique=True)      # GL Code (auto)
#     name = models.CharField(max_length=255)               # GL Name (Excel GL)

#     # Excel keys (to prevent mixing)
#     unit = models.CharField(max_length=50, blank=True, default="")
#     department = models.CharField(max_length=120, blank=True, default="")
#     budget_head = models.CharField(max_length=255, blank=True, default="")

#     blocked = models.BooleanField(default=False)
#     is_active = models.BooleanField(default=True)

#     last_date_modified = models.DateField(auto_now=True)

#     created_by = models.ForeignKey(
#         settings.AUTH_USER_MODEL, null=True, blank=True,
#         on_delete=models.SET_NULL, related_name="created_gl_accounts"
#     )

#     class Meta:
#         indexes = [
#             models.Index(fields=["department", "unit"]),
#             models.Index(fields=["department", "unit", "budget_head"]),
#             models.Index(fields=["budget_head"]),
#             models.Index(fields=["no"]),
#             models.Index(fields=["name"]),
#         ]

#     def __str__(self):
#         extra = []
#         if self.unit:
#             extra.append(self.unit)
#         if self.department:
#             extra.append(self.department)
#         if self.budget_head:
#             extra.append(self.budget_head)
#         suffix = f" ({' | '.join(extra)})" if extra else ""
#         return f"{self.no} - {self.name}{suffix}"

#     @staticmethod
#     def _next_auto_no() -> str:
#         """
#         SQL Server safe: TRY_CONVERT(BIGINT, no)
#         Then max + 1, padded to 5 digits (matches typical '18600' style).
#         """
#         agg = (
#             GLAccount.objects
#             .annotate(no_int=RawSQL("TRY_CONVERT(BIGINT, [no])", []))
#             .aggregate(m=Max("no_int"))
#         )
#         last = int(agg.get("m") or 0)
#         nxt = last + 1
#         return f"{nxt:05d}" if nxt < 100000 else str(nxt)

#     def save(self, *args, **kwargs):
#         self.unit = (self.unit or "").strip()
#         self.department = (self.department or "").strip()
#         self.budget_head = (self.budget_head or "").strip()
#         self.name = (self.name or "").strip()

#         # Auto-create code if missing
#         if not self.no:
#             # minimal concurrency safety
#             for _ in range(5):
#                 self.no = GLAccount._next_auto_no()
#                 try:
#                     with transaction.atomic():
#                         return super().save(*args, **kwargs)
#                 except IntegrityError:
#                     self.no = ""
#             raise IntegrityError("Could not generate unique GL code. Try again.")

#         super().save(*args, **kwargs)

# ----------------------------
# helpers
# ----------------------------
def _d0(v, places="0.000000") -> Decimal:
    """
    Coerce None/""/" " to Decimal(0) with fixed dp string.
    places must be like "0.000000".
    """
    if v in (None, ""):
        return Decimal(places)
    try:
        s = str(v).strip()
        if s == "":
            return Decimal(places)
        return Decimal(s)
    except Exception:
        return Decimal(places)


# =============================================================================
# PRODUCTION BOM MASTER (NEW)
# =============================================================================
class ProductionBOM(models.Model):
    fg_name = models.CharField(max_length=255, unique=True)
    fg_alpha_name = models.CharField(max_length=255, blank=True, default="")
    bom_code = models.CharField(max_length=30, unique=True, null=True, blank=True)

    is_active = models.BooleanField(default=True)

    created_by = models.ForeignKey(
        User, on_delete=models.SET_NULL,
        null=True, blank=True, related_name="prod_boms"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("fg_name",)

    def __str__(self) -> str:
        return self.fg_name

    def save(self, *args, **kwargs):
        # normalize
        self.fg_name = (self.fg_name or "").strip()
        self.fg_alpha_name = (self.fg_alpha_name or "").strip()

        creating = self.pk is None
        super().save(*args, **kwargs)

        # generate bom_code after PK exists
        if creating and not self.bom_code:
            self.bom_code = f"BOM{int(self.pk):06d}"
            super().save(update_fields=["bom_code"])


# =============================================================================
# ✅ NEW: BOM Input Material lines (tab 1)
# =============================================================================
class ProductionBOMInputLine(models.Model):
    bom = models.ForeignKey(
        ProductionBOM,
        on_delete=models.CASCADE,
        related_name="input_lines"
    )
    sr_no = models.PositiveIntegerField(default=0)

    bom_item_code = models.CharField(max_length=100, blank=True, default="")
    material_category = models.CharField(max_length=150, blank=True, default="")
    material_name = models.CharField(max_length=255)

    # ✅ NEW: Captive tick for this input line (saved in DB)
    is_captive = models.BooleanField(default=False, db_index=True)

    budget_norm = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        default=Decimal("0.000000")
    )
    target_norm = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        null=True,
        blank=True
    )
    unit = models.CharField(max_length=50, blank=True, default="")

    class Meta:
        indexes = [
            models.Index(fields=["bom"]),
            models.Index(fields=["bom", "bom_item_code"]),
            models.Index(fields=["bom", "is_captive"]),
        ]
        ordering = ("sr_no", "id")

    def __str__(self):
        return f"{self.bom.fg_name} -> {self.material_name}"

    # -----------------------------
    # helpers
    # -----------------------------
    @staticmethod
    def _norm_text(v: str) -> str:
        return (v or "").strip()

    @staticmethod
    def _norm_code(v: str) -> str:
        return (v or "").strip().upper()

    @staticmethod
    def _to_bool(v) -> bool:
        if isinstance(v, bool):
            return v
        if v is None:
            return False
        s = str(v).strip().lower()
        return s in ("1", "true", "on", "yes", "y")

    def clean(self):
        # normalize text
        self.bom_item_code = self._norm_code(self.bom_item_code)
        self.material_category = self._norm_text(self.material_category)
        self.material_name = self._norm_text(self.material_name)
        self.unit = self._norm_text(self.unit)

        # normalize captive
        self.is_captive = self._to_bool(self.is_captive)

        # decimals
        self.budget_norm = _d0(self.budget_norm, "0.000000")
        if self.target_norm in ("",):
            self.target_norm = None

        # IMPORTANT: don't raise for blank line here.
        # Blank skipping should be handled by formset/view.

    def save(self, *args, **kwargs):
        # keep normalization even if full_clean not called
        self.bom_item_code = self._norm_code(self.bom_item_code)
        self.material_category = self._norm_text(self.material_category)
        self.material_name = self._norm_text(self.material_name)
        self.unit = self._norm_text(self.unit)

        self.is_captive = self._to_bool(self.is_captive)

        self.budget_norm = _d0(self.budget_norm, "0.000000")
        if self.target_norm in ("",):
            self.target_norm = None

        return super().save(*args, **kwargs)

# =============================================================================
# ✅ NEW: Waste Generation (Effluent) lines (tab 2)
# =============================================================================
class ProductionBOMEffluentLine(models.Model):
    bom = models.ForeignKey(
        "ProductionBOM",
        on_delete=models.CASCADE,
        related_name="effluent_lines",
        db_index=True
    )
    sr_no = models.PositiveIntegerField(default=0)

    waste_type = models.CharField(max_length=120, blank=True, default="")
    waste_name = models.CharField(max_length=255, blank=True, default="")

    waste_budget_norm = models.DecimalField(
        max_digits=18, decimal_places=6,
        default=Decimal("0.000000")
    )
    waste_target_norm = models.DecimalField(
        max_digits=18, decimal_places=6,
        null=True, blank=True
    )

    class Meta:
        indexes = [models.Index(fields=["bom"])]
        ordering = ("sr_no", "id")

    def clean(self):
        self.waste_type = (self.waste_type or "").strip()
        self.waste_name = (self.waste_name or "").strip()

        self.waste_budget_norm = _d0(self.waste_budget_norm, "0.000000")
        if self.waste_target_norm in ("",):
            self.waste_target_norm = None

        # IMPORTANT: don't raise for blank rows here.

    def save(self, *args, **kwargs):
        self.waste_type = (self.waste_type or "").strip()
        self.waste_name = (self.waste_name or "").strip()

        self.waste_budget_norm = _d0(self.waste_budget_norm, "0.000000")
        if self.waste_target_norm in ("",):
            self.waste_target_norm = None

        return super().save(*args, **kwargs)


# =============================================================================
# LEGACY BOM Line (keep only if still used somewhere)
# =============================================================================
class ProductionBOMLine(models.Model):
    bom = models.ForeignKey(
        "ProductionBOM",
        on_delete=models.CASCADE,
        related_name="lines",
        db_index=True
    )
    sr_no = models.PositiveIntegerField(default=0)

    material_name = models.CharField(max_length=255)
    norm = models.DecimalField(max_digits=12, decimal_places=6, default=Decimal("0.000000"))
    mat_qty_mt = models.DecimalField(max_digits=14, decimal_places=3, null=True, blank=True)

    class Meta:
        indexes = [models.Index(fields=["bom"])]
        ordering = ("sr_no", "id")

    def __str__(self) -> str:
        return f"{self.bom.fg_name} -> {self.material_name}"

    def clean(self):
        self.material_name = (self.material_name or "").strip()
        self.norm = _d0(self.norm, "0.000000")
        if self.mat_qty_mt in ("",):
            self.mat_qty_mt = None

        # IMPORTANT: don't raise for blank rows here.

    def save(self, *args, **kwargs):
        self.material_name = (self.material_name or "").strip()
        self.norm = _d0(self.norm, "0.000000")
        if self.mat_qty_mt in ("",):
            self.mat_qty_mt = None
        return super().save(*args, **kwargs)

# =============================================================================
# PRODUCTION BUDGET (NEW)
# =============================================================================

class ProductionBudget(models.Model):
    """
    Production Budget per plan & FG BOM.
    User fills monthly FG quantities (MT). System computes RM qty using norms.
    """
    plan = models.ForeignKey(BudgetPlan, on_delete=models.CASCADE, related_name="production_budgets")
    bom = models.ForeignKey(ProductionBOM, on_delete=models.PROTECT, related_name="budgets")

    # FY months FG Qty (Apr..Mar)
    apr = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))
    may = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))
    jun = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))
    jul = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))
    aug = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))
    sep = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))
    oct = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))
    nov = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))
    dec = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))
    jan = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))
    feb = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))
    mar = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))

    total_fg_qty = models.DecimalField(max_digits=16, decimal_places=3, default=Decimal("0.000"))

    remarks = models.CharField(max_length=255, blank=True, default="")
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="production_budgets")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("plan", "bom")
        indexes = [
            models.Index(fields=["plan", "bom"]),
        ]
        ordering = ("-updated_at",)

    def __str__(self) -> str:
        return f"{self.plan.fy} - {self.bom.fg_name}"

    def compute_total_fg(self) -> Decimal:
        vals = [
            self.apr, self.may, self.jun, self.jul, self.aug, self.sep,
            self.oct, self.nov, self.dec, self.jan, self.feb, self.mar,
        ]
        s = Decimal("0.000")
        for v in vals:
            s += (v or Decimal("0.000"))
        return s

    def save(self, *args, **kwargs):
        self.total_fg_qty = self.compute_total_fg()
        super().save(*args, **kwargs)


class ProductionBudgetLine(models.Model):
    """
    Budget lines (materials) copied from BOM at create time.
    Norm can be adjusted in budget (if needed).
    RM quantities are computed at runtime: FG Qty × Norm.

    ✅ Target Norm:
    - Manual entry field for target/reference purpose only
    - No correlation with calculations unless you explicitly use it somewhere
    """
    budget = models.ForeignKey(ProductionBudget, on_delete=models.CASCADE, related_name="lines")
    sr_no = models.PositiveIntegerField(default=0)

    material_name = models.CharField(max_length=255)

    # Existing
    norm = models.DecimalField(max_digits=12, decimal_places=6, default=Decimal("0.000000"))

    # ✅ NEW: Target Norm (manual input only, independent)
    target_norm = models.DecimalField(
        max_digits=12,
        decimal_places=6,
        null=True,
        blank=True,
        help_text="Manual target norm (no correlation / calculation)."
    )

    remarks = models.CharField(max_length=255, blank=True, default="")

    class Meta:
        indexes = [
            models.Index(fields=["budget"]),
        ]
        ordering = ("sr_no", "id")

    def __str__(self) -> str:
        return f"{self.budget} -> {self.material_name}"

    def qty_for_month(self, month_field: str) -> Decimal:
        fg_qty = getattr(self.budget, month_field, None) or Decimal("0.000")
        return (fg_qty * (self.norm or Decimal("0.000000")))

class ERPBOMRow(models.Model):
    """
    BOM rows synced from ERP database into Main DB.
    Represents SQL result row-wise (CTE_BOMDetails).

    Used for:
    - FG selection (FGName)
    - Inputs listing (BOMItemCode/Name/Unit/BOMQty etc.)
    - Traceability (BomId, SeqId, BOMCode etc.)
    """

    # ---------------------------------------------------------------------
    # ERP identity / traceability
    # ---------------------------------------------------------------------
    sr_no = models.BigIntegerField(null=True, blank=True)  # ROW_NUMBER() AS [Sr.No] (display/debug)

    # IMPORTANT: keep these populated from query (recommended)
    bom_id = models.BigIntegerField(null=True, blank=True)  # det.lBomId
    seq_id = models.BigIntegerField(null=True, blank=True)  # det.lSeqId

    cflag = models.CharField(max_length=5, blank=True, default="")  # det.cFlag

    # ---------------------------------------------------------------------
    # FG / header side (BOM header)
    # ---------------------------------------------------------------------
    itm_type = models.CharField(max_length=100, blank=True, default="")   # TYP.sName
    item_name = models.CharField(max_length=255, blank=True, default="")  # MST.sName
    fg_name = models.CharField(max_length=255, blank=True, default="")    # ITMCF FG Name
    item_code = models.CharField(max_length=100, blank=True, default="")  # MST.sCode

    quantity = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)  # BOM.dQty
    rate = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)      # BOM.dRate

    bom_code = models.CharField(max_length=100, blank=True, default="")    # BOM.sCode
    bom_name = models.CharField(max_length=255, blank=True, default="")    # BOM.sName

    based_on = models.CharField(max_length=20, blank=True, default="")     # BOM.cTyp
    percentage = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)  # dPercentage
    bom_cnv = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)     # BOM.dCnv

    # ---------------------------------------------------------------------
    # Input side (BOM component)
    # ---------------------------------------------------------------------
    type = models.CharField(max_length=100, blank=True, default="")             # TYP1.sName
    bom_item_code = models.CharField(max_length=100, blank=True, default="")   # MST1.sCode
    bom_item_name = models.CharField(max_length=255, blank=True, default="")   # MST1.sName

    unit = models.CharField(max_length=100, blank=True, default="")            # UNTMST or lUntId
    bom_qty = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)  # det.dQty/dQtyPrc

    resource_type = models.CharField(max_length=50, blank=True, default="")     # DSG.sCode
    stock_parameter = models.CharField(max_length=255, blank=True, default="")  # CASE..BOM.svalueX

    # ---------------------------------------------------------------------
    # Sync meta
    # ---------------------------------------------------------------------
    snapshot_at = models.DateTimeField(auto_now_add=True)
    source_db = models.CharField(max_length=100, blank=True, default="eresOCSPL_Test")

    class Meta:
        indexes = [
            models.Index(fields=["fg_name"]),
            models.Index(fields=["type"]),
            models.Index(fields=["bom_code"]),
            models.Index(fields=["bom_item_code"]),
            models.Index(fields=["bom_id"]),
        ]
        constraints = [
            # ✅ FIX: include seq_id so multiple lines under same BOM item can exist
            models.UniqueConstraint(
                fields=["bom_id", "seq_id", "bom_item_code", "fg_name", "type"],
                name="uq_erp_bom_row",
            )
        ]

    def __str__(self) -> str:
        fg = self.fg_name or "-"
        item = self.bom_item_name or self.bom_item_code or "-"
        return f"{fg} -> {item}"

class ProductionNorm(models.Model):
    """
    Norm master maintained in Main DB:
    FG Name + BOM Item + Norm + Target Norm (manual reference).
    """

    fg_name = models.CharField(max_length=255, db_index=True)
    bom_item_code = models.CharField(max_length=100, blank=True, default="", db_index=True)

    # ✅ NOT NULL in SQL Server; keep default False
    is_captive = models.BooleanField(default=False, db_index=True)

    bom_item_name = models.CharField(max_length=255)
    unit = models.CharField(max_length=100, blank=True, default="")

    # Used for computations
    norm = models.DecimalField(max_digits=18, decimal_places=6, default=Decimal("0.000000"))

    # Manual reference only
    target_norm = models.DecimalField(
        max_digits=18,
        decimal_places=6,
        null=True,
        blank=True,
        help_text="Manual target norm for budgeting reference (does not affect calculations).",
    )

    updated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=["fg_name", "bom_item_code"]),
            models.Index(fields=["fg_name", "is_captive"]),  # ✅ common filter on screen
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["fg_name", "bom_item_code"],
                name="uq_prod_norm_fg_code",
            )
        ]

    def __str__(self) -> str:
        return (
            f"{self.fg_name} | {self.bom_item_code} | "
            f"captive={self.is_captive} | norm={self.norm} | target={self.target_norm or ''}"
        )

    # -----------------------------
    # Normalization helpers
    # -----------------------------
    @staticmethod
    def _norm_text(v: str) -> str:
        return (v or "").strip()

    @staticmethod
    def _norm_code(v: str) -> str:
        return (v or "").strip().upper()

    @staticmethod
    def _to_bool(v) -> bool:
        """
        Robust conversion for values coming from forms/POST/JSON:
        True for: True, 1, "1", "true", "on", "yes", "y"
        """
        if isinstance(v, bool):
            return v
        if v is None:
            return False
        s = str(v).strip().lower()
        return s in ("1", "true", "on", "yes", "y")

    def clean(self):
        # Normalize key fields to avoid update_or_create misses
        self.fg_name = self._norm_text(self.fg_name)
        self.bom_item_code = self._norm_code(self.bom_item_code)

        # Normalize descriptive fields
        self.bom_item_name = self._norm_text(self.bom_item_name)
        self.unit = self._norm_text(self.unit)

        # Coerce booleans/decimals safely
        self.is_captive = self._to_bool(self.is_captive)
        self.norm = _d0(self.norm, "0.000000")
        if self.target_norm in ("",):
            self.target_norm = None

    def save(self, *args, **kwargs):
        # Ensure normalization happens even if full_clean() isn't called
        self.fg_name = self._norm_text(self.fg_name)
        self.bom_item_code = self._norm_code(self.bom_item_code)
        self.bom_item_name = self._norm_text(self.bom_item_name)
        self.unit = self._norm_text(self.unit)

        self.is_captive = self._to_bool(self.is_captive)
        self.norm = _d0(self.norm, "0.000000")
        if self.target_norm in ("",):
            self.target_norm = None

        super().save(*args, **kwargs)


class ProductionBudgetFG(models.Model):
    """
    Production budget header for one FG under a plan.
    FG is selected from ERP BOM cache (fg_name list).
    """
    plan = models.ForeignKey("BudgetPlan", on_delete=models.CASCADE, related_name="production_fg_budgets")
    fg_name = models.CharField(max_length=255)

    # monthly FG plan (MT or as per your unit) - keep same Apr..Mar
    apr = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    may = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    jun = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    jul = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    aug = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    sep = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    oct = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    nov = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    dec = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    jan = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    feb = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    mar = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))

    total_fg_qty = models.DecimalField(max_digits=20, decimal_places=3, default=Decimal("0.000"))
    remarks = models.CharField(max_length=255, blank=True, default="")

    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="prod_fg_budgets")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("plan", "fg_name")
        indexes = [
            models.Index(fields=["plan", "fg_name"]),
        ]

    def compute_total_fg(self):
        vals = [self.apr, self.may, self.jun, self.jul, self.aug, self.sep,
                self.oct, self.nov, self.dec, self.jan, self.feb, self.mar]
        s = Decimal("0.000")
        for v in vals:
            s += (v or Decimal("0.000"))
        return s

    def save(self, *args, **kwargs):
        self.total_fg_qty = self.compute_total_fg()
        super().save(*args, **kwargs)

# =============================================================================

class ProductionBudgetFGLine(models.Model):
    budget = models.ForeignKey(
        "ProductionBudgetFG",
        on_delete=models.CASCADE,
        related_name="lines"
    )
    sr_no = models.PositiveIntegerField(default=0)

    bom_item_code = models.CharField(max_length=100, blank=True, default="")
    material_name = models.CharField(max_length=255)
    unit = models.CharField(max_length=100, blank=True, default="")

    norm = models.DecimalField(max_digits=18, decimal_places=6, default=Decimal("0.000000"))
    remarks = models.CharField(max_length=255, blank=True, default="")

    class Meta:
        indexes = [
            models.Index(fields=["budget"]),
            models.Index(fields=["bom_item_code"]),
        ]
        ordering = ("sr_no", "id")
        constraints = [
            models.UniqueConstraint(
                fields=["budget", "bom_item_code"],
                name="uq_prod_budget_fg_line_budget_item",
            )
        ]

    def __str__(self) -> str:
        return f"{self.budget.fg_name} -> {self.material_name}"
    
# -----------------------------------------------------------------------------
# ✅ NEW: Waste Generation (Effluent) lines under FG Budget (tab 2)
# -----------------------------------------------------------------------------
DEC6 = Decimal("0.000000")
MONTHS = ["apr","may","jun","jul","aug","sep","oct","nov","dec","jan","feb","mar"]

class ProductionBudgetFGEffluentLine(models.Model):
    budget = models.ForeignKey(
        "ProductionBudgetFG",
        on_delete=models.CASCADE,
        related_name="effluent_lines",
        db_index=True
    )
    sr_no = models.PositiveIntegerField(default=0)

    waste_type = models.CharField(max_length=120, blank=True, default="")
    waste_name = models.CharField(max_length=255, blank=True, default="")

    waste_norm = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)
    waste_target_norm = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)

    # ✅ monthwise breakup (SAVED)
    qty_apr = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)
    qty_may = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)
    qty_jun = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)
    qty_jul = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)
    qty_aug = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)
    qty_sep = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)
    qty_oct = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)
    qty_nov = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)
    qty_dec = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)
    qty_jan = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)
    qty_feb = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)
    qty_mar = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)

    # ✅ computed and saved (Qty per Year)
    waste_qty_year = models.DecimalField(max_digits=18, decimal_places=6, default=DEC6)

    class Meta:
        indexes = [
            models.Index(fields=["budget"]),
            models.Index(fields=["budget", "sr_no"]),
        ]
        ordering = ("sr_no", "id")
        constraints = [
            models.UniqueConstraint(
                fields=["budget", "sr_no", "waste_type", "waste_name"],
                name="uq_prod_budget_fg_effluent",
            )
        ]

    def _recalc(self):
        """
        Sets qty_<month> and waste_qty_year from budget FG months and waste_norm.
        """
        b = self.budget
        norm = _d0(self.waste_norm, "0.000000")

        total = DEC6
        for m in MONTHS:
            fg = DEC6
            if b and hasattr(b, m):
                fg = _d0(getattr(b, m, None), "0.000000")

            q = (fg * norm)
            setattr(self, f"qty_{m}", _d0(q, "0.000000"))
            total += q

        self.waste_qty_year = _d0(total, "0.000000")

    def save(self, *args, **kwargs):
        self.waste_type = (self.waste_type or "").strip()
        self.waste_name = (self.waste_name or "").strip()
        self.waste_norm = _d0(self.waste_norm, "0.000000")

        if self.waste_target_norm in ("",):
            self.waste_target_norm = None

        # ✅ Always keep breakup + year consistent
        self._recalc()

        super().save(*args, **kwargs)

# =============================================================================
# Despatch Plan Models
# ============================================================================= 
User = get_user_model()

class SaleType(models.TextChoices):
    DOMESTIC = "DOMESTIC", "DOMESTIC"
    EXPORT = "EXPORT", "EXPORT"


class SalesBudget(models.Model):
    """
    One Sales budget per plan.
    Stores INR/USD rate used for conversions (if needed).
    """
    plan = models.OneToOneField("BudgetPlan", on_delete=models.CASCADE, related_name="sales_budget")
    inr_usd = models.DecimalField(max_digits=10, decimal_places=2, default=Decimal("86.00"))

    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Sales Budget {self.plan.fy}"


class SalesBudgetLine(models.Model):
    """
    Monthwise quantity breakup per product.
    Rate is assumed INR per KG (based on your sample: Amt = MT * 1000 * Rate-INR).
    """
    budget = models.ForeignKey(SalesBudget, on_delete=models.CASCADE, related_name="lines")

    product_name = models.CharField(max_length=255)
    sale_type = models.CharField(max_length=20, choices=SaleType.choices, default=SaleType.DOMESTIC)

    annual_qty_mt = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))

    rate_usd = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)
    rate_inr = models.DecimalField(max_digits=18, decimal_places=6, null=True, blank=True)   # INR per KG

    amt_inr = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0.00"))
    amt_inr_cr = models.DecimalField(max_digits=20, decimal_places=2, default=Decimal("0.00"))

    # Monthwise qty (MT)
    apr = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    may = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    jun = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    jul = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    aug = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    sep = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    oct = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    nov = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    dec = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    jan = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    feb = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))
    mar = models.DecimalField(max_digits=18, decimal_places=3, default=Decimal("0.000"))

    total_month_qty = models.DecimalField(max_digits=20, decimal_places=3, default=Decimal("0.000"))

    class Meta:
        indexes = [
            models.Index(fields=["product_name"]),
            models.Index(fields=["sale_type"]),
        ]
        constraints = [
            models.UniqueConstraint(fields=["budget", "product_name", "sale_type"], name="uq_sales_budget_product")
        ]

    def _sum_months(self):
        months = [self.apr, self.may, self.jun, self.jul, self.aug, self.sep, self.oct, self.nov, self.dec, self.jan, self.feb, self.mar]
        s = Decimal("0.000")
        for v in months:
            s += (v or Decimal("0.000"))
        return s

    def save(self, *args, **kwargs):
        self.total_month_qty = self._sum_months()

        # If annual_qty_mt is not set but months are set, derive it
        if (self.annual_qty_mt or Decimal("0.000")) == Decimal("0.000") and self.total_month_qty > Decimal("0.000"):
            self.annual_qty_mt = self.total_month_qty

        # If rate_inr missing but rate_usd present, derive from budget.inr_usd
        if (self.rate_inr is None or self.rate_inr == 0) and self.rate_usd:
            self.rate_inr = (Decimal(self.rate_usd) * (self.budget.inr_usd or Decimal("86.00")))

        # Amount in INR = MT * 1000 * INR_per_KG
        rinr = self.rate_inr or Decimal("0.0")
        qty = self.annual_qty_mt or Decimal("0.000")
        self.amt_inr = (qty * Decimal("1000") * rinr).quantize(Decimal("0.01"))
        self.amt_inr_cr = (self.amt_inr / Decimal("10000000")).quantize(Decimal("0.01"))

        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.product_name} ({self.sale_type})"
    
# =============================================================================
# RMC Budget Models
# =============================================================================

class RMCPurchaseType(models.TextChoices):
    LOCAL = "LOCAL", "Local Purchase"
    IMPORT = "IMPORT", "Import Purchase"


class RMCBudget(models.Model):
    """
    One RMC Budget per BudgetPlan.
    Header keeps USD/INR conversion for import rate calculation.
    """
    plan = models.OneToOneField("BudgetPlan", on_delete=models.CASCADE, related_name="rmc_budget")
    usd_inr = models.DecimalField(max_digits=12, decimal_places=4, default=Decimal("86.0000"))

    # ✅ NEW: selected FG products for this RMC budget (persist selection)
    # Stores list of fg_name values (from ProductionBudgetFG for same plan)
    selected_fgs = models.JSONField(default=list, blank=True)

    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="rmc_budgets")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"RMC Budget {self.plan.fy}"

    # Optional helpers (do not change flow, just convenience)
    def get_selected_fgs(self) -> list[str]:
        v = self.selected_fgs or []
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        return []

    def set_selected_fgs(self, fgs: list[str]) -> None:
        self.selected_fgs = [str(x).strip() for x in (fgs or []) if str(x).strip()]

class RMCBudgetLine(models.Model):
    budget = models.ForeignKey(RMCBudget, on_delete=models.CASCADE, related_name="lines")

    rm_code = models.CharField(max_length=60)
    rm_name = models.CharField(max_length=255)

    # ✅ separate rows for captive vs non-captive
    is_captive = models.BooleanField(default=False)

    required_qty = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))
    unit = models.CharField(max_length=50, blank=True, default="")

    purchase_type = models.CharField(max_length=10, choices=RMCPurchaseType.choices, default=RMCPurchaseType.LOCAL)

    local_rate_inr = models.DecimalField(max_digits=18, decimal_places=4, null=True, blank=True)
    import_rate_usd = models.DecimalField(max_digits=18, decimal_places=4, null=True, blank=True)
    duty_percent = models.DecimalField(max_digits=10, decimal_places=4, default=Decimal("0.0000"))
    freight_inr = models.DecimalField(max_digits=18, decimal_places=4, default=Decimal("0.0000"))
    clearance_inr = models.DecimalField(max_digits=18, decimal_places=4, default=Decimal("0.0000"))

    # month qty fields
    qty_apr = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))
    qty_may = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))
    qty_jun = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))
    qty_jul = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))
    qty_aug = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))
    qty_sep = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))
    qty_oct = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))
    qty_nov = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))
    qty_dec = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))
    qty_jan = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))
    qty_feb = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))
    qty_mar = models.DecimalField(max_digits=20, decimal_places=6, default=Decimal("0.000000"))

    total_qty = models.DecimalField(max_digits=22, decimal_places=6, default=Decimal("0.000000"))
    budget_rate_inr = models.DecimalField(max_digits=18, decimal_places=4, default=Decimal("0.0000"))

    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["budget", "rm_code", "is_captive"],
                name="uq_rmc_budget_rm_code_captive"
            ),
        ]
        ordering = ("rm_name", "id")

    def compute_total_qty(self) -> Decimal:
        months = [
            self.qty_apr, self.qty_may, self.qty_jun, self.qty_jul, self.qty_aug, self.qty_sep,
            self.qty_oct, self.qty_nov, self.qty_dec, self.qty_jan, self.qty_feb, self.qty_mar,
        ]
        s = Decimal("0.000000")
        for v in months:
            s += (v or Decimal("0.000000"))
        return s

    def compute_budget_rate_inr(self) -> Decimal:
        ex = self.budget.usd_inr or Decimal("0")
        if self.purchase_type == RMCPurchaseType.IMPORT:
            base_inr = (self.import_rate_usd or Decimal("0")) * ex
            duty = base_inr * (self.duty_percent or Decimal("0")) / Decimal("100")
            return (base_inr + duty + (self.freight_inr or Decimal("0")) + (self.clearance_inr or Decimal("0")))
        return (self.local_rate_inr or Decimal("0"))

    def save(self, *args, **kwargs):
        # ✅ keep your current calculations
        self.budget_rate_inr = (self.compute_budget_rate_inr() or Decimal("0")).quantize(Decimal("0.0001"))
        self.total_qty = (self.compute_total_qty() or Decimal("0")).quantize(Decimal("0.000001"))

        # Preserve manual required_qty when month split is not used
        manual_required = (self.required_qty or Decimal("0")).quantize(Decimal("0.000001"))

        # ✅ existing sync flow
        if self.total_qty > Decimal("0.000000"):
            # months entered → required qty must match months
            self.required_qty = self.total_qty
        else:
            # months empty → allow manual required qty
            self.required_qty = manual_required

        # maintain update_fields logic
        update_fields = kwargs.get("update_fields")
        if update_fields:
            uf = set(update_fields)
            uf.update({"budget_rate_inr", "total_qty", "required_qty"})
            uf.add("updated_at")
            kwargs["update_fields"] = list(uf)

        super().save(*args, **kwargs)

# =============================================================================
# COA MODELS FOR BUDGET ALLOCATION
# =============================================================================

class AccountGroup(models.Model):
    """
    This is your 'group' used for budget tracking. Examples:
    - Direct Expenses
    - Admin Expenses
    - Safety & Health Expenses
    - R&D Expenses
    etc.
    """
    name = models.CharField(max_length=120, unique=True)
    code = models.CharField(max_length=30, unique=True)

    def __str__(self):
        return f"{self.code} - {self.name}"


class AccountCategory(models.Model):
    """
    Equivalent of Business Central 'Account Category'.
    """
    name = models.CharField(max_length=120, unique=True)
    code = models.CharField(max_length=30, unique=True)

    # Optional: If Category itself maps to a default group
    default_group = models.ForeignKey(
        AccountGroup, null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="default_for_categories"
    )

    def __str__(self):
        return f"{self.code} - {self.name}"


class AccountSubcategory(models.Model):
    """
    Equivalent of Business Central 'Account Subcategory'.
    Usually determines the group more precisely.
    """
    category = models.ForeignKey(AccountCategory, on_delete=models.CASCADE, related_name="subcategories")
    name = models.CharField(max_length=120)
    code = models.CharField(max_length=30)

    # This is the "accounting angel" mapping: subcategory -> group
    group = models.ForeignKey(
        AccountGroup, null=True, blank=True,
        on_delete=models.SET_NULL,
        related_name="subcategories"
    )

    class Meta:
        unique_together = [("category", "code")]

    def __str__(self):
        return f"{self.category.code}/{self.code} - {self.name}"
    
# =============================================================================
# GL ACCOUNT MODEL
# =============================================================================

class GLAccount(models.Model):
    """
    Excel-driven Ledger master.
    The 'no' (GL code) is auto-generated.
    """

    # Auto GL code (allow NULL until auto-filled)
    no = models.CharField(max_length=20, unique=True, null=True, blank=True)

    # Excel column: GL
    name = models.CharField(max_length=255)

    # Excel-driven keys (must NOT mix across dept/unit/head)
    unit = models.CharField(max_length=50, blank=True, default="")
    department = models.CharField(max_length=120, blank=True, default="")
    budget_head = models.CharField(max_length=255, blank=True, default="")

    # Active flag (fixes your form error)
    is_active = models.BooleanField(default=True)

    # Optional legacy compatibility (keep if used elsewhere)
    blocked = models.BooleanField(default=False)
    direct_posting = models.BooleanField(default=True)

    last_date_modified = models.DateField(auto_now=True)

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, null=True, blank=True,
        on_delete=models.SET_NULL, related_name="created_gl_accounts"
    )
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["unit", "department", "budget_head", "name"],
                name="uq_gl_unit_dept_head_name",
            ),
        ]
        indexes = [
            models.Index(fields=["department", "unit"]),
            models.Index(fields=["department", "unit", "budget_head"]),
            models.Index(fields=["name"]),
            models.Index(fields=["no"]),
        ]

    def save(self, *args, **kwargs):
        # Keep compatibility: blocked is inverse of active
        self.blocked = (not self.is_active)

        # First save to get PK, then generate code if missing
        creating = self.pk is None
        if creating and not self.no:
            super().save(*args, **kwargs)
            # Safe numeric code: 900000 + PK
            self.no = str(900000 + int(self.pk))
            kwargs2 = {"update_fields": ["no", "blocked", "last_date_modified", "updated_at"]}
            super().save(**kwargs2)
            return

        super().save(*args, **kwargs)

    def __str__(self):
        extra = []
        if self.unit:
            extra.append(self.unit)
        if self.department:
            extra.append(self.department)
        if self.budget_head:
            extra.append(self.budget_head)
        suffix = f" ({' | '.join(extra)})" if extra else ""
        return f"{self.no or '-'} - {self.name}{suffix}"
    
    @property
    def group(self):
        """
        Compatibility for BudgetLine.save(): returns an AccountGroup instance or None.

        Priority:
        1) direct mapping on GLAccount if present (future-proof)
        2) via subcategory.group if present
        3) via account_category.default_group if present
        """
        # 1) direct group field if you add it later
        g = getattr(self, "group_fk", None) or getattr(self, "account_group", None)
        if g:
            return g

        # 2) via subcategory.group (your AccountSubcategory already has group FK)
        sub = getattr(self, "subcategory", None) or getattr(self, "account_subcategory", None)
        if sub and getattr(sub, "group", None):
            return sub.group

        # 3) via category.default_group
        cat = getattr(self, "account_category", None)
        if cat and getattr(cat, "default_group", None):
            return cat.default_group

        return None
    
# =============================================================================
# CAPTIVE CONSUMPTION BUDGET MODELS
# =============================================================================
class CaptiveConsumptionBudget(models.Model):
    plan = models.ForeignKey("BudgetPlan", on_delete=models.CASCADE, related_name="captive_budgets")
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="captive_budgets_created"
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="captive_budgets_updated"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "Budget_captiveconsumptionbudget"   # ✅ IMPORTANT
        ordering = ("-updated_at", "-id")

    def __str__(self):
        return f"Captive Consumption ({self.plan})"


class CaptiveConsumptionLine(models.Model):
    class CaptiveType(models.TextChoices):
        CAPTIVE = "CAPTIVE", "Captive"
        TRIAL = "TRIAL", "Trial"
        SAMPLE = "SAMPLE", "Sample"
        INTERNAL = "INTERNAL", "Internal"

    budget = models.ForeignKey(CaptiveConsumptionBudget, on_delete=models.CASCADE, related_name="lines")

    item_name = models.CharField(max_length=255)
    captive_type = models.CharField(max_length=30, choices=CaptiveType.choices, default=CaptiveType.CAPTIVE)

    qty = models.DecimalField(max_digits=14, decimal_places=3, default=Decimal("0.000"))

    # ✅ your new columns
    rate = models.DecimalField(max_digits=14, decimal_places=4, default=Decimal("0.0000"))
    amount = models.DecimalField(max_digits=18, decimal_places=2, default=Decimal("0.00"))

    # ✅ keep timestamps but allow NULL because existing table doesn’t have them yet
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)

    class Meta:
        db_table = "Budget_captiveconsumptionline"     # ✅ IMPORTANT
        ordering = ("id",)

    def __str__(self):
        return f"{self.item_name} ({self.captive_type})"

# =============================================================================
# PACKING MATERIAL (NEW)
# =============================================================================

DEC0_QTY = Decimal("0.000000")
DEC0_AMT = Decimal("0.00")
DEC_AMT_Q = Decimal("0.01")


class PackingMaterialMaster(models.Model):
    """
    Master for Packing Materials.

    The list is typically synced from ERPBOMRow where `type` is Packing Material.
    Users maintain:
      - packing_size (Kg/pack, Ltr/drum etc)
      - rate (per pack / per unit)
    which are then used to compute requirements and values from Sales quantities.
    """

    item_code = models.CharField(max_length=100, unique=True, db_index=True)
    item_name = models.CharField(max_length=255)
    unit = models.CharField(max_length=50, blank=True, default="")

    # Kg per pack / capacity.
    packing_size = models.DecimalField(max_digits=18, decimal_places=3, null=True, blank=True)

    # ✅ NEW: Rate per unit (₹ / pack / drum / bag etc.)
    rate = models.DecimalField(max_digits=18, decimal_places=4, null=True, blank=True)

    is_active = models.BooleanField(default=True)

    updated_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("item_name", "item_code")
        indexes = [
            models.Index(fields=["item_name"]),
            models.Index(fields=["is_active"]),
        ]

    def __str__(self) -> str:
        return f"{self.item_name} ({self.item_code})"

    def clean(self):
        self.item_code = (self.item_code or "").strip().upper()
        self.item_name = (self.item_name or "").strip()
        self.unit = (self.unit or "").strip()

        # normalize numeric nulls
        if self.packing_size is not None and self.packing_size < 0:
            self.packing_size = None
        if self.rate is not None and self.rate < 0:
            self.rate = None

    def save(self, *args, **kwargs):
        self.clean()
        super().save(*args, **kwargs)


class PackingBudget(models.Model):
    """
    Packing budget header per plan.

    Pages:
      1) Products page: selects FG list from ProductionNorm and shows Sales Qty.
      2) Inputs page: assigns packing materials.
      3) Summary page: aggregated requirements.

    Excel sample formula:
      req = (qty_kg * (1 + wastage%/100)) / packing_size
    """

    plan = models.OneToOneField("BudgetPlan", on_delete=models.CASCADE, related_name="packing_budget")

    selected_products = models.JSONField(default=list, blank=True)
    wastage_percent_default = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal("10.00"))

    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True, related_name="packing_budgets")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("-updated_at", "-id")

    def __str__(self) -> str:
        return f"Packing Budget {self.plan.fy}"

    def get_selected_products(self) -> list[str]:
        v = self.selected_products or []
        if isinstance(v, list):
            return [str(x).strip() for x in v if str(x).strip()]
        return []

    def set_selected_products(self, items: list[str]) -> None:
        self.selected_products = [str(x).strip() for x in (items or []) if str(x).strip()]


class PackingBudgetLine(models.Model):
    """Packing requirements per Product (FG) and Packing Material."""

    budget = models.ForeignKey(PackingBudget, on_delete=models.CASCADE, related_name="lines")
    product_name = models.CharField(max_length=255, db_index=True)

    packing_material = models.ForeignKey(PackingMaterialMaster, on_delete=models.PROTECT, related_name="budget_lines")

    # Snapshots (copied from master to keep history stable)
    packing_code = models.CharField(max_length=100, blank=True, default="")
    packing_name = models.CharField(max_length=255, blank=True, default="")
    unit = models.CharField(max_length=50, blank=True, default="")
    packing_size = models.DecimalField(max_digits=18, decimal_places=3, null=True, blank=True)

    # ✅ NEW snapshot: rate per unit
    rate = models.DecimalField(max_digits=18, decimal_places=4, null=True, blank=True)

    wastage_percent = models.DecimalField(max_digits=6, decimal_places=2, default=Decimal("10.00"))

    # Monthwise requirement (No. of packs)
    req_apr = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)
    req_may = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)
    req_jun = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)
    req_jul = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)
    req_aug = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)
    req_sep = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)
    req_oct = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)
    req_nov = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)
    req_dec = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)
    req_jan = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)
    req_feb = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)
    req_mar = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)

    req_total = models.DecimalField(max_digits=24, decimal_places=6, default=DEC0_QTY)

    # ✅ NEW: Monthwise value (₹)
    val_apr = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)
    val_may = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)
    val_jun = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)
    val_jul = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)
    val_aug = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)
    val_sep = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)
    val_oct = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)
    val_nov = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)
    val_dec = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)
    val_jan = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)
    val_feb = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)
    val_mar = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)

    val_total = models.DecimalField(max_digits=24, decimal_places=2, default=DEC0_AMT)

    remarks = models.CharField(max_length=255, blank=True, default="")
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ("product_name", "packing_name", "packing_code", "id")
        constraints = [
            models.UniqueConstraint(
                fields=["budget", "product_name", "packing_material"],
                name="uq_packing_budget_product_material",
            )
        ]
        indexes = [
            models.Index(fields=["budget", "product_name"]),
            models.Index(fields=["budget", "packing_code"]),
        ]

    def clean(self):
        self.product_name = (self.product_name or "").strip()

        if self.packing_material_id:
            pm = self.packing_material
            self.packing_code = (pm.item_code or "").strip().upper()
            self.packing_name = (pm.item_name or "").strip()
            self.unit = (pm.unit or "").strip()
            self.packing_size = pm.packing_size
            self.rate = pm.rate  # ✅ copy rate from master

    @staticmethod
    def _q(v, default=DEC0_QTY) -> Decimal:
        if v in (None, ""):
            return default
        if isinstance(v, Decimal):
            return v
        try:
            return Decimal(str(v).replace(",", "").strip())
        except (InvalidOperation, ValueError):
            return default

    @staticmethod
    def _money(v: Decimal) -> Decimal:
        return (v or Decimal("0")).quantize(DEC_AMT_Q, rounding=ROUND_HALF_UP)

    def save(self, *args, **kwargs):
        self.clean()

        # --- total required qty ---
        months_req = [
            self.req_apr, self.req_may, self.req_jun, self.req_jul, self.req_aug, self.req_sep,
            self.req_oct, self.req_nov, self.req_dec, self.req_jan, self.req_feb, self.req_mar,
        ]
        s = DEC0_QTY
        for v in months_req:
            s += (v or DEC0_QTY)
        self.req_total = s

        # --- monthwise values ---
        rate = self._q(self.rate, Decimal("0"))
        def calc_val(req):
            reqd = self._q(req, DEC0_QTY)
            return self._money(reqd * rate)

        self.val_apr = calc_val(self.req_apr)
        self.val_may = calc_val(self.req_may)
        self.val_jun = calc_val(self.req_jun)
        self.val_jul = calc_val(self.req_jul)
        self.val_aug = calc_val(self.req_aug)
        self.val_sep = calc_val(self.req_sep)
        self.val_oct = calc_val(self.req_oct)
        self.val_nov = calc_val(self.req_nov)
        self.val_dec = calc_val(self.req_dec)
        self.val_jan = calc_val(self.req_jan)
        self.val_feb = calc_val(self.req_feb)
        self.val_mar = calc_val(self.req_mar)

        self.val_total = self._money(
            (self.val_apr or DEC0_AMT) + (self.val_may or DEC0_AMT) + (self.val_jun or DEC0_AMT) +
            (self.val_jul or DEC0_AMT) + (self.val_aug or DEC0_AMT) + (self.val_sep or DEC0_AMT) +
            (self.val_oct or DEC0_AMT) + (self.val_nov or DEC0_AMT) + (self.val_dec or DEC0_AMT) +
            (self.val_jan or DEC0_AMT) + (self.val_feb or DEC0_AMT) + (self.val_mar or DEC0_AMT)
        )

        super().save(*args, **kwargs)