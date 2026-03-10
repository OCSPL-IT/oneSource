from django.db import models
from datetime import date, time

class Vehicle(models.Model):
    record_date = models.DateField(default=date.today, null=True)  # Record Date column
    invoice = models.CharField(max_length=100, null=True)
    name_of_supplier = models.CharField(max_length=100)
    material = models.CharField(max_length=100)
    unit = models.CharField(max_length=20)
    qty = models.FloatField()
    reporting_date = models.DateField()
    report_time = models.TimeField()
    unloading_date = models.DateField(blank=True, null=True)
    unloading_time = models.TimeField(blank=True, null=True)
    unloading_days = models.IntegerField(default=0,blank=True, null=True)
    vehicle_no = models.CharField(max_length=100)
    name_of_transporter = models.CharField(max_length=100)
    status = models.CharField(max_length=50)
    manufacture = models.CharField(max_length=255, blank=True, null=True)
    remark = models.CharField(max_length=200, blank=True, null=True)

    class Meta:
        db_table = 'vehicle'


class MaterialRequest(models.Model):
    """One row for each material request / plan."""
    DOMESTIC = "DOM"
    EXPORT   = "EXP"

    TYPE_CHOICES = [
        (DOMESTIC, "Domestic"),
        (EXPORT,   "Export"),
    ]

    type            = models.CharField(
        max_length=3,
        choices=TYPE_CHOICES,
        default=DOMESTIC,
    )
    material_name   = models.CharField(max_length=150)
    trade_name      = models.CharField(max_length=150, blank=True)
    unit            = models.CharField(max_length=30, help_text="e.g. Kg / Litre / Nos")
    qty             = models.DecimalField("Quantity", max_digits=12, decimal_places=3)
    tentative_date  = models.DateField("Tentative Needed Date")

    created_at      = models.DateTimeField(auto_now_add=True)
    updated_at      = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]
        db_table = 'dispatch'

    def __str__(self):
        return f"{self.get_type_display()} – {self.material_name} ({self.qty} {self.unit})"

    def get_absolute_url(self):
        return reverse("store:material-detail", args=[self.pk])

   
# ----------------------------------------------------------------------------
# STORE/models.py  (cleaned & consolidated)
# ----------------------------------------------------------------------------
from django.db import models
from django.conf import settings

# Use your usual ERP alias rule (erp_source if present else default)
ERP_ALIAS = "erp_source" if "erp_source" in getattr(settings, "DATABASES", {}) else "default"

# ─────────────────────────────────────────────────────────────────────────────
# Rack & Pallet Masters
# ─────────────────────────────────────────────────────────────────────────────
class Rack(models.Model):
    code  = models.CharField(max_length=30, unique=True)    # e.g., A, B, C, GROUND, TANK
    zone  = models.CharField(max_length=30, blank=True)
    row   = models.CharField(max_length=30, blank=True)
    level = models.CharField(max_length=30, blank=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "store_rack"

    def __str__(self) -> str:
        return self.code


class Pallet(models.Model):
    """Pallets belong to a Rack."""
    rack   = models.ForeignKey('STORE.Rack', on_delete=models.PROTECT, related_name="pallets")
    number = models.CharField(max_length=30)                 # e.g., A101, GR03, TNK02
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "store_pallet"
        constraints = [
            models.UniqueConstraint(fields=["rack", "number"], name="uq_store_pallet_rack_number"),
        ]

    def __str__(self) -> str:
        return self.number


# ─────────────────────────────────────────────────────────────────────────────
# ERP Caches
# ─────────────────────────────────────────────────────────────────────────────
class GrnLineCache(models.Model):
    """Local cache of ERP GRN register rows (RM/PM)."""
    company_id    = models.IntegerField()
    year_id       = models.IntegerField()
    doc_no        = models.CharField(max_length=50, db_index=True)        # GRN No
    doc_date      = models.DateField(db_index=True)
    supplier_code = models.CharField(max_length=30, blank=True)
    supplier_name = models.CharField(max_length=200, blank=True)
    item_code     = models.CharField(max_length=50, db_index=True)
    item_name     = models.CharField(max_length=300, blank=True)
    # PR Item Type (from PRDET.lItmtyp -> ITMTYP.sName). Optional for backward-compat.
    pr_item_type  = models.CharField(max_length=100, blank=True)
    batch_no      = models.CharField(max_length=80, blank=True)
    uom           = models.CharField(max_length=20, blank=True)
    qty           = models.DecimalField(max_digits=18, decimal_places=3)    # received qty
    rm_pm         = models.CharField(max_length=2, default="RM", db_index=True)  # RM/PM derived
    warehouse     = models.CharField(max_length=50, blank=True)
    erp_line_id   = models.CharField(max_length=80, unique=True)            # stable uid from ERP line

    class Meta:
        db_table = "store_grn_line_cache"
        indexes = [
            models.Index(fields=["doc_date"]),
            models.Index(fields=["item_code"]),
            models.Index(fields=["rm_pm"]),
            models.Index(fields=["pr_item_type"]),
        ]

    def __str__(self) -> str:
        return f"{self.doc_no} / {self.item_code} / {self.batch_no or '-'}"


class IssueLineCache(models.Model):
    """Local cache of ERP material-issue rows that reduce stock."""
    company_id      = models.IntegerField()
    year_id         = models.IntegerField()
    issue_no        = models.CharField(max_length=50, db_index=True)
    issue_date      = models.DateField(db_index=True)
    cost_center     = models.CharField(max_length=120, blank=True)
    item_code       = models.CharField(max_length=50, db_index=True)
    item_name       = models.CharField(max_length=300, blank=True)
    # Issue Item Type (ITP.sName) exposed by SQL; optional to keep flow unchanged.
    issue_item_type = models.CharField(max_length=100, blank=True)
    batch_no        = models.CharField(max_length=80, blank=True)
    uom             = models.CharField(max_length=20, blank=True)
    qty             = models.DecimalField(max_digits=18, decimal_places=3)   # issued qty (positive)
    warehouse       = models.CharField(max_length=50, blank=True)
    erp_line_id     = models.CharField(max_length=80, unique=True)

    class Meta:
        db_table = "store_issue_line_cache"
        indexes = [
            models.Index(fields=["issue_date"]),
            models.Index(fields=["item_code"]),
            models.Index(fields=["issue_item_type"]),
        ]

    def __str__(self) -> str:
        return f"{self.issue_no} / {self.item_code} / {self.batch_no or '-'}"


# ─────────────────────────────────────────────────────────────────────────────
# Allocations & Issue break-up
# ─────────────────────────────────────────────────────────────────────────────
class RackAllocation(models.Model):
    """Allocation of a GRN line qty to a rack/pallet."""
    grn           = models.ForeignKey('STORE.GrnLineCache', on_delete=models.PROTECT, related_name="allocations")
    rack          = models.ForeignKey('STORE.Rack', on_delete=models.PROTECT)
    pallet        = models.ForeignKey('STORE.Pallet', on_delete=models.PROTECT, null=True, blank=True)
    allocated_qty = models.DecimalField(max_digits=18, decimal_places=3)
    created_at    = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "store_rack_allocation"

    @property
    def consumed_qty(self):
        agg = self.consumptions.aggregate(total=models.Sum("qty"))
        return agg["total"] or 0

    @property
    def balance_qty(self):
        return self.allocated_qty - self.consumed_qty


class RackIssue(models.Model):
    """Breakdown of one Issue line against one RackAllocation (FIFO)."""
    issue      = models.ForeignKey('STORE.IssueLineCache', on_delete=models.CASCADE, related_name="rack_breakups")
    allocation = models.ForeignKey('STORE.RackAllocation', on_delete=models.PROTECT, related_name="consumptions")
    qty        = models.DecimalField(max_digits=18, decimal_places=3)

    class Meta:
        db_table = "store_rack_issue"




from django.db import models


class InvAgeingPreview(models.Model):
    invageingpreviewid = models.BigAutoField(
        primary_key=True,
        db_column="InvAgeingPreviewId",
    )

    company_id = models.IntegerField(db_column="company_id")
    doc_no = models.CharField(max_length=50, db_column="doc_no")
    doc_date = models.DateField(db_column="doc_date")

    company_name = models.CharField(max_length=200, db_column="company_name")
    transaction_type = models.CharField(max_length=200, null=True, blank=True, db_column="transaction_type")

    item_type = models.CharField(max_length=100, null=True, blank=True, db_column="item_type")
    item_code = models.CharField(max_length=60, null=True, blank=True, db_column="item_code")
    item_name = models.CharField(max_length=255, null=True, blank=True, db_column="item_name")
    unit = models.CharField(max_length=60, null=True, blank=True, db_column="unit")

    from_location = models.CharField(max_length=200, null=True, blank=True, db_column="from_location")
    from_project = models.CharField(max_length=200, null=True, blank=True, db_column="from_project")
    to_location = models.CharField(max_length=200, null=True, blank=True, db_column="to_location")
    to_project = models.CharField(max_length=200, null=True, blank=True, db_column="to_project")

    txn_location = models.CharField(max_length=200, null=True, blank=True, db_column="txn_location")
    stock_location = models.CharField(max_length=200, null=True, blank=True, db_column="stock_location")

    batch_no = models.CharField(max_length=120, null=True, blank=True, db_column="batch_no")
    mfg_date = models.DateField(null=True, blank=True, db_column="mfg_date")
    retest_date = models.DateField(null=True, blank=True, db_column="retest_date")

    opening_qty = models.DecimalField(max_digits=21, decimal_places=3, db_column="opening_qty")
    receipt_qty = models.DecimalField(max_digits=21, decimal_places=3, db_column="receipt_qty")
    issue_qty = models.DecimalField(max_digits=21, decimal_places=3, db_column="issue_qty")
    closing_qty = models.DecimalField(max_digits=21, decimal_places=3, db_column="closing_qty")
    closing_value = models.DecimalField(max_digits=21, decimal_places=3, db_column="closing_value")

    inventory_category = models.CharField(max_length=120, null=True, blank=True, db_column="inventory_category")
    inventory_subcategory = models.CharField(max_length=120, null=True, blank=True, db_column="inventory_subcategory")

    row_in_batch = models.IntegerField(null=True, blank=True, db_column="row_in_batch")
    total_closing_per_batch = models.DecimalField(max_digits=21, decimal_places=3, null=True, blank=True, db_column="total_closing_per_batch")
    total_closing_value_per_batch = models.DecimalField(max_digits=21, decimal_places=3, null=True, blank=True, db_column="total_closing_value_per_batch")

    sum_closing_qty = models.DecimalField(max_digits=21, decimal_places=3, null=True, blank=True, db_column="sum_closing_qty")
    sum_closing_value = models.DecimalField(max_digits=21, decimal_places=3, null=True, blank=True, db_column="sum_closing_value")

    batch_start_date = models.DateField(null=True, blank=True, db_column="batch_start_date")
    age_days = models.IntegerField(null=True, blank=True, db_column="age_days")

    synced_at = models.DateTimeField(db_column="synced_at")

    class Meta:
        db_table = "InvAgeingPreview"
        managed = False  # ✅ table already exists in SQL Server
        verbose_name = "Inventory Ageing Preview"
        verbose_name_plural = "Inventory Ageing Preview"


        # ✅ add ONE custom permission only for Sync
        permissions = [
            ("sync_invageingpreview", "Can sync Inventory Ageing Preview"),
        ]

    def __str__(self):
        return f"{self.company_name} | {self.item_name or ''} | {self.batch_no or ''}"
