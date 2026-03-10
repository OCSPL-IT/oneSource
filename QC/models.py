# products/models.py

from django.db import models
from django.core.exceptions import ValidationError
from django.conf import settings
from django.utils import timezone
from django.db.models import Max
from datetime import date



class Product(models.Model):
    name = models.CharField(max_length=200)
    code = models.CharField(max_length=50, blank=True)
    item_type = models.CharField(max_length=100, blank=True)
    appearance_options = models.ManyToManyField('AppearanceOption', blank=True, related_name='products')
    stages = models.CharField(max_length=200, blank=True, help_text="Comma-separated list of stages")
    
    class Meta:
        db_table = 'qc_product'

    def __str__(self):
        return self.name


class Spec(models.Model):
    TYPE_NUMERIC = 'numeric'
    TYPE_CHOICE  = 'choice'
    TYPE_TEXT    = 'text'

    SPEC_TYPE_CHOICES = [
        (TYPE_NUMERIC, 'Numeric Range'),
        (TYPE_CHOICE,  'Choice List'),
        (TYPE_TEXT,    'Free‐Text'),
    ]

    product = models.ForeignKey(Product, on_delete=models.CASCADE, related_name='specs')
    group   = models.CharField(max_length=100,blank=True,null=True,help_text="Heading for this spec batch, e.g. '8 hr', '15 hr', etc.")
    name = models.CharField(max_length=100)
    spec_type = models.CharField(max_length=10, choices=SPEC_TYPE_CHOICES, default=TYPE_NUMERIC, help_text="Choose whether this spec uses a numeric range, a set of choices, or free text.")
    min_val = models.DecimalField(max_digits=8, decimal_places=3, blank=True, null=True)
    max_val = models.DecimalField(max_digits=8, decimal_places=3, blank=True, null=True)
    allowed_choices = models.TextField(blank=True, help_text="(Choice type only) Comma‐separate the allowed text values, e.g. “Brown liquid,Clear liquid,White powder”")
    acceptance_criteria = models.DecimalField(max_digits=8, decimal_places=3,blank=True, null=True,help_text="Deviation limit outside Min/Max range (e.g. < Min or > Max).")
    unit = models.CharField(max_length=50, blank=True,help_text="Display unit, e.g. °C, %, ppm")
    is_critical = models.BooleanField(default=False,db_index=True,help_text="Tick if this specification is critical (master definition).")
    
    class Meta:
        db_table = 'qc_spec'
        unique_together = ('product', 'group', 'name')
        ordering        = ['product', 'group', 'name']

    def __str__(self):
        return f"{self.product.name} – {self.name}"
    
    

    def clean(self):
        """
        Enforce:
          - If spec_type is numeric → min_val and max_val must both be provided and min_val ≤ max_val.
          - If spec_type is choice  → allowed_choices must be a non‐empty comma‐separated list.
          - If spec_type is text    → neither min/max nor allowed_choices are required.
        """
        super().clean()

        if self.spec_type == self.TYPE_NUMERIC:
            if self.min_val is None or self.max_val is None:
                raise ValidationError({
                    'min_val': "Numeric spec requires both a minimum and a maximum value.",
                    'max_val': "Numeric spec requires both a minimum and a maximum value."
                })
            if self.min_val > self.max_val:
                raise ValidationError("Minimum value must be less than or equal to maximum value.")

        elif self.spec_type == self.TYPE_CHOICE:
            if not self.allowed_choices.strip():
                raise ValidationError({
                    'allowed_choices': "Choice spec requires at least one allowed choice (comma‐separated)."
                })

        else:  # TYPE_TEXT
            # Free‐text specs do not need min_val/max_val or allowed_choices.
            pass

class AppearanceOption(models.Model):
    """
    A master table to store “Appearance” choices for specs. 
    You will upload your Excel sheet once, populating this table.
    """
    name = models.CharField(max_length=200, unique=True)

    class Meta:
        db_table = 'qc_appearance_option'
        verbose_name = "Appearance Option"
        verbose_name_plural = "Appearance Options"
        ordering = ["name"]

    def __str__(self):
        return self.name





class SampleDescriptionOption(models.Model):
    """
    Predefined sample descriptions for QCEntry.
    """
    name       = models.CharField(max_length=100, unique=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)


    class Meta:
        db_table = 'sample_description'
        ordering = ['name']
        indexes = [ models.Index(fields=["name"]), ]
        verbose_name = "Sample Description Option"
        verbose_name_plural = "Sample Description Options"

    def __str__(self):
        return self.name


class QCTestParameter(models.Model):
    """
    Predefined QC Test Parameters for use in specifications.
    """
    name = models.CharField(max_length=100, unique=True)

    class Meta:
        db_table = 'qc_test_parameter'
        ordering = ['name']
        verbose_name = "QC Test Parameter"
        verbose_name_plural = "QC Test Parameters" # Updated plural name

    def __str__(self):
        return self.name



class QCEntry(models.Model):
    STAGE_CHOICES = [
        ('raw',      'Raw Material'),
        ('inproc_1', 'In-Process Stage 1'),
        ('inproc_2', 'In-Process Stage 2'),
        ('finished', 'Finished Goods'),
    ]

    STATUS_CHOICES = [
        ('draft',        'Draft (Production)'),
        ('pending_qc',   'Pending QC'),
        ('qc_completed', 'QC Completed'),
        ('cancelled',    'Cancelled'),
    ]

    DECISION_CHOICES = [
        ('approved',                 'Approved'),
        ('approved_under_deviation', 'Approved under deviation'),
        ('rejected',                 'Rejected'),
        ('fail',                     'Fail'),
    ]
    AR_TYPE_CHOICES = [
        ('RM', 'Raw Material'),
        ('PM', 'Packing Material'),
        ('IP', 'In-Process'),
        ('FG', 'Finished Goods'),
        ('SFG', 'Semi Finished Goods'),
    ]
    INSTRUMENT_CHOICES = [
        ("OCSPL/QC/GC/001",   "OCSPL/QC/GC/001"),("OCSPL/QC/GC/003",   "OCSPL/QC/GC/003"),("OCSPL/QC/GC/004",   "OCSPL/QC/GC/004"),
        ("OCSPL/QC/GC/006",   "OCSPL/QC/GC/006"),("OCSPL/QC/GC/007",   "OCSPL/QC/GC/007"),("OCSPL/QC/GC/008",   "OCSPL/QC/GC/008"),
        ("OCSPL/QC/GC/009",   "OCSPL/QC/GC/009"),("OCSPL/QC/GC/010",   "OCSPL/QC/GC/010"),("OCSPL/QC/GC/011",   "OCSPL/QC/GC/011"),
        ("OCSPL/QC/GC/012",   "OCSPL/QC/GC/012"),("OCSPL/QC/GC/013",   "OCSPL/QC/GC/013"),
        ("OCSPL/QC/HPLC/001", "OCSPL/QC/HPLC/001"),("OCSPL/QC/HPLC/002", "OCSPL/QC/HPLC/002"),("OCSPL/QC/HPLC/003", "OCSPL/QC/HPLC/003"),
        ("OCSPL/QC/HPLC/004", "OCSPL/QC/HPLC/004"),("OCSPL/QC/AT/002", "OCSPL/QC/AT/002"),("OCSPL/QC/KF/003", "OCSPL/QC/KF/003"),
        ("OCSPL/QC/KF/004", "OCSPL/QC/KF/004"),("OCSPL/QC/pH/003",   "OCSPL/QC/pH/003"),("OCSPL/QC/pH/004",   "OCSPL/QC/pH/004"),
    ("OCSPL/QC/MR/002",   "OCSPL/QC/MR/002"),("OCSPL/QC/POL/001",  "OCSPL/QC/POL/001"),("OCSPL/QC/UV/001",   "OCSPL/QC/UV/001"),
    ("OCSPL/QC/GT/001",   "OCSPL/QC/GT/001"),("OCSPL/QC/MA/001",   "OCSPL/QC/MA/001"),("OCSPL/QC/MA/002",   "OCSPL/QC/MA/002"),
    ("OCSPL/QC/AM/001",   "OCSPL/QC/AM/001"),("OCSPL/QC/CM/001",   "OCSPL/QC/CM/001"),("None", "None"),
        ]

    # ─── New fields ──────────────────────────────────────
    decision_status = models.CharField(max_length=30, choices=DECISION_CHOICES, blank=True, null=True, help_text="QC decision: approved, approved under deviation, or rejected")
    entry_no = models.PositiveIntegerField(unique=True, editable=False, null=True, help_text="Automatically assigned sequential entry number.")
    ar_type = models.CharField("AR Category",max_length=5,choices=AR_TYPE_CHOICES,default='RM',help_text="Category for AR number generation")
    ar_no = models.CharField("AR No.", max_length=100, blank=True, help_text="Analysis Request number assigned by Production.")
    product = models.ForeignKey(Product, on_delete=models.CASCADE, help_text="Select the product for which you’re generating this QC entry.")
    block = models.CharField(max_length=50, blank=True, help_text="Auto-populated when an equipment is chosen.")
    equipment_id = models.CharField(max_length=50, blank=True, help_text="Select which equipment/batch line this QC pertains to.")
    test_required_for = models.CharField("Test Required For", max_length=100, blank=True, help_text="Why this QC is being performed.")
    stage = models.CharField(max_length=100, choices=STAGE_CHOICES, help_text="At which stage of production this sample was taken.")
    group   = models.CharField("Specification Group",max_length=100,blank=True, help_text="Which specs group was used when entering QC results.")
    selected_group     = models.CharField(max_length=200,blank=True,help_text="Which specification group was selected when entering QC results.")
    general_remarks    = models.TextField(blank=True, max_length=250,help_text="General remarks entered by QC (up to 250 characters).")
    prod_sign_date = models.DateField(null=True, blank=True, help_text="Date when Production signed off on this batch.")
    batch_no = models.CharField(max_length=100, blank=True, help_text="Batch number (pulled from ERP BMR).")
    sample_received_at = models.DateTimeField("Sample received at QC", null=True, blank=True, help_text="When Production delivered the sample to QC.")
    entry_date = models.DateTimeField(default=timezone.now, help_text="Date/time when this QC entry was created.")
    sample_sent_at = models.DateTimeField(null=True, blank=True, help_text="Date/time when sample was sent for analysis.")
    sample_description = models.TextField("Sample Description", blank=True, help_text="Short description or notes about the sample.")
    frequency = models.CharField(max_length=50, blank=True, null=True)
    sample_description_text = models.TextField("Description Notes",blank=True, max_length=250,help_text="Additional notes about the sample (up to 250 chars).")
    release_by_qc_at = models.DateTimeField("Sample Released from QC", null=True, blank=True, help_text="When QC officially released the batch.")
    created = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='draft', help_text="draft → pending_qc → qc_completed/cancelled")
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, related_name='qc_created_entries', null=True, blank=True, on_delete=models.SET_NULL, help_text="Production user who created this entry.")
    qc_completed_by = models.ForeignKey(settings.AUTH_USER_MODEL, related_name='qc_entries_completed', null=True, blank=True, on_delete=models.SET_NULL, help_text="QC user who completed the results.")
    test_parameters = models.CharField("Test Parameters", max_length=500, blank=True, help_text="Comma-separated list of selected parameters.")
    instrument_id = models.CharField("Instrument ID",choices=INSTRUMENT_CHOICES,max_length=50,blank=True,help_text="Instrument identifier (e.g. OCSPL/QC/GC/001)")
    qty = models.DecimalField(max_digits=12, decimal_places=3, blank=True, null=True,help_text="Finished Goods quantity (only used on FGQC form).")
    
    class Meta:
        db_table = 'qc_entry'
        ordering = ['-created']
        indexes = [
            models.Index(fields=['product', 'batch_no']),
            models.Index(fields=['product', 'entry_date']),
            models.Index(fields=['decision_status', 'entry_date']),
        ]

    def save(self, *args, **kwargs):
        # Assign sequential entry_no on first save
        if self.entry_no is None:
            last = QCEntry.objects.aggregate(max_no=Max('entry_no'))['max_no'] or 0
            self.entry_no = last + 1

        # Auto-generate AR No. if not set
        if not self.ar_no:
            today = timezone.now().date()
            year = today.year
            month = today.month
            if month >= 4:
                fy_start = year % 100
                fy_end = (year + 1) % 100
            else:
                fy_start = (year - 1) % 100
                fy_end = year % 100
            fy_string = f"{fy_start:02d}-{fy_end:02d}"
            prefix = f"QC/{self.ar_type}/{fy_string}/"

            last_ar = (
                QCEntry.objects
                .filter(ar_no__startswith=prefix)
                .order_by('-ar_no')
                .first()
            )
            if last_ar and last_ar.ar_no:
                last_seq = int(last_ar.ar_no.split('/')[-1])
                next_seq = last_seq + 1
            else:
                next_seq = 1
            self.ar_no = f"{prefix}{next_seq:05d}"

        super().save(*args, **kwargs)

    def __str__(self):
        return (
            f"Entry No. {self.entry_no} – {self.product.name} "
            f"(Batch: {self.batch_no}) [{self.get_status_display()}]"
        )

class SpecEntry(models.Model):
    qc_entry = models.ForeignKey(QCEntry, on_delete=models.CASCADE, related_name='values', help_text="The QCEntry to which this particular specification result belongs.")
    spec = models.ForeignKey(Spec, on_delete=models.CASCADE, help_text="Which specification (test parameter) this result is for.")
    value = models.CharField(max_length=200, blank=True, null=True, help_text="Result for this spec: numeric or free-text (e.g. appearance).")
    remark = models.TextField(blank=True, help_text="Automatically set to 'Pass' or 'Fail' based on min/max, or left blank.")
    is_critical = models.BooleanField(default=False,db_index=True,help_text="Critical at the time of entry (copied from master Spec).")

    class Meta:
        db_table = 'qc_spec_entry'
        unique_together = ('qc_entry', 'spec')
        ordering = ['qc_entry', 'spec']
        indexes = [
            models.Index(fields=['qc_entry', 'spec']),
            models.Index(fields=['spec', 'is_critical']),
        ]
        
    def save(self, *args, **kwargs):
        # Always mirror from master spec so reports can filter reliably
        if self.spec_id:
            self.is_critical = bool(getattr(self.spec, 'is_critical', False))
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.qc_entry.id} – {self.spec.name}: {self.value}"

class LocalItemMaster(models.Model):
    """
    A local copy of the ERP Item Master.
    We will sync this table with the ERP view periodically.
    """
    product_id = models.CharField(max_length=50, primary_key=True, help_text="ERP’s unique product ID.")
    product_name = models.CharField(max_length=200, help_text="ERP’s product description/name.")
    item_type = models.CharField(max_length=100, help_text="ERP’s item type/category.")

    class Meta:
        db_table = 'qc_localitemmaster'
        ordering = ['product_name']
        verbose_name = "Local Item Master"
        verbose_name_plural = "Local Item Masters"

    def __str__(self):
        return f"{self.product_name} ({self.product_id})"


class LocalEquipmentMaster(models.Model):
    """
    A local copy of the ERP Equipment Master.
    We will sync this table with the ERP view periodically.
    """
    eqp_code = models.CharField(max_length=50, primary_key=True, help_text="ERP’s unique equipment ID.")
    eqp_name = models.CharField(max_length=200, help_text="ERP’s equipment name/description.")
    eqp_remarks = models.CharField(max_length=500, blank=True, null=True, help_text="Optional remarks or notes from ERP.")
    unit_code = models.CharField(max_length=50, blank=True, null=True, help_text="Optional unit code (e.g. department or plant).")
    tag_no = models.CharField(max_length=100, blank=True, null=True, help_text="Optional tag number for this equipment.")
    block_name = models.CharField(max_length=100, blank=True, null=True, help_text="If applicable, the block or area name this equipment is in.")
    

    class Meta:
        db_table = 'qc_localequipmentmaster'
        ordering = ['eqp_name']
        verbose_name = "Local Equipment Master"
        verbose_name_plural = "Local Equipment Masters"

    def __str__(self):
        return f"{self.eqp_name} ({self.eqp_code})"


class BmrIssue(models.Model):
    """
    Holds a synced copy of “BMR Issue” rows from the ERP database.
    We only populate this once (in qc_create), then read from it locally for any dropdowns or filters.
    """
    bmr_issue_type = models.CharField(max_length=100, help_text="ERP’s BMR Issue Type (e.g. Fresh Batch, Cleaning, etc.).")
    bmr_issue_no = models.CharField(max_length=100, help_text="ERP’s document number for the BMR Issue.")
    bmr_issue_date = models.DateField(help_text="Date of the BMR Issue in ERP.")
    fg_name = models.CharField(max_length=200, help_text="Finished Goods name.")
    op_batch_no = models.CharField(max_length=100, help_text="ERP’s “Output Batch Number” to filter on.")
    product_name = models.CharField(max_length=200, blank=True, null=True, help_text="ERP’s Product Name if available.")
    block = models.CharField(max_length=200, blank=True, null=True, help_text="ERP’s Block value if available.")
    line_no = models.IntegerField(help_text="Line number within the BMR Issue (ERP detail row).")
    item_type = models.CharField(max_length=100, help_text="ERP’s item type description.")
    item_code = models.CharField(max_length=100, help_text="ERP’s item code.")
    item_name = models.CharField(max_length=200, help_text="ERP’s item name.")
    item_narration = models.TextField(blank=True, null=True, help_text="Optional narration/remarks from ERP.")
    uom = models.CharField(max_length=50, help_text="Unit of measure code.")
    batch_quantity = models.DecimalField(max_digits=18, decimal_places=3, help_text="Quantity (in ERP) for this line.")


    class Meta:
        db_table = 'qc_bmr_issue'
        unique_together = ('bmr_issue_no', 'line_no')
        ordering = ['bmr_issue_no', 'line_no']
        verbose_name = "BMR Issue"
        verbose_name_plural = "BMR Issues"

    def __str__(self):
        return f"{self.bmr_issue_no} – Line {self.line_no}"
    

class LocalBOMDetail(models.Model):
    sr_no         = models.IntegerField()
    itm_type      = models.CharField(max_length=200)
    item_name     = models.CharField(max_length=200)
    fg_name       = models.CharField(max_length=200, blank=True, null=True)
    item_code     = models.CharField(max_length=50)
    quantity      = models.DecimalField(max_digits=18, decimal_places=6)
    bom_code      = models.CharField(max_length=50)
    bom_name      = models.CharField(max_length=200)
    type          = models.CharField(max_length=200)
    bom_item_code = models.CharField(max_length=50)
    name          = models.CharField(max_length=200)
    unit          = models.CharField(max_length=100)
    bom_qty       = models.DecimalField(max_digits=18, decimal_places=6)
    cflag         = models.CharField(max_length=5)

class Meta:
        db_table = 'qc_localbomdetail'
        verbose_name = "Local BOM Detail"
        verbose_name_plural = "Local BOM Details"
        ordering = ['sr_no']

def __str__(self):
        return f"{self.sr_no}: {self.item_name} ({self.bom_code})"


class COARecord(models.Model):
    qc_entry    = models.ForeignKey("QCEntry", on_delete=models.CASCADE, related_name="coa_records")
    created_by  = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True)
    created_at  = models.DateTimeField(auto_now_add=True)
    extra_data  = models.JSONField(default=dict, blank=True)
    # ---------------------------------------------------------------------
    # Change upload_to to empty so it writes into MEDIA_ROOT directly:
    pdf         = models.FileField(upload_to="qc/coa_pdfs/", max_length=255)
    # ---------------------------------------------------------------------

    def __str__(self):
        return f"COA #{self.pk} for QCEntry {self.qc_entry.pk}"


# -----------------------------------------------------------------------------
# NEW: Daily QA Report (for your PDF-aligned form; prefilled from ERP/OnSource)
# -----------------------------------------------------------------------------

from django.conf import settings
from django.db import models

STATUS_CHOICES = [
    ("Pass", "Pass"),
    ("Fail", "Fail"),
    ("Hold", "Hold"),
    ("NA", "N/A"),
]


# --------------------------------------------------------------------------
# Main Daily QA Report (Header)
# --------------------------------------------------------------------------
class DailyQAReport(models.Model):
    report_date = models.DateField(db_index=True)
    created_by = models.ForeignKey(
        getattr(settings, "AUTH_USER_MODEL", "auth.User"),
        on_delete=models.SET_NULL, null=True, blank=True
    )

    # “Other Details”
    customer_complaints = models.PositiveIntegerField(default=0)
    analytical_mistakes = models.PositiveIntegerField(default=0)
    process_deviations = models.PositiveIntegerField(default=0)
    incident_first_aid_injury = models.PositiveIntegerField(default=0)
    ftr_percent = models.DecimalField(max_digits=5, decimal_places=2, default=0)
    analytical_downtime_hrs = models.DecimalField(max_digits=6, decimal_places=2, default=0)
    finished_goods_inspections = models.PositiveIntegerField(default=0)
    any_other_abnormality = models.TextField(blank=True)

    # Safety Observation summary (if still needed)
    safety_observation_text = models.TextField(blank=True)
    obs_total = models.PositiveIntegerField(default=0)
    obs_closed = models.PositiveIntegerField(default=0)
    obs_open = models.PositiveIntegerField(default=0)
    near_miss_total = models.PositiveIntegerField(default=0)
    near_miss_closed = models.PositiveIntegerField(default=0)
    near_miss_open = models.PositiveIntegerField(default=0)

    class Meta:
        db_table = "qc_daily_qareport"
        ordering = ["-report_date"]
        constraints = [
            models.UniqueConstraint(fields=["report_date"], name="uq_daily_report_date"),
        ]

    def __str__(self):
        return f"QA Daily Report – {self.report_date:%d-%m-%Y}"


# --------------------------------------------------------------------------
# Incoming Material (RM / PM)
# --------------------------------------------------------------------------
class IncomingMaterial(models.Model):
    """Incoming Material [RM/PM] linked to Daily QA Report"""

    RM_PM = [
        ("RM", "Raw Material"),
        ("PM", "Packing Material"),
    ]

    report = models.ForeignKey(
        DailyQAReport,
        on_delete=models.CASCADE,
        related_name="incoming"
    )

    # From ERP (prefilled via sync_incoming_grn)
    grn_no = models.CharField(max_length=50, blank=True, null=True)
    grn_date = models.DateField(blank=True, null=True, db_index=True)
    supplier_code = models.CharField(max_length=50, blank=True)
    supplier = models.CharField(max_length=200, blank=True)

    material_type = models.CharField(max_length=2, choices=RM_PM, default="RM")
    material_code = models.CharField(max_length=50, blank=True)
    material = models.CharField(max_length=200)

    qty_mt = models.DecimalField(max_digits=12, decimal_places=3, default=0)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default="Pass")
    remarks = models.CharField(max_length=300, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "qc_daily_incoming"
        ordering = ["id"]
        indexes = [models.Index(fields=["material_type"])]

    def __str__(self):
        return f"{self.material_type} – {self.material} ({self.qty_mt} MT)"

# --------------------------------------------------------------------------
# PDL Samples
# --------------------------------------------------------------------------
# class PDLSample(models.Model):
#     report = models.ForeignKey(DailyQAReport, on_delete=models.CASCADE, related_name="pdl_samples")
#     sample_name = models.CharField(max_length=200)
#     pending = models.BooleanField(default=True)
#     remark = models.CharField(max_length=300, blank=True)

#     class Meta:
#         db_table = "qc_daily_pdl_sample"
#         ordering = ["id"]

#     def __str__(self):
#         return f"{self.sample_name} ({'Pending' if self.pending else 'Done'})"

# --------------------------------------------------------------------------
# PDL Samples (updated to match forms)
# --------------------------------------------------------------------------
class PDLSample(models.Model):
    report = models.ForeignKey(
        DailyQAReport,
        on_delete=models.CASCADE,
        related_name="pdl_samples"
    )

    stage = models.CharField(max_length=100, blank=True)                     # e.g., "Filtration", "Drying"
    sample_name = models.CharField(max_length=200)                           # e.g., "PDL-123"
    result = models.CharField(max_length=10, choices=STATUS_CHOICES, default="NA")
    remarks = models.CharField(max_length=300, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "qc_daily_pdl_sample"
        ordering = ["id"]
        indexes = [
            models.Index(fields=["report", "stage"]),
        ]

    def __str__(self):
        return f"{self.stage or 'Stage'} – {self.sample_name} ({self.result})"

# --------------------------------------------------------------------------
# Incoming GRN Cache
# --------------------------------------------------------------------------
class IncomingGRNCache(models.Model):
    """Local cache of ERP GRN lines used to prefill Daily QA Report (D-1 or any date)."""
    grn_no = models.CharField(max_length=50)
    grn_date = models.DateField(db_index=True)

    supplier_code = models.CharField(max_length=50, blank=True)
    supplier_name = models.CharField(max_length=200, blank=True)

    # Item info from ERP
    item_type = models.CharField(max_length=100)   # 'Key Raw Material' / 'Raw Material' / 'Packing Material'
    item_code = models.CharField(max_length=50, blank=True)
    item_name = models.CharField(max_length=200)
    qty = models.DecimalField(max_digits=18, decimal_places=3)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "qc_incoming_grn_cache"
        indexes = [
            models.Index(fields=["grn_date", "item_type"]),
            models.Index(fields=["item_code"]),
        ]

    def __str__(self):
        return f"{self.grn_no} – {self.item_name} ({self.qty})"
    
# ----------------------------------------------------------------------------------------
# CustomerComplaint
# ----------------------------------------------------------------------------------------
    
class CustomerComplaint(models.Model):
    STATUS_CHOICES = [
        ("Open", "Open"),
        ("Under Investigation", "Under Investigation"),
        ("Closed", "Closed"),
    ]

    complaint_date = models.DateField(db_index=True)
    complaint_no   = models.CharField(max_length=50, unique=True)

    # NEW
    product_name = models.CharField(max_length=200)
    finished_product_name = models.CharField(max_length=200, blank=True)   # <— NEW

    customer_name = models.CharField(max_length=200)
    nature_of_complaint = models.TextField()
    complaint_type = models.CharField(max_length=150)
    investigation = models.TextField(blank=True, verbose_name="Investigation / Root Cause Analysis")
    corrective_action = models.TextField(blank=True)
    preventive_action = models.TextField(blank=True)
    status = models.CharField(max_length=40, choices=STATUS_CHOICES, default="Open")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "qc_customer_complaint"
        ordering = ["-complaint_date"]

    def __str__(self):
        return f"{self.complaint_no} - {self.product_name}"
    
# -----------------------------------------------------------------------------------
#                   Analytical Downtime 
# -----------------------------------------------------------------------------------
from django.db import models
from django.utils import timezone
from django.apps import apps


class AnalyticalDowntime(models.Model):
    STATUS = [
        ("open", "Open"),
        ("progress", "In Progress"),
        ("closed", "Closed"),
    ]
    CATEGORY = [
        ("maintenance", "Maintenance"),
        ("calibration", "Calibration"),
        ("repair", "Repair"),
        ("other", "Other"),
        ("busy", "Busy"),
    ]

    incident_no = models.CharField(
        max_length=30,
        unique=True,
        editable=False,
        help_text="Auto-generated, e.g. INC/25-26/00001",
    )
    instrument_id = models.CharField(max_length=50, help_text="HPL-01 / GC-03 / etc.")
    start_at = models.DateTimeField()
    end_at = models.DateTimeField(blank=True, null=True)
    ongoing = models.BooleanField(default=False, help_text="Tick if still ongoing")
    category = models.CharField(max_length=20, choices=CATEGORY, default="maintenance")
    short_reason = models.CharField(max_length=200, blank=True)
    detail_reason = models.TextField(blank=True)

    # 🔹 Impact assessment (with Stage added)
    stage = models.CharField(max_length=120, blank=True)
    product_name = models.CharField(max_length=200, blank=True)
    batch_no = models.CharField(max_length=60, blank=True)
    tests_delayed = models.PositiveIntegerField(default=0)
    retest_due_date = models.DateField(blank=True, null=True)
    resolved_by = models.CharField(max_length=120, blank=True)
    remarks = models.CharField(max_length=300, blank=True)

    # Status (auto-managed but editable)
    status = models.CharField(max_length=10, choices=STATUS, default="open", db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "qc_analytical_downtime"
        ordering = ["-start_at"]
        indexes = [
            models.Index(fields=["instrument_id", "status"]),
            models.Index(fields=["stage"]),
        ]

    def __str__(self):
        return f"{self.incident_no or '(unsaved)'} – {self.instrument_id}"

    # Derived
    @property
    def duration_minutes(self) -> int:
        end = self.end_at or timezone.now()
        delta = end - self.start_at
        return max(int(delta.total_seconds() // 60), 0)

    def clean(self):
        if self.end_at and self.end_at < self.start_at:
            from django.core.exceptions import ValidationError
            raise ValidationError({"end_at": "End time must be after start time."})

    # Helper: infer product from Stage using ERP-synced BOM cache (LocalBOMDetail)
    def _infer_product_from_stage(self) -> str:
        st = (self.stage or "").strip()
        if not st or self.product_name:  # don't override if already set
            return ""
        try:
            LocalBOMDetail = apps.get_model("QC", "LocalBOMDetail")
        except Exception:
            return ""
        hit = (
            LocalBOMDetail.objects
            .filter(item_name__iexact=st)
            .values_list("fg_name", flat=True)
            .first()
        ) or (
            LocalBOMDetail.objects
            .filter(item_name__icontains=st)
            .values_list("fg_name", flat=True)
            .first()
        )
        return (hit or "").strip()

    # 🔹 Generate Incident No (Prefix + FY + Sequence)
    def _generate_incident_no(self) -> str:
        prefix = "INC"
        now = timezone.now()

        # Determine financial year (Apr–Mar)
        if now.month >= 4:
            start_year = now.year % 100
            end_year = (now.year + 1) % 100
        else:
            start_year = (now.year - 1) % 100
            end_year = now.year % 100

        fy_str = f"{start_year:02d}-{end_year:02d}"

        # Find latest number for this FY
        last = (
            AnalyticalDowntime.objects.filter(incident_no__startswith=f"{prefix}/{fy_str}/")
            .order_by("-incident_no")
            .values_list("incident_no", flat=True)
            .first()
        )

        next_no = 1
        if last:
            try:
                next_no = int(last.split("/")[-1]) + 1
            except (ValueError, IndexError):
                next_no = 1

        return f"{prefix}/{fy_str}/{next_no:05d}"

    def save(self, *args, **kwargs):
        # Auto incident number only if not set
        if not self.incident_no:
            self.incident_no = self._generate_incident_no()

        # auto status
        if self.ongoing or not self.end_at:
            self.status = "open"
        elif self.end_at and not self.ongoing:
            self.status = "closed"

        # Auto-fill product_name from Stage if empty
        if not (self.product_name or "").strip():
            inferred = self._infer_product_from_stage()
            if inferred:
                self.product_name = inferred

        super().save(*args, **kwargs)

# --- NEW: Instrument master -----------------------------------
class QCInstrument(models.Model):
    """Master list of QC instruments used across QC forms."""
    instument_id = models.CharField(max_length=120,null=True, blank=True)
    name       = models.CharField(max_length=120,null=True, blank=True)                 # e.g., 'Gas Chromatograph'
    code       = models.CharField(max_length=50, unique=True, db_index=True,null=True, blank=True)  # e.g., 'OCSPL/QC/GC/001'
    category   = models.CharField(max_length=60,null=True, blank=True)      # e.g., 'GC', 'HPLC', etc. (optional)
    is_active  = models.BooleanField(default=True,null=True, blank=True)
    notes      = models.CharField(max_length=250,null=True, blank=True)

    class Meta:
        db_table = "qc_instrument_master"
        ordering = ["category", "name", "code"]
        indexes = [models.Index(fields=["is_active"])]

    def __str__(self) -> str:
        label = f"{self.name} – {self.code}"
        return label.strip(" –")
    
# ------------------------------------------------------------------------------------
#           Deviation
# ------------------------------------------------------------------------------------

from django.db import models
from django.utils import timezone

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────
def _fy_str(dt):
    # FY = Apr–Mar, "25-26"
    y = dt.year
    if dt.month < 4:
        y -= 1
    return f"{str(y)[-2:]}-{str(y+1)[-2:]}"

def _next_running_no(prefix: str, fy: str, qs, field: str = "deviation_no") -> str:
    """
    Generate next ID like 'DEV/25-26/00001' within the same FY on the given field.
    Explicitly uses `deviation_no` (or the provided field) and never assumes legacy names.
    """
    # Validate the target field on the model to avoid "Cannot resolve keyword"
    model = qs.model
    model_fields = {f.name for f in model._meta.get_fields() if hasattr(f, "attname")}
    if field not in model_fields:
        # Fallback to deviation_no if available, else use the model PK name (last resort)
        field = "deviation_no" if "deviation_no" in model_fields else model._meta.pk.name

    like = f"{prefix}/{fy}/"
    last = (
        qs.filter(**{f"{field}__startswith": like})
          .values_list(field, flat=True)
          .order_by("-" + field)  # string order is fine because suffix is zero-padded
          .first()
    )
    n = 1
    if last:
        try:
            n = int(str(last).rsplit("/", 1)[-1]) + 1
        except Exception:
            n = 1
    return f"{like}{n:05d}"

# ─────────────────────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────────────────────
class Deviation(models.Model):
    STATUS = [
        ("open", "Open"),
        ("progress", "In Progress"),
        ("closed", "Closed"),
    ]

    deviation_no       = models.CharField(max_length=30, unique=True, editable=False)
    date               = models.DateField(default=timezone.localdate)
    product            = models.CharField(max_length=200, blank=True)
    finished_product   = models.CharField(max_length=200, blank=True, default="")
    plant              = models.CharField(max_length=80, blank=True)
    batch_no           = models.CharField(max_length=60, blank=True)

    description        = models.TextField("Description of Deviation", blank=True)
    root_cause         = models.TextField("Root Cause", blank=True)
    corrective_action  = models.TextField(blank=True)
    preventive_action  = models.TextField(blank=True)

    status             = models.CharField(max_length=10, choices=STATUS, default="open", db_index=True)
    created_at         = models.DateTimeField(auto_now_add=True)
    updated_at         = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "qc_deviation"
        ordering = ["-date"]
        indexes = [
            models.Index(fields=["status"]),
            models.Index(fields=["product", "batch_no"]),
        ]

    def __str__(self):
        return self.deviation_no or "Deviation"

    def save(self, *args, **kwargs):
        if not self.deviation_no:
            fy = _fy_str(self.date or timezone.localdate())
            # Explicitly pass the field to avoid accidental legacy fields (e.g., am_no)
            self.deviation_no = _next_running_no("DEV", fy, type(self).objects, field="deviation_no")
        super().save(*args, **kwargs)


class AlfaProductMaster(models.Model):
    alfa_name = models.CharField(max_length=120, db_index=True, unique=True)
    finished_product_name = models.CharField(max_length=200)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "qc_alfa_product_master"
        ordering = ["alfa_name"]

    def __str__(self):
        return f"{self.alfa_name} → {self.finished_product_name}"

# -----------------------------------------------------------------
# AnalyticalMistake
# -----------------------------------------------------------------

def _fy_str(dt):
    # FY = Apr–Mar, "25-26"
    y = dt.year
    if dt.month < 4:
        y -= 1
    return f"{str(y)[-2:]}-{str(y+1)[-2:]}"

def _next_running_no(prefix, fy, qs, field="am_no"):
    like = f"{prefix}/{fy}/"
    last = (
        qs.filter(**{f"{field}__startswith": like})
          .values_list(field, flat=True)
          .order_by("-" + field)
          .first()
    )
    n = int(last.rsplit("/", 1)[-1]) + 1 if last else 1
    return f"{like}{n:05d}"

class AnalyticalMistake(models.Model):
    # NEW: FY-wise Sr. No.
    am_no = models.CharField(max_length=30, unique=True, editable=False)

    date             = models.DateField(default=timezone.localdate, db_index=True)
    product          = models.CharField(max_length=200, blank=True)
    finished_product = models.CharField(max_length=200, blank=True)
    plant            = models.CharField(max_length=80, blank=True)
    batch_no         = models.CharField(max_length=60, blank=True)

    description       = models.TextField("Description of Analytical Mistake", blank=True)
    root_cause        = models.TextField("Root Cause", blank=True)
    corrective_action = models.TextField(blank=True)
    preventive_action = models.TextField(blank=True)

    created_at       = models.DateTimeField(auto_now_add=True)
    updated_at       = models.DateTimeField(auto_now=True)

    class Meta:
        db_table  = "qc_analytical_mistake"
        ordering  = ["-date", "-created_at"]
        indexes   = [
            models.Index(fields=["date"]),
            models.Index(fields=["plant"]),
            models.Index(fields=["product", "batch_no"]),
        ]

    def __str__(self):
        return self.am_no or "Analytical Mistake"

    def save(self, *args, **kwargs):
        if not self.am_no:
            fy = _fy_str(self.date or timezone.localdate())
            self.am_no = _next_running_no("AM", fy, type(self).objects, field="am_no")
        super().save(*args, **kwargs)



#-------------------------------------------------------------------------
###########  QC Calibration     ##################################


class QCCalibrationSchedule(models.Model):
    instrument = models.ForeignKey(QCInstrument,on_delete=models.PROTECT,related_name="calibration_schedules",)
    schedule_year = models.CharField(max_length=15, blank=True, null=True,verbose_name="Schedule Year", help_text="E.g. 2026-27", )
    calibration_date = models.DateField()
    calibration_due_date = models.DateField()
    reminder_date = models.DateField()
    remarks = models.CharField(max_length=255, blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "qc_calibration_schedule"
        verbose_name = "QC Calibration Schedule"
        verbose_name_plural = "QC Calibration Schedules"
        unique_together = ("instrument", "calibration_date")
        # use instument_id here
        ordering = ["instrument__instument_id", "calibration_date"]

    def __str__(self):
        return f"{self.instrument.instument_id} - {self.calibration_date}"
    
    
    


class FGProductQCStatus(models.Model):
    date = models.DateField( default=date.today, verbose_name="Record Date",)
    product = models.CharField( Product, max_length=200)
    approved_qty = models.DecimalField(max_digits=12, decimal_places=3, default=0, verbose_name="Approved Qty", )
    off_spec_qty = models.DecimalField( max_digits=12, decimal_places=3,  default=0,verbose_name="Off Spec Qty", )
    under_analysis = models.DecimalField( max_digits=12,decimal_places=3, default=0, verbose_name="Under Analysis Qty", )
    total_qty = models.DecimalField( max_digits=12,decimal_places=3, default=0, verbose_name="Total Qty", )
    remark = models.TextField( blank=True, null=True,)

    class Meta:
        db_table = 'fg_product_qc_status'
        verbose_name = "FG Product QC Status"
        verbose_name_plural = "FG Product QC Statuses"

    def __str__(self):
        return f"{self.product} – Total: {self.total_qty}"
    
    

class InstrumentOccupancy(models.Model):
    """
    Stores occupancy of individual instruments (GC-1, GC-3, etc.).
    Matches the columns in your Excel/header: Area, Make, Model, % Occupancy, Remarks.
    """
    date = models.DateField(default=date.today, verbose_name="Date", db_index=True)
    area = models.CharField(max_length=50,verbose_name="Area",    )
    make = models.CharField(    max_length=100,  verbose_name="Make",  )
    model = models.CharField( max_length=100, verbose_name="Model",  )
    occupancy_percent = models.DecimalField(  max_digits=5,  decimal_places=2,
        verbose_name="% Occupancy", help_text="Instrument occupancy in percent (0–100).", )
    remarks = models.TextField( blank=True, null=True, verbose_name="Remarks",)

    class Meta:
        db_table = "instrument_occupancy"
        verbose_name = "Instrument Occupancy"
        verbose_name_plural = "Instrument Occupancies"
        ordering = ["area", "make", "model"]

    def __str__(self):
        return f"{self.area} – {self.make} {self.model} ({self.occupancy_percent}%)"
    
