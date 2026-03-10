from django.db import models
from django.utils import timezone
from django.contrib.auth import get_user_model
from decimal import Decimal
from django.conf import settings
from django.db import models
from django.db.models import Sum
from django.utils import timezone


User = get_user_model()
# --------------------------------------------------------
# Receivable Model
# --------------------------------------------------------
class Receivable(models.Model):
    STATUS_CHOICES = [
        ("OPEN", "Open"),
        ("PARTIAL", "Partially Received"),
        ("CLOSED", "Closed"),
    ]
    entry_date = models.DateField(default=timezone.localdate)
    customer_code = models.CharField(max_length=30, blank=True)
    customer_name = models.CharField(max_length=255)

    invoice_number = models.CharField(max_length=50)
    invoice_date = models.DateField()
    due_date = models.DateField()

    currency = models.CharField(max_length=10, default="INR")
    invoice_amount = models.DecimalField(max_digits=14, decimal_places=2)
    received_amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)

    cheque_no = models.CharField(max_length=50, blank=True, null=True)
    cheque_date = models.DateField(blank=True, null=True)

    # ✅ NEW: Instrument No (Cheque / UTR / NEFT / RTGS / etc.)
    instrument_no = models.CharField(max_length=80, blank=True, null=True, db_index=True)

    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default="OPEN")
    remarks = models.TextField(blank=True)

    days_overdue = models.IntegerField(default=0)
    aging_bucket = models.CharField(max_length=20, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL, related_name="receivables_created"
    )

    class Meta:
        db_table = "ACCOUNTS_receivable"
        ordering = ["-invoice_date", "-id"]

    def __str__(self):
        return f"{self.customer_name} / {self.invoice_number}"

    @property
    def balance_amount(self):
        return (self.invoice_amount or 0) - (self.received_amount or 0)

    def _compute_aging(self):
        today = timezone.localdate()
        self.days_overdue = (today - self.due_date).days if self.due_date else 0

        d = self.days_overdue
        if d <= 0:
            self.aging_bucket = "Not due"
        elif 1 <= d <= 30:
            self.aging_bucket = "1–30"
        elif 31 <= d <= 60:
            self.aging_bucket = "31–60"
        elif 61 <= d <= 90:
            self.aging_bucket = "61–90"
        elif 91 <= d <= 180:
            self.aging_bucket = "91–180"
        else:
            self.aging_bucket = "180+"

    def save(self, *args, **kwargs):
        self._compute_aging()

        if self.balance_amount <= 0:
            self.status = "CLOSED"
        elif 0 < self.received_amount < self.invoice_amount:
            self.status = "PARTIAL"
        else:
            self.status = "OPEN"

        super().save(*args, **kwargs)

# --------------------------------------------------------
# Payment Target Models
# --------------------------------------------------------
class PaymentTargetWeek(models.Model):
    week_start = models.DateField(db_index=True)
    week_end = models.DateField(db_index=True)

    company_group = models.CharField(max_length=20, blank=True, default="")

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="payment_targets_created"
    )
    created_at = models.DateTimeField(default=timezone.now)
    notes = models.TextField(blank=True, default="")

    class Meta:
        db_table = "ACCOUNTS_paymenttargetweek"  # ✅
        ordering = ["-week_start", "-created_at"]
        indexes = [
            models.Index(fields=["week_start", "week_end"]),
            models.Index(fields=["company_group", "week_start"]),
        ]

    def __str__(self):
        return f"Target Week {self.week_start} to {self.week_end} ({self.company_group or 'ALL'})"

    @property
    def customer(self):
        return "ALL"

    @property
    def party_display(self):
        return "ALL"

    @property
    def company_group_display(self):
        return self.company_group or "ALL"

    @property
    def expected_total(self):
        return self.lines.aggregate(s=Sum("expected_amount"))["s"] or Decimal("0")

    @property
    def outstanding_total(self):
        return self.lines.aggregate(s=Sum("outstanding_amount"))["s"] or Decimal("0")


class PaymentTargetLine(models.Model):
    target = models.ForeignKey(PaymentTargetWeek, on_delete=models.CASCADE, related_name="lines")

    party_code = models.CharField(max_length=50, db_index=True)
    party_name = models.CharField(max_length=255, db_index=True)

    invoice_no = models.CharField(max_length=80, blank=True, default="")
    invoice_date = models.CharField(max_length=50, blank=True, default="")
    due_date = models.CharField(max_length=50, blank=True, default="")

    bill_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    outstanding_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    expected_amount = models.DecimalField(max_digits=18, decimal_places=2, default=0)

    promised_date = models.DateField(null=True, blank=True)
    discussion_notes = models.TextField(blank=True, default="")
    followup_owner = models.CharField(max_length=120, blank=True, default="")
    status = models.CharField(
        max_length=30, default="Open",
        choices=[("Open","Open"),("Part Received","Part Received"),("Received","Received"),("Deferred","Deferred")]
    )

    created_at = models.DateTimeField(default=timezone.now, db_index=True)

    class Meta:
        db_table = "ACCOUNTS_paymenttargetline"  # ✅
        indexes = [
            models.Index(fields=["target", "party_code"]),
            models.Index(fields=["target", "invoice_no"]),
            models.Index(fields=["party_code", "invoice_no"]),
        ]
        constraints = [
            models.UniqueConstraint(fields=["target", "party_code", "invoice_no"], name="uq_target_party_invoice")
        ]

    def __str__(self):
        return f"{self.party_name} | {self.invoice_no} | Exp: {self.expected_amount}"

    @property
    def bill_key(self):
        return f"{self.party_code}||{self.invoice_no}"

#  --------------------------------------------------------
#  Party and PartyContact models
# ------------------------------------------------------------------
class Party(models.Model):
    party_code = models.CharField(max_length=50, unique=True, db_index=True)
    party_name = models.CharField(max_length=255, db_index=True)

    gst_no = models.CharField(max_length=30, blank=True, default="")
    address = models.TextField(blank=True, default="")
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "ACCOUNTS_party"  # ✅

    def __str__(self):
        return f"{self.party_name} ({self.party_code})"


class PartyContact(models.Model):
    party = models.ForeignKey(Party, on_delete=models.CASCADE, related_name="contacts")

    contact_person = models.CharField(max_length=120, blank=True, default="")
    designation = models.CharField(max_length=120, blank=True, default="")

    email = models.EmailField(blank=True, default="")
    mobile = models.CharField(max_length=30, blank=True, default="")
    phone = models.CharField(max_length=30, blank=True, default="")

    is_primary = models.BooleanField(default=False)
    receive_pdc_reminder = models.BooleanField(default=True)
    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "ACCOUNTS_partycontact"  # ✅ important
        indexes = [models.Index(fields=["email"])]

    def __str__(self):
        return f"{self.party.party_code} | {self.contact_person} | {self.email}"
    
#-------------------------------------------------------- 
# accounts/models_mail.py
#--------------------------------------------------------
class OutgoingEmailAccount(models.Model):
    company_group = models.CharField(max_length=40, default="ALL", db_index=True)

    from_email = models.EmailField()
    from_name = models.CharField(max_length=120, blank=True, default="Accounts Team")

    use_custom_smtp = models.BooleanField(default=False)
    host = models.CharField(max_length=200, blank=True, default="")
    port = models.IntegerField(null=True, blank=True)
    username = models.CharField(max_length=200, blank=True, default="")
    password = models.CharField(max_length=200, blank=True, default="")
    use_tls = models.BooleanField(default=True)
    use_ssl = models.BooleanField(default=False)

    is_active = models.BooleanField(default=True)

    class Meta:
        db_table = "ACCOUNTS_outgoingemailaccount"  # ✅
        constraints = [
            models.UniqueConstraint(fields=["company_group", "from_email"], name="uq_outgoing_email_account_group_email")
        ]

    def __str__(self):
        return f"{self.company_group} | {self.from_email}"    
# --------------------------------------------------------
# accounts/models_pdc.py
# --------------------------------------------------------
class PDCReminderLog(models.Model):
    receivable = models.ForeignKey(
        "receivable.Receivable",
        on_delete=models.CASCADE,
        related_name="pdc_reminders",
    )
    reminder_date = models.DateField(db_index=True)
    sent_to = models.TextField(blank=True, default="")
    subject = models.CharField(max_length=255, blank=True, default="")
    status = models.CharField(max_length=20, default="Sent", choices=[("Sent","Sent"),("Failed","Failed")])
    error = models.TextField(blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL)

    class Meta:
        db_table = "ACCOUNTS_pdcreminderlog"  # ✅
        constraints = [
            models.UniqueConstraint(fields=["receivable", "reminder_date"], name="uq_pdc_reminder_receivable_date")
        ]

# --------------------------------------------------------
# ACCOUNTS/models.py
# --------------------------------------------------------
from django.db import models


class ReceivableSnapshotRow(models.Model):
    snapshot_date = models.DateField(db_index=True)

    erp_lid = models.BigIntegerField(null=True, blank=True, db_index=True)
    erp_acc_id = models.BigIntegerField(null=True, blank=True, db_index=True)
    erp_comp_id = models.IntegerField(null=True, blank=True, db_index=True)
    erp_typ_id = models.IntegerField(null=True, blank=True, db_index=True)

    company_name = models.CharField(max_length=255, blank=True, db_index=True)
    party_code = models.CharField(max_length=60, blank=True, db_index=True)
    party_name = models.CharField(max_length=255, blank=True, db_index=True)

    trans_type = models.CharField(max_length=120, blank=True)
    trans_no = models.CharField(max_length=120, blank=True, db_index=True)

    # ✅ NEW: Instrument No (from ERP receipt/ref/utr etc.)
    instrument_no = models.CharField(max_length=120, blank=True, default="", db_index=True)

    trans_date_display = models.CharField(max_length=40, blank=True)
    due_date_display = models.CharField(max_length=40, blank=True)
    overdue_date_display = models.CharField(max_length=40, blank=True)

    trans_date = models.DateField(null=True, blank=True, db_index=True)
    due_date = models.DateField(null=True, blank=True, db_index=True)
    overdue_date = models.DateField(null=True, blank=True, db_index=True)

    bill_amt = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    paid_amt = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    outstanding_amt = models.DecimalField(max_digits=18, decimal_places=2, default=0)

    item_name = models.CharField(max_length=255, blank=True)
    location = models.CharField(max_length=255, blank=True)

    customer_po_no = models.CharField(max_length=120, blank=True, db_index=True)
    customer_po_date_display = models.CharField(max_length=40, blank=True)
    customer_po_date = models.DateField(null=True, blank=True, db_index=True)

    raw = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "ACCOUNTS_receivablesnapshotrow"
        indexes = [
            models.Index(fields=["snapshot_date", "party_code"]),
            models.Index(fields=["snapshot_date", "party_name"]),
            models.Index(fields=["snapshot_date", "trans_no"]),
            models.Index(fields=["snapshot_date", "company_name"]),

            models.Index(fields=["snapshot_date", "party_code", "trans_no"], name="ix_rcv_snap_pc_inv"),
            models.Index(fields=["snapshot_date", "paid_amt"], name="ix_rcv_snap_paid"),
            models.Index(fields=["snapshot_date", "outstanding_amt"], name="ix_rcv_snap_os"),
            models.Index(fields=["snapshot_date", "trans_date"], name="ix_rcv_snap_transdt"),
            models.Index(fields=["snapshot_date", "due_date"], name="ix_rcv_snap_duedt"),
            models.Index(fields=["snapshot_date", "overdue_date"], name="ix_rcv_snap_overduedt"),
            models.Index(fields=["snapshot_date", "customer_po_no"], name="ix_rcv_snap_pono"),
            models.Index(fields=["snapshot_date", "customer_po_date"], name="ix_rcv_snap_podt"),

            # ✅ NEW index for Instrument No (optional but useful for searching/matching)
            models.Index(fields=["snapshot_date", "instrument_no"], name="ix_rcv_snap_instno"),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["snapshot_date", "erp_lid", "party_code", "trans_no"],
                name="uq_receivable_snapshotrow",
            )
        ]

    def __str__(self):
        return f"{self.snapshot_date} | {self.party_code} | {self.trans_no}"

# --------------------------------------------------------
# accounts/models_outstanding.py
# --------------------------------------------------------
    
class ReceivableOutstandingRemark(models.Model):
    """
    Stores user remarks line-wise for an outstanding bill row.
    Keyed by snapshot_date + party_code + invoice_no (stable + reproducible).
    """
    snapshot_date = models.DateField(db_index=True)
    party_code = models.CharField(max_length=50, db_index=True)
    invoice_no = models.CharField(max_length=100, db_index=True)

    company_name = models.CharField(max_length=255, blank=True, default="")
    party_name = models.CharField(max_length=255, blank=True, default="")

    # ✅ Display label updated (DB column remains `remark`)
    remark = models.TextField(
        blank=True,
        default="",
        verbose_name="Remark by the user for the customer",
        help_text="Internal follow-up remark entered by the user for this customer/invoice.",
    )

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="outstanding_remarks_created",
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="outstanding_remarks_updated",
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ("snapshot_date", "party_code", "invoice_no")
        indexes = [
            models.Index(fields=["snapshot_date", "party_code"]),
            models.Index(fields=["snapshot_date", "invoice_no"]),
        ]

    def __str__(self):
        return f"{self.snapshot_date} | {self.party_code} | {self.invoice_no}"
