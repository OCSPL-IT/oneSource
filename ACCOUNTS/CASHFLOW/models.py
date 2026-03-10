# ACCOUNTS/models.py
from django.db import models
from django.contrib.auth.models import User

class ManualPayableEntry(models.Model):
    NATURE_CHOICES = [
        # ---------------- OTHER ----------------
        ("ADVANCE PAYMENT - CAPEX/RM", "ADVANCE PAYMENT - CAPEX/RM"),
        ("SALARY & WAGES", "SALARY & WAGES"),
        ("BONUS", "BONUS"),
        ("PAYMENT TO LABOUR CONTRACTORS", "PAYMENT TO LABOUR CONTRACTORS"),
        ("PAYMENT TO VEHICLE RENTAL", "PAYMENT TO VEHICLE RENTAL"),
        ("ELECTRICITY CHARGES", "ELECTRICITY CHARGES"),
        ("WATER CHARGES", "WATER CHARGES"),
        ("JOB WORK NOT BILLED/BOOKED YET", "JOB WORK NOT BILLED/BOOKED YET"),
        ("INSURANCE", "INSURANCE"),

        # ---------------- TAXES ----------------
        ("TDS & TCS", "TDS & TCS"),
        ("GST (MONTHLY RETURN)", "GST (MONTHLY RETURN)"),
        ("CUSTOM DUTY + IGST", "CUSTOM DUTY + IGST"),
        ("ADVANCE TAX", "ADVANCE TAX"),

        # ---------------- FINANCING ----------------
        ("INTEREST PAYMENTS", "INTEREST PAYMENTS"),
        ("LOAN RE-PAYMENT", "LOAN RE-PAYMENT"),
    ]

    COMPANY_GROUP_CHOICES = [
        ("specialities", "OC Specialities Private Limited"),
        ("chemicals", "OC Specialities Chemicals Private Limited"),
    ]

    company_group = models.CharField(
        max_length=20,
        choices=COMPANY_GROUP_CHOICES,
        blank=True,
        null=True,
        help_text="Optional: link to cashflow Company Group filter",
    )

    # ✅ increased max_length to safely fit all strings
    nature = models.CharField(max_length=80, choices=NATURE_CHOICES)

    due_date = models.DateField()
    amount = models.DecimalField(max_digits=18, decimal_places=2)
    remarks = models.CharField(max_length=200, blank=True)

    created_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "manual_payable_entry"
        ordering = ["-due_date", "-id"]

    def __str__(self):
        return f"{self.nature} - {self.due_date} - {self.amount}"



from django.conf import settings
from django.db import models

def _norm_party_name(x: str) -> str:
    return " ".join((x or "").strip().upper().split())

class PayablePartyExtension(models.Model):
    """
    Stores user-defined "extend due date by N days" for a payable party.
    Applies at report time only (no ERP updates).
    """
    party_name = models.CharField(max_length=255)
    party_norm = models.CharField(max_length=255, db_index=True, editable=False)

    # If set, apply only when company filter matches this key (e.g., "specialities"/"chemicals").
    # If blank/null => applies for all.
    company_group = models.CharField(max_length=30, blank=True, null=True)

    extend_days = models.IntegerField(default=0)  # e.g. +10
    active = models.BooleanField(default=True)

    remarks = models.CharField(max_length=250, blank=True, default="")

    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, blank=True
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "ACCOUNTS_payablepartyextension"   # 👈 IMPORTANT
        indexes = [
            models.Index(fields=["party_norm", "company_group", "active"]),
        ]

    def save(self, *args, **kwargs):
        self.party_norm = _norm_party_name(self.party_name)
        super().save(*args, **kwargs)

    def __str__(self):
        scope = self.company_group or "ALL"
        sign = "+" if (self.extend_days or 0) >= 0 else ""
        return f"{self.party_name} ({scope}) {sign}{self.extend_days}d"
