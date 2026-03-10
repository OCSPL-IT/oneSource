from django.db import models
from django.contrib.auth import get_user_model

User = get_user_model()


class LeadStatus(models.TextChoices):
    NEW = "NEW", "New"
    IN_PROGRESS = "IN_PROGRESS", "In Progress"
    QUALIFIED = "QUALIFIED", "Qualified"
    WON = "WON", "Won"
    LOST = "LOST", "Lost"


class LeadSource(models.TextChoices):
    CALL = "CALL", "Call"
    EMAIL = "EMAIL", "Email"
    WEBSITE = "WEBSITE", "Website"
    REFERRAL = "REFERRAL", "Referral"
    OTHER = "OTHER", "Other"


class SalesLead(models.Model):
    # Core
    name = models.CharField(max_length=200)
    company = models.CharField(max_length=200, blank=True, default="")
    phone = models.CharField(max_length=50, blank=True, default="")
    email = models.EmailField(blank=True, default="")
    city = models.CharField(max_length=120, blank=True, default="")
    state = models.CharField(max_length=120, blank=True, default="")

    status = models.CharField(max_length=20, choices=LeadStatus.choices, default=LeadStatus.NEW)
    source = models.CharField(max_length=20, choices=LeadSource.choices, default=LeadSource.OTHER)

    expected_value = models.DecimalField(max_digits=18, decimal_places=2, null=True, blank=True)
    remarks = models.TextField(blank=True, default="")

    # Ownership + timestamps
    assigned_to = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL, related_name="crm_assigned_leads"
    )
    created_by = models.ForeignKey(
        User, null=True, blank=True, on_delete=models.SET_NULL, related_name="crm_created_leads"
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-updated_at", "-id"]
        indexes = [
            models.Index(fields=["status"]),
            models.Index(fields=["assigned_to"]),
            models.Index(fields=["created_at"]),
        ]
        # ✅ Permission for sidebar + access control
        permissions = [
            ("access_sales_crm", "Can access Sales CRM"),
        ]

    def __str__(self):
        return f"{self.name} ({self.company})" if self.company else self.name


class LeadFollowUp(models.Model):
    lead = models.ForeignKey(SalesLead, on_delete=models.CASCADE, related_name="followups")
    note = models.TextField()
    next_date = models.DateField(null=True, blank=True)

    created_by = models.ForeignKey(User, null=True, blank=True, on_delete=models.SET_NULL)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at", "-id"]

    def __str__(self):
        return f"FollowUp #{self.id} - {self.lead_id}"
    



from django.db import models


class Customer(models.Model):
    name = models.CharField(max_length=200, unique=True)

    def __str__(self):
        return self.name


class Product(models.Model):
    name = models.CharField(max_length=200, unique=True)

    def __str__(self):
        return self.name


class Industry(models.Model):
    name = models.CharField(max_length=200, unique=True)

    def __str__(self):
        return self.name


class SalesPerson(models.Model):
    name = models.CharField(max_length=200, unique=True)

    def __str__(self):
        return self.name


class CustomerVisit(models.Model):
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
    purchaser_name = models.CharField(max_length=200)
    cell_no = models.CharField(max_length=20)
    direct_ll_no = models.CharField(max_length=20, blank=True, null=True)
    email = models.EmailField()

    product = models.ForeignKey(Product, on_delete=models.CASCADE)
    quantity = models.IntegerField(null=True, blank=True)
    industry = models.ForeignKey(Industry, on_delete=models.CASCADE)

    designation = models.CharField(max_length=200)
    visit_date = models.DateField()
    remark = models.TextField()

    sales_person = models.ForeignKey(SalesPerson, on_delete=models.CASCADE)

    def __str__(self):
        return f"{self.customer} - {self.visit_date}"



from django.db import models

class FollowUp(models.Model):

    FOLLOWUP_TYPE = [
        ("Call", "Call"),
        ("Meeting", "Meeting"),
        ("Email", "Email"),
        ("WhatsApp", "WhatsApp"),
    ]

    STATUS_CHOICES = [
        ("Pending", "Pending"),
        ("Done", "Done"),
    ]

    visit = models.ForeignKey(
        "CustomerVisit",
        on_delete=models.CASCADE,
        related_name="followups"
    )

    followup_date = models.DateField()
    followup_type = models.CharField(max_length=20, choices=FOLLOWUP_TYPE)
    notes = models.TextField(blank=True)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default="Pending")

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.visit.customer} - {self.followup_date}"




from django.db import models
from django.contrib.auth.models import User

class Task(models.Model):

    PRIORITY_CHOICES = [
        ("Low", "Low"),
        ("Medium", "Medium"),
        ("High", "High"),
    ]

    STATUS_CHOICES = [
        ("Pending", "Pending"),
        ("In Progress", "In Progress"),
        ("Completed", "Completed"),
    ]

    visit = models.ForeignKey(
        "CustomerVisit",
        on_delete=models.CASCADE,
        related_name="tasks"
    )

    title = models.CharField(max_length=200)
    description = models.TextField(blank=True)

    assigned_to = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )

    due_date = models.DateField()
    priority = models.CharField(max_length=10, choices=PRIORITY_CHOICES, default="Medium")
    status = models.CharField(max_length=15, choices=STATUS_CHOICES, default="Pending")

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.title} - {self.visit.customer}"