from django.db import models
from django.utils import timezone
from django.core.validators import RegexValidator


class Credentials(models.Model):
    LOCATION_CHOICES = [
        ('head_office', 'Head Office'),
        ('solapur', 'Solapur'),
    ]

    location = models.CharField(max_length=50, choices=LOCATION_CHOICES, blank=True, null=True)
    device = models.CharField(max_length=255, blank=True, null=True)
    lan_ip = models.GenericIPAddressField(blank=True, null=True)
    wan_ip = models.GenericIPAddressField(blank=True, null=True)
    port_no = models.PositiveIntegerField(blank=True, null=True)
    frwd_to = models.CharField(max_length=255, blank=True, null=True)
    url = models.URLField(blank=True, null=True)
    user_name = models.CharField(max_length=150, blank=True, null=True)
    old_password = models.CharField(max_length=255, blank=True, null=True)
    new_password = models.CharField(max_length=255, blank=True, null=True)

    status_CHOICES = [
        ('reset', 'Reset'),
        ('created', 'Created'),
    ]
    status = models.CharField(max_length=20, choices=status_CHOICES, blank=True, null=True)
    action_date = models.DateTimeField(blank=True, null=True)
    expiry_on = models.DateField(blank=True, null=True)

    def save(self, *args, **kwargs):
        if self.pk:
            orig = Credentials.objects.filter(pk=self.pk).first()
            if orig and orig.status != self.status:
                self.action_date = timezone.now()
        else:
            if self.status and not self.action_date:
                self.action_date = timezone.now()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.location} - {self.device} - {self.user_name}"

    class Meta:
        db_table = 'credentials'
        
        
        


# ── Below code for Extension List ────────────────────────────────────────────────────────────────


class ExtentionList(models.Model):
    # ── Choices ────────────────────────────────────────────────────────────────
    LOCATION_CHOICES = [
        ("Solapur", "Solapur"),
        ("Mumbai", "Mumbai"),
        ("Virar", "Virar"),
        ("Vasai", "Vasai"),
    ]

    DEPARTMENT_CHOICES = [
        ("Directors", "Directors"),("Board/Reception( Office 205)", "Board/Reception( Office 205)"),("Reception(Office 206)", "Reception(Office 206)"),
        ("Executive Asst.", "Executive Asst."),("Business Development", "Business Development"),("Operation", "Operation"),
        ("Personal Care", "Personal Care"),("Sales", "Sales"),("Purchase", "Purchase"),("Dispatch", "Dispatch"),("Finance & Account", "Finance & Account"),
        ("Import & Export", "Import & Export"),("Logistics", "Logistics"),("HR & Admin", "HR & Admin"),("IT", "IT"),
        ("Office Boy", "Office Boy"),("QA & QC", "QA & QC"),("R & D", "R & D"),("Engineering", "Engineering"),("Production", "Production"),
        ("Production Block", "Production Block"),("EHS", "EHS"),("Intstrumentation", "Intstrumentation"),("Store", "Store"),("Marketing", "Marketing"),("Project", "Project"),
        ("Utility", "Utility"),("E-17 Reception", "E-17 Reception"),("Security", "Security"),("Conference Room", "Conference Room"),
		("Technical Services", "Technical Services"),("Maintenance", "Maintenance"),("Electric", "Electric"),("Boiler", "Boiler"),("Pantry", "Pantry"),("Others", "Others"),]
    
    DESIGNATION_CHOICES = [
        ("Director", "Director"),("President", "President"),("Sr.Vice President", "Sr.Vice President"),("Vice President", "Vice President"),("Asst. Vice President", "Asst. Vice President"),("Asst. General Manager", "Asst. General Manager"),
        ("Sr. Manager", "Sr. Manager"),("Manager", "Manager"),("Asst. Manager", "Asst. Manager"),("Sr. Manager Finance & Accounts", "Sr. Manager Finance & Accounts"),("Research Scientist", "Research Scientist"),
        ("Officer", "Officer"),("Sr. Executive", "Sr. Executive"),("Supervisor", "Supervisor"),("Guards", "Guards"),
		("Sr.Supervisor", "Sr.Supervisor"),("Block", "Block"),("Executive", "Executive"),("Dy. Manager", "Dy. Manager"),("Jr.Officer", "Jr.Officer"),("jr.Executive", "jr.Executive"),("Sr.Executive", "Sr.Executive"),("Draftsman", "Draftsman"),
		("Jr.Officer", "Jr.Officer"),("Sr.Officer", "Sr.Officer"),("Store Associate", "Store Associate"),("General Manager", "General Manager"),("Doctor", "Doctor"),("Assistant", "Assistant"),("Office Boy", "Office Boy"),("None", "None"),
    ]

    # ── Fields ────────────────────────────────────────────────────────────────
    name = models.CharField(max_length=120)
    department = models.CharField(max_length=40, choices=DEPARTMENT_CHOICES)
    designation = models.CharField(
        max_length=30,
        choices=DESIGNATION_CHOICES,
        null=True,
        blank=True,
    )
    extension_no = models.CharField(
        "Extension No.",
        max_length=20,
        help_text="Enter one or two extensions, separated by comma (e.g., 205,206)",
        null=True,
        blank=True,
    )
    mobile = models.CharField(
        max_length=15,
        validators=[RegexValidator(r"^\+?\d{10,15}$", "Enter a valid phone number")],
        help_text="10–15 digits, optional +",
        null=True,
        blank=True
    )
    location = models.CharField(max_length=10, choices=LOCATION_CHOICES)

    class Meta:
        db_table = 'extension_list'
        ordering = ["name"]
        verbose_name = "Extension List"
        verbose_name_plural = "Extension List"
        

    def __str__(self):
        return f"{self.name} · {self.department}"
