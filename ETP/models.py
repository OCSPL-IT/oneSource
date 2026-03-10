from django.db import models
from datetime import date
from django.core.validators import MinValueValidator


class EffluentRecord(models.Model):
    id = models.AutoField(primary_key=True)
    record_date = models.DateField(default=date.today)
    product_name = models.CharField(max_length=200, null=True, blank=True)
    stage_name = models.CharField(max_length=255, null=True, blank=True)  # New field
    batch_no = models.CharField(max_length=50, null=True, blank=True)  # New field (nullable)
    voucher_no = models.CharField(max_length=50, null=True, blank=True)
    block = models.CharField(max_length=50, null=True, blank=True)

    class Meta:
        db_table = 'effluent_records'

class EffluentQty(models.Model):
    id = models.AutoField(primary_key=True)
    effluent_record = models.ForeignKey(EffluentRecord, on_delete=models.CASCADE, related_name="qty_units")
    category = models.CharField(max_length=20)
    effluent_nature = models.CharField(max_length=100, null=True, blank=True)
    plan_quantity = models.FloatField(default=0.0,null=True, blank=True)
    actual_quantity = models.FloatField(default=0.0,null=True, blank=True)
    quantity_kg  = models.FloatField(default=0.0,null=True, blank=True)

    class Meta:
        db_table = 'effluent_qty'

class GeneralEffluent(models.Model):
    id = models.AutoField(primary_key=True)
    record_date = models.DateField(default=date.today)
    location = models.CharField(max_length=50, null=True, blank=True)
    effluent_nature = models.CharField(max_length=100, null=True, blank=True)
    actual_quantity = models.FloatField(default=0.0)

    class Meta:
        db_table = 'general_effluent'




class ProductionSchedule(models.Model):
    id                  = models.AutoField(primary_key=True)
    doc_no              = models.CharField(max_length=50, unique=True)
    type                = models.CharField(max_length=50, blank=True, null=True)
    bom_id              = models.CharField(max_length=50, blank=True, null=True)
    product_id          = models.CharField(max_length=255)  # FK to your Product master
    stage_name          = models.CharField(max_length=100, blank=True, null=True)
    block               = models.CharField(max_length=50, blank=True, null=True)
    production_quantity = models.DecimalField(max_digits=18, decimal_places=2)
    equipment_id        = models.CharField(max_length=50, blank=True, null=True)
    equipment_capacity  = models.DecimalField(max_digits=18, decimal_places=2, blank=True, null=True)
    bct_in_hrs          = models.DecimalField(max_digits=18, decimal_places=2, blank=True, null=True)
    no_of_batches       = models.DecimalField(max_digits=18, decimal_places=2, blank=True, null=True)
    batch_size          = models.DecimalField(max_digits=18, decimal_places=2, default=0)
    start_date          = models.DateTimeField()
    end_date            = models.DateTimeField()
    wait_time           = models.DecimalField(max_digits=18, decimal_places=2, blank=True, null=True)
    batch_number        = models.CharField(max_length=50, blank=True, null=True)
    scheduling_approach = models.IntegerField(default=0)
    bom_name            = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        db_table = "production_schedule"
        managed = False  # because you’re using an external DB/router

class ProductionScheduleLine(models.Model):
    id                = models.AutoField(primary_key=True)
    schedule          = models.ForeignKey(
        ProductionSchedule,
        on_delete=models.CASCADE,
        db_column="schedule_id",
        related_name="lines"
    )
    line_type         = models.CharField(max_length=20)   # 'input'/'output'/'waste'/'equipment'
    material_category = models.CharField(max_length=50, blank=True, null=True)
    material_name     = models.CharField(max_length=100, blank=True, null=True)
    quantity          = models.FloatField(default=0.0)
    ratio             = models.FloatField(default=1.0)
    density           = models.FloatField(default=0.0)
    litre             = models.FloatField(default=0.0)
    include_in_total  = models.BooleanField(default=True)
    closed            = models.BooleanField(default=False)
    closed_date       = models.DateTimeField(blank=True, null=True)

    equipment_id      = models.CharField(max_length=50, blank=True, null=True)
    std_bct           = models.FloatField(default=0.0)
    wait_time         = models.FloatField(default=0.0)
    equipment_type    = models.CharField(max_length=50, blank=True, null=True)
    capacity_size     = models.CharField(max_length=50, blank=True, null=True)
    moc_equipment     = models.CharField(max_length=50, blank=True, null=True)
    star              = models.BooleanField(default=False)

    class Meta:
        db_table = "production_schedule_lines"
        managed = False


# -------------------- Primary Treatment Effluent ----------------------------------

# New Model for Primary Treatment Effluent
class PrimaryTreatmentEffluent(models.Model):
    EFFLUENT_NATURE_CHOICES = [
        ('Scrubber Acidic Effluent', 'Scrubber Acidic Effluent'),
        ('Spent HCL', 'Spent HCL'),
        ('Acidic', 'Acidic'),
        ('Acidic Aq. Layer', 'Acidic Aq. Layer'),
        ('Basic', 'Basic'), 
        ('Residue', 'Residue'),
        ('Sodium Cyanide Effluent', 'Sodium Cyanide Effluent'),
        ('Sulphuric above 50% effluent', 'Sulphuric above 50% effluent'),
        ('Sulphuric below 50 % effluent', 'Sulphuric below 50 % effluent'),
    ]


    date = models.DateField()
    effluent_nature = models.CharField(max_length=100, choices=EFFLUENT_NATURE_CHOICES)
    effluent_received = models.DecimalField(max_digits=10, decimal_places=2, default=0.0)
    effluent_neutralized = models.DecimalField(max_digits=10, decimal_places=2, default=0.0)

    class Meta:
        db_table = 'primary_treatment_effluent'
        verbose_name = "Primary Treatment Effluent"
        verbose_name_plural = "Primary Treatment Effluents"

    def __str__(self):
        return f"{self.record_date} - {self.get_effluent_nature_display()}"
    
    
# NEW Model to store chemical usage with quantities
class PrimaryTreatmentChemical(models.Model):
    # Link to the main effluent record
    effluent_record = models.ForeignKey(
        PrimaryTreatmentEffluent, 
        on_delete=models.CASCADE, 
        related_name="chemicals_used"
    )
    chemical_name = models.CharField(max_length=100)
    quantity = models.DecimalField(max_digits=10, decimal_places=2)

    class Meta:
        db_table = 'primary_treatment_chemical'
        verbose_name = "Primary Treatment Chemical"
        verbose_name_plural = "Primary Treatment Chemicals"

    def __str__(self):
        return f"{self.chemical_name} ({self.quantity})"
    
    
# --------------------Hazardous Waste ----------------------------------    

DISPOSAL_METHOD_CHOICES = [
    ("Pre-Processing", "Pre-Processing"),
    ("Incineration", "Incineration"),
    ("Land filling", "Land filling"),
    ("Recycle", "Recycle"),
    ("For Further Treatment","For Further Treatment"),
    ("Other", "Other"),
]

class HazardousWaste(models.Model):
    date = models.DateField(help_text="Record date")
    challan_no = models.CharField(max_length=50)
    manifest_no = models.CharField(max_length=50, blank=True, null=True)
    transporter_name = models.CharField(max_length=255, verbose_name="Name of the Transporter")
    vehicle_registration_numbers = models.TextField(
        help_text="Vehicle registration number(s). Use one per line if multiple.",
        verbose_name="Vehicle Registration Number"  )
    type_of_waste = models.TextField(
        help_text="e.g., 20.3 Distillation Residues; 28.1 Process Residue and wastes"  )
    waste_category = models.CharField(max_length=20, blank=True, null=True,
        help_text="e.g., 20.3 / 28.1 / 35.3" )
    quantity_mt = models.DecimalField(
        max_digits=12, decimal_places=3, validators=[MinValueValidator(0)],
        verbose_name="Quantity of Waste (MT)" )
    disposal_rate_rs_per_mt = models.DecimalField(
        max_digits=12, decimal_places=2, validators=[MinValueValidator(0)],
        verbose_name="Waste Disposal Rate (Rs/MT)"   )
    transportation_cost = models.DecimalField(
        max_digits=12, decimal_places=2, validators=[MinValueValidator(0)],
        blank=True, null=True  )
    total_cost = models.DecimalField(
        max_digits=12, decimal_places=2, validators=[MinValueValidator(0)],
        blank=True, null=True  )
    disposal_method = models.CharField(
        max_length=100, choices=DISPOSAL_METHOD_CHOICES, blank=True, null=True )
    disposal_facility = models.CharField(max_length=255)
    license_valid_upto = models.DateField(blank=True, null=True, verbose_name="License Validity (upto)")

    class Meta:
        db_table = "hazardous_waste"
        verbose_name = "Hazardous Waste"
        verbose_name_plural = "Hazardous Waste"
        indexes = [
            models.Index(fields=["date"]),
            models.Index(fields=["challan_no"]),
            models.Index(fields=["manifest_no"]),
            models.Index(fields=["transporter_name"]),
        ]

    def __str__(self):
        return f"{self.challan_no} · {self.transporter_name} ({self.date})"


# --------------------Effluent Storage Tank ----------------------------------

class EffluentTank(models.Model):
    name     = models.CharField(max_length=100, unique=True)
    capacity = models.DecimalField(max_digits=10, decimal_places=2,
                                   validators=[MinValueValidator(0)])

    class Meta:
        db_table = "effluent_tank"
        ordering = ["name"]

    def __str__(self): return f"{self.name} ({self.capacity})"


class EffluentOpeningBalance(models.Model):
    """
    One row per tank per month (use month as the month's first day).
    """
    tank   = models.ForeignKey(EffluentTank, on_delete=models.CASCADE, related_name="opening_balances")
    month  = models.DateField(help_text="First day of month (YYYY-MM-01)")
    opening_balance = models.DecimalField(max_digits=10, decimal_places=2, validators=[MinValueValidator(0)])

    class Meta:
        db_table = "Effluent_opening_balance"
        unique_together = (("tank", "month"),)
        indexes = [models.Index(fields=["tank", "month"])]

    def __str__(self):
        return f"{self.tank.name} @ {self.month:%Y-%m} = {self.opening_balance}"
    