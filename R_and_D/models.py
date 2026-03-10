from django.db import models
from django.core.validators import RegexValidator
import re

MP_PATTERN = r"^\s*\d+(\.\d+)?(\s*-\s*\d+(\.\d+)?)?\s*$"  # "101" or "101-102" or "101.5-102.2"



class RDMaster(models.Model):
    CATEGORY_CHOICES = [
        ('Analyst', 'Analyst'),
        ('Unit', 'Unit'),
        ('Instrument', 'Instrument'),
    ]
    category = models.CharField(max_length=20, choices=CATEGORY_CHOICES)
    name = models.CharField(max_length=200)

    class Meta:
        db_table = 'r_and_d_master'
        unique_together = ('category', 'name')
        verbose_name = "R & D Master"

    def __str__(self):
        return f"{self.category}: {self.name}"



class R_and_D_Moisture(models.Model):
    entry_date = models.DateField(verbose_name="Entry Date")
    entry_time = models.TimeField(verbose_name="Entry Time")
    eln_id = models.CharField(max_length=50, verbose_name="ELN ID", blank=True, null=True)
    # ForeignKey for Product (category=Product)
    product_name = models.CharField(max_length=100, verbose_name="Product Name")
    batch_no = models.CharField(max_length=100, verbose_name="Batch No")
    sample_description = models.CharField(max_length=255, verbose_name="Sample Description")
    # ForeignKey for Unit (category=Unit)
    unit = models.ForeignKey(
        RDMaster,
        limit_choices_to={'category': 'Unit'},
        related_name='moisture_units',
        on_delete=models.PROTECT,
        verbose_name="Unit",
        blank=True, null=True
    )
    # ForeignKey for Instrument (category=Instrument)
    instrument = models.ForeignKey(
        RDMaster,
        limit_choices_to={'category': 'Instrument'},
        related_name='moisture_instruments',
        on_delete=models.PROTECT,
        verbose_name="Instrument"
    )
    factor_mg_per_ml = models.DecimalField(max_digits=10, decimal_places=4, verbose_name="Factor (mg/mL)")
    sample_weight_gm = models.DecimalField(max_digits=10, decimal_places=5, verbose_name="Sample Weight (mg)")
    burette_reading_ml = models.DecimalField(max_digits=10, decimal_places=4, verbose_name="Burette Reading (mL)")
    moisture_percent = models.DecimalField(max_digits=6, decimal_places=2, verbose_name="Moisture (%)")
    # ForeignKey for Analyst (category=Analyst)
    analysed_by = models.ForeignKey(
        RDMaster,
        limit_choices_to={'category': 'Analyst'},
        related_name='moisture_analysts',
        on_delete=models.PROTECT,
        verbose_name="Analysed By"
        , blank=True, null=True
    )
    completed_date = models.DateField(verbose_name="Completed Date", blank=True, null=True)
    completed_time = models.TimeField(verbose_name="Completed Time", blank=True, null=True)

    class Meta:
        db_table = 'r_and_d_moisture'
        verbose_name = "R&D Moisture"
        verbose_name_plural = "R&D Moisture"

    def __str__(self):
        return f"{self.product_name} - {self.entry_date}"



class KFFactorEntry(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)  # Auto (don't show in form)
    instrument = models.ForeignKey(
        RDMaster,
        limit_choices_to={'category': 'Instrument'},
        related_name='kf_entries_instrument',  # Unique related_name
        on_delete=models.PROTECT
    )
    analysed_by = models.ForeignKey(
        RDMaster,
        limit_choices_to={'category': 'Analyst'},
        related_name='kf_entries_analysed',    # Unique related_name
        on_delete=models.PROTECT,
        blank=True, null=True
    )
    # You can add other header fields here
    class Meta:
        db_table = 'kf_factor'

    def __str__(self):
        return f"{self.instrument} | {self.analysed_by} | {self.created_at.date()}"


class KFFactorEntryLine(models.Model):
    entry = models.ForeignKey(KFFactorEntry, related_name='lines', on_delete=models.CASCADE)
    sample_weight_mg = models.DecimalField(max_digits=10, decimal_places=4)
    burette_reading_ml = models.DecimalField(max_digits=10, decimal_places=4)

    class Meta:
        db_table = 'kf_factor_line'

    @property
    def kf_factor(self):
        if self.sample_weight_mg and self.burette_reading_ml:
            try:
                # FIX HERE: sample_weight / burette_reading
                return float(self.sample_weight_mg) / float(self.burette_reading_ml)
            except ZeroDivisionError:
                return None
        return None
    



class MeltingPointRecord(models.Model):
    entry_date = models.DateField()
    entry_time = models.TimeField()
    eln_id = models.CharField(max_length=50, blank=True, null=True)  # Not mandatory
    product_name = models.CharField(max_length=100, blank=True, null=True)  # Not mandatory
    batch_no = models.CharField(max_length=50, blank=True, null=True)  # Not mandatory
    sample_description = models.CharField(max_length=255, blank=True, null=True)  # Not mandatory
    unit = models.CharField(max_length=20, blank=True, null=True)  # Not mandatory
    instrument = models.CharField(
        max_length=20,
        choices=[('Lab India', 'Lab India'), ('Veego', 'Veego')],
        blank=True, null=True
    )
    melting_point = models.CharField(
        max_length=20, blank=True, null=True,
        validators=[RegexValidator( regex=MP_PATTERN,
            message="Enter a number or a range like 101-102 or 101.5-102.2",
            code="invalid_melting_point"
        )],
        help_text="e.g., 101-102 or 101.5" )
    analysed_by = models.CharField(max_length=100, blank=True, null=True)  # Not mandatory
    completed_date = models.DateField(verbose_name="Completed Date", blank=True, null=True)  # Auto-generated date
    completed_time = models.TimeField(verbose_name="Completed Time", blank=True, null=True)  # Auto-generated time

    class Meta:
        db_table = 'melting_point_record'
        verbose_name = "Melting Point Record"
        verbose_name_plural = "Melting Point Records"
        
     # Optional helpers to read min/max as floats later if needed
    @property
    def mp_min(self):
        if not self.melting_point: return None
        s = re.sub(r"[–—]", "-", self.melting_point).strip()
        parts = [p.strip() for p in s.split("-")]
        try: return float(parts[0])
        except: return None

    @property
    def mp_max(self):
        if not self.melting_point: return None
        s = re.sub(r"[–—]", "-", self.melting_point).strip()
        parts = [p.strip() for p in s.split("-")]
        try: return float(parts[-1]) if len(parts) > 1 else float(parts[0])
        except: return None

