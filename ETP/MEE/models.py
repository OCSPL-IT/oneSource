# models.py
from django.db import models
from datetime import datetime ,time as dtime
from django.contrib.auth.models import User
from django.db.models import Sum
from decimal import Decimal,InvalidOperation


class MEEReadingCategory(models.Model):
    """Top-level header, e.g. 'Effluent Feed', 'Steam Consume', etc."""
    name = models.CharField(max_length=100, unique=True)
    unit = models.CharField(max_length=50, null=True, blank=True, help_text="Unit of measurement, e.g. KL, MT, °C")
    order = models.PositiveIntegerField(default=0,help_text="Display order in Utility Entry tabs (lower = left side)",)
    class Meta:
        db_table = 'mee_reading_category'
        ordering = ['order', 'name']      # 👈 use order by default
        
    def __str__(self):
        return self.name


class MEEReadingSubCategory(models.Model):
    """Sub-header belonging to a ReadingCategory, e.g. 'Stripper', 'MEE', etc."""
    category = models.ForeignKey(MEEReadingCategory, on_delete=models.CASCADE, related_name="subcategories")
    name = models.CharField(max_length=100)
    order = models.PositiveIntegerField(default=0,help_text="Display order in tabs/header (lower = left).",)
    
    class Meta:
        db_table = 'mee_reading_Subcategory'
        unique_together = ('category', 'name')
        ordering = ["category","order", "id"]

    def __str__(self):
        return f"{self.category.name} - {self.name}"



class MEEDailyReading(models.Model):
    """Stores actual entered values per day and subcategory."""
    reading_date = models.DateField()
    subcategory = models.ForeignKey(MEEReadingSubCategory, on_delete=models.CASCADE, related_name="mee_daily_readings")
    value = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    entered_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('reading_date', 'subcategory')
        ordering = ['reading_date', 'subcategory']
        db_table = 'mee_daily_reading'

    def __str__(self):
        return f"{self.reading_date} - {self.subcategory.name}: {self.value}"
        
        
class MEEDowntime(models.Model):
    reading = models.ForeignKey( MEEDailyReading,on_delete=models.CASCADE,related_name="downtimes",)
    downtime_start = models.DateTimeField(null=True, blank=True)
    downtime_end   = models.DateTimeField(null=True, blank=True)
    downtime_hours = models.DecimalField(max_digits=7, decimal_places=2,null=True, blank=True,help_text="Duration in hours" )
    reason = models.CharField(max_length=255,  null=True, blank=True,  help_text="Downtime reason", )

    class Meta:
        db_table = "mee_downtime"
        ordering = ["reading__reading_date", "downtime_start"]

    def __str__(self):
        return f"{self.reading.reading_date} - {self.downtime_start} → {self.downtime_end}"

    def _compute_duration(self):
        if not self.downtime_start or not self.downtime_end:
            return None
        if self.downtime_end <= self.downtime_start:
            return None
        seconds = (self.downtime_end - self.downtime_start).total_seconds()
        return round(Decimal(seconds) / Decimal("3600"), 2)

    def save(self, *args, **kwargs):
        self.downtime_hours = self._compute_duration()
        super().save(*args, **kwargs)
        
        
        
        
##------------------Below code is for ATFD ---------------------------------        
        
        
        
from decimal import Decimal, InvalidOperation

class ATFDReading(models.Model):
    reading_date = models.DateField(db_index=True)

    effluent_feed = models.DecimalField("ATFD Feed (KL)", max_digits=12, decimal_places=3, null=True, blank=True)
    steam_consume = models.DecimalField("Steam Consume (MT)", max_digits=12, decimal_places=3, null=True, blank=True)

    steam_economy = models.DecimalField(
        "Steam Economy (KL/MT)",
        max_digits=12,
        decimal_places=4,
        null=True,
        blank=True,
        help_text="Auto: ATFD Feed / Steam Consume",
    )

    blower_draft = models.DecimalField("Blower Draft(mmWC)", max_digits=12, decimal_places=3, null=True, blank=True)
    steam_inlet_pressure = models.DecimalField("Stean Inlet Pressure(kg/cm2)", max_digits=12, decimal_places=3, null=True, blank=True)
    atfd_rpm = models.DecimalField("ATFD RPM", max_digits=12, decimal_places=3, null=True, blank=True)

    atfd_salt = models.DecimalField("ATFD Salt (MT)", max_digits=12, decimal_places=3, null=True, blank=True)

    effluent_feed_ph = models.DecimalField("Effluent Feed pH", max_digits=6, decimal_places=2, null=True, blank=True)
    effluent_feed_TDS = models.DecimalField("Effluent Feed TDS(%)", max_digits=6, decimal_places=2, null=True, blank=True)
    effluent_feed_cod = models.DecimalField("Effluent Feed cod(%)", max_digits=6, decimal_places=2, null=True, blank=True)
    effluent_feed_spgr = models.DecimalField("Effluent Feed spGr", max_digits=6, decimal_places=3, null=True, blank=True)

    # ✅ auto: effluent_feed * effluent_feed_spgr - atfd_salt
    atfd_qty = models.DecimalField("ATFD Vapor condensate Qty(KL)", max_digits=6, decimal_places=2, null=True, blank=True)

    vapor_contensate_ph = models.DecimalField("ATFD Vapor condensate pH", max_digits=6, decimal_places=2, null=True, blank=True)
    vapor_contensate_tds = models.DecimalField("ATFD Vapor condensate TDS(%)", max_digits=6, decimal_places=2, null=True, blank=True)
    vapor_contensate_cod = models.DecimalField("ATFD Vapor condensate COD(%)", max_digits=6, decimal_places=2, null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "atfd_reading"
        ordering = ["-reading_date"]
        constraints = [
            models.UniqueConstraint(fields=["reading_date"], name="uq_atfd_reading_date")
        ]

    def __str__(self):
        return f"ATFD Reading - {self.reading_date}"

    def _compute_steam_economy(self):
        """
        steam_economy = effluent_feed(KL) / steam_consume(MT)
        """
        try:
            if self.effluent_feed is None or self.steam_consume is None:
                return None
            if Decimal(self.steam_consume) <= 0:
                return None
            return (Decimal(self.effluent_feed) / Decimal(self.steam_consume)).quantize(Decimal("0.0001"))
        except (InvalidOperation, ZeroDivisionError, TypeError):
            return None

    def _compute_atfd_qty(self):
        """
        atfd_qty = effluent_feed * effluent_feed_spgr - atfd_salt
        """
        try:
            if self.effluent_feed is None or self.effluent_feed_spgr is None or self.atfd_salt is None:
                return None
            qty = (Decimal(self.effluent_feed) * Decimal(self.effluent_feed_spgr)) - Decimal(self.atfd_salt)
            return qty.quantize(Decimal("0.01"))  # because atfd_qty has decimal_places=2
        except (InvalidOperation, TypeError):
            return None

    def save(self, *args, **kwargs):
        self.steam_economy = self._compute_steam_economy()
        self.atfd_qty = self._compute_atfd_qty()
        super().save(*args, **kwargs)



class ATFDDowntime(models.Model):
    reading = models.ForeignKey(
        ATFDReading, on_delete=models.CASCADE, related_name="downtimes"
    )
    downtime_start = models.DateTimeField(null=True, blank=True)
    downtime_end = models.DateTimeField(null=True, blank=True)
    downtime_hours = models.DecimalField(
        max_digits=7, decimal_places=2, null=True, blank=True,
        help_text="Duration in hours"
    )
    reason = models.CharField(
        max_length=255, null=True, blank=True, help_text="Downtime reason"
    )

    class Meta:
        db_table = "atfd_downtime"
        ordering = ["reading__reading_date", "downtime_start"]

    def __str__(self):
        return f"{self.reading.reading_date} - {self.downtime_start} → {self.downtime_end}"

    def _compute_duration(self):
        if not self.downtime_start or not self.downtime_end:
            return None
        if self.downtime_end <= self.downtime_start:
            return None
        seconds = (self.downtime_end - self.downtime_start).total_seconds()
        return (Decimal(seconds) / Decimal("3600")).quantize(Decimal("0.01"))

    def save(self, *args, **kwargs):
        self.downtime_hours = self._compute_duration()
        super().save(*args, **kwargs)