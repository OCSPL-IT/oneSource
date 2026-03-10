from django.db import models
from django.conf import settings


class Downtime(models.Model):
    date               = models.DateField(null=True)
    idle               = models.CharField(max_length=5, default="No", null=True, blank=True)
    eqpt_id            = models.CharField(max_length=50, null=True)
    eqpt_name          = models.CharField(max_length=100, null=True)
    product_name       = models.CharField(max_length=255, blank=True, null=True)
    stage_name         = models.CharField(max_length=255, blank=True, null=True)
    product_code       = models.CharField(max_length=100, blank=True, null=True)
    batch_no           = models.CharField(max_length=50, blank=True, null=True)
    start_date         = models.DateField(null=True)
    end_date           = models.DateField(null=True)
    start_time         = models.TimeField(null=True)
    end_time           = models.TimeField(null=True)
    total_duration     = models.FloatField(null=True)
    block              = models.CharField(max_length=50, null=True)
    downtime_dept      = models.CharField(max_length=255, null=True)
    downtime_category  = models.CharField(max_length=255, null=True, blank=True)
    reason             = models.TextField(null=True)
    bom_qty            = models.FloatField(null=True, blank=True)
    bct                = models.FloatField(null=True, blank=True)
    loss               = models.FloatField(null=True, blank=True)
    batch_size = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)


    class Meta:
        db_table = "production_downtime"
        verbose_name = "Downtime Record"
        verbose_name_plural = "Downtime Records"

    def __str__(self):
        return (
            f"{self.date} | {self.stage_name or 'N/A'} | "
            f"{self.eqpt_name or self.eqpt_id or 'Unknown'}"
        )

class DowntimeCriticalEquip(models.Model):
    eqp       = models.CharField(max_length=50,  db_column='eqp')
    fg        = models.CharField(max_length=255, db_column='fg')
    stage     = models.CharField(max_length=255, db_column='stage')
    from_date = models.DateField(db_column='from_date')
    to_date   = models.DateField(db_column='to_date')

    class Meta:
        managed = False
        db_table = 'downtime_critical_equip'


class DeptCategory(models.Model):
    department = models.CharField(max_length=100)
    category   = models.CharField(max_length=255)
    is_active  = models.BooleanField(default=True)

    class Meta:
        db_table = "production_category_master"
        verbose_name = "Department Category"
        verbose_name_plural = "Department Categories"
        unique_together = ("department", "category")
        ordering = ["department", "category"]

    def __str__(self):
        return f"{self.department} — {self.category}"
