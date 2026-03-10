# reports/models.py
from django.db import models


class ProductionSchedule(models.Model):
    # ---- header fields used by the algorithm ----
    schedule_id      = models.AutoField(primary_key=True)  # adjust if needed
    product_id       = models.CharField(max_length=150, blank=True, null=True)
    stage_name       = models.CharField(max_length=150, blank=True, null=True)
    type             = models.CharField(max_length=80,  blank=True, null=True)  # e.g., "Semi Finished Good"
    start_date       = models.DateTimeField()                                    # or DateField; algorithm converts anyway
    closed_date      = models.DateField(blank=True, null=True)
    no_of_batches    = models.IntegerField(default=0)
    scheduling_approach = models.IntegerField(default=0)  # 0 ROLL, 1 FIFO, 3 STAR
    batch_number     = models.CharField(max_length=50, blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'production_schedule'         # <-- put your actual table name


class ProductionScheduleLine(models.Model):
    # minimal fields referenced by generate_batch_rows
    id               = models.AutoField(primary_key=True)
    schedule         = models.ForeignKey(
        ProductionSchedule, related_name='lines', on_delete=models.DO_NOTHING
    )
    line_type        = models.CharField(max_length=20)  # 'input' | 'output' | 'waste' | 'equipment'

    # material fields
    material_category = models.CharField(max_length=100, blank=True, null=True)
    material_name     = models.CharField(max_length=200, blank=True, null=True)
    quantity          = models.DecimalField(max_digits=18, decimal_places=3, blank=True, null=True)
    ratio             = models.DecimalField(max_digits=18, decimal_places=5, blank=True, null=True)
    density           = models.DecimalField(max_digits=18, decimal_places=5, blank=True, null=True)
    litre             = models.DecimalField(max_digits=18, decimal_places=3, blank=True, null=True)
    include_in_total  = models.BooleanField(default=True)

    # equipment fields
    equipment_id      = models.CharField(max_length=50, blank=True, null=True)
    std_bct           = models.DecimalField(max_digits=18, decimal_places=3, blank=True, null=True)
    wait_time         = models.DecimalField(max_digits=18, decimal_places=3, blank=True, null=True)
    star              = models.BooleanField(default=False)
    closed_date       = models.DateField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = 'production_schedule_line'    # <-- put your actual table name
