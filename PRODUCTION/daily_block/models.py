# daily_checks/models.py
from django.db import models
from django.utils import timezone
from django.conf import settings


class Block(models.Model):
    code          = models.CharField(max_length=20, primary_key=True)
    display_name  = models.CharField(max_length=60)

    class Meta:
        db_table = "dc_block"
        verbose_name        = "Block"
        verbose_name_plural = "Blocks"

    def __str__(self):
        return self.display_name


# ────────────────────────────────────────────────────────────────
#  Header (“Report Details” card)
# ────────────────────────────────────────────────────────────────
class DailyCheckHeader(models.Model):
    transaction_number = models.CharField(max_length=25, unique=True,null=True, blank=True,)
    block  = models.ForeignKey(Block, on_delete=models.PROTECT)
    report_dt = models.DateTimeField()
    remarks   = models.CharField(max_length=500, blank=True)
    prepared_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.PROTECT,
        null=True, blank=True,
    )
    created_at = models.DateTimeField(default=timezone.now, editable=False)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table            = "dc_daily_check_hdr"
        ordering            = ["-report_dt"]
        verbose_name        = "Daily Check Header"
        verbose_name_plural = "Daily Check Headers"

    def __str__(self):
        return f"DailyCheck #{self.transaction_number} ({self.report_dt:%Y-%m-%d %H:%M})"


# ────────────────────────────────────────────────────────────────
#  ASSETS & INVENTORY  (tab 3)
# ────────────────────────────────────────────────────────────────
class AssetsFlag(models.TextChoices):
    OUTPUT = "OUTPUT", "WIP/SFG/FG/ Output"
    WIP     = "WIP",     "WIP"
    EFFLUENT     = "EFFLUENT",  "Effluent"


class EWSTypeChoices(models.TextChoices):
    SCRUBBER_ACIDIC = "Scrubber Acidic Effluent", "Scrubber Acidic Effluent"
    NUETRAL = "Nuetral", "Nuetral"
    ACIDIC = "Acidic", "Acidic"
    ACIDIC_AQ_LAYER = "Acidic Aq. Layer", "Acidic Aq. Layer"
    SODIUM_CYANIDE = "Sodium Cyanide Effluent", "Sodium Cyanide Effluent"
    AMMONIUM_CHLORIDE = "Ammonium Chloride effluent", "Ammonium Chloride effluent"
    SCRUBBER_BASIC = "Scrubber Basic Effluent", "Scrubber Basic Effluent"
    SULPHURIC_BELOW_50 = "Sulphuric below 50 % effluent", "Sulphuric below 50 % effluent"
    SULPHURIC_ABOVE_50 = "Sulphuric above 50% effluent", "Sulphuric above 50% effluent"
    BASIC = "Basic", "Basic"
    RESIDUE = "Reside", "Residue"

class AssetsInventory(models.Model):
    id = models.AutoField(primary_key=True)
    daily_check = models.ForeignKey(
        DailyCheckHeader, on_delete=models.CASCADE, related_name="asset_rows"
    )
    row_flag = models.CharField(max_length=10, choices=AssetsFlag.choices)
    # ── SFG / FG Output ────────────────────────
    batch_size = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    out_stage   = models.CharField(max_length=100, null=True, blank=True)
    out_item_id = models.CharField(max_length=100, null=True, blank=True)
    out_bno     = models.CharField(max_length=100, null=True, blank=True)
    out_equip     = models.CharField(max_length=100, null=True, blank=True)
    out_b_starttime     = models.DateTimeField(max_length=30,null=True, blank=True)
    out_b_endtime     = models.DateTimeField(max_length=30,null=True, blank=True)
    out_bct     = models.DecimalField(max_digits=12,decimal_places=2, null=True, blank=True)
    out_qty     = models.DecimalField(max_digits=12, decimal_places=2,null=True, blank=True)
    out_remarks = models.CharField(max_length=200, blank=True)
    out_status = models.CharField(max_length=100, blank=True)
    

    class Meta:
        db_table            = "dc_assets_inventory"
        verbose_name        = "Asset / Inventory Row"
        verbose_name_plural = "Asset / Inventory Rows"

    def __str__(self):
        return f"{self.get_row_flag_display()} ({self.daily_check})"



# ────────────────────────────────────────────────────────────────
#  ERP-SYNC REFERENCE TABLES (managed = False)
# ────────────────────────────────────────────────────────────────
class BlockItemMaster(models.Model):
    product_id = models.CharField(max_length=100, primary_key=True)
    product_name = models.CharField(max_length=200)
    item_type = models.CharField(max_length=100)

    class Meta:
        db_table = "blockitemmaster"
        managed = False  # ← do NOT run migrations on this

    def __str__(self):
        return self.product_name



class BmrIssue(models.Model):
    bmr_issue_type = models.CharField(max_length=50)
    bmr_issue_no = models.CharField(max_length=50)
    bmr_issue_date = models.DateField()
    fg_name = models.CharField(max_length=200)
    op_batch_no = models.CharField(max_length=100, db_index=True)
    product_name = models.CharField(max_length=200, blank=True)
    block = models.CharField(max_length=100, blank=True)
    line_no = models.IntegerField()
    item_type = models.CharField(max_length=100)
    item_code = models.CharField(max_length=100)
    item_name = models.CharField(max_length=200)
    item_narration = models.TextField(blank=True)
    uom = models.CharField(max_length=20)
    batch_quantity = models.DecimalField(max_digits=18, decimal_places=3)

    class Meta:
        db_table = "bmr_issue"
        managed = True
        unique_together = ("bmr_issue_no", "line_no")

    def __str__(self):
        return f"{self.bmr_issue_no} – Line {self.line_no}"


class ERPBOMDetail(models.Model):
    """
    Holds the Stage → FG → Equipment mappings from the last ERP sync.
    """

    stage_name = models.CharField(max_length=200, unique=True)
    fg_name = models.CharField(max_length=200)
    equipment = models.CharField(max_length=200)

    class Meta:
        db_table = "daily_checks_erpbomdetail"
        managed = True
        verbose_name = "ERP BOM Detail"
        verbose_name_plural = "ERP BOM Details"

    def __str__(self):
        return f"{self.stage_name} → {self.fg_name} ({self.equipment})"

### 4. Import the New Models



# =============Below code is related production scheduling database ========================================================

class BOMHeader(models.Model):
    bom_id = models.AutoField(primary_key=True)
    fg_name = models.CharField(max_length=100)
    stage_name = models.CharField(max_length=100)
    batch_size = models.FloatField(default=0.0)
    bom_code = models.CharField(max_length=50)
    bom_name = models.CharField(max_length=100)
    remarks = models.TextField(blank=True, null=True)
    fixed_equipment_id = models.CharField(max_length=100)

    class Meta:
        managed = False
        db_table = 'bom_headers'
        app_label = 'daily_checks'


class BOMLine(models.Model):
    line_id = models.AutoField(primary_key=True)
    bom_header = models.ForeignKey(
        BOMHeader, on_delete=models.DO_NOTHING, db_column='bom_id', related_name='lines'
    )
    line_type = models.CharField(max_length=20, blank=True, null=True)
    material_category = models.CharField(max_length=50, blank=True, null=True)
    material_name = models.CharField(max_length=100, blank=True, null=True)
    ratio = models.FloatField(default=0.0)
    quantity = models.FloatField(default=0.0)
    density = models.FloatField(default=0.0)
    litre = models.FloatField(default=0.0)
    include_in_total = models.BooleanField(default=True)

    class Meta:
        managed = False
        db_table = 'bom_lines'
        app_label = 'daily_checks'


class BOMEquipment(models.Model):
    equipment_id = models.AutoField(primary_key=True)
    bom_header = models.ForeignKey(
        BOMHeader, on_delete=models.DO_NOTHING, db_column='bom_id', related_name='equipments'
    )
    equipment_type = models.CharField(max_length=50)
    capacity_size = models.CharField(max_length=50)
    moc_equipment = models.CharField(max_length=50)
    equipment_ref = models.CharField(max_length=50)
    std_bct = models.FloatField(default=0.0)
    wait_time = models.FloatField(default=0.0)

    class Meta:
        managed = False
        db_table = 'bom_equipment'
        app_label = 'daily_checks'
        
        
        
# ----------------------------------------------------------------
# START: NEW MODELS TO STORE ACTUAL BOM DATA
# ----------------------------------------------------------------

class DailyCheckBOMInput(models.Model):
    """ Stores the actual Input Items used for a specific output row. """
    output_row = models.ForeignKey(
        AssetsInventory, on_delete=models.CASCADE, related_name="bom_inputs"
    )
    material_category = models.CharField(max_length=100, blank=True)
    material_name = models.CharField(max_length=100, blank=True)
    actual_qty = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    quantity = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    litre = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    density = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    ratio = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    
    class Meta:
        db_table = "dc_bom_inputs"
        verbose_name = "Daily Check BOM Input"
        verbose_name_plural = "Daily Check BOM Inputs"


class DailyCheckBOMOutput(models.Model):
    """ Stores the actual Output Items generated from a specific output row. """
    output_row = models.ForeignKey(
        AssetsInventory, on_delete=models.CASCADE, related_name="bom_outputs"
    )
    material_category = models.CharField(max_length=100, blank=True)
    material_name = models.CharField(max_length=100, blank=True)
    actual_qty = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    quantity = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    litre = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    density = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    ratio = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)
    
    class Meta:
        db_table = "dc_bom_outputs"
        verbose_name = "Daily Check BOM Output"
        verbose_name_plural = "Daily Check BOM Outputs"



class DailyCheckBOMWaste(models.Model):
    """ Stores the actual Waste Generated for a specific output row. """
    output_row = models.ForeignKey(
        AssetsInventory, on_delete=models.CASCADE, related_name="bom_waste"
    )
    waste_type = models.CharField(max_length=100, blank=True)
    waste_name = models.CharField(max_length=100, blank=True)
    actual_qty = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    quantity = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    litre = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    density = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    ratio = models.DecimalField(max_digits=12, decimal_places=3, null=True, blank=True)

    class Meta:
        db_table = "dc_bom_waste"
        verbose_name = "Daily Check BOM Waste"
        verbose_name_plural = "Daily Check BOM Wastes"

class DailyCheckBOMEquipment(models.Model):
    """ Stores the actual Equipment Used for a specific output row. """
    output_row = models.ForeignKey(
        AssetsInventory, on_delete=models.CASCADE, related_name="bom_equipment"
    )
    equipment_type = models.CharField(max_length=100, blank=True)
    moc = models.CharField(max_length=100, blank=True)
    capacity = models.CharField(max_length=50, blank=True)
    equipment_id = models.CharField(max_length=100, blank=True)
    std_bct = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    wait_time = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)
    starttime = models.DateTimeField(null=True, blank=True)
    endtime = models.DateTimeField(null=True, blank=True)
    actual_bct = models.DecimalField(max_digits=12, decimal_places=2, null=True, blank=True)

    class Meta:
        db_table = "dc_bom_equipment"
        verbose_name = "Daily Check BOM Equipment"
        verbose_name_plural = "Daily Check BOM Equipment"
        
        
class Equipment(models.Model):
    """
    Represents the existing 'equipment' table in the 'production_scheduler'
    database. This is an unmanaged model, so Django will not perform
    migrations on it.
    """
    # Assuming 'eq_id' is the unique identifier for an equipment item.
    # We set it as the primary key for Django's ORM.
    eq_id = models.CharField(max_length=100, primary_key=True)
    block = models.CharField(max_length=100)

    class Meta:
        managed = False  # Tells Django not to manage this table's schema
        db_table = 'equipment'
        verbose_name = "Scheduler Equipment"
        verbose_name_plural = "Scheduler Equipment"