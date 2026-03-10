from django.db import models



class UtilityRecord(models.Model):
    reading_date       = models.DateField(db_column='reading_date')
    reading_type       = models.CharField(max_length=100, db_column='reading_type')
    sb_3_e_22_main_fm_fv  = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    sb_3_sub_fm_oc      = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    block_a_reading     = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    block_b_reading     = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    mee_total_reading   = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    stripper_reading    = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    old_atfd            = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    mps_d_block_reading = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    lps_e_17            = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    mps_e_17            = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    jet_ejector_atfd_c  = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    deareator           = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True) #new field 
    new_atfd            = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    boiler_water_meter  = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    midc_water_e_18     = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    midc_water_e_17     = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    midc_water_e_22     = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    midc_water_e_16     = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    midc_water_e_20     = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    briquette_sb_3      = models.DecimalField(max_digits=10, decimal_places=3, null=True, blank=True) # BRIQUETTE
    briquette_tfh      = models.DecimalField(max_digits=10, decimal_places=3, null=True, blank=True) # BRIQUETTE
    dm_water_for_boiler = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)   #"Boiler Water meter Reading"
 
    class Meta:
        db_table = 'utility_records'

    def __str__(self):
        return f"{self.reading_date} | {self.reading_type}"


# ==========================Below is power utility related code ==============================================================================


 
class UtilityPowerReading(models.Model):
    reading_date = models.DateField(db_column='reading_date')
    reading_type = models.CharField(max_length=100, db_column='reading_type')
    block_a1 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    block_a2 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    block_b1 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    block_b2 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    block_d1 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    block_d2 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    block_c1 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    block_b_all_ejector = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    utility_2 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    utility_3 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    block_b3_ut04 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    utility_05_block_b_anfd = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    tf_unit = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    ct_75hp_pump2 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    stabilizer = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    etp_e17 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    mee_e17 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    c03_air_compressor_40hp = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    trane1_brine_comp_110tr = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    chiller_02_trane2 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    voltas_chiller_02 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    block_c2_d04 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    ct_75hp_pump1 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    new_ro = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    new_atfd = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    admin = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    etp_press_filter = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    others_e18_fire = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    mcc_total = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    imcc_panel_01_utility = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    imcc_panel_02_utility = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    imcc_panel_03 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    imcc_panel_04 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    imcc_panel_05 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    row_power_panel = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    lighting_panel = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    brine_chiller_1_5f_30 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    water_chiller_2_4r_440 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    others_e17 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    imcc_total = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    e22_mseb = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    e22_pcc = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    e22_boiler = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    e22_aircom_tf_boiler_other = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    mcc_imcc_total = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    pcc_main_e17 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    mseb_e18 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    pcc_01 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    pcc_02 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    tr_losses_e18 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    tr_losses_e22 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    e16_mseb = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    dg_total_e18 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    dg_total_e22 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    dg_pcc_e18 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    dg_pcc_e22 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    total_kwh_e18_e22_e16 = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)

    class Meta:
        db_table = 'utility_power_readings'


