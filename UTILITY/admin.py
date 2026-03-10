from django.contrib import admin
from .models import UtilityRecord

@admin.register(UtilityRecord)
class UtilityRecordAdmin(admin.ModelAdmin):
    list_display = [
        'reading_date',
        'reading_type',
        'sb_3_e_22_main_fm_fv',
        'sb_3_sub_fm_oc',
        'block_a_reading',
        'block_b_reading',
        'mee_total_reading',
        'stripper_reading',
        'old_atfd',
        'mps_d_block_reading',
        'lps_e_17',
        'mps_e_17',
        'jet_ejector_atfd_c',
        'deareator',
        'new_atfd',
        'boiler_water_meter',
        'midc_water_e_18',
        'midc_water_e_17',
        'midc_water_e_22',
        'midc_water_e_16',
        'midc_water_e_20',
        'briquette_sb_3',
        'dm_water_for_boiler',
    ]
    list_filter = ['reading_date', 'reading_type']
    search_fields = ['reading_date', 'reading_type']

    # Optional: to show default ordering
    ordering = ['-reading_date', 'reading_type']

    # To enable filtering by year/month/day in admin filter sidebar (if needed)
    date_hierarchy = 'reading_date'