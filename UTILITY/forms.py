from django import forms
from .models import *
import decimal
from django.forms.formsets import BaseFormSet

TYPE_CHOICES = [
    ("STEAM GENERATION READING", "STEAM GENERATION READING"),
    ("STEAM CONSUMPTION READING", "STEAM CONSUMPTION READING"),
    ("Boiler Water meter Reading", "Boiler Water meter Reading"),
    ("MIDC reading", "MIDC reading"),
    ("BRIQUETTE", "BRIQUETTE"),
    ("DM Water consumed for boiler", "DM Water consumed for boiler"),
]

COMMON_INPUT_CLASSES = (
    "w-full px-3 py-2.5 text-sm text-slate-700 "
    "border border-slate-300 rounded-lg shadow-sm "
    "focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 "
    "placeholder-slate-400 transition"
)

class UtilityRecordForm(forms.ModelForm):
    reading_type = forms.ChoiceField(
        choices=TYPE_CHOICES,
        widget=forms.HiddenInput()
    )

    class Meta:
        model = UtilityRecord
        fields = [
            "reading_type",
            "sb_3_e_22_main_fm_fv",
            "sb_3_sub_fm_oc",
            "block_a_reading",
            "block_b_reading",
            "mee_total_reading",
            "stripper_reading",
            "old_atfd",
            "mps_d_block_reading",
            "lps_e_17",
            "mps_e_17",
            "jet_ejector_atfd_c",
            "deareator",
            "new_atfd",
            "boiler_water_meter",
            "midc_water_e_18",
            "midc_water_e_17",
            "midc_water_e_22",
            "midc_water_e_16",
            "midc_water_e_20",
            "briquette_sb_3",
            "briquette_tfh",
            "dm_water_for_boiler",
        ]
        widgets = {
            field: forms.NumberInput(attrs={
                "class": COMMON_INPUT_CLASSES,
                "placeholder": "0.00",
                "step": "any",
            })
            for field in fields if field != "reading_type"
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for fname, f in self.fields.items():
            if fname != "reading_type":
                f.required = False


#===========================Below code for power reading ==================================================================


POWER_TYPE_CHOICES = [
    ("E-18 POWER CONSUMPTION", "E-18 POWER CONSUMPTION"),
    ("E-17 POWER CONSUMPTION", "E-17 POWER CONSUMPTION"),
    ("E-22 POWER CONSUMPTION", "E-22 POWER CONSUMPTION"),
    ("TOTAL POWER CONSUMPTION", "TOTAL POWER CONSUMPTION"),
]

COMMON_INPUT_CLASSES = (
    "w-full px-3 py-2.5 text-sm text-slate-700 "
    "border border-slate-300 rounded-lg shadow-sm "
    "focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 "
    "placeholder-slate-400 transition"
)


CALC_FIELDS = [
    # All your calculated/readonly fields
    "mcc_total",
    "others_e18_fire",
    "imcc_total",
    "others_e17",
    "mcc_imcc_total",
    "tr_losses_e18",
    "tr_losses_e22",
    "dg_pcc_e18",
    "dg_pcc_e22",
    "total_kwh_e18_e22_e16",
]


class UtilityPowerReadingForm(forms.ModelForm):
    reading_type = forms.ChoiceField(choices=POWER_TYPE_CHOICES, widget=forms.HiddenInput())

    class Meta:
        model = UtilityPowerReading
        fields = [f.name for f in UtilityPowerReading._meta.fields if f.name != "id"]
        widgets = {
            field: forms.NumberInput(attrs={
                "class": COMMON_INPUT_CLASSES, "placeholder": "0.00", "step": "any",
            })
            for field in fields if field not in ["reading_type", "reading_date"]
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for fname, f in self.fields.items():
            if fname not in ["reading_type", "reading_date"]:
                f.required = False
            if fname in CALC_FIELDS:
                f.widget.attrs["readonly"] = "readonly"
                f.widget.attrs["class"] += " bg-gray-100"
            f.widget.attrs["id"] = f"id_{fname}"
        self.fields["reading_date"].required = False

   
class BasePowerReadingFormSet(BaseFormSet):
    """
    This is the correct place for cross-form validation and calculation.
    The clean method here runs AFTER each individual form's clean method and has access to all forms' data.
    """
    def clean(self):
        super().clean()
        if any(self.errors):
            # Don't bother calculating if there are individual form errors.
            return

        # Consolidate all cleaned data from the entire formset into one dictionary
        combined_data = {}
        for form_data in self.cleaned_data:
            if form_data:
                combined_data.update(form_data)
        
        get_val = lambda key: combined_data.get(key) or decimal.Decimal(0)

        # --- ALL CALCULATIONS CENTRALIZED HERE ---
        # This logic now perfectly matches your front-end JavaScript.
        
        # Step 1: Intermediate Totals
        mcc_fields = ['block_a1', 'block_a2', 'block_b1', 'block_b2', 'block_d1', 'block_d2', 'block_c1', 'block_b_all_ejector', 'utility_2', 'utility_3', 'block_b3_ut04', 'utility_05_block_b_anfd', 'tf_unit', 'ct_75hp_pump2', 'stabilizer', 'etp_e17', 'mee_e17', 'c03_air_compressor_40hp', 'trane1_brine_comp_110tr', 'chiller_02_trane2', 'voltas_chiller_02', 'block_c2_d04', 'ct_75hp_pump1', 'new_ro', 'new_atfd', 'admin', 'etp_press_filter']
        mcc_total = sum(get_val(f) for f in mcc_fields)

        imcc_fields = ['imcc_panel_01_utility', 'imcc_panel_02_utility', 'imcc_panel_03', 'imcc_panel_04', 'imcc_panel_05', 'row_power_panel', 'lighting_panel', 'brine_chiller_1_5f_30', 'water_chiller_2_4r_440']
        imcc_total = sum(get_val(f) for f in imcc_fields)

        # Step 2: Final Dependent Calculations (mirroring the JS exactly)
        mcc_imcc_total = mcc_total + get_val('pcc_main_e17')
        others_e17 = get_val('pcc_main_e17') - imcc_total
        others_e18_fire = get_val('pcc_01') + get_val('dg_total_e18') - mcc_imcc_total
        tr_losses_e18 = get_val('mseb_e18') - get_val('pcc_01')
        tr_losses_e22 = get_val('e22_mseb') - get_val('e22_pcc')
        dg_pcc_e18 = get_val('pcc_01') + get_val('dg_total_e18')
        dg_pcc_e22 = get_val('e22_pcc') + get_val('dg_total_e22')
        total_kwh = get_val('e22_mseb') + get_val('mseb_e18') + get_val('e16_mseb') + get_val('dg_total_e18') + get_val('dg_total_e22')

        # Step 3: Create a dictionary of all calculated values
        calculated_values = {
            'mcc_total': mcc_total, 'imcc_total': imcc_total, 'mcc_imcc_total': mcc_imcc_total,
            'others_e17': others_e17, 'others_e18_fire': others_e18_fire,
            'tr_losses_e18': tr_losses_e18, 'tr_losses_e22': tr_losses_e22,
            'dg_pcc_e18': dg_pcc_e18, 'dg_pcc_e22': dg_pcc_e22,
            'total_kwh_e18_e22_e16': total_kwh
        }

        # Step 4: Update the cleaned_data for each form. This is crucial.
        # It ensures that when you save each form in the view, it has all the correct calculated values.
        for form in self.forms:
            form.cleaned_data.update(calculated_values)























