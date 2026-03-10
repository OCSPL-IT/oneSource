# forms.py
from django import forms
from django.forms import inlineformset_factory

from .models import *


BASE_INPUT = (
    "w-full border border-slate-300 rounded-md px-2 py-1 text-sm "
    "focus:outline-none focus:ring-1 focus:ring-emerald-500 bg-white"
)


READONLY_INPUT = BASE_INPUT + " bg-slate-100 cursor-not-allowed"

# Fixed list of chemicals for this screen
BIOREACTOR_CHEMICALS = [
    ("PAC", "PAC (Kg)"),
    ("DAP", "DAP (Kg)"),
]


class BioreactorDailyReadingForm(forms.ModelForm):
    class Meta:
        model = BioreactorDailyReading
        fields = [
            "date",
            "bioreactor_feed",

            # Bioreactor 1
            "bioreactor_1_ph",
            "bioreactor_1_cod",
            "bioreactor_1_mlss",
            "bioreactor_1_mlvss",
            "bioreactor_1_svi",
            "bioreactor_1_do",
            "bioreactor_1_fm_ratio",

            # Bioreactor 2
            "bioreactor_2_ph",
            "bioreactor_2_cod",
            "bioreactor_2_mlss",
            "bioreactor_2_mlvss",
            "bioreactor_2_svi",
            "bioreactor_2_do",
            "bioreactor_2_fm_ratio",

            # Polishing tank
            "polishing_tank_ph",
            "polishing_tank_tss",
            "polishing_tank_tds",
            "polishing_tank_cod",

            # Feed detailed
            "bioreactor_feed_ph",
            "bioreactor_feed_temp",
            "bioreactor_feed_tds",
            "bioreactor_feed_tss",
            "bioreactor_feed_cod",
        ]
        widgets = {
            "date": forms.DateInput(
                attrs={"type": "date", "class": BASE_INPUT}
            ),
            "bioreactor_feed": forms.NumberInput(
                attrs={"class": BASE_INPUT, "step": "0.001"}
            ),

            # Bioreactor 1
            "bioreactor_1_ph": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_1_cod": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_1_mlss": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_1_mlvss": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_1_svi": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_1_do": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            # Bioreactor 1
            "bioreactor_1_fm_ratio": forms.NumberInput(attrs={"class": READONLY_INPUT,"step": "0.001", "readonly": "readonly",}),

           
            # Bioreactor 2
            "bioreactor_2_ph": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_2_cod": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_2_mlss": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_2_mlvss": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_2_svi": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_2_do": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
             # Bioreactor 2
            "bioreactor_2_fm_ratio": forms.NumberInput(attrs={"class": READONLY_INPUT, "step": "0.001", "readonly": "readonly", }),


            # Polishing tank
            "polishing_tank_ph": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "polishing_tank_tss": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "polishing_tank_tds": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "polishing_tank_cod": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),

            # Feed detailed
            "bioreactor_feed_ph": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_feed_temp": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.01"}),
            "bioreactor_feed_tds": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_feed_tss": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
            "bioreactor_feed_cod": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.001"}),
        }


class BioreactorChemicalForm(forms.ModelForm):
    class Meta:
        model = BioreactorChemical
        fields = ["chemical_name", "quantity"]
        widgets = {
            "chemical_name": forms.Select(attrs={"class": BASE_INPUT}),
            "quantity": forms.NumberInput(
                attrs={"class": BASE_INPUT, "step": "0.001"}
            ),
        }


BioreactorChemicalFormSet = inlineformset_factory(
    BioreactorDailyReading,
    BioreactorChemical,
    fields=["chemical_name", "quantity"],
    extra=2,          # at least 2 rows for PAC & DAP
    can_delete=False,
)
