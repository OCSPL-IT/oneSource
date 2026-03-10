# forms.py
from django import forms
from .models import *


class MEEDailyReadingForm(forms.ModelForm):
    class Meta:
        model = MEEDailyReading
        fields = ["reading_date", "subcategory", "value"]
        widgets = {
            "reading_date": forms.DateInput(
                attrs={
                    "type": "date",
                    "class": "border rounded-lg p-2 w-full",
                }
            ),
            "subcategory": forms.Select(
                attrs={"class": "border rounded-lg p-2 w-full"}
            ),
            "value": forms.NumberInput(
                attrs={
                    "class": "border rounded-lg p-2 w-full",
                    "step": "0.001",
                }
            ),
        }




class MEEDowntimeForm(forms.ModelForm):
    class Meta:
        model = MEEDowntime
        fields = ["downtime_start", "downtime_end", "reason"]
        widgets = {
            "downtime_start": forms.DateTimeInput(
                attrs={
                    "type": "datetime-local",
                    "class": "border rounded-lg p-2 w-full",
                },
                format="%Y-%m-%dT%H:%M",
            ),
            "downtime_end": forms.DateTimeInput(
                attrs={
                    "type": "datetime-local",
                    "class": "border rounded-lg p-2 w-full",
                },
                format="%Y-%m-%dT%H:%M",
            ),
            "reason": forms.TextInput(
                attrs={"class": "border rounded-lg p-2 w-full"}
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for f in ("downtime_start", "downtime_end"):
            self.fields[f].input_formats = ["%Y-%m-%dT%H:%M"]


##=============================================================================================




class ATFDReadingForm(forms.ModelForm):
    class Meta:
        model = ATFDReading
        fields = [
            "reading_date",

            # ---- Process / Utility ----
            "effluent_feed",
            "steam_consume",
            "steam_economy",
            "blower_draft",
            "steam_inlet_pressure",
            "atfd_rpm",
            "atfd_salt",

            # ---- Quality (Effluent Feed) ----
            "effluent_feed_ph",
            "effluent_feed_TDS",
            "effluent_feed_cod",
            "effluent_feed_spgr",

            # ---- Auto calculated ----
            "atfd_qty",

            # ---- Vapor Condensate quality ----
            "vapor_contensate_ph",
            "vapor_contensate_tds",
            "vapor_contensate_cod",
        ]
        widgets = {
            "reading_date": forms.DateInput(attrs={"type": "date"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        base_cls = (
            "border border-gray-300 rounded-xl px-3 py-2 w-full text-sm "
            "focus:outline-none focus:ring-2 focus:ring-indigo-500"
        )

        for name, field in self.fields.items():
            if name == "reading_date":
                field.widget.attrs.update({"class": base_cls + " sm:w-60"})
            else:
                field.widget.attrs.update({"class": base_cls})

        # --- Auto calculated (readonly in UI, model will compute on save) ---
        if "steam_economy" in self.fields:
            self.fields["steam_economy"].required = False
            self.fields["steam_economy"].widget.attrs.update(
                {"readonly": "readonly", "class": base_cls + " bg-slate-100"}
            )

        if "atfd_qty" in self.fields:
            self.fields["atfd_qty"].required = False
            self.fields["atfd_qty"].widget.attrs.update(
                {"readonly": "readonly", "class": base_cls + " bg-slate-100"}
            )