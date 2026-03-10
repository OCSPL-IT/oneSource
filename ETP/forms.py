from django import forms
from .models import *
from django_select2.forms import Select2Widget
from django.utils.timezone import now
from datetime import date
from django.forms import HiddenInput
from .constants import *
from decimal import Decimal, ROUND_HALF_UP
import re



BLOCK_CHOICES = [
    ('', '–– Select Block ––'),   # first empty placeholder
    ('BLOCK-A', 'BLOCK-A'),
    ('BLOCK-B', 'BLOCK-B'),
    ('BLOCK-C', 'BLOCK-C'),
    ('BLOCK-D', 'BLOCK-D'),
    ('BLOCK-E', 'BLOCK-E'),
]

# pick-list that the user will see
CATEGORY_CHOICES = [
    ("", "–– Select Category ––"),
    ("Process",   "Process"),
    ("Unprocess", "Unprocess"),
]

NATURE_MAP = {
    "Process": [
        "Acidic", "Basic", "Neutral", "Sodium Cyanide Effluent", "3CHP effluent",
        "Ammonium Chloride effluent", "Spent Sulphuric Acid", "Residue",
    ],
    "Unprocess": [
        "Spent HCL", "Scrubber Basic Effluent", "Scrubber Acidic Effluent",
        "QC effluent", "Outside Drainage Water", "Dyke Effluent",
        "PCO Cleaning / cleaning Effluent", "Ejector effluent",
        "Scrubber Nox effluent",
    ],
}

class EffluentRecordForm(forms.ModelForm):
    product_name = forms.CharField(
        label="Product Name",
        widget=Select2Widget(
            attrs={
                'data-url': '/get_products/',
                'data-placeholder': 'Search Product Name',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }
        )
    )
    stage_name = forms.CharField(
        label="Stage Name",
        widget=Select2Widget(
            attrs={
                'data-url': '/get_stage_names/',
                'data-placeholder': 'Search Stage Name',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }
        )
    )
    batch_no = forms.CharField(
        label="Batch No",
        widget=Select2Widget(
            attrs={
                'data-url': '/get_batch_nos/',
                'data-placeholder': 'Search Batch No',
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }
        )
    )
    block = forms.ChoiceField(
        label="Block",
        choices=BLOCK_CHOICES,
        widget=forms.Select(attrs={
            "class": "w-full p-2 border border-gray-300 rounded-lg",
        }),
        required=True,          # or False if you really want it optional
    )
    record_date = forms.DateField(
        initial=date.today,
        widget=forms.DateInput(attrs={
            'type': 'date',
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        }) )
    voucher_no = forms.CharField(
        required=False,
        widget=HiddenInput(attrs={
            'id': 'id_voucher_no',})  )    # so your JS $('#id_voucher_no') still works
        
   
    class Meta:
        model = EffluentRecord
        fields = ['record_date','product_name', 'stage_name', 'batch_no', 'voucher_no', 'block']
       



class EffluentQtyForm(forms.ModelForm):
    CATEGORY_CHOICES = [
        ("", "–– Select Category ––"),
        ("Process",   "Process"),
        ("Unprocess", "Unprocess"),
        ]
    category = forms.ChoiceField(choices=CATEGORY_CHOICES, required=True)
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # If this is a blank form (newly added via JS), allow selection
        if not self.instance.pk:
            self.fields['category'].widget.attrs.update({
                'class': 'w-full p-2 border rounded category-select',
            })
            self.fields['effluent_nature'].widget.attrs.update({
                'class': 'w-full p-2 border rounded nature-select',
            })
        else:
            # Old prefilled rows - keep as readonly input
            self.fields['category'].widget.attrs.update({
                'readonly': 'readonly',
                'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-50'
            })
            self.fields['effluent_nature'].widget.attrs.update({
                'readonly': 'readonly',
                'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-50'
            })

    class Meta:
        model = EffluentQty
        fields = ['category', 'effluent_nature', 'plan_quantity', 'actual_quantity','quantity_kg']
        widgets = {
            'plan_quantity': forms.NumberInput(attrs={
                'readonly': 'readonly',
                'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-50'
            }),
            'actual_quantity': forms.NumberInput(attrs={
                'class': 'w-full p-2 border border-gray-300 rounded-lg'
            }),
            'quantity_kg': forms.NumberInput(attrs={
                'readonly': 'readonly',
                'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-50'
            }),
        }




class GeneralEffluentForm(forms.ModelForm):
    LOCATION_CHOICES = [
        ('', '–– Select Location ––'),
        ('BLOCK-A', 'BLOCK-A'),
        ('BLOCK-B', 'BLOCK-B'),
        ('BLOCK-C', 'BLOCK-C'),
        ('BLOCK-D', 'BLOCK-D'),
        ('BLOCK-E', 'BLOCK-E'),
        ('ETP', 'ETP'),
        ('QC', 'QC'),
    ]

    EFFLUENT_CHOICES = [
        ('', '–– Select Effluent Nature ––'),
        ('Spent HCL', 'Spent HCL'),
        ('Scrubber Basic Effluent', 'Scrubber Basic Effluent'),
        ('Scrubber Acidic Effluent', 'Scrubber Acidic Effluent'),
        ('QC effluent', 'QC effluent'),
        ('Outside Drainage Water', 'Outside Drainage Water'),
        ('Dyke Effluent', 'Dyke Effluent'),
        ('PCO Cleaning / cleaning Effluent', 'PCO Cleaning / cleaning Effluent'),
        ('Ejector effluent', 'Ejector effluent'),
        ('Scrubber Nox effluent', 'Scrubber Nox effluent'),
    ]

    record_date = forms.DateField(
        initial=date.today,
        widget=forms.DateInput(attrs={
            'type': 'date',
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )

    location = forms.ChoiceField(
        choices=LOCATION_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        })
    )

    effluent_nature = forms.ChoiceField(
        choices=EFFLUENT_CHOICES,
        widget=forms.Select(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg'
        }),
        required=False
    )

    actual_quantity = forms.FloatField(
        widget=forms.NumberInput(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'step': 'any',
            'placeholder': 'Enter quantity'
        }),
        required=False
    )

    class Meta:
        model = GeneralEffluent
        fields = ['record_date', 'location', 'effluent_nature', 'actual_quantity']




# NEW: choices for chemicals used (labels exactly as you provided)
CHEMICAL_USED_CHOICES = [
    ('Caustic Lye(KL)', 'Caustic Lye(KL)'),
    ('Hydrochloric Acid(KL)', 'Hydrochloric Acid(KL)'),
    ('Hydrogen Peroxide(Kg)', 'Hydrogen Peroxide(Kg)'),
    ('Copper Sulphate(Kg)', 'Copper Sulphate(Kg)'),
    ('Alum(Kg)', 'Alum(Kg)'),
    ('Poly-electrolyte(Kg)', 'Poly-electrolyte(Kg)'),
]

class PrimaryTreatmentEffluentForm(forms.ModelForm):
    class Meta:
        model = PrimaryTreatmentEffluent
        fields = ["date", "effluent_nature", "effluent_received", "effluent_neutralized"]
        widgets = {
            "date": forms.DateInput(
                attrs={
                    "type": "date",
                    "class": "mt-1 py-2 px-2 block w-full rounded-md border  border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm",
                    "id": "id_date"
                }
            ),
            "effluent_nature": forms.Select(
                attrs={
                    "class": "mt-1 py-2 px-2 block w-full rounded-md border  border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm",
                    "id": "id_effluent_nature"
                }
            ),
            "effluent_received": forms.NumberInput(
                attrs={
                    "step": "0.01",
                    "class": "mt-1 py-2 px-2 block w-full rounded-md border border-gray-300 bg-gray-100 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm",
                    "id": "id_effluent_received",
                    "readonly": True  # Made readonly as it's auto-calculated
                }
            ),
            "effluent_neutralized": forms.NumberInput(
                attrs={
                    "step": "0.01",
                    "class": "mt-1 py-2 px-2 block w-full rounded-md border border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 sm:text-sm",
                    "id": "id_effluent_neutralized"
                }
            ),
        }
    
        


# ----------------------------------------------------------------------------------------------



# assumes these exist/imported above:
# TRANSPORTER_VEHICLES, TYPE_OF_WASTE_CHOICES, FACILITY_WASTE_RATES
OTHER = "Other"
_VRN_REGEX = re.compile(r"^[A-Z]{2}\d{1,2}[A-Z]{0,3}\d{4}$")  # e.g., MH04LQ2879

_TW_INPUT  = "mt-1 py-2 px-3 block w-full rounded-lg border border-gray-300 shadow-sm focus:border-indigo-500 focus:ring-2 focus:ring-indigo-500 sm:text-sm"
_TW_SELECT = _TW_INPUT
_TW_DATE   = _TW_INPUT

# dropdown for disposal_facility choices (keep where you maintain constants)
DISPOSAL_FACILITY_CHOICES = [
    ("M/s Maharashtra Enviro Power Ltd. (MEPL) Ranjangaon, Pune",
     "M/s Maharashtra Enviro Power Ltd. (MEPL) Ranjangaon, Pune"),
    ("M/s. Hazardous Waste Management System", "M/s. Hazardous Waste Management System"),
    ("M/s. Green Gene Enviro Protection & Infrastructure Pvt. Ltd.",
     "M/s. Green Gene Enviro Protection & Infrastructure Pvt. Ltd."),
    ("M/S. Ferric Flow Private Ltd, Plot No G, 7/9, Near Cummins India pvt Ltd",
     "M/S. Ferric Flow Private Ltd, Plot No G, 7/9, Near Cummins India pvt Ltd"),
    ("M/S. Greenfield CET Plant Pvt Ltd, P-17, Chincholi MIDC, Solapur",
     "M/S. Greenfield CET Plant Pvt Ltd, P-17, Chincholi MIDC, Solapur"),
]

class HazardousWasteForm(forms.ModelForm):
    # Transporter dropdown (we’ll override choices in __init__)
    transporter_name = forms.ChoiceField(
        choices=[],  # set in __init__
        widget=forms.Select(attrs={"class": _TW_SELECT, "id": "id_transporter_name"})
    )

     # 🔹 NEW: manual transporter name (not a model field)
    transporter_name_manual = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            "class": _TW_INPUT,
            "id": "id_transporter_name_manual",
            "placeholder": "Enter transporter name",
        })
    )

    # single-select dropdown (final value saved back to model field)
    vehicle_registration_numbers = forms.ChoiceField(
        required=False,
        choices=[],  # set dynamically in __init__
        widget=forms.Select(attrs={"class": _TW_SELECT, "id": "id_vehicle_registration_numbers"})
    )

    # manual input only when user picks "Other" (not a model field)
    vehicle_registration_manual = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            "class": _TW_INPUT,
            "id": "id_vehicle_registration_manual",
            "placeholder": "Enter vehicle number (e.g., MH04LQ2879)"
        })
    )

    type_of_waste = forms.ChoiceField(
        required=False,
        choices=[("", "— Select Type —")] + TYPE_OF_WASTE_CHOICES,
        widget=forms.Select(attrs={"class": _TW_SELECT, "id": "id_type_of_waste"})
    )

    disposal_facility = forms.ChoiceField(
        choices=[("", "— Select Facility —")] + DISPOSAL_FACILITY_CHOICES,
        widget=forms.Select(attrs={"class": _TW_SELECT, "id": "id_disposal_facility"})
    )
    
    waste_category = forms.CharField(
        required=False,
        widget=forms.HiddenInput(attrs={
            "class": _TW_INPUT,
            "readonly": "readonly",
            "placeholder": "Auto-filled (e.g., 20.3)" })
    )

    class Meta:
        from .models import HazardousWaste  # adjust import path if needed
        model = HazardousWaste
        fields = [
            "date",                    # 1) Date
            "challan_no",              # 2) Challan no
            "manifest_no",             # 3) Manifest No
            "transporter_name",        # 4) Transporter
            "vehicle_registration_numbers",  # 5) Vehicle Registration No(s)
            "disposal_facility",       # 6) Disposal Facility
            "type_of_waste",           # 7) Type of Waste
            "waste_category",         
            "quantity_mt",             # 8) Quantity (MT)
            "disposal_rate_rs_per_mt", # 9) Disposal Rate (Rs/MT)
            "transportation_cost",     # 10) Transportation Cost
            "total_cost",              # 11) Total Cost
            "disposal_method",         # 12) Disposal Method
            "license_valid_upto",      # 13) License Valid Upto
        ]
        widgets = {
            "date": forms.DateInput(attrs={"type": "date", "class": _TW_DATE, "id": "id_date"}),
            "challan_no": forms.TextInput(attrs={"class": _TW_INPUT}),
            "manifest_no": forms.TextInput(attrs={"class": _TW_INPUT}),
            "quantity_mt": forms.NumberInput(attrs={"step": "0.001", "class": _TW_INPUT, "id": "id_quantity_mt"}),
            "disposal_rate_rs_per_mt": forms.HiddenInput(attrs={"id": "id_disposal_rate"}),
            "transportation_cost":     forms.HiddenInput(attrs={"id": "id_transport_cost"}),
            "total_cost":              forms.HiddenInput(attrs={"id": "id_total_cost"}),
            "disposal_method": forms.Select(attrs={"class": _TW_SELECT}),
            "license_valid_upto": forms.DateInput(attrs={"type": "date", "class": _TW_DATE}),
        }

    # ───────────────────────────────────────────────────────────────────
    # init: set date default, build vehicle choices, prefill on edit
    # ───────────────────────────────────────────────────────────────────
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 🔹 Build transporter choices (add "Other")
        trans_choices = [("", "— Select Transporter —")] + \
                        [(k, k) for k in TRANSPORTER_VEHICLES.keys()] + \
                        [(OTHER, "Other (manual)")]
        self.fields["transporter_name"].choices = trans_choices

        # default date on new forms
        if not self.is_bound and not (self.instance and self.instance.pk and self.instance.date):
            self.fields["date"].initial = date.today()

        # vehicle options depend on transporter (create/edit)
        tname = self.data.get("transporter_name") or (
            self.instance.transporter_name if self.instance and self.instance.pk else ""
        )
        vehicles = TRANSPORTER_VEHICLES.get(tname, [])
        if OTHER not in vehicles:
            vehicles = [*vehicles, OTHER] if vehicles else [OTHER]
        self.fields["vehicle_registration_numbers"].choices = \
            [("", "— Select Vehicle —")] + [(v, v) for v in vehicles]

        # Prefill waste_category if edit / bound
        tw = None
        if self.is_bound:
            tw = (self.data.get("type_of_waste") or "").strip()
        elif self.instance and self.instance.pk:
            tw = (self.instance.type_of_waste or "").strip()
        if tw:
            self.fields["waste_category"].initial = WASTE_CATEGORY_BY_TYPE.get(tw, "")

        # 🔹 Editing existing object: handle manual transporter & vehicle
        if self.instance and self.instance.pk:
            # transporter
            current_t = (self.instance.transporter_name or "").strip()
            if current_t:
                valid_vals = {val for val, _ in trans_choices}
                if current_t in valid_vals:
                    self.initial["transporter_name"] = current_t
                else:
                    # was manual
                    self.initial["transporter_name"] = OTHER
                    self.initial["transporter_name_manual"] = current_t

            # vehicle
            current_vrn = (self.instance.vehicle_registration_numbers or "").strip()
            if current_vrn:
                existing = {c[0] for c in self.fields["vehicle_registration_numbers"].choices}
                if current_vrn in existing:
                    self.initial["vehicle_registration_numbers"] = current_vrn
                else:
                    self.initial["vehicle_registration_numbers"] = OTHER
                    self.initial["vehicle_registration_manual"] = current_vrn

            if self.instance.type_of_waste:
                self.initial["type_of_waste"] = self.instance.type_of_waste.strip()
            if self.instance.disposal_facility:
                self.initial["disposal_facility"] = self.instance.disposal_facility.strip()

    # ───────────────── helpers / clean ─────────────────
    def _normalize_vrn(self, s: str) -> str:
        return (s or "").replace(" ", "").replace("-", "").upper()

    def clean_type_of_waste(self):
        return (self.cleaned_data.get("type_of_waste") or "").strip()

    def clean(self):
        cleaned = super().clean()

        # 🔹 Transporter handling
        selected_t = (cleaned.get("transporter_name") or "").strip()
        manual_t   = (cleaned.get("transporter_name_manual") or "").strip()

        if selected_t == OTHER:
            if not manual_t:
                self.add_error("transporter_name_manual",
                               "Please enter the transporter name for 'Other'.")
            else:
                cleaned["transporter_name"] = manual_t
        elif not selected_t and manual_t:
            # user skipped dropdown but typed manually
            cleaned["transporter_name"] = manual_t

        # 🔹 Vehicle handling (unchanged logic, just moved down)
        selected = (cleaned.get("vehicle_registration_numbers") or "").strip()
        manual   = (cleaned.get("vehicle_registration_manual") or "").strip()

        # Auto-set waste_category from type_of_waste (server-side guard)
        tw = (cleaned.get("type_of_waste") or "").strip()
        mapped = WASTE_CATEGORY_BY_TYPE.get(tw)
        if mapped:
            cleaned["waste_category"] = mapped

        if selected == OTHER:
            if not manual:
                self.add_error("vehicle_registration_manual",
                               "Please enter the vehicle number for 'Other'.")
            else:
                norm = self._normalize_vrn(manual)
                if not _VRN_REGEX.match(norm):
                    self.add_error("vehicle_registration_manual",
                                   "Vehicle number looks invalid. Example: MH04LQ2879")
                else:
                    cleaned["vehicle_registration_numbers"] = norm

        elif not selected and manual:
            norm = self._normalize_vrn(manual)
            if not _VRN_REGEX.match(norm):
                self.add_error("vehicle_registration_manual",
                               "Vehicle number looks invalid. Example: MH04LQ2879")
            else:
                cleaned["vehicle_registration_numbers"] = norm

        # Autofill rate/transport from mapping (if missing)
        fac   = (cleaned.get("disposal_facility") or "").strip()
        waste = (cleaned.get("type_of_waste") or "").strip()
        mapping = FACILITY_WASTE_RATES.get(fac, {}).get(waste)

        if mapping:
            if not cleaned.get("disposal_rate_rs_per_mt"):
                cleaned["disposal_rate_rs_per_mt"] = Decimal(str(mapping["rate"]))

            # default transport from mapping (will be overridden for MEPL below)
            if cleaned.get("transportation_cost") in (None, ""):
                cleaned["transportation_cost"] = Decimal(str(mapping.get("transport", 0)))

        # ✅ NEW RULE: only for MEPL facility (qty based transport)
        MEPL_FACILITY = "M/s Maharashtra Enviro Power Ltd. (MEPL) Ranjangaon, Pune"
        if fac == MEPL_FACILITY:
            q_qty = cleaned.get("quantity_mt") or Decimal("0")
            cleaned["transportation_cost"] = Decimal("65000") if q_qty > Decimal("15") else Decimal("49000")

        # Compute total cost
        q  = cleaned.get("quantity_mt") or Decimal("0")
        r  = cleaned.get("disposal_rate_rs_per_mt") or Decimal("0")
        tc = cleaned.get("transportation_cost") or Decimal("0")
        cleaned["total_cost"] = (q * r + tc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        return cleaned


    # ───────────────────────────────────────────────────────────────────
    # save: ensure total_cost persisted (also handled in clean)
    # ───────────────────────────────────────────────────────────────────
    def save(self, commit=True):
        obj = super().save(commit=False)
        q  = self.cleaned_data.get("quantity_mt") or Decimal("0")
        r  = self.cleaned_data.get("disposal_rate_rs_per_mt") or Decimal("0")
        tc = self.cleaned_data.get("transportation_cost") or Decimal("0")
        obj.total_cost = (q * r + tc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if commit:
            obj.save()
        return obj
    
    def clean_challan_no(self):
        val = (self.cleaned_data.get("challan_no") or "").strip()
        if not val:
            return val
        qs = HazardousWaste.objects.filter(challan_no__iexact=val)
        if self.instance and self.instance.pk:
            qs = qs.exclude(pk=self.instance.pk)
        if qs.exists():
            raise forms.ValidationError("DC challan already present.")
        return val


# ====================================================================================================    
# ───────────────────────────────────────────────────────────────────
# Below code is for Effluent storage tank
# ───────────────────────────────────────────────────────────────────
_TW_INPUT = (
    "mt-1 block w-full rounded-lg border border-gray-300 px-3 py-2 shadow-sm "
    "focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
)

class OpeningBalanceBulkForm(forms.Form):
    month = forms.DateField(
        # accept both YYYY-MM and YYYY-MM-DD on submit
        input_formats=["%Y-%m", "%Y-%m-%d"],
        # render value as YYYY-MM so <input type="month"> shows it
        widget=forms.DateInput(format="%Y-%m", attrs={"type": "month", "class": _TW_INPUT}),
        help_text="Select month (stores as the 1st of the month).",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ensure the widget keeps this format when re-rendered
        self.fields["month"].widget.format = "%Y-%m"
        self.fields["month"].widget.attrs.update({"class": _TW_INPUT})

        for tank in EffluentTank.objects.all().order_by("name"):
            self.fields[f"tank_{tank.id}"] = forms.DecimalField(
                max_digits=10, decimal_places=2, min_value=0, required=False,
                label=f"{tank.name} opening balance",
                widget=forms.NumberInput(attrs={
                    "step": "0.01", "placeholder": "0.00",
                    "class": _TW_INPUT, "inputmode": "decimal",
                }),
            )

    def clean_month(self):
        m = self.cleaned_data["month"]
        return m.replace(day=1)  # store as the first of the month

    def save(self):
        m = self.cleaned_data["month"]  # already coerced to first of month
        for tank in EffluentTank.objects.all():
            val = self.cleaned_data.get(f"tank_{tank.id}")
            if val is None:
                continue
            EffluentOpeningBalance.objects.update_or_create(
                tank=tank, month=m, defaults={"opening_balance": val}
            )
        return m