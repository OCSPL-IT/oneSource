from django import forms
from .models import Vehicle,MaterialRequest
from datetime import date
from django.db import connections
from django_select2.forms import Select2Widget, Select2MultipleWidget
from STORE import urls

class VehicleForm(forms.ModelForm):
    name_of_supplier = forms.CharField(
        widget=Select2Widget(
            attrs={'data-url': '/search_supplier/', 'class': 'w-full p-2 border border-gray-300 rounded-lg'}
        )
    )
    
    material = forms.CharField(
        widget=Select2Widget(
            attrs={'data-url': '/search_item/', 'class': 'w-full p-2 border border-gray-300 rounded-lg'}
        )
    )
    unloading_days = forms.IntegerField(
        required=False,
        widget=forms.HiddenInput()  # Makes it hidden but still part of form submission
    )
    qty = forms.FloatField(
        required=False,
        widget=forms.NumberInput(attrs={
            'class': 'w-full p-2 border border-gray-300 rounded-lg',
            'step': 'any',
            'inputmode': 'decimal'
        })  
    )

    class Meta:
        model = Vehicle
        fields = [
            'record_date', 'invoice', 'name_of_supplier', 'material', 'unit', 'qty',
            'reporting_date', 'report_time', 'unloading_date', 'unloading_time','unloading_days',
            'vehicle_no', 'name_of_transporter', 'status', 'manufacture', 'remark'
        ]
        widgets = {
            'record_date': forms.DateInput(attrs={'type': 'date','readonly':'readonly', 'class': 'w-full p-2 border border-gray-300 rounded-lg bg-gray-100'}),
            'invoice': forms.TextInput(attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
            'unit': forms.TextInput(attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
            'reporting_date': forms.DateInput(attrs={'type': 'date', 'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
            'report_time': forms.TimeInput(attrs={'type': 'time', 'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
            'unloading_date': forms.DateInput(attrs={'type': 'date', 'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
            'unloading_time': forms.TimeInput(attrs={'type': 'time', 'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
            'vehicle_no': forms.TextInput(attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
            'name_of_transporter': forms.TextInput(attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
            'status': forms.Select(choices=[('Pending', 'Pending'), ('Unloaded', 'Unloaded')], attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
            'manufacture': forms.TextInput(attrs={'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
            'remark': forms.Textarea(attrs={'rows': 3, 'class': 'w-full p-2 border border-gray-300 rounded-lg'}),
        }
    
    def __init__(self, *args, **kwargs):
        super(VehicleForm, self).__init__(*args, **kwargs)

        # Set every field to not required
        for field_name, field in self.fields.items():
            field.required = False
        
        if self.instance and self.instance.pk:
            # Pre-populating the Supplier and Material Select2 fields
            self.fields['name_of_supplier'].initial = self.instance.name_of_supplier
            self.fields['material'].initial = self.instance.material

    def clean(self):
        cleaned_data = super().clean()
        reporting_date = cleaned_data.get("reporting_date")
        unloading_date = cleaned_data.get("unloading_date")

        if reporting_date and unloading_date:
            unloading_days = (unloading_date - reporting_date).days
            cleaned_data["unloading_days"] = unloading_days
            # cleaned_data["status"] = "unloaded" if unloading_days >= 0 else "pending"

        return cleaned_data
    
    def clean_qty(self):
        value = self.cleaned_data.get('qty')
        print(f"[DEBUG] Cleaned qty value: {value} (type: {type(value)})")
        return value

from django import forms
from .models import MaterialRequest

class MaterialRequestForm(forms.ModelForm):
    tentative_date = forms.DateField(
        widget=forms.DateInput(attrs={
            "type": "date",
            "class": (
                "w-full rounded-md border-gray-300 shadow-sm "
                "focus:border-indigo-500 focus:ring focus:ring-indigo-200 "
                "focus:ring-opacity-50"
            )
        }),
        label="Tentative Date"
    )

    class Meta:
        model  = MaterialRequest
        fields = ["type", "material_name", "trade_name", "unit", "qty", "tentative_date"]
        widgets = {
            "type": forms.Select(attrs={
                "class": (
                    "w-full rounded-md border-gray-300 pr-8 shadow-sm "
                    "focus:border-indigo-500 focus:ring focus:ring-indigo-200 "
                    "focus:ring-opacity-50"
                )
            }),
            "material_name": forms.TextInput(attrs={
                "class": (
                    "w-full rounded-md border-gray-300 shadow-sm "
                    "focus:border-indigo-500 focus:ring focus:ring-indigo-200 "
                    "focus:ring-opacity-50"
                )
            }),
            "trade_name": forms.TextInput(attrs={
                "class": (
                    "w-full rounded-md border-gray-300 shadow-sm "
                    "focus:border-indigo-500 focus:ring focus:ring-indigo-200 "
                    "focus:ring-opacity-50"
                )
            }),
            "unit": forms.TextInput(attrs={
                "class": (
                    "w-full rounded-md border-gray-300 shadow-sm "
                    "focus:border-indigo-500 focus:ring focus:ring-indigo-200 "
                    "focus:ring-opacity-50"
                ),
                "placeholder": "e.g. Kg / Litre / Nos"
            }),
            "qty": forms.NumberInput(attrs={
                "step": "0.001",
                "class": (
                    "w-full rounded-md border-gray-300 shadow-sm "
                    "focus:border-indigo-500 focus:ring focus:ring-indigo-200 "
                    "focus:ring-opacity-50"
                )
            }),
        }



# -----------------------------------------------------------
# Rack_RM Store
# -----------------------------------------------------------

# STORE/forms.py
from django import forms
from .models import RackAllocation, Rack, Pallet

class AllocationForm(forms.ModelForm):
    class Meta:
        model = RackAllocation
        fields = ["rack", "pallet", "allocated_qty"]

    rack = forms.ModelChoiceField(queryset=Rack.objects.filter(is_active=True).order_by("code"))
    pallet = forms.ModelChoiceField(queryset=Pallet.objects.filter(is_active=True).order_by("number"), required=False)

# STORE/forms.py
from django import forms
from .models import RackAllocation, Rack, Pallet

class AllocationForm(forms.ModelForm):
    rack   = forms.ModelChoiceField(queryset=Rack.objects.filter(is_active=True).order_by("code"))
    pallet = forms.ModelChoiceField(queryset=Pallet.objects.none(), required=False)

    class Meta:
        model  = RackAllocation
        fields = ["rack", "pallet", "allocated_qty"]

    def __init__(self, *args, **kwargs):
        rack_initial = kwargs.pop("rack_initial", None)
        super().__init__(*args, **kwargs)
        # If a rack is preselected (POST or initial), narrow pallets
        rack = None
        if self.data.get("rack"):
            rack = Rack.objects.filter(pk=self.data.get("rack")).first()
        elif rack_initial:
            rack = rack_initial
        elif getattr(self.instance, "rack_id", None):
            rack = self.instance.rack
        if rack:
            self.fields["pallet"].queryset = rack.pallets.filter(is_active=True).order_by("number")

