# ACCOUNTS/forms.py
from django import forms
from .models import ManualPayableEntry

BASE_INPUT = (
    "w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-800 "
    "placeholder:text-slate-400 shadow-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
)
BASE_SELECT = BASE_INPUT


class ManualPayableEntryForm(forms.ModelForm):
    class Meta:
        model = ManualPayableEntry
        fields = ["company_group", "nature", "due_date", "amount", "remarks"]
        widgets = {
            "company_group": forms.Select(attrs={"class": BASE_SELECT}),
            "nature": forms.Select(attrs={"class": BASE_SELECT}),
            "due_date": forms.DateInput(attrs={"class": BASE_INPUT, "type": "date"}),
            "amount": forms.NumberInput(attrs={"class": BASE_INPUT, "step": "0.01", "min": "0"}),
            "remarks": forms.Textarea(attrs={"class": BASE_INPUT, "rows": 2}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ✅ force dropdown order exactly as models.NATURE_CHOICES (OTHER → TAXES → FINANCING)
        self.fields["nature"].choices = ManualPayableEntry.NATURE_CHOICES

from django import forms
from .models import PayablePartyExtension

class PayablePartyExtensionForm(forms.ModelForm):
    extend_days = forms.IntegerField(min_value=0, max_value=365, initial=10)

    class Meta:
        model = PayablePartyExtension
        fields = ["party_name", "company_group", "extend_days", "active", "remarks"]

    def clean_party_name(self):
        v = (self.cleaned_data.get("party_name") or "").strip()
        if not v:
            raise forms.ValidationError("Party name is required.")
        return v
