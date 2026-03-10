# daily_block/forms.py
from django import forms
from django.forms import inlineformset_factory
from django.core.exceptions import ValidationError

from .models import (
    DailyCheckHeader,
    AssetsInventory,
    Block,
)

# ────────────────────────────────────────────────────────────────
#  Static choice lists (used only for the plain <select> widgets)
# ────────────────────────────────────────────────────────────────
BLOCK_CHOICES = [
    ("Block A", "Block A"), ("Block B", "Block B"),
    ("Block C", "Block C"), ("Block D", "Block D"),("Block E", "Block E"),
    
]

# ────────────────────────────────────────────────────────────────
#  Header Form
# ────────────────────────────────────────────────────────────────
class HeaderForm(forms.ModelForm):
    block = forms.ChoiceField(choices=BLOCK_CHOICES)

    class Meta:
        model  = DailyCheckHeader
        fields = (
            "transaction_number", "block", "report_dt", "remarks",
        )
        # Tailwind CSS styling is applied in the template, so class attributes are removed here.
        widgets = {
            "transaction_number": forms.NumberInput(attrs={
                "readonly": True,
            }),
            "report_dt": forms.DateTimeInput(attrs={
                "type": "datetime-local",
            }),
            "remarks": forms.Textarea(attrs={
                "rows": 4,
            }),
        }

    # ────────────────────────────────────────────────────────────
    #  Initialise readable strings when editing a record
    # ────────────────────────────────────────────────────────────
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance and self.instance.pk:
            if self.instance.block_id:
                self.initial["block"] = self.instance.block.display_name

    # ────────────────────────────────────────────────────────────
    #  Convert posted strings back to real FK objects
    # ───────────────────────────────────────────────────────────
    def clean_block(self):
        code = self.cleaned_data["block"]
        try:
            return Block.objects.get(display_name=code)
        except Block.DoesNotExist:
            raise ValidationError(f"Unknown block “{code}”")


# ────────────────────────────────────────────────────────────────
#  Inline FormSets (still present for future use)
# ────────────────────────────────────────────────────────────────
AssetsFS = inlineformset_factory(
    DailyCheckHeader, AssetsInventory,
    fields="__all__", extra=1, can_delete=True,
)