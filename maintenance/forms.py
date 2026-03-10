# maintenance/forms.py
from django import forms
from django.utils import timezone
from .models import MaintenanceSchedule

class MaintenanceUpdateForm(forms.ModelForm):
    class Meta:
        model = MaintenanceSchedule
        fields = [
            "status",
            "downtime_minutes",
            "downtime_reason",
            "notes",
            "rescheduled_to",
        ]
        widgets = {
            "status": forms.Select(attrs={"class": "form-select"}),
            "downtime_minutes": forms.NumberInput(attrs={"class": "form-control", "min": 0}),
            "downtime_reason": forms.Textarea(attrs={"class": "form-control", "rows": 3}),
            "notes": forms.Textarea(attrs={"class": "form-control", "rows": 3}),
            "rescheduled_to": forms.DateInput(attrs={"type": "date", "class": "form-control"}),
        }

    def clean(self):
        cleaned = super().clean()
        status = cleaned.get("status")
        rescheduled_to = cleaned.get("rescheduled_to")

        if status == MaintenanceSchedule.STATUS_POSTPONED and not rescheduled_to:
            self.add_error("rescheduled_to", "Please choose the new date when postponing.")

        return cleaned
