from django import forms
from .models import DomesticETATracking


_BASE_INPUT = (
    "mt-1 block w-full rounded-md border border-slate-300 "
    "shadow-sm px-3 py-2 text-sm "
    "focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
)


class DomesticETATrackingForm(forms.ModelForm):
    # Not a model field – used only for upload
    photo_file = forms.FileField(
        required=False,
        label="Photos",
        widget=forms.ClearableFileInput(
            attrs={
                "class": "mt-1 block w-full text-sm text-slate-900 border border-slate-300 "
                         "rounded-md cursor-pointer bg-slate-50 focus:outline-none "
                         "focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
            }
        ),
        help_text="Upload supporting photo / document (optional)",
    )

    class Meta:
        model = DomesticETATracking
        # NOTE: Photos (BinaryField) is *not* listed, we handle it via photo_file
        fields = [
            "PoNumber",
            "Status",
            "RequiredDate",
            "ETDDate",
            "RevisedETADate",
            "RawMaterial",
            "Packing",
            "Qty",
            "FreightCharges",  # ? NEW FIELD
            "Supplier",
            "LiftingLocation",
            "TransporterName",
            "VehicleNo",
            "LRNo",
            "DriverNo",
            "Evaluation",
            "InvoiceNoRemark",
            "InvoiceDate",
            "photo_file",
            "Remark",
        ]
        widgets = {
            "PoNumber": forms.TextInput(attrs={"class": _BASE_INPUT}),
            "Status": forms.Select(attrs={"class": _BASE_INPUT}),
            "RequiredDate": forms.DateInput(
                attrs={"type": "date", "class": _BASE_INPUT}
            ),
            "ETDDate": forms.DateInput(
                attrs={"type": "date", "class": _BASE_INPUT}
            ),
            "FreightCharges": forms.NumberInput(
            attrs={"class": _BASE_INPUT, "step": "0.01"}
            ),
            "ETADate": forms.DateInput(
                attrs={"type": "date", "class": _BASE_INPUT}
            ),
            "RevisedETADate": forms.DateInput(
                attrs={"type": "date", "class": _BASE_INPUT}
            ),
            "RawMaterial": forms.TextInput(attrs={"class": _BASE_INPUT}),
            "Packing": forms.TextInput(attrs={"class": _BASE_INPUT}),
            "Qty": forms.NumberInput(
                attrs={"class": _BASE_INPUT, "step": "0.001"}
            ),
            "Supplier": forms.TextInput(attrs={"class": _BASE_INPUT}),
            "LiftingLocation": forms.TextInput(attrs={"class": _BASE_INPUT}),
            "TransporterName": forms.TextInput(
                attrs={"class": _BASE_INPUT, "list": "transporter_list"}
            ),
            "VehicleNo": forms.TextInput(attrs={"class": _BASE_INPUT}),
            "LRNo": forms.TextInput(attrs={"class": _BASE_INPUT}),
            "DriverNo": forms.TextInput(attrs={"class": _BASE_INPUT}),
            # ?? CHANGED: now a dropdown, not textarea
            "Evaluation": forms.Select(attrs={"class": _BASE_INPUT}),
            "InvoiceNoRemark": forms.TextInput(attrs={"class": _BASE_INPUT}),
            "InvoiceDate": forms.DateInput(
                attrs={"type": "date", "class": _BASE_INPUT}
            ),
            "Remark": forms.Textarea(
                attrs={
                    "class": _BASE_INPUT + " resize-y",
                    "rows": 3,
                }
            ),
        }

    def save(self, commit=True):
        """
        Save uploaded file into the BinaryField `Photos`.
        """
        instance = super().save(commit=False)

        upload = self.cleaned_data.get("photo_file")
        if upload:
            # read all bytes into the BinaryField
            instance.Photos = upload.read()

        # if user does NOT upload anything on edit, keep existing instance.Photos

        if commit:
            instance.save()
        return instance
