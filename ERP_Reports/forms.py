from django import forms

class GST2BUploadForm(forms.Form):
    file = forms.FileField(
        label="GST 2B file (CSV or Excel)",
        help_text="Columns must include: GSTIN of supplier, Trade/Legal name, Invoice number, Invoice Date, Invoice Value(₹)"
    )
    replace_month = forms.BooleanField(
        required=False,
        initial=True,
        label="Replace existing GST-2B rows for that month"
    )
