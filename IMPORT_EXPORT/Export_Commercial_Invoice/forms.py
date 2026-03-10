from decimal import Decimal, InvalidOperation
from django import forms
from .models import InvoicePostShipment
from django.core.exceptions import ValidationError 

BASE_INPUT_CLASS = (
    "w-full border border-slate-300 rounded-md px-2 py-1 text-sm "
    "focus:outline-none focus:ring-1 focus:ring-emerald-500 bg-white"
)

BASE_READONLY_CLASS = (
    "w-full border border-slate-300 rounded-md px-2 py-1 text-sm "
    "bg-slate-100 text-slate-700 cursor-not-allowed"
)

BASE_TEXTAREA_CLASS = (
    "w-full border border-slate-300 rounded-md px-2 py-1 text-sm "
    "focus:outline-none focus:ring-1 focus:ring-emerald-500 min-h-[70px] bg-white"
)


class InvoicePostShipmentForm(forms.ModelForm):
     # use a FileField in the form, but store bytes in the model
    attachment_file = forms.FileField(
        required=False,
        label="Attachment (Image / PDF / Excel)",
        widget=forms.ClearableFileInput(
            attrs={"class": BASE_INPUT_CLASS}
        ),
    )
    
    class Meta:
        model = InvoicePostShipment
        fields = [
            # Header
            "transaction_type",
            "invoice_number",
            "invoice_date",
            "buyers_order_no",
            "buyers_order_date",

            # Consignee / Notify
            "consignee_name",
            "consignee_address",
            "notify_party1_name",
            "notify_party1_address",
            "notify_party2_name",
            "notify_party2_address",

            # Origin / Destination
            "country_origin",
            "country_destination",
            "district_origin",
            "state_origin",
            "vessel_name_no",
            "port_loading",
            "port_discharge",
            "final_destination",
            "preferential_agreement",
            "standard_unit_qty_code",

            # Terms / Bank
            "delivery",
            "shipment_mode",
            "payment_terms",
            "due_date",
            "bank_name",
            "bank_account_no",
            "ad_code",
            "swift_code",
            "bank_address",

            # Marks & Container / Packing
            "product_name",
            "container_no",
            "merks_and_container_no",
            "merks_and_container_no1",
            "packing_details",

            # Description of Goods
            "description_of_goods",
            "item_no",
            "hsn_no",
            "quantity",
            "quantity_unit",
            "rate_usd",
            "amount_usd",
            "gross_wt",
            "conversion_rate",
            # BL / Shipping Bill
            "bl_number",
            "bl_date",
            "shipping_bill_no",
            "shipping_bill_date",
            # "attachment",
        ]

        widgets = {
            # Header
            "transaction_type": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "invoice_number": forms.TextInput(attrs={"class": BASE_INPUT_CLASS, "required": "required"}),
            "invoice_date": forms.DateInput(attrs={"type": "date", "class": BASE_INPUT_CLASS}),
            "buyers_order_no": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "buyers_order_date": forms.DateInput(attrs={"type": "date", "class": BASE_INPUT_CLASS}),

            # Consignee / Notify
            "consignee_name": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "consignee_address": forms.Textarea(attrs={"class": BASE_TEXTAREA_CLASS, "rows": 4}),
            "notify_party1_name": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "notify_party1_address": forms.Textarea(attrs={"class": BASE_TEXTAREA_CLASS, "rows": 4}),
            "notify_party2_name": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "notify_party2_address": forms.Textarea(attrs={"class": BASE_TEXTAREA_CLASS, "rows": 4}),

            # Origin / Destination
            "country_origin": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "country_destination": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "district_origin": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "state_origin": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "vessel_name_no": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "port_loading": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "port_discharge": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "final_destination": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "preferential_agreement": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "standard_unit_qty_code": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),

            # Terms / Bank
            "delivery": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "shipment_mode": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "payment_terms": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "due_date": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "bank_name": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "bank_account_no": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "ad_code": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "swift_code": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "bank_address": forms.Textarea(attrs={"class": BASE_TEXTAREA_CLASS, "rows": 2}),

            # Marks & Container / Packing
            "product_name": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "container_no": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "merks_and_container_no": forms.Textarea(attrs={"class": BASE_INPUT_CLASS, "rows": 3}),
            "merks_and_container_no1": forms.Textarea(attrs={"class": BASE_INPUT_CLASS, "rows": 3}),
            "packing_details": forms.Textarea(attrs={"class": BASE_TEXTAREA_CLASS, "rows": 3}),

            # Description of Goods
            "description_of_goods": forms.Textarea(attrs={"class": BASE_INPUT_CLASS, "rows": 3}),
            "item_no": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "hsn_no": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "quantity": forms.NumberInput(attrs={"class": BASE_READONLY_CLASS, "step": "0.001"}),
            "quantity_unit": forms.TextInput(attrs={"class": BASE_READONLY_CLASS}),
            "rate_usd": forms.NumberInput(attrs={"class": BASE_READONLY_CLASS, "step": "0.0001"}),
            "amount_usd": forms.NumberInput(attrs={"class": BASE_READONLY_CLASS, "step": "0.01"}),
            "gross_wt": forms.TextInput(attrs={"class": BASE_READONLY_CLASS}),
            "conversion_rate": forms.NumberInput(attrs={"class": BASE_READONLY_CLASS, "step": "0.000001"}),
            # BL / Shipping Bill
            "bl_number": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "bl_date": forms.DateInput(attrs={"type": "date", "class": BASE_INPUT_CLASS}),
            "shipping_bill_no": forms.TextInput(attrs={"class": BASE_INPUT_CLASS}),
            "shipping_bill_date": forms.DateInput(attrs={"type": "date", "class": BASE_INPUT_CLASS}),
        }

    READONLY_FIELDS = ["quantity", "quantity_unit", "rate_usd", "amount_usd", "gross_wt","item_no","hsn_no","conversion_rate"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ✅ readonly (sent in POST) instead of disabled (NOT sent in POST)
        for f in self.READONLY_FIELDS:
            if f in self.fields:
                self.fields[f].widget.attrs["readonly"] = "readonly"
                self.fields[f].widget.attrs["class"] = BASE_READONLY_CLASS
                
        # ✅ make bank_name required at form level
        self.fields["bank_name"].required = True
        self.fields["bank_name"].widget.attrs["required"] = "required"
    
    def clean_bank_name(self):
        """
        Extra safety: bank_name must not be empty / spaces.
        """
        value = (self.cleaned_data.get("bank_name") or "").strip()
        if not value:
            raise ValidationError("Bank Name is required.")
        return value

    def clean(self):
        cleaned = super().clean()

        qty = cleaned.get("quantity")
        rate = cleaned.get("rate_usd")

        if qty is not None and rate is not None:
            try:
                amt = (Decimal(qty) * Decimal(rate)).quantize(Decimal("0.01"))
            except (InvalidOperation, TypeError):
                amt = None
            cleaned["amount_usd"] = amt

        return cleaned

    def clean_gross_wt(self):
        v = (self.cleaned_data.get("gross_wt") or "").strip()
        return v or None

    def save(self, commit=True):
        obj = super().save(commit=False)

        qty = self.cleaned_data.get("quantity")
        rate = self.cleaned_data.get("rate_usd")

        if qty is not None and rate is not None:
            obj.amount_usd = (Decimal(qty) * Decimal(rate)).quantize(Decimal("0.01"))

        # handle attachment (store bytes in BinaryField)
        uploaded = self.cleaned_data.get("attachment_file")
        if uploaded:
            # read() returns bytes; safe to store in BinaryField
            obj.attachment = uploaded.read()
            # ✅ store original file name too
            obj.attachment_name = uploaded.name

        if commit:
            obj.save()
        return obj
