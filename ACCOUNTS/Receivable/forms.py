from django import forms
from django.forms import inlineformset_factory
from .models import Receivable
from django.utils import timezone
from django.core.exceptions import FieldDoesNotExist

from .models import (
    Receivable,
    Party,
    PartyContact,
    OutgoingEmailAccount,
)


# ---------------------------------------------------------------------------
# ACCOUNTS/forms.py (Receivable + Targets)
# ---------------------------------------------------------------------------

class ReceivableForm(forms.ModelForm):
    class Meta:
        model = Receivable

        # Base fields that MUST exist in your model
        fields = [
            "entry_date",          # ✅ different from invoice_date
            "customer_code",       # ✅ dropdown (AJAX)
            "customer_name",       # ✅ auto-fill (readonly)
            "invoice_number",      # ✅ dropdown (AJAX)
            "invoice_date",        # ✅ auto-fill
            "due_date",            # ✅ auto-fill
            "currency",
            "invoice_amount",
            "received_amount",
            "cheque_no",
            "cheque_date",
            "status",
            "remarks",
        ]

        widgets = {
            # dates
            "entry_date": forms.DateInput(attrs={"type": "date"}),
            "invoice_date": forms.DateInput(attrs={"type": "date"}),
            "due_date": forms.DateInput(attrs={"type": "date"}),
            "cheque_date": forms.DateInput(attrs={"type": "date"}),

            # textarea
            "remarks": forms.Textarea(attrs={"rows": 2}),

            # ✅ dropdown widgets (AJAX will populate options)
            "customer_code": forms.Select(),
            "invoice_number": forms.Select(),

            # ✅ name is filled by JS; keep readonly to prevent manual mismatch
            "customer_name": forms.TextInput(attrs={"readonly": "readonly"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ✅ Prefill entry_date on create screen (and keep editable)
        if not self.instance.pk and not self.initial.get("entry_date"):
            self.initial["entry_date"] = timezone.localdate()

        # ✅ Ensure entry_date is always set (even if browser doesn't send it)
        self.fields["entry_date"].required = True

        # --- OPTIONAL PO fields (only if model actually has them) ---
        # NOTE: These are NOT in Meta.fields; we'll insert them only if present.
        optional_po = []
        for f in ("customer_po_no", "customer_po_date"):
            try:
                Receivable._meta.get_field(f)
                optional_po.append(f)
            except FieldDoesNotExist:
                pass

        # If model has PO fields, include them in this ModelForm instance
        # (this is safe even if you later remove/rename these fields)
        for f in optional_po:
            if f not in self.fields:
                # ModelForm won't create it since it's not in Meta.fields, so create it now
                model_field = Receivable._meta.get_field(f)
                self.fields[f] = model_field.formfield()

            if f == "customer_po_date":
                self.fields[f].widget = forms.DateInput(attrs={"type": "date"})

        # Reorder: entry_date first, then customer fields, then PO, then rest
        ordered = []
        for k in ("entry_date", "customer_code", "customer_name"):
            if k in self.fields:
                ordered.append(k)
        for k in ("customer_po_no", "customer_po_date"):
            if k in self.fields:
                ordered.append(k)
        ordered += [k for k in self.fields.keys() if k not in ordered]
        self.order_fields(ordered)

        # --- styling (same intent, plus some sensible defaults) ---
        base_classes = (
            "mt-1 block w-full border border-gray-300 rounded-md "
            "px-3 py-2 text-sm focus:outline-none focus:ring-1 "
            "focus:ring-blue-500 focus:border-blue-500"
        )

        for name, field in self.fields.items():
            css = field.widget.attrs.get("class", "")
            field.widget.attrs["class"] = (css + " " + base_classes).strip()

            # nice UX hints
            if name in ("invoice_amount", "received_amount"):
                field.widget.attrs.setdefault("inputmode", "decimal")

        # right-align numeric fields
        if "invoice_amount" in self.fields:
            self.fields["invoice_amount"].widget.attrs["class"] += " text-right"
        if "received_amount" in self.fields:
            self.fields["received_amount"].widget.attrs["class"] += " text-right"

        # ✅ For edit page: make sure current values are available as <option>
        # (AJAX dropdowns often render empty without an initial option)
        if self.instance.pk:
            cc = (getattr(self.instance, "customer_code", None) or "").strip()
            cn = (getattr(self.instance, "customer_name", None) or "").strip()
            inv = (getattr(self.instance, "invoice_number", None) or "").strip()

            if "customer_code" in self.fields and cc:
                # show "CODE - NAME" as the selected option
                self.fields["customer_code"].choices = [("", "---------"), (cc, f"{cc} - {cn}" if cn else cc)]
            if "invoice_number" in self.fields and inv:
                self.fields["invoice_number"].choices = [("", "---------"), (inv, inv)]

class PaymentTargetWeekForm(forms.Form):
    company_group = forms.ChoiceField(
        choices=[
            ("", "All"),
            ("ALL", "All"),
            ("OCSPL", "OCSPL"),
            ("OCCHEM", "OCCHEM"),
        ],
        required=False,
    )

    week_start = forms.DateField(widget=forms.DateInput(attrs={"type": "date"}))
    week_end = forms.DateField(widget=forms.DateInput(attrs={"type": "date"}))

    notes = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 2}))

    def clean_company_group(self):
        v = (self.cleaned_data.get("company_group") or "").strip().upper()
        # normalize ALL -> "" (if your model stores blank for ALL)
        if v == "ALL":
            return ""
        return v

    def clean(self):
        cleaned = super().clean()
        ws = cleaned.get("week_start")
        we = cleaned.get("week_end")
        if ws and we and we < ws:
            self.add_error("week_end", "Week end cannot be before week start.")
        return cleaned


class PaymentTargetSelectPartyForm(forms.Form):
    party_code = forms.CharField(required=False)
    party_name = forms.CharField(required=False)

    def clean_party_code(self):
        return (self.cleaned_data.get("party_code") or "").strip()

    def clean_party_name(self):
        return (self.cleaned_data.get("party_name") or "").strip()

    def clean(self):
        cleaned = super().clean()

        # Do NOT force party selection globally; enforce only when action=fetch.
        action = ""
        try:
            action = (self.data.get("action") or "").strip().lower()
        except Exception:
            action = ""

        if action == "fetch":
            if not cleaned.get("party_code") and not cleaned.get("party_name"):
                raise forms.ValidationError("Select a customer (party code or party name).")

        return cleaned


# ---------------------------------------------------------------------------
# accounts/forms_party.py
# ---------------------------------------------------------------------------

class PartyForm(forms.ModelForm):
    class Meta:
        model = Party
        fields = [
            "party_code",
            "party_name",
            "gst_no",
            "address",
            "is_active",
        ]
        widgets = {
            "party_code": forms.TextInput(attrs={
                "class": "w-full border border-slate-300 rounded px-3 py-2 text-sm"
            }),
            "party_name": forms.TextInput(attrs={
                "class": "w-full border border-slate-300 rounded px-3 py-2 text-sm"
            }),
            "gst_no": forms.TextInput(attrs={
                "class": "w-full border border-slate-300 rounded px-3 py-2 text-sm"
            }),
            "address": forms.Textarea(attrs={
                "rows": 3,
                "class": "w-full border border-slate-300 rounded px-3 py-2 text-sm"
            }),
            "is_active": forms.CheckboxInput(attrs={
                "class": "h-4 w-4 rounded border-slate-300"
            }),
        }


PartyContactFormSet = inlineformset_factory(
    Party,
    PartyContact,
    fields=[
        "contact_person",
        "designation",
        "email",
        "mobile",
        "phone",
        "is_primary",
        "receive_pdc_reminder",
        "is_active",
    ],
    extra=1,
    can_delete=True,
    widgets={
        "contact_person": forms.TextInput(attrs={
            "class": "border border-slate-300 rounded px-2 py-1 text-sm w-full"
        }),
        "designation": forms.TextInput(attrs={
            "class": "border border-slate-300 rounded px-2 py-1 text-sm w-full"
        }),
        "email": forms.EmailInput(attrs={
            "class": "border border-slate-300 rounded px-2 py-1 text-sm w-full"
        }),
        "mobile": forms.TextInput(attrs={
            "class": "border border-slate-300 rounded px-2 py-1 text-sm w-full"
        }),
        "phone": forms.TextInput(attrs={
            "class": "border border-slate-300 rounded px-2 py-1 text-sm w-full"
        }),
        "is_primary": forms.CheckboxInput(attrs={"class": "h-4 w-4"}),
        "receive_pdc_reminder": forms.CheckboxInput(attrs={"class": "h-4 w-4"}),
        "is_active": forms.CheckboxInput(attrs={"class": "h-4 w-4"}),
    }
)


# ---------------------------------------------------------------------------
# accounts/forms_mail.py
# ---------------------------------------------------------------------------

class OutgoingEmailAccountForm(forms.ModelForm):
    class Meta:
        model = OutgoingEmailAccount
        fields = [
            "company_group",
            "from_name",
            "from_email",
            "is_active",
        ]
        widgets = {
            "company_group": forms.Select(attrs={
                "class": "w-full border border-slate-300 rounded px-3 py-2 text-sm"
            }),
            "from_name": forms.TextInput(attrs={
                "class": "w-full border border-slate-300 rounded px-3 py-2 text-sm"
            }),
            "from_email": forms.EmailInput(attrs={
                "class": "w-full border border-slate-300 rounded px-3 py-2 text-sm"
            }),
            "is_active": forms.CheckboxInput(attrs={
                "class": "h-4 w-4 rounded border-slate-300"
            }),
        }
