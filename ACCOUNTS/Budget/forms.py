from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from django.core.exceptions import ImproperlyConfigured
from django.db import models as dj_models
from typing import List, Optional, Set
from .models import ProductionBudgetFGEffluentLine
from .models import ProductionNorm 

from django import forms
from django.core.cache import cache
from django.forms import (
    inlineformset_factory,
    BaseInlineFormSet,
    modelformset_factory,
)

from .models import *

from .models import (
    GLAccount, AccountCategory, AccountSubcategory,
    DepartmentBudgetHead,
    BudgetPlan, BudgetLine,
    BudgetLineAttachment,

    # ----------------------------
    # ✅ PRODUCTION BOM (MASTER + LINES)
    # ----------------------------
    ProductionBOM,

    # ✅ NEW TABLES (correct targets)
    ProductionBOMInputLine,
    ProductionBOMEffluentLine,

    # ✅ LEGACY TABLE (keep for compatibility)
    ProductionBOMLine,

    ProductionBudget, ProductionBudgetLine,
    ERPBOMRow, ProductionNorm, ProductionBudgetFG,
    SalesBudget, SalesBudgetLine, SaleType,
    RMCBudget, RMCBudgetLine, RMCPurchaseType,
    CaptiveConsumptionBudget, CaptiveConsumptionLine,PackingMaterialMaster,
)

# ----------------------------
# Model fields helpers (ONE place only)
# ----------------------------
BUDGET_LINE_FIELDS = {
    f.name for f in BudgetLine._meta.get_fields()
    if getattr(f, "concrete", False)
}

RMC_LINE_MODEL_FIELDS = {
    f.name for f in RMCBudgetLine._meta.get_fields()
    if getattr(f, "concrete", False)
}

BUDGET_LINE_MODEL_FIELDS = {
    f.name for f in BudgetLine._meta.get_fields()
    if getattr(f, "concrete", False)
}

# ----------------------------
# BudgetLineAttachment upload field detection (ONE place only)
# Supports: file / attachment / document
# ----------------------------
BUDGET_LINE_ATTACHMENT_MODEL_FIELDS = {
    f.name
    for f in BudgetLineAttachment._meta.get_fields()
    if getattr(f, "concrete", False)
}

if "file" in BUDGET_LINE_ATTACHMENT_MODEL_FIELDS:
    BUDGET_LINE_ATTACHMENT_UPLOAD_FIELD = "file"
elif "attachment" in BUDGET_LINE_ATTACHMENT_MODEL_FIELDS:
    BUDGET_LINE_ATTACHMENT_UPLOAD_FIELD = "attachment"
elif "document" in BUDGET_LINE_ATTACHMENT_MODEL_FIELDS:
    BUDGET_LINE_ATTACHMENT_UPLOAD_FIELD = "document"
else:
    BUDGET_LINE_ATTACHMENT_UPLOAD_FIELD = None


def _norm_code(v: str) -> str:
    return (v or "").strip().upper()


# ----------------------------
# Approval / Maker-Checker helpers (FORMS)
# ----------------------------
class ApprovalLockMixin:
    """
    Generic mixin to make a form read-only when entry is submitted/approved.
    Usage:
        form = SomeForm(..., locked=True)
    """
    def __init__(self, *args, **kwargs):
        self.locked = bool(kwargs.pop("locked", False))
        super().__init__(*args, **kwargs)
        if self.locked:
            self._apply_lock()

    def _apply_lock(self):
        for _, field in self.fields.items():
            field.disabled = True


class ApprovalLockFormSetMixin:
    """
    Mixin for FormSet / InlineFormSet to disable all forms and DELETE when locked=True.
    """
    locked = False

    def __init__(self, *args, **kwargs):
        self.locked = bool(kwargs.pop("locked", getattr(self, "locked", False)))
        super().__init__(*args, **kwargs)

        if self.locked:
            for f in self.forms:
                for _, field in f.fields.items():
                    field.disabled = True
                if "DELETE" in f.fields:
                    f.fields["DELETE"].disabled = True


# ----------------------------
# Approved forms
# ----------------------------
class MCRejectForm(forms.Form):
    remarks = forms.CharField(
        required=True,
        widget=forms.Textarea(attrs={
            "class": "w-full rounded-lg border border-slate-300 px-3 py-2",
            "rows": 4,
            "placeholder": "Reason for disapproval...",
        })
    )


# ----------------------------
# BUDGET PLAN + LINES
# ----------------------------
ALLOWED_BOM_TYPES = ("Key Raw Material", "Raw Material")

class BudgetPlanForm(ApprovalLockMixin, forms.ModelForm):
    class Meta:
        model = BudgetPlan
        fields = ("fy", "company_group", "is_active")
        widgets = {
            "fy": forms.TextInput(attrs={"class": "form-control", "placeholder": "2025-26"}),
            "company_group": forms.TextInput(attrs={"class": "form-control", "placeholder": "OCSPL (optional)"}),
            "is_active": forms.CheckboxInput(attrs={"class": "form-check-input"}),
        }


MONTH_FIELDS = ("apr","may","jun","jul","aug","sep","oct","nov","dec","jan","feb","mar")


def _is_effectively_blank_cleaned(cd: dict) -> bool:
    """
    Treat as blank if no ledger selected and all other inputs are empty/zero.
    This prevents saving the single blank row.
    """
    if not cd:
        return True

    gl = cd.get("gl_account")
    if gl:
        return False

    checks = [
        cd.get("particulars"),
        cd.get("remarks"),
        cd.get("prev_budget"),
        cd.get("prev_actual"),
        cd.get("apr"), cd.get("may"), cd.get("jun"), cd.get("jul"),
        cd.get("aug"), cd.get("sep"), cd.get("oct"), cd.get("nov"),
        cd.get("dec"), cd.get("jan"), cd.get("feb"), cd.get("mar"),
    ]
    for v in checks:
        if v not in (None, "", 0, 0.0, Decimal("0"), Decimal("0.0"), Decimal("0.00")):
            return False

    return True


class BudgetLineForm(ApprovalLockMixin, forms.ModelForm):
    """
    Note:
    - BudgetLine model may or may not have an 'attachment' FK/file field.
    - If you store attachments in BudgetLineAttachment instead (recommended),
      this form remains valid.
    """

    class Meta:
        model = BudgetLine
        fields = (
            "sr_no",
            "gl_account",
            "particulars",
            "prev_budget", "prev_actual",
            "apr","may","jun","jul","aug","sep","oct","nov","dec","jan","feb","mar",
            "remarks",
            * (["attachment"] if "attachment" in BUDGET_LINE_FIELDS else []),
        )
        widgets = {
            "sr_no": forms.NumberInput(attrs={"class": "w-20 rounded-md border-slate-300"}),

            # Driven by TomSelect UI
            "gl_account": forms.HiddenInput(),
            "particulars": forms.HiddenInput(),

            "prev_budget": forms.NumberInput(attrs={"step": "0.01", "class": "w-32 rounded-md border-slate-300 text-right"}),
            "prev_actual": forms.NumberInput(attrs={"step": "0.01", "class": "w-32 rounded-md border-slate-300 text-right"}),
            "remarks": forms.TextInput(attrs={"class": "w-full rounded-md border-slate-300"}),

            **(
                {"attachment": forms.ClearableFileInput(attrs={"class": "w-full rounded-md border-slate-300"})}
                if "attachment" in BUDGET_LINE_FIELDS else {}
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # allow blank row
        for f in ("sr_no", "gl_account", "particulars", "remarks", "prev_budget", "prev_actual"):
            if f in self.fields:
                self.fields[f].required = False

        # if BudgetLine actually has an attachment field, allow blank too
        if "attachment" in self.fields:
            self.fields["attachment"].required = False

        # month widgets: allow blank in UI but we will coerce to 0.00 in clean()
        for f in MONTH_FIELDS:
            if f in self.fields:
                self.fields[f].required = False
                self.fields[f].widget = forms.NumberInput(attrs={
                    "step": "0.01",
                    "class": "w-28 rounded-md border-slate-300 text-right",
                    "inputmode": "decimal",
                })

    def clean(self):
        cd = super().clean()

        # ✅ prevent NULL inserts to SQL Server (apr..mar are NOT NULL in DB)
        for f in MONTH_FIELDS:
            if f in cd:
                v = cd.get(f)
                if v in (None, ""):
                    cd[f] = Decimal("0.00")

        # prev fields can remain nullable
        for f in ("prev_budget", "prev_actual"):
            if f in cd and cd.get(f) in (None, ""):
                cd[f] = None

        return cd


class _BaseBudgetLineFormSet(ApprovalLockFormSetMixin, BaseInlineFormSet):
    """
    Keep inline formset flow. Enforce:
    - only ONE blank row rendered (extra=1)
    - do NOT save blank rows
    - force category on save_new (existing behavior)
    - ensure no NULL months slip through
    """
    category_value = None

    # ✅ SAFETY: avoid returning None objects from formset.save()
    def save_new_objects(self, commit=True):
        objs = super().save_new_objects(commit=commit)
        self.new_objects = [o for o in objs if o is not None]
        return self.new_objects

    def save_existing_objects(self, commit=True):
        objs = super().save_existing_objects(commit=commit)
        return [o for o in objs if o is not None]

    def save_new(self, form, commit=True):
        cd = getattr(form, "cleaned_data", None) or {}

        # ✅ Skip saving blank-ish row
        if _is_effectively_blank_cleaned(cd):
            return None

        obj = form.save(commit=False)

        # keep your existing category enforcement
        if self.category_value:
            obj.category = self.category_value

        # hard safety: no NULL months
        for f in MONTH_FIELDS:
            if getattr(obj, f, None) is None:
                setattr(obj, f, Decimal("0.00"))

        if commit:
            obj.save()
            if hasattr(form, "save_m2m"):
                form.save_m2m()

        return obj

    def save_existing(self, form, instance, commit=True):
        obj = form.save(commit=False)

        if self.category_value:
            obj.category = self.category_value

        for f in MONTH_FIELDS:
            if getattr(obj, f, None) is None:
                setattr(obj, f, Decimal("0.00"))

        if commit:
            obj.save()
            if hasattr(form, "save_m2m"):
                form.save_m2m()

        return obj


BudgetLineFormSet = inlineformset_factory(
    BudgetPlan,
    BudgetLine,
    form=BudgetLineForm,
    formset=_BaseBudgetLineFormSet,
    extra=1,          # ✅ prevents many blank rows on GET
    can_delete=True,
)


class BudgetLineAttachmentForm(forms.ModelForm):
    """
    Upload form for BudgetLineAttachment.
    Supports upload field name: file / attachment / document (auto-detect).
    """

    class Meta:
        model = BudgetLineAttachment
        fields = ()  # ✅ keep empty; we will add the real field dynamically

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Detect actual upload field on the MODEL (safe, no FieldError)
        model_fields = {
            f.name for f in BudgetLineAttachment._meta.get_fields()
            if getattr(f, "concrete", False)
        }

        upload_field = None
        for name in ("file", "attachment", "document"):
            if name in model_fields:
                upload_field = name
                break

        if not upload_field:
            raise ImproperlyConfigured(
                "BudgetLineAttachment model must have a concrete FileField named "
                "`file` or `attachment` or `document`."
            )

        # Ensure the form contains ONLY that upload field
        self.fields.clear()
        self.fields[upload_field] = forms.FileField(
            required=False,
            widget=forms.ClearableFileInput(attrs={"class": "form-control"})
        )

        # helpful for views/templates
        self.upload_field_name = upload_field


# ----------------------------
# PRODUCTION BOM
# ----------------------------
# ----------------------------
# Lock mixins (keep your existing ones)
# ----------------------------
class ApprovalLockMixin:
    def __init__(self, *args, **kwargs):
        self.locked = bool(kwargs.pop("locked", False))
        super().__init__(*args, **kwargs)
        if self.locked:
            for _, field in self.fields.items():
                field.disabled = True

class ApprovalLockFormSetMixin:
    locked = False
    def __init__(self, *args, **kwargs):
        self.locked = bool(kwargs.pop("locked", getattr(self, "locked", False)))
        super().__init__(*args, **kwargs)
        if self.locked:
            for f in self.forms:
                for _, field in f.fields.items():
                    field.disabled = True
                if "DELETE" in f.fields:
                    f.fields["DELETE"].disabled = True


# ----------------------------
# Production BOM master
# ----------------------------
class ProductionBOMForm(ApprovalLockMixin, forms.ModelForm):
    class Meta:
        model = ProductionBOM
        fields = ["fg_name", "fg_alpha_name", "is_active"]
        widgets = {
            "fg_name": forms.TextInput(attrs={"class": "form-control"}),
            "fg_alpha_name": forms.TextInput(attrs={"class": "form-control"}),
        }


# ----------------------------
# Helpers
# ----------------------------
def _zeroish(v) -> bool:
    s = ("" if v is None else str(v)).strip()
    return s in ("", "0", "0.0", "0.00", "0.000", "0.000000")

def _txt(v) -> str:
    return (v or "").strip()

def _concrete_field_names(model_cls) -> Set[str]:
    return {f.name for f in model_cls._meta.get_fields() if getattr(f, "concrete", False)}

def _pick_first(existing: Set[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in existing:
            return c
    return None

def _widgets_for(model_cls, field_names):
    w = {}
    for name in field_names:
        if name == "sr_no":
            w[name] = forms.NumberInput(attrs={"class": "form-control", "style": "width:80px"})
            continue

        try:
            f = model_cls._meta.get_field(name)
        except Exception:
            continue

        # ✅ BooleanField => checkbox
        if isinstance(f, dj_models.BooleanField):
            w[name] = forms.CheckboxInput(attrs={"class": "form-check-input"})
            continue

        is_numeric = isinstance(
            f,
            (
                dj_models.IntegerField,
                dj_models.BigIntegerField,
                dj_models.DecimalField,
                dj_models.FloatField,
                dj_models.PositiveIntegerField,
                dj_models.PositiveSmallIntegerField,
                dj_models.SmallIntegerField,
            ),
        ) or any(k in name.lower() for k in ["qty", "quantity", "norm", "rate", "value", "amt", "amount"])

        if is_numeric:
            w[name] = forms.NumberInput(attrs={"class": "form-control", "step": "0.000001", "style": "width:120px"})
        else:
            w[name] = forms.TextInput(attrs={"class": "form-control"})
    return w

# ============================================================
# ✅ INPUT LINES (SAVES INTO ProductionBOMInputLine)
# ============================================================
input_fields = _concrete_field_names(ProductionBOMInputLine)

sr_f   = _pick_first(input_fields, ["sr_no", "seq", "seq_id"])
cat_f  = _pick_first(input_fields, ["material_category", "category", "type"])
code_f = _pick_first(input_fields, ["bom_item_code", "item_code", "code"])
unit_f = _pick_first(input_fields, ["unit", "uom"])
name_f = _pick_first(input_fields, ["material_name", "name"])
bn_f   = _pick_first(input_fields, ["budget_norm", "norm"])
cap_f = _pick_first(input_fields, [
    "is_captive",
    "captive_tick",
    "captive",
    "is_captive_tick",
    "captive_flag",
])
tn_f   = _pick_first(input_fields, ["target_norm", "mat_qty_mt"])

# ✅ include cap_f (prefer placing it before target_norm)
INPUT_CHOSEN_FIELDS = [x for x in [sr_f, cat_f, code_f, unit_f, name_f, bn_f, cap_f, tn_f] if x]

def _input_line_blank(cd: dict) -> bool:
    if not cd:
        return True
    code = _txt(cd.get(code_f) if code_f else "")
    name = _txt(cd.get(name_f) if name_f else "")
    bn = cd.get(bn_f) if bn_f else None
    tn = cd.get(tn_f) if tn_f else None
    return (not code and not name and _zeroish(bn) and _zeroish(tn))

class ProductionBOMInputLineForm(ApprovalLockMixin, forms.ModelForm):
    class Meta:
        model = ProductionBOMInputLine
        fields = INPUT_CHOSEN_FIELDS
        widgets = _widgets_for(ProductionBOMInputLine, INPUT_CHOSEN_FIELDS)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ✅ allow empty_permitted rows (no "required" errors)
        for f in self.fields.values():
            f.required = False

        # ✅ IMPORTANT: stop browser from posting 0.000000 for blank rows
        # by not pre-filling defaults on extra rows
        if bn_f and bn_f in self.fields:
            self.fields[bn_f].initial = None
        if tn_f and tn_f in self.fields:
            self.fields[tn_f].initial = None

class _BaseProductionBOMInputLineFormSet(ApprovalLockFormSetMixin, BaseInlineFormSet):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        existing = [getattr(o, "sr_no", None) for o in self.queryset]
        mx = max([x for x in existing if x not in (None, "")] or [0])
        self._sr_next = int(mx) + 1

    def save_new(self, form, commit=True):
        cd = getattr(form, "cleaned_data", None) or {}
        if _input_line_blank(cd):
            return None

        obj = form.save(commit=False)

        if hasattr(obj, "sr_no") and not getattr(obj, "sr_no", None):
            obj.sr_no = self._sr_next
            self._sr_next += 1

        if commit:
            obj.save()
            if hasattr(form, "save_m2m"):
                form.save_m2m()
        return obj

    def save_existing(self, form, instance, commit=True):
        cd = getattr(form, "cleaned_data", None) or {}
        if cd.get("DELETE"):
            return super().save_existing(form, instance, commit=commit)

        if _input_line_blank(cd):
            instance.delete()
            return None

        obj = form.save(commit=False)
        if commit:
            obj.save()
            if hasattr(form, "save_m2m"):
                form.save_m2m()
        return obj

ProductionBOMInputLineFormSet = inlineformset_factory(
    ProductionBOM,
    ProductionBOMInputLine,     # ✅ correct model/table
    form=ProductionBOMInputLineForm,
    formset=_BaseProductionBOMInputLineFormSet,
    extra=0,                    # ✅ NO default blank row
    can_delete=True,
)


# ============================================================
# ✅ EFFLUENT LINES (SAVES INTO ProductionBOMEffluentLine)
# ============================================================
eff_fields = _concrete_field_names(ProductionBOMEffluentLine)

eff_sr = _pick_first(eff_fields, ["sr_no"])
eff_wt = _pick_first(eff_fields, ["waste_type", "type"])
eff_wn = _pick_first(eff_fields, ["waste_name", "name"])
eff_bn = _pick_first(eff_fields, ["waste_budget_norm", "budget_norm", "norm"])
eff_tn = _pick_first(eff_fields, ["waste_target_norm", "target_norm", "qty"])

EFF_CHOSEN_FIELDS = [x for x in [eff_sr, eff_wt, eff_wn, eff_bn, eff_tn] if x]

def _eff_line_blank(cd: dict) -> bool:
    if not cd:
        return True
    wt = _txt(cd.get(eff_wt) if eff_wt else "")
    wn = _txt(cd.get(eff_wn) if eff_wn else "")
    bn = cd.get(eff_bn) if eff_bn else None
    tn = cd.get(eff_tn) if eff_tn else None
    return (not wt and not wn and _zeroish(bn) and _zeroish(tn))

class ProductionBOMEffluentLineForm(ApprovalLockMixin, forms.ModelForm):
    class Meta:
        model = ProductionBOMEffluentLine
        fields = EFF_CHOSEN_FIELDS
        widgets = _widgets_for(ProductionBOMEffluentLine, EFF_CHOSEN_FIELDS)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for f in self.fields.values():
            f.required = False
        if eff_bn and eff_bn in self.fields:
            self.fields[eff_bn].initial = None
        if eff_tn and eff_tn in self.fields:
            self.fields[eff_tn].initial = None

class _BaseProductionBOMEffluentFormSet(ApprovalLockFormSetMixin, BaseInlineFormSet):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        existing = [getattr(o, "sr_no", None) for o in self.queryset]
        mx = max([x for x in existing if x not in (None, "")] or [0])
        self._sr_next = int(mx) + 1

    def save_new(self, form, commit=True):
        cd = getattr(form, "cleaned_data", None) or {}
        if _eff_line_blank(cd):
            return None
        obj = form.save(commit=False)
        if hasattr(obj, "sr_no") and not getattr(obj, "sr_no", None):
            obj.sr_no = self._sr_next
            self._sr_next += 1
        if commit:
            obj.save()
        return obj

    def save_existing(self, form, instance, commit=True):
        cd = getattr(form, "cleaned_data", None) or {}
        if cd.get("DELETE"):
            return super().save_existing(form, instance, commit=commit)
        if _eff_line_blank(cd):
            instance.delete()
            return None
        obj = form.save(commit=False)
        if commit:
            obj.save()
        return obj

ProductionBOMEffluentLineFormSet = inlineformset_factory(
    ProductionBOM,
    ProductionBOMEffluentLine,
    form=ProductionBOMEffluentLineForm,
    formset=_BaseProductionBOMEffluentFormSet,
    extra=0,             # ✅ NO default blank row
    can_delete=True,
)
# ----------------------------
# PRODUCTION BUDGET
# ----------------------------
class ProductionBudgetForm(ApprovalLockMixin, forms.ModelForm):
    class Meta:
        model = ProductionBudget
        fields = [
            "apr","may","jun","jul","aug","sep","oct","nov","dec","jan","feb","mar",
            "remarks",
        ]
        widgets = {
            "apr": forms.NumberInput(attrs={"class": "form-control text-end", "step": "0.001"}),
            "may": forms.NumberInput(attrs={"class": "form-control text-end", "step": "0.001"}),
            "jun": forms.NumberInput(attrs={"class": "form-control text-end", "step": "0.001"}),
            "jul": forms.NumberInput(attrs={"class": "form-control text-end", "step": "0.001"}),
            "aug": forms.NumberInput(attrs={"class": "form-control text-end", "step": "0.001"}),
            "sep": forms.NumberInput(attrs={"class": "form-control text-end", "step": "0.001"}),
            "oct": forms.NumberInput(attrs={"class": "form-control text-end", "step": "0.001"}),
            "nov": forms.NumberInput(attrs={"class": "form-control text-end", "step": "0.001"}),
            "dec": forms.NumberInput(attrs={"class": "form-control text-end", "step": "0.001"}),
            "jan": forms.NumberInput(attrs={"class": "form-control text-end", "step": "0.001"}),
            "feb": forms.NumberInput(attrs={"class": "form-control text-end", "step": "0.001"}),
            "mar": forms.NumberInput(attrs={"class": "form-control text-end", "step": "0.001"}),
            "remarks": forms.TextInput(attrs={"class": "form-control"}),
        }


class _BaseProductionBudgetLineFormSet(ApprovalLockFormSetMixin, BaseInlineFormSet):
    pass


ProductionBudgetLineFormSet = inlineformset_factory(
    ProductionBudget,
    ProductionBudgetLine,
    fields=["sr_no", "material_name", "norm", "remarks"],
    formset=_BaseProductionBudgetLineFormSet,
    extra=0,
    can_delete=True,
    widgets={
        "sr_no": forms.NumberInput(attrs={"class": "form-control", "style": "width:80px"}),
        "material_name": forms.TextInput(attrs={"class": "form-control"}),
        "norm": forms.NumberInput(attrs={"class": "form-control", "step": "0.000001", "style": "width:120px"}),
        "remarks": forms.TextInput(attrs={"class": "form-control"}),
    }
)


class ProductionBudgetCreateForm(ApprovalLockMixin, forms.Form):
    fg_name = forms.ChoiceField(choices=[], required=True)

    def __init__(self, *args, **kwargs):
        # kept for backward compatibility with existing calls
        allowed_types = kwargs.pop("allowed_types", None)
        super().__init__(*args, **kwargs)

        # ✅ now cache key is BOM-based (types no longer affect choices)
        cache_key = "budget::prod_fg_choices::bom_master::v1"
        choices = cache.get(cache_key)

        if choices is None:
            qs = (
                ProductionBOM.objects
                .filter(is_active=True)
                .exclude(fg_name="")
                .exclude(fg_name__isnull=True)
                .order_by("fg_name")
                .only("fg_name", "fg_alpha_name")
            )

            # ✅ show alpha name if available
            choices = [("", "— Select FG —")]
            for b in qs:
                label = b.fg_name
                if getattr(b, "fg_alpha_name", ""):
                    label = f"{b.fg_name} ({b.fg_alpha_name})"
                choices.append((b.fg_name, label))

            cache.set(cache_key, choices, 10 * 60)

        self.fields["fg_name"].choices = choices
        self.fields["fg_name"].widget.attrs.update({
            "class": "w-full rounded-lg border border-slate-300 px-3 py-2"
        })

class ProductionFGSelectForm(ApprovalLockMixin, forms.Form):
    fg_name = forms.ChoiceField(
        choices=[],
        widget=forms.Select(attrs={"class": "form-select"}),
        label="FG Name"
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        fgs = (
            ERPBOMRow.objects.exclude(fg_name__isnull=True)
            .exclude(fg_name__exact="")
            .order_by("fg_name")
            .values_list("fg_name", flat=True)
            .distinct()
        )
        self.fields["fg_name"].choices = [("", "---- Select FG ----")] + [(x, x) for x in fgs]


# ----------------------------
# Production Budget FG (HEADER MONTHS)
# ----------------------------
DEC6 = Decimal("0.000000")
MONTHS = ["apr","may","jun","jul","aug","sep","oct","nov","dec","jan","feb","mar"]

def _to_decimal_safe(v, default=DEC6) -> Decimal:
    """
    Robust parser for month inputs.
    Accepts: 33, 33.0, "33", "1,234.500", "".
    Returns Decimal (not None).
    """
    if v in (None, ""):
        return default
    if isinstance(v, Decimal):
        return v
    s = str(v).strip()
    if not s:
        return default
    s = s.replace(",", "")  # allow 1,234.50
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        raise forms.ValidationError(f"Invalid number: {v}")

class ProductionBudgetFGForm(ApprovalLockMixin, forms.ModelForm):
    """
    IMPORTANT:
    - Keep only ONE ProductionBudgetFGForm in this file.
    - Ensures months never become NULL and are properly parsed.
    - ✅ Forces cleaned month values onto instance before save (fixes "saved but still 0.000" issue).
    """

    class Meta:
        model = ProductionBudgetFG
        fields = list(MONTHS) + ["remarks"]
        widgets = {
            **{
                m: forms.NumberInput(
                    attrs={
                        "class": "form-control text-end",
                        "step": "0.001",
                        "inputmode": "decimal",
                    }
                )
                for m in MONTHS
            },
            "remarks": forms.TextInput(attrs={"class": "form-control"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # allow blanks in UI; we coerce them to 0.000 in clean()
        for m in MONTHS:
            if m in self.fields:
                self.fields[m].required = False

    def clean(self):
        cd = super().clean()

        # Normalize months
        for m in MONTHS:
            if m not in self.fields:
                continue

            cd[m] = _to_decimal_safe(cd.get(m), DEC6)

            # DB is DecimalField(decimal_places=3) in ProductionBudgetFG
            cd[m] = cd[m].quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)

        return cd

    def save(self, commit=True):
        """
        ✅ Hard guarantee: months from cleaned_data are applied to instance.
        This fixes cases where months appear in POST but instance still saves 0.000.
        """
        inst = super().save(commit=False)

        # Apply cleaned month values onto instance explicitly
        for m in MONTHS:
            if m in self.cleaned_data:
                setattr(inst, m, self.cleaned_data[m] or Decimal("0.000"))

        # remarks safety
        if "remarks" in self.cleaned_data:
            inst.remarks = (self.cleaned_data.get("remarks") or "").strip()

        if commit:
            inst.save()
            self.save_m2m()

        return inst
    
    
class NormRowForm(forms.Form):
    """
    Dynamic form row is easier in template: we will POST norm values like:
    norm__<bom_item_code> = value
    """
    pass

# ----------------------------
# SALES BUDGET
# ----------------------------

# ----------------------------
# helpers
# ----------------------------
def _model_field_names(model_cls) -> set[str]:
    return {
        f.name
        for f in model_cls._meta.get_fields()
        if getattr(f, "concrete", False)
    }

def _has_field(model_cls, name: str) -> bool:
    return name in _model_field_names(model_cls)


# ----------------------------
# SALES FG choices from ProductionNorm
# ----------------------------
def sales_fg_choices_from_production_norm(*, allowed_types=None, extra_names=None):
    """
    Backward compatible name.
    Source = ProductionBOM (same as ProductionBudgetCreateForm FG list)
    - Only is_active=True BOMs
    - Label shows fg_alpha_name if present: "FG (ALPHA)"
    - extra_names ensures old saved products still appear
    """
    # allowed_types kept only for backward compatibility (ignored now)
    key = "budget::sales_fg_choices::bom_master::v2"  # ✅ bump v2 to break old cache
    base_choices = cache.get(key)

    if base_choices is None:
        qs = (
            ProductionBOM.objects
            .filter(is_active=True)
            .exclude(fg_name__isnull=True)
            .exclude(fg_name__exact="")
            .order_by("fg_name")
            .only("fg_name", "fg_alpha_name")
        )

        base_choices = [("", "— Select FG —")]
        for b in qs:
            fg = (b.fg_name or "").strip()
            if not fg:
                continue
            alpha = (getattr(b, "fg_alpha_name", "") or "").strip()
            label = f"{fg} ({alpha})" if alpha else fg
            base_choices.append((fg, label))

        cache.set(key, base_choices, 10 * 60)

    # ✅ Add extras (NOT cached) so existing saved rows don't break
    if extra_names:
        existing_vals = set(v for (v, _) in base_choices if v)
        extras = []
        for nm in extra_names:
            nm = (nm or "").strip()
            if nm and nm not in existing_vals:
                extras.append((nm, nm))
                existing_vals.add(nm)

        if extras:
            return [base_choices[0]] + extras + base_choices[1:]

    return base_choices

# =============================================================================
# SALES BUDGET FORMS
# =============================================================================

class SalesBudgetForm(ApprovalLockMixin, forms.ModelForm):
    class Meta:
        model = SalesBudget
        fields = ["inr_usd"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["inr_usd"].widget.attrs.update({
            "class": "w-40 rounded-lg border border-slate-300 px-3 py-2"
        })


class SalesBudgetLineForm(ApprovalLockMixin, forms.ModelForm):
    product_name = forms.ChoiceField(choices=[], required=True)

    class Meta:
        model = SalesBudgetLine
        fields = [
            "product_name", "sale_type",
            "annual_qty_mt", "rate_usd", "rate_inr",
            "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec", "jan", "feb", "mar",
        ]

    MONTHS = ["apr","may","jun","jul","aug","sep","oct","nov","dec","jan","feb","mar"]

    def __init__(self, *args, **kwargs):
        self._budget = kwargs.pop("budget", None)
        allowed_types = kwargs.pop("allowed_types", None)
        extra_names = kwargs.pop("extra_names", None)

        # ✅ BACKWARD COMPAT: view may pass fg_choices=form_kwargs
        fg_choices = kwargs.pop("fg_choices", None)

        super().__init__(*args, **kwargs)

        # ✅ Build product choices
        if fg_choices:
            # fg_choices may already include ("", "— Select FG —")
            choices = list(fg_choices)
            if not choices or choices[0][0] != "":
                choices = [("", "— Select FG —")] + choices
        else:
            choices = list(sales_fg_choices_from_production_norm(
                allowed_types=allowed_types,
                extra_names=extra_names
            ))

        # ✅ Ensure current value always appears (edit safety)
        current = (self.initial.get("product_name") or getattr(self.instance, "product_name", "") or "").strip()
        if current and current not in dict(choices):
            choices.insert(1, (current, current))

        self.fields["product_name"].choices = choices

        # UI attrs
        base = "w-full rounded-lg border border-slate-300 px-3 py-2"
        self.fields["product_name"].widget.attrs.update({"class": base})
        self.fields["sale_type"].widget.attrs.update({"class": "w-32 rounded-lg border border-slate-300 px-3 py-2"})

        self.fields["annual_qty_mt"].widget.attrs.update({
            "readonly": "readonly",
            "step": "0.001",
            "data-dp": "3",
        })

        self.fields["rate_usd"].widget.attrs.update({"step": "0.000001", "data-dp": "6"})
        self.fields["rate_inr"].widget.attrs.update({"step": "0.000001", "data-dp": "6"})

        for m in self.MONTHS:
            self.fields[m].widget.attrs.update({"step": "0.001", "data-dp": "3"})

    def _d(self, v, default=Decimal("0")) -> Decimal:
        if v in (None, ""):
            return default
        if isinstance(v, Decimal):
            return v
        try:
            return Decimal(str(v))
        except (InvalidOperation, ValueError):
            return default

    def _q(self, v: Decimal, places: str) -> Decimal:
        return v.quantize(Decimal(places), rounding=ROUND_HALF_UP)

    def clean(self):
        cd = super().clean()

        pn = (cd.get("product_name") or "").strip()
        cd["product_name"] = pn

        total = Decimal("0")
        for m in self.MONTHS:
            cd[m] = self._q(self._d(cd.get(m), Decimal("0")), "0.001")
            total += cd[m]
        cd["annual_qty_mt"] = self._q(total, "0.001")

        cd["rate_usd"] = self._q(self._d(cd.get("rate_usd"), Decimal("0")), "0.000001")
        cd["rate_inr"] = self._q(self._d(cd.get("rate_inr"), Decimal("0")), "0.000001")

        sale_type = cd.get("sale_type") or SaleType.DOMESTIC

        ex = None
        if self._budget and self._budget.inr_usd:
            ex = self._d(self._budget.inr_usd, None)
            if ex is not None and ex <= 0:
                ex = None

        if ex:
            if sale_type == SaleType.EXPORT:
                if cd["rate_usd"] > 0 and (cd["rate_inr"] == 0):
                    cd["rate_inr"] = self._q(cd["rate_usd"] * ex, "0.000001")
            else:
                if cd["rate_inr"] > 0 and (cd["rate_usd"] == 0):
                    cd["rate_usd"] = self._q(cd["rate_inr"] / ex, "0.000001")

        return cd


class _BaseSalesBudgetLineFormSet(ApprovalLockFormSetMixin, forms.BaseModelFormSet):
    pass


SalesBudgetLineFormSet = modelformset_factory(
    SalesBudgetLine,
    form=SalesBudgetLineForm,
    formset=_BaseSalesBudgetLineFormSet,
    extra=0,
    can_delete=True
)

# ----------------------------
# RMC BUDGET (UPDATED: supports captive split + required qty recalculation)
# ----------------------------
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

MONTHS = ("apr","may","jun","jul","aug","sep","oct","nov","dec","jan","feb","mar")

ALLOWED_BOM_TYPES = (
    "Key Raw Material",
    "Raw Material",
    "Work in Progress",
    "Semi Finished Good",
    "WIP FR",
)

def _norm_code(v: str) -> str:
    return (v or "").strip().upper()

def _norm_pt(v: str) -> str:
    """
    Normalize purchase type values coming from UI labels too.
    """
    s = (v or "").strip().upper()
    if s in ("IMP", "IMPORT", "IMPORT PURCHASE", "IMPORTED"):
        return "IMPORT"
    if s in ("LOCAL", "LOCAL PURCHASE", "DOMESTIC", "DOMESTIC PURCHASE"):
        return "LOCAL"
    if "IMPORT" in s:
        return "IMPORT"
    return "LOCAL"

def _rm_choices_rmc():
    """
    Returns:
      choices: [("", "— Select Raw Material —"), (code, "Name (CODE)"), ...]
      code_to_name: {code: name}
    """
    key = "budget::rm_choices_rmc::v4"
    cached = cache.get(key)
    if cached is not None:
        return cached

    qs = (
        ERPBOMRow.objects
        .filter(type__in=ALLOWED_BOM_TYPES)
        .exclude(bom_item_code="").exclude(bom_item_code__isnull=True)
        .values("bom_item_code", "bom_item_name")
        .distinct()
    )

    code_to_name = {}
    for r in qs:
        code = (r.get("bom_item_code") or "").strip().upper()
        name = (r.get("bom_item_name") or "").strip()
        if not code:
            continue
        if code not in code_to_name or (not code_to_name[code] and name):
            code_to_name[code] = name

    choices = [("", "— Select Raw Material —")]

    for code, name in sorted(code_to_name.items(), key=lambda x: ((x[1] or "").lower(), x[0])):
        label = f"{name} ({code})" if name else code
        choices.append((code, label))

    payload = (choices, code_to_name)
    cache.set(key, payload, 10 * 60)
    return payload


def _dec(v, default=Decimal("0")) -> Decimal:
    if v in (None, "", "None"):
        return default
    if isinstance(v, Decimal):
        return v
    try:
        s = str(v).replace(",", "").strip()
        if not s or s.upper() == "NONE":
            return default
        return Decimal(s)
    except (InvalidOperation, ValueError, TypeError):
        return default


def _q(v, places="0.0000") -> Decimal:
    qv = Decimal(places)
    x = _dec(v, Decimal("0"))
    try:
        return x.quantize(qv, rounding=ROUND_HALF_UP)
    except Exception:
        return Decimal("0").quantize(qv, rounding=ROUND_HALF_UP)

def _q_qty(v) -> Decimal:
    # quantities (MT/KG) – keep 6 dp as per your sync logic
    return _q(v, "0.000000")


def _calc_required_from_months(obj) -> Decimal | None:
    """
    Required Qty must be based on synced monthly quantities:
        required_qty = SUM(apr..mar)

    Returns None if the model has no month fields.
    """
    has_any = any(hasattr(obj, m) for m in MONTHS)
    if not has_any:
        return None

    total = Decimal("0")
    for m in MONTHS:
        if hasattr(obj, m):
            total += _q_qty(getattr(obj, m, None))
    return _q_qty(total)


def _set_required_fields(obj, required: Decimal):
    """
    Write into whichever required qty field exists.
    Keep compatible with multiple schema variants.
    """
    for f in ("required_qty", "required_qty_mt", "required_qty_kg", "req_qty_mt", "req_qty", "required_qty_value"):
        if hasattr(obj, f):
            setattr(obj, f, required)

    # Keep annual-ish fields aligned too (optional, but prevents confusion)
    for f in ("annual_qty_mt", "annual_qty", "annual_qty_kg", "qty", "rm_qty", "quantity"):
        if hasattr(obj, f):
            setattr(obj, f, required)


class RMCBudgetForm(ApprovalLockMixin, forms.ModelForm):
    class Meta:
        model = RMCBudget
        fields = ["usd_inr"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["usd_inr"].widget.attrs.update({
            "class": "w-40 rounded-lg border border-slate-300 px-3 py-2",
            "step": "0.0001",
            "data-dp": "4",
        })


class RMCBudgetLineForm(ApprovalLockMixin, forms.ModelForm):
    """
    ✅ Updated with internal helpers (cleaner + safer), WITHOUT changing existing flow:
    - Same validations (LOCAL needs local_rate_inr, IMPORT needs import_rate_usd)
    - Same captive resolution priority (POST -> force_captive -> instance)
    - Same quantize behavior
    - OPTIONAL hook: validate_rates (default True) so you *can* relax validations on "Save" later,
      but nothing changes unless your view passes validate_rates=False.
    """

    class Meta:
        model = RMCBudgetLine
        fields = [
            "rm_code",
            * (["is_captive"] if "is_captive" in RMC_LINE_MODEL_FIELDS else []),
            "purchase_type",
            * (["required_qty"] if "required_qty" in RMC_LINE_MODEL_FIELDS else []),
            * (["unit"] if "unit" in RMC_LINE_MODEL_FIELDS else []),
            "local_rate_inr",
            "import_rate_usd", "duty_percent", "freight_inr", "clearance_inr",
        ]

    # -------------------------------------------------------------------------
    # helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def _to_bool(v) -> bool:
        if isinstance(v, bool):
            return v
        if v is None:
            return False
        s = str(v).strip().lower()
        return s in ("1", "true", "on", "yes", "y")

    def _posted(self, name: str, default=None):
        """Prefix-safe POST getter: form-0-xxx"""
        return self.data.get(f"{self.prefix}-{name}", default)

    def _resolve_is_captive(self, cd):
        """
        ✅ Existing flow preserved:
        POST -> force_captive -> instance
        """
        if "is_captive" not in self.fields:
            return

        cap_val = cd.get("is_captive", None)

        # if template didn't render hidden field, try raw POST (prefix-safe)
        if cap_val in (None, ""):
            cap_val = self._posted("is_captive", None)

        # if still missing, use force_captive (optional)
        if cap_val in (None, "") and self._force_captive is not None:
            cap_val = self._force_captive

        # final fallback: instance value
        if cap_val in (None, ""):
            cap_val = getattr(self.instance, "is_captive", False)

        cd["is_captive"] = self._to_bool(cap_val)

    def _coerce_money_fields(self, cd):
        """
        Coerce all numeric inputs safely to Decimal using your existing helpers.
        (keeps behavior identical to your current code)
        """
        local = _dec(cd.get("local_rate_inr"), default=Decimal("0"))
        usd   = _dec(cd.get("import_rate_usd"), default=Decimal("0"))
        duty  = _dec(cd.get("duty_percent"), default=Decimal("0"))
        frt   = _dec(cd.get("freight_inr"), default=Decimal("0"))
        clr   = _dec(cd.get("clearance_inr"), default=Decimal("0"))
        return local, usd, duty, frt, clr

    def _apply_purchase_rules(self, cd, *, local, usd, duty, frt, clr):
        """
        ✅ Existing validation preserved, with OPTIONAL validate_rates switch.
        Default validate_rates=True => NO flow change.
        """
        ptype = _norm_pt(cd.get("purchase_type"))
        cd["purchase_type"] = ptype

        if ptype == "LOCAL":
            if self._validate_rates and local <= 0:
                self.add_error("local_rate_inr", "Local ₹/Kg is required for LOCAL purchase.")
            cd["local_rate_inr"] = local
            cd["import_rate_usd"] = Decimal("0")
            cd["duty_percent"] = Decimal("0")
            cd["freight_inr"] = Decimal("0")
            cd["clearance_inr"] = Decimal("0")
        else:  # IMPORT (kept as-is)
            if self._validate_rates and usd <= 0:
                self.add_error("import_rate_usd", "Import $/Kg is required for IMPORT purchase.")
            cd["local_rate_inr"] = Decimal("0")
            cd["import_rate_usd"] = usd
            cd["duty_percent"] = duty
            cd["freight_inr"] = frt
            cd["clearance_inr"] = clr

    def _quantize_all(self, cd):
        cd["local_rate_inr"]   = _q(cd.get("local_rate_inr"), "0.0000")
        cd["import_rate_usd"]  = _q(cd.get("import_rate_usd"), "0.0000")
        cd["duty_percent"]     = _q(cd.get("duty_percent"), "0.0000")
        cd["freight_inr"]      = _q(cd.get("freight_inr"), "0.0000")
        cd["clearance_inr"]    = _q(cd.get("clearance_inr"), "0.0000")

    # -------------------------------------------------------------------------
    # init
    # -------------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        self._budget = kwargs.pop("budget", None)

        # OPTIONAL (does not change existing flow):
        self._force_captive = kwargs.pop("force_captive", None)

        # OPTIONAL (does not change existing flow because default True):
        # Your view may pass validate_rates=False on "Save"
        self._validate_rates = bool(kwargs.pop("validate_rates", True))

        super().__init__(*args, **kwargs)

        (choices, code_to_name) = _rm_choices_rmc()
        self._rm_map = code_to_name
        self.fields["rm_code"].widget = forms.Select(choices=choices)

        base = "w-full rounded-lg border border-slate-300 px-3 py-2"
        num  = "w-28 rounded-lg border border-slate-300 px-3 py-2"

        self.fields["rm_code"].widget.attrs.update({"class": base})
        self.fields["purchase_type"].widget.attrs.update({"class": "w-40 " + base})

        # captive hidden (server/UI controlled)
        if "is_captive" in self.fields:
            self.fields["is_captive"].required = False
            self.fields["is_captive"].widget = forms.HiddenInput()

            # Important: initial must reflect instance/bucket so new rows don’t default to False
            if self._force_captive is not None and not self.is_bound and not getattr(self.instance, "pk", None):
                self.fields["is_captive"].initial = bool(self._force_captive)
            else:
                self.fields["is_captive"].initial = bool(getattr(self.instance, "is_captive", False))

        # required_qty: show as read-only in UI, but MUST be posted
        # (disabled fields are not submitted by browser)
        if "required_qty" in self.fields:
            self.fields["required_qty"].required = False
            self.fields["required_qty"].disabled = False
            self.fields["required_qty"].widget.attrs.update({
                "class": num,
                "readonly": "readonly",
                "step": "0.0001",
                "data-dp": "4",
            })

        # unit derived (read-only)
        if "unit" in self.fields:
            self.fields["unit"].required = False
            self.fields["unit"].disabled = True
            self.fields["unit"].widget.attrs.update({"class": "w-24 " + num})

        for f in ("import_rate_usd", "duty_percent", "freight_inr", "clearance_inr", "local_rate_inr"):
            if f in self.fields:
                self.fields[f].required = False
                self.fields[f].widget.attrs.update({"class": num, "step": "0.0001", "data-dp": "4"})

        # ensure existing rm_code is present in dropdown
        current = (self.initial.get("rm_code") or getattr(self.instance, "rm_code", "") or "").strip().upper()
        if current:
            ch_dict = dict(choices)
            if current not in ch_dict:
                nm = (
                    ERPBOMRow.objects
                    .filter(bom_item_code=current)
                    .exclude(bom_item_name__isnull=True)
                    .exclude(bom_item_name__exact="")
                    .values_list("bom_item_name", flat=True)
                    .first()
                )
                label = f"{nm} ({current})" if nm else current
                self.fields["rm_code"].choices = [choices[0], (current, label)] + choices[1:]

    # -------------------------------------------------------------------------
    # clean
    # -------------------------------------------------------------------------
    def clean(self):
        cd = super().clean()

        cd["rm_code"] = _norm_code(cd.get("rm_code"))

        # ✅ FIX (existing behavior): resolve captive reliably
        self._resolve_is_captive(cd)

        # numeric coercion (same defaults)
        local, usd, duty, frt, clr = self._coerce_money_fields(cd)

        # apply LOCAL/IMPORT rules (same validations by default)
        self._apply_purchase_rules(cd, local=local, usd=usd, duty=duty, frt=frt, clr=clr)

        # quantize (same)
        self._quantize_all(cd)

        return cd

    def save(self, commit=True):
        obj = super().save(commit=False)

        obj.rm_code = _norm_code(obj.rm_code)
        obj.purchase_type = _norm_pt(getattr(obj, "purchase_type", ""))

        # ✅ FIX: persist captive from cleaned_data (not from default obj value)
        if hasattr(obj, "is_captive") and "is_captive" in getattr(self, "cleaned_data", {}):
            obj.is_captive = bool(self.cleaned_data.get("is_captive") or False)
        elif hasattr(obj, "is_captive") and self._force_captive is not None:
            obj.is_captive = bool(self._force_captive)
        elif hasattr(obj, "is_captive"):
            obj.is_captive = bool(getattr(obj, "is_captive", False))

        # fill rm_name if blank
        code = (obj.rm_code or "").strip().upper()
        nm = (self._rm_map.get(code) or "").strip()
        if not nm and code:
            nm = (
                ERPBOMRow.objects
                .filter(bom_item_code=code)
                .exclude(bom_item_name__isnull=True)
                .exclude(bom_item_name__exact="")
                .values_list("bom_item_name", flat=True)
                .first()
            ) or ""
        if hasattr(obj, "rm_name"):
            obj.rm_name = (nm or getattr(obj, "rm_name", "") or "").strip()

        # attach budget if missing
        if self._budget and not getattr(obj, "budget_id", None):
            obj.budget = self._budget

        # keep your existing required qty logic as-is (no flow change)
        required = _calc_required_from_months(obj)
        if required is not None:
            _set_required_fields(obj, required)

        if commit:
            obj.save()
        return obj


class _BaseRMCBudgetLineFormSet(ApprovalLockFormSetMixin, forms.BaseModelFormSet):
    """
    Duplicates allowed ONLY if is_captive differs.
    Blocks duplicates of (rm_code + is_captive).
    """
    def clean(self):
        super().clean()

        seen = set()
        dups = set()

        for form in self.forms:
            if not hasattr(form, "cleaned_data"):
                continue
            cd = form.cleaned_data

            if cd.get("DELETE"):
                continue

            code = _norm_code(cd.get("rm_code"))
            if not code:
                continue

            cap = bool(cd.get("is_captive") or False) if "is_captive" in cd else False
            k = (code, cap)

            if k in seen:
                dups.add(k)
            else:
                seen.add(k)

        if dups:
            msg = [f"{code} ({'CAPTIVE' if cap else 'NORMAL'})" for code, cap in sorted(dups)]
            raise forms.ValidationError(
                "Duplicate RM Code(s) detected in same bucket: " + ", ".join(msg) +
                ". Keep only one row per RM Code per bucket (CAPTIVE/NORMAL)."
            )


RMCBudgetLineFormSet = modelformset_factory(
    RMCBudgetLine,
    form=RMCBudgetLineForm,
    formset=_BaseRMCBudgetLineFormSet,
    extra=0,
    can_delete=True
)

# -----------------------------
# COA Forms (required by existing views)
# -----------------------------
class AccountCategoryForm(forms.ModelForm):
    class Meta:
        model = AccountCategory
        fields = ["code", "name", "default_group"]


class AccountSubcategoryForm(forms.ModelForm):
    class Meta:
        model = AccountSubcategory
        fields = ["category", "code", "name", "group"]


class DepartmentBudgetHeadUploadForm(forms.Form):
    file = forms.FileField()


class GLAccountExcelForm(forms.ModelForm):
    """
    Excel-driven GL:
    - unit/department/budget_head must exist in DepartmentBudgetHead
    - name auto-fills from master.gl_name
    - code (no) auto-generated in model.save() if blank
    """
    class Meta:
        model = GLAccount
        fields = ["unit", "department", "budget_head", "name", "blocked"]

    def clean(self):
        cd = super().clean()

        unit = (cd.get("unit") or "").strip()
        dept = (cd.get("department") or "").strip()
        head = (cd.get("budget_head") or "").strip()

        if not dept:
            raise forms.ValidationError("Department is required.")
        if not head:
            raise forms.ValidationError("Budget Head is required.")

        master = DepartmentBudgetHead.objects.filter(
            unit=unit,
            department=dept,
            budget_head=head,
            is_active=True,
        ).first()

        if not master:
            raise forms.ValidationError(
                "This Unit/Department/Budget Head is not available in uploaded Excel master."
            )

        if master.gl_name:
            cd["name"] = master.gl_name

        return cd

    def save(self, commit=True):
        obj = super().save(commit=False)

        unit = (self.cleaned_data.get("unit") or "").strip()
        dept = (self.cleaned_data.get("department") or "").strip()
        head = (self.cleaned_data.get("budget_head") or "").strip()

        master = DepartmentBudgetHead.objects.filter(
            unit=unit,
            department=dept,
            budget_head=head,
            is_active=True,
        ).first()

        if master:
            obj.master = master
            if master.gl_name:
                obj.name = master.gl_name

        if commit:
            obj.save()
        return obj


class BudgetHeadUploadForm(forms.Form):
    file = forms.FileField(required=True, label="Upload Excel File")

    def clean_file(self):
        file = self.cleaned_data.get("file")
        if not file.name.endswith(".xlsx"):
            raise forms.ValidationError("Only Excel files are allowed!")
        return file


class GLExcelUploadForm(forms.Form):
    file = forms.FileField()

LEDGER_DEPARTMENT_CHOICES = [
    ("Safety & Health", "Safety & Health"),
    ("Environment", "Environment"),
    ("QA& QC", "QA& QC"),
    ("Engineering", "Engineering"),
    ("Utility", "Utility"),
    ("Admin", "Admin"),
    ("HR", "HR"),
    ("R&D", "R&D"),
    ("Logistic", "Logistic"),
    ("Finance & Accounts", "Finance & Accounts"),
    ("Steam", "Steam"),
    ("Electricity", "Electricity"),
    ("R & M", "R & M"),
    ("Employee Cost (OC)", "Employee Cost (OC)"),
    ("Employee Cost (Contract)", "Employee Cost (Contract)"),
]

class GLAccountForm(forms.ModelForm):
    # ✅ force dropdown + validation (still saves to CharField)
    department = forms.ChoiceField(
        choices=[("", "— Select Department —")] + LEDGER_DEPARTMENT_CHOICES,
        required=False,
        widget=forms.Select(attrs={"class": "form-input"}),
    )

    class Meta:
        model = GLAccount
        fields = ["unit", "department", "budget_head", "name", "is_active"]
        widgets = {
            "unit": forms.TextInput(attrs={"class": "form-input"}),
            "budget_head": forms.TextInput(attrs={"class": "form-input"}),
            "name": forms.TextInput(attrs={"class": "form-input"}),
        }

    def clean_department(self):
        val = (self.cleaned_data.get("department") or "").strip()
        if not val:
            return ""

        allowed = {k for k, _ in LEDGER_DEPARTMENT_CHOICES}
        if val not in allowed:
            raise forms.ValidationError("Invalid department selected.")
        return val


# -----------------------------
# Captive Consumption Budget
# -----------------------------
class CaptiveConsumptionBudgetForm(forms.ModelForm):
    class Meta:
        model = CaptiveConsumptionBudget
        fields = []


class CaptiveConsumptionLineForm(forms.ModelForm):
    class Meta:
        model = CaptiveConsumptionLine
        fields = ["item_name", "captive_type", "qty", "rate"]
        widgets = {
            "item_name": forms.TextInput(attrs={
                "class": "w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 "
                         "placeholder:text-slate-400 focus:outline-none focus:ring-2 focus:ring-indigo-500/30 "
                         "focus:border-indigo-500",
                "placeholder": "Type & search FG / WIP / SFG...",
                "list": "fg_items",
                "autocomplete": "off",
            }),
            "captive_type": forms.Select(attrs={
                "class": "w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 "
                         "focus:outline-none focus:ring-2 focus:ring-indigo-500/30 focus:border-indigo-500",
            }),
            "qty": forms.NumberInput(attrs={
                "class": "w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 text-right "
                         "focus:outline-none focus:ring-2 focus:ring-indigo-500/30 focus:border-indigo-500",
                "step": "0.001",
                "min": "0",
                "inputmode": "decimal",
            }),
            "rate": forms.NumberInput(attrs={
                "class": "w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 text-right "
                         "focus:outline-none focus:ring-2 focus:ring-indigo-500/30 focus:border-indigo-500",
                "step": "0.0001",
                "min": "0",
                "inputmode": "decimal",
            }),
        }

    def clean_qty(self):
        v = self.cleaned_data.get("qty")
        return v if v is not None else Decimal("0.000")

    def clean_rate(self):
        v = self.cleaned_data.get("rate")
        return v if v is not None else Decimal("0.0000")


CaptiveConsumptionLineFormSet = inlineformset_factory(
    CaptiveConsumptionBudget,
    CaptiveConsumptionLine,
    form=CaptiveConsumptionLineForm,
    extra=1,
    can_delete=True,
)





# =============================================================================
# PACKING MATERIAL (NEW)
# =============================================================================

class PackingMaterialMasterForm(forms.ModelForm):
    class Meta:
        model = PackingMaterialMaster
        fields = ["item_code", "item_name", "unit", "packing_size", "rate", "is_active"]  # ✅ added rate
        widgets = {
            "item_code": forms.TextInput(attrs={
                "class": "w-full rounded-lg border border-slate-300 px-3 py-2",
                "placeholder": "Item Code (e.g. 26001234)",
            }),
            "item_name": forms.TextInput(attrs={
                "class": "w-full rounded-lg border border-slate-300 px-3 py-2",
                "placeholder": "Packing Material Name",
            }),
            "unit": forms.TextInput(attrs={
                "class": "w-full rounded-lg border border-slate-300 px-3 py-2",
                "placeholder": "Unit (e.g. NOS / KG)",
            }),
            "packing_size": forms.NumberInput(attrs={
                "class": "w-full rounded-lg border border-slate-300 px-3 py-2 text-right",
                "step": "0.001",
                "placeholder": "Packing size / capacity (Kg per pack)",
            }),
            "rate": forms.NumberInput(attrs={  # ✅ NEW widget
                "class": "w-full rounded-lg border border-slate-300 px-3 py-2 text-right",
                "step": "0.0001",
                "placeholder": "Rate per unit (₹ / pack)",
            }),
            "is_active": forms.CheckboxInput(attrs={"class": "h-4 w-4 rounded border-slate-300"}),
        }

    def clean_item_code(self):
        return (self.cleaned_data.get("item_code") or "").strip().upper()

    def clean_item_name(self):
        return (self.cleaned_data.get("item_name") or "").strip()

    def clean_unit(self):
        return (self.cleaned_data.get("unit") or "").strip()

    def _to_decimal_or_none(self, v):
        if v in (None, ""):
            return None
        if isinstance(v, Decimal):
            return v
        try:
            return Decimal(str(v).replace(",", "").strip())
        except (InvalidOperation, ValueError):
            return None

    def clean_packing_size(self):
        v = self._to_decimal_or_none(self.cleaned_data.get("packing_size"))
        if v is not None and v < 0:
            raise forms.ValidationError("Packing size cannot be negative.")
        return v

    def clean_rate(self):
        v = self._to_decimal_or_none(self.cleaned_data.get("rate"))
        if v is not None and v < 0:
            raise forms.ValidationError("Rate cannot be negative.")
        return v


class PackingMaterialUploadForm(forms.Form):
    file = forms.FileField(
        label="Upload Packing List (.xlsx)",
        help_text="Excel columns required: Short Name, Name, Stock Keeping Unit",
        widget=forms.ClearableFileInput(attrs={
            "class": "block w-full text-sm text-slate-700 file:mr-3 file:py-2 file:px-3 file:rounded-lg file:border-0 file:bg-slate-900 file:text-white hover:file:bg-slate-800",
            "accept": ".xlsx",
        })
    )