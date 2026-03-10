from django import forms
from django.forms import inlineformset_factory
from .models import *
from django.utils import timezone
from django.forms import DateInput, DateTimeInput
from django.forms.models import construct_instance
from django.core.exceptions import ValidationError
import datetime


# ─── S P E C    F O R M ─────────────────────────────────────────────────────────────
class SpecForm(forms.ModelForm):
    group = forms.CharField(widget=forms.HiddenInput(), required=False)
    allowed_choices = forms.MultipleChoiceField(
        required=False,
        widget=forms.SelectMultiple(attrs={
            'class': 'bc-select choices-select',
            'data-tags': 'true',
            'data-token-separators': '[", "]',
        }),
        choices=[],  # to be set in __init__
    )

    class Meta:
        model = Spec
        fields = ('group', 'name',"unit", 'spec_type', 'min_val', 'max_val',"acceptance_criteria", 'allowed_choices',"is_critical",)
        widgets = {
            'name': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Spec name'}),
            "unit": forms.TextInput(attrs={"class": "form-control", "placeholder": "Unit"}),
            'spec_type': forms.Select(attrs={'class': 'form-control spec-type-select'}),
            'min_val': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01', 'placeholder': 'e.g. 0.00'}),
            'max_val': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01', 'placeholder': 'e.g. 100.00'}),
            "acceptance_criteria": forms.TextInput(attrs={"class": "form-control", "placeholder": "e.g. NMT 1.0%"}),
            "is_critical": forms.CheckboxInput(attrs={"class": "form-check-input"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        opts = [(o.name, o.name) for o in AppearanceOption.objects.all()]
        self.fields['allowed_choices'].choices = opts
        self.fields['allowed_choices'].widget.attrs.update({
            'data-tags': 'true',
            'data-token-separators': '[", "]',
        })
        
        # When editing, convert stored CSV to list for the MultiChoice field
        if self.instance and getattr(self.instance, "allowed_choices", ""):
            csv = self.instance.allowed_choices
            self.initial["allowed_choices"] = [c.strip() for c in csv.split(",") if c.strip()]

    def clean_allowed_choices(self):
        data = self.cleaned_data.get("allowed_choices") or []
        valid = set(AppearanceOption.objects.values_list("name", flat=True))

        invalid = [c for c in data if c not in valid]
        if invalid:
            raise forms.ValidationError(f"Invalid choice(s): {', '.join(invalid)}")

        # Store as CSV in the model field (keeps your existing storage pattern)
        return ",".join(data)

    def clean(self):
        data = super().clean()
        stype   = data.get("spec_type")
        name    = data.get("name")
        minv    = data.get("min_val")
        maxv    = data.get("max_val")
        acv     = data.get("acceptance_criteria")
        choices = data.get("allowed_choices")  # CSV from clean_allowed_choices()

        if stype == Spec.TYPE_NUMERIC:
            if minv is None or maxv is None:
                raise forms.ValidationError("Numeric specs require both Min and Max.")
            if minv > maxv:
                raise forms.ValidationError("Min must be ≤ Max.")

        elif stype == Spec.TYPE_CHOICE:
            if (name or "").strip().lower() != "appearance" and not choices:
                raise forms.ValidationError("Choice specs require at least one option.")
            if acv not in (None, ""):
                raise forms.ValidationError("Acceptance criteria only applies to numeric specs.")

        return data

SpecFormSetCreate = inlineformset_factory(
    Product, Spec, form=SpecForm,
    fields=('group', 'name', 'unit', 'spec_type', 'min_val', 'max_val','acceptance_criteria', 'allowed_choices', "is_critical"),
    extra=1,
    can_delete=True,
)

SpecFormSetUpdate = inlineformset_factory(
    Product, Spec, form=SpecForm,
    fields=('group', 'name','unit', 'spec_type', 'min_val', 'max_val','acceptance_criteria', 'allowed_choices', "is_critical"),
    extra=0,
    can_delete=True,
)


# ─── P R O D U C T    F O R M ───────────────────────────────────────────────────────

class ProductForm(forms.ModelForm):
    """
    We present a 'stage' dropdown (pulled from LocalBOMDetail.item_name),
    but we don't store it to a non-existent Product.stage attribute. Instead
    we copy it into the existing Product.stages field on save. We also default
    the Product.name from FG Name when the user hasn't typed one.
    """
    name = forms.CharField(
        required=False,   # make optional so we can default it
        label="Product Name",
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter a product name'
        })
    )

    stage = forms.ChoiceField(
        required=True,
        label="Stage",
        widget=forms.Select(attrs={
            'class': 'form-select',
            'id': 'id_stage'
        }),
    )

    code = forms.CharField(
        label="BOM Code",
        required=False,
        widget=forms.TextInput(attrs={
            'readonly': 'readonly',
            'class': 'form-control',
            'id': 'id_code',
        })
    )

    item_type = forms.CharField(
        label="Item Type",
        required=False,
        widget=forms.TextInput(attrs={
            'readonly': 'readonly',
            'class': 'form-control',
            'id': 'id_item_type',
        })
    )

    stages = forms.CharField(
        label="Stages (free text)",
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g. Cutting,Assembling,Finishing'
        })
    )

    class Meta:
        model = Product
        fields = ['name', 'code', 'item_type', 'stages']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        raw = LocalBOMDetail.objects.values_list('item_name', flat=True).order_by('item_name')
        unique = list(dict.fromkeys(raw))
        self.fields['stage'].choices = [('', '-- Select Stage --')] + [(n, n) for n in unique]

        if self.instance.pk:
            self.fields['name'].initial   = self.instance.name
            self.fields['stages'].initial = self.instance.stages
            sel = self.instance.stages or ''
            self.fields['stage'].initial  = sel
            detail = LocalBOMDetail.objects.filter(item_name=sel).first()
            if detail:
                self.fields['code'].initial      = detail.bom_code
                self.fields['item_type'].initial = detail.itm_type

    def clean(self):
        data      = super().clean()
        sel_stage = data.get('stage')
        name      = data.get('name','').strip()
        detail    = None

        if sel_stage:
            data['stages']       = sel_stage
            self.instance.stages = sel_stage
            detail = LocalBOMDetail.objects.filter(item_name=sel_stage).first()
            if detail:
                data['code']            = detail.bom_code
                data['item_type']       = detail.itm_type
                self.instance.code      = detail.bom_code
                self.instance.item_type = detail.itm_type

        if not name and detail:
            data['name']       = detail.fg_name
            self.instance.name = detail.fg_name
        elif name:
            self.instance.name = name

        return data
    
# ─── I M P O R T    A P P E A R A N C E    F O R M ───────────────────────────────
class ImportAppearanceForm(forms.Form):
    file = forms.FileField(
        label="Select Excel file (.xlsx)",
        help_text="Upload the 'In Process Specs.xlsx'"
    )

class SpecUploadForm(forms.Form):
    excel_file = forms.FileField(
        label="Upload specs Excel",
        help_text="Must have columns: Name, Type, Choices, Min Value, Max Value"
    )


class FinishedGoodProductForm(forms.ModelForm):
    """
    A dedicated form for 'Finished Good' products.
    The 'Stage' field is now hidden as it duplicates the Product Name.
    """
    search_product_name = forms.CharField(
        label="Product Name",
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-input',
            'placeholder': 'Search and select a Finished Good...',
            'list': 'product-datalist',
            'autocomplete': 'off'
        })
    )

    # --- KEY CHANGE ---
    # The stage field is now hidden from the user but still populated by JavaScript.
    stage = forms.CharField(
        required=True,
        widget=forms.HiddenInput()
    )

    class Meta:
        model = Product
        fields = ['name', 'code', 'item_type', 'stages']
        widgets = {
            'name': forms.HiddenInput(),
            'code': forms.TextInput(attrs={'class': 'form-input', 'readonly': 'readonly'}),
            'item_type': forms.TextInput(attrs={'class': 'form-input', 'readonly': 'readonly'}),
            'stages': forms.HiddenInput(),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance and self.instance.pk:
            self.fields['search_product_name'].initial = self.instance.name
            self.fields['stage'].initial = self.instance.stages
            self.fields['search_product_name'].widget.attrs['readonly'] = True
            self.fields['search_product_name'].widget.attrs['style'] = 'background-color: #e9ecef; cursor: not-allowed;'

    def clean(self):
        cleaned_data = super().clean()
        
        if not self.instance.pk and not cleaned_data.get('name'):
            self.add_error('search_product_name', 'You must select a valid product from the list.')

        # The 'stages' field is populated from the hidden 'stage' input.
        stage_value = self.data.get('stage')
        if stage_value:
            cleaned_data['stages'] = stage_value
        elif not self.instance.pk:
             self.add_error(None, 'Stage could not be determined. Please re-select the product.')
            
        return cleaned_data









# This part remains the same
FREQUENCY_CHOICES = [("", "— Select Frequency —")] + [
    (f"{i} Hrs", f"{i} Hrs") for i in range(1, 37)
]

# ────────────────────────────────────────────────────────────────────────────
#  MODIFIED Base form (QCEntryForm)
# ────────────────────────────────────────────────────────────────────────────
class QCEntryForm(forms.ModelForm):
    # This section remains mostly the same
    product = forms.ModelChoiceField(queryset=Product.objects.all(), empty_label="— Select a product —", widget=forms.Select(attrs={"id": "id_product", "class": "form-select"}))
    frequency = forms.ChoiceField(required=False, label="Frequency", choices=FREQUENCY_CHOICES, widget=forms.Select(attrs={"id": "id_frequency", "class": "form-select"}))
    ar_type = forms.ChoiceField(label="AR Category", choices=QCEntry.AR_TYPE_CHOICES, widget=forms.Select(attrs={"id": "id_ar_type", "class": "form-select"}), required=True)
    stage = forms.CharField(required=False, label="Stage", widget=forms.Select(attrs={"id": "id_stage", "class": "form-select"}))
    sample_description = forms.ChoiceField(required=False, label="Sample Description", choices=[], widget=forms.Select(attrs={"id": "id_sample_description", "class": "form-control"}))
    sample_description_text = forms.CharField(required=False, label="Description Notes", widget=forms.Textarea(attrs={"id": "id_sample_description_text", "class": "form-control", "rows": 3, "maxlength": "250", "placeholder": "Up to 250 characters…"}))

    # We change it from a TextInput to a dropdown (Select widget).
    test_required_for = forms.ChoiceField(
        required=False, # Make it False, as it depends on other fields
        label="Test Required For",
        choices=[('', '— First select a Stage —')],
        widget=forms.Select(attrs={"id": "id_test_required_for", "class": "form-select"}),
    )
    test_parameters = forms.ModelMultipleChoiceField(
        queryset=QCTestParameter.objects.all(),
        required=False,
        label="Test Parameters",
        widget=forms.SelectMultiple(attrs={"id": "id_test_parameters","class": "form-select", 'style': 'width:100%'})
    )

    def __init__(self, *args, stage_choices=None, sample_description_options=None, **kwargs):
        # allow views to lock AR Category to a fixed series (e.g., "IP" or "FG")
        fixed_ar_type = kwargs.pop("fixed_ar_type", None)
        super().__init__(*args, **kwargs)

        # If the view wants us to lock the AR series, set & disable the field.
        if fixed_ar_type in dict(QCEntry.AR_TYPE_CHOICES):
            self.fields["ar_type"].initial = fixed_ar_type
            self.fields["ar_type"].widget.attrs["disabled"] = True

        # Populate test_required_for choices when product is known (edit/POST)
        instance = kwargs.get('instance')
        if not instance and 'product' in self.data:
            try:
                product_id = self.data.get('product')
                if product_id:
                    spec_groups = list(
                        Spec.objects.filter(product_id=product_id, group__isnull=False)
                        .exclude(group__exact='').values_list('group', flat=True).distinct()
                    )
                    self.fields['test_required_for'].choices = [('', '— Select Test Group —')] + [(g, g) for g in spec_groups]
            except (ValueError, TypeError):
                pass

        if "frequency" in self.fields and self.instance and self.instance.frequency:
            self.fields["frequency"].initial = self.instance.frequency

        if stage_choices:
            self.fields["stage"].widget.choices = [("", "— Select a stage —")] + [
                (opt["stages"], opt["stages"]) for opt in stage_choices if opt.get("stages")
            ]

        if sample_description_options is not None:
            self.fields["sample_description"].choices = [("", "— Select Description —")] + [(opt, opt) for opt in sample_description_options]
        else:
            self.fields["sample_description"].choices = [("", "— Select Description —")] + [
                (o.name, o.name) for o in SampleDescriptionOption.objects.all().order_by("name")
            ]

        self.label_suffix = ""
        for fld in self.fields.values():
            cls = fld.widget.attrs.get("class", "")
            if "form-control" not in cls and isinstance(fld.widget, (forms.TextInput, forms.Textarea, forms.DateInput, forms.DateTimeInput)):
                fld.widget.attrs["class"] = (cls + " form-control").strip()

        if not (self.instance and self.instance.pk):
            self.fields["entry_date"].initial = timezone.localtime().strftime("%Y-%m-%dT%H:%M")

        self.fields["sample_received_at"].label = "Sample received at QC"
        self.fields["ar_no"].label = "AR No."
        self.fields["release_by_qc_at"].label = "Sample released from QC"

        if self.instance and self.instance.pk:
            self.fields["stage"].initial = self.instance.stage
            self.fields["sample_description"].initial = self.instance.sample_description
            self.fields["sample_description_text"].initial = self.instance.sample_description_text
            self.fields["ar_type"].initial = self.instance.ar_type
            self.fields["ar_no"].initial = self.instance.ar_no

        # Initialize test_parameters from CSV → queryset for multi-select
        if self.instance and self.instance.pk and self.instance.test_parameters:
            names = [n.strip() for n in self.instance.test_parameters.split(',') if n.strip()]
            self.fields['test_parameters'].initial = QCTestParameter.objects.filter(name__in=names)

    # This part remains the same
    def _post_clean(self) -> None:
        opts = self._meta
        self.instance = construct_instance(self, self.instance, opts.fields, opts.exclude)
        try:
            self.instance.full_clean(exclude=["stage"])
        except ValidationError as exc:
            self._update_errors(exc)

    def clean(self):
        cleaned_data = super().clean()
        psd = cleaned_data.get("prod_sign_date")
        ed = cleaned_data.get("entry_date")
        if psd and ed and psd > ed:
            self.add_error("entry_date", "Entry date must be on or after production sign date.")

        # If AR Category was disabled, keep its initial (disabled fields don’t POST)
        if self.fields["ar_type"].widget.attrs.get("disabled"):
            cleaned_data["ar_type"] = self.fields["ar_type"].initial

        # Validate test group against product
        product = cleaned_data.get('product')
        test_group = cleaned_data.get('test_required_for')
        if product and test_group:
            is_valid_group = Spec.objects.filter(product=product, group=test_group).exists()
            if not is_valid_group:
                self.add_error('test_required_for', 'This test group is not valid for the selected product.')

        return cleaned_data

    def clean_test_parameters(self):
        params = self.cleaned_data.get('test_parameters')
        if params and not isinstance(params, str):
            return ', '.join([p.name for p in params])
        return params or ''

    def save(self, commit=True):
        instance = super().save(commit=False)
        test_params = self.cleaned_data.get('test_parameters', '')
        if test_params:
            if isinstance(test_params, str):
                instance.test_parameters = test_params
            else:
                instance.test_parameters = ', '.join([p.name for p in test_params])
        else:
            instance.test_parameters = ''
        if commit:
            instance.save()
        return instance

    class Meta:
        model = QCEntry
        # Make sure 'test_required_for' is in the list
        fields = [
            "product", "block", "equipment_id", "test_required_for", "test_parameters", "stage",
            "ar_type", "prod_sign_date", "batch_no", "sample_received_at",
            "entry_date", "ar_no", "sample_sent_at", "release_by_qc_at",
            "sample_description", "frequency", "sample_description_text",
        ]
        # REMOVED: test_required_for from widgets, as it's now an explicit ChoiceField
        widgets = {
            "block": forms.TextInput(attrs={"id": "id_block", "class": "form-control"}),
            "equipment_id": forms.TextInput(attrs={"id": "id_equipment_id", "class": "form-control"}),
            "prod_sign_date": DateInput(attrs={"type": "date", "class": "form-control"}),
            "batch_no": forms.TextInput(attrs={"id": "id_batch_no", "class": "form-control"}),
            "sample_received_at": DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"}),
            "entry_date": DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"}),
             # ⬇️ UPDATED: AR No – readonly + grey background
            "ar_no": forms.TextInput(
                attrs={
                    "id": "id_ar_no",
                    "class": "form-control",
                    "readonly": "readonly",
                    "style": "background-color:#f3f4f6; color:#6b7280;",
                }
            ),
            "sample_sent_at": DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"}),
            "release_by_qc_at": DateTimeInput(attrs={"type": "datetime-local", "class": "form-control"}),
        }

# ────────────────────────────────────────────────────────────────────────────
#  Phase-1 form (Production) - NO CHANGES NEEDED HERE
# ────────────────────────────────────────────────────────────────────────────
class ProductionQCEntryForm(QCEntryForm):
    """
    • Adds readonly ‘block’ field
    • Disables all widgets if status ≠ draft
    """
    def __init__(self, *args, stage_choices=None, sample_description_options=None, **kwargs):
        super().__init__(*args, stage_choices=stage_choices, sample_description_options=sample_description_options, **kwargs)
        self.fields["block"].widget.attrs["readonly"] = True
        if self.instance and self.instance.pk and self.instance.status != "draft":
            for fld in self.fields.values():
                fld.widget.attrs["disabled"] = True



# ────────────────────────────────────────────────────────────────────────────
#  F G Q C   H E A D E R  (P H A S E - 1)  –  adds Qty
# ────────────────────────────────────────────────────────────────────────────
class FGQCEntryForm(ProductionQCEntryForm):
    """
    Phase-1 (Production) header for FG QC.
    Same as ProductionQCEntryForm, plus a Qty field.
    AR Type is forced to 'FG' (UI + server-side).
    """
    qty = forms.DecimalField(
        required=False, max_digits=12, decimal_places=3, label="Qty",
        widget=forms.NumberInput(attrs={"class": "form-control", "placeholder": "e.g. 100.000", "step": "0.001"})
    )

    class Meta(ProductionQCEntryForm.Meta):
        fields  = ProductionQCEntryForm.Meta.fields + ["qty"]
        widgets = dict(ProductionQCEntryForm.Meta.widgets, **{
            "qty": forms.NumberInput(attrs={"class": "form-control", "step": "0.001"}),
        })

    def __init__(self, *args, **kwargs):
        # also accept fixed_ar_type from the view, but we always force "FG"
        kwargs.setdefault("fixed_ar_type", "FG")
        super().__init__(*args, **kwargs)
        # Prefill qty when editing
        if getattr(self.instance, "qty", None) is not None:
            self.fields["qty"].initial = self.instance.qty

    def clean(self):
        cleaned = super().clean()
        cleaned["ar_type"] = "FG"  # enforce even if someone tampers with the DOM
        return cleaned

    def save(self, commit=True):
        obj = super().save(commit=False)
        obj.ar_type = "FG"
        obj.qty  = self.cleaned_data.get("qty")
        if commit:
            obj.save()
        return obj



class SFGQCEntryForm(ProductionQCEntryForm):
    """
    Phase-1 (Production) header for SFG QC.
    Same as ProductionQCEntryForm, plus a Qty field.
    AR Type is forced to 'SFG' (UI + server-side).
    """
    qty = forms.DecimalField(
        required=False, max_digits=12, decimal_places=3, label="Qty",
        widget=forms.NumberInput(attrs={"class": "form-control", "placeholder": "e.g. 100.000", "step": "0.001"})
    )

    class Meta(ProductionQCEntryForm.Meta):
        # Inherit all fields from the base production form and add the qty field
        fields  = ProductionQCEntryForm.Meta.fields + ["qty"]
        
        # Inherit all widgets and add the specific widget for qty
        widgets = dict(ProductionQCEntryForm.Meta.widgets, **{
            "qty": forms.NumberInput(attrs={"class": "form-control", "step": "0.001"}),
        })

    def __init__(self, *args, **kwargs):
        # We always force the AR Type to "SFG" for this form
        kwargs.setdefault("fixed_ar_type", "SFG")
        super().__init__(*args, **kwargs)
        
        # Prefill the quantity field when editing an existing record
        if getattr(self.instance, "qty", None) is not None:
            self.fields["qty"].initial = self.instance.qty

    def clean(self):
        # Call the parent clean method first
        cleaned = super().clean()
        
        # Enforce the AR Type server-side to prevent tampering
        cleaned["ar_type"] = "SFG"
        return cleaned

    def save(self, commit=True):
        # Call the parent save method without committing to the database yet
        obj = super().save(commit=False)
        
        # Set the specific SFG fields before the final save
        obj.ar_type = "SFG"
        obj.qty  = self.cleaned_data.get("qty")
        
        if commit:
            obj.save()
        return obj



# ────────────────────────────────────────────────────────────────────────────
#  Phase-2 form (QC results)  – untouched logic
# ────────────────────────────────────────────────────────────────────────────
class QCResultsForm(forms.ModelForm):
    """QC team fills in results & decision."""
    instrument_id = forms.ChoiceField(
        label="Instrument ID",
        choices=QCEntry.INSTRUMENT_CHOICES,  # <-- use your choices tuple here
        required=False,
        widget=forms.Select(attrs={"class": "form-select"})
    )
    group = forms.ChoiceField(
        label="Specification Group",
        required=True,
        widget=forms.Select(attrs={"class": "form-select"}) # Will be styled by Tailwind in the template
    )

    selected_group = forms.CharField(widget=forms.HiddenInput(), required=False)
    
    ar_type = forms.ChoiceField(
        label="AR Category",
        choices=QCEntry.AR_TYPE_CHOICES,
        widget=forms.Select(attrs={"class": "form-select"}),
        required=True
    )
    decision_status = forms.ChoiceField(
        label="Final QC Decision",
        choices=QCEntry.DECISION_CHOICES,
        required=True,
        widget=forms.Select(
            attrs={"class": "form-select", "id": "id_decision_status"}
        ),
    )
    
    class Meta:
        model = QCEntry
        fields = (
            "batch_no", "sample_received_at", "ar_type", "ar_no",
            "release_by_qc_at", "group", "general_remarks", "status",
            "decision_status", "selected_group","instrument_id",
        )
        widgets = {
            "batch_no": forms.TextInput(attrs={"readonly": True}),
            "sample_received_at": DateTimeInput(attrs={"type": "datetime-local"}),
            "ar_no": forms.TextInput(),
            "release_by_qc_at": DateTimeInput(attrs={"type": "datetime-local"}),
            "general_remarks": forms.Textarea(attrs={"rows": 3, "maxlength": 250, "placeholder": "Up to 250 characters…"}),
            "status": forms.HiddenInput(),
            "instrument_id": forms.Select(attrs={"class": "form-select"}),
        }
        labels = {
            "sample_received_at": "Sample received at QC",
            "ar_no": "AR No.",
            "release_by_qc_at": "Sample released from QC",
            "decision_status": "QC Decision",
            "general_remarks": "General Remarks",
        }

    # The __init__ method is crucial for accepting the dynamic choices from the view.
    def __init__(self, *args, group_options=None, **kwargs):
        super().__init__(*args, **kwargs)

        # Populate the choices for the 'group' dropdown.
        if group_options is not None:
            self.fields['group'].choices = [("", "— Select Group —")] + [(g, g) for g in group_options]
        
        # Pre-select the group if the instance already has one saved.
        if self.instance and self.instance.group:
            self.fields['group'].initial = self.instance.group
        
        # Initialize other fields as before
        if self.instance and self.instance.pk:
            self.fields["selected_group"].initial = self.instance.selected_group
            self.fields["general_remarks"].initial = self.instance.general_remarks or ""
            self.fields["ar_type"].initial = self.instance.ar_type
            self.fields["ar_no"].initial = self.instance.ar_no

    def save(self, commit=True):
        self.instance.selected_group = self.cleaned_data.get("selected_group", "")
        self.instance.general_remarks = self.cleaned_data.get("general_remarks", "")
        return super().save(commit=commit)

    def clean_decision_status(self):
        value = self.cleaned_data.get("decision_status")
        if not value:
            raise forms.ValidationError("Final QC Decision is required.")
        return value



class SampleDescriptionOptionForm(forms.ModelForm):
    class Meta:
        model = SampleDescriptionOption
        fields = ['name']
        widgets = {
            'name': forms.TextInput(attrs={
                'class': 'w-full px-3 py-2 rounded border border-slate-300 focus:ring-indigo-500 focus:border-indigo-500',
                'placeholder': 'Enter sample description',
                'maxlength': '100',
                'autofocus': True
            }),
        }



class COAExtraForm(forms.Form):
    customer_name = forms.CharField(label="Customer Name", max_length=200)
    quantity      = forms.DecimalField(label="Quantity", max_digits=10, decimal_places=2)
    mfg_date      = forms.DateField(label="Manufacture Date")
    retest_date   = forms.DateField(label="Retest Date")


class QCTestParameterForm(forms.ModelForm):
    """
    Form for creating a new QCTestParameter.
    """
    class Meta:
        model = QCTestParameter # Updated model
        fields = ['name']
        widgets = {
            'name': forms.TextInput(attrs={
                'class': 'w-full px-3 py-2 rounded border border-slate-300 focus:ring-indigo-500 focus:border-indigo-500',
                'placeholder': 'Enter QC test parameter name',
                'maxlength': '100',
                'autofocus': True
            }),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['name'].label = "Parameter Name"

# ─────────────────────────────────────────────────────────────────────────────
#  D A I L Y   Q A   R E P O R T   F O R M S
# ─────────────────────────────────────────────────────────────────────────────
from django import forms
from django.core.exceptions import ValidationError
from django.forms.models import inlineformset_factory, BaseInlineFormSet

from .models import DailyQAReport, IncomingMaterial, PDLSample


# ─────────────────────────────────────────────────────────────────────────────
# Standalone header form (used on the separate PDL page)
# ─────────────────────────────────────────────────────────────────────────────
class PDLHeaderForm(forms.Form):
    report_date = forms.DateField(
        widget=forms.DateInput(attrs={"type": "date", "class": "form-control"}),
        label="Report Date",
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main Daily QA Report Form
# ─────────────────────────────────────────────────────────────────────────────
class DailyQAReportForm(forms.ModelForm):
    class Meta:
        model = DailyQAReport
        fields = [
            "report_date",
            "customer_complaints",
            "analytical_mistakes",
            "process_deviations",
            "incident_first_aid_injury",
            "ftr_percent",
            "analytical_downtime_hrs",
            "finished_goods_inspections",
            "any_other_abnormality",
            "safety_observation_text",
            "obs_total", "obs_closed", "obs_open",
            "near_miss_total", "near_miss_closed", "near_miss_open",
        ]
        widgets = {
            "report_date": forms.DateInput(attrs={"type": "date", "class": "form-control"}),
            "any_other_abnormality": forms.Textarea(
                attrs={"rows": 2, "placeholder": "Describe any other abnormal events...", "class": "form-control"}
            ),
            "safety_observation_text": forms.Textarea(
                attrs={"rows": 2, "placeholder": "Key safety observations...", "class": "form-control"}
            ),
        }
        labels = {
            "report_date": "Report Date",
            "customer_complaints": "Customer Complaints",
            "analytical_mistakes": "Analytical Mistakes",
            "process_deviations": "Process Deviations",
            "incident_first_aid_injury": "First-Aid / Injury",
            "ftr_percent": "FTR (%)",
            "analytical_downtime_hrs": "Analytical Downtime (hrs)",
            "finished_goods_inspections": "FG Inspections",
            "any_other_abnormality": "Other Abnormalities",
            "safety_observation_text": "Safety Observation Summary",
        }

    def clean(self):
        cleaned = super().clean()
        non_negative_ints = [
            "customer_complaints", "analytical_mistakes", "process_deviations",
            "incident_first_aid_injury", "finished_goods_inspections",
            "obs_total", "obs_closed", "obs_open",
            "near_miss_total", "near_miss_closed", "near_miss_open",
        ]
        for f in non_negative_ints:
            v = cleaned.get(f)
            if v is None:
                cleaned[f] = 0
            elif v < 0:
                self.add_error(f, "Must be ≥ 0.")
        for f in ["ftr_percent", "analytical_downtime_hrs"]:
            v = cleaned.get(f)
            if v is None:
                cleaned[f] = 0
            elif v < 0:
                self.add_error(f, "Must be ≥ 0.")
        return cleaned


# ─────────────────────────────────────────────────────────────────────────────
# Incoming Material inline formset
# ─────────────────────────────────────────────────────────────────────────────
class IncomingMaterialForm(forms.ModelForm):
    class Meta:
        model = IncomingMaterial
        fields = ["material_type", "material", "supplier", "qty_mt", "status", "remarks"]
        widgets = {
            "material_type": forms.Select(attrs={"class": "form-select"}),
            "material": forms.TextInput(attrs={"placeholder": "Enter material name", "class": "form-control"}),
            "supplier": forms.TextInput(attrs={"placeholder": "Supplier name", "class": "form-control"}),
            "qty_mt": forms.NumberInput(
                attrs={"step": "0.001", "class": "form-control", "style": "width:120px;", "min": "0"}
            ),
            "status": forms.Select(attrs={"class": "form-select"}),
            "remarks": forms.Textarea(attrs={"rows": 1, "class": "form-control", "placeholder": "Any remarks..."}),
        }

    def clean_qty_mt(self):
        v = self.cleaned_data.get("qty_mt")
        if v is None:
            return 0
        if v < 0:
            raise ValidationError("Quantity must be ≥ 0.")
        return v

    def clean_material(self): return (self.cleaned_data.get("material") or "").strip()
    def clean_supplier(self): return (self.cleaned_data.get("supplier") or "").strip()
    def clean_remarks(self):  return (self.cleaned_data.get("remarks") or "").strip()


IncomingFS = inlineformset_factory(
    DailyQAReport,
    IncomingMaterial,
    form=IncomingMaterialForm,
    fields=["material_type", "material", "supplier", "qty_mt", "status", "remarks"],
    extra=3,
    can_delete=True,
)


# ─────────────────────────────────────────────────────────────────────────────
# PDL Samples inline formset (used on Daily form and separate PDL page)
# ─────────────────────────────────────────────────────────────────────────────
class _PDLBaseFS(BaseInlineFormSet):
    """Enforce unique sample_name per report (case-insensitive)."""
    ENFORCE_UNIQUE = True
    def clean(self):
        super().clean()
        if not self.ENFORCE_UNIQUE:
            return
        seen = set()
        for form in self.forms:
            if not getattr(form, "cleaned_data", None) or form.cleaned_data.get("DELETE"):
                continue
            name = (form.cleaned_data.get("sample_name") or "").strip().lower()
            if not name:
                continue
            if name in seen:
                form.add_error("sample_name", "Duplicate sample name for this report.")
            seen.add(name)


class PDLForm(forms.ModelForm):
    class Meta:
        model = PDLSample
        fields = ["stage", "sample_name", "result", "remarks"]
        widgets = {
            "stage": forms.TextInput(attrs={"class": "form-control", "placeholder": "e.g., Filtration"}),
            "sample_name": forms.TextInput(attrs={"class": "form-control", "placeholder": "e.g., PDL-123"}),
            "result": forms.Select(attrs={"class": "form-select"}),  # uses model choices
            "remarks": forms.Textarea(attrs={"rows": 1, "class": "form-control", "placeholder": "Optional remarks"}),
        }
    def clean_stage(self): return (self.cleaned_data.get("stage") or "").strip()
    def clean_sample_name(self):
        name = (self.cleaned_data.get("sample_name") or "").strip()
        if not name:
            raise ValidationError("Sample name is required.")
        return name
    def clean_remarks(self): return (self.cleaned_data.get("remarks") or "").strip()


# Define once and export under BOTH names to preserve existing imports/usages.
PDLFS = inlineformset_factory(
    DailyQAReport,
    PDLSample,
    form=PDLForm,
    formset=_PDLBaseFS,
    fields=["stage", "sample_name", "result", "remarks"],
    extra=3,
    can_delete=True,
)
PDLOnlyFS = PDLFS  # alias for the standalone PDL page

# ------------------------------------------------------------------------
#               CustomerComplaint
# ------------------------------------------------------------------------

from django import forms
from .models import CustomerComplaint, AlfaProductMaster as AlfaProduct

class CustomerComplaintForm(forms.ModelForm):
    product_name = forms.CharField(
        label="Product name",
        widget=forms.TextInput(attrs={
            "list": "alfa-options",
            "placeholder": "Start typing Alfa product…",
            "style": "color:#dc2626;font-weight:600;",
            "autocomplete": "off",
        })
    )

    finished_product_name = forms.CharField(
        label="Finished product name",
        required=False,
        widget=forms.TextInput(attrs={
            "readonly": "readonly",
            "placeholder": "(auto-filled from Alfa Master)",
        })
    )

    class Meta:
        model = CustomerComplaint
        fields = [
            "complaint_date",
            "complaint_no",
            "product_name",
            "finished_product_name",   # newly added
            "customer_name",
            "nature_of_complaint",
            "complaint_type",
            "investigation",
            "corrective_action",
            "preventive_action",
            "status",
        ]
        widgets = {
            "complaint_date": forms.DateInput(attrs={"type": "date"}),
            "nature_of_complaint": forms.Textarea(attrs={"rows": 2}),
            "investigation": forms.Textarea(attrs={"rows": 2}),
            "corrective_action": forms.Textarea(attrs={"rows": 2}),
            "preventive_action": forms.Textarea(attrs={"rows": 2}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Populate datalist with active Alfa products
        self.alfa_options = list(
            AlfaProduct.objects.filter(is_active=True)
            .order_by("alfa_name")
            .values_list("alfa_name", flat=True)
        )

        # Pre-fill finished_product_name for edit form or existing instance
        p = (
            self.initial.get("product_name")
            or (self.instance.product_name if getattr(self.instance, "pk", None) else "")
            or ""
        ).strip()

        if p and not (
            self.initial.get("finished_product_name")
            or getattr(self.instance, "finished_product_name", "")
        ):
            hit = (
                AlfaProduct.objects
                .filter(is_active=True, alfa_name__iexact=p)
                .values_list("finished_product_name", flat=True)
                .first()
            )
            if hit:
                self.initial["finished_product_name"] = hit

    def clean(self):
        cleaned = super().clean()
        p = (cleaned.get("product_name") or "").strip()
        f = (cleaned.get("finished_product_name") or "").strip()

        # Auto-fill on form submission as well
        if p and not f:
            fin = (
                AlfaProduct.objects
                .filter(is_active=True, alfa_name__iexact=p)
                .values_list("finished_product_name", flat=True)
                .first()
            )
            if fin:
                cleaned["finished_product_name"] = fin

        return cleaned

# -----------------------------------------------------------------------------------------------
# AnalyticalDowntime
# ----------------------------------------------------------------------------------------------

from django import forms
from .models import AnalyticalDowntime, LocalBOMDetail,QCInstrument

class AnalyticalDowntimeForm(forms.ModelForm):
    # Stage as free-text with datalist suggestions
    stage = forms.CharField(
        required=False,
        label="Stage",
        widget=forms.TextInput(attrs={
            "class": "form-control",
            "list": "stage-options",          # <datalist id="stage-options"> in template
            "placeholder": "Start typing to search…",
            "autocomplete": "off",
        }),
        help_text="Type to filter, then pick the stage.",
    )

    class Meta:
        model = AnalyticalDowntime
        # ✅ Removed incident_no from editable form (handled automatically)
        fields = [
            "instrument_id", "start_at", "end_at", "ongoing",
            "category", "short_reason", "detail_reason",
            "stage", "product_name", "batch_no", "tests_delayed",
            "retest_due_date", "resolved_by", "remarks", "status",
        ]
        widgets = {
            "start_at": forms.DateTimeInput(attrs={"type": "datetime-local"}),
            "end_at": forms.DateTimeInput(attrs={"type": "datetime-local"}),
            "retest_due_date": forms.DateInput(attrs={"type": "date"}),
            "detail_reason": forms.Textarea(attrs={"rows": 3}),
            "remarks": forms.Textarea(attrs={"rows": 2}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # ✅ Instrument dropdown from master
        instruments = (
            QCInstrument.objects.filter(is_active=True)
            .order_by("category", "name", "code")
            .values_list("code", "name")
        )
        if instruments.exists():
            self.fields["instrument_id"].widget = forms.Select(
                choices=[("", "-- Select Instrument --")] +
                        [(code, f"{name} – {code}") for code, name in instruments],
                attrs={"class": "form-control"},
            )
        else:
            # Fallback: plain text if no master data
            self.fields["instrument_id"].widget = forms.TextInput(
                attrs={
                    "class": "form-control",
                    "placeholder": "Instrument ID (master empty)",
                }
            )

        # ---- Build Stage suggestions (DISTINCT without ORDER BY; sort in Python) ----
        stages_qs = (
            LocalBOMDetail.objects
            .exclude(item_name__isnull=True)
            .exclude(item_name__exact="")
            .values_list("item_name", flat=True)
            .order_by()                      # SQL Server friendly with distinct()
            .distinct()
        )

        # Case-insensitive unique & sorted suggestions
        seen = set()
        stages = []
        for s in stages_qs:
            k = (s or "").strip()
            if not k:
                continue
            key = k.lower()
            if key not in seen:
                seen.add(key)
                stages.append(k)
        self.stage_options = sorted(stages, key=str.lower)

        # Pre-fill stage if editing
        if self.instance and getattr(self.instance, "stage", ""):
            self.fields["stage"].initial = self.instance.stage

        # ---- Add a read-only pseudo-field for displaying Incident No ----
        self.incident_display = getattr(self.instance, "incident_no", "") or "(auto-generated on save)"

    def clean_stage(self):
        return (self.cleaned_data.get("stage") or "").strip()

    def clean(self):
        cleaned = super().clean()
        stage = (cleaned.get("stage") or "").strip()
        product = (cleaned.get("product_name") or "").strip()

        # Auto-fill product from stage if empty
        if stage and not product:
            hit = (
                LocalBOMDetail.objects
                .filter(item_name__iexact=stage)
                .values_list("fg_name", flat=True)
                .first()
            )
            if hit:
                cleaned["product_name"] = hit
        return cleaned
    
# -------------------------------------------------
# Deviation Form
# -------------------------------------------------
from django import forms
from .models import Deviation, AlfaProductMaster as AlfaProduct


class DeviationForm(forms.ModelForm):
    PLANT_CHOICES = [
        ("", "— Select Block —"),
        ("A-Block", "A-Block"),
        ("B-Block", "B-Block"),
        ("C-Block", "C-Block"),
        ("D-Block", "D-Block"),
        ("E-Block", "E-Block"),
    ]

    plant = forms.ChoiceField(
        label="Plant",
        choices=PLANT_CHOICES,
        required=False,
        widget=forms.Select(attrs={"class": "form-select"})
    )

    product = forms.CharField(
        label="Product (Alfa)",
        widget=forms.TextInput(attrs={
            "list": "alfa-options",
            "placeholder": "Start typing alfa…",
            "style": "color:#dc2626;font-weight:600;",
            "autocomplete": "off",
        })
    )

    finished_product = forms.CharField(
        label="Finished Product",
        required=False,
        widget=forms.TextInput(attrs={"readonly": "readonly", "placeholder": "(auto)"}),
    )

    class Meta:
        model = Deviation
        fields = [
            "date",
            "status",
            "product",
            "plant",  # keep order same as HTML
            "batch_no",
            "description",
            "root_cause",
            "corrective_action",
            "preventive_action",
            "finished_product",
        ]
        widgets = {
            "date": forms.DateInput(attrs={"type": "date"}),
            "description": forms.Textarea(attrs={"rows": 3}),
            "root_cause": forms.Textarea(attrs={"rows": 3}),
            "corrective_action": forms.Textarea(attrs={"rows": 3}),
            "preventive_action": forms.Textarea(attrs={"rows": 3}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.alfa_options = list(
            AlfaProduct.objects.filter(is_active=True)
            .order_by("alfa_name")
            .values_list("alfa_name", flat=True)
        )
        p = (
            self.initial.get("product")
            or (self.instance.product if getattr(self.instance, "pk", None) else "")
            or ""
        ).strip()
        if p and not (
            self.initial.get("finished_product")
            or getattr(self.instance, "finished_product", "")
        ):
            hit = (
                AlfaProduct.objects.filter(is_active=True, alfa_name__iexact=p)
                .values_list("finished_product_name", flat=True)
                .first()
            )
            if hit:
                self.initial["finished_product"] = hit


# ------------------------------------------------------------------------------------------
#  AnalyticalMistake
# ------------------------------------------------------------------------------------------

from django import forms
from .models import AnalyticalMistake, AlfaProductMaster as AlfaProduct

class AnalyticalMistakeForm(forms.ModelForm):
    PLANT_CHOICES = [
        ("", "— Select Block —"),
        ("A-Block", "A-Block"),
        ("B-Block", "B-Block"),
        ("C-Block", "C-Block"),
        ("D-Block", "D-Block"),
        ("E-Block", "E-Block"),
    ]

    product = forms.CharField(
        label="Product (Alfa)",
        widget=forms.TextInput(attrs={
            "list": "alfa-options",
            "placeholder": "Start typing alfa…",
            "style": "color:#dc2626;font-weight:600;",
            "autocomplete": "off",
        })
    )

    finished_product = forms.CharField(
        label="Finished Product",
        required=False,
        widget=forms.TextInput(attrs={"readonly": "readonly", "placeholder": "(auto)"}),
    )

    plant = forms.ChoiceField(
        label="Plant",
        required=False,
        choices=PLANT_CHOICES,
        widget=forms.Select()
    )

    class Meta:
        model = AnalyticalMistake
        fields = [
            "date", "product", "finished_product",
            "plant", "batch_no",
            "description", "root_cause",
            "corrective_action", "preventive_action",
        ]
        widgets = {
            "date": forms.DateInput(attrs={"type": "date"}),
            "description": forms.Textarea(attrs={"rows": 3}),
            "root_cause": forms.Textarea(attrs={"rows": 3}),
            "corrective_action": forms.Textarea(attrs={"rows": 3}),
            "preventive_action": forms.Textarea(attrs={"rows": 3}),
        }

    # Helpers used by the template
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.alfa_options = list(
            AlfaProduct.objects.filter(is_active=True)
            .order_by("alfa_name")
            .values_list("alfa_name", flat=True)
        )
        # Backfill finished product on edit
        p = (
            self.initial.get("product")
            or (self.instance.product if getattr(self.instance, "pk", None) else "")
            or ""
        ).strip()
        if p and not (self.initial.get("finished_product") or getattr(self.instance, "finished_product", "")):
            hit = (
                AlfaProduct.objects.filter(is_active=True, alfa_name__iexact=p)
                .values_list("finished_product_name", flat=True)
                .first()
            )
            if hit:
                self.initial["finished_product"] = hit

    def clean(self):
        cleaned = super().clean()
        p = (cleaned.get("product") or "").strip()
        f = (cleaned.get("finished_product") or "").strip()
        if p and not f:
            fp = (
                AlfaProduct.objects.filter(is_active=True, alfa_name__iexact=p)
                .values_list("finished_product_name", flat=True)
                .first()
            )
            if fp:
                cleaned["finished_product"] = fp
        return cleaned



# -------------------------------------------------------------------------------------
############  QC Calibration #####################

BASE_INPUT_CLASS = (
    "mt-1 block w-full rounded-lg border border-slate-300 px-3 py-2 text-sm "
    "bg-white shadow-sm "
    "focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 "
    "transition ease-out duration-150"
)

BASE_SELECT_CLASS = (
    "mt-1 block w-full rounded-lg border border-slate-300 px-3 py-2 text-sm "
    "bg-white shadow-sm cursor-pointer "
    "focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 "
    "transition ease-out duration-150"
)

BASE_TEXTAREA_CLASS = (
    "mt-1 block w-full rounded-lg border border-slate-300 px-3 py-2 text-sm "
    "bg-white shadow-sm resize-y min-h-[80px] "
    "focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 "
    "transition ease-out duration-150"
)

BASE_CHECKBOX_CLASS = (
    "h-4 w-4 rounded border-slate-300 text-indigo-600 "
    "focus:ring-indigo-500 focus:ring-offset-0"
)



# -----------------------------
# Instrument Master Form
# -----------------------------
class QCInstrumentFormNew(forms.ModelForm):
    # Force a normal BooleanField -> checkbox (not tri-state select)
    is_active = forms.BooleanField(
        required=False,
        initial=True,
        label="Active",
        widget=forms.CheckboxInput(attrs={"class": BASE_CHECKBOX_CLASS}),
    )

    class Meta:
        model = QCInstrument
        fields = ["instument_id", "name", "code", "category", "is_active", "notes"]
        widgets = {
            "instument_id": forms.TextInput(
                attrs={
                    "class": BASE_INPUT_CLASS,
                    "placeholder": "SHIMADZU GC/001",
                    "autocomplete": "off",
                }
            ),
            "name": forms.TextInput(
                attrs={
                    "class": BASE_INPUT_CLASS,
                    "placeholder": "Instrument name",
                    "autocomplete": "off",
                }
            ),
            "code": forms.TextInput(
                attrs={
                    "class": BASE_INPUT_CLASS,
                    "placeholder": "OCSPL/QC/GC/001",
                    "autocomplete": "off",
                }
            ),
            "category": forms.TextInput(
                attrs={
                    "class": BASE_INPUT_CLASS,
                    "placeholder": "GC / HPLC / KF ...",
                    "autocomplete": "off",
                }
            ),
            # ❌ do NOT put is_active here – it’s defined above
            "notes": forms.Textarea(
                attrs={
                    "class": BASE_TEXTAREA_CLASS,
                    "rows": 3,
                    "placeholder": "Additional notes / location etc.",
                }
            ),
        }

    def clean_is_active(self):
        # Make sure None becomes False instead of tri-state
        return bool(self.cleaned_data.get("is_active"))

# -----------------------------
# Calibration Schedule Form
# -----------------------------
class InstrumentChoiceField(forms.ModelChoiceField):
    """Show instument_id in the dropdown label."""

    def label_from_instance(self, obj: QCInstrument) -> str:
        if obj.instument_id:
            return obj.instument_id
        # fallback if ID is empty
        return f"{obj.name or ''} {obj.code or ''}".strip()


def get_schedule_year_choices(start_offset=-1, end_offset=1):
    """
    Build choices like ('2025-2026', '2025-2026') from
    current_year + start_offset   to   current_year + end_offset.
    """
    today_year = datetime.date.today().year
    choices = []
    for y in range(today_year + start_offset, today_year + end_offset + 1):
        label = f"{y}-{y + 1}"
        choices.append((label, label))
    return choices


class QCCalibrationScheduleForm(forms.ModelForm):
    # override instrument field so label = instument_id
    instrument = InstrumentChoiceField(
        queryset=QCInstrument.objects.filter(is_active=True)
        .order_by("category", "instument_id"),
        label="Instrument ID",
        widget=forms.Select(attrs={"class": BASE_SELECT_CLASS}),
    )

    # override schedule_year as a dropdown
    schedule_year = forms.ChoiceField(
        choices=(),  # filled in __init__
        required=False,
        label="Schedule Year",
        widget=forms.Select(attrs={"class": BASE_SELECT_CLASS}),
    )

    class Meta:
        model = QCCalibrationSchedule
        fields = [
            "instrument",
            "schedule_year",
            "calibration_date",
            "calibration_due_date",
            "reminder_date",
            "remarks",
        ]
        widgets = {
            "calibration_date": forms.DateInput(
                attrs={
                    "type": "date",
                    "class": BASE_INPUT_CLASS,
                    "placeholder": "dd-mm-yyyy",
                }
            ),
            "calibration_due_date": forms.DateInput(
                attrs={
                    "type": "date",
                    "class": BASE_INPUT_CLASS,
                    "placeholder": "dd-mm-yyyy",
                }
            ),
            "reminder_date": forms.DateInput(
                attrs={
                    "type": "date",
                    "class": BASE_INPUT_CLASS,
                    "placeholder": "dd-mm-yyyy",
                }
            ),
            "remarks": forms.Textarea(
                attrs={
                    "class": BASE_TEXTAREA_CLASS,
                    "rows": 2,
                    "placeholder": "Any remarks for this calibration…",
                }
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # set dynamic year choices (current year and surrounding years)
        self.fields["schedule_year"].choices = get_schedule_year_choices()

        # default initial = current year-next year if not editing
        if not self.instance.pk and not self.initial.get("schedule_year"):
            current_year = datetime.date.today().year
            self.fields["schedule_year"].initial = f"{current_year}-{current_year + 1}"



# -----------------------------------------------------------------------------------------
### FG QC STATUS FORM
class FGProductQCStatusForm(forms.ModelForm):
    # override as ChoiceField so we control choices
    product = forms.ChoiceField(
        label="Product",
        choices=[],  # filled in __init__
        widget=forms.Select(
            attrs={
                "class": (
                    "block w-full rounded-md border border-slate-300 bg-white "
                    "py-2.5 px-3 text-sm shadow-sm "
                    "focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                ),
            }
        ),
    )

    class Meta:
        model = FGProductQCStatus
        fields = [
            "date",
            "product",
            "approved_qty",
            "off_spec_qty",
            "under_analysis",
            "total_qty",
            "remark",
        ]
        widgets = {
            "date": forms.DateInput(
                attrs={
                    "type": "date",
                    "class": (
                        "block w-full rounded-md border border-slate-300 bg-slate-100 "
                        "py-2.5 px-3 text-sm shadow-sm "
                        "focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    ),
                }
            ),
            "approved_qty": forms.NumberInput(
                attrs={
                    "class": (
                        "block w-full rounded-md border border-slate-300 bg-white "
                        "py-2.5 px-3 text-sm text-right shadow-sm "
                        "focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    ),
                    "step": "0.001",
                    "placeholder": "0.000",
                }
            ),
            "off_spec_qty": forms.NumberInput(
                attrs={
                    "class": (
                        "block w-full rounded-md border border-slate-300 bg-white "
                        "py-2.5 px-3 text-sm text-right shadow-sm "
                        "focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    ),
                    "step": "0.001",
                    "placeholder": "0.000",
                }
            ),
            "under_analysis": forms.NumberInput(
                attrs={
                    "class": (
                        "block w-full rounded-md border border-slate-300 bg-white "
                        "py-2.5 px-3 text-sm text-right shadow-sm "
                        "focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    ),
                    "step": "0.001",
                    "placeholder": "0.000",
                }
            ),
            "total_qty": forms.NumberInput(
                attrs={
                    "class": (
                        "block w-full rounded-md border border-slate-300 "
                        "py-2.5 px-3 text-sm text-right text-slate-700 shadow-sm "
                        
                    ),
                    "step": "0.001",
            
                }
            ),
            "remark": forms.Textarea(
                attrs={
                    "class": (
                        "block w-full rounded-md border border-slate-300 bg-white "
                        "py-2.5 px-3 text-sm shadow-sm "
                        "focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    ),
                    "rows": 3,
                    "placeholder": "Optional remark…",
                }
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # 1) get ALL names from Product
        raw_names = Product.objects.values_list("name", flat=True)

        # 2) dedupe in Python on trimmed + uppercased key
        seen = {}  # key -> display_name
        for n in raw_names:
            if not n:
                continue
            display = n.strip()
            key = display.upper()
            if key not in seen:
                seen[key] = display

        # 3) sort nicely and build choices
        names = sorted(seen.values(), key=lambda x: x.upper())
        self.fields["product"].choices = [("", "— Select Product —")] + [
            (n, n) for n in names
        ]

        # 4) edit mode – preselect stored product name
        if self.instance and self.instance.pk:
            self.fields["product"].initial = self.instance.product

    
    
    

class InstrumentOccupancyForm(forms.ModelForm):
    class Meta:
        model = InstrumentOccupancy
        fields = ["date","area", "make", "model", "occupancy_percent", "remarks"]

        widgets = {
            "date": forms.DateInput(
                attrs={
                    "type": "date",
                    "class": (
                        "block w-full rounded-md border border-slate-300 bg-white "
                        "py-2.5 px-3 text-sm shadow-sm "
                        "focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    ),
                }
            ),
            "area": forms.TextInput(
                attrs={
                    "class": (
                        "block w-full rounded-md border border-slate-300 bg-white "
                        "py-2.5 px-3 text-sm shadow-sm "
                        "focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    ),
                    "placeholder": "e.g. GC-1",
                }
            ),
            "make": forms.TextInput(
                attrs={
                    "class": (
                        "block w-full rounded-md border border-slate-300 bg-white "
                        "py-2.5 px-3 text-sm shadow-sm "
                        "focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    ),
                    "placeholder": "e.g. Shimadzu",
                }
            ),
            "model": forms.TextInput(
                attrs={
                    "class": (
                        "block w-full rounded-md border border-slate-300 bg-white "
                        "py-2.5 px-3 text-sm shadow-sm "
                        "focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    ),
                    "placeholder": "e.g. GC-2014",
                }
            ),
            "occupancy_percent": forms.NumberInput(
                attrs={
                    "class": (
                        "block w-full rounded-md border border-slate-300 bg-white "
                        "py-2.5 px-3 text-sm text-right shadow-sm "
                        "focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    ),
                    "step": "0.01",
                    "min": "0",
                    "max": "100",
                    "placeholder": "0.00 – 100.00",
                }
            ),
            "remarks": forms.Textarea(
                attrs={
                    "class": (
                        "block w-full rounded-md border border-slate-300 bg-white "
                        "py-2.5 px-3 text-sm shadow-sm "
                        "focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    ),
                    "rows": 3,
                    "placeholder": "Optional remarks…",
                }
            ),
        }

    def clean_occupancy_percent(self):
        value = self.cleaned_data.get("occupancy_percent")
        if value is not None and (value < 0 or value > 100):
            raise forms.ValidationError("Occupancy must be between 0 and 100%.")
        return value