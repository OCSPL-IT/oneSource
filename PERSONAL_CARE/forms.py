# PERSONAL_CARE/forms.py

from django import forms
from django.utils import timezone
from .models import *
from django.forms import inlineformset_factory



TAILWIND_INPUT = (
    "mt-1 block w-full rounded-md border-gray-300 shadow-sm "
    "focus:border-indigo-500 focus:ring-indigo-500 text-sm border py-2 px-2"
)

TAILWIND_TEXTAREA = (
    "mt-1 block w-full rounded-md border-gray-300 shadow-sm "
    "focus:border-indigo-500 focus:ring-indigo-500 text-sm border py-2"
)


class PCCustomerMasterForm(forms.ModelForm):
    # ---------- master-driven text fields (with suggestions) ----------
    customer_name = forms.CharField(
        required=False,
        label="Customer Name",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "customer_name_list"}
        ),
    )
    customer_profile = forms.CharField(
        required=False,
        label="Customer profile",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "customer_profile_list"}
        ),
    )
    sub_profile = forms.CharField(
        required=False,
        label="Sub profile",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "sub_profile_list"}
        ),
    )
    designation = forms.CharField(
        required=False,
        label="Designation",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "designation_list"}
        ),
    )
    place = forms.CharField(
        required=False,
        label="Place",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "place_list"}
        ),
    )
    city = forms.CharField(
        required=False,
        label="City",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "city_list"}
        ),
    )
    state = forms.CharField(
        required=False,
        label="State",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "state_list"}
        ),
    )
    zone = forms.CharField(
        required=False,
        label="Zone",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "zone_list"}
        ),
    )
    executive_name = forms.CharField(
        required=True,
        label="Executive name",
        widget=forms.TextInput(
            attrs={
                "class": TAILWIND_INPUT,
                "list": "executive_list",
                "required": "required",
            }
        ),
    )
    source = forms.CharField(
        required=False,
        label="Source",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "source_list"}
        ),
    )

    class Meta:
        model = PC_CustomerMaster
        # only *non-FK* model fields here; FKs set in save()
        fields = [
            "created_at",
            "contact_person",
            "contact_no",
            "email_id",
            "address",
        ]
        widgets = {
            "created_at": forms.DateInput(
                attrs={"class": TAILWIND_INPUT, "type": "date"}
            ),
            "contact_person": forms.TextInput(attrs={"class": TAILWIND_INPUT}),
            "contact_no": forms.TextInput(attrs={"class": TAILWIND_INPUT}),
            "email_id": forms.EmailInput(attrs={"class": TAILWIND_INPUT}),
            "address": forms.Textarea(
                attrs={"class": TAILWIND_TEXTAREA, "rows": 3}
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # For a new, unbound form, pre-fill created_at with today's date
        if not self.is_bound and not getattr(self.instance, "pk", None):
            if not self.initial.get("created_at"):
                self.fields["created_at"].initial = timezone.localdate()

        # When editing, pre-fill text fields from existing FK values
        inst = self.instance
        if inst and inst.pk:
            if inst.customer_name:
                self.fields["customer_name"].initial = inst.customer_name.subcategory
            if inst.customer_profile:
                self.fields["customer_profile"].initial = inst.customer_profile.subcategory
            if inst.sub_profile:
                self.fields["sub_profile"].initial = inst.sub_profile.subcategory
            if inst.designation:
                self.fields["designation"].initial = inst.designation.subcategory
            if inst.place:
                self.fields["place"].initial = inst.place.subcategory
            if inst.city:
                self.fields["city"].initial = inst.city.subcategory
            if inst.state:
                self.fields["state"].initial = inst.state.subcategory
            if inst.zone:
                self.fields["zone"].initial = inst.zone.subcategory
            if inst.executive_name:
                self.fields["executive_name"].initial = inst.executive_name.subcategory
            if inst.source:
                self.fields["source"].initial = inst.source.subcategory

    # helper to fetch/create master row
    def _get_or_create_pcm(self, category, value):
        value = (value or "").strip()
        if not value:
            return None
        obj, _ = PersonalCareMaster.objects.get_or_create(
            category=category,
            subcategory=value,
        )
        return obj
    def save(self, commit=True):
        # save basic fields first (no FKs)
        instance = super().save(commit=False)
        cd = self.cleaned_data
        # Executive is required – extra safety
        if not cd.get("executive_name"):
            self.add_error("executive_name", "This field is required.")
            raise forms.ValidationError("Executive name is required.")

        # map text inputs to FK fields, auto-creating masters if needed
        instance.customer_name = self._get_or_create_pcm(
            "Customer Name", cd.get("customer_name")
        )
        instance.customer_profile = self._get_or_create_pcm(
            "Customer Profile", cd.get("customer_profile")
        )
        instance.sub_profile = self._get_or_create_pcm(
            "Sub Profile", cd.get("sub_profile")
        )
        instance.designation = self._get_or_create_pcm(
            "Designation", cd.get("designation")
        )
        instance.place = self._get_or_create_pcm("Place", cd.get("place"))
        instance.city = self._get_or_create_pcm("City", cd.get("city"))
        instance.state = self._get_or_create_pcm("State", cd.get("state"))
        instance.zone = self._get_or_create_pcm("Zone", cd.get("zone"))
        instance.executive_name = self._get_or_create_pcm(
            "Executive Name", cd.get("executive_name")
        )
        instance.source = self._get_or_create_pcm("Source", cd.get("source"))

        if commit:
            instance.save()
        return instance




# --------------------  Below for sample Request ----------------------------------

class PCSampleRequestForm(forms.ModelForm):
    # ---------- master-driven text fields (with suggestions) ----------
    customer_name = forms.CharField(
        required=False,
        label="Customer Name",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "customer_name_list"}
        ),
    )
    product_name = forms.CharField(
        required=False,
        label="Product Name",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "product_name_list"}
        ),
    )
    project_name = forms.CharField(
        required=False,
        label="Project Name",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "project_name_list"}
        ),
    )
    supplier_name = forms.CharField(
        required=False,
        label="Supplier Name",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "supplier_name_list"}
        ),
    )
    remarks_master = forms.CharField(
        required=False,
        label="Remarks",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "remarks_list"}
        ),
    )
    stage = forms.CharField(
        required=False,
        label="Stage",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "stage_list"}
        ),
    )
    executive_name = forms.CharField(
        required=True,
        label="Executive Name",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "executive_list","required": "required"}
        ),
    )
    followup_date = forms.DateField(required=False,label="Last Follow-up Month",input_formats=["%Y-%m"],                       # expect '2025-12'
        widget=forms.DateInput(format="%Y-%m",     # render '2025-12'
            attrs={"class": TAILWIND_INPUT,
                "type": "month",                   # HTML month picker
            }, ), )

    class Meta:
        model = PC_SampleRequest
        # Only non-FK fields here – FK text fields handled manually
        fields = [
            "inquiry_date",
            "sample_dispatch_date",
            "project_close_date",
            "project_type",
            "contact_person",
            "contact_no",
            "email",
            "address",
            "sample_quantity",
            "price_indication_given",
            "followup_date",
            "approval_by_nmp",
            "approved_quantity",
        ]
        widgets = {
            "inquiry_date": forms.DateInput( attrs={"class": TAILWIND_INPUT, "type": "date"}  ),
            "sample_dispatch_date": forms.DateInput(attrs={"class": TAILWIND_INPUT, "type": "date"}),
            "project_close_date": forms.DateInput(attrs={"class": TAILWIND_INPUT, "type": "date"}),
            "project_type": forms.Select(attrs={"class": TAILWIND_INPUT}),
            "contact_person": forms.TextInput(attrs={"class": TAILWIND_INPUT, "list": "contact_person_list"}),
            "contact_no": forms.TextInput(attrs={"class": TAILWIND_INPUT}),
            "email": forms.TextInput(attrs={"class": TAILWIND_INPUT}),
            "address": forms.Textarea( attrs={"class": TAILWIND_TEXTAREA, "rows": 3} ),
            "sample_quantity": forms.NumberInput(attrs={"class": TAILWIND_INPUT}),
            "price_indication_given": forms.TextInput(attrs={"class": TAILWIND_INPUT}),
            "approval_by_nmp": forms.Select(attrs={"class": TAILWIND_INPUT}),
            "approved_quantity": forms.NumberInput(attrs={"class": TAILWIND_INPUT}),
            
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # For a new form, pre-fill inquiry_date with today
        if not self.is_bound and not getattr(self.instance, "pk", None):
            if not self.initial.get("inquiry_date"):
                self.fields["inquiry_date"].initial = timezone.localdate()

        # When editing, pre-fill text fields from existing FK values
        inst = self.instance
        if inst and inst.pk:
            if inst.customer_name:
                self.fields["customer_name"].initial = inst.customer_name.subcategory
            if inst.product_name:
                self.fields["product_name"].initial = inst.product_name.subcategory
            if inst.project_name:
                self.fields["project_name"].initial = inst.project_name.subcategory
            if inst.supplier_name:
                self.fields["supplier_name"].initial = inst.supplier_name.subcategory
            if inst.remarks_master:
                self.fields["remarks_master"].initial = inst.remarks_master.subcategory
            if inst.stage:
                self.fields["stage"].initial = inst.stage.subcategory
            if inst.executive_name:
                self.fields["executive_name"].initial = inst.executive_name.subcategory

    # helper to fetch/create master row
    def _get_or_create_pcm(self, category, value):
        value = (value or "").strip()
        if not value:
            return None
        obj, _ = PersonalCareMaster.objects.get_or_create(
            category=category,
            subcategory=value,
        )
        return obj

    def save(self, commit=True):
        # save basic fields first (no FKs)
        instance = super().save(commit=False)
        cd = self.cleaned_data

        # map text inputs to FK fields, auto-creating masters if needed
        instance.customer_name = self._get_or_create_pcm(
            "Customer Name", cd.get("customer_name")
        )
        instance.product_name = self._get_or_create_pcm(
            "Product Name", cd.get("product_name")
        )
        instance.project_name = self._get_or_create_pcm(
            "Project Name", cd.get("project_name")
        )
        instance.supplier_name = self._get_or_create_pcm(
            "Supplier Name", cd.get("supplier_name")
        )
        instance.remarks_master = self._get_or_create_pcm(
            "Remarks", cd.get("remarks_master")
        )
        instance.stage = self._get_or_create_pcm("Stage", cd.get("stage"))
        instance.executive_name = self._get_or_create_pcm(
            "Executive Name", cd.get("executive_name")
        )

        if commit:
            instance.save()
        return instance



# ------------------------------------------------------------------------------------
TAILWIND_INPUT = (
    "mt-1 block w-full rounded-md border-gray-300 shadow-sm "
    "focus:border-indigo-500 focus:ring-indigo-500 text-sm border py-2 px-2"
)

TAILWIND_TEXTAREA = (
    "mt-1 block w-full rounded-md border-gray-300 shadow-sm "
    "focus:border-indigo-500 focus:ring-indigo-500 text-sm border py-2"
)


class CustomerFollowupForm(forms.ModelForm):
    customer_name_text = forms.CharField(
        required=False,
        label="Customer Name",
        widget=forms.TextInput(attrs={"class": TAILWIND_INPUT, "list": "customer_name_list"}),
    )

    customer_profile_text = forms.CharField(
        required=False,
        label="Customer Profile",
        widget=forms.TextInput(attrs={"class": TAILWIND_INPUT, "list": "customer_profile_list"}),
    )

    executive_name_text = forms.CharField(
        required=False,
        label="Executive Name",
        widget=forms.TextInput(attrs={"class": TAILWIND_INPUT, "list": "executive_name_list"}),
    )

    followup_status_text = forms.CharField(
        required=False,
        label="Followup Status",
        widget=forms.TextInput(attrs={"class": TAILWIND_INPUT, "list": "followup_status_list"}),
    )

    class Meta:
        model = Customer_Followup
        fields = [
            # hidden FKs
            "customer_name",
            "customer_profile",   # ✅ NEW hidden FK
            "executive_name",
            "followup_status",

            # visible helper inputs
            "customer_name_text",
            "customer_profile_text",  # ✅ NEW visible helper
            "executive_name_text",
            "followup_status_text",

            "mode_of_followup",
            "date",
            "description",
        ]
        widgets = {
            "customer_name": forms.HiddenInput(),
            "customer_profile": forms.HiddenInput(),   # ✅ NEW
            "executive_name": forms.HiddenInput(),
            "followup_status": forms.HiddenInput(),
            "mode_of_followup": forms.Select(attrs={"class": TAILWIND_INPUT}),
            "date": forms.DateInput(attrs={"type": "date", "class": TAILWIND_INPUT}),
            "description": forms.Textarea(attrs={"rows": 3, "class": TAILWIND_TEXTAREA}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # set by *_text helpers
        self.fields["customer_name"].required = False
        self.fields["customer_profile"].required = False  # ✅ NEW
        self.fields["executive_name"].required = False
        self.fields["followup_status"].required = False

        # dropdown restrictions (optional but good)
        self.fields["followup_status"].queryset = PersonalCareMaster.objects.filter(
            category="Followup Status"
        ).order_by("subcategory")

        self.fields["customer_profile"].queryset = PersonalCareMaster.objects.filter(
            category="Sub Profile"
        ).order_by("subcategory")

        # Init visible text values when editing
        if self.instance.pk and self.instance.customer_name:
            self.fields["customer_name_text"].initial = (
                self.instance.customer_name.subcategory
                if getattr(self.instance.customer_name, "subcategory", None)
                else str(self.instance.customer_name)
            )

        if self.instance.pk and self.instance.customer_profile:
            self.fields["customer_profile_text"].initial = (
                self.instance.customer_profile.subcategory
                if getattr(self.instance.customer_profile, "subcategory", None)
                else str(self.instance.customer_profile)
            )

        if self.instance.pk and self.instance.executive_name:
            self.fields["executive_name_text"].initial = (
                self.instance.executive_name.subcategory
                if getattr(self.instance.executive_name, "subcategory", None)
                else str(self.instance.executive_name)
            )

        if self.instance.pk and self.instance.followup_status:
            self.fields["followup_status_text"].initial = (
                self.instance.followup_status.subcategory
                if getattr(self.instance.followup_status, "subcategory", None)
                else str(self.instance.followup_status)
            )

    def clean(self):
        cleaned = super().clean()
        cname = (cleaned.get("customer_name_text") or "").strip()
        prof  = (cleaned.get("customer_profile_text") or "").strip()
        ename = (cleaned.get("executive_name_text") or "").strip()
        sname = (cleaned.get("followup_status_text") or "").strip()

        # ----- Customer (from PC_CustomerMaster → customer_name PCM) -----
        if cname:
            cm_qs = (
                PC_CustomerMaster.objects
                .select_related("customer_name")
                .filter(customer_name__subcategory__iexact=cname)
            )
            if cm_qs.exists() and cm_qs.first().customer_name:
                cleaned["customer_name"] = cm_qs.first().customer_name
            else:
                self.add_error(
                    "customer_name_text",
                    "Customer not found in master. Please select a valid name from list.",
                )
        else:
            self.add_error("customer_name_text", "Customer Name is required.")

        # ----- Customer Profile (PersonalCareMaster category='Sub Profile') -----
        if prof:
            prof_qs = PersonalCareMaster.objects.filter(
                category="Sub Profile",
                subcategory__iexact=prof,
            )
            if prof_qs.exists():
                cleaned["customer_profile"] = prof_qs.first()
            else:
                self.add_error(
                    "customer_profile_text",
                    "Customer Profile not found in master. Please select from list.",
                )
        else:
            cleaned["customer_profile"] = None  # optional (make mandatory if you want)

        # ----- Executive (PersonalCareMaster category='Executive Name') -----
        if ename:
            exec_qs = PersonalCareMaster.objects.filter(
                category="Executive Name",
                subcategory__iexact=ename,
            )
            if exec_qs.exists():
                cleaned["executive_name"] = exec_qs.first()
            else:
                self.add_error(
                    "executive_name_text",
                    "Executive not found in master. Please select from list.",
                )
        else:
            cleaned["executive_name"] = None  # optional

        # ----- Followup Status (PersonalCareMaster category='Followup Status') -----
        if sname:
            status_qs = PersonalCareMaster.objects.filter(
                category="Followup Status",
                subcategory__iexact=sname,
            )
            if status_qs.exists():
                cleaned["followup_status"] = status_qs.first()
            else:
                self.add_error(
                    "followup_status_text",
                    "Followup status not found in master. Please select from list.",
                )
        else:
            cleaned["followup_status"] = None

        return cleaned


#==================================================================================


TAILWIND_INPUT = (
    "mt-1 block w-full rounded-md border-gray-300 shadow-sm "
    "focus:border-indigo-500 focus:ring-indigo-500 text-sm border py-2 px-2"
)

TAILWIND_TEXTAREA = (
    "mt-1 block w-full rounded-md border-gray-300 shadow-sm "
    "focus:border-indigo-500 focus:ring-indigo-500 text-sm border py-2"
)



class PCOtherCustomerMasterForm(forms.ModelForm):
    # ---------- master-driven text fields (with suggestions) ----------
    customer_name = forms.CharField(
        required=False,
        label="Customer Name",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "customer_name_list"}
        ),
    )
    customer_profile = forms.CharField(
        required=False,
        label="Customer profile",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "customer_profile_list"}
        ),
    )
    sub_profile = forms.CharField(
        required=False,
        label="Sub profile",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "sub_profile_list"}
        ),
    )
    designation = forms.CharField(
        required=False,
        label="Designation",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "designation_list"}
        ),
    )
    place = forms.CharField(
        required=False,
        label="Place",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "place_list"}
        ),
    )
    city = forms.CharField(
        required=False,
        label="City",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "city_list"}
        ),
    )
    state = forms.CharField(
        required=False,
        label="State",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "state_list"}
        ),
    )
    zone = forms.CharField(
        required=False,
        label="Zone",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "zone_list"}
        ),
    )
    executive_name = forms.CharField(
        required=True,
        label="Executive name",
        widget=forms.TextInput(
            attrs={
                "class": TAILWIND_INPUT,
                "list": "executive_list",
                "required": "required",
            }
        ),
    )
    source = forms.CharField(
        required=False,
        label="Source",
        widget=forms.TextInput(
            attrs={"class": TAILWIND_INPUT, "list": "source_list"}
        ),
    )

    class Meta:
        model = PC_Other_CustomerMaster
        # only *non-FK* model fields here; FKs set in save()
        fields = [
            "created_at",
            "contact_person",
            "contact_no",
            "email_id",
            "address",
            "core_business",
        ]
        widgets = {
            "created_at": forms.DateInput(
                attrs={"class": TAILWIND_INPUT, "type": "date"}
            ),
            "contact_person": forms.TextInput(attrs={"class": TAILWIND_INPUT}),
            "contact_no": forms.TextInput(attrs={"class": TAILWIND_INPUT}),
            "email_id": forms.EmailInput(attrs={"class": TAILWIND_INPUT}),
            "address": forms.Textarea(
                attrs={"class": TAILWIND_TEXTAREA, "rows": 3}
            ),
            "core_business": forms.Textarea(      # 👈 NEW
                attrs={"class": TAILWIND_TEXTAREA, "rows": 3}
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # For a new, unbound form, pre-fill created_at with today's date
        if not self.is_bound and not getattr(self.instance, "pk", None):
            if not self.initial.get("created_at"):
                self.fields["created_at"].initial = timezone.localdate()

        # When editing, pre-fill text fields from existing FK values
        inst = self.instance
        if inst and inst.pk:
            if inst.customer_name:
                self.fields["customer_name"].initial = inst.customer_name.subcategory
            if inst.customer_profile:
                self.fields["customer_profile"].initial = inst.customer_profile.subcategory
            if inst.sub_profile:
                self.fields["sub_profile"].initial = inst.sub_profile.subcategory
            if inst.designation:
                self.fields["designation"].initial = inst.designation.subcategory
            if inst.place:
                self.fields["place"].initial = inst.place.subcategory
            if inst.city:
                self.fields["city"].initial = inst.city.subcategory
            if inst.state:
                self.fields["state"].initial = inst.state.subcategory
            if inst.zone:
                self.fields["zone"].initial = inst.zone.subcategory
            if inst.executive_name:
                self.fields["executive_name"].initial = inst.executive_name.subcategory
            if inst.source:
                self.fields["source"].initial = inst.source.subcategory

    # helper to fetch/create master row
    def _get_or_create_pcm(self, category, value):
        value = (value or "").strip()
        if not value:
            return None
        obj, _ = PersonalCareMaster.objects.get_or_create(
            category=category,
            subcategory=value,
        )
        return obj

    def save(self, commit=True):
        # save basic fields first (no FKs)
        instance = super().save(commit=False)
        cd = self.cleaned_data

        # Executive is required – extra safety
        if not cd.get("executive_name"):
            self.add_error("executive_name", "This field is required.")
            raise forms.ValidationError("Executive name is required.")

        # map text inputs to FK fields, auto-creating masters if needed
        instance.customer_name = self._get_or_create_pcm(
            "Customer Name", cd.get("customer_name")
        )
        instance.customer_profile = self._get_or_create_pcm(
            "Customer Profile", cd.get("customer_profile")
        )
        instance.sub_profile = self._get_or_create_pcm(
            "Sub Profile", cd.get("sub_profile")
        )
        instance.designation = self._get_or_create_pcm(
            "Designation", cd.get("designation")
        )
        instance.place = self._get_or_create_pcm("Place", cd.get("place"))
        instance.city = self._get_or_create_pcm("City", cd.get("city"))
        instance.state = self._get_or_create_pcm("State", cd.get("state"))
        instance.zone = self._get_or_create_pcm("Zone", cd.get("zone"))
        instance.executive_name = self._get_or_create_pcm(
            "Executive Name", cd.get("executive_name")
        )
        instance.source = self._get_or_create_pcm("Source", cd.get("source"))

        if commit:
            instance.save()
        return instance

