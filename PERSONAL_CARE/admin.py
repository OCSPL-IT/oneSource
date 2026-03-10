# HR/admin.py

from django.contrib import admin, messages
from import_export import resources, fields, widgets    
from import_export.admin import ImportExportModelAdmin
from import_export.widgets import ForeignKeyWidget
from .models import *
from datetime import datetime


@admin.register(PC_Executive)
class PCExecutiveAdmin(admin.ModelAdmin):
    list_display = ("user", "active")
    list_filter = ("active",)
    autocomplete_fields = ("user", "executive")
    search_fields = ("user__username", "user__first_name", "user__last_name", "executive__subcategory")


# ===================== PersonalCareMaster =====================

class PersonalCareMasterResource(resources.ModelResource):
    class Meta:
        model = PersonalCareMaster
        # matches your first Excel: category | subcategory
        fields = ("category", "subcategory")
        import_id_fields = ("category", "subcategory")


@admin.register(PersonalCareMaster)
class PersonalCareMasterAdmin(ImportExportModelAdmin):
    resource_class = PersonalCareMasterResource

    list_display = ("category", "subcategory")
    search_fields = ("category", "subcategory")
    list_filter = ("category",)


# ===================== Helper widget for FK lookup =====================

class PCMWidget(ForeignKeyWidget):
    """
    Maps a cell value (subcategory text) to PersonalCareMaster
    filtered by a fixed category, in a tolerant way
    (ignores case + trailing spaces).
    """

    def __init__(self, category, *args, **kwargs):
        self.fixed_category = category
        super().__init__(PersonalCareMaster, "subcategory", *args, **kwargs)

    def clean(self, value, row=None, *args, **kwargs):
        value = (value or "").strip()
        if not value:
            return None

        qs = PersonalCareMaster.objects.filter(
            category__iexact=self.fixed_category,
        )

        # try exact (case-insensitive) first
        obj = qs.filter(subcategory__iexact=value).first()

        # fallback: startswith (handles trailing junk/spaces in DB)
        if obj is None:
            obj = qs.filter(subcategory__istartswith=value).first()

        if obj is None:
            raise ValueError(
                f"'{value}' not found in PersonalCareMaster "
                f"(category='{self.fixed_category}')"
            )

        return obj


# ===================== PC_CustomerMaster =====================

class PCCustomerMasterResource(resources.ModelResource):
    created_at = fields.Field(
        column_name="Created Date",
        attribute="created_at",
        widget=widgets.DateWidget(format="%d-%m-%Y"),  # match your Excel format
    )
    customer_name = fields.Field(
        column_name="Customer Name",
        attribute="customer_name",
        widget=PCMWidget("Customer Name"),
    )
    customer_profile = fields.Field(
        column_name="Customer Profile",
        attribute="customer_profile",
        widget=PCMWidget("Customer Profile"),
    )
    sub_profile = fields.Field(
        column_name="Sub Profile",
        attribute="sub_profile",
        widget=PCMWidget("Sub Profile"),
    )
    contact_person = fields.Field(
        column_name="Contact Person",
        attribute="contact_person",
    )
    designation = fields.Field(
        column_name="Designation",
        attribute="designation",
        widget=PCMWidget("Designation"),
    )
    contact_no = fields.Field(
        column_name="Contact No",
        attribute="contact_no",
    )
    email_id = fields.Field(
        column_name="Email ID",
        attribute="email_id",
    )
    address = fields.Field(
        column_name="Address",
        attribute="address",
    )
    place = fields.Field(
        column_name="Place",
        attribute="place",
        widget=PCMWidget("Place"),
    )
    city = fields.Field(
        column_name="City",
        attribute="city",
        widget=PCMWidget("City"),
    )
    state = fields.Field(
        column_name="State",
        attribute="state",
        widget=PCMWidget("State"),
    )
    zone = fields.Field(
        column_name="Zone",
        attribute="zone",
        widget=PCMWidget("Zone"),
    )
    executive_name = fields.Field(
        column_name="Executive Name",
        attribute="executive_name",
        widget=PCMWidget("Executive Name"),
    )
    source = fields.Field(
        column_name="Source",
        attribute="source",
        widget=PCMWidget("Source"),
    )

    class Meta:
        model = PC_CustomerMaster

        # 🔑 Use these columns as the “natural key”.
        # If ALL of these match an existing row → update;
        # otherwise → create a new row.
        import_id_fields = (
            "customer_name",
            "customer_profile",
            "sub_profile",
            "contact_person",
        )

        # Whitelist of fields (no 'id' here)
        fields = (
            "created_at",
            "customer_name",
            "customer_profile",
            "sub_profile",
            "contact_person",
            "designation",
            "contact_no",
            "email_id",
            "address",
            "place",
            "city",
            "state",
            "zone",
            "executive_name",
            "source",
        )


@admin.register(PC_CustomerMaster)
class PCCustomerMasterAdmin(ImportExportModelAdmin):
    resource_class = PCCustomerMasterResource

    list_display = (
        "customer_name",
        "customer_profile",
        "sub_profile",
        "contact_person",
        "designation",
        "city",
        "state",
        "zone",
        "executive_name",
        "source",
    )
    search_fields = (
        "customer_name",
        "contact_person",
        "contact_no",
        "email_id",
    )
    list_filter = (
        "customer_profile",
        "sub_profile",
        "city",
        "state",
        "zone",
        "executive_name",
        "source",
    )

    # ---- FIX: make logging safe ----
    def log_import(self, request, result, *args, **kwargs):
        """
        Override default import_export logging.
        Avoid Django admin LogEntry (which expects a real object),
        just show a friendly message instead.
        """
        totals = getattr(result, "totals", None) or {}
        rows = (
            totals.get("rows")
            or totals.get("new")
            or getattr(result, "total_rows", 0)
            or 0
        )
        self.message_user(
            request,
            f"Imported {rows} PC customer row(s) successfully.",
            level=messages.INFO,
        )





# ===================== PC_SampleRequest =====================



class SafeDateWidget(widgets.DateWidget):
    """
    A tolerant DateWidget that can handle empty cells,
    text-formatted dates, and multiple formats.
    """
    def clean(self, value, row=None, *args, **kwargs):
        if not value or str(value).strip() == "":
            return None

        # Accept Excel datetime, Python date, or str
        if isinstance(value, datetime):
            return value.date()

        val = str(value).strip()
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(val, fmt).date()
            except Exception:
                continue
        raise ValueError(f"Could not parse date '{val}' using known formats.")


class SafeIntegerWidget(widgets.IntegerWidget):
    """
    Accepts blank/space/'-'/NA etc. as None for Integer fields.
    """
    def clean(self, value, row=None, *args, **kwargs):
        if value is None:
            return None

        s = str(value).strip()
        if s == "" or s in {"-", "NA", "N/A"}:
            return None

        # normal integer handling
        return super().clean(s, row=row, *args, **kwargs)
    

# widgets / helper
class PCMWidget(ForeignKeyWidget):
    """
    Maps Excel text -> PersonalCareMaster(subcategory) filtered by category.
    If create_missing=True, it auto-creates the master row when not found.
    """
    def __init__(self, category, create_missing=False, *args, **kwargs):
        self.fixed_category = category
        self.create_missing = create_missing
        super().__init__(PersonalCareMaster, "subcategory", *args, **kwargs)

    def clean(self, value, row=None, *args, **kwargs):
        value = (value or "").strip()
        if not value:
            return None

        qs = PersonalCareMaster.objects.filter(category__iexact=self.fixed_category)

        # try exact (case-insensitive) first
        obj = qs.filter(subcategory__iexact=value).first()

        # fallback: startswith (handles trailing junk/spaces in DB)
        if obj is None:
            obj = qs.filter(subcategory__istartswith=value).first()

        # ✅ auto-create only when enabled
        if obj is None and self.create_missing:
            obj = PersonalCareMaster.objects.create(
                category=self.fixed_category,
                subcategory=value,
            )

        if obj is None:
            raise ValueError(
                f"'{value}' not found in PersonalCareMaster (category='{self.fixed_category}')"
            )

        return obj    



class PCSampleRequestResource(resources.ModelResource):
    inquiry_date = fields.Field(
        column_name="Inquiry Date",
        attribute="inquiry_date",
        widget=SafeDateWidget(),
    )
    sample_dispatch_date = fields.Field(
        column_name="Sample Dispatch Date",
        attribute="sample_dispatch_date",
        widget=SafeDateWidget(),
    )
    project_close_date = fields.Field(
        column_name="Project Close Date",
        attribute="project_close_date",
        widget=SafeDateWidget(),
    )
    customer_name = fields.Field(
        column_name="Customer Name",
        attribute="customer_name",
        widget=PCMWidget("Customer Name", create_missing=True),
        
    )
    product_name = fields.Field(
        column_name="Product Name",
        attribute="product_name",
        widget=PCMWidget("Product Name", create_missing=True),
    )
    project_name = fields.Field(
        column_name="Project Name",
        attribute="project_name",
        widget=PCMWidget("Project Name", create_missing=True),
    )
    supplier_name = fields.Field(
        column_name="Supplier Name",
        attribute="supplier_name",
        widget=PCMWidget("Supplier Name"),
    )
    remarks_master = fields.Field(
        column_name="Remarks",
        attribute="remarks_master",
        widget=PCMWidget("Remarks"),
    )
    stage = fields.Field(
        column_name="Stage",
        attribute="stage",
        widget=PCMWidget("Stage"),
    )
    executive_name = fields.Field(
        column_name="Executive Name",
        attribute="executive_name",
        widget=PCMWidget("Executive Name"),
    )

    # ---------- CONTACT / ADDRESS FIELDS ----------
    contact_person = fields.Field(
        column_name="Contact Person",
        attribute="contact_person",
    )
    contact_no = fields.Field(
        column_name="Contact No",
        attribute="contact_no",
    )
    email = fields.Field(
        column_name="Email",
        attribute="email",
    )
    address = fields.Field(
        column_name="Address",
        attribute="address",
    )

    sample_quantity = fields.Field(
        column_name="Sample Quantity (gm/ml)",
        attribute="sample_quantity",
        widget=SafeIntegerWidget(),
    )
    project_type = fields.Field(
        column_name="Project Type",
        attribute="project_type",
    )
    price_indication_given = fields.Field(
        column_name="Price Indication Given",
        attribute="price_indication_given",
    )
    followup_date = fields.Field(
        column_name="Last Followup Date",
        attribute="followup_date",
        widget=SafeDateWidget(),
    )
    approval_by_nmp = fields.Field(
        column_name="Approval",
        attribute="approval_by_nmp",
    )

    approved_quantity = fields.Field(
        column_name="ApprovedQuantity",
        attribute="approved_quantity",
        widget=SafeIntegerWidget(),   # ✅ here too
    )

    class Meta:
        model = PC_SampleRequest
        import_id_fields = (
            "customer_name",
            "product_name",
            "sample_dispatch_date",
        )
        fields = (
            "inquiry_date",
            "sample_dispatch_date",
            "year",
            "project_close_date",
            "customer_name",
            "contact_person",
            "contact_no",
            "email",
            "address",
            "product_name",
            "sample_quantity",
            "project_name",
            "project_type",
            "price_indication_given",
            "supplier_name",
            "followup_date",
            "remarks_master",
            "stage",
            "executive_name",
            "approval_by_nmp",
            "approved_quantity",
        )

    # 🔧 Normalize header for “Contact Person”
    def before_import_row(self, row, **kwargs):
        """
        Make the resource accept any header that *looks* like 'Contact Person'
        (extra spaces, different case, etc.).
        """
        if not row.get("Contact Person"):
            for key in list(row.keys()):
                if (
                    key
                    and isinstance(key, str)
                    and key.strip().lower() == "contact person"
                ):
                    row["Contact Person"] = row.get(key)
                    break
                    # 🔧 Set year from sample_dispatch_date during import
    # 🔧 Set year from sample_dispatch_date during import
    def before_save_instance(self, instance, row, **kwargs):
        # set year from sample_dispatch_date
        instance.year = instance.sample_dispatch_date.year if instance.sample_dispatch_date else None
            
            

@admin.register(PC_SampleRequest)
class PCSampleRequestAdmin(ImportExportModelAdmin):
    resource_class = PCSampleRequestResource

    list_display = (
        "inquiry_date",
        "customer_name",
        "product_name",
        "project_name",
        "contact_person",
        "sample_quantity",
        "stage",
        "executive_name",
        "approval_by_nmp",
        "followup_date",
    )
    search_fields = (
        "product_name__subcategory",
        "project_name__subcategory",
        "contact_person",
        "contact_no",
        "email",
    )
    list_filter = (
        "project_type",
        "stage",
        "executive_name",
        "approval_by_nmp",
    )

    # same safe logging style as PC_CustomerMaster
    def log_import(self, request, result, *args, **kwargs):
        """
        Override default import_export logging.
        Avoid Django admin LogEntry (which expects a real object),
        just show a friendly message instead.
        """
        totals = getattr(result, "totals", None) or {}
        rows = (
            totals.get("rows")
            or totals.get("new")
            or getattr(result, "total_rows", 0)
            or 0
        )
        self.message_user(
            request,
            f"Imported {rows} PC sample request row(s) successfully.",
            level=messages.INFO,
        )




#---------------------------------------------------------------------------------

# If SafeDateWidget is already defined above, remove this duplicate.
class SafeDateWidget(widgets.DateWidget):
    """
    A tolerant DateWidget that can handle empty cells,
    text-formatted dates, and multiple formats.
    """
    def clean(self, value, row=None, *args, **kwargs):
        if not value or str(value).strip() == "":
            return None

        if isinstance(value, datetime):
            return value.date()

        val = str(value).strip()
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(val, fmt).date()
            except Exception:
                continue
        raise ValueError(f"Could not parse date '{val}' using known formats.")


# ----------------- Customer_Followup import resource -----------------
class CustomerFollowupResource(resources.ModelResource):
    """
    Import / export mapping for Customer_Followup.
    Excel headers must match screenshot.
    """

    customer_name = fields.Field(
        column_name="Customer Name",
        attribute="customer_name",
        widget=PCMWidget("Customer Name"),
    )

    customer_profile = fields.Field(
        column_name="Customer Profile",
        attribute="customer_profile",
        widget=PCMWidget("Sub Profile"),  # category in master is "Sub Profile"
    )

    date = fields.Field(
        column_name="Date",
        attribute="date",
        widget=SafeDateWidget(),
    )

    mode_of_followup = fields.Field(
        column_name="Mode of Follow-up",
        attribute="mode_of_followup",
    )

    followup_status = fields.Field(
        column_name="Stage",
        attribute="followup_status",
        widget=PCMWidget("Followup Status"),
    )

    description = fields.Field(
        column_name="Follow-up Status",
        attribute="description",
    )

    executive_name = fields.Field(
        column_name="Executive Name",
        attribute="executive_name",
        widget=PCMWidget("Executive Name"),
    )

    class Meta:
        model = Customer_Followup

        # ✅ Identify duplicates based on your sheet columns (no 'id' needed)
        import_id_fields = (
            "date",
            "customer_name",
            "customer_profile",
            "executive_name",
            "followup_status",
            "mode_of_followup",
        )

        fields = (
            "customer_name",
            "customer_profile",
            "date",
            "mode_of_followup",
            "followup_status",
            "description",
            "executive_name",
        )

        export_order = (
            "customer_name",
            "customer_profile",
            "date",
            "mode_of_followup",
            "followup_status",
            "description",
            "executive_name",
        )



# ----------------- Customer_Followup admin -----------------
@admin.register(Customer_Followup)
class CustomerFollowupAdmin(ImportExportModelAdmin):
    resource_class = CustomerFollowupResource

    list_display = (
        "date",
        "customer_name",
        "customer_profile",   # ✅ NEW
        "executive_name",
        "followup_status",
        "mode_of_followup",
        "description",
        "created_at",
        "created_by",
    )

    search_fields = (
        "customer_name__subcategory",
        "customer_profile__subcategory",  # ✅ NEW
        "executive_name__subcategory",
        "followup_status__subcategory",
        "description",
    )

    list_filter = (
        "mode_of_followup",
        "followup_status",
        "executive_name",
        "customer_profile",  # ✅ NEW
        "created_at",
    )

    def log_import(self, request, result, *args, **kwargs):
        """
        Friendly message after import.
        """
        totals = getattr(result, "totals", None) or {}
        rows = (
            totals.get("rows")
            or totals.get("new")
            or getattr(result, "total_rows", 0)
            or 0
        )
        self.message_user(
            request,
            f"Imported {rows} customer follow-up row(s) successfully.",
            level=messages.INFO,
        )
        
        
        
        
        
# ===================== PC_Other_CustomerMaster =====================

class PCOtherCustomerMasterResource(resources.ModelResource):
    created_at = fields.Field(
        column_name="Created Date",
        attribute="created_at",
        widget=widgets.DateWidget(format="%d-%m-%Y"),
    )
    customer_name = fields.Field(
        column_name="Customer Name",
        attribute="customer_name",
        widget=PCMWidget("Customer Name"),
    )
    customer_profile = fields.Field(
        column_name="Customer Profile",
        attribute="customer_profile",
        widget=PCMWidget("Customer Profile"),
    )
    sub_profile = fields.Field(
        column_name="Sub Profile",
        attribute="sub_profile",
        widget=PCMWidget("Sub Profile"),
    )
    contact_person = fields.Field(
        column_name="Contact Person",
        attribute="contact_person",
    )
    designation = fields.Field(
        column_name="Designation",
        attribute="designation",
        widget=PCMWidget("Designation"),
    )
    contact_no = fields.Field(
        column_name="Contact No",
        attribute="contact_no",
    )
    email_id = fields.Field(
        column_name="Email ID",
        attribute="email_id",
    )
    address = fields.Field(
        column_name="Address",
        attribute="address",
    )
    place = fields.Field(
        column_name="Place",
        attribute="place",
        widget=PCMWidget("Place"),
    )
    city = fields.Field(
        column_name="City",
        attribute="city",
        widget=PCMWidget("City"),
    )
    state = fields.Field(
        column_name="State",
        attribute="state",
        widget=PCMWidget("State"),
    )
    zone = fields.Field(
        column_name="Zone",
        attribute="zone",
        widget=PCMWidget("Zone"),
    )
    executive_name = fields.Field(
        column_name="Executive Name",
        attribute="executive_name",
        widget=PCMWidget("Executive Name"),
    )
    source = fields.Field(
        column_name="Source",
        attribute="source",
        widget=PCMWidget("Source"),
    )

    class Meta:
        model = PC_Other_CustomerMaster

        # 🔑 Natural key for update vs create (same as your main model)
        import_id_fields = (
            "customer_name",
            "customer_profile",
            "sub_profile",
            "contact_person",
        )

        fields = (
            "created_at",
            "customer_name",
            "customer_profile",
            "sub_profile",
            "contact_person",
            "designation",
            "contact_no",
            "email_id",
            "address",
            "place",
            "city",
            "state",
            "zone",
            "executive_name",
            "source",
        )


@admin.register(PC_Other_CustomerMaster)
class PCOtherCustomerMasterAdmin(ImportExportModelAdmin):
    resource_class = PCOtherCustomerMasterResource

    list_display = (
        "customer_name",
        "customer_profile",
        "sub_profile",
        "contact_person",
        "designation",
        "city",
        "state",
        "zone",
        "executive_name",
        "source",
    )
    search_fields = (
        "customer_name__sName",  # PersonalCareMaster name field (change if different)
        "contact_person",
        "contact_no",
        "email_id",
    )
    list_filter = (
        "customer_profile",
        "sub_profile",
        "city",
        "state",
        "zone",
        "executive_name",
        "source",
    )

    # ---- FIX: make logging safe ----
    def log_import(self, request, result, *args, **kwargs):
        totals = getattr(result, "totals", None) or {}
        rows = (
            totals.get("rows")
            or totals.get("new")
            or getattr(result, "total_rows", 0)
            or 0
        )
        self.message_user(
            request,
            f"Imported {rows} PC other customer row(s) successfully.",
            level=messages.INFO,
        )