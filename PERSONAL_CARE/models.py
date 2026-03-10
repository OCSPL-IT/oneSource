# app_name/models.py
from django.db import models
from django.utils import  timezone
from django.conf import settings
from django.contrib.auth import get_user_model
User = get_user_model()


class PersonalCareMaster(models.Model):
    category = models.CharField(max_length=100)
    subcategory = models.CharField(max_length=100)

    class Meta:
        db_table = "personal_care_master"   # optional: custom table name
        verbose_name = "Personal Care Master"
        verbose_name_plural = "Personal Care Master"

    def __str__(self):
        return f"{self.category} - {self.subcategory}"



class PC_Executive(models.Model):
    """
    Links a Django user to a PersonalCareMaster row for 'Executive Name'.
    Used to restrict customer views to that executive.
    """
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="pc_executive",
    )
    executive = models.ForeignKey(
        PersonalCareMaster,
        on_delete=models.PROTECT,
        limit_choices_to={"category": "Executive Name"},
        related_name="pc_executive_links",
        blank=True,null=True
    )
    active = models.BooleanField(default=True)

    class Meta:
        db_table = "pc_executive"
        verbose_name = "PC Executive"
        verbose_name_plural = "PC Executives"

    def __str__(self):
        uname = self.user.get_full_name() or self.user.username
        return f"{uname} → {self.executive.subcategory}"



class PC_CustomerMaster(models.Model):
    created_at = models.DateField(null=True,blank=False,verbose_name="Created date",default=timezone.now,)
    customer_name = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL, null=True, blank=True,
        related_name="customer_Name",limit_choices_to={"category": "Customer Name"},)
    # will use PersonalCareMaster rows where category = "Customer Profile"
    customer_profile = models.ForeignKey( PersonalCareMaster,on_delete=models.SET_NULL, null=True, blank=True,
        related_name="customer_profile_customers",limit_choices_to={"category": "Customer Profile"},)
    # will use rows where category = "Sub Profile"
    sub_profile = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL, null=True, blank=True,
        related_name="sub_profile_customers",limit_choices_to={"category": "Sub Profile"},)
    contact_person = models.CharField(max_length=255, blank=True, null=True)
    # will use rows where category = "Designation"
    designation = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="designation_customers",limit_choices_to={"category": "Designation"}, )
    contact_no = models.CharField(max_length=150, blank=True,null=True)
    email_id = models.CharField(blank=True,null=True,max_length=355)
    address = models.TextField(blank=True,null=True,)
    place = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL, null=True, blank=True,
        related_name="Place_customers",limit_choices_to={"category": "Place"}, )
    city = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="City_customers",limit_choices_to={"category": "City"}, )
    state = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="State_customers",limit_choices_to={"category": "State"}, )
    zone = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="Zone_customers",limit_choices_to={"category": "Zone"}, )
    executive_name = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="Executive_Name",limit_choices_to={"category": "Executive Name"}, )
    source = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="Source",limit_choices_to={"category": "Source"}, )
    updated_at = models.DateField(auto_now=True,verbose_name="Updated date",null=True,)

    class Meta:
        db_table = "pc_customer_master"    
        verbose_name = "Personal Care Customer"
        verbose_name_plural = "Customers"





class PC_SampleRequest(models.Model):
    inquiry_date = models.DateField(verbose_name="Inquiry Date (dd-mm-yy)",null=True,blank=True,)
    sample_dispatch_date = models.DateField(verbose_name="Sample Dispatch Date (Project Start Date)",
        null=True, blank=True,  )
    year = models.IntegerField( null=True,blank=True, verbose_name="Year (from Sample Dispatch Date)", )
    project_close_date = models.DateField(verbose_name="Project Close Date (mm-yy)", null=True, blank=True,
        help_text="Store as any date within the closing month", )
    # Master-driven fields
    customer_name = models.ForeignKey( PersonalCareMaster, on_delete=models.SET_NULL, null=True,
        blank=True,  related_name="project_customer_name",   limit_choices_to={"category": "Customer Name"}, )
    product_name = models.ForeignKey( PersonalCareMaster, on_delete=models.SET_NULL, null=True,blank=True,
        related_name="project_product_name", limit_choices_to={"category": "Product Name"}, )
    project_name = models.ForeignKey( PersonalCareMaster,  on_delete=models.SET_NULL, null=True, blank=True,
        related_name="project_project_name", limit_choices_to={"category": "Project Name"},  )
    PROJECT_TYPE_CHOICES = [
        ("REPLACEMENT", "Replacement"),
        ("NPD", "NPD"),
        ("PROTOTYPE", "Prototype"),
    ]
    project_type = models.CharField(  max_length=20, choices=PROJECT_TYPE_CHOICES,   blank=True, null=True,verbose_name="Project Type",)
    supplier_name = models.ForeignKey( PersonalCareMaster, on_delete=models.SET_NULL,null=True,blank=True,
        related_name="project_supplier_name", limit_choices_to={"category": "Supplier Name"},)
    remarks_master = models.ForeignKey(PersonalCareMaster, on_delete=models.SET_NULL,null=True,
        blank=True, related_name="project_remarks", limit_choices_to={"category": "Remarks"},verbose_name="Remarks (master)",)
    stage = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True,blank=True,related_name="project_stage",
        limit_choices_to={"category": "Stage"}, )
    executive_name = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL, null=True,blank=True,
        related_name="project_executive_name", limit_choices_to={"category": "Executive Name"}, )
    # Free-text fields (contact info etc.)
    contact_person = models.CharField(max_length=255, blank=True, null= True)
    contact_no = models.CharField(max_length=50, blank=True, null= True)
    email = models.EmailField(blank=True, null=True)
    address = models.TextField(blank=True, null= True)
    # Sample quantity
    sample_quantity = models.IntegerField(null=True,blank=True,
        verbose_name="Sample Quantity (in gm/ml)", )
    # Price indication given (Yes/No etc. – keep as text or choices if you want)
    price_indication_given = models.CharField(max_length=50,blank=True,null=True,verbose_name="Price Indication Given", )
    # Follow-up & approval
    followup_date = models.DateField(null=True,blank=True,verbose_name="Last Follow-up Month",
                                     help_text="Only month & year are used; stored as 1st day of that month.",)
    APPROVAL_NMP_CHOICES = [
        ("Approved", "Approved"),
        ("Pending", "Pending"),
        ("Hold", "Hold"),
    ]
    approval_by_nmp = models.CharField(max_length=10, choices=APPROVAL_NMP_CHOICES, blank=True,null=True,default='Pending',
        verbose_name="Approval By NMP",)
    approved_quantity = models.IntegerField(null=True,blank=True,verbose_name="Approved Quantity",)
    created_at = models.DateTimeField(default=timezone.now, editable=False)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "pc_sample_request"
        verbose_name = "PC Sample Request"
        verbose_name_plural = "PC Sample Requests"
        permissions = [
            ("can_approve_sample_request", "can_approve_sample_request"), 
        ]
        
    def save(self, *args, **kwargs):
        """
        Auto-populate `year` from sample_dispatch_date.
        Works for forms and for import_export as well.
        """
        if self.sample_dispatch_date:
            self.year = self.sample_dispatch_date.year
        else:
            self.year = None
            
        # ✅ force default if not set (covers imports, forms, etc.)
        if not self.approval_by_nmp:
            self.approval_by_nmp = "Pending"    
        
        super().save(*args, **kwargs)
        
        
    def __str__(self):
        # fall back nicely if any piece is missing
        cust = self.customer_name.subcategory if self.customer_name else "Unknown customer"
        prod = self.product_name.subcategory if self.product_name else "Unknown product"
        return f"{cust} - {prod}"
    
    
    
    
    
class Customer_Followup(models.Model):
    MODE_OF_FOLLOWUP_CHOICES = (
        ("In-Person Visit", "In-Person Visit"),
        ("Email", "Email"),
        ("Phone Call", "Phone Call"),
        ("No Follow-up", "No Follow-up"),
    )
    customer_name = models.ForeignKey( PersonalCareMaster, on_delete=models.SET_NULL,
        null=True,  blank=True,related_name="customer_followups_customer",
        limit_choices_to={"category": "Customer Name"},verbose_name="Customer Name", )
    executive_name = models.ForeignKey(PersonalCareMaster,
        on_delete=models.SET_NULL, null=True,blank=True, related_name="customer_followups_executive",
        limit_choices_to={"category": "Executive Name"}, verbose_name="Executive Name", )
    customer_profile = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,
        null=True, blank=True, related_name="customer_profile",limit_choices_to={"category": "Sub Profile"},   # keep same as you asked
        verbose_name="Customer Profile", )
    mode_of_followup = models.CharField( max_length=100, verbose_name="Mode of Follow-up",
        choices=MODE_OF_FOLLOWUP_CHOICES, )
    date = models.DateField( verbose_name="Date", null=True, blank=True,)
    followup_status = models.ForeignKey( PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="followup_status", limit_choices_to={"category": "Followup Status"},verbose_name="Followup Status",)
    description = models.TextField( null=True,blank=True,verbose_name="Description", )
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey( User, on_delete=models.SET_NULL, null=True, blank=True,
        related_name="customer_followups_created", )

    class Meta:
        db_table = "customer_followup"
        verbose_name = "Customer Follow-up"
        verbose_name_plural = "Customer Follow-ups"

    def __str__(self):
        cust = (
            self.customer_name.subcategory
            if self.customer_name and self.customer_name.subcategory
            else "Customer"
        )
        return f"{cust} – {self.mode_of_followup} on {self.date or 'N/A'}"
    
    
    
    
class PC_Other_CustomerMaster(models.Model):
    created_at = models.DateField(null=True,blank=False,verbose_name="Created date",default=timezone.now,)
    customer_name = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL, null=True, blank=True,
        related_name="Other_customer_Name",limit_choices_to={"category": "Customer Name"},)
    # will use PersonalCareMaster rows where category = "Customer Profile"
    customer_profile = models.ForeignKey( PersonalCareMaster,on_delete=models.SET_NULL, null=True, blank=True,
        related_name="customer_profile_customers_other",limit_choices_to={"category": "Customer Profile"},)
    # will use rows where category = "Sub Profile"
    sub_profile = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL, null=True, blank=True,
        related_name="sub_profile_customers_other",limit_choices_to={"category": "Sub Profile"},)
    contact_person = models.CharField(max_length=255, blank=True, null=True)
    # will use rows where category = "Designation"
    designation = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="designation_customers_other",limit_choices_to={"category": "Designation"}, )
    contact_no = models.CharField(max_length=150, blank=True,null=True)
    email_id = models.CharField(blank=True,null=True,max_length=355)
    address = models.TextField(blank=True,null=True,)
    place = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL, null=True, blank=True,
        related_name="Place_customers_other",limit_choices_to={"category": "Place"}, )
    city = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="City_customers_other",limit_choices_to={"category": "City"}, )
    state = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="State_customers_other",limit_choices_to={"category": "State"}, )
    zone = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="Zone_customers_other",limit_choices_to={"category": "Zone"}, )
    executive_name = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="Executive_Name_other",limit_choices_to={"category": "Executive Name"}, )
    source = models.ForeignKey(PersonalCareMaster,on_delete=models.SET_NULL,null=True, blank=True,
        related_name="Source_other",limit_choices_to={"category": "Source"}, )
    core_business = models.TextField(blank=True,null=True,)
    updated_at = models.DateField(auto_now=True,verbose_name="Updated date",null=True,)

    class Meta:
        db_table = "pc_other_customer_master"    
        verbose_name = "Personal Care Other Customer"
        verbose_name_plural = "Others Customers"
