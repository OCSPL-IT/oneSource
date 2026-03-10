from django.db import models
from django.db import models
from django.conf import settings
from django.contrib.auth import get_user_model
User = get_user_model()



class HR(models.Model):
    date = models.DateField()
    permanent_employees = models.IntegerField()
    contract_labour_production = models.IntegerField()
    contract_labour_others = models.IntegerField()
    total_employee = models.IntegerField()
    hrs = models.IntegerField(default=0)
    total_no_of_hrs = models.IntegerField(default=0)
    
    class Meta:
        db_table = 'HR'

        
    def __str__(self):
        return f"HR Record on {self.date}"


class DailyAttendance(models.Model):
    employee_code = models.CharField(max_length=10)
    full_name = models.CharField(max_length=100)
    employment_status = models.CharField(max_length=50)
    company = models.CharField(max_length=100)
    business_unit = models.CharField(max_length=100, null=True, blank=True)
    department = models.CharField(max_length=50)
    sub_department = models.CharField(max_length=200, null=True, blank=True)
    designation = models.CharField(max_length=100, null=True, blank=True)
    branch = models.CharField(max_length=50)
    sub_branch = models.CharField(max_length=50)
    attendance_date = models.DateField()
    punch_in_punch_out_time = models.CharField(max_length=20,null=True)
    status_in_out = models.CharField(max_length=20)
    shift_code = models.CharField(max_length=50)
    shift_timing = models.CharField(max_length=20)
    Late_or_early = models.CharField(max_length=50,null=True)
    working_hours = models.CharField(max_length=10,null=True)
    total_office_hours = models.CharField(max_length=10, null=True, blank=True)
    source = models.CharField(max_length=100, null=True, blank=True)
    date_of_joining = models.DateField(null=True, blank=True)
    employment_type = models.CharField(max_length=50, null=True, blank=True)
    grade = models.CharField(max_length=20, null=True, blank=True)
    lattitude_longitude = models.CharField(max_length=200, null=True, blank=True)
    level = models.CharField(max_length=50, null=True, blank=True)
    location = models.CharField(max_length=500, null=True, blank=True)
    mobile = models.CharField(max_length=10, null=True, blank=True)
    region = models.CharField(max_length=20, null=True, blank=True)
    reporting_manager = models.CharField(max_length=100, null=True, blank=True)
    work_email = models.EmailField(null=True, blank=True)

    class Meta:
        db_table = 'daily_attendance'

    # def __str__(self):
        # return f"{self.full_name} ({self.attendance_date})"


class AttendanceRegulation(models.Model):
    employee_code = models.CharField(max_length=10)
    full_name = models.CharField(max_length=100)
    employment_status = models.CharField(max_length=50)
    company = models.CharField(max_length=100)
    business_unit = models.CharField(max_length=100)
    department = models.CharField(max_length=100)
    designation = models.CharField(max_length=100)
    branch = models.CharField(max_length=50)
    sub_branch = models.CharField(max_length=50)
    request_type = models.CharField(max_length=50)
    attendance_date = models.DateField()
    attendance_day = models.CharField(max_length=20)
    reason = models.CharField(max_length=200)
    shift_code = models.CharField(max_length=50)
    shift_timings = models.CharField(max_length=20)
    actual_punch_in_out = models.CharField(max_length=100, null=True, blank=True)
    punch_in_date = models.DateField(null=True, blank=True)
    punch_in_time = models.CharField(max_length=10, null=True, blank=True)
    punch_out_date = models.DateField(null=True, blank=True)
    punch_out_time = models.CharField(max_length=10, null=True, blank=True)
    remarks = models.TextField(null=True, blank=True)
    request_status = models.CharField(max_length=20)
    requested_by = models.CharField(max_length=100)
    requested_on = models.DateField()
    approved_by = models.CharField(max_length=100, null=True, blank=True)
    approved_on = models.DateField(null=True, blank=True)
    approver_remark = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'attendance_regulation'

    def __str__(self):
        return f"{self.full_name} - {self.attendance_date} ({self.request_status})"


class DailyCheckIn(models.Model):
    employee_code = models.CharField(max_length=10)
    full_name = models.CharField(max_length=100)
    employment_status = models.CharField(max_length=50)
    company = models.CharField(max_length=100)
    business_unit = models.CharField(max_length=100)
    department = models.CharField(max_length=100)
    designation = models.CharField(max_length=100)
    branch = models.CharField(max_length=100)
    sub_branch = models.CharField(max_length=100)
    attendance_date = models.DateField()
    shift = models.CharField(max_length=100, null=True, blank=True)
    check_in = models.CharField(max_length=10, null=True, blank=True)
    source = models.CharField(max_length=150, null=True, blank=True)
    first_punch = models.TimeField(max_length=10, null=True, blank=True)
    last_punch = models.TimeField(max_length=10, null=True, blank=True)
    raw_punch = models.CharField(max_length=255,null=True,blank=True)  # Assuming multiple punch times could be stored as a comma-separated string

    class Meta:
        db_table = 'daily_check_in'

    def __str__(self):
        return f"{self.full_name} ({self.employee_code})"



class Late_Early_Go(models.Model):
    employee_code = models.CharField(max_length=10)
    full_name = models.CharField(max_length=100)
    employment_status = models.CharField(max_length=50)
    company = models.CharField(max_length=100)
    business_unit = models.CharField(max_length=100)
    department = models.CharField(max_length=100)
    designation = models.CharField(max_length=100)
    branch = models.CharField(max_length=50)
    sub_branch = models.CharField(max_length=50)
    late_early = models.CharField(max_length=50)
    attendance_date = models.DateField()
    late_early_by_min = models.IntegerField()
    shift_code = models.CharField(max_length=50)
    shift_timings = models.CharField(max_length=50)

    class Meta:
        db_table = 'late_early_go'

    def __str__(self):
        return f"{self.full_name} ({self.employee_code})"
    

class On_Duty_Request(models.Model):
    employee_code = models.CharField(max_length=20)
    full_name = models.CharField(max_length=100)
    employment_status = models.CharField(max_length=50)
    company = models.CharField(max_length=100)
    business_unit = models.CharField(max_length=100)
    department = models.CharField(max_length=100)
    designation = models.CharField(max_length=100)
    branch = models.CharField(max_length=50)
    sub_branch = models.CharField(max_length=50)
    request_type = models.CharField(max_length=50)
    attendance_date = models.DateField()
    attendance_day = models.CharField(max_length=15)
    on_duty_type = models.CharField(max_length=50, blank=True, null=True)
    shift_code = models.CharField(max_length=50)
    shift_timings = models.CharField(max_length=50)
    actual_punch_in_out = models.CharField(max_length=50, blank=True, null=True)
    punch_in_date = models.DateField(blank=True, null=True)
    punch_out_date = models.DateField(blank=True, null=True)
    remarks = models.TextField(blank=True, null=True)
    request_status = models.CharField(max_length=50)
    request_by = models.CharField(max_length=100)
    request_on = models.DateTimeField()
    pending_with = models.CharField(max_length=100, blank=True, null=True)
    approved_by = models.CharField(max_length=100, blank=True, null=True)
    approved_on = models.DateTimeField(blank=True, null=True)
    approver_remark = models.TextField(blank=True, null=True)
    billable_type = models.CharField(max_length=50, blank=True, null=True)
    punch_in_timing = models.CharField(max_length=20, blank=True, null=True)
    punch_out_timing = models.CharField(max_length=20, blank=True, null=True)

    class Meta:
        db_table = 'on_duty_request'


    def __str__(self):
        return f"{self.employee_code} - {self.attendance_date}"




class OvertimeReport(models.Model):
    employee_code = models.CharField(max_length=20)
    full_name = models.CharField(max_length=100)
    employment_status = models.CharField(max_length=50)
    company = models.CharField(max_length=100)
    business_unit = models.CharField(max_length=100)
    department = models.CharField(max_length=100)
    designation = models.CharField(max_length=100)
    branch = models.CharField(max_length=50)
    sub_branch = models.CharField(max_length=50)
    attendance_date = models.DateField()
    attendance_day = models.CharField(max_length=15)
    shift_code = models.CharField(max_length=50)
    shift = models.CharField(max_length=100)
    shift_timings = models.CharField(max_length=50)
    punch_in_time = models.CharField(max_length=10)
    punch_out_time = models.CharField(max_length=10)
    working_hours = models.CharField(max_length=10)
    overtime_hours = models.CharField(max_length=10)
    request_status = models.CharField(max_length=50)
    request_by = models.CharField(max_length=100)
    request_on = models.DateTimeField(blank=True, null=True)
    pending_with = models.CharField(max_length=200, blank=True, null=True)
    approved_by = models.CharField(max_length=100, blank=True, null=True)
    approved_on = models.DateTimeField(blank=True, null=True)
    approver_remark = models.TextField(blank=True, null=True)
    overtime_minutes = models.IntegerField()

    class Meta:
        db_table = 'overtime_report'

    def __str__(self):
        return f"{self.employee_code} - {self.full_name}"



class ShortLeave(models.Model):
    employee_code = models.CharField(max_length=20)
    full_name = models.CharField(max_length=100)
    employment_status = models.CharField(max_length=50)
    company = models.CharField(max_length=100)
    business_unit = models.CharField(max_length=100)
    department = models.CharField(max_length=100)
    designation = models.CharField(max_length=100)
    branch = models.CharField(max_length=50)
    sub_branch = models.CharField(max_length=50)
    request_type = models.CharField(max_length=50)
    attendance_date = models.DateField()
    attendance_day = models.CharField(max_length=15)
    shift_code = models.CharField(max_length=50)
    shift_timings = models.CharField(max_length=50)
    actual_punch_in_out = models.CharField(max_length=50, blank=True, null=True)
    punch_in_date = models.DateField()
    punch_in_timing = models.CharField(max_length=10)
    punch_out_date = models.DateField()
    punch_out_timing = models.CharField(max_length=10)
    remarks = models.TextField(blank=True, null=True)
    request_status = models.CharField(max_length=50)
    request_by = models.CharField(max_length=100)
    request_on = models.DateField()
    pending_with = models.CharField(max_length=500, blank=True, null=True)
    approved_by = models.CharField(max_length=100, blank=True, null=True)
    approved_on = models.DateField(blank=True, null=True)
    approver_remark = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'short_leave'

    def __str__(self):
        return f"{self.employee_code} - {self.full_name}"




class Helpdesk_Ticket(models.Model):
    employee_code = models.CharField(max_length=20)
    full_name = models.CharField(max_length=100)
    employment_status = models.CharField(max_length=50)
    company = models.CharField(max_length=100)
    business_unit = models.CharField(max_length=100)
    department = models.CharField(max_length=100)
    designation = models.CharField(max_length=100)
    branch = models.CharField(max_length=50)
    sub_branch = models.CharField(max_length=50)
    ticket_id = models.CharField(max_length=100)
    ticket_details = models.TextField()
    category = models.CharField(max_length=100)
    sub_category = models.CharField(max_length=100)
    priority = models.CharField(max_length=20)
    status = models.CharField(max_length=20)
    raised_on = models.DateTimeField()
    assigned_to = models.CharField(max_length=100)
    pending_with = models.CharField(max_length=100, blank=True, null=True)
    closed_by = models.CharField(max_length=100, blank=True, null=True)
    closed_on = models.DateTimeField(blank=True, null=True)
    is_closed_on_time = models.CharField(max_length=10,blank=True, null=True)
    feedback_rating = models.CharField(max_length=50,blank=True, null=True)
    was_ticket_escalated = models.CharField(max_length=10)
    escalated_to = models.CharField(max_length=100, blank=True, null=True)
    rca = models.TextField(blank=True, null=True)
    time_to_close = models.CharField(max_length=100,blank=True, null=True)

    class Meta:
        db_table = 'helpdesk_ticket'

    def __str__(self):
        return f"{self.ticket_id} - {self.full_name}"








### EMployeee Joining Form Model


class EmployeeJoining(models.Model):
    GENDER_CHOICES = [("Male", "Male"), ("Female", "Female"), ("Other", "Other"),   ]
    APPLICABLE_CHOICES = [ ("Applicable", "Applicable"),("Not Applicable", "Not Applicable"),  ]
    SIM_CARD_CHOICES = [("Existing Card", "Existing Card"),("New Card", "New Card"),("Not Applicable", "Not Applicable"),]
    COMPUTER_CHOICES = [ ("Desktop", "Desktop"),("Laptop", "Laptop"),("Not Applicable", "Not Applicable"), ]
    COMPANY_CHOICES = [("OC Specialities Chemicals Pvt. Ltd", "OC Specialities Chemicals Pvt. Ltd"),
        ("OC Specialities Pvt.Ltd", "OC Specialities Pvt.Ltd"), ]
    LOCATION_CHOICES = [("Mumbai", "Mumbai"), ("Solapur", "Solapur"), ("Vasai", "Vasai"),("Virar", "Virar"), ]
    DEPARTMENT_CHOICES = [("Accounts", "Accounts"), ("ADL", "ADL"), ("Admin", "Admin"),("Boiler", "Boiler"),
        ("Business Development", "Business Development"),("Business Development & Strategy (Personal Care)", "Business Development & Strategy (Personal Care)"),
        ("Business Development &Strategy (Agrochemicals)", "Business Development &Strategy (Agrochemicals)"),
        ("EHS", "EHS"), ("Electrical", "Electrical"),("Engineering", "Engineering"), ("Environment", "Environment"),
        ("Finance", "Finance"), ("HR", "HR"), ("Import Export", "Import Export"),  ("Instrument", "Instrument"),
        ("IT", "IT"), ("Logistic", "Logistic"),  ("Maintenance", "Maintenance"), ("Management", "Management"),
        ("Marketing", "Marketing"), ("MD's Office", "MD's Office"),("Operation", "Operation"),("PD Lab", "PD Lab"),
        ("Personal Care", "Personal Care"),("Production", "Production"),("Project", "Project"),  ("Project & Maintenance", "Project & Maintenance"),
        ("Purchase", "Purchase"), ("QC & QA", "QC & QA"), ("Quality Assurance", "Quality Assurance"),
        ("Quality Control", "Quality Control"), ("R & D", "R & D"),("Sales", "Sales"),("Sales & Logistics", "Sales & Logistics"),
        ("SCM", "SCM"),("Store", "Store"), ("Technical Services", "Technical Services"),("Utility", "Utility"), ]
    # Basic details
    employee_name        = models.CharField(max_length=100, verbose_name="Employee Name")
    gender               = models.CharField(max_length=10, choices=GENDER_CHOICES, blank=True, null=True)
    date_of_birth        = models.DateField(blank=True, null=True, verbose_name="Date of Birth")
    company              = models.CharField( max_length=150,verbose_name="Company", choices=COMPANY_CHOICES, )
    location             = models.CharField( max_length=100, verbose_name="Location",choices=LOCATION_CHOICES, )
    department           = models.CharField(max_length=100,blank=True, null=True,choices=DEPARTMENT_CHOICES, )
    designation          = models.CharField(max_length=100, blank=True, null=True)
    date_of_joining      = models.DateField(verbose_name="Date of Joining")
    date_of_confirmation = models.DateField(blank=True, null=True, verbose_name="Date of Confirmation")
    employee_id          = models.CharField(max_length=20, unique=True, verbose_name="Employee ID")
    # Access / infra
    Biomatric_enrollment = models.CharField(max_length=20,choices=APPLICABLE_CHOICES,blank=True,null=True,
        verbose_name="BioMetric Enrollment", )
    attendance_card = models.CharField( max_length=20,choices=APPLICABLE_CHOICES, blank=True,
        null=True, verbose_name="Attendance Card", )
    smart_office_entry = models.CharField(max_length=20,choices=APPLICABLE_CHOICES, blank=True, null=True,
        verbose_name="Smart Office Entry",  )
    mobile_phone = models.CharField(max_length=50, choices=APPLICABLE_CHOICES,blank=True, null=True,
        verbose_name="Mobile Phone", )
    sim_card = models.CharField(max_length=20, choices=SIM_CARD_CHOICES, blank=True, null=True, verbose_name="SIM Card", )
    telephone_extension = models.CharField( max_length=20,choices=APPLICABLE_CHOICES,blank=True,null=True,
        verbose_name="Telephone Extension", )
    computer = models.CharField( max_length=20, choices=COMPUTER_CHOICES,  blank=True, null=True, verbose_name="Computer", )
    erp_login = models.CharField( max_length=20,choices=APPLICABLE_CHOICES,blank=True, null=True,verbose_name="ERP Login",)
    office_365_id = models.CharField(max_length=20,choices=APPLICABLE_CHOICES,blank=True,null=True,verbose_name="Office 365 ID",)
    email_id = models.EmailField(blank=True, null=True, verbose_name="Email ID")
    sharepoint_site = models.CharField(max_length=255, blank=True, null=True, verbose_name="SharePoint Site")
    specific_folder_rights = models.TextField(blank=True, null=True, verbose_name="Specific Folder Rights")
    remark = models.TextField(blank=True, null=True, verbose_name="Remark")
    # ---- Workflow status ----
    STATUS_CHOICES = [
        ("hr_submitted",     "HR Submitted (Pending IT)"),
        ("pending_approval", "Pending Approval"),
        ("approved",         "Approved"),
        ("rejected",         "Rejected"),
    ]
    status = models.CharField(max_length=30, choices=STATUS_CHOICES, default="hr_submitted",verbose_name="Status",)
    hr_user = models.ForeignKey( User,null=True,blank=True,on_delete=models.SET_NULL,related_name="joining_hr_created",
        verbose_name="Joining HR User",  )
    it_user = models.ForeignKey( User, null=True,  blank=True,  on_delete=models.SET_NULL, related_name="joining_it_processed",
        verbose_name="IT Infra User", )
    it_approver = models.ForeignKey( User,null=True,   blank=True, on_delete=models.SET_NULL,
        related_name="joining_approved",   verbose_name="IT Approver",   )
    hr_submitted_at = models.DateTimeField(null=True, blank=True)
    it_completed_at = models.DateTimeField(null=True, blank=True)
    approved_at     = models.DateTimeField(null=True, blank=True)
    approval_remark = models.TextField(blank=True, null=True)
    # Optional audit fields
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "employee_joining"
        verbose_name = "Employee Joining"
        verbose_name_plural = "Employee Joining Records"

    def __str__(self):
        return f"{self.employee_id} - {self.employee_name}"