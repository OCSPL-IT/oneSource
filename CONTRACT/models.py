from django.db import models


CONTRACTOR_CHOICES = [
    ('Aldar', 'Aldar'),
    ('Yash', 'Yash'),
    ('ACP', 'ACP'),
    ('HE', 'HE'),
]


SHIFT_CHOICES = [
    ('A', 'A'),
    ('G', 'G'),
    ('B', 'B'),
    ('C', 'C'),
]


CONTRACT_NAME_CHOICES = [
    ("Aldar", "Aldar"),
    ("Yash", "Yash"),
    ("ACP", "ACP"),
    ("HE", "HE"),
]

LOCATION_CHOICES = [
    ('A Block', 'A Block'),
    ('B Block', 'B Block'),
    ('C Block', 'C Block'),
    ('D Block', 'D Block'),
    ('E Block- 17 Production', 'E Block- 17 Production'),
    ('PKG', 'PKG'),
    ('Pilot', 'Pilot'),
    ('MEE/ETP', 'MEE/ETP'),
    ('RO Plant', 'RO Plant'),
    ('MNTS', 'MNTS'),
    ('ELE', 'ELE'),
    ('INST', 'INST'),
    ('RM/ENGG 16 - 17 & 18', 'RM/ENGG 16 - 17 & 18'),
    ('QC & PD', 'QC & PD'),
    ('Boiler', 'Boiler'),
    ('Dozer Driver', 'Dozer Driver'),
    ('HouseKeeping 16 - 17 & 18', 'HouseKeeping 16 - 17 & 18'),
    ('Office Boy', 'Office Boy'),
    ('Gardenar', 'Gardenar'),
    ('OHC', 'OHC'),
    ('Painting', 'Painting'),
    ('MNTS-E 17', 'MNTS-E 17'),
    ('ELE- E-17', 'ELE- E-17'),
    ('INST-E-17', 'INST-E-17'),
]


# Static dropdown options for Department
DEPARTMENT_CHOICES = [
    ('ACCOUNTS', 'ACCOUNTS'),
    ('BOILER UTILITY', 'BOILER UTILITY'),
    ('ELECTRICAL', 'ELECTRICAL'),
    ('EHS', 'EHS'),
    ('HR ADMIN', 'HR ADMIN'),
    ('INSTRUMENT', 'INSTRUMENT'),
    ('IT', 'IT'),
    ('MAINTENANCE', 'MAINTENANCE'),
    ('OPERATION', 'OPERATION'),
    ('PRODUCTION', 'PRODUCTION'),
    ('QA/QC', 'QA/QC'),
    ('SECURITY', 'SECURITY'),
    ('STORE', 'STORE'),
    ('ETP', 'ETP'),
]

LOCATION_CHOICES = [
    ('A Block', 'A Block'),
    ('B Block', 'B Block'),
    ('C Block', 'C Block'),
]

class HRContract(models.Model):
    # match the existing columns
    employee = models.ForeignKey(
        'ContractEmployee',
        db_column='employee_id',
        to_field='id',
        on_delete=models.DO_NOTHING,
        related_name='hr_records'
    )
    work_date   = models.DateField(db_column='work_date')
    in_date     = models.DateField(db_column='in_date', null=True, blank=True)
    in_time     = models.TimeField(db_column='in_time', null=True, blank=True)
    out_date    = models.DateField(db_column='out_date', null=True, blank=True)
    out_time    = models.TimeField(db_column='out_time', null=True, blank=True)
    shift       = models.CharField(max_length=50, db_column='shift')
    work_hhmm   = models.CharField(max_length=10, db_column='work_hhmm', null=True, blank=True)
    ot_hours    = models.DecimalField(max_digits=5, decimal_places=2, db_column='ot_hours', null=True, blank=True)
    double_ot_hours = models.DecimalField(max_digits=5, decimal_places=2, db_column='double_ot_hours', null=True, blank=True)

    # — new column —
    block       = models.CharField(
        max_length=50,
        choices=LOCATION_CHOICES,
        null=True,
        blank=True
    )

    class Meta:
        db_table  = 'hr_contract'
        managed   = False   # we’re not letting Django CREATE this table

    def __str__(self):
        return f"{self.employee.name} on {self.work_date} ({self.shift})"


# Create your models here.
class ContractEmpDepartment(models.Model):
    name = models.CharField(max_length=100, unique=True)

    class Meta:
        db_table = 'contract_employee_dept'
    def __str__(self):
        return self.name

class ContractEmployee(models.Model):
    id = models.CharField(max_length=50, primary_key=True)  # Employee ID from device/excel
    name = models.CharField(max_length=100)
    employee_type = models.CharField(max_length=50)
    department = models.ForeignKey(ContractEmpDepartment, on_delete=models.CASCADE, related_name='employees')
    
    class Meta:
        db_table = 'contract_employee'
    def __str__(self):
        return self.name


class ContractorName(models.Model):
    name = models.CharField(max_length=100, unique=True)

    class Meta:
        db_table = 'contract_name'
    def __str__(self):
        return self.name


# --- Choices for the new department field ---
DEPARTMENT_CHOICES = [
    ('ACCOUNTS', 'ACCOUNTS'),('BOILER', 'BOILER'),('UTILITY', 'UTILITY'),('ELECTRICAL', 'ELECTRICAL'),
    ('EHS', 'EHS'),('HR ADMIN', 'HR ADMIN'),('INSTRUMENT', 'INSTRUMENT'),('IT', 'IT'),
    ('MAINTENANCE', 'MAINTENANCE'),('OPERATION', 'OPERATION'),('PRODUCTION', 'PRODUCTION'),('QA/QC', 'QA/QC'),
    ('SECURITY', 'SECURITY'),('STORE', 'STORE'),('ETP', 'ETP'),
]

BLOCK_LOCATIONS = [
    ("A Block", "A Block"), ("B Block", "B Block"), ("C Block", "C Block"),
    ("D Block", "D Block"), ("E Block", "E Block"),("PKG", "PKG"),("Pilot", "Pilot"),
    ("E Block- 17 Production", "E Block- 17 Production"), ("MEE/ETP", "MEE/ETP"), 
    ("MNTS", "MNTS"),("PKG", "PKG"), ("QC & PD", "QC & PD"),("RO Plant", "RO Plant"),
    ("ELE", "ELE"),("INST", "INST"),("RM/ENGG 16 - 17 & 18", "RM/ENGG 16 - 17 & 18"),("Boiler", "Boiler"),
    ("Dozer Driver", "Dozer Driver"), ("HouseKeeping 16 - 17 & 18", "HouseKeeping 16 - 17 & 18"),("Office Boy","Office Boy"),
    ("Gardenar","Gardenar"),("OHC","OHC"),("Painting", "Painting"), ("MNTS-E 17","MNTS-E 17"),("ELE-E 17","ELE-E 17"),
    ("INST-E 17","INST-E 17"),("Gate-E-16","Gate-E-16"),("Gate-E-17","Gate-E-17"),("Gate-E-18","Gate-E-18")
    ,("Gate-E-20","Gate-E-20"),("Gate-E-22","Gate-E-22"),("Driver", "Driver"),  
]
SHIFT_CHOICES = [
    ("1st Shift (07:00AM-15:00PM)", "1st Shift (07:00AM-15:00PM)"),
    ("General (09:00AM-18:00PM)", "General (09:00AM-18:00PM)"),
    ("2nd Shift (15:00PM-23:00PM)", "2nd Shift (15:00PM-23:00PM)"),
    ("4th Shift (19:00PM-07:00AM)", "4th Shift (19:00PM-07:00AM)"),
    ("Night (23:00PM-07:00AM)", "Night (23:00PM-07:00AM)"),
    ("3rd Shift (07:00AM-19:00PM)", "3rd Shift (07:00AM-19:00PM)"),
]

class EmployeeAssignment(models.Model):
    punch_date = models.DateField()
    employee = models.ForeignKey(ContractEmployee, on_delete=models.CASCADE, related_name='assignments')
    contractor = models.ForeignKey(ContractorName, on_delete=models.SET_NULL, null=True, blank=True, verbose_name="Contractor Name")
    department = models.CharField(max_length=50, choices=DEPARTMENT_CHOICES, null=True, blank=True)
    block_location = models.CharField(max_length=100, choices=BLOCK_LOCATIONS)
    shift = models.CharField(max_length=100, choices=SHIFT_CHOICES, null=True, blank=True)
    punch_in = models.TimeField(null=True, blank=True)
    punch_out = models.TimeField(null=True, blank=True)
    assigned_date = models.DateTimeField(auto_now_add=True, editable=False)
    is_reassigned = models.BooleanField(default=False)

    class Meta:
        db_table = 'contract_employee_assignment'    
    
    def __str__(self):
        return f"{self.employee.name} - {self.punch_date}"
