from django.db import models

# Contractor wages
class ContractorWages(models.Model):
    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50,blank=True, null=True)
    contractor_name = models.CharField(
        max_length=100,
        choices=[
            ('Pratap Enterprises', 'Pratap Enterprises'),
            ('Yogesh Waghmode', 'Yogesh Waghmode'),
            ('Yash Enterprises', 'Yash Enterprises'),
            ('Shital Shinde Services', 'Shital Shinde Services'),
            ('Abhi Consultancy', 'Abhi Consultancy')
        ]
    )
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'contractor_wages'

    def save(self, *args, **kwargs):
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)


# Security wages
class SecurityWages(models.Model):
    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    contractor_name = models.CharField(
        max_length=100,
        choices=[
            ('Badalapur Enterprises', 'Badalapur Enterprises'),
            ('Other', 'Other'),
        ]
    )
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)  # <-- NEW FIELD
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'security_wages'

    def save(self, *args, **kwargs):
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.invoice_no} - {self.contractor_name}"

# Welfare
class HrBudgetWelfare(models.Model):
    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    welfare_name = models.CharField(
        max_length=100,
        choices=[
            ('Uniform', 'Uniform'),
            ('Employee Engagement', 'Employee Engagement'),
            ('Open house meeting', 'Open house meeting'),
            ('DiwaliSweets', 'DiwaliSweets'),
            ('Other', 'Other')
        ]
    )
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)  # <-- NEW FIELD
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'hrbudget_welfare'

    def __str__(self):
        return f"{self.invoice_no} - {self.welfare_name}"

    def save(self, *args, **kwargs):
        # Auto-calculate total_bill_amount
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)


# Canteen
class HrBudgetCanteen(models.Model):
    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    name = models.CharField(
        max_length=100,
        choices=[
            ('Balaji Tea Corner', 'Balaji Tea Corner'),
            ('Dayanand Mhetre', 'Dayanand Mhetre'),
            ('Ambai Pure Wage', 'Ambai Pure Wage'),
            ('Tea & Biscuit', 'Tea & Biscuit'),
            ('Other', 'Other'),
        ]
    )
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)  # <-- New field
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'hrbudget_canteen'

    def __str__(self):
        return f"{self.invoice_no} - {self.name}"

    def save(self, *args, **kwargs):
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)

# Medical
class HrBudgetMedical(models.Model):
    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    doctor_hospital_name = models.CharField(
        max_length=150,
        choices=[
            ('Dr. Ganesh Atwade(FMO)', 'Dr. Ganesh Atwade(FMO)'),
            ('Baladava Hospital', 'Baladava Hospital'),
            ('Dr. Chidgupkar Hospital', 'Dr. Chidgupkar Hospital'),
            ('Shilpakaya Hospital', 'Shilpakaya Hospital'),
            ('Doctor Ukarande', 'Doctor Ukarande'),
            ('Medicines for OHC', 'Medicines for OHC'),
            ('New joinee health check up', 'New joinee health check up'),
            ('Annual Health Check up', 'Annual Health Check up'),
            ('Other', 'Other'),
        ]
    )
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'hrbudget_medical'

    def save(self, *args, **kwargs):
        # Auto-calculate total_bill_amount
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.invoice_no} - {self.doctor_hospital_name}"



class HrBudgetVehicle(models.Model):
    CATEGORY_CHOICES = [
        ('Daily Pickup-drop', 'Daily Pickup-drop'),
        ('Fuel expense', 'Fuel expense'),
        ('Insurance', 'Insurance'),
        ('Repair & Maintenance', 'Repair & Maintenance'),
        ('Material / Goods', 'Material / Goods'),
        ('Other', 'Other'),
    ]

    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    vehicle_name = models.CharField(max_length=100, blank=True, null=True)
    vehicle_number = models.CharField(max_length=50, blank=True, null=True)
    category = models.CharField(max_length=50, choices=CATEGORY_CHOICES, blank=True, null=True)
    # ? New Fields
    liter = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    kilometer = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'hrbudget_vehicle'

    def save(self, *args, **kwargs):
        # Auto-calculate total_bill_amount
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)



class HrBudgetTravellingLodging(models.Model):
    TRAVELLING_CHOICES = [
        ('Vaishnavi/Raghvendra/Darshan Tours & Travels', 'Vaishnavi/Raghvendra/Darshan Tours & Travels'),
        ('Balaji Sarovar Premire', 'Balaji Sarovar Premire'),
        ('Hotel Lotus', 'Hotel Lotus'),
        ('Hotel Kyriad', 'Hotel Kyriad'),
        ('Hotel Sai Prasad Executive', 'Hotel Sai Prasad Executive'),
        ('Other', 'Other'),
    ]

    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    name = models.CharField(max_length=100, choices=TRAVELLING_CHOICES, blank=True, null=True)
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)  # <-- Added
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'hrbudget_travelling_lodging'

    def save(self, *args, **kwargs):
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.invoice_no} - {self.name}"

    
class HRBudgetGuestHouse(models.Model):
    NAME_CHOICES = [
        ('Manmeet', 'Manmeet'),
        ('Haripadam', 'Haripadam'),
        ('New guest house', 'New guest house'),
    ]
    CATEGORY_CHOICES = [
        ('Rent', 'Rent'),
        ('Maintenance', 'Maintenance'),
        ('Grocery/Vegetables/Milk/Curd/Laundary', 'Grocery/Vegetables/Milk/Curd/Laundary'),
        ('A/c Servicing', 'A/c Servicing'),
        ('Cleaning Boy', 'Cleaning Boy'),
        ('DISH Recharge', 'DISH Recharge'),
        ('House Keeping', 'House Keeping'),
        ('Light bill', 'Light bill'),
        ('Guest House Cook', 'Guest House Cook'),
        ('Other', 'Other'),
    ]

    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    name = models.CharField(max_length=50, choices=NAME_CHOICES)
    category = models.CharField(max_length=100, choices=CATEGORY_CHOICES)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)  # Added
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'hrbudget_guesthouse'

    def save(self, *args, **kwargs):
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.invoice_no} - {self.name}"


class HRBudgetGeneralAdmin(models.Model):
    CATEGORY_CHOICES = [
        ('Printing', 'Printing'),
        ('Stationary', 'Stationary'),
        ('Cleaning Material', 'Cleaning Material'),
        ('Harshada Courier Services', 'Harshada Courier Services'),
        ('Courier (Sample to Vasai)', 'Courier (Sample to Vasai)'),
        ('Pest Control', 'Pest Control'),
        ('Sunshine Services Xerox Conon E-16 Admin', 'Sunshine Services Xerox Conon E-16 Admin'),
        ('Sunshine Services Xerox Conon QC Lab', 'Sunshine Services Xerox Conon QC Lab'),
        ('Sunshine Services Xerox Conon E-18 Admin', 'Sunshine Services Xerox Conon E-18 Admin'),
        ('Sunshine Services Xerox Conon E-17 Admin', 'Sunshine Services Xerox Conon E-17 Admin'),
        ('Fridge repairing', 'Fridge repairing'),
        ('Water bottle', 'Water bottle'),
        ('Aquaguard/RO', 'Aquaguard/RO'),
        ('Balanand Kirana / Shri Datta Kirana (Jagerry)', 'Balanand Kirana / Shri Datta Kirana (Jagerry)'),
        ('Pantry', 'Pantry'),
        ('Conveyance', 'Conveyance'),
        ('Post Office', 'Post Office'),
        ('Night round police', 'Night round police'),
        ('Air Conditioner - Services', 'Air Conditioner - Services'),
        ('Furniture', 'Furniture'),
        ('Mahi Jal', 'Mahi Jal'),
        ('Other', 'Other'),
    ]

    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    category = models.CharField(max_length=100, choices=CATEGORY_CHOICES, blank=True, null=True)
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'hrbudget_general_admin'

    def save(self, *args, **kwargs):
        # Auto-calculate total_bill_amount
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.invoice_no} - {self.category}"

    

class HRBudgetCommunication(models.Model):
    NAME_CHOICES = [
        ('Internet Tata', 'Internet Tata'),
        ('NAS Net', 'NAS Net'),
        ('JIO - Sim', 'JIO - Sim'),
        ('VI - Sim', 'VI - Sim'),
        ('New Mobile', 'New Mobile'),
        ('EPABX - Intercome (Jio PRI)', 'EPABX - Intercome (Jio PRI)'),
        ('IT material - CCTV', 'IT material - CCTV'),
        ('Hard Disck', 'Hard Disck'),
        ('Tonner', 'Tonner'),
        ('Other', 'Other'),
    ]

    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    invoice_name = models.CharField(max_length=100, choices=NAME_CHOICES)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'hrbudget_communication'

    def __str__(self):
        return f"{self.invoice_no} - {self.invoice_name}"

    def save(self, *args, **kwargs):
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)
    


class InsuranceMediclaim(models.Model):
    CATEGORY_CHOICES = [
        ('Insurance Mediclaim GMP', 'Insurance Mediclaim GMP'),
        ('GPAP', 'GPAP'),
        ('WC Policy', 'WC Policy'),
        ('Other', 'Other'),
    ]

    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    category = models.CharField(max_length=50, choices=CATEGORY_CHOICES, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)  # <-- NEW FIELD
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'insurance_mediclaim'

    def save(self, *args, **kwargs):
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.invoice_no} - {self.category or 'No Category'}"


class HRBudgetAMC(models.Model):
    AMC_CHOICES = [
        ('UNITY  SERVICE', 'UNITY  SERVICE'),
        ('BADAMIKAR & SON', 'BADAMIKAR & SON'),
        ('Other', 'Other'),
    ]
    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    amc_name = models.CharField(max_length=50, choices=AMC_CHOICES, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)  # <-- NEW FIELD
    description = models.TextField(blank=True, null=True)
    
    class Meta:
        db_table = 'hrbudget_amc'

    def save(self, *args, **kwargs):
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.invoice_no} - {self.amc_name or 'No Name'}"

class HRBudgetTraining(models.Model):
    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    name = models.CharField(max_length=50, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)  # NEW FIELD
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'hrbudget_training'

    def save(self, *args, **kwargs):
        # Always recalculate total
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)


class HRBudgetLegal(models.Model):
    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    name = models.CharField(max_length=50, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)  # NEW FIELD
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'hrbudget_legal'

    def save(self, *args, **kwargs):
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.invoice_no or ''} - {self.name or ''}"


class AdminRepairAndMaintenance(models.Model):
    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    name = models.CharField(max_length=50, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    description = models.TextField(blank=True, null=True)
    
    class Meta:
        db_table = 'admin_repair_and_maintenance'
        verbose_name = 'Admin Repair and Maintenance'
        verbose_name_plural = 'Admin Repair and Maintenance'

    def save(self, *args, **kwargs):
        # Always update total_bill_amount as bill + gst (handling None)
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.invoice_no or ''} - {self.name or ''}"
    


class AdminCapex(models.Model):
    invoice_date = models.DateField()
    invoice_no = models.CharField(max_length=50, blank=True, null=True)
    name = models.CharField(max_length=50, blank=True, null=True)
    gst = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    total_bill_amount = models.DecimalField(max_digits=12, decimal_places=2, blank=True, null=True)
    description = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'admin_capex'
        verbose_name = 'Admin Capex'
        verbose_name_plural = 'Admin Capex'

    def save(self, *args, **kwargs):
        self.total_bill_amount = (self.bill_amount or 0) + (self.gst or 0)
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.invoice_no or ''} - {self.name or ''}"



HR_BUDGET_CATEGORY_CHOICES = [
    ('Contractor Wages', 'Contractor Wages'),
    ('Security Wages', 'Security Wages'),
    ('Welfare', 'Welfare'),
    ('Canteen', 'Canteen'),
    ('Medical Expenses', 'Medical Expenses'),
    ('Vehicle Expenses', 'Vehicle Expenses'),
    ('Travelling & Lodging', 'Travelling & Lodging'),
    ('Guest House Expenses', 'Guest House Expenses'),
    ('General Admin Expenses', 'General Admin Expenses'),
    ('Communication Expenses', 'Communication Expenses'),
    ('Insurance Mediclaim', 'Insurance Mediclaim'),
    ('AMC', 'AMC'),
    ('Training', 'Training'),
    ('Legal', 'Legal'),
    ('Admin Repair & Maintenance', 'Admin Repair & Maintenance'),
    ('Admin Capex', 'Admin Capex'),
    ('Other', 'Other'),
]

class HRBudgetPlan(models.Model):
    year = models.CharField(max_length=9, help_text="Eg. 2025-26")
    category = models.CharField(max_length=50, choices=HR_BUDGET_CATEGORY_CHOICES)
    plan_amount = models.DecimalField(max_digits=12, decimal_places=2, help_text="Enter amount..")

    class Meta:
        unique_together = ('year', 'category')
        db_table = 'hrbudget_plan'

    def __str__(self):
        return f"{self.year} {self.category}: {self.plan_amount_lacs} Lacs"





