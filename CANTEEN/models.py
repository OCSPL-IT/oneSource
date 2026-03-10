from django.db import models
from django.utils import timezone
import pytz
from datetime import time as dtime

# Replace with your actual timezone
LOCAL_TIMEZONE = pytz.timezone('Asia/Kolkata')  # or any relevant timezone


class Department(models.Model):
    name = models.CharField(max_length=100)

    class Meta:
        db_table = 'canteen_employee_dept'

    def __str__(self):
        return self.name


EMPLOYEE_TYPE_CHOICES = (
    ('Company', 'Company'),
    ('Trainee', 'Trainee'),
    ('Guest',   'Guest'),
    ('Casual',  'Casual'),
)

class Employee(models.Model):
    id = models.CharField(primary_key=True,max_length=50)
    name = models.CharField(max_length=100)
    employee_type = models.CharField(max_length=20,choices=EMPLOYEE_TYPE_CHOICES,null=True, blank=True,default='company')
    department = models.ForeignKey(Department, on_delete=models.CASCADE, related_name='employees')
    location = models.CharField(max_length=100, blank=True, null=True) 

    class Meta:
        db_table = 'canteen_employee'

    def __str__(self):
        return self.name
    

class Shift(models.Model):
    id = models.BigAutoField(primary_key=True)
    name = models.CharField(max_length=50)
    start_time = models.TimeField()
    end_time = models.TimeField()

    class Meta:
        db_table = 'canteen_shift'

    def __str__(self):
        return self.name

    @property
    def crosses_midnight(self):
        return self.end_time < self.start_time
    

class Attendance(models.Model):
    employee = models.ForeignKey(
        'Employee', on_delete=models.PROTECT, related_name='attendances'
    )
    punched_at = models.DateTimeField(default=timezone.now)
    shift = models.ForeignKey(
        'Shift', on_delete=models.CASCADE, null=True, blank=True, related_name='attendances'
    )
    meal_type = models.CharField(max_length=50, blank=True, null=True)  # e.g., Breakfast, Lunch
    

    class Meta:
        db_table = 'canteen_attendance'

    def __str__(self):
        return f"{self.employee.name} - {self.punched_at.strftime('%Y-%m-%d %H:%M')}"

    @property
    def punched_at_local(self):
        return self.punched_at.astimezone(LOCAL_TIMEZONE)
        
    
    
# ---------------------------------------------------------------------------------------------


## canteen head count

MEAL_TYPE_CHOICES = (
    ("Lunch", "Lunch"),
    ("Dinner", "Dinner"),
)

class CanteenHeadCount(models.Model):
    employee = models.ForeignKey("Employee", on_delete=models.PROTECT, related_name="CanteenHeadCount",)
    punched_at = models.DateTimeField(default=timezone.now)
    # single field – will store only "Lunch" or "Dinner"
    meal_type = models.CharField( max_length=50, choices=MEAL_TYPE_CHOICES, blank=True, null=True,)

    class Meta:
        db_table = "canteen_headcount"

    def __str__(self):
        return f"{self.employee.name} - {self.punched_at.strftime('%Y-%m-%d %H:%M')}"

    @property
    def punched_at_local(self):
        return self.punched_at.astimezone(LOCAL_TIMEZONE)

    def _auto_meal_type(self):
        """
        Decide Lunch / Dinner based on local punched_at time:
          - 06:30 – 10:00  → Lunch
          - 14:30 – 19:30  → Dinner
        """
        local_dt = self.punched_at_local
        t = local_dt.time()

        if dtime(6, 30) <= t <= dtime(10, 0):
            return "Lunch"
        if dtime(14, 30) <= t <= dtime(19, 30):
            return "Dinner"
        # fallback – you can also return None here
        return "Lunch"

    def save(self, *args, **kwargs):
        # if meal_type not explicitly given, derive from punched_at
        if not self.meal_type:
            self.meal_type = self._auto_meal_type()
        super().save(*args, **kwargs)
