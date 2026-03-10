from django.contrib import admin
from .models import Department, Employee, Shift, Attendance
from import_export import resources
from import_export.admin import ExportMixin, ImportExportModelAdmin
from datetime import datetime,date
from import_export.formats.base_formats import XLSX, CSV
from import_export import resources, fields
from import_export.widgets import ForeignKeyWidget
from .utils import determine_shift



class DepartmentResource(resources.ModelResource):
    class Meta:
        model = Department
        import_id_fields = ('name',)
        fields = ('id','name',)

@admin.register(Department)
class DepartmentAdmin(ImportExportModelAdmin):
    resource_class = DepartmentResource
    list_display = ('id', 'name')
    search_fields = ('name',)





# Step 1: Define a Resource class for Employee
class EmployeeResource(resources.ModelResource):
    class Meta:
        model = Employee
        import_id_fields = ['id']  # Ensure primary key is used to detect updates
        fields = ('id', 'name', 'employee_type', 'department','location')  # Explicit fields to include

# Step 2: Register admin with import/export functionality
@admin.register(Employee)
class EmployeeAdmin(ImportExportModelAdmin):
    resource_class = EmployeeResource
    list_display = ('id', 'name', 'employee_type', 'department','location')
    list_filter = ('employee_type', 'department','location')
    search_fields = ('name', 'id')
    formats = [XLSX, CSV]


@admin.register(Shift)
class ShiftAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'start_time', 'end_time', 'crosses_midnight')
    search_fields = ('name',)
    list_filter = ('start_time', 'end_time')





class AttendanceResource(resources.ModelResource):
    employee = fields.Field(
        column_name='employee',
        attribute='employee',
        widget=ForeignKeyWidget(Employee, 'id')  # Map by Employee.name from Excel
    )
    shift = fields.Field(
        column_name='shift',
        attribute='shift',
        widget=ForeignKeyWidget(Shift, 'id')  # Map by Shift.name from Excel
    )

    class Meta:
        model = Attendance
        fields = ('id', 'employee', 'punched_at', 'shift', 'meal_type')


@admin.register(Attendance)
class AttendanceAdmin(ImportExportModelAdmin):
    resource_class = AttendanceResource
    list_display = ('id', 'employee', 'punched_at', 'shift', 'meal_type')
    list_filter = ('shift', 'meal_type', 'punched_at')
    search_fields = ('employee__name', 'employee__id')
    autocomplete_fields = ["employee"]
    formats = [XLSX, CSV]

    # HIDE both 'shift' and 'meal_type' fields from the admin form
    exclude = ('shift', 'meal_type',)

    def save_model(self, request, obj, form, change):
        # Auto-assign shift and meal_type before saving
        obj.shift = determine_shift(obj.punched_at)
        if obj.shift is None:
            raise ValidationError("Selected 'Punched at' time does not match any allowed shift time window.")
        obj.meal_type = "Meal"
        super().save_model(request, obj, form, change)
