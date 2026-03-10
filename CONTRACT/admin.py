from django.contrib import admin
from .models import *
from import_export import resources
from import_export.admin import ImportExportModelAdmin

@admin.register(ContractorName)
class ContractDepartmentAdmin(admin.ModelAdmin):
    list_display = ('id', 'name')
    search_fields = ('name',)
    ordering = ('name',)

class ContractEmployeeResource(resources.ModelResource):
    class Meta:
        model = ContractEmployee
        import_id_fields = ['id']  # Use ID for updates
        fields = ('id', 'name', 'employee_type', 'department')


@admin.register(ContractEmployee)
class ContractEmployeeAdmin(ImportExportModelAdmin):
    resource_class = ContractEmployeeResource
    list_display = ('id', 'name', 'employee_type', 'department')
    list_filter = ('department', 'employee_type')
    search_fields = ('id', 'name')




    
# admin.py
from django.contrib import admin
from import_export.admin import ImportExportModelAdmin

from .models import EmployeeAssignment


@admin.register(EmployeeAssignment)
class EmployeeAssignmentAdmin(ImportExportModelAdmin):
    """
    EmployeeAssignment admin with Import/Export buttons (django-import-export)
    WITHOUT creating a separate Resource file.
    """

    # List page
    list_display = (
        "punch_date",
        "employee",
        "get_employee_id",
        "contractor",
        "department",
        "block_location",
        "shift",
        "punch_in",
        "punch_out",
        "is_reassigned",
        "assigned_date",
    )
    list_select_related = ("employee", "contractor")
    list_filter = (
        "punch_date",
        "department",
        "block_location",
        "shift",
        "is_reassigned",
        "contractor",
    )
    search_fields = (
        "employee__id",
        "employee__name",
        "employee__employee_type",
        "contractor__name",
        "department",
        "block_location",
        "shift",
    )
    date_hierarchy = "punch_date"
    ordering = ("-punch_date", "employee__name")
    list_per_page = 50

    # Form page
    autocomplete_fields = ("employee", "contractor")  # needs search_fields in those ModelAdmins if you add them
    raw_id_fields = ()  # keep empty since autocomplete is used
    readonly_fields = ("assigned_date",)

    fieldsets = (
        ("Punch", {"fields": ("punch_date", "employee", "punch_in", "punch_out")}),
        ("Assignment", {"fields": ("contractor", "department", "block_location", "shift", "is_reassigned")}),
        ("System", {"fields": ("assigned_date",)}),
    )

    # Import-export settings
    # (No Resource file; ImportExportModelAdmin will infer fields)
    import_id_fields = ("id",)  # if you import by PK; remove if you don't want updates by PK
    skip_admin_log = False

    def get_employee_id(self, obj):
        return obj.employee_id  # FK_id shortcut
    get_employee_id.short_description = "Emp ID"
    get_employee_id.admin_order_field = "employee__id"
