from django.contrib import admin
from .models import Physical_Location, LeadingRecords, Lagging_Indicator, LaggingCapaEntry
from import_export import resources
from import_export.admin import ImportExportModelAdmin

class PhysicalLocationResource(resources.ModelResource):
    class Meta:
        model = Physical_Location
        # Optionally, specify fields to export/import:
        fields = ('id', 'name')

@admin.register(Physical_Location)
class PhysicalLocationAdmin(ImportExportModelAdmin):
    resource_class = PhysicalLocationResource
    list_display = ('id','name',)
    search_fields = ('name',)

@admin.register(LeadingRecords)
class LeadingRecordsAdmin(admin.ModelAdmin):
    list_display = (
        'id',
        'observation_date',
        'department',
        'physical_location',
        'leading_abnormality',
        'risk_factor',
        'status',
    )
    list_filter = ('observation_date', 'department', 'status')
    search_fields = ('department', 'leading_abnormality', 'initiated_by', 'psl_member_name')


class LaggingCapaEntryInline(admin.TabularInline):
    model = LaggingCapaEntry
    extra = 1
    fields = ('capa', 'department', 'frp', 'target_date', 'compliance_status')
    # Optionally, you can add readonly_fields if needed:
    # readonly_fields = ('compliance_status',)

@admin.register(Lagging_Indicator)
class LaggingIndicatorAdmin(admin.ModelAdmin):
    list_display = (
        'id', 
        'record_date', 
        'incident_date', 
        'employee_type', 
        'department', 
        'complience_status', 
        'complience_status_date'
    )
    list_filter = ('record_date', 'complience_status')
    search_fields = ('employee_type', 'department', 'psm_failure')
    inlines = [LaggingCapaEntryInline]

# Optionally, you can also register the inline model separately if needed:
@admin.register(LaggingCapaEntry)
class LaggingCapaEntryAdmin(admin.ModelAdmin):
    list_display = ('id', 'lagging_indicator', 'capa', 'department', 'frp', 'target_date', 'compliance_status')
    list_filter = ('compliance_status',)
    search_fields = ('capa', 'department', 'frp')
