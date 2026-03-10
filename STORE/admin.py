from django.contrib import admin
from .models import Vehicle

@admin.register(Vehicle)
class VehicleAdmin(admin.ModelAdmin):
    list_display = ('vehicle_no', 'name_of_supplier', 'material', 'reporting_date', 'unloading_date', 'status')
    list_filter = ('status', 'reporting_date', 'unloading_date', 'name_of_supplier')
    search_fields = ('vehicle_no', 'name_of_supplier', 'material', 'invoice')
    ordering = ('-record_date',)  # Orders by the most recent record first
    readonly_fields = ('unloading_days',)  # Prevent manual changes in unloading_days

# -------------------------------------------
# Rack_RM Store
# -------------------------------------------

from django.contrib import admin
from django.contrib.admin.sites import AlreadyRegistered
from .models import Rack, Pallet, GrnLineCache, IssueLineCache, RackAllocation, RackIssue

class RackAdmin(admin.ModelAdmin):
    list_display = ("code", "zone", "row", "level", "is_active")
    search_fields = ("code",)

class PalletAdmin(admin.ModelAdmin):
    list_display = ("number", "rack", "is_active")
    list_filter  = ("rack", "is_active")
    search_fields = ("number",)

def _safe_register(model, admin_class=None):
    try:
        admin.site.register(model, admin_class) if admin_class else admin.site.register(model)
    except AlreadyRegistered:
        pass

_safe_register(Rack, RackAdmin)
_safe_register(Pallet, PalletAdmin)
_safe_register(GrnLineCache)
_safe_register(IssueLineCache)
_safe_register(RackAllocation)
_safe_register(RackIssue)

