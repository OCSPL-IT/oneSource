# maintenance/admin.py
from django.contrib import admin
from .models import MaintenanceSchedule

@admin.register(MaintenanceSchedule)
class MaintenanceScheduleAdmin(admin.ModelAdmin):
    list_display = ("equipment_id", "location", "scheduled_date", "rescheduled_to", "status", "completed_at")
    list_filter  = ("status", "location", "scheduled_date")
    search_fields = ("equipment_id", "location", "downtime_reason", "notes")
