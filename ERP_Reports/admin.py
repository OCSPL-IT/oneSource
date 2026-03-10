from django.contrib import admin
from .models import CogsReportSchedule

@admin.register(CogsReportSchedule)
class CogsReportScheduleAdmin(admin.ModelAdmin):
    list_display = ("name", "is_enabled", "schedule_type", "hour", "minute", "day_of_week", "cron_month", "cron_day", "cron_dow", "date_range", "last_run_at")
    list_filter  = ("is_enabled", "schedule_type", "date_range")
    search_fields = ("name", "to_emails", "subject")
    fieldsets = (
        (None, {
            "fields": ("name", "is_enabled", "to_emails", "subject", "greet")
        }),
        ("Filters", {
            "fields": ("company_id", "year_id", "cust_code", "item_id", "txn_name", "customer_name", "item_name")
        }),
        ("Date Range", {
            "fields": ("date_range", "from_date", "to_date")
        }),
        ("Schedule", {
            "fields": ("schedule_type", "hour", "minute", "day_of_week", "cron_month", "cron_day", "cron_dow", "timezone")
        }),
        ("Audit", {
            "fields": ("last_run_at",),
        }),
    )
    readonly_fields = ("last_run_at",)
