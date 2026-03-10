from django.contrib import admin
from .models import SalesLead, LeadFollowUp

@admin.register(SalesLead)
class SalesLeadAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "company", "status", "source", "assigned_to", "updated_at")
    list_filter = ("status", "source", "assigned_to")
    search_fields = ("name", "company", "phone", "email")
    ordering = ("-updated_at", "-id")

@admin.register(LeadFollowUp)
class LeadFollowUpAdmin(admin.ModelAdmin):
    list_display = ("id", "lead", "next_date", "created_by", "created_at")
    list_filter = ("next_date", "created_by")
    search_fields = ("lead__name", "lead__company", "note")