from django.contrib import admin

# Register your models here.
from .models import OutgoingEmailAccount

@admin.register(OutgoingEmailAccount)
class OutgoingEmailAccountAdmin(admin.ModelAdmin):
    list_display = ("company_group", "from_email", "is_active")
    list_filter = ("company_group", "is_active")
    search_fields = ("from_email",)
