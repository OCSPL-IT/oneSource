from django.apps import AppConfig

class SalesCrmConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "SALES_MARKETING.sales_crm"
    label = "sales_crm"   # IMPORTANT: matches permissions + namespace
    verbose_name = "Sales CRM"