# STORE/material_issue_capex/apps.py

from django.apps import AppConfig


class MaterialIssueCapexConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    # full dotted path to this app package
    name = "STORE.material_issue_capex"
    verbose_name = "Material Issue – CAPEX"
