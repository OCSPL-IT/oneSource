from django.apps import AppConfig


class ReceivableConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"

    # This is the python import path of the app package
    name = "ACCOUNTS.Receivable"

    # This is the app label used by migrations: ('receivable', '0001_initial')
    label = "receivable"
