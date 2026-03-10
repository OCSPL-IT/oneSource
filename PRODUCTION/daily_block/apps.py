from django.apps import AppConfig

class DailyBlockConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    # Use a dot notation path to the module
    name = 'PRODUCTION.daily_block'