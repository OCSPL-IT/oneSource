from django.apps import AppConfig
from .scheduler import start
import os

class HrConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'HR'

    def ready(self):
        # Prevent scheduler from running twice when using runserver
        if os.environ.get('RUN_MAIN') == 'true':
            start()

