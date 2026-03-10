from django.core.management.base import BaseCommand
from CONTRACT.hr_contract_etl import run_hr_contract_etl

class Command(BaseCommand):
    help = "Sync and upload contract attendance to SQL Server"

    def handle(self, *args, **kwargs):
        run_hr_contract_etl()
