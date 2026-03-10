# STORE/material_issue_capex/management/commands/sync_capex_grn.py

from django.core.management.base import BaseCommand
from STORE.material_issue_capex.services import rebuild_capex_grn_lines

class Command(BaseCommand):
    help = "FULL refresh of CAPEX GRN lines into capex_grn_line table"

    def handle(self, *args, **options):
        inserted = rebuild_capex_grn_lines()
        self.stdout.write(self.style.SUCCESS(
            f"CAPEX GRN sync completed. Inserted {inserted} rows."
        ))
