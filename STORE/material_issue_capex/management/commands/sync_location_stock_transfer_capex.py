from django.core.management.base import BaseCommand
from STORE.material_issue_capex.services import rebuild_location_stock_transfer_capex_lines


class Command(BaseCommand):
    help = "FULL refresh of Location Stock Transfer CAPEX lines into location_stock_transfer_capex table"

    def handle(self, *args, **options):
        inserted = rebuild_location_stock_transfer_capex_lines()
        self.stdout.write(
            self.style.SUCCESS(
                f"Location Stock Transfer CAPEX sync completed. Inserted {inserted} rows."
            )
        )