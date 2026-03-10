from django.core.management.base import BaseCommand
from STORE.material_issue_capex.services import rebuild_material_issue_lines


class Command(BaseCommand):
    help = "FULL refresh of Material Issue lines into material_issue_line table"

    def handle(self, *args, **options):
        inserted = rebuild_material_issue_lines()
        self.stdout.write(
            self.style.SUCCESS(
                f"Material Issue sync completed. Inserted {inserted} rows."
            )
        )
