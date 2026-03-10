from django.core.management.base import BaseCommand
from QC.models import QCInstrument

SEED = [
    ("Gas Chromatograph", "OCSPL/QC/GC/001", "GC"),
    ("Gas Chromatograph", "OCSPL/QC/GC/003", "GC"),
    ("Gas Chromatograph", "OCSPL/QC/GC/004", "GC"),
    ("Gas Chromatograph", "OCSPL/QC/GC/006", "GC"),
    ("Gas Chromatograph", "OCSPL/QC/GC/007", "GC"),
    ("Gas Chromatograph", "OCSPL/QC/GC/008", "GC"),
    ("Gas Chromatograph", "OCSPL/QC/GC/009", "GC"),
    ("Gas Chromatograph", "OCSPL/QC/GC/010", "GC"),
    ("Gas Chromatograph", "OCSPL/QC/GC/011", "GC"),
    ("HPLC",              "OCSPL/QC/HPLC/001", "HPLC"),
    ("HPLC",              "OCSPL/QC/HPLC/002", "HPLC"),
    ("HPLC",              "OCSPL/QC/HPLC/003", "HPLC"),
    ("HPLC",              "OCSPL/QC/HPLC/004", "HPLC"),
    ("Potentiometric Titrator", "OCSPL/QC/AT/002", "AT"),
    ("Karl Fischer",      "OCSPL/QC/KF/003", "KF"),
    ("Karl Fischer",      "OCSPL/QC/KF/004", "KF"),
    ("Karl Fischer",      "OCSPL/QC/KF/005", "KF"),
    ("pH Meter",          "OCSPL/QC/pH/003", "pH"),
    ("pH Meter",          "OCSPL/QC/pH/004", "pH"),
    ("Melting Range",     "OCSPL/QC/MR/002", "MR"),
    ("Polarimeter",       "OCSPL/QC/POL/001", "POL"),
    ("UV Spectrophotometer", "OCSPL/QC/UV/001", "UV"),
    ("Gel Timer",         "OCSPL/QC/GT/001", "GT"),
    ("Moisture Balance",  "OCSPL/QC/MA/001", "MA"),
    ("Moisture Balance",  "OCSPL/QC/MA/002", "MA"),
    ("APHA",              "OCSPL/QC/AM/001", "AM"),
    ("Conductivity meter","OCSPL/QC/CM/001", "CM"),
    ("None",              "None",            "Other"),
]

class Command(BaseCommand):
    help = "Seed QC instrument master"

    def handle(self, *args, **kwargs):
        created = 0
        for name, code, cat in SEED:
            obj, was_created = QCInstrument.objects.get_or_create(
                code=code,
                defaults={"name": name, "category": cat, "is_active": True}
            )
            created += int(was_created)
        self.stdout.write(self.style.SUCCESS(f"Seeded {created} instruments"))
