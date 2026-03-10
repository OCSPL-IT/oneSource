# QC/management/commands/import_alfa_master.py
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from QC.models import AlfaProductMaster
import pandas as pd
from pathlib import Path

def _nz(val) -> str:
    """Normalize cell to clean string (keeps empty for NaN/None)."""
    try:
        if pd.isna(val):
            return ""
    except Exception:
        pass
    return str(val).strip()

def _norm_key(s: str) -> str:
    """Normalization key for case/space-insensitive matching."""
    return " ".join((s or "").split()).lower()

class Command(BaseCommand):
    help = "Import Alfa Product master from Excel. Required columns: 'Alfa Name', 'Finished Product Name'"

    def add_arguments(self, parser):
        parser.add_argument(
            "--file",
            required=True,
            help="Path to Excel file with columns: 'Alfa Name', 'Finished Product Name'",
        )
        parser.add_argument(
            "--sheet",
            default=None,
            help="Optional sheet name or index (default: first non-empty sheet).",
        )
        parser.add_argument(
            "--deactivate-missing",
            action="store_true",
            help="Mark existing rows not present in the file as is_active=False.",
        )

    def _load_single_sheet_df(self, path: Path, sheet_opt):
        """
        Returns a single DataFrame:
        - If sheet_opt is None/blank: load first non-empty sheet.
        - If sheet_opt is int-like: treat as index.
        - Else: treat as sheet name.
        Handles the case where pandas returns a dict of DataFrames.
        """
        try:
            if sheet_opt is None or str(sheet_opt).strip() == "":
                df = pd.read_excel(path, sheet_name=None, dtype=str)
                if isinstance(df, dict):
                    # Pick first non-empty sheet in workbook order
                    for _name, _df in df.items():
                        if _df is not None and not _df.empty:
                            return _df
                    raise CommandError("All sheets are empty in the provided workbook.")
                return df
            else:
                # Try as index first, then as name
                try:
                    idx = int(sheet_opt)
                    return pd.read_excel(path, sheet_name=idx, dtype=str)
                except (ValueError, TypeError):
                    return pd.read_excel(path, sheet_name=str(sheet_opt), dtype=str)
        except Exception as e:
            raise CommandError(f"Failed to read Excel: {e}")

    @transaction.atomic
    def handle(self, *args, **opts):
        path = Path(opts["file"])
        if not path.exists():
            raise CommandError(f"File not found: {path}")

        # Load a single DataFrame from Excel robustly
        df = self._load_single_sheet_df(path, opts.get("sheet"))
        # Ensure string dtype consistently
        df = df.astype(str)

        # Flexible header mapping (case/space insensitive)
        cols_lower = {str(c).strip().lower(): c for c in df.columns}
        alfa_col = cols_lower.get("alfa name") or cols_lower.get("alfa")
        fin_col  = cols_lower.get("finished product name") or cols_lower.get("finished_product_name")
        if not alfa_col or not fin_col:
            raise CommandError("Missing required columns: 'Alfa Name' and 'Finished Product Name'.")

        # Preload existing rows by normalized alfa_name
        existing = { _norm_key(obj.alfa_name): obj for obj in AlfaProductMaster.objects.all() }

        seen_keys = set()
        created = updated = unchanged = 0

        # Iterate rows
        for _, row in df.iterrows():
            raw_alfa = _nz(row.get(alfa_col))
            raw_fin  = _nz(row.get(fin_col))
            if not raw_alfa:
                continue

            key = _norm_key(raw_alfa)
            seen_keys.add(key)

            new_alfa = " ".join(raw_alfa.split())
            new_fin  = raw_fin

            if key in existing:
                obj = existing[key]
                changed = False

                if obj.alfa_name != new_alfa:
                    obj.alfa_name = new_alfa
                    changed = True
                if obj.finished_product_name != new_fin:
                    obj.finished_product_name = new_fin
                    changed = True
                if not obj.is_active:
                    obj.is_active = True
                    changed = True

                if changed:
                    obj.save(update_fields=["alfa_name", "finished_product_name", "is_active"])
                    updated += 1
                else:
                    unchanged += 1
            else:
                obj = AlfaProductMaster(
                    alfa_name=new_alfa,
                    finished_product_name=new_fin,
                    is_active=True,
                )
                obj.save()
                existing[key] = obj
                created += 1

        deactivated = 0
        if opts["deactivate_missing"]:
            for key, obj in existing.items():
                if key not in seen_keys and obj.is_active:
                    obj.is_active = False
                    obj.save(update_fields=["is_active"])
                    deactivated += 1

        self.stdout.write(self.style.SUCCESS(
            f"Imported: {created} created, {updated} updated, {unchanged} unchanged"
            + (f", {deactivated} deactivated" if opts["deactivate_missing"] else "")
        ))
