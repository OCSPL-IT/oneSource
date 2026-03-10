# QC/management/commands/sync_incoming_grn.py
from datetime import date, datetime, timedelta
from django.core.management.base import BaseCommand, CommandError
from django.db import connections, transaction
from django.conf import settings

from QC.models import IncomingGRNCache

# NOTE: use positional param (%s) for broad mssql backend compatibility
# QC/management/commands/sync_incoming_grn.py

SQL_TEXT = r"""
SELECT
    HDR.sDocNo  AS grn_no,
    CONVERT(date, CONVERT(varchar(8), HDR.dtDocDate, 112)) AS grn_date,
    SUPP.sCode  AS supplier_code,
    SUPP.sName  AS supplier_name,
    LTRIM(RTRIM(ITP.sName)) AS item_type,
    ITM.sCode   AS item_code,
    ITM.sName   AS item_name,
    Convert(Decimal(18,3), DET.dQty) AS qty
FROM txnhdr HDR
INNER JOIN CMPNY  cmp       ON HDR.lCompId = cmp.lId
LEFT  JOIN BUSMST SUPP      ON HDR.lAccId1 = SUPP.lId
INNER JOIN TXNDET DET       ON HDR.lId = DET.lId
INNER JOIN ITMMST ITM       ON DET.lItmId = ITM.lId
INNER JOIN ITMTYP ITP       ON ITP.lTypid = DET.lItmtyp
LEFT  JOIN HSNMST HSN       ON HSN.lid = DET.lHSNid
WHERE
    HDR.ltypid in (164,528,540,548,551,779,790,791,792,793,795,797,794,796,802,801,800,798,803,
                   804,805,807,808,809,841,842,844,845,850,958,932,868,867)
    AND HDR.bDel = 0 AND DET.bDel = 0 AND DET.bDel <> -2
    AND HDR.lClosed = 0 AND DET.lClosed <> -2
    AND HDR.lcompid = 27
    -- keep only required item types (NO leading space in 'Raw Material')
    AND LTRIM(RTRIM(ITP.sName)) IN ('Key Raw Material','Raw Material','Packing Material')
    -- limit to a specific day (param provided by command)
    AND CONVERT(date, CONVERT(varchar(8), HDR.dtDocDate, 112)) = %s
"""
PREFERRED_ALIASES = ("ocspl_test", "readonly_db", "erp", "default")


def _pick_alias(explicit_alias: str | None) -> str:
    """Use explicit --alias if given; otherwise pick the first available preferred alias."""
    if explicit_alias:
        if explicit_alias not in connections.databases:
            raise CommandError(
                f"Database alias '{explicit_alias}' not found. "
                f"Available: {tuple(connections.databases.keys())}"
            )
        return explicit_alias
    for a in PREFERRED_ALIASES:
        if a in connections.databases:
            return a
    raise CommandError(
        f"No suitable ERP DB alias found. Tried {PREFERRED_ALIASES}. "
        f"Available: {tuple(connections.databases.keys())}"
    )


class Command(BaseCommand):
    help = "Sync previous day GRN lines (RM/PM) from ERP into qc_incoming_grn_cache"

    def add_arguments(self, parser):
        parser.add_argument(
            "--date",
            dest="date",
            help="Date to pull (YYYY-MM-DD). Defaults to yesterday.",
        )
        parser.add_argument(
            "--alias",
            dest="alias",
            default=None,  # default resolved by _pick_alias → 'ocspl_test'
            help="DATABASES alias for ERP connection (default picks ocspl_test → readonly_db → erp → default)",
        )

    @transaction.atomic
    def handle(self, *args, **opts):
        # Resolve DB alias (prefers your existing 'ocspl_test')
        alias = _pick_alias(opts.get("alias"))
        self.stdout.write(self.style.NOTICE(f"Using ERP connection alias: {alias}"))

        # Resolve target date
        if opts.get("date"):
            try:
                target = datetime.strptime(opts["date"], "%Y-%m-%d").date()
            except ValueError:
                raise CommandError("Invalid --date. Use YYYY-MM-DD.")
        else:
            target = date.today() - timedelta(days=1)  # yesterday

        self.stdout.write(self.style.NOTICE(f"Pulling ERP GRNs for {target}…"))

        # Query ERP and fetch rows
        with connections[alias].cursor() as cur:
            cur.execute(SQL_TEXT, [target])
            rows = cur.fetchall()
            desc = [c[0] for c in cur.description]

        # Map columns → objects
        objs: list[IncomingGRNCache] = []
        for r in rows:
            rec = dict(zip(desc, r))
            objs.append(
                IncomingGRNCache(
                    grn_no=rec["grn_no"],
                    grn_date=rec["grn_date"],
                    supplier_code=rec.get("supplier_code") or "",
                    supplier_name=rec.get("supplier_name") or "",
                    item_type=(rec.get("item_type") or "").strip(),
                    item_code=rec.get("item_code") or "",
                    item_name=rec.get("item_name") or "",
                    qty=rec.get("qty") or 0,
                )
            )

        # Idempotent replace for that day
        IncomingGRNCache.objects.filter(grn_date=target).delete()
        if objs:
            IncomingGRNCache.objects.bulk_create(objs, batch_size=1000)

        self.stdout.write(self.style.SUCCESS(f"Inserted {len(objs)} rows for {target}"))
