from decimal import Decimal
from django.core.management.base import BaseCommand, CommandError
from django.db import connections, transaction, DatabaseError

from ACCOUNTS.Budget.models import ERPBOMRow


ERP_BOM_SQL = r"""
WITH CTE_BOMDetails AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY det.lBomId, det.lSeqId) AS [SrNo],
        det.lBomId AS [BomId],
        det.lSeqId AS [SeqId],
        TYP.sName AS [ItmType],
        MST.sName AS [ItemName],
        c.sValue AS [FGName],
        MST.sCode AS [ItemCode],
        BOM.dQty AS [Quantity],
        BOM.dRate AS [Rate],
        BOM.sCode AS [BOMCode],
        BOM.sName AS [BOMName],
        TYP1.sName AS [Type],
        MST1.sCode AS [BOMItemCode],
        MST1.sName AS [Name],
        CASE
            WHEN det.cFlag='P' THEN CAST(det.lUntId AS VARCHAR)
            ELSE u.sName
        END AS [Unit],
        BOM.cTyp AS [BasedOn],
        det.dPercentage AS [Percentage],
        CASE
            WHEN det.cFlag='P' THEN det.dQtyPrc
            ELSE det.dQty
        END AS [BOMQty],
        BOM.dCnv AS [BOMCnv],
        det.cFlag AS [cFlag],
        DSG.sCode AS [ResourceType],
        CASE
            WHEN st.lFieldNo=1 THEN BOM.svalue1
            WHEN st.lFieldNo=2 THEN BOM.svalue2
            WHEN st.lFieldNo=3 THEN BOM.svalue3
            WHEN st.lFieldNo=4 THEN BOM.svalue4
            WHEN st.lFieldNo=5 THEN BOM.svalue5
            WHEN st.lFieldNo=6 THEN BOM.svalue6
            WHEN st.lFieldNo=7 THEN BOM.svalue7
            WHEN st.lFieldNo=8 THEN BOM.svalue8
            WHEN st.lFieldNo=9 THEN BOM.svalue9
            WHEN st.lFieldNo=10 THEN BOM.svalue10
            ELSE ''
        END AS [StockParameter]
    FROM ITMBOMDET det
    INNER JOIN ITMBOM BOM ON det.lBomId = BOM.lBomId
    INNER JOIN ITMMST MST ON MST.lId = BOM.lId
    LEFT JOIN ITMCF c ON BOM.lId = c.lId AND c.sName = 'FG Name'
    INNER JOIN ITMTYP TYP ON TYP.lTypId = BOM.lTypId
    LEFT JOIN ITMMST MST1 ON MST1.lId = det.lBomItm
    LEFT JOIN ITMDET DT ON det.lBomItm = DT.lId
    LEFT JOIN ITMTYP TYP1 ON TYP1.lTypId = DT.lTypId
    LEFT JOIN UNTMST u ON det.lUntId = u.lId
    LEFT OUTER JOIN DSGMST DSG ON DSG.lId = det.lResourceId
    LEFT JOIN STKPRM st ON st.lTypId = TYP.lTypId AND st.bBOM = 1
)
SELECT *
FROM CTE_BOMDetails
ORDER BY [SrNo];
"""


def _to_dec(v):
    if v in (None, "", "NA", "N/A"):
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


class Command(BaseCommand):
    help = "Sync BOM master from ERP DB into main DB as ERPBOMRow."

    def add_arguments(self, parser):
        parser.add_argument(
            "--truncate",
            action="store_true",
            help="Delete existing ERPBOMRow before sync",
        )
        parser.add_argument(
            "--erp-alias",
            default="readonly_db",  # ✅ your real alias (as seen in connections.databases)
            help="DB alias to use for ERP connection (default: readonly_db).",
        )

    def handle(self, *args, **opts):
        truncate = bool(opts.get("truncate"))
        alias = (opts.get("erp_alias") or "readonly_db").strip()

        # --- Validate alias exists in settings.DATABASES ---
        if alias not in connections.databases:
            available = ", ".join(connections.databases.keys())
            raise CommandError(
                f"Database alias '{alias}' does not exist in settings.DATABASES. "
                f"Available aliases: {available}"
            )

        erp_conn = connections[alias]

        # --- Read ERP rows ---
        try:
            with erp_conn.cursor() as cur:
                cur.execute(ERP_BOM_SQL)
                cols = [c[0] for c in cur.description]
                rows = cur.fetchall()
        except DatabaseError as e:
            raise CommandError(f"Failed to execute ERP_BOM_SQL on alias '{alias}': {e}")

        if not rows:
            self.stdout.write(self.style.WARNING(f"No rows returned from ERP query (alias='{alias}')."))
            return

        data = [dict(zip(cols, r)) for r in rows]

        # --- Sync into main DB ---
        with transaction.atomic():
            if truncate:
                ERPBOMRow.objects.all().delete()

            created = 0
            updated = 0
            skipped = 0
            deduped = 0

            # IMPORTANT:
            # Your DB constraint uq_erp_bom_row is on (bom_id, bom_item_code, fg_name, type).
            # So the update_or_create lookup MUST match that exact key.
            seen = set()

            for d in data:
                bom_id = d.get("BomId")  # must be present in SQL for stable upsert
                seq_id = d.get("SeqId")  # optional (store if present)

                fg_name = (d.get("FGName") or "").strip()
                typ = (d.get("Type") or "").strip()
                bom_item_code = (d.get("BOMItemCode") or "").strip()[:100]

                # Strong guard: must satisfy unique key columns + essential fields
                if bom_id in (None, "", 0) or not bom_item_code or not fg_name or not typ:
                    skipped += 1
                    continue

                key = (bom_id, bom_item_code, fg_name, typ)
                if key in seen:
                    deduped += 1
                    continue
                seen.add(key)

                defaults = dict(
                    # row marker (if your SQL returns it as Sr.No / SrNo adjust here)
                    sr_no=d.get("Sr.No") if "Sr.No" in d else d.get("SrNo"),

                    seq_id=seq_id,
                    cflag=(d.get("cFlag") or "").strip(),

                    itm_type=(d.get("ItmType") or "").strip(),
                    item_name=(d.get("ItemName") or "").strip(),
                    fg_name=fg_name,
                    item_code=(d.get("ItemCode") or "").strip(),

                    quantity=_to_dec(d.get("Quantity")),
                    rate=_to_dec(d.get("Rate")),

                    bom_code=(d.get("BOMCode") or "").strip(),
                    bom_name=(d.get("BOMName") or "").strip(),

                    type=typ,
                    bom_item_code=bom_item_code,
                    bom_item_name=(d.get("Name") or "").strip(),
                    unit=(d.get("Unit") or "").strip(),

                    based_on=(d.get("Based on") or d.get("BasedOn") or "").strip(),
                    percentage=_to_dec(d.get("Percentage")),
                    bom_qty=_to_dec(d.get("BOMQty")),
                    bom_cnv=_to_dec(d.get("BOMCnv")),

                    resource_type=(d.get("Resource Type") or d.get("ResourceType") or "").strip(),
                    stock_parameter=(d.get("Stock Parameter") or d.get("StockParameter") or "").strip(),

                    source_db=erp_conn.settings_dict.get("NAME", "") or alias,
                )

                obj, is_created = ERPBOMRow.objects.update_or_create(
                    # ✅ MUST match DB unique constraint uq_erp_bom_row
                    bom_id=bom_id,
                    bom_item_code=bom_item_code,
                    fg_name=fg_name,
                    type=typ,
                    defaults=defaults,
                )

                if is_created:
                    created += 1
                else:
                    updated += 1

        self.stdout.write(
            self.style.SUCCESS(
                "ERP BOM sync completed. "
                f"alias='{alias}' rows={len(rows)} created={created} updated={updated} "
                f"skipped={skipped} deduped={deduped}"
            )
        )