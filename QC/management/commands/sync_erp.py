# qc/management/commands/sync_erp.py

from django.apps import apps
from django.core.management.base import BaseCommand
from django.db import connections, transaction

# Retrieve the models dynamically to avoid import errors
LocalItemMaster      = apps.get_model('QC', 'LocalItemMaster')
LocalEquipmentMaster = apps.get_model('QC', 'LocalEquipmentMaster')
LocalBOMDetail       = apps.get_model('QC', 'LocalBOMDetail')

class Command(BaseCommand):
    help = "Sync BOM Details, Item Master, and Equipment Master from ERP."

    def handle(self, *args, **options):
        self.stdout.write("Starting ERP → QC sync…")
        erp_conn = connections['readonly_db']

        # ─── 1) Sync BOM DETAILS ───────────────────────────────────────────────
        with erp_conn.cursor() as cursor:
            self.stdout.write("  • Fetching BOM Details from ERP…")
            cursor.execute("""
            WITH CTE_BOMDetails AS (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY det.lBomId, det.lSeqId) AS SrNo,
                    TYP.sName        AS ItmType,
                    MST.sName        AS ItemName,
                    c.sValue         AS FGName,
                    MST.sCode        AS ItemCode,
                    BOM.dQty         AS Quantity,
                    BOM.sCode        AS BOMCode,
                    BOM.sName        AS BOMName,
                    TYP1.sName       AS Type,
                    MST1.sCode       AS BOMItemCode,
                    MST1.sName       AS Name,
                    CASE 
                        WHEN det.cFlag='P' THEN CAST(det.lUntId AS VARCHAR)
                        ELSE u.sName
                    END               AS Unit,
                    CASE
                        WHEN det.cFlag='P' THEN det.dQtyPrc
                        ELSE det.dQty
                    END               AS BOMQty,
                    det.cFlag         AS CFlag
                FROM ITMBOMDET det
                INNER JOIN ITMBOM     BOM  ON det.lBomId    = BOM.lBomId
                INNER JOIN ITMMST     MST  ON MST.lId       = BOM.lId
                LEFT  JOIN ITMCF      c    ON c.lId = BOM.lId AND c.sName = 'FG Name'
                INNER JOIN ITMTYP     TYP  ON TYP.lTypId    = BOM.lTypId
                LEFT  JOIN ITMMST     MST1 ON MST1.lId      = det.lBomItm
                LEFT  JOIN ITMDET     DT   ON det.lBomItm   = DT.lId
                LEFT  JOIN ITMTYP     TYP1 ON TYP1.lTypId   = DT.lTypId
                LEFT  JOIN UNTMST     u    ON det.lUntId    = u.lId
            )
            SELECT *
            FROM CTE_BOMDetails
            WHERE Type = 'Equipment Master'
            ORDER BY SrNo;
            """)
            bom_rows = cursor.fetchall()

        with transaction.atomic():
            LocalBOMDetail.objects.all().delete()
            to_create = [
                LocalBOMDetail(
                    sr_no         = row[0],
                    itm_type      = row[1],
                    item_name     = row[2],
                    fg_name       = row[3],
                    item_code     = row[4],
                    quantity      = row[5],
                    bom_code      = row[6],
                    bom_name      = row[7],
                    type          = row[8],
                    bom_item_code = row[9],
                    name          = row[10],
                    unit          = row[11],
                    bom_qty       = row[12],
                    cflag         = row[13],
                )
                for row in bom_rows
            ]
            LocalBOMDetail.objects.bulk_create(to_create)
        self.stdout.write(f"    → Inserted {len(to_create)} BOM detail rows.")

        # ─── 2) Sync ITEM MASTER ───────────────────────────────────────────────
        with erp_conn.cursor() as cursor:
            self.stdout.write("  • Fetching Item Master from ERP…")
            cursor.execute("""
                SELECT 
                    i.sCode AS ProductID,
                    i.sName AS ProductName,
                    typ.sName AS ItemType
                FROM ITMMST i
                INNER JOIN ITMDET id ON i.lId = id.lId
                LEFT JOIN UNTMST u ON id.lUntStk = u.lId
                INNER JOIN ITMTYP typ ON typ.lTypId = id.lTypId
                WHERE id.lTypId IN (57,60,61,62,66,76,77,80,81);
            """)
            rows = cursor.fetchall()

        with transaction.atomic():
            LocalItemMaster.objects.all().delete()
            bulk_items = [
                LocalItemMaster(
                    product_id   = row[0],
                    product_name = row[1],
                    item_type    = row[2]
                )
                for row in rows
            ]
            LocalItemMaster.objects.bulk_create(bulk_items)
        self.stdout.write(f"    → Inserted {len(bulk_items)} ItemMaster rows.")

        # ─── 3) Sync EQUIPMENT MASTER ──────────────────────────────────────────
        with erp_conn.cursor() as cursor:
            self.stdout.write("  • Fetching Equipment Master from ERP…")
            cursor.execute("""
                SELECT 
                    i.sCode    AS [EQP Code],
                    i.sName    AS [EQP Name],
                    i.sRemarks AS [EQP Remarks],
                    u.sCode    AS UnitCode,
                    (SELECT c.svalue 
                     FROM itmcf c 
                     WHERE c.lid = i.lid 
                       AND c.lline = 0 
                       AND c.sname = 'Tag No')    AS [Tag No],
                    (SELECT c.svalue 
                     FROM itmcf c 
                     WHERE c.lid = i.lid 
                       AND c.lline = 0 
                       AND c.sname = 'Block')     AS [Block Name]
                FROM ITMMST i
                INNER JOIN ITMDET id 
                    ON i.lId = id.lId 
                   AND id.lTypId = 63
                LEFT JOIN UNTMST u 
                    ON id.lUntStk = u.lid;
            """)
            eq_rows = cursor.fetchall()

        with transaction.atomic():
            LocalEquipmentMaster.objects.all().delete()
            bulk_equips = [
                LocalEquipmentMaster(
                    eqp_code    = row[0],
                    eqp_name    = row[1],
                    eqp_remarks = row[2],
                    unit_code   = row[3],
                    tag_no      = row[4],
                    block_name  = row[5]
                )
                for row in eq_rows
            ]
            LocalEquipmentMaster.objects.bulk_create(bulk_equips)
        self.stdout.write(f"    → Inserted {len(bulk_equips)} EquipmentMaster rows.")

        self.stdout.write(self.style.SUCCESS("ERP → QC sync complete."))
