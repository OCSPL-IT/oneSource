# PRODUCTION/daily_block/management/commands/sync_erp_block.py

from django.apps import apps
from django.core.management.base import BaseCommand
from django.db import connections, transaction
from PRODUCTION.daily_block.models import (BlockItemMaster,BmrIssue,ERPBOMDetail)



class Command(BaseCommand):
    help = "Sync Item Master, Equipment Master, BMR Issues, and BOM details from ERP → Block tables."

    def handle(self, *args, **options):
        self.stdout.write("Starting ERP → Block sync…")
        erp = connections['readonly_db']

        # 1) Item Master
        with erp.cursor() as cur:
            self.stdout.write("  • Fetching Item Master…")
            cur.execute("""
                SELECT
                    i.sCode    AS product_id,
                    i.sName    AS product_name,
                    typ.sName  AS item_type
                FROM ITMMST i
                INNER JOIN ITMDET id ON i.lId = id.lId
                INNER JOIN ITMTYP typ ON typ.lTypId = id.lTypId
                WHERE id.lTypId IN (57,60,61,62,66,76,77,80,81);
            """)
            rows = cur.fetchall()

        with transaction.atomic():
            BlockItemMaster.objects.all().delete()
            bulk = [
                BlockItemMaster(
                    product_id   = r[0],
                    product_name = r[1],
                    item_type    = r[2],
                ) for r in rows
            ]
            BlockItemMaster.objects.bulk_create(bulk)
        self.stdout.write(f"    → {len(bulk)} Item Master rows inserted.")

        

        # 3) BMR Issues
        with erp.cursor() as cur:
            self.stdout.write("  • Fetching BMR Issues…")
            cur.execute("""
                SELECT
                    ROW_NUMBER() OVER (ORDER BY HDR.sDocNo) AS RowNumber,
                    CASE HDR.ltypid
                         WHEN 664 THEN 'Fresh Batch BMR Issue'
                         WHEN 717 THEN 'Cleaning Batch BMR Issue'
                         WHEN 718 THEN 'Reprocess Batch BMR Issue'
                         WHEN 719 THEN 'Blending Batch BMR Issue'
                         WHEN 720 THEN 'Distillation Batch BMR Issue'
                         WHEN 721 THEN 'ETP Batch BMR Issue'
                         ELSE 'NA'
                    END AS BMR_Issue_Type,
                    HDR.sDocNo        AS BMR_Issue_No,
                    CONVERT(DATE, CONVERT(VARCHAR(8), HDR.dtDocDate)) AS BMR_Issue_Date,
                    ITMCF.svalue      AS FG_Name,
                    BATCHCF.sValue    AS OP_Batch_No,
                    (SELECT sValue FROM txncf
                       WHERE lid = HDR.lid AND sName = 'Product Name' AND lLine = 0
                    ) AS Product_Name,
                    (SELECT sValue FROM txncf
                       WHERE lid = HDR.lid AND sName = 'Block' AND lLine = 0
                    ) AS Block,
                    DET.lLine         AS Line_No,
                    ITP.sName         AS Item_Type,
                    ITM.sCode         AS Item_Code,
                    ITM.sName         AS Item_Name,
                    DET.sNarr         AS Item_Narration,
                    UOM2.sCode        AS UOM,
                    CONVERT(DECIMAL(18,3), DET.dQty2) AS Batch_Quantity
                FROM txnhdr HDR
                  INNER JOIN txncf AS BATCHCF 
                    ON HDR.lId = BATCHCF.lId AND BATCHCF.sName = 'Batch No' AND BATCHCF.lLine = 0
                  INNER JOIN TXNDET DET ON HDR.lId = DET.lId
                  INNER JOIN ITMMST ITM  ON DET.lItmId = ITM.lId
                  INNER JOIN ITMTYP ITP  ON ITP.lTypid = DET.lItmtyp
                  INNER JOIN UNTMST UOM2 ON DET.lUntId2 = UOM2.lId
                  INNER JOIN ITMCF ITMCF
                    ON DET.lItmId = ITMCF.lId AND ITMCF.lFieldNo = 10 AND ITMCF.lLine = 0
                WHERE HDR.ltypid IN (664,717,718,719,720,721)
                  AND DET.lItmTyp <> 63
                  AND DET.bDel <> -2
                  AND HDR.bDel <> 1
                  AND DET.lClosed <> -2
                  AND HDR.lClosed = 0
                  AND HDR.lcompid = 27
                ORDER BY HDR.sDocNo, DET.lLine;
            """)
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()

        with transaction.atomic():
            BmrIssue.objects.all().delete()
            objs = []
            for r in rows:
                data = dict(zip(cols, r))
                objs.append(BmrIssue(
                    bmr_issue_type  = data['BMR_Issue_Type'],
                    bmr_issue_no    = data['BMR_Issue_No'],
                    bmr_issue_date  = data['BMR_Issue_Date'],
                    fg_name         = data['FG_Name'],
                    op_batch_no     = data['OP_Batch_No'],
                    product_name    = data.get('Product_Name') or '',
                    block           = data.get('Block') or '',
                    line_no         = data['Line_No'],
                    item_type       = data['Item_Type'],
                    item_code       = data['Item_Code'],
                    item_name       = data['Item_Name'],
                    item_narration  = data.get('Item_Narration') or '',
                    uom             = data['UOM'],
                    batch_quantity  = data['Batch_Quantity'],
                ))
            BmrIssue.objects.bulk_create(objs)
        self.stdout.write(f"    → {len(objs)} BMR Issues inserted.")

        # 4) BOM Details (Stage → FG, Equipment)
        with erp.cursor() as cur:
            self.stdout.write("  • Fetching BOM details (Stage → FG, Equipment)…")
            cur.execute("""
            ;WITH CTE_BOM AS (
                SELECT
                    MST.sName   AS StageName,
                    c.sValue    AS FGName,
                    MST1.sName  AS Equipment,
                    ROW_NUMBER() OVER (
                        PARTITION BY MST.sName
                        ORDER BY det.lBomId, det.lSeqId
                    ) AS rn
                FROM ITMBOMDET det
                INNER JOIN ITMBOM BOM  ON det.lBomId = BOM.lBomId
                INNER JOIN ITMMST MST    ON MST.lId    = BOM.lId
                LEFT JOIN ITMCF c        ON c.lId      = BOM.lId AND c.sName = 'FG Name'
                INNER JOIN ITMTYP TYP    ON TYP.lTypId = BOM.lTypId
                LEFT JOIN ITMMST MST1    ON MST1.lId   = det.lBomItm
                LEFT JOIN ITMDET DT      ON det.lBomItm = DT.lId
                INNER JOIN ITMTYP TYP1   ON TYP1.lTypId = DT.lTypId
                WHERE TYP1.sName = 'Equipment Master'
            )
            SELECT StageName, FGName, Equipment
            FROM CTE_BOM
            WHERE rn = 1
            ORDER BY StageName;
            """)
            rows = cur.fetchall()

        with transaction.atomic():
            ERPBOMDetail.objects.all().delete()
            bulk_bom = [
                ERPBOMDetail(
                    stage_name = r[0],
                    fg_name    = r[1] or '',
                    equipment  = r[2] or ''
                ) for r in rows
            ]
            ERPBOMDetail.objects.bulk_create(bulk_bom)
        self.stdout.write(f"    → {len(bulk_bom)} BOM detail rows inserted.")

        self.stdout.write(self.style.SUCCESS("ERP → Block sync complete."))


