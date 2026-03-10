# qc/management/commands/sync_bmr_master.py

from django.core.management.base import BaseCommand
from QC.models import BmrIssue   # ← import the correct model

from django.db import connections, transaction

class Command(BaseCommand):
    help = "Synchronize BMR rows from ERP into the QC DB"

    def handle(self, *args, **options):
        """
        If the BmrIssue table is empty, pull everything from ERP and bulk‐insert.
        (This duplicates exactly what qc_create does on first run.)
        """
        # if BmrIssue.objects.exists():
        #     self.stdout.write("BmrIssue table already populated; skipping sync.")
        #     return

        with connections['readonly_db'].cursor() as cursor:
            cursor.execute("""
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

                    (SELECT sValue
                       FROM txncf
                      WHERE lid = HDR.lid 
                        AND sName = 'Product Name' 
                        AND lLine = 0
                    ) AS Product_Name,

                    (SELECT sValue
                       FROM txncf
                      WHERE lid = HDR.lid 
                        AND sName = 'Block' 
                        AND lLine = 0
                    ) AS Block,

                    DET.lLine            AS Line_No,
                    ITP.sName            AS Item_Type,
                    ITM.sCode            AS Item_Code,
                    ITM.sName            AS Item_Name,
                    DET.sNarr            AS Item_Narration,

                    UOM2.sCode           AS UOM,
                    CONVERT(DECIMAL(18,3), DET.dQty2) AS Batch_Quantity

                FROM
                    txnhdr HDR
                    INNER JOIN txncf AS BATCHCF 
                         ON HDR.lId = BATCHCF.lId
                        AND BATCHCF.sName = 'Batch No'
                        AND BATCHCF.lLine = 0

                    INNER JOIN TXNDET (NOLOCK) AS DET 
                         ON HDR.lId = DET.lId

                    INNER JOIN ITMMST (NOLOCK) AS ITM 
                         ON DET.lItmId = ITM.lId

                    INNER JOIN ITMTYP (NOLOCK) AS ITP 
                         ON ITP.lTypid = DET.lItmtyp

                    INNER JOIN UNTMST (NOLOCK) AS UOM 
                         ON DET.lUntId = UOM.lId

                    INNER JOIN UNTMST (NOLOCK) AS UOM2 
                         ON DET.lUntId2 = UOM2.lId

                    INNER JOIN ITMCF ITMCF
                         ON DET.lItmId = ITMCF.lId 
                        AND ITMCF.lFieldNo = 10 
                        AND ITMCF.lLine = 0

                WHERE
                    HDR.ltypid IN (664, 717, 718, 719, 720, 721)
                    AND DET.lItmTyp <> 63
                    AND DET.bDel <> -2
                    AND HDR.bDel <> 1
                    AND DET.lClosed <> -2
                    AND HDR.lClosed = 0
                    AND HDR.lcompid = 27

                ORDER BY
                    HDR.sDocNo,
                    DET.lLine;
            """)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

            existing_keys = set(BmrIssue.objects.values_list('bmr_issue_no', 'line_no'))

            to_create = []
            for row in rows:
                data = dict(zip(columns, row))
                key = (data['BMR_Issue_No'], data['Line_No'])
                if key not in existing_keys:
                    to_create.append(
                    BmrIssue(
                        bmr_issue_type  = data['BMR_Issue_Type'],
                        bmr_issue_no    = data['BMR_Issue_No'],
                        bmr_issue_date  = data['BMR_Issue_Date'],
                        fg_name         = data['FG_Name'],
                        op_batch_no     = data['OP_Batch_No'],
                        product_name    = data.get('Product_Name', '') or '',
                        block           = data.get('Block', '') or '',
                        line_no         = data['Line_No'],
                        item_type       = data['Item_Type'],
                        item_code       = data['Item_Code'],
                        item_name       = data['Item_Name'],
                        item_narration  = data.get('Item_Narration', '') or '',
                        uom             = data['UOM'],
                        batch_quantity  = data['Batch_Quantity'],
                    )
                )
            BmrIssue.objects.bulk_create(to_create)

        self.stdout.write(self.style.SUCCESS("BmrIssue table populated from ERP."))
