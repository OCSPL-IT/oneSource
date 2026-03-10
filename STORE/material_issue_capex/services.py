# STORE/material_issue_capex/services.py

import logging
from django.db import connections, transaction
from datetime import datetime, date
from .models import *

logger = logging.getLogger("custom_logger")

SQL_CAPEX_GRN_LINES = r"""
;WITH vTxnHdr AS
(
    SELECT
        dt.sName,
        d.*
    FROM TXNTYP (NOLOCK) AS dt
    INNER JOIN TXNHDR (NOLOCK) AS d
        ON dt.lTypId = d.lTypId
       AND dt.lTypId = 164          -- CAPEX GRN type
    WHERE d.lId      > 0
      AND d.lCompId  = 27
      AND d.dtDocDate >= 20260101
),
dd AS
(
    SELECT dd.*
    FROM vTxnHdr AS d
    INNER JOIN TXNDET (NOLOCK) AS dd
        ON d.lId = dd.lId
    WHERE d.bDel = 0
      AND dd.bDel = 0
),
dc AS
(
    SELECT
        cg.lId,
        cg.lLine,
        SUM(CASE WHEN cg.lFieldNo = 1  THEN cg.dRate  ELSE 0 END) AS dcRate1,
        SUM(CASE WHEN cg.lFieldNo = 1  THEN cg.dValue ELSE 0 END) AS dcValue1,
        SUM(CASE WHEN cg.lFieldNo = 16 THEN cg.dValue ELSE 0 END) AS dcValue16
    FROM vTxnHdr AS d
    INNER JOIN TXNCHRG (NOLOCK) AS cg
        ON d.lId = cg.lId
       AND d.bDel = 0
    WHERE cg.lLine > 0
    GROUP BY cg.lId, cg.lLine
),
dcf AS
(
    SELECT
        cf.lId,
        cf.lLine,
        MAX(CASE WHEN cf.lFieldNo = 7 THEN cf.sValue ELSE '' END) AS cfValue7
    FROM vTxnHdr AS d
    INNER JOIN TXNCF (NOLOCK) AS cf
        ON d.lId = cf.lId
    WHERE cf.lLine > 0
    GROUP BY cf.lId, cf.lLine
),
vView AS
(
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY d.dtDocDate DESC, d.sDocNo DESC, dd.lLine
        )                                           AS RowNumber,

        dd.sValue1                                  AS BatchNo,
        l1.sName                                    AS [Location],
        dcf.cfValue7                                AS [Virtual Location],
        i.sCode                                     AS ItemCode,
        i.sName                                     AS ItemName,
        d.sDocNo                                    AS DocNo,
        d.dTotal                                    AS TxnTotal,
        dd.dQty2                                    AS [Quantity],
        dc.dcRate1                                  AS [Rate],
        dc.dcValue1                                 AS [Rate Amount],
        dc.dcValue16                                AS [Total Amount]

    FROM vTxnHdr AS d
    INNER JOIN dd
        ON d.lId = dd.lId
    LEFT JOIN ITMMST (NOLOCK) AS i
        ON dd.lItmId = i.lId
    LEFT JOIN DIMMST (NOLOCK) AS l1
        ON dd.lLocId = l1.lId
    LEFT JOIN dc
        ON dd.lId = dc.lId
       AND dd.lLine = dc.lLine
    LEFT JOIN dcf
        ON dd.lId = dcf.lId
       AND dd.lLine = dcf.lLine
)

SELECT
    BatchNo,
    [Location],
    [Virtual Location],
    ItemCode,
    ItemName,
    DocNo,
    TxnTotal,
    [Quantity],
    [Rate],
    [Rate Amount],
    [Total Amount]
FROM vView
ORDER BY RowNumber;
"""


def rebuild_capex_grn_lines() -> int:
    """
    FULL REFRESH:
    - Deletes all rows from capex_grn_line on default DB
    - Re-loads from readonly_db using SQL_CAPEX_GRN_LINES
    """
    logger.info("Starting CAPEX GRN FULL refresh")

    # 1) Read from readonly_db
    with connections["readonly_db"].cursor() as cursor:
        cursor.execute(SQL_CAPEX_GRN_LINES)
        rows = cursor.fetchall()

    logger.info("Fetched %s rows from readonly_db", len(rows))

    # 2) Write into default DB
    with transaction.atomic(using="default"):
        # Always clear existing rows
        CapexGrnLine.objects.using("default").all().delete()
        logger.info("Cleared existing capex_grn_line records")

        objs = []
        for r in rows:
            # order: BatchNo, Location, Virtual Location, ItemCode, ItemName,
            #        DocNo, TxnTotal, Quantity, Rate, Rate Amount, Total Amount
            objs.append(
                CapexGrnLine(
                    batch_no=r[0],
                    location=r[1],
                    virtual_location=r[2],
                    item_code=r[3],
                    item_name=r[4],
                    doc_no=r[5],
                    txn_total=r[6],
                    quantity=r[7],
                    rate=r[8],
                    rate_amount=r[9],
                    total_amount=r[10],
                )
            )

        CapexGrnLine.objects.using("default").bulk_create(objs, batch_size=500)

    logger.info("Inserted %s rows into capex_grn_line", len(objs))
    return len(objs)






SQL_MATERIAL_ISSUE_LINES = r"""
-- Material Issue (only required columns + TxnTotal + Doc. No.)

WITH vTxnHdr AS (
    SELECT dt.sName, d.*
    FROM TXNTYP  (NOLOCK) AS dt
    INNER JOIN TXNHDR (NOLOCK) AS d
        ON dt.lTypId = d.lTypId
       AND dt.lTypId = 987
    WHERE d.lId > 0
      AND d.lCompId = 27
      AND d.dtDocDate >= 20260101
),
vView AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY d.dtDocDate DESC, d.sDocNo DESC, dd.lLine
        )                                                AS RowNumber,

        CAST(CONVERT(DATETIME, CONVERT(VARCHAR(11), d.dtDocDate)) AS DATE)
                                                         AS [DocDate],

        d.sDocNo                                          AS [Doc. No.],   -- ✅ ADDED

        cf.cfValue3                                       AS [Material Requisition Date],
        dd.sValue1                                        AS BatchNo,
        l1.sName                                          AS [Location From],
        dcf.cfValue3                                      AS [Virtual Location],
        i.sCode                                           AS ItemCode,
        i.sName                                           AS ItemName,
        dd.dQty2                                          AS [Quantity],
        d.dTotal                                          AS [TxnTotal]

    FROM vTxnHdr AS d

    INNER JOIN (
        SELECT dd.*
        FROM vTxnHdr AS d1
        INNER JOIN TXNDET (NOLOCK) AS dd
            ON d1.lId = dd.lId
        WHERE d1.bDel = 0
          AND dd.bDel = 0
    ) AS dd
        ON d.lId = dd.lId

    LEFT JOIN ITMMST (NOLOCK) AS i
        ON dd.lItmId = i.lId

    LEFT JOIN DIMMST (NOLOCK) AS l1
        ON dd.lLocId = l1.lId

    -- Header CF (Material Requisition Date)
    LEFT JOIN (
        SELECT
            d.lId,
            MAX(CASE WHEN cf.lFieldNo = 3 THEN cf.sValue ELSE '' END) AS cfValue3
        FROM vTxnHdr AS d
        INNER JOIN TXNCF (NOLOCK) AS cf
            ON d.lId = cf.lId
        WHERE cf.lLine = 0
        GROUP BY d.lId
    ) AS cf
        ON d.lId = cf.lId

    -- Line CF (Virtual Location)
    LEFT JOIN (
        SELECT
            d.lId,
            cf.lLine,
            MAX(CASE WHEN cf.lFieldNo = 3 THEN cf.sValue ELSE '' END) AS cfValue3
        FROM vTxnHdr AS d
        INNER JOIN TXNCF (NOLOCK) AS cf
            ON d.lId = cf.lId
        WHERE cf.lLine > 0
        GROUP BY d.lId, cf.lLine
    ) AS dcf
        ON d.lId = dcf.lId
       AND dd.lLine = dcf.lLine
)

SELECT
    [DocDate],
    [Doc. No.],                      -- ✅ ADDED
    [Material Requisition Date],
    BatchNo,
    [Location From],
    [Virtual Location],
    ItemCode,
    ItemName,
    [Quantity],
    [TxnTotal]
FROM vView
ORDER BY RowNumber;
"""



def _safe_parse_date(val):
    """
    Convert SQL value (date/datetime/string) to a Python date
    for our DateField columns.
    """
    if val in (None, "", " "):
        return None

    if isinstance(val, date):
        return val
    if isinstance(val, datetime):
        return val.date()

    # Try a few common string formats
    for fmt in (
        "%Y-%m-%d",   # 2026-01-01
        "%d-%m-%Y",   # 01-01-2026
        "%d.%m.%Y",   # 01.01.2026
        "%d/%m/%Y",   # 01/01/2026
        "%d-%b-%Y",   # 01-Jan-2026   <-- your current format
        "%d-%B-%Y",   # 01-January-2026 (just in case)
    ):
        try:
            return datetime.strptime(str(val).strip(), fmt).date()
        except (ValueError, TypeError):
            continue

    # Fallback: leave as None if unparsable
    logger.warning("Could not parse date value %r", val)
    return None


def rebuild_material_issue_lines() -> int:
    logger.info("Starting Material Issue FULL refresh")

    with connections["readonly_db"].cursor() as cursor:
        cursor.execute(SQL_MATERIAL_ISSUE_LINES)
        rows = cursor.fetchall()

    logger.info("Fetched %s rows from readonly_db for MaterialIssueLine", len(rows))

    with transaction.atomic(using="default"):
        MaterialIssueLine.objects.using("default").all().delete()
        logger.info("Cleared existing material_issue_line records")

        objs = []
        for r in rows:
            # Order from SELECT:
            # 0 DocDate
            # 1 Doc. No.
            # 2 Material Requisition Date
            # 3 BatchNo
            # 4 Location From
            # 5 Virtual Location
            # 6 ItemCode
            # 7 ItemName
            # 8 Quantity
            # 9 TxnTotal

            objs.append(
                MaterialIssueLine(
                    doc_date=_safe_parse_date(r[0]),
                    doc_no=r[1],  # ✅ ADDED (Doc. No.)
                    material_requisition_date=_safe_parse_date(r[2]),
                    batch_no=r[3],
                    location_from=r[4],
                    virtual_location=r[5],
                    item_code=r[6],
                    item_name=r[7],
                    quantity=r[8],
                    txn_total=r[9],
                )
            )

        MaterialIssueLine.objects.using("default").bulk_create(objs, batch_size=500)

    logger.info("Inserted %s rows into material_issue_line (Material Issue)", len(objs))
    return len(objs)






SQL_LOCATION_STOCK_TRANSFER_CAPEX_LINES = r"""
WITH vTxnHdr AS (
    SELECT dt.sName, d.*
    FROM TXNTYP  (NOLOCK) AS dt
    INNER JOIN TXNHDR (NOLOCK) AS d
        ON dt.lTypId = d.lTypId
       AND dt.lTypId = 781
    WHERE d.lId > 0
      AND d.lCompId = 27
      AND d.bDel <> 1
      AND d.lClosed = 0
      AND d.dtDocDate >= 20250401
),
vDet AS (
    SELECT dd.*
    FROM vTxnHdr AS d
    INNER JOIN TXNDET (NOLOCK) AS dd
        ON d.lId = dd.lId
    WHERE dd.bDel <> -2
      AND dd.lClosed <> -2
),
vView AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY d.dtDocDate DESC, d.sDocNo DESC, dd.lLine
        ) AS RowNumber,

        d.sDocNo AS [Location Transfer Capex No],

        -- return date as DATE (better for Django DateField)
        CAST(CONVERT(DATETIME, CONVERT(VARCHAR(11), d.dtDocDate)) AS DATE)
            AS [Location Transfer Capex Date],

        loc1.sName AS [From Location],
        loc2.sName AS [To Location],

        i.sCode AS [Item Code],
        i.sName AS [Item Name],

        dd.sValue1 AS [Batch No],

        CONVERT(DECIMAL(18,3), dd.dQty2) AS [Transfer Quantity],
        CONVERT(DECIMAL(18,3), dd.dStkVal * -1) AS [Issue Value],

        cf.sValue AS [Virtual Location]

    FROM vTxnHdr AS d
    INNER JOIN vDet AS dd
        ON d.lId = dd.lId

    LEFT JOIN ITMMST (NOLOCK) AS i
        ON dd.lItmId = i.lId

    LEFT JOIN DIMMST (NOLOCK) AS loc1
        ON loc1.lId = d.lLocId1

    LEFT JOIN DIMMST (NOLOCK) AS loc2
        ON loc2.lId = d.lLocId2

    LEFT JOIN TXNCF (NOLOCK) AS cf
        ON d.lId = cf.lId
       AND cf.lLine = 1
       AND cf.sName = 'Virtual Location'
)

SELECT
    [Location Transfer Capex No],
    [Location Transfer Capex Date],
    [From Location],
    [To Location],
    [Item Code],
    [Item Name],
    [Batch No],
    [Transfer Quantity],
    [Issue Value],
    [Virtual Location]
FROM vView
WHERE [Location Transfer Capex Date] BETWEEN '2025-04-01' AND '2026-03-06'
ORDER BY RowNumber;
"""





def rebuild_location_stock_transfer_capex_lines() -> int:
    """
    FULL REFRESH:
    - Deletes all rows from location_stock_transfer_capex on default DB
    - Re-loads from readonly_db using SQL_LOCATION_STOCK_TRANSFER_CAPEX_LINES
    """
    logger.info("Starting Location Stock Transfer CAPEX FULL refresh")

    # 1) Read from readonly_db
    with connections["readonly_db"].cursor() as cursor:
        cursor.execute(SQL_LOCATION_STOCK_TRANSFER_CAPEX_LINES)
        rows = cursor.fetchall()

    logger.info("Fetched %s rows from readonly_db for LocationStockTransferCapex", len(rows))

    # 2) Write into default DB
    with transaction.atomic(using="default"):
        LocationStockTransferCapex.objects.using("default").all().delete()
        logger.info("Cleared existing location_stock_transfer_capex records")

        objs = []
        for r in rows:
            # Order from SELECT:
            # 0 Location Transfer Capex No
            # 1 Location Transfer Capex Date (DATE)
            # 2 From Location
            # 3 To Location
            # 4 Item Code
            # 5 Item Name
            # 6 Batch No
            # 7 Transfer Quantity
            # 8 Issue Value
            # 9 Virtual Location
            objs.append(
                LocationStockTransferCapex(
                    location_transfer_capex_no=r[0],
                    location_transfer_capex_date=_safe_parse_date(r[1]),
                    location=r[2],              # ? changed from_location ? location
                    to_location=r[3],
                    item_code=r[4],
                    item_name=r[5],
                    batch_no=r[6],
                    transfer_quantity=r[7] or 0,
                    issue_value=r[8] or 0,
                    virtual_location=r[9],
                )
            )

        LocationStockTransferCapex.objects.using("default").bulk_create(objs, batch_size=500)

    logger.info("Inserted %s rows into location_stock_transfer_capex", len(objs))
    return len(objs)