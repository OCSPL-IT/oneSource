from datetime import date, datetime
from decimal import Decimal
from collections import defaultdict
import logging
from ACCOUNTS.CASHFLOW.models import ManualPayableEntry, PayablePartyExtension  # 👈 add this
from django.contrib.auth.decorators import login_required
from django.db import connections, DatabaseError
from django.shortcuts import render
from django.contrib import messages
from datetime import date, timedelta
from decimal import Decimal
from django.db.models import Q

logger = logging.getLogger("custom_logger")


from datetime import date, timedelta

def _week_label_for_date(d: date) -> str | None:
    """
    NEXT-week bucketing:
    label like: 'Wk-40. 29-Dec-2025'
    Fiscal year = Apr–Mar, week starts Monday,
    but bucket is NEXT Monday (current Monday + 7).
    """
    if not d:
        return None

    fiscal_start_year = d.year if d.month >= 4 else d.year - 1
    fiscal_start = date(fiscal_start_year, 4, 1)

    def monday_of(dt: date) -> date:
        return dt - timedelta(days=dt.weekday())  # Monday

    # ✅ shift into next week bucket
    week_start = monday_of(d) + timedelta(days=7)
    fiscal_start_monday = monday_of(fiscal_start)

    week_no = ((week_start - fiscal_start_monday).days // 7) + 1
    return f"Wk-{week_no:02d}. {week_start.strftime('%d-%b-%Y')}"



COMPANY_GROUPS = {
    "specialities": {
        "label": "OC Specialities Private Limited",
        "ids": [4, 7, 27, 28, 40, 93],
    },
    "chemicals": {
        "label": "OC Specialities Chemicals Private Limited",
        "ids": [8, 9, 25, 26],
    },
}

# --- EXCLUDED PARTIES (remove from main tables + weekly cashflow, show separately) ---
EXCLUDED_PAY_PARTIES = {
    "DEEDY CHEMICALS PVT. LTD. (CR)",
    "FREESIA CHEMICALS (CR)",
    "HINDUSTHAN CHEMICALS COMPANY (CR)",
    "REMEDIUM LIFECARE LTD. (CR)",
    "ZEOLITES AND ALLIED PRODUCTS PVT. LTD. (CR)",
}

EXCLUDED_RECV_PARTIES = {
    "DEEDY CHEMICALS PVT. LTD.",
    "FREESIA CHEMICALS",
    "HINDUSTHAN CHEMICAL COMPANY",
    "REMEDIUM LIFECARE LTD.",
    "ZEOLITES AND ALLIED PRODUCTS PVT. LTD.",
    "ARISTA CHEMICALS LLP",
}

def _norm_party_name(x: str) -> str:
    # normalize spacing + case so matching is stable
    return " ".join((x or "").strip().upper().split())



# ==============================================================================
# PAYABLES QUERY
# ==============================================================================

SQL_PAYABLES_CASHFLOW = r"""
SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

DECLARE @decimal       DECIMAL(21,18);
DECLARE @AsOfDate      DATE = CAST(GETDATE() AS DATE);

DECLARE @FromDateInt   INT  = {FROM_DATE_INT};  -- will be injected from Django
DECLARE @ToDateInt     INT  = {TO_DATE_INT};    -- will be injected from Django


DECLARE @BaseCurRate   DECIMAL(18,6);

-- Decimal precision from company 27
SELECT @decimal = cm.lDigDec
FROM CMPNY c
INNER JOIN CURMST cm ON c.lCurrId = cm.lId
WHERE c.lId = 27;

-- Base currency conversion (company currency -> base currency)
SELECT TOP (1) @BaseCurRate = cd.dCurrCnv
FROM CURDET cd
WHERE cd.lId = 1
  AND cd.lCurrId = 0
ORDER BY cd.dtWefDate DESC;

IF @BaseCurRate IS NULL SET @BaseCurRate = 1;

--------------------------------------------------------------------------------
-- COMPANY FILTER
--------------------------------------------------------------------------------
DECLARE @CompanyIds TABLE (lCompId INT);
INSERT INTO @CompanyIds(lCompId)
VALUES (4),(27),(28),(40),(93),(7),(8),(9),(25),(26);

--------------------------------------------------------------------------------
-- 1) #vACC (NO bDocAdj)
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS #vACC;

SELECT
    a.lId,
    a.sCode,
    a.sName,
    act.cAccTyp,
    coa.sName AS GroupName,

    ISNULL(bus.dCreditLimit, 0) AS dCreditLimit,
    ISNULL(bus.dCreditDay,   0) AS dCreditDay,

    act.cLdgTyp,

    ISNULL(
        CASE
            WHEN addr.lMSMETyp = 1 THEN 'Micro'
            WHEN addr.lMSMETyp = 2 THEN 'Small'
            WHEN addr.lMSMETyp = 3 THEN 'Medium'
            ELSE ''
        END, ''
    ) AS sMSME,

    ISNULL(COALESCE(cnt.sName,   addr.sName),   '') AS ConName,
    ISNULL(COALESCE(cnt.sMobile, addr.sMobile), '') AS ConMobile
INTO #vACC
FROM ACCMST a
INNER JOIN ACCTYP act ON act.lTypId = a.lTypId
INNER JOIN COAMST coa ON coa.lId = a.lCoaId

OUTER APPLY (
    SELECT TOP (1)
        b.lId,
        b.dCreditLimit,
        b.dCreditDay
    FROM BUSMST b
    WHERE b.lAccId = a.lId
      AND b.bDel   = 0
    ORDER BY b.lId DESC
) bus

OUTER APPLY (
    SELECT TOP (1)
        ad.sName,
        ad.sMobile,
        ad.lMSMETyp
    FROM BUSADD ad
    WHERE bus.lId IS NOT NULL
      AND ad.lId = bus.lId
    ORDER BY ad.lId DESC
) addr

OUTER APPLY (
    SELECT TOP (1)
        bc.sName,
        bc.sMobile
    FROM BUSCNT bc
    WHERE bus.lId IS NOT NULL
      AND bc.lId = bus.lId
    ORDER BY bc.lId DESC
) cnt

WHERE act.cLdgTyp = 'S';  -- Suppliers only

CREATE CLUSTERED INDEX IX_vACC_lId ON #vACC(lId);

--------------------------------------------------------------------------------
-- 2) #TXNACC using TXNTYP flags (NO bDocAdj)
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS #TXNACC;

;WITH vTxnHdr AS (
    -- Normal TxnAcc rows (no project/profit-center split)
    SELECT
        d.lTypId,
        dt.sName,
        dt.lFinTyp,
        d.lId,
        d.lClosed,
        d.bDel,
        d.dtDueDate,
        d.dtDocDate,
        d.dTotal,
        d.sPrefix,
        d.lDocNo,
        d.sExtNo,
        d.sDocNo,
        d.lCurrId,
        d.lPayTrmId,
        d.dCurrCnv AS dCurrCnv,
        CASE WHEN dt.bEmpDet > 0 THEN da.lEmpId ELSE d.lEmpId END AS lEmpId,
        da.lLine,
        0 AS lSubLine,
        da.lAccId,
        da.lAstId,
        d.lCompId,
        da.lPrjId,
        da.lDimId,
        d.sNarr AS sNarr1,
        da.sNarr,
        da.dAmtDr,
        da.dAmtCr,
        da.dOtstndAmt,
        da.dRate,
        d.lLocId
    FROM TXNTYP dt
    INNER JOIN TXNHDR d
        ON dt.lTypId = d.lTypId
       AND dt.lFinTyp < 2
       AND dt.lFinTyp NOT IN (-16,-18)
       AND (dt.bComp = 0 AND dt.bPrjDet = 0 AND dt.bProfitCenter = 0)
    INNER JOIN TXNACC da
        ON d.lId = da.lId
       AND da.cFlag = 'A'
       AND da.bDel  = 0
    INNER JOIN #vACC a
        ON da.lAccId = a.lId
    WHERE d.bDel = 0
      AND d.lClosed <= 0
      AND d.lCompId IN (SELECT lCompId FROM @CompanyIds)
      AND d.dtDueDate BETWEEN @FromDateInt AND @ToDateInt
      AND d.lTypId NOT IN (24,26,27,32,338,340,345,498,585,589,592,945,957,1008,1106,1113,1122,1152,1207)

    UNION ALL

    -- Project / Profit-center level TxnAccSub rows (cTyp='P')
    SELECT
        d.lTypId,
        dt.sName,
        dt.lFinTyp,
        d.lId,
        d.lClosed,
        d.bDel,
        d.dtDueDate,
        d.dtDocDate,
        d.dTotal,
        d.sPrefix,
        d.lDocNo,
        d.sExtNo,
        d.sDocNo,
        d.lCurrId,
        d.lPayTrmId,
        d.dCurrCnv AS dCurrCnv,
        CASE WHEN dt.bEmpDet > 0 THEN da.lEmpId ELSE d.lEmpId END AS lEmpId,
        ds.lLine,
        ds.lSubLine,
        da.lAccId,
        da.lAstId,
        ds.lCompId,
        ds.lPrjId,
        ds.lDimId,
        d.sNarr AS sNarr1,
        da.sNarr,
        ds.dAmtDr,
        ds.dAmtCr,
        ds.dOtstndAmt,
        da.dRate,
        d.lLocId
    FROM TXNTYP dt
    INNER JOIN TXNHDR d
        ON dt.lTypId = d.lTypId
       AND dt.lFinTyp < 2
       AND dt.lFinTyp NOT IN (-16,-18)
       AND NOT (dt.bComp = 0 AND dt.bPrjDet = 0 AND dt.bProfitCenter = 0)
    INNER JOIN TXNACC da
        ON d.lId = da.lId
       AND da.cFlag = 'A'
       AND da.bDel  = 0
    INNER JOIN TXNACCSUB ds
        ON da.lId = ds.lId
       AND da.lLine = ds.lLine
       AND ds.cTyp  = 'P'
       AND ds.cFlag = 'A'
       AND ds.bDel  = 0
    INNER JOIN #vACC a
        ON da.lAccId = a.lId
    WHERE d.bDel = 0
      AND d.lClosed <= 0
      AND d.lCompId IN (SELECT lCompId FROM @CompanyIds)
      AND d.dtDueDate BETWEEN @FromDateInt AND @ToDateInt
      AND d.lTypId NOT IN (24,26,27,32,338,340,345,498,585,589,592,945,957,1008,1106,1113,1122,1152,1207)
)
SELECT *
INTO #TXNACC
FROM vTxnHdr
WHERE (dAmtDr + dAmtCr) > 0;

CREATE CLUSTERED INDEX IX_TXNACC_lId_lLine ON #TXNACC (lId, lLine);
CREATE INDEX IX_TXNACC_lAccId              ON #TXNACC (lAccId);

--------------------------------------------------------------------------------
-- 3) #TXNHDR (one row per document) + Doc No (sDocNo)
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS #TXNHDR;

SELECT DISTINCT
    x.lTypId,
    cu.sName AS CurType,
    x.lFinTyp,
    x.sName,
    x.dCurrCnv,
    c1.sName AS CurType1,
    CASE
        WHEN x.lCurrId <> 0 THEN ISNULL(@BaseCurRate, 1)
        ELSE ISNULL(x.dCurrCnv, 1)
    END AS CurRate,
    x.lId,
    x.sPrefix,
    x.lDocNo AS TrnNo,
    x.sExtNo,
    x.sDocNo AS sDocument,          -- ✅ Doc No
    CONVERT(VARCHAR(20), x.dtDocDate) AS mDocDate,
    CONVERT(VARCHAR(20), x.dtDueDate) AS mDueDate,
    x.dtDueDate,
    x.dtDocDate,
    x.dTotal,
    x.bDel,
    x.lClosed,
    pt.sName AS PayTrm,
    ISNULL(pt.dValue, 0) AS PayTrmDays,
    cm.lId AS lCompId,
    cm.sRemarks AS CompanyName,
    '' AS Narration
INTO #TXNHDR
FROM #TXNACC x
INNER JOIN CURMST cu  ON x.lCurrId = cu.lId
INNER JOIN CMPNY cm   ON x.lCompId = cm.lId
INNER JOIN CURMST c1  ON cm.lCurrId = c1.lId
INNER JOIN ACCMST a   ON x.lAccId = a.lId
INNER JOIN ACCTYP act ON act.lTypId = a.lTypId
LEFT JOIN PayTrm pt   ON x.lPayTrmId = pt.lId
WHERE x.bDel = 0
  AND x.lCompId IN (SELECT lCompId FROM @CompanyIds)
  AND act.cLdgTyp = 'S';

CREATE CLUSTERED INDEX IX_TXNHDR_lId ON #TXNHDR (lId);

--------------------------------------------------------------------------------
-- 4) Custom fields + Ref + LedgerName
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS #vTXNCF;
DROP TABLE IF EXISTS #vTXNCF1;
DROP TABLE IF EXISTS #Ledger;

SELECT
    d.lId,
    ISNULL(MAX(CASE WHEN cf.sName = 'Item Name'           THEN cf.sValue ELSE '' END), '') AS [Item Name],
    ISNULL(MAX(CASE WHEN cf.sName = 'Purchase Order Date' THEN cf.sValue ELSE '' END), '') AS [Purchase Order Date],
    ISNULL(MAX(CASE WHEN cf.sName = 'Purchase Order No'   THEN cf.sValue ELSE '' END), '') AS [Purchase Order No],
    ISNULL(MAX(CASE WHEN cf.sName = 'Bill of Entry Date'  THEN cf.sValue ELSE '' END), '') AS [Bill of Entry Date],
    ISNULL(MAX(CASE WHEN cf.sName = 'Bill of Entry No'    THEN cf.sValue ELSE '' END), '') AS [Bill of Entry No],
    ISNULL(MAX(CASE WHEN cf.sName = 'Bank Details'        THEN cf.sValue ELSE '' END), '') AS [Bank Details]
INTO #vTXNCF
FROM #TXNHDR d
LEFT JOIN TXNCF cf
    ON d.lId = cf.lId
   AND cf.lLine = 0
GROUP BY d.lId;

CREATE CLUSTERED INDEX IX_vTXNCF_lId ON #vTXNCF (lId);

;WITH vTxnCf1 AS (
    SELECT
        d.lId,
        MAX(CASE WHEN cf.sName = 'Reference No.' OR cf.sName = 'Invoice No.' THEN cf.sValue ELSE '' END) AS RefNo,
        MAX(CASE WHEN cf.sName = 'Reference Dt.' OR cf.sName = 'Invoice Dt.' THEN cf.sValue ELSE '' END) AS RefDate
    FROM #TXNHDR d
    INNER JOIN TXNCF cf ON d.lId = cf.lId
    WHERE cf.lLine = 0
      AND ISNULL(cf.sValue,'') <> ''
      AND (
            cf.sName = 'Reference Dt.'
         OR cf.sName = 'Invoice Dt.'
         OR cf.sName = 'Reference No.'
         OR cf.sName = 'Invoice No.'
      )
    GROUP BY d.lId
)
SELECT *
INTO #vTXNCF1
FROM vTxnCf1;

CREATE CLUSTERED INDEX IX_vTXNCF1_lId ON #vTXNCF1 (lId);

SELECT
    td.lId,
    MAX(a.sName) AS LedgerName
INTO #Ledger
FROM TXNDET td
INNER JOIN #TXNHDR h ON td.lId = h.lId
INNER JOIN ACCMST a  ON td.lAccId = a.lId
GROUP BY td.lId;

CREATE CLUSTERED INDEX IX_Ledger_lId ON #Ledger (lId);

--------------------------------------------------------------------------------
-- 5) Settlement + Summary -> FINAL OUTPUT (Nature + all columns)
--------------------------------------------------------------------------------
;WITH vTxnAcc AS (
    SELECT
        x.lId,
        x.lLine,
        x.lAccId,
        a.GroupName,
        a.cLdgTyp,
        x.dAmtDr,
        x.dAmtCr,
        CASE WHEN h.dtDueDate IS NULL OR h.dtDueDate = 0
             THEN NULL
             ELSE CONVERT(date, CONVERT(char(8), h.dtDueDate)) END AS DueDateDate,
        CASE WHEN h.dtDueDate IS NULL OR h.dtDueDate = 0
             THEN 0
             ELSE DATEDIFF(DAY, CONVERT(date, CONVERT(char(8), h.dtDueDate)), @AsOfDate) END AS OverdueDays
    FROM #TXNACC x
    INNER JOIN #TXNHDR h ON h.lId = x.lId
    INNER JOIN #vACC a   ON a.lId = x.lAccId
),
vTxnSett AS (
    SELECT
        d.lId,
        d.lLine,
        CASE
            WHEN MAX(d.dAmtDr) > 0
                THEN ISNULL(SUM(ISNULL(fs.dAdjAmtDr,0) - ISNULL(fs.dAdjAmtCr,0)),0)
            ELSE ISNULL(SUM(ISNULL(fs.dAdjAmtCr,0) - ISNULL(fs.dAdjAmtDr,0)),0)
        END AS dAmt
    FROM vTxnAcc d
    LEFT JOIN TXNFINSET fs
        ON fs.lId   = d.lId
       AND fs.lLine = d.lLine
    GROUP BY d.lId, d.lLine
),
vSumm AS (
    SELECT
        h.lId,
        h.CompanyName,
        h.lCompId,
        a.sCode AS AccCode,
        a.sName AS AccName,
        a.GroupName,
        h.sName AS TrnTyp,
        h.TrnNo,
        h.sDocument,
        CONVERT(VARCHAR, CONVERT(DATETIME, CONVERT(VARCHAR(10), h.mDocDate)), 106) AS TrnDate,
        ISNULL(cf1.RefNo,'')   AS RefNo,
        ISNULL(cf1.RefDate,'') AS RefDate,
        h.PayTrm,
        h.PayTrmDays,
        v.DueDateDate,
        v.OverdueDays,
        h.CurType1,

        ISNULL(SUM(v.dAmtDr + v.dAmtCr),0) AS BillAmt,
        ABS(ISNULL(SUM(s.dAmt),0))         AS PaidAmt,
        (ISNULL(SUM(v.dAmtDr + v.dAmtCr),0) - ISNULL(SUM(s.dAmt),0)) AS BillOSAmt,
        0 AS UnAdjAmt,
        (ISNULL(SUM(v.dAmtDr + v.dAmtCr),0) - ISNULL(SUM(s.dAmt),0)) AS OsAmt,

        a.dCreditLimit AS CrLimit,
        '' AS Narration,
        CASE WHEN SUM(v.dAmtDr) > SUM(v.dAmtCr) THEN 'Dr' ELSE 'Cr' END AS cSgn
    FROM #TXNHDR h
    INNER JOIN vTxnAcc v   ON v.lId = h.lId
    INNER JOIN #vACC a     ON a.lId = v.lAccId
    INNER JOIN vTxnSett s  ON s.lId = v.lId AND s.lLine = v.lLine
    LEFT JOIN #vTXNCF1 cf1 ON cf1.lId = h.lId
    WHERE (ISNULL(v.dAmtDr + v.dAmtCr,0) - ISNULL(s.dAmt,0)) <> 0
    GROUP BY
        h.lId,h.CompanyName,h.lCompId,
        a.sCode,a.sName,a.GroupName,
        h.sName,h.TrnNo,h.sDocument,h.mDocDate,
        cf1.RefNo,cf1.RefDate,
        h.PayTrm,h.PayTrmDays,
        v.DueDateDate,v.OverdueDays,
        h.CurType1,
        a.dCreditLimit
),
vSumm2 AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY AccName, AccCode, TrnDate, lId) AS myRow,
        *
    FROM vSumm
    WHERE ROUND(CASE WHEN ABS(OsAmt) < 0.001 THEN 0 ELSE OsAmt END, @decimal) <> 0
),
FinalRows AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY s.AccCode ORDER BY s.myRow) AS TmpRow,
        s.myRow,
        s.CompanyName,
        s.lCompId,
        s.AccCode,
        s.AccName,
        L.LedgerName,
        s.GroupName,
        s.TrnTyp,
        s.TrnNo,
        s.sDocument,
        s.TrnDate,

        cf.[Item Name],
        cf.[Purchase Order Date],
        cf.[Purchase Order No],
        cf.[Bill of Entry Date],
        cf.[Bill of Entry No],
        cf.[Bank Details],

        s.RefNo,
        s.RefDate,
        s.PayTrm,
        s.PayTrmDays,
        s.DueDateDate,
        CASE WHEN s.DueDateDate IS NULL THEN NULL ELSE CONVERT(CHAR(11), s.DueDateDate, 106) END AS OverdueDateStr,
        s.OverdueDays,

        s.CurType1,
        1 AS [Conversion Rate],

        s.BillAmt,
        s.PaidAmt,
        s.BillOSAmt,
        s.CrLimit,
        0 AS [Credit Limit Days],
        s.UnAdjAmt,
        s.OsAmt,

        s.Narration,
        s.cSgn,

        COUNT(*) OVER (PARTITION BY s.AccCode) AS lRecordCount
    FROM vSumm2 s
    LEFT JOIN #vTXNCF  cf ON cf.lId = s.lId
    LEFT JOIN #Ledger   L ON L.lId  = s.lId
)
SELECT
    TmpRow,
    myRow,
    CompanyName AS [Company Name],
    lCompId     AS lCompId,
    AccCode     AS [Party Code],
    AccName     AS [Party Name],
    LedgerName  AS [Account Name],

    ----------------------------------------------------------------
    -- Nature (same logic as your reference)
    ----------------------------------------------------------------
    CASE
        -- Direct Expense Voucher → Nature from Ledger mapping
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'REPAIRS & MAINTENANCE - P & M'                        THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'FREIGHT INWARD-DOMESTIC'                               THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'C&F EXPENSES-IMPORT'                                  THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'REPAIRS & MAINTENANCE - CIVIL'                        THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'STORAGE EXPENSES'                                     THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'POSTAGE AND COURIER'                                  THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'RENT FOR TANK'                                        THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'R & D EXPENSES'                                       THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'SCIENTIFIC & TECHNICAL SERVICE CHARGES'               THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'FREIGHT OUTWARD - DOMESTIC SALES'                     THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'FREIGHT INWARD-IMPORT'                                THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'REPAIRS AND MAINTENANCE FOR VEHICLES (SERVICES)'      THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'OCEAN FREIGHT FOR EXPORT'                             THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'C&F EXPENSES FOR EXPORT'                              THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'REPAIRS AND MAINTENANCE FOR VEHICLES (GOODS)'         THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'SUBSCRIPTION CHARGES'                                 THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'OFFICE EXPENSES'                                      THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'MOBILES'                                              THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'LOADING AND UNLOADING EXPENSES (PURCHASES)'           THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'OFFICE RENOVATION'                                    THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'PROFESSIONAL & TECHNICAL SERVICE CHARGES'             THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'TECHNICAL KNOW-HOW'                                   THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'LICENSE, REGISTRATION AND CERTIFICATION FEES'         THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'ETP OPERATION EXPENSES'                               THEN '07 EXPENSES-ETP RELATED'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'BANK CHARGES FOR EXPORT DOCUMENTS'                    THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'BANK CHARGES FOR FOREX CONVERSION'                    THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'BANK CHARGES FOR IMPORT DOCUMENTS'                    THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'BANK CHARGES FOR BUYER''S CREDIT'                     THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'BANK CHARGES (OTHERS)'                                THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'BANK LOAN PROCESSING FEES'                            THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'BANK CHARGES FOR FORWARD CONTRACT BOOKING AND CANCELLATION' THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'BANK CHARGES FOR LETTER OF CREDIT'                    THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'INSURANCE EMPLOYEE ACCIDENT POLICY'                   THEN 'INSURANCE'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'TRAVELING EXPENSES - DOMESTIC'                        THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'SALES PROMOTION EXPESNSES'                            THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'RENT FOR GODOWN'                                      THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'FREIGHT OUTWARD - EXPORT SALES'                       THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'LOADING AND UNLOADING EXPENSES - SALES'               THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'SAFETY EXPENSES'                                      THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'COMMUNICATION EXPENSES'                               THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'WATER CHARGES'                                        THEN 'WATER CHARGES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'ELECTRICITY EXPENSES (FACTORY)'                       THEN 'ELECTRICITY CHARGES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'COMMISSION - DOMESTIC SALES'                          THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'VEHICLE HIRE CHARGES'                                 THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'PRINTING & STATIONERY'                                THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'REPAIRS & MAINTENANCE - ELECTRICAL'                   THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'LEGAL SERVICE CHARGES'                                THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'QC EXPENSES'                                          THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'INSURANCE FOR VEHICLES'                               THEN 'INSURANCE'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'HOUSEKEEPING EXPENSES'                                THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'INSURANCE BURGLARY'                                   THEN 'INSURANCE'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'TRAINING EXPENSES'                                    THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'COMMISSION - FOREIGN'                                 THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'ADVERTISEMENT & PUBLICITY EXPENSES'                   THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'GUEST HOUSE RENT'                                     THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'MEDICAL EXPENSES'                                     THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'REPAIRS & MAINTENANCE - ADMIN'                        THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'STORAGE INCOME'                                      THEN '06 EXPENSES'
        WHEN TrnTyp = 'Direct Expense Voucher' AND LedgerName = 'XEROX MACHINE EXPENSES'                                      THEN '06 EXPENSES'

        -- Fallback for ANY other Direct Expense Voucher ledger
        WHEN TrnTyp = 'Direct Expense Voucher' THEN LedgerName

        -- Other transaction types mapping
        WHEN TrnTyp = 'Direct Expense Voucher Service Capex' THEN '09 CAPEX'
        WHEN TrnTyp = 'Direct Expense Voucher Service MOC'   THEN '05 ENGINEERING-SERVICES'
        WHEN TrnTyp = 'Domestic Material Purchase Invoice (RMPM)' THEN '01 RAW MATERIAL'
        WHEN TrnTyp = 'Domestic Purchase Invoice Admin'      THEN '06 EXPENSES'
        WHEN TrnTyp = 'Domestic Purchase Invoice Capex'      THEN '09 CAPEX'
        WHEN TrnTyp = 'Domestic Purchase Invoice Engineering'THEN '04 ENGINEERING-MATERIAL'
        WHEN TrnTyp = 'Domestic Purchase Invoice Fuel'       THEN '03 FUEL SUPPLIES'
        WHEN TrnTyp = 'Domestic Purchase Invoice Key RM'     THEN '01 RAW MATERIAL'
        WHEN TrnTyp = 'Domestic Purchase Invoice Lab Chemicals-Equipment' THEN '06 EXPENSES'
        WHEN TrnTyp = 'Domestic Purchase Invoice R&D'        THEN '06 EXPENSES'
        WHEN TrnTyp = 'Domestic Purchase Invoice Safety'     THEN '06 EXPENSES'
        WHEN TrnTyp = 'Domestic Purchase Invoice Trading'    THEN '01 RAW MATERIAL'
        WHEN TrnTyp = 'Import Purchase Invoice Key RM'       THEN '01 RAW MATERIAL'
        WHEN TrnTyp = 'Import Purchase Invoice Trading'      THEN '10 IMPORT'
        WHEN TrnTyp = 'Import Purchase Invoice RMPM'      THEN '10 IMPORT'
        WHEN TrnTyp = 'Job Work Invoice'                     THEN '02 JOB WORK'
        WHEN TrnTyp = 'Journal Voucher'                      THEN '08 EXPENSES-LOGISTICS'
        ELSE ''
    END AS [Nature],

    GroupName,
    TrnTyp      AS [Trans Type],
    TrnNo       AS [Doc No.],
    sDocument   AS [Trans No],
    TrnDate     AS [Trans Date],

    RefNo       AS [Ref_No],
    RefDate     AS [Ref_Date],

    [Item Name],
    [Purchase Order Date],
    [Purchase Order No],
    [Bill of Entry Date],
    [Bill of Entry No],
    [Bank Details],

    CurType1    AS [Currency Code],
    [Conversion Rate],

    CONVERT(DECIMAL(21,2), BillAmt)    AS [Bill Amt],
    CONVERT(DECIMAL(21,2), PaidAmt)    AS [Paid Amt],
    CONVERT(DECIMAL(21,2), BillOSAmt)  AS [Bill OS Amt],

    CrLimit     AS [Credit Limit],
    [Credit Limit Days],

    PayTrm,
    PayTrmDays,

    DueDateDate AS [Due Date],
    OverdueDateStr AS [Overdue Date],
    OverdueDays AS [Overdue Days],

    CONVERT(DECIMAL(21,2), UnAdjAmt)   AS [Unadjustment Amt],
    CONVERT(DECIMAL(21,2), OsAmt)      AS [Outstanding Amt],

    Narration,
    cSgn,

    -- Fiscal Week No based on Due Date (Monday start, FY Apr–Mar)
    CASE
      WHEN DueDateDate IS NULL THEN NULL
      ELSE
        'Wk-' + RIGHT('0' + CAST(DATEDIFF(WEEK, W2.FiscalStartMonday, W1.WeekStart) + 1 AS VARCHAR(2)),2)
        + '. ' + REPLACE(CONVERT(CHAR(11), W1.WeekStart, 106), ' ', '-')
    END AS [Week No],

    lRecordCount
FROM FinalRows
CROSS APPLY (
    SELECT
        WeekStart =
    DATEADD(DAY, 7,
        DATEADD(DAY, -((DATEPART(WEEKDAY, DueDateDate) + 5) % 7), DueDateDate)
    ),

FiscalStart =
    CASE
      WHEN MONTH(
            DATEADD(DAY, 7,
              DATEADD(DAY, -((DATEPART(WEEKDAY, DueDateDate) + 5) % 7), DueDateDate)
            )
          ) >= 4
        THEN DATEFROMPARTS(
              YEAR(
                DATEADD(DAY, 7,
                  DATEADD(DAY, -((DATEPART(WEEKDAY, DueDateDate) + 5) % 7), DueDateDate)
                )
              ),
              4, 1
        )
      ELSE DATEFROMPARTS(
              YEAR(
                DATEADD(DAY, 7,
                  DATEADD(DAY, -((DATEPART(WEEKDAY, DueDateDate) + 5) % 7), DueDateDate)
                )
              ) - 1,
              4, 1
        )
    END

) W1
CROSS APPLY (
    SELECT
        FiscalStartMonday =
            CASE WHEN W1.FiscalStart IS NULL THEN NULL
                 ELSE DATEADD(DAY, -((DATEPART(WEEKDAY, W1.FiscalStart) + 5) % 7), W1.FiscalStart)
            END
) W2
WHERE AccName NOT IN (
    'OC SPECIALITIES CHEMICALS PVT. LTD. (CR)',
    'OC SPECIALITIES CHEMICALS PVT. LTD. (IMP)',
    'OC SPECIALITIES PVT. LTD. (CR)'
)
ORDER BY myRow, TmpRow;

--------------------------------------------------------------------------------
-- Cleanup
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS #Ledger;
DROP TABLE IF EXISTS #vTXNCF1;
DROP TABLE IF EXISTS #vTXNCF;
DROP TABLE IF EXISTS #TXNHDR;
DROP TABLE IF EXISTS #TXNACC;
DROP TABLE IF EXISTS #vACC;

SET ANSI_WARNINGS ON;

"""


# ==============================================================================
# RECEIVABLES QUERY  (parameterised + lCompId in final SELECT)
# ==============================================================================

SQL_RECEIVABLES_CASHFLOW = r"""
SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

DECLARE @AsOfDateInt INT = CONVERT(INT, CONVERT(CHAR(8), GETDATE(), 112));  -- yyyymmdd
DECLARE @FromDateInt INT = 20250401;  -- injected from Django 
DECLARE @ToDateInt   INT = 20280401;  -- injected from Django

--------------------------------------------------------------------------------
-- 0) Cleanup old temp objects (SQL 2012/2014 compatible)
--------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#CompanyIds') IS NOT NULL DROP TABLE #CompanyIds;
IF OBJECT_ID('tempdb..#TypIds')     IS NOT NULL DROP TABLE #TypIds;
IF OBJECT_ID('tempdb..#TXNACC')     IS NOT NULL DROP TABLE #TXNACC;
IF OBJECT_ID('tempdb..#TXNHDR')     IS NOT NULL DROP TABLE #TXNHDR;
IF OBJECT_ID('tempdb..#vTXNCF')     IS NOT NULL DROP TABLE #vTXNCF;
IF OBJECT_ID('tempdb..#vACC')       IS NOT NULL DROP TABLE #vACC;
IF OBJECT_ID('tempdb..#temp')       IS NOT NULL DROP TABLE #temp;

--------------------------------------------------------------------------------
-- 0.1) Small filter tables
--------------------------------------------------------------------------------
CREATE TABLE #CompanyIds (lCompId INT NOT NULL PRIMARY KEY);
INSERT INTO #CompanyIds(lCompId)
VALUES (3),(4),(27),(28),(40),(93),(7),(8),(9),(25),(26);

CREATE TABLE #TypIds (lTypId INT NOT NULL PRIMARY KEY);
INSERT INTO #TypIds (lTypId)
VALUES
 (654),(939),(990),(499),(341),(498),(828),(940),(339),(824),(826);

--------------------------------------------------------------------------------
-- 1) #TXNACC : filtered doc+line rows
--------------------------------------------------------------------------------
;WITH BaseDocs AS (
    SELECT
        d.lId, d.lTypId, d.lCompId,
        d.dtDocDate, d.dtDueDate,
        d.dTotal, d.bDel, d.lClosed,
        d.sPrefix, d.lDocNo, d.sExtNo, d.sDocNo,
        d.lCurrId, d.lPayTrmId, d.dCurrCnv,
        d.lEmpId,
        d.lLocId,
        d.sNarr,
        dt.sName     AS TrnTypName,
        dt.lFinTyp,
        dt.bEmpDet,
        dt.bComp, dt.bPrjDet, dt.bProfitCenter
    FROM TXNHDR d
    INNER JOIN #CompanyIds c ON c.lCompId = d.lCompId
    INNER JOIN #TypIds     t ON t.lTypId  = d.lTypId
    INNER JOIN TXNTYP dt      ON dt.lTypId = d.lTypId
    WHERE d.bDel = 0
      AND d.lClosed <= 0
      AND d.dtDueDate BETWEEN @FromDateInt AND @ToDateInt
),
Lines AS (
    SELECT
        b.lTypId,
        b.TrnTypName AS sName,
        b.lFinTyp,
        b.lId,
        b.lClosed,
        b.bDel,
        b.dtDueDate,
        b.dtDocDate,
        b.dTotal,
        b.sPrefix,
        b.lDocNo,
        b.sExtNo,
        b.sDocNo,
        b.lCurrId,
        b.lPayTrmId,
        b.dCurrCnv,
        CASE WHEN b.bEmpDet > 0 THEN da.lEmpId ELSE b.lEmpId END AS lEmpId,
        da.lLine,
        0 AS lSubLine,
        da.lAccId,
        da.lAstId,
        b.lCompId,
        da.lPrjId,
        da.lDimId,
        b.sNarr AS sNarr1,
        da.sNarr,
        da.dAmtDr,
        da.dAmtCr,
        da.dOtstndAmt,
        da.dRate,
        b.lLocId AS HeaderLocId
    FROM BaseDocs b
    INNER JOIN TXNACC da
        ON da.lId = b.lId
       AND da.bDel = 0
    WHERE (b.bComp = 0 AND b.bPrjDet = 0 AND b.bProfitCenter = 0)

    UNION ALL

    SELECT
        b.lTypId,
        b.TrnTypName AS sName,
        b.lFinTyp,
        b.lId,
        b.lClosed,
        b.bDel,
        b.dtDueDate,
        b.dtDocDate,
        b.dTotal,
        b.sPrefix,
        b.lDocNo,
        b.sExtNo,
        b.sDocNo,
        b.lCurrId,
        b.lPayTrmId,
        b.dCurrCnv,
        CASE WHEN b.bEmpDet > 0 THEN da.lEmpId ELSE b.lEmpId END AS lEmpId,
        ds.lLine,
        ds.lSubLine,
        da.lAccId,
        da.lAstId,
        ds.lCompId,
        ds.lPrjId,
        ds.lDimId,
        b.sNarr AS sNarr1,
        da.sNarr,
        ds.dAmtDr,
        ds.dAmtCr,
        ds.dOtstndAmt,
        da.dRate,
        b.lLocId AS HeaderLocId
    FROM BaseDocs b
    INNER JOIN TXNACC da
        ON da.lId = b.lId
       AND da.bDel = 0
    INNER JOIN TXNACCSUB ds
        ON ds.lId = da.lId
       AND ds.lLine = da.lLine
       AND ds.cTyp = 'P'
       AND ds.bDel = 0
    WHERE NOT (b.bComp = 0 AND b.bPrjDet = 0 AND b.bProfitCenter = 0)
)
SELECT
    L.*,
    EffectiveLocId =
        CASE
            WHEN L.lFinTyp < 2 AND L.lFinTyp NOT IN (-1,-2)
                 THEN ISNULL(PartyLoc.PartyLocId, L.HeaderLocId)
            ELSE L.HeaderLocId
        END,
    GEoPrevId =
        CASE
            WHEN ISNULL(G.lPrevId,0) = 0 THEN ISNULL(G.lId,0) ELSE ISNULL(G.lPrevId,0)
        END
INTO #TXNACC
FROM Lines L
INNER JOIN ACCMST am ON am.lId = L.lAccId
INNER JOIN ACCTYP at ON at.lTypId = am.lTypId
OUTER APPLY (
    SELECT TOP (1) b.lId
    FROM BUSMST b
    WHERE b.lAccId = L.lAccId AND b.bDel = 0
    ORDER BY b.lId DESC
) BM
OUTER APPLY (
    SELECT TOP (1) ba.lLocId AS PartyLocId
    FROM BUSADD ba
    WHERE BM.lId IS NOT NULL
      AND ba.lId = BM.lId
      AND ba.bDefault = 1
    ORDER BY ba.lId DESC
) PartyLoc
LEFT JOIN GEOLOC G
    ON G.lId =
        CASE
            WHEN L.lFinTyp < 2 AND L.lFinTyp NOT IN (-1,-2)
                 THEN ISNULL(PartyLoc.PartyLocId, L.HeaderLocId)
            ELSE L.HeaderLocId
        END
WHERE at.cLdgTyp = 'C'
  AND (L.dAmtDr + L.dAmtCr) > 0;

CREATE CLUSTERED INDEX IX_TXNACC_IdLine ON #TXNACC(lId, lLine, lSubLine);
CREATE INDEX IX_TXNACC_Acc             ON #TXNACC(lAccId);
CREATE INDEX IX_TXNACC_DueCompTyp      ON #TXNACC(dtDueDate, lCompId, lTypId);

--------------------------------------------------------------------------------
-- 2) #TXNHDR (one row per document)
--------------------------------------------------------------------------------
;WITH OneRowPerDoc AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY lId ORDER BY lLine, lSubLine) AS rn
    FROM #TXNACC
)
SELECT
    d.lTypId,
    cu.sName AS CurType,
    d.lFinTyp,
    d.sName,
    d.dCurrCnv,
    c1.sName AS CurType1,
    ISNULL(CASE WHEN d.lCurrId <> 0 THEN cr.dCurrCnv ELSE d.dCurrCnv END, 1) AS CurRate,
    d.lId,
    d.sPrefix,
    d.lDocNo AS TrnNo,
    d.sExtNo,
    d.sDocNo AS sDocument,
    CONVERT(VARCHAR(20), d.dtDocDate) AS mDocDate,
    CONVERT(VARCHAR(20), d.dtDueDate) AS mDueDate,
    d.dtDueDate,
    d.dtDocDate,
    d.dTotal,
    d.bDel,
    d.lClosed,
    pt.sName AS PayTrm,
    ISNULL(pt.dValue,0) AS PayTrmDays,
    cm.lId AS lCompId,
    cm.sRemarks AS CompanyName,
    Geo.sName  AS Location,
    Geo1.sName AS Location1,
    d.sNarr1   AS Narration
INTO #TXNHDR
FROM OneRowPerDoc d
INNER JOIN CURMST cu   ON d.lCurrId = cu.lId
INNER JOIN CMPNY  cm   ON d.lCompId = cm.lId
INNER JOIN CURMST c1   ON cm.lCurrId = c1.lId
LEFT  JOIN GEOLOC Geo  ON d.EffectiveLocId = Geo.lId
LEFT  JOIN GEOLOC Geo1 ON d.GEoPrevId      = Geo1.lId
LEFT  JOIN PayTrm pt   ON d.lPayTrmId      = pt.lId
OUTER APPLY (
    SELECT TOP(1) cd.dCurrCnv
    FROM CURDET cd
    WHERE cd.lId = 1
      AND cd.lCurrId = 0
      AND cd.dtWefDate < d.dtDocDate
    ORDER BY cd.dtWefDate DESC
) cr
WHERE d.rn = 1;

CREATE CLUSTERED INDEX IX_TXNHDR_lId ON #TXNHDR(lId);

--------------------------------------------------------------------------------
-- 3) Header custom fields (Customer PO is the real Reference)
--------------------------------------------------------------------------------
SELECT
    d.lId,
    ISNULL(MAX(CASE WHEN cf.sName='Customer PO Date' THEN cf.sValue ELSE '' END),'') AS [Customer PO Date],
    ISNULL(MAX(CASE WHEN cf.sName='Customer PO No.'  THEN cf.sValue ELSE '' END),'') AS [Customer PO No.],
    ISNULL(MAX(CASE WHEN cf.sName='Item Name'        THEN cf.sValue ELSE '' END),'') AS [Item Name],
    ISNULL(MAX(CASE WHEN cf.sName='Destination'      THEN cf.sValue ELSE '' END),'') AS [Destination],
    ISNULL(MAX(CASE WHEN cf.sName='Bank Details'     THEN cf.sValue ELSE '' END),'') AS [Bank Details]
INTO #vTXNCF
FROM #TXNHDR d
INNER JOIN TXNCF cf ON cf.lId = d.lId AND cf.lLine = 0
GROUP BY d.lId;

CREATE CLUSTERED INDEX IX_vTXNCF_lId ON #vTXNCF(lId);

--------------------------------------------------------------------------------
-- 4) #vACC (only parties present)
--------------------------------------------------------------------------------
;WITH NeedAcc AS (
    SELECT DISTINCT lAccId FROM #TXNACC
)
SELECT
    a.lId,
    a.sCode,
    a.sName,
    act.cAccTyp,
    coa.sName AS GroupName,
    ISNULL(bus.dCreditLimit,0) AS dCreditLimit,
    ISNULL(bus.dCreditDay,0)   AS dCreditDay,
    act.cLdgTyp,
    ISNULL(CASE
        WHEN addr.lMSMETyp = 1 THEN 'Micro'
        WHEN addr.lMSMETyp = 2 THEN 'Small'
        WHEN addr.lMSMETyp = 3 THEN 'Medium'
        ELSE '' END,'') AS sMSME,
    ISNULL(COALESCE(cnt.sName,   addr.sName),   '') AS ConName,
    ISNULL(COALESCE(cnt.sMobile, addr.sMobile), '') AS ConMobile
INTO #vACC
FROM NeedAcc na
INNER JOIN ACCMST a   ON a.lId = na.lAccId
INNER JOIN ACCTYP act ON act.lTypId = a.lTypId
INNER JOIN COAMST coa ON coa.lId = a.lCoaId
OUTER APPLY (
    SELECT TOP (1) b.lId, b.dCreditLimit, b.dCreditDay
    FROM BUSMST b
    WHERE b.lAccId = a.lId AND b.bDel = 0
    ORDER BY b.lId DESC
) bus
OUTER APPLY (
    SELECT TOP (1) ad.sName, ad.sMobile, ad.lMSMETyp
    FROM BUSADD ad
    WHERE bus.lId IS NOT NULL
      AND ad.lId = bus.lId
      AND ad.bDefault = 1
    ORDER BY ad.lId DESC
) addr
OUTER APPLY (
    SELECT TOP (1) bc.sName, bc.sMobile
    FROM BUSCNT bc
    WHERE bus.lId IS NOT NULL
      AND bc.lId = bus.lId
    ORDER BY bc.lId
) cnt
WHERE act.cLdgTyp = 'C';

CREATE CLUSTERED INDEX IX_vACC_lId ON #vACC(lId);

--------------------------------------------------------------------------------
-- 5) Summary + Week No  (RefNo/RefDate taken from Customer PO fields)
--------------------------------------------------------------------------------
;WITH vTxnAcc AS (
    SELECT
        d.lId, d.lTypId, d.lLine, d.lAccId,
        a.GroupName,
        d.dAmtCr, d.dAmtDr,
        CASE WHEN d.dtDueDate IS NULL OR d.dtDueDate = 0 THEN NULL
             ELSE CONVERT(date, CONVERT(char(8), d.dtDueDate)) END AS DueDateDate,
        CASE WHEN d.dtDueDate IS NULL OR d.dtDueDate = 0 THEN 0
             ELSE DATEDIFF(DAY, CONVERT(date, CONVERT(char(8), d.dtDueDate)),
                                CONVERT(date, CONVERT(char(8), @AsOfDateInt))) END AS OverdueDays,
        CASE WHEN d.dtDueDate IS NULL OR d.dtDueDate = 0 THEN NULL
             ELSE CONVERT(CHAR(11), CONVERT(date, CONVERT(char(8), d.dtDueDate)), 106) END AS OverdueDateStr,
        a.dCreditLimit AS CrLimit,
        a.dCreditDay   AS CrDays
    FROM #TXNACC d
    INNER JOIN #vACC a ON a.lId = d.lAccId
),
vTxnSett AS (
    SELECT
        d.lId, d.lLine,
        CASE WHEN MAX(d.dAmtDr) > 0
             THEN ISNULL(SUM(ISNULL(fs.dAdjAmtDr,0) - ISNULL(fs.dAdjAmtCr,0)),0)
             ELSE ISNULL(SUM(ISNULL(fs.dAdjAmtCr,0) - ISNULL(fs.dAdjAmtDr,0)),0)
        END AS dAmt,
        ISNULL(MAX(CONVERT(INT, fs.bSystem)), 0) AS bSystem
    FROM #TXNACC d
    LEFT JOIN TXNFINSET fs
        ON fs.lId = d.lId AND fs.lLine = d.lLine
    GROUP BY d.lId, d.lLine
),
vSumm AS (
    SELECT
        h.lId,
        h.CompanyName,
        h.lCompId,
        a.sCode AS AccCode,
        a.sName AS AccName,
        a.GroupName,
        a.sMSME,
        h.sName AS TrnTyp,
        h.sDocument AS TrnNo,
        CONVERT(VARCHAR, CONVERT(DATETIME, CONVERT(VARCHAR(10), h.mDocDate)), 106) AS TrnDate,

        -- ✅ Actual Ref fields (from Customer PO)
        ISNULL(cf.[Customer PO No.],'')   AS RefNo,
        ISNULL(cf.[Customer PO Date],'') AS RefDate,

        h.PayTrm,
        h.PayTrmDays,
        da.OverdueDateStr AS OverdueDate,
        da.DueDateDate,
        da.OverdueDays,
        h.CurType1,
        1 AS ConvRate,
        ISNULL(SUM(da.dAmtDr + da.dAmtCr),0) AS BillAmt,
        ABS(ISNULL(SUM(ds.dAmt),0))         AS PaidAmt,
        ISNULL(SUM(da.dAmtDr + da.dAmtCr),0) - ISNULL(SUM(ds.dAmt),0) AS BillOSAmt,
        0 AS UnAdjAmt,
        ISNULL(SUM(da.dAmtDr + da.dAmtCr),0) - ISNULL(SUM(ds.dAmt),0) AS OsAmt,
        a.dCreditLimit AS CrLimit,
        a.dCreditDay   AS CrDays,
        h.Location,
        h.Location1,
        h.Narration
    FROM #TXNHDR h
    INNER JOIN vTxnAcc  da ON da.lId = h.lId
    INNER JOIN #vACC     a ON a.lId  = da.lAccId
    INNER JOIN vTxnSett ds ON ds.lId = da.lId AND ds.lLine = da.lLine
    LEFT  JOIN #vTXNCF  cf ON cf.lId = h.lId
    WHERE (ISNULL(da.dAmtDr + da.dAmtCr,0) - ISNULL(ds.dAmt,0)) <> 0
      AND ISNULL(ds.bSystem,0) = 0
    GROUP BY
        h.lId,h.CompanyName,h.lCompId,
        a.sCode,a.sName,a.GroupName,a.sMSME,
        h.sName,h.sDocument,h.mDocDate,
        cf.[Customer PO No.], cf.[Customer PO Date],
        h.PayTrm,h.PayTrmDays,
        da.OverdueDateStr,da.DueDateDate,da.OverdueDays,
        h.CurType1,
        a.dCreditLimit,a.dCreditDay,
        h.Location,h.Location1,h.Narration
),
vSumm2 AS (
    SELECT ROW_NUMBER() OVER (ORDER BY AccName, AccCode, TrnDate, lId) AS myRow, *
    FROM vSumm
)
SELECT
    t.CompanyName AS [Company Name],
    t.lCompId     AS lCompId,
    t.AccCode     AS [Party Code],
    t.AccName     AS [Party Name],
    t.GroupName   AS [Account Group],
    t.sMSME       AS [MSME Type],

    t.TrnTyp      AS [Trans Type],
    CASE
        WHEN t.TrnTyp IN ('Export Material Sales Invoice','Export Sales Invoice - SL') THEN 'Export'
        ELSE 'Domestic'
    END AS [Type],

    t.TrnNo       AS [Trans No],
    t.TrnDate     AS [Trans Date],
    t.RefNo       AS [Ref No],
    t.RefDate     AS [Ref Date],

    cf.[Customer PO Date],
    cf.[Customer PO No.],
    cf.[Item Name],
    cf.[Destination],
    cf.[Bank Details],

    t.PayTrm      AS [Payment Term],
    t.PayTrmDays  AS [Payment Term Days],

    t.Location,
    t.Location1,

    t.CurType1    AS [Currency Code],
    1             AS [Conversion Rate],

    CONVERT(DECIMAL(21,2), t.BillAmt)   AS [Bill Amt],
    CONVERT(DECIMAL(21,2), t.PaidAmt)   AS [Paid Amt],
    CONVERT(DECIMAL(21,2), t.BillOSAmt) AS [Bill OS Amt],

    t.CrLimit     AS [Credit Limit],
    0             AS [Credit Limit Days],
    CONVERT(CHAR(11), t.DueDateDate, 106) AS [Due Date], 
    t.OverdueDate AS [Overdue Date],
    t.OverdueDays AS [Overdue Days],

    CONVERT(DECIMAL(21,2), t.UnAdjAmt)  AS [Unadjustment Amt],
    CONVERT(DECIMAL(21,2), t.OsAmt)     AS [Outstanding Amt],

    t.Narration,

    CASE
      WHEN t.DueDateDate IS NULL THEN NULL
      ELSE
        'Wk-' + RIGHT(
                  '0' + CAST(DATEDIFF(WEEK, W2.FiscalStartMonday, W1.WeekStart) + 1 AS VARCHAR(2)),
                  2
               )
        + '. ' + REPLACE(CONVERT(CHAR(11), W1.WeekStart, 106), ' ', '-')
    END AS [Week No]

FROM vSumm2 t
LEFT JOIN #vTXNCF cf ON cf.lId = t.lId
CROSS APPLY (
    SELECT
        WeekStart =
            DATEADD(DAY, 7,
                DATEADD(DAY, -((DATEPART(WEEKDAY, DueDateDate) + 5) % 7), DueDateDate)
            ),
        FiscalStart =
            CASE
              WHEN MONTH(
                    DATEADD(DAY, 7,
                      DATEADD(DAY, -((DATEPART(WEEKDAY, DueDateDate) + 5) % 7), DueDateDate)
                    )
                  ) >= 4
                THEN DATEFROMPARTS(
                      YEAR(
                        DATEADD(DAY, 7,
                          DATEADD(DAY, -((DATEPART(WEEKDAY, DueDateDate) + 5) % 7), DueDateDate)
                        )
                      ), 4, 1
                )
              ELSE DATEFROMPARTS(
                      YEAR(
                        DATEADD(DAY, 7,
                          DATEADD(DAY, -((DATEPART(WEEKDAY, DueDateDate) + 5) % 7), DueDateDate)
                        )
                      ) - 1, 4, 1
                )
            END
) W1
CROSS APPLY (
    SELECT FiscalStartMonday =
        CASE WHEN W1.FiscalStart IS NULL THEN NULL
             ELSE DATEADD(DAY, -((DATEPART(WEEKDAY, W1.FiscalStart) + 5) % 7), W1.FiscalStart)
        END
) W2
WHERE t.AccName NOT IN (
        'OC SPECIALITIES CHEMICALS PVT. LTD.',
        'OC Specialities Private Limited (DR)',
        'OC SPECIALITIES PVT. LTD.'
)
AND t.BillOSAmt NOT BETWEEN -100 AND 1000
ORDER BY t.myRow
OPTION (RECOMPILE);

--------------------------------------------------------------------------------
-- Cleanup
--------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#CompanyIds') IS NOT NULL DROP TABLE #CompanyIds;
IF OBJECT_ID('tempdb..#TypIds')     IS NOT NULL DROP TABLE #TypIds;
IF OBJECT_ID('tempdb..#TXNACC')     IS NOT NULL DROP TABLE #TXNACC;
IF OBJECT_ID('tempdb..#TXNHDR')     IS NOT NULL DROP TABLE #TXNHDR;
IF OBJECT_ID('tempdb..#vTXNCF')     IS NOT NULL DROP TABLE #vTXNCF;
IF OBJECT_ID('tempdb..#vACC')       IS NOT NULL DROP TABLE #vACC;

SET ANSI_WARNINGS ON;



"""
# ======================================================================
# HELPERS + VIEW (PASTE THIS BLOCK RIGHT AFTER YOUR SQL QUERIES)
# NOTE: Your SQL strings MUST use %s placeholders (NOT ?)
#       Example inside SQL:
#           DECLARE @FromDateInt INT = %s;
#           DECLARE @ToDateInt   INT = %s;
# ======================================================================

import re
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from collections import defaultdict

from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.shortcuts import render
from django.db import connections, DatabaseError

from .models import ManualPayableEntry  # make sure this model exists

logger = logging.getLogger("custom_logger")


# ---------------------------------------------------------------------
# Company groups (keep/edit ids as per your setup)
# ---------------------------------------------------------------------
COMPANY_GROUPS = {
    "specialities": {
        "label": "OC Specialities Private Limited",
        "ids": [4, 7, 27, 28, 40, 93],
    },
    "chemicals": {
        "label": "OC Specialities Chemicals Private Limited",
        "ids": [8, 9, 25, 26],
    },
}

# ---------------------------------------------------------------------
# Helper constants for manual natures grouping
# ---------------------------------------------------------------------
TAX_NATURES_UPPER = {
    "TDS & TCS",
    "GST (MONTHLY RETURN)",
    "CUSTOM DUTY + IGST",
    "ADVANCE TAX",
}
FIN_NATURES_UPPER = {
    "INTEREST PAYMENTS",
    "LOAN RE-PAYMENT",
}

OTHER_NATURES_UPPER = {
    "ADVANCE PAYMENT - CAPEX/RM",
    "SALARY & WAGES",
    "BONUS",
    "PAYMENT TO LABOUR CONTRACTORS",
    "PAYMENT TO VEHICLE RENTAL",
    "ELECTRICITY CHARGES",
    "WATER CHARGES",
    "JOB WORK NOT BILLED/BOOKED YET",
    "INSURANCE",
}


def _parse_date_safe(s):
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _to_decimal(val, default="0"):
    if val is None or val == "":
        return Decimal(default)
    if isinstance(val, Decimal):
        return val
    try:
        return Decimal(str(val))
    except Exception:
        return Decimal(default)


def _round0(x):
    x = x if isinstance(x, Decimal) else Decimal(str(x or "0"))
    return x.quantize(Decimal("1"), rounding=ROUND_HALF_UP)



def _week_meta(label):
    if not label:
        return (0, None)

    week_no = 0
    week_dt = None

    m = re.search(r"Wk-(\d+)", label, re.IGNORECASE)
    if m:
        try:
            week_no = int(m.group(1))
        except Exception:
            week_no = 0

    parts = label.split(".", 1)
    if len(parts) == 2:
        dt_str = parts[1].strip()
        try:
            week_dt = datetime.strptime(dt_str, "%d-%b-%Y").date()
        except Exception:
            week_dt = None

    return (week_no, week_dt)


def _week_sort_key(label):
    wk_no, wk_dt = _week_meta(label)
    return (wk_dt or date.max, wk_no, label)


def _iter_week_starts(start_d, end_d):
    cur = start_d - timedelta(days=start_d.weekday())
    while cur <= end_d:
        yield cur
        cur += timedelta(days=7)


def _execute_cashflow_sql(sql_template, from_int, to_int, log_label):
    """
    SQL template uses:
        {FROM_DATE_INT}
        {TO_DATE_INT}
    We inject those INTs and execute with NO params.
    """
    sql = (sql_template or "")
    sql = sql.replace("{FROM_DATE_INT}", str(from_int))
    sql = sql.replace("{TO_DATE_INT}", str(to_int))

    rows = []
    try:
        with connections["readonly_db"].cursor() as cursor:
            cursor.execute(sql)

            final_cols = None
            final_rows = None

            while True:
                if cursor.description is not None:
                    cols = [c[0] for c in cursor.description]
                    data = cursor.fetchall()
                    if cols and data:
                        final_cols = cols
                        final_rows = data

                try:
                    has_next = cursor.nextset()
                except Exception:
                    has_next = False

                if not has_next:
                    break

            if final_cols and final_rows:
                rows = [dict(zip(final_cols, r)) for r in final_rows]
                logger.info("%s cashflow SQL: loaded %s rows", log_label, len(rows))
            else:
                logger.error("%s cashflow SQL: no final result set with data", log_label)

    except DatabaseError:
        logger.exception("Error fetching %s cashflow data from ERP", log_label)
        rows = []

    return rows

def _fmt_date(val):
    if not val:
        return ""
    if isinstance(val, datetime):
        val = val.date()
    if isinstance(val, date):
        return val.strftime("%d-%b-%Y")
    return str(val)



@login_required
def payables_cashflow_report(request):
    """
    Weekly Budgeted Cashflow Matrix (Receivables & Payables)
    + Exclude specific parties from main Payable/Receivable tables AND weekly cashflow
    + Show excluded parties separately at bottom (payable + receivable)
    + ERP Due Date is adjusted by saved PayablePartyExtension.extend_days after fetching ERP rows
    """
    today = date.today()

    # -------------------------------------------------------------------------
    # EXCLUDED PARTIES
    # -------------------------------------------------------------------------
    EXCLUDED_PAY_PARTIES = {
        "DEEDY CHEMICALS PVT. LTD. (CR)",
        "FREESIA CHEMICALS (CR)",
        "HINDUSTHAN CHEMICALS COMPANY (CR)",
        "REMEDIUM LIFECARE LTD. (CR)",
        "ZEOLITES AND ALLIED PRODUCTS PVT. LTD. (CR)",
        "THE ARAB POTASH CO. PLC (CR)",
    }

    EXCLUDED_RECV_PARTIES = {
        "DEEDY CHEMICALS PVT. LTD.",
        "FREESIA CHEMICALS",
        "HINDUSTHAN CHEMICAL COMPANY",
        "REMEDIUM LIFECARE LTD.",
        "ZEOLITES AND ALLIED PRODUCTS PVT. LTD.",
        "ARISTA CHEMICALS LLP",
        "UNIVERSAL CHEMICALS & INDUSTRIES PVT. LTD.",
    }

    def _norm_party_name(x: str) -> str:
        return " ".join((x or "").strip().upper().split())

    EXCL_PAY_NORM = {_norm_party_name(x) for x in EXCLUDED_PAY_PARTIES}
    EXCL_RECV_NORM = {_norm_party_name(x) for x in EXCLUDED_RECV_PARTIES}

    # -------------------------------------------------------------------------
    # Safe date conversion (ERP can return date/datetime/str)
    # -------------------------------------------------------------------------
    def _safe_to_date(v):
        if not v:
            return None
        if isinstance(v, datetime):
            return v.date()
        if isinstance(v, date):
            return v
        s = str(v).strip()
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", "%d %b %Y", "%Y%m%d"):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                continue
        try:
            return datetime.fromisoformat(s[:19]).date()
        except Exception:
            return None

    # -------------------------------------------------------------------------
    # Company filter
    # -------------------------------------------------------------------------
    company_param = (request.GET.get("company") or "").strip().lower()
    if company_param in COMPANY_GROUPS:
        company_selected_key = company_param
        allowed_company_ids = set(COMPANY_GROUPS[company_param]["ids"])
    else:
        company_selected_key = None
        allowed_company_ids = None

    # -------------------------------------------------------------------------
    # Week helpers
    # -------------------------------------------------------------------------
    this_week_label = _week_label_for_date(today)
    _, this_week_dt = _week_meta(this_week_label)
    this_week_dt = this_week_dt or (today - timedelta(days=today.weekday()))  # Monday

    def _rebucket_week_label(raw_label):
        if not raw_label:
            return this_week_label
        _, wk_dt = _week_meta(raw_label)
        if wk_dt is None or wk_dt < this_week_dt:
            return this_week_label
        return raw_label

    # -------------------------------------------------------------------------
    # Date window
    # -------------------------------------------------------------------------
    data_start = date(2025, 4, 1)

    fy_start_year = today.year if today.month >= 4 else today.year - 1
    next_fy_end = date(fy_start_year + 2, 3, 31)

    from_date = _parse_date_safe(request.GET.get("from")) or data_start
    to_date = _parse_date_safe(request.GET.get("to")) or next_fy_end

    if from_date < data_start:
        from_date = data_start
    if to_date < from_date:
        to_date = from_date

    display_from_date = max(from_date, today)

    from_int = int(from_date.strftime("%Y%m%d"))
    to_int = int(to_date.strftime("%Y%m%d"))

    opening_start = _round0(_to_decimal(request.GET.get("opening"), "0"))

    # -------------------------------------------------------------------------
    # EXTENSION LOOKUP (uses your model exactly)
    # -------------------------------------------------------------------------
    ext_map_all = {}    # party_norm -> days (company_group blank/null)
    ext_map_cmp = {}    # (company_group, party_norm) -> days

    ext_qs = PayablePartyExtension.objects.filter(active=True)

    # If report is filtered by company, load:
    # - company_group == selected
    # - company_group blank/null (ALL)
    if company_selected_key:
        ext_qs = ext_qs.filter(
            Q(company_group__isnull=True) | Q(company_group="") | Q(company_group=company_selected_key)
        )
    else:
        # If no company filter, apply ONLY ALL-scoped extensions
        ext_qs = ext_qs.filter(Q(company_group__isnull=True) | Q(company_group=""))

    for e in ext_qs:
        pn = (e.party_norm or _norm_party_name(e.party_name))
        days = int(e.extend_days or 0)
        cg = (e.company_group or "").strip().lower()

        if cg:
            ext_map_cmp[(cg, pn)] = days
        else:
            ext_map_all[pn] = days

    def _get_ext_days(party_name: str) -> int:
        pn = _norm_party_name(party_name)
        if company_selected_key:
            if (company_selected_key, pn) in ext_map_cmp:
                return int(ext_map_cmp[(company_selected_key, pn)] or 0)
        return int(ext_map_all.get(pn, 0) or 0)

    # -------------------------------------------------------------------------
    # Excluded buckets (to show separately)
    # -------------------------------------------------------------------------
    excluded_pay_rows = []
    excluded_recv_rows = []

    excluded_pay_bill = Decimal("0")
    excluded_pay_paid = Decimal("0")
    excluded_pay_os = Decimal("0")

    excluded_recv_bill = Decimal("0")
    excluded_recv_paid = Decimal("0")
    excluded_recv_os = Decimal("0")

    # =========================================================================
    # 1. PAYABLES (ERP)
    # =========================================================================
    raw_pay = _execute_cashflow_sql(SQL_PAYABLES_CASHFLOW, from_int, to_int, "PAYABLES")

    pay_rows = []
    total_pay_bill = Decimal("0")
    total_pay_paid = Decimal("0")
    total_pay_os = Decimal("0")

    for r in raw_pay:
        comp_id = r.get("lCompId")
        if allowed_company_ids is not None and comp_id not in allowed_company_ids:
            continue

        party_name = (r.get("Party Name") or "").strip()
        party_norm = _norm_party_name(party_name)

        bill_amt = _round0(_to_decimal(r.get("Bill Amt")))
        paid_amt = _round0(_to_decimal(r.get("Paid Amt")))
        os_amt = _round0(_to_decimal(r.get("Outstanding Amt")))

        due_date_val = r.get("Due Date") or r.get("DueDateDate") or r.get("mDueDate")
        base_due = _safe_to_date(due_date_val)

        ext_days = _get_ext_days(party_name)
        adj_due = (base_due + timedelta(days=ext_days)) if base_due else None

        # ✅ bucket on adjusted due date
        week_lbl = _rebucket_week_label(_week_label_for_date(adj_due) if adj_due else r.get("Week No"))

        ref_no_val = r.get("Ref No") or r.get("Ref_No") or r.get("RefNo") or r.get("RefNo_Out") or ""
        ref_date_val = r.get("Ref Date") or r.get("Ref_Date") or r.get("RefDate") or r.get("RefDate_Out") or ""

        row_obj = {
            "company_id": comp_id,
            "company_name": r.get("Company Name"),
            "party_code": (r.get("Party Code") or "").strip(),
            "party_name": party_name,
            "account_name": r.get("Account Name"),
            "group_name": r.get("GroupName"),
            "nature": r.get("Nature"),
            "trans_type": r.get("Trans Type"),
            "trans_no": r.get("Trans No"),
            "trans_date": r.get("Trans Date"),
            "ref_no": ref_no_val,
            "ref_date": ref_date_val,

            "week_no": week_lbl,

            # ✅ overdue based on adjusted due date
            "overdue_days": (today - adj_due).days if adj_due else r.get("Overdue Days"),

            "bill_amt": bill_amt,

            # ✅ show adjusted due date
            "due_date": _fmt_date(adj_due) if adj_due else _fmt_date(due_date_val),
            "orig_due_date": _fmt_date(base_due) if base_due else _fmt_date(due_date_val),
            "extended_days": ext_days,

            "paid_amt": paid_amt,
            "outstanding_amt": os_amt,
            "is_manual": False,
            "remarks": "",
        }

        if party_norm in EXCL_PAY_NORM:
            excluded_pay_rows.append(row_obj)
            excluded_pay_bill += bill_amt
            excluded_pay_paid += paid_amt
            excluded_pay_os += os_amt
            continue

        pay_rows.append(row_obj)
        total_pay_bill += bill_amt
        total_pay_paid += paid_amt
        total_pay_os += os_amt

    # =========================================================================
    # 2. PAYABLES (MANUAL)
    # =========================================================================
    manual_qs = ManualPayableEntry.objects.filter(due_date__gte=from_date, due_date__lte=to_date)
    if company_selected_key:
        manual_qs = manual_qs.filter(company_group=company_selected_key)

    company_label_map = {k: v["label"] for k, v in COMPANY_GROUPS.items()}

    for e in manual_qs:
        week_lbl = _rebucket_week_label(_week_label_for_date(e.due_date))
        company_name = company_label_map.get(e.company_group) or e.company_group
        amt = _round0(e.amount or Decimal("0"))

        row_obj = {
            "company_id": None,
            "company_name": company_name,
            "party_code": "",
            "party_name": "",
            "account_name": "",
            "group_name": "",
            "nature": e.nature,
            "trans_type": "Manual Payable",
            "trans_no": "",
            "trans_date": e.due_date.strftime("%d-%b-%Y"),
            "ref_no": "",
            "ref_date": "",
            "week_no": week_lbl,
            "overdue_days": (today - e.due_date).days,
            "bill_amt": amt,
            "paid_amt": Decimal("0"),
            "outstanding_amt": amt,
            "is_manual": True,
            "remarks": e.remarks,
        }

        pay_rows.append(row_obj)
        total_pay_bill += amt
        total_pay_os += amt

    total_pay_bill = _round0(total_pay_bill)
    total_pay_paid = _round0(total_pay_paid)
    total_pay_os = _round0(total_pay_os)

    excluded_pay_bill = _round0(excluded_pay_bill)
    excluded_pay_paid = _round0(excluded_pay_paid)
    excluded_pay_os = _round0(excluded_pay_os)

    # =========================================================================
    # 3. RECEIVABLES (ERP)
    # =========================================================================
    raw_recv = _execute_cashflow_sql(SQL_RECEIVABLES_CASHFLOW, from_int, to_int, "RECEIVABLES")

    recv_rows = []
    total_recv_bill = Decimal("0")
    total_recv_paid = Decimal("0")
    total_recv_os = Decimal("0")

    for r in raw_recv:
        comp_id = r.get("lCompId")
        if allowed_company_ids is not None and comp_id not in allowed_company_ids:
            continue

        party_name = (r.get("Party Name") or "").strip()
        party_norm = _norm_party_name(party_name)

        due_date_val = r.get("Due Date") or r.get("DueDateDate")
        base_due = _safe_to_date(due_date_val)

        ext_days = _get_ext_days(party_name)
        adj_due = (base_due + timedelta(days=ext_days)) if base_due else None

        bill_amt = _round0(_to_decimal(r.get("Bill Amt")))
        paid_amt = _round0(_to_decimal(r.get("Paid Amt")))
        os_amt = _round0(_to_decimal(r.get("Outstanding Amt")))

        # ✅ bucket on adjusted due date
        week_lbl = _rebucket_week_label(_week_label_for_date(adj_due) if adj_due else r.get("Week No"))

        ref_no_val = r.get("Ref No") or r.get("Ref_No") or r.get("RefNo") or r.get("RefNo_Out") or ""
        ref_date_val = r.get("Ref Date") or r.get("Ref_Date") or r.get("RefDate") or r.get("RefDate_Out") or ""

        row_obj = {
            "company_id": comp_id,
            "company_name": r.get("Company Name"),
            "party_code": (r.get("Party Code") or "").strip(),
            "party_name": party_name,
            "account_group": r.get("Account Group"),
            "msme_type": r.get("MSME Type"),
            "contact_name": r.get("Contact Name"),
            "contact_mobile": r.get("Contact Mobile"),
            "trans_type": r.get("Trans Type"),
            "invoice_type": r.get("Type") or "Domestic",
            "trans_no": r.get("Trans No"),
            "trans_date": r.get("Trans Date"),

            "due_date": _fmt_date(adj_due) if adj_due else _fmt_date(due_date_val),
            "orig_due_date": _fmt_date(base_due) if base_due else _fmt_date(due_date_val),
            "extended_days": ext_days,

            "ref_no": ref_no_val,
            "ref_date": ref_date_val,
            "week_no": week_lbl,
            "overdue_days": (today - adj_due).days if adj_due else r.get("Overdue Days"),

            "bill_amt": bill_amt,
            "paid_amt": paid_amt,
            "outstanding_amt": os_amt,
        }

        if party_norm in EXCL_RECV_NORM:
            excluded_recv_rows.append(row_obj)
            excluded_recv_bill += bill_amt
            excluded_recv_paid += paid_amt
            excluded_recv_os += os_amt
            continue

        recv_rows.append(row_obj)
        total_recv_bill += bill_amt
        total_recv_paid += paid_amt
        total_recv_os += os_amt

    total_recv_bill = _round0(total_recv_bill)
    total_recv_paid = _round0(total_recv_paid)
    total_recv_os = _round0(total_recv_os)

    excluded_recv_bill = _round0(excluded_recv_bill)
    excluded_recv_paid = _round0(excluded_recv_paid)
    excluded_recv_os = _round0(excluded_recv_os)

    # ---------------- WEEK COLUMNS ----------------
    cashflow_weeks = []
    seen = set()

    for wk_start in _iter_week_starts(this_week_dt, to_date):
        lbl = _week_label_for_date(wk_start)
        if lbl and lbl not in seen:
            cashflow_weeks.append(lbl)
            seen.add(lbl)

    for r in (pay_rows + recv_rows):
        lbl = r.get("week_no") or this_week_dt
        if lbl not in seen:
            cashflow_weeks.append(lbl)
            seen.add(lbl)

    cashflow_weeks = sorted(cashflow_weeks, key=_week_sort_key)

    # ---------------- BUILD MATRIX ----------------
    Decimal0 = Decimal("0")

    pay_by_week = defaultdict(lambda: Decimal0)
    pay_nature_week = defaultdict(lambda: defaultdict(lambda: Decimal0))

    for r in pay_rows:
        wk = r.get("week_no") or this_week_dt
        nature = (r.get("nature") or "OTHER").strip() or "OTHER"
        amt = r.get("outstanding_amt") or Decimal0
        pay_by_week[wk] += amt
        pay_nature_week[nature][wk] += amt

    recv_by_week = defaultdict(lambda: Decimal0)
    recv_type_week = defaultdict(lambda: defaultdict(lambda: Decimal0))
    recv_fin_by_week = defaultdict(lambda: Decimal0)
    recv_gst_by_week = defaultdict(lambda: Decimal0)

    for r in recv_rows:
        wk = r.get("week_no") or this_week_dt
        typ = (r.get("invoice_type") or "Domestic").strip() or "Domestic"
        amt = r.get("outstanding_amt") or Decimal0
        recv_by_week[wk] += amt
        recv_type_week[typ][wk] += amt

    opening_per_week = {}
    net_per_week = {}
    total_inflow_by_week = {}
    total_outflow_by_week = {}

    current_opening = opening_start

    for wk in cashflow_weeks:
        inflow = _round0(recv_by_week[wk] + recv_fin_by_week[wk] + recv_gst_by_week[wk])
        outflow = _round0(pay_by_week[wk])

        total_inflow_by_week[wk] = inflow
        total_outflow_by_week[wk] = outflow

        opening_per_week[wk] = current_opening
        net = _round0(current_opening + inflow - outflow)
        net_per_week[wk] = net
        current_opening = net

    total_inflow_all = _round0(sum(total_inflow_by_week.values(), Decimal0))
    total_outflow_all = _round0(sum(total_outflow_by_week.values(), Decimal0))
    net_all = current_opening

    def _build_values(mapping, cumulative=False):
        values = []
        running = Decimal0
        for wk in cashflow_weeks:
            amt = _round0(mapping.get(wk, Decimal0))
            if cumulative:
                running = _round0(running + amt)
                values.append({"week": wk, "value": running})
            else:
                values.append({"week": wk, "value": amt})
        return values

    cashflow_rows = []

    cashflow_rows.append({"key": "section_receipts", "label": "Receipts", "is_section": True, "values": []})
    for inv_type in sorted(recv_type_week.keys()):
        cashflow_rows.append({
            "key": f"recv_{inv_type.lower().replace(' ', '_')}",
            "label": inv_type,
            "kind": "receivable",
            "filter_type": inv_type,
            "values": _build_values(recv_type_week[inv_type]),
        })

    cashflow_rows.append({
        "key": "recv_financing",
        "label": "Receipts From Financing",
        "remark": "actual",
        "kind": "receivable_extra",
        "values": _build_values(recv_fin_by_week),
    })
    cashflow_rows.append({
        "key": "recv_gst",
        "label": "GST Refund",
        "remark": "actual",
        "kind": "receivable_extra",
        "values": _build_values(recv_gst_by_week),
    })

    cashflow_rows.append({
        "key": "total_inflows",
        "label": "Total Cash Inflows",
        "is_total": True,
        "values": _build_values(total_inflow_by_week, cumulative=False),
    })

    cashflow_rows.append({"key": "section_payments", "label": "Payments", "is_section": True, "values": []})

    normal_natures, tax_natures, fin_natures ,other_natures = [], [], [],[]
    for n in sorted(pay_nature_week.keys()):
        label = (n or "").strip()
        if not label:
            continue
        u = label.upper()
        if u in TAX_NATURES_UPPER:
            tax_natures.append(label)
        elif u in FIN_NATURES_UPPER:
            fin_natures.append(label)
        elif u in OTHER_NATURES_UPPER:
            other_natures.append(label)
        else:
            normal_natures.append(label)

    for nature in normal_natures:
        cashflow_rows.append({
            "key": f"pay_{nature.lower().replace(' ', '_')}",
            "label": nature,
            "kind": "payable",
            "filter_nature": nature,
            "values": _build_values(pay_nature_week[nature]),
        })

    if tax_natures:
        cashflow_rows.append({"key": "pay_group_taxes", "label": "TAXES", "kind": "pay_group_header", "values": _build_values({})})
        for nature in tax_natures:
            cashflow_rows.append({
                "key": f"pay_{nature.lower().replace(' ', '_')}",
                "label": nature,
                "kind": "payable",
                "filter_nature": nature,
                "values": _build_values(pay_nature_week[nature]),
            })

    if fin_natures:
        cashflow_rows.append({"key": "pay_group_financing", "label": "FINANCING", "kind": "pay_group_header", "values": _build_values({})})
        for nature in fin_natures:
            cashflow_rows.append({
                "key": f"pay_{nature.lower().replace(' ', '_')}",
                "label": nature,
                "kind": "payable",
                "filter_nature": nature,
                "values": _build_values(pay_nature_week[nature]),
            })
    # ✅ OTHER group
    if other_natures:
        cashflow_rows.append({
            "key": "pay_group_other",
            "label": "OTHER",
            "kind": "pay_group_header",
            "values": _build_values({}),
        })
        for nature in other_natures:
            cashflow_rows.append({
                "key": f"pay_{nature.lower().replace(' ', '_')}",
                "label": nature,
                "kind": "payable",
                "filter_nature": nature,
                "values": _build_values(pay_nature_week[nature]),
            })

    cashflow_rows.append({
        "key": "total_outflows",
        "label": "Total Cash Outflows",
        "is_total": True,
        "values": _build_values(total_outflow_by_week, cumulative=False),
    })

    cashflow_rows.append({"key": "section_net", "label": "", "is_section": True, "values": []})
    cashflow_rows.append({
        "key": "opening",
        "label": "OPENING Balance",
        "values": [{"week": wk, "value": opening_per_week.get(wk, opening_start)} for wk in cashflow_weeks],
    })
    cashflow_rows.append({
        "key": "net_balance",
        "label": "NET BALANCE (Receivable - Payable)",
        "is_net": True,
        "values": [{"week": wk, "value": net_per_week.get(wk, Decimal0)} for wk in cashflow_weeks],
    })

    company_groups_for_template = [(k, v["label"]) for k, v in COMPANY_GROUPS.items()]
    current_month_tag = today.strftime("%b-%Y")

    context = {
        "title": "Cashflow Weekly",
        "from_str": display_from_date.strftime("%d-%b-%Y"),
        "to_str": to_date.strftime("%d-%b-%Y"),
        "from_val": display_from_date.strftime("%Y-%m-%d"),
        "to_val": to_date.strftime("%Y-%m-%d"),
        "opening_start": opening_start,
        "company_groups": company_groups_for_template,
        "company_selected": company_selected_key,

        "pay_rows": pay_rows,
        "recv_rows": recv_rows,

        "total_pay_bill": total_pay_bill,
        "total_pay_paid": total_pay_paid,
        "total_pay_os": total_pay_os,
        "total_recv_bill": total_recv_bill,
        "total_recv_paid": total_recv_paid,
        "total_recv_os": total_recv_os,

        "cashflow_weeks": cashflow_weeks,
        "cashflow_rows": cashflow_rows,
        "total_inflow_all": total_inflow_all,
        "total_outflow_all": total_outflow_all,
        "net_all": net_all,

        "excluded_pay_rows": excluded_pay_rows,
        "excluded_recv_rows": excluded_recv_rows,
        "excluded_pay_bill": excluded_pay_bill,
        "excluded_pay_paid": excluded_pay_paid,
        "excluded_pay_os": excluded_pay_os,
        "excluded_recv_bill": excluded_recv_bill,
        "excluded_recv_paid": excluded_recv_paid,
        "excluded_recv_os": excluded_recv_os,

        "current_month_tag": current_month_tag,
    }
    return render(request, "payables_cashflow_report.html", context)

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.core.paginator import Paginator
from django.shortcuts import get_object_or_404, redirect, render
from django.utils.dateparse import parse_date
from django.utils.http import url_has_allowed_host_and_scheme

from .forms import ManualPayableEntryForm
from .models import ManualPayableEntry

PER_PAGE_ALLOWED = [10, 25, 50, 100]


def _can_manage_entry(user, obj: ManualPayableEntry) -> bool:
    if user.is_superuser:
        return True
    if getattr(obj, "created_by_id", None) and obj.created_by_id == user.id:
        return True
    return False


def _page_links(current: int, total: int, delta: int = 2):
    if total <= 12:
        return list(range(1, total + 1))

    keep = {1, total}
    for i in range(current - delta, current + delta + 1):
        if 1 <= i <= total:
            keep.add(i)

    keep = sorted(keep)
    out = []
    prev = None
    for x in keep:
        if prev is not None and x - prev > 1:
            out.append(None)
        out.append(x)
        prev = x
    return out


def _safe_next_url(request, default_name: str):
    nxt = (request.POST.get("next") or request.GET.get("next") or "").strip()
    if nxt and url_has_allowed_host_and_scheme(nxt, allowed_hosts={request.get_host()}):
        return nxt
    return redirect(default_name).url


@login_required
def manual_payable_manage_view(request):
    # ---------------- Create ----------------
    if request.method == "POST":
        form = ManualPayableEntryForm(request.POST)
        if form.is_valid():
            obj = form.save(commit=False)
            obj.created_by = request.user
            obj.save()
            messages.success(request, "Manual payable entry added.")
            return redirect("ACCOUNTS:manual_payable_manage")
    else:
        form = ManualPayableEntryForm()

    # ---------------- Filters ----------------
    qs = ManualPayableEntry.objects.all().order_by("-due_date", "-id")

    company = (request.GET.get("company") or "").strip()
    nature = (request.GET.get("nature") or "").strip()
    from_str = (request.GET.get("from") or "").strip()
    to_str = (request.GET.get("to") or "").strip()

    if company:
        qs = qs.filter(company_group=company)
    if nature:
        qs = qs.filter(nature__icontains=nature)

    d_from = parse_date(from_str) if from_str else None
    d_to = parse_date(to_str) if to_str else None
    if d_from:
        qs = qs.filter(due_date__gte=d_from)
    if d_to:
        qs = qs.filter(due_date__lte=d_to)

    # ---------------- Pagination ----------------
    try:
        per_page = int(request.GET.get("per_page") or 25)
    except ValueError:
        per_page = 25
    if per_page not in PER_PAGE_ALLOWED:
        per_page = 25

    paginator = Paginator(qs, per_page)
    page_number = request.GET.get("page") or 1
    page_obj = paginator.get_page(page_number)

    q = request.GET.copy()
    if "page" in q:
        del q["page"]
    qs_no_page_str = q.urlencode()

    page_links = _page_links(page_obj.number, paginator.num_pages)

    company_choices = []
    try:
        company_choices = ManualPayableEntry._meta.get_field("company_group").choices
    except Exception:
        pass

    return render(
        request,
        "manual_payable_form.html",
        {
            "form": form,
            "page_obj": page_obj,
            "paginator": paginator,
            "per_page": per_page,
            "company": company,
            "nature": nature,
            "from_str": from_str,
            "to_str": to_str,
            "company_choices": company_choices,
            "qs_no_page_str": qs_no_page_str,
            "page_links": page_links,
            "current_full_path": request.get_full_path(),
        },
    )


@login_required
def manual_payable_edit_view(request, pk):
    if request.method != "POST":
        return redirect("ACCOUNTS:manual_payable_manage")

    obj = get_object_or_404(ManualPayableEntry, pk=pk)
    if not _can_manage_entry(request.user, obj):
        raise PermissionDenied("You do not have permission to edit this record.")

    form = ManualPayableEntryForm(request.POST, instance=obj)
    if form.is_valid():
        form.save()
        messages.success(request, "Manual payable entry updated.")
    else:
        messages.error(request, "Edit failed. Please check fields.")

    return redirect(_safe_next_url(request, "ACCOUNTS:manual_payable_manage"))


@login_required
def manual_payable_delete_view(request, pk):
    if request.method != "POST":
        return redirect("ACCOUNTS:manual_payable_manage")

    obj = get_object_or_404(ManualPayableEntry, pk=pk)
    if not _can_manage_entry(request.user, obj):
        raise PermissionDenied("You do not have permission to delete this record.")

    obj.delete()
    messages.success(request, "Manual payable entry deleted.")
    return redirect(_safe_next_url(request, "ACCOUNTS:manual_payable_manage"))


from datetime import datetime, date, timedelta
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.shortcuts import redirect
from django.utils.http import url_has_allowed_host_and_scheme

from .forms import PayablePartyExtensionForm
from .models import PayablePartyExtension

def _norm_party_name(x: str) -> str:
    return " ".join((x or "").strip().upper().split())

def _safe_next_url(request, default_url: str):
    nxt = (request.POST.get("next") or "").strip()
    if nxt and url_has_allowed_host_and_scheme(nxt, allowed_hosts={request.get_host()}):
        return nxt
    return default_url

def _to_date_obj(val):
    """
    Convert ERP due-date variants to Python date.
    Handles date/datetime and strings like '22-Dec-2025' or '2025-12-22'.
    """
    if not val:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            continue
    return None

def _load_payable_extensions_map(company_selected_key: str | None):
    """
    Returns dict: {party_norm: extend_days}
    Scope rules:
      - company_group NULL/blank applies to ALL
      - company_group == selected applies only for that filter
    Latest updated wins.
    """
    qs = PayablePartyExtension.objects.filter(active=True).order_by("-updated_at", "-id")
    if company_selected_key:
        qs = qs.filter(company_group__in=[company_selected_key, None, ""])
    else:
        qs = qs.filter(company_group__in=[None, ""])

    ext_map = {}
    for e in qs:
        pn = e.party_norm
        if pn and pn not in ext_map:
            ext_map[pn] = int(e.extend_days or 0)
    return ext_map

@login_required
def payable_party_extend_save(request):
    """
    Saves a payable extension rule. Used by the Cashflow report page (payable-only).
    """
    if request.method != "POST":
        return redirect("ACCOUNTS:payables_cashflow_report")

    form = PayablePartyExtensionForm(request.POST)
    if form.is_valid():
        obj = form.save(commit=False)
        obj.created_by = request.user

        # Optional: if company_group posted as "" => store NULL
        if (obj.company_group or "").strip() == "":
            obj.company_group = None

        obj.save()
        messages.success(request, f"Saved extension: {obj}")
    else:
        messages.error(request, "Could not save extension. Please check inputs.")

    return redirect(_safe_next_url(request, default_url=redirect("ACCOUNTS:payables_cashflow_report").url))


# ACCOUNTS/views.py
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.core.paginator import Paginator
from django.shortcuts import get_object_or_404, redirect, render
from django.utils.http import url_has_allowed_host_and_scheme

from .models import PayablePartyExtension

PER_PAGE_ALLOWED = [10, 25, 50, 100]

# Keep the same groups you already use in report:


def _safe_next_url(request, default_name: str):
    nxt = (request.POST.get("next") or request.GET.get("next") or "").strip()
    if nxt and url_has_allowed_host_and_scheme(nxt, allowed_hosts={request.get_host()}):
        return nxt
    return redirect(default_name).url

def _can_manage_extension(user, obj: PayablePartyExtension) -> bool:
    if user.is_superuser:
        return True
    # Allow creator to edit/delete
    if getattr(obj, "created_by_id", None) and obj.created_by_id == user.id:
        return True
    return False

def _page_links(current: int, total: int, delta: int = 2):
    if total <= 12:
        return list(range(1, total + 1))
    keep = {1, total}
    for i in range(current - delta, current + delta + 1):
        if 1 <= i <= total:
            keep.add(i)
    keep = sorted(keep)
    out = []
    prev = None
    for x in keep:
        if prev is not None and x - prev > 1:
            out.append(None)
        out.append(x)
        prev = x
    return out


@login_required
def payable_party_extension_manage_view(request):
    """
    Add + List in same page (like your manual payable manage page)
    """
    # ------------------- CREATE -------------------
    if request.method == "POST" and request.POST.get("action") == "create":
        party_name = (request.POST.get("party_name") or "").strip()
        company_group = (request.POST.get("company_group") or "").strip().lower() or None
        remarks = (request.POST.get("remarks") or "").strip()
        active = True if request.POST.get("active") else False

        try:
            extend_days = int(request.POST.get("extend_days") or 0)
        except ValueError:
            extend_days = 0

        if not party_name:
            messages.error(request, "Party Name is required.")
            return redirect("ACCOUNTS:payable_party_extension_manage")

        obj = PayablePartyExtension(
            party_name=party_name,
            company_group=company_group,
            extend_days=extend_days,
            active=active,
            remarks=remarks,
            created_by=request.user,
        )
        obj.save()
        messages.success(request, "Extension saved.")
        return redirect("ACCOUNTS:payable_party_extension_manage")

    # ------------------- FILTERS -------------------
    qs = PayablePartyExtension.objects.all().order_by("-active", "-updated_at", "-id")

    q = (request.GET.get("q") or "").strip()
    company = (request.GET.get("company") or "").strip().lower()
    is_active = (request.GET.get("active") or "").strip()

    if q:
        qs = qs.filter(party_name__icontains=q)

    if company in COMPANY_GROUPS:
        qs = qs.filter(company_group=company)

    if is_active in ("1", "0"):
        qs = qs.filter(active=(is_active == "1"))

    # ------------------- PAGINATION -------------------
    try:
        per_page = int(request.GET.get("per_page") or 25)
    except ValueError:
        per_page = 25
    if per_page not in PER_PAGE_ALLOWED:
        per_page = 25

    paginator = Paginator(qs, per_page)
    page_number = request.GET.get("page") or 1
    page_obj = paginator.get_page(page_number)

    qparams = request.GET.copy()
    if "page" in qparams:
        del qparams["page"]
    qs_no_page_str = qparams.urlencode()

    page_links = _page_links(page_obj.number, paginator.num_pages)

    company_choices = [(k, v["label"]) for k, v in COMPANY_GROUPS.items()]

    return render(
        request,
        "payable_party_extension_manage.html",
        {
            "page_obj": page_obj,
            "paginator": paginator,
            "per_page": per_page,
            "page_links": page_links,
            "qs_no_page_str": qs_no_page_str,

            "q": q,
            "company": company,
            "active": is_active,
            "company_choices": company_choices,

            "current_full_path": request.get_full_path(),
            "per_page_allowed": PER_PAGE_ALLOWED,
        },
    )


@login_required
def payable_party_extension_edit_view(request, pk):
    """
    POST-only edit (like your manual payable edit)
    """
    if request.method != "POST":
        return redirect("ACCOUNTS:payable_party_extension_manage")

    obj = get_object_or_404(PayablePartyExtension, pk=pk)
    if not _can_manage_extension(request.user, obj):
        raise PermissionDenied("You do not have permission to edit this record.")

    party_name = (request.POST.get("party_name") or "").strip()
    company_group = (request.POST.get("company_group") or "").strip().lower() or None
    remarks = (request.POST.get("remarks") or "").strip()
    active = True if request.POST.get("active") else False

    try:
        extend_days = int(request.POST.get("extend_days") or 0)
    except ValueError:
        extend_days = obj.extend_days

    if not party_name:
        messages.error(request, "Party Name is required.")
        return redirect(_safe_next_url(request, "ACCOUNTS:payable_party_extension_manage"))

    obj.party_name = party_name
    obj.company_group = company_group
    obj.extend_days = extend_days
    obj.active = active
    obj.remarks = remarks
    obj.save()

    messages.success(request, "Extension updated.")
    return redirect(_safe_next_url(request, "ACCOUNTS:payable_party_extension_manage"))


@login_required
def payable_party_extension_delete_view(request, pk):
    """
    POST-only delete
    """
    if request.method != "POST":
        return redirect("ACCOUNTS:payable_party_extension_manage")

    obj = get_object_or_404(PayablePartyExtension, pk=pk)
    if not _can_manage_extension(request.user, obj):
        raise PermissionDenied("You do not have permission to delete this record.")

    obj.delete()
    messages.success(request, "Extension deleted.")
    return redirect(_safe_next_url(request, "ACCOUNTS:payable_party_extension_manage"))
