from io import BytesIO
from datetime import date, timedelta
from email.utils import formataddr

import pandas as pd
from django.conf import settings
from django.db import connections
from django.core.mail import EmailMessage

def _q(s: str) -> str:
    return s.replace("'", "''") if s else s

def _date_range(from_date, to_date, range_key):
    today = date.today()

    # NEW: open-ended custom (from_date set, to_date empty) -> up to today
    if from_date and not to_date:
        return from_date.isoformat(), today.isoformat()

    if from_date and to_date:
        return from_date.isoformat(), to_date.isoformat()

    if range_key == "yesterday":
        d = today - timedelta(days=1)
        return d.isoformat(), d.isoformat()
    if range_key == "last7":
        return (today - timedelta(days=7)).isoformat(), today.isoformat()
    if range_key == "mtd":
        return date(today.year, today.month, 1).isoformat(), today.isoformat()

    # default
    return "2022-04-01", today.isoformat()

def build_cogs_excel(company_id, year_id, from_date, to_date,
                     cust_code=0, item_id=0, txn_name="", customer_name="", item_name="") -> tuple[bytes, str]:
    to_date_sql = f"'{_q(to_date)}'"

    extra_where = ""
    if txn_name:
        extra_where += f" AND [Transaction Name] LIKE '%{_q(txn_name)}%'"
    if customer_name:
        extra_where += f" AND [Customer Name] LIKE '%{_q(customer_name)}%'"
    if item_name:
        extra_where += f" AND [Item Name] LIKE '%{_q(item_name)}%'"

    base_ctes = f"""
SET NOCOUNT ON;

DECLARE
    @CompanyID INT   = {company_id},
    @YearId    INT   = {year_id},
    @FromDate  DATE  = '{_q(from_date)}',
    @ToDate    DATE  = {to_date_sql},
    @CustCode  INT   = {cust_code},
    @ItemId    INT   = {item_id};

;WITH RawSales AS (
    SELECT
        dt.sName AS [Transaction Name],
        d.lId    AS SalesInvoiceId,
        d.sDocNo AS [Sales Invoice No],
        CONVERT(VARCHAR, CONVERT(DATE, CONVERT(VARCHAR(8), d.dtDocDate ,112)),106) AS [Sales Invoice Date],
        CUST.sName AS [Customer Name],
        dd.lLnkDocId, dd.lLnkLine, dd.lLine,
        ITP.sName AS [Item Type],
        ITM.sCode AS [Item Code],
        ITM.sName AS [Item Name],
        dd.sValue1 AS [Batch No.],
        UOM.sCode  AS [UOM],
        CASE WHEN d.lTypId NOT IN (990,341,1079)
             THEN CONVERT(DECIMAL(18,2), dd.dQty2)
             ELSE CONVERT(DECIMAL(18,2), dd.dQty2) * -1 END              AS [Sales Quantity],
        CASE WHEN d.lTypId NOT IN (990,341,1079)
             THEN CONVERT(DECIMAL(18,2), dd.dRate)
             ELSE CONVERT(DECIMAL(18,2), dd.dRate) * -1 END              AS [Sales Rate],
        CASE WHEN d.lTypId NOT IN (990,341,1079)
             THEN CONVERT(DECIMAL(18,2), dd.dQty2 * dd.dRate)
             ELSE CONVERT(DECIMAL(18,2), dd.dQty2 * -1 * dd.dRate) END   AS [Sales Value],
        CONVERT(DECIMAL(18,2), -(dd.dStkVal / NULLIF(dd.dQty2,0)))
          - (CONVERT(DECIMAL(18,2), dds.dRate3) - CONVERT(DECIMAL(18,2), dds.dRate)) AS [Material Rate],
        CONVERT(DECIMAL(18,2), -(dd.dStkVal))
          - dd.dQty2 * (CONVERT(DECIMAL(18,2), dds.dRate3) - CONVERT(DECIMAL(18,2), dds.dRate)) AS [Material Value],
        CONVERT(DECIMAL(18,2), dds.dRate3) - CONVERT(DECIMAL(18,2), dds.dRate) AS [Other Rate],
        dd.dQty2 * (CONVERT(DECIMAL(18,2), dds.dRate3) - CONVERT(DECIMAL(18,2), dds.dRate)) AS [Other Value],
        CONVERT(DECIMAL(18,2), -(dd.dStkVal / NULLIF(dd.dQty2,0))) AS [COGS Rate],
        CONVERT(DECIMAL(18,2), -(dd.dStkVal)) AS [COGS Value],
        C.sName AS [Cost Centre],
        u.sRemarks AS [Sale Person Name]
    FROM  TXNTYP  dt
    JOIN  TXNHDR  d   ON d.lTypId = dt.lTypId
                     AND d.lTypId IN (341,499,504,650,654,824,825,826,827,828,829,939,940,990,1079)
    JOIN  TXNDET  dd  ON d.lId = dd.lId AND dd.cFlag = 'I'
    LEFT  JOIN TXNDET dds ON dd.lStkId   = dds.lId AND dd.lStkLine = dds.lLine
    LEFT  JOIN BUSMST CUST ON d.lAccId1 = CUST.lId
    LEFT  JOIN ITMMST ITM  ON dd.lItmId = ITM.lId
    LEFT  JOIN ITMTYP ITP  ON ITP.lTypId = dd.lItmTyp
    LEFT  JOIN UNTMST UOM  ON dd.lUntId  = UOM.lId
    LEFT  JOIN DIMMST C    ON dd.lDimId  = C.lId AND C.cTyp = 'C'
    LEFT  JOIN USRMST u    ON d.lEmpId   = u.lId
    WHERE (CUST.lId  = @CustCode OR @CustCode = 0)
      AND (dd.lItmId = @ItemId   OR @ItemId  = 0)
      AND  d.lCompId IN (27,9,28,25,26)
      AND  d.bDel    = 0
      AND  CONVERT(DATE, CONVERT(VARCHAR(8), d.dtDocDate,112))
           BETWEEN @FromDate AND @ToDate
),
BondAdj AS (
    SELECT
        dd.lId   AS LnkDocId,
        dd.lLine AS LnkLine,
        CONVERT(DECIMAL(18,2), -(dd.dStkVal / NULLIF(dd.dQty2,0))) AS NewCOGSRate,
        CONVERT(DECIMAL(18,2), -(dd.dStkVal)) AS NewCOGSValue
    FROM  TXNDET dd
    WHERE dd.lTypId = 902
),
Final AS (
    SELECT
        RS.[Transaction Name],
        RS.[Sales Invoice No],
        RS.[Sales Invoice Date],
        RS.[Customer Name],
        RS.[Cost Centre],
        RS.[Sale Person Name],
        RS.[Item Type],
        RS.[Item Code],
        RS.[Item Name],
        RS.[Batch No.],
        RS.[UOM],
        RS.[Sales Quantity],
        RS.[Sales Rate],
        RS.[Sales Value],
        RS.[Material Rate],
        RS.[Material Value],
        RS.[Other Rate],
        RS.[Other Value],
        COALESCE(BA.NewCOGSRate , RS.[COGS Rate])  AS [COGS Rate],
        COALESCE(BA.NewCOGSValue, RS.[COGS Value]) AS [COGS Value],
        RS.[Sales Rate] - COALESCE(BA.NewCOGSRate, RS.[COGS Rate]) AS [GrossProfitPerKG],
        RS.[Sales Quantity] * (RS.[Sales Rate] - COALESCE(BA.NewCOGSRate, RS.[COGS Rate])) AS [Value],
        CASE WHEN RS.[Sales Value] <> 0
             THEN (RS.[Sales Quantity] * (RS.[Sales Rate] - COALESCE(BA.NewCOGSRate, RS.[COGS Rate])))
                  / RS.[Sales Value] * 100
             ELSE 0 END AS [Percent]
    FROM RawSales RS
    LEFT JOIN BondAdj BA
           ON BA.LnkDocId = RS.lLnkDocId
          AND BA.LnkLine  = RS.lLnkLine
          AND RS.[Transaction Name] = 'Ex Bond Sales Invoice - Domestic'
)
"""
    sql_export = f"""
{base_ctes}
SELECT
    [Transaction Name],
    [Sales Invoice No],
    [Sales Invoice Date],
    [Customer Name],
    [Cost Centre],
    [Item Type],
    [Item Code],
    [Item Name],
    [Batch No.],
    [UOM],
    [Sales Quantity],
    [Sales Rate],
    [Sales Value],
    [Material Rate],
    [Material Value],
    [Other Rate],
    [Other Value],
    [COGS Rate],
    [COGS Value],
    [GrossProfitPerKG],
    [Value],
    [Percent]
FROM Final
WHERE [Cost Centre] = 'PERSONAL CARE (DIVISION)' {extra_where}
ORDER BY [Sales Invoice Date], [Sales Invoice No];
"""

    with connections["readonly_db"].cursor() as cur:
        cur.execute(sql_export)
        cols = [c[0] for c in cur.description]
        data = cur.fetchall()

    df = pd.DataFrame(data, columns=cols)
    out = BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        df.to_excel(xw, index=False, sheet_name="COGS")
        ws = xw.book["COGS"]
        ws.freeze_panes = ws["A2"]
        for i, col in enumerate(df.columns, start=1):
            try:
                sample = (str(v) for v in df[col].head(200).values)
                maxlen = max([len(str(col)), *[len(s) for s in sample]]) + 2
            except ValueError:
                maxlen = len(str(col)) + 2
            ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = min(maxlen, 40)

    parts = []
    if from_date:       parts.append(f"From: {from_date}")
    if to_date:         parts.append(f"To: {to_date}")
    if txn_name:        parts.append(f"Txn: {txn_name}")
    if customer_name:   parts.append(f"Customer: {customer_name}")
    if item_name:       parts.append(f"Item: {item_name}")
    filters_line = " | ".join(parts) if parts else "All records"

    return out.getvalue(), filters_line

def send_cogs_email(to_emails, subject, greet, excel_bytes, filters_line):
    from_email = formataddr(("OSSupport Team", getattr(settings, "EMAIL_HOST_USER", "workflow@ocspl.com")))
    body = (
        f"Dear {greet},\n\n"
        "Please find attached the latest COGS Report.\n"
        f"Filters: {filters_line}\n\n"
        "Regards,\n"
        "OSSupport Team"
    )
    email = EmailMessage(
        subject=subject,
        body=body,
        from_email=from_email,
        to=[e.strip() for e in to_emails.split(",") if e.strip()],
    )
    email.attach("COGS_Report.xlsx", excel_bytes,
                 "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    email.send(fail_silently=False)

def send_cogs_report_job(schedule_obj):
    f, t = _date_range(schedule_obj.from_date, schedule_obj.to_date, schedule_obj.date_range)
    excel_bytes, filters_line = build_cogs_excel(
        schedule_obj.company_id, schedule_obj.year_id,
        f, t,
        cust_code=schedule_obj.cust_code, item_id=schedule_obj.item_id,
        txn_name=schedule_obj.txn_name, customer_name=schedule_obj.customer_name, item_name=schedule_obj.item_name
    )
    send_cogs_email(schedule_obj.to_emails, schedule_obj.subject, schedule_obj.greet, excel_bytes, filters_line)
