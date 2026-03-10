# ERP_Reports/management/commands/send_cogs_report.py
"""
Management command: generate the COGS report (Excel) and email it.

Usage examples:
  python manage.py send_cogs_report --to finance@ocspl.com
  python manage.py send_cogs_report --to a@b.com,c@d.com --range mtd --subject "COGS (MTD)"
  python manage.py send_cogs_report --to you@ocspl.com --from-date 2025-08-01 --to-date 2025-08-26 \
      --txn-name "Ex Bond" --customer-name "BASF" --item-name "AMIDO"
"""

from io import BytesIO
from datetime import date, timedelta
from email.utils import formataddr

import pandas as pd
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from django.db import connections
from django.core.mail import EmailMessage


class Command(BaseCommand):
    help = "Generate COGS report (Excel) and email it."

    def add_arguments(self, parser):
        # Recipients + subject/greeting
        parser.add_argument("--to", required=True, help="Comma-separated recipient emails")
        parser.add_argument("--subject", default="COGS Report")
        parser.add_argument("--greet", default="Team", help='Greeting name, e.g., "Team" or "Finance"')

        # Filters
        parser.add_argument("--company-id", type=int, default=27)
        parser.add_argument("--year-id", type=int, default=7)
        parser.add_argument("--cust-code", type=int, default=0)
        parser.add_argument("--item-id", type=int, default=0)

        parser.add_argument("--from-date", dest="from_date", help="YYYY-MM-DD")
        parser.add_argument("--to-date", dest="to_date", help="YYYY-MM-DD")

        parser.add_argument("--txn-name", default="")
        parser.add_argument("--customer-name", default="")
        parser.add_argument("--item-name", default="")

        # Quick date ranges (ignored if from/to provided)
        parser.add_argument("--range", choices=["yesterday", "last7", "mtd"],
                            help="Use quick date range if from/to not supplied")

    # --------------------------- helpers ---------------------------

    @staticmethod
    def _q(s: str) -> str:
        return s.replace("'", "''") if s else s

    @staticmethod
    def _resolve_dates(from_date: str | None, to_date: str | None, range_key: str | None) -> tuple[str, str]:
        today = date.today()
        if from_date and to_date:
            return from_date, to_date
        if range_key == "yesterday":
            d = today - timedelta(days=1)
            return d.isoformat(), d.isoformat()
        if range_key == "last7":
            return (today - timedelta(days=7)).isoformat(), today.isoformat()
        if range_key == "mtd":
            return date(today.year, today.month, 1).isoformat(), today.isoformat()
        # default fallback
        return (from_date or "2022-04-01"), (to_date or today.isoformat())

    def _build_sql(self, company_id, year_id, from_date, to_date, cust_code, item_id,
                   txn_name, customer_name, item_name) -> str:
        q = self._q
        to_date_sql = f"'{q(to_date)}'"

        extra_where = ""
        if txn_name:
            extra_where += f" AND [Transaction Name] LIKE '%{q(txn_name)}%'"
        if customer_name:
            extra_where += f" AND [Customer Name] LIKE '%{q(customer_name)}%'"
        if item_name:
            extra_where += f" AND [Item Name] LIKE '%{q(item_name)}%'"

        base_ctes = f"""
SET NOCOUNT ON;

DECLARE
    @CompanyID INT   = {company_id},
    @YearId    INT   = {year_id},
    @FromDate  DATE  = '{q(from_date)}',
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
WHERE [Cost Centre] = 'CHEMICALS' {extra_where}
ORDER BY [Sales Invoice Date], [Sales Invoice No];
"""
        return sql_export

    @staticmethod
    def _conn_alias() -> str:
        # Use 'readonly_db' if defined, else fall back to 'default'
        try:
            from django.conf import settings as dj_settings
            return "readonly_db" if "readonly_db" in dj_settings.DATABASES else "default"
        except Exception:
            return "default"

    def handle(self, *args, **opts):
        # -------- dates --------
        from_date, to_date = self._resolve_dates(opts.get("from_date"), opts.get("to_date"), opts.get("range"))

        # -------- filters --------
        company_id = int(opts["company_id"])
        year_id    = int(opts["year_id"])
        cust_code  = int(opts["cust_code"])
        item_id    = int(opts["item_id"])

        txn_name      = (opts["txn_name"] or "").strip()
        customer_name = (opts["customer_name"] or "").strip()
        item_name     = (opts["item_name"] or "").strip()

        # -------- SQL --------
        sql_export = self._build_sql(company_id, year_id, from_date, to_date, cust_code, item_id,
                                     txn_name, customer_name, item_name)

        # -------- fetch data --------
        alias = self._conn_alias()
        with connections[alias].cursor() as cur:
            cur.execute(sql_export)
            cols = [c[0] for c in cur.description]
            data = cur.fetchall()

        df = pd.DataFrame(data, columns=cols)

        # -------- build Excel in-memory --------
        out = BytesIO()
        with pd.ExcelWriter(out, engine="openpyxl") as xw:
            df.to_excel(xw, index=False, sheet_name="COGS")
            ws = xw.book["COGS"]
            ws.freeze_panes = ws["A2"]
            # auto-width using first 200 rows
            for i, col in enumerate(df.columns, start=1):
                try:
                    sample_vals = (str(v) for v in df[col].head(200).values)
                    maxlen = max([len(str(col)), *[len(s) for s in sample_vals]]) + 2
                except ValueError:
                    maxlen = len(str(col)) + 2
                ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = min(maxlen, 40)

        # -------- email --------
        to_emails = [e.strip() for e in (opts["to"] or "").split(",") if e.strip()]
        if not to_emails:
            raise CommandError("No recipients provided via --to")

        # filter string for the body
        parts = []
        if from_date:       parts.append(f"From: {from_date}")
        if to_date:         parts.append(f"To: {to_date}")
        if txn_name:        parts.append(f"Txn: {txn_name}")
        if customer_name:   parts.append(f"Customer: {customer_name}")
        if item_name:       parts.append(f"Item: {item_name}")
        filters_line = " | ".join(parts) if parts else "All records"

        body = (
            f"Dear {opts['greet']},\n\n"
            "Please find attached the latest COGS Report.\n"
            f"Filters: {filters_line}\n\n"
            "Regards,\n"
            "OSSupport Team"
        )

        from_email = formataddr(("OSSupport Team", getattr(settings, "EMAIL_HOST_USER", "workflow@ocspl.com")))
        email = EmailMessage(
            subject=opts["subject"],
            body=body,
            from_email=from_email,
            to=to_emails,
        )
        email.attach(
            "COGS_Report.xlsx",
            out.getvalue(),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        email.send(fail_silently=False)

        self.stdout.write(self.style.SUCCESS(f"COGS mail sent to: {', '.join(to_emails)}"))
