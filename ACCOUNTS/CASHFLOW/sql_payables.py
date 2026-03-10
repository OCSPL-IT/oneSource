# ACCOUNTS/sql_payables.py

# Paste your FULL final SQL query here as a triple-quoted string.
# (The same query we just finalised in SSMS.)
# No changes to the query body needed – Django will just execute it.

PAYABLES_OUTSTANDING_SQL = """
DECLARE @decimal DECIMAL(21,18);

DECLARE @AsOfDate      DATE = CAST(GETDATE() AS DATE);
DECLARE @FromDateInt   INT  = 20250401;  -- your current fiscal start
DECLARE @ToDateInt     INT  = CONVERT(INT, CONVERT(CHAR(8), GETDATE(), 112));

DECLARE @BaseCurRate   DECIMAL(18,6);

-- 🔽🔽🔽  PASTE EVERYTHING FROM HERE (the whole query we built) 🔽🔽🔽

SELECT @decimal = lDigDec
FROM CMPNY c
INNER JOIN CURMST cm ON c.lCurrId = cm.lid
WHERE c.lId = 27;

-- ... (entire query body unchanged, down to the final DROP TABLEs)

-- Clean up temp tables
DROP TABLE IF EXISTS #temp;
DROP TABLE IF EXISTS #TXNHDR;
DROP TABLE IF EXISTS #vTXNCF;
DROP TABLE IF EXISTS #vACC;
DROP TABLE IF EXISTS #TXNACC;
DROP TABLE IF EXISTS #vTXNCF1;
DROP TABLE IF EXISTS #Ledger;
"""
