from __future__ import annotations

import json
from datetime import datetime

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connections

# Pick qc_source if configured, else fallback to default
_QC_ALIAS = "qc_source" if "qc_source" in settings.DATABASES else "default"

_SQL_DEVIATIONS = """
-- Show ALL records with AR No. breakdown + Quantity + Spec Range
SELECT
    qc.id                       AS qc_entry_id,
    qc.entry_no                 AS qc_entry_no,
    qc.ar_no                    AS ar_no,                    -- Analysis Request No
    parts.AR_Type_Code          AS ar_type_code,             -- RM/PM/IP/FG/SFG (from AR No. or qc.ar_type)
    parts.AR_Type_Label         AS ar_type,                  -- Human-readable label
    parts.AR_FY                 AS ar_fy,                    -- e.g., 25-26
    parts.AR_Seq                AS ar_seq,                   -- e.g., 00012
    qc.batch_no                 AS batch_no,
    qc.entry_date               AS entry_date,
    p.name                      AS product_name,
    qc.stage                    AS stage,
    qc.[group]                  AS spec_group,
    qc.status                   AS status,
    qc.decision_status          AS decision_status,
    u.username                  AS qc_completed_by,
    qc.fg_qty                   AS qty,                      -- Finished Goods Qty
    se.spec_id                  AS spec_id,
    s.name                      AS spec_name,
    s.unit                      AS spec_unit,
    CASE
        WHEN s.spec_type = 'numeric' AND s.min_val IS NOT NULL AND s.max_val IS NOT NULL
            THEN CONCAT(CAST(s.min_val AS VARCHAR(32)), ' – ', CAST(s.max_val AS VARCHAR(32)))
        WHEN s.spec_type = 'choice' AND s.allowed_choices IS NOT NULL AND LTRIM(RTRIM(s.allowed_choices)) <> ''
            THEN s.allowed_choices
        ELSE NULL
    END                         AS spec_range,               -- << NEW COLUMN
    se.value                    AS entered_value,
    se.remark                   AS remark
FROM dbo.qc_entry               AS qc
LEFT JOIN dbo.auth_user         AS u   ON qc.qc_completed_by_id = u.id
INNER JOIN dbo.qc_product       AS p   ON qc.product_id         = p.id
LEFT JOIN dbo.qc_spec_entry     AS se  ON se.qc_entry_id        = qc.id
LEFT JOIN dbo.qc_spec           AS s   ON s.id                  = se.spec_id

-- ---------- Derive AR parts from qc.ar_no (with fallback to qc.ar_type) ----------
OUTER APPLY (
    SELECT
        UPPER(PARSENAME(REPLACE(qc.ar_no, '/', '.'), 3))     AS AR_Type_FromNo,
        PARSENAME(REPLACE(qc.ar_no, '/', '.'), 2)            AS AR_FY_FromNo,
        PARSENAME(REPLACE(qc.ar_no, '/', '.'), 1)            AS AR_Seq_FromNo
) AS split
OUTER APPLY (
    SELECT
        CAST(COALESCE(split.AR_Type_FromNo, NULLIF(qc.ar_type, '')) AS VARCHAR(5)) AS AR_Type_Code,
        COALESCE(split.AR_FY_FromNo,  '')                            AS AR_FY,
        COALESCE(split.AR_Seq_FromNo, '')                            AS AR_Seq,
        CASE UPPER(COALESCE(split.AR_Type_FromNo, qc.ar_type, ''))
            WHEN 'RM'  THEN 'Raw Material'
            WHEN 'PM'  THEN 'Packing Material'
            WHEN 'IP'  THEN 'In-Process'
            WHEN 'FG'  THEN 'Finished Goods'
            WHEN 'SFG' THEN 'Semi Finished Goods'
            ELSE 'Unknown'
        END AS AR_Type_Label
) AS parts

WHERE CAST(qc.entry_date AS DATE) = %s
ORDER BY qc.entry_no, se.id;
"""

class Command(BaseCommand):
    help = "Fetch deviation rows (all AR types) for a date and print JSON to stdout."

    def add_arguments(self, parser):
        parser.add_argument(
            "--date", required=True, help="Date in YYYY-MM-DD format (filters qc.entry_date)"
        )
        parser.add_argument(
            "--alias", default=_QC_ALIAS, help="Database alias (default: qc_source if configured)"
        )
        parser.add_argument(
            "--indent", type=int, default=2, help="JSON indent (default: 2)"
        )

    def handle(self, *args, **opts):
        # Parse date safely
        try:
            day = datetime.strptime(opts["date"], "%Y-%m-%d").date()
        except Exception as e:
            raise CommandError(f"Invalid --date '{opts['date']}': {e}")

        alias = opts["alias"]
        if alias not in settings.DATABASES:
            raise CommandError(f"DB alias '{alias}' is not configured.")

        # Execute the SQL and stream results
        with connections[alias].cursor() as cur:
            cur.execute(_SQL_DEVIATIONS, [day])
            cols = [c[0] for c in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]

        self.stdout.write(json.dumps(rows, default=str, indent=opts["indent"]))
