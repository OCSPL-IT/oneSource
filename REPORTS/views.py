# REPORTS/views.py
from __future__ import annotations
from HR.models import DailyAttendance
from maintenance.models import MaintenanceSchedule
import json
from pathlib import Path
from urllib.parse import urlencode
from django.db.models.functions import Coalesce
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Set
from django.contrib.auth.decorators import login_required
from django.db import connections
from django.shortcuts import render
from django.utils import timezone
from .data_access import load_plan_schedules
from .utils import generate_batch_report
from CONTRACT.models import HRContract,EmployeeAssignment
from django.db.models import Count, Q
from QC.models import QCEntry
from EHS.models import Lagging_Indicator, LeadingRecords
# at top with other imports
from django.db.models import F
from django.db.models import Count, Q, Sum
from HR.models import (
    DailyAttendance,
    HR as HRHeadcount,
    AttendanceRegulation,
    On_Duty_Request,
    ShortLeave,
    OvertimeReport,
    Helpdesk_Ticket,
    DailyCheckIn,
    Late_Early_Go,
)

from QC.models import IncomingGRNCache
from django.core.management import call_command  # optional, to backfill cache
def _inc_type(item_type: str | None) -> str:
    s = (item_type or "").strip().lower()
    return "PM" if s.startswith("packing") else "RM"

def get_incoming_rows_for(day: date) -> list[dict]:
    # ensure the GRN cache exists for that day (same behavior as QC page)
    if not IncomingGRNCache.objects.filter(grn_date=day).exists():
        try:
            call_command("sync_incoming_grn", date=str(day), verbosity=0)
        except Exception:
            pass  # swallow – empty list is fine if sync isn’t available

    rows = []
    qs = IncomingGRNCache.objects.filter(grn_date=day).order_by("grn_no", "item_name")
    for r in qs:
        rows.append({
            "type":     _inc_type(r.item_type),                 # "RM" / "PM"
            "material": (r.item_name or "").strip(),
            "supplier": (r.supplier_name or "").strip(),
            "qty_mt":   float(r.qty or 0.0),
            "status":   "Pass",
            "remarks":  "",
        })
    return rows



# ────────────────────────────────────────────────
# Simple in-view profiler
# ────────────────────────────────────────────────
import logging
from time import perf_counter

class _Profiler:
    def __init__(self, enabled: bool = False, logger_name: str = __name__):
        self.enabled = bool(enabled)
        self._t0 = perf_counter()
        self._last = self._t0
        self.rows = []  # list of (label, ms_since_last, ms_total)
        self.log = logging.getLogger(logger_name)

    def mark(self, label: str):
        """Record a split timing since last mark and from the very start."""
        if not self.enabled:
            return
        now = perf_counter()
        split_ms = (now - self._last) * 1000.0
        total_ms = (now - self._t0) * 1000.0
        self.rows.append((label, round(split_ms, 2), round(total_ms, 2)))
        self._last = now

    def as_rows(self):
        """Return rows as list[{'step','ms','cum_ms'}] and total."""
        out = [{"step": s, "ms": ms, "cum_ms": cum} for (s, ms, cum) in self.rows]
        total = out[-1]["cum_ms"] if out else 0.0
        return out, total

    def dump_to_log(self, heading: str = "Daily Ops Profiler"):
        if not self.enabled:
            return
        rows, total = self.as_rows()
        self.log.info("── %s (total %.2f ms) ──", heading, total)
        for r in rows:
            self.log.info("  %-32s  %8.2f ms  (cum: %8.2f ms)", r["step"], r["ms"], r["cum_ms"])

def _to_ymd(d):  # date -> "YYYY-MM-DD"
    return d.strftime("%Y-%m-%d")


def _month_tag(d: date) -> str:
    return d.strftime("%b-%y").upper()  # e.g., OCT-25

def _range_month_label(dfrom: date, dto: date, period: str) -> str:
    """
    Label rules:
      - MTD  : month of dto (first->dto is implied)
      - Custom within same month: that month
      - Custom spanning months : 'MMM-YY → MMM-YY'
    """
    if period == "MTD":
        return _month_tag(dto)
    if dfrom.year == dto.year and dfrom.month == dto.month:
        return _month_tag(dto)
    return f"{_month_tag(dfrom)} → {_month_tag(dto)}"


def _build_url(request, **replacements) -> str:
    """
    Clone current ?query, overwrite keys with replacements, and return path?query.
    Use None to drop a key.
    """
    q = request.GET.copy()
    for k, v in replacements.items():
        if v is None:
            q.pop(k, None)
        else:
            q[k] = v
    qs = urlencode(q, doseq=True)
    return f"{request.path}?{qs}" if qs else request.path


# ────────────────────────────────────────────────
# DB aliases / tables
# ────────────────────────────────────────────────
DISPATCH_PLAN_CONN  = "production_scheduler"           # production-scheduler DB
DISPATCH_PLAN_TABLE = "dbo.dispatch_plans"             # dispatch-plan table
ALPHA_TABLE         = "dbo.alpha"                      # product → alpha code map
ERP_CONN            = "readonly_db"                    # ERP / read-only DB
DEFAULT_CONN        = "default"                        # DB_DEFAULT_NAME (power/briquette/water)
# ────────────────────────────────────────────────



def _fy_start(d: date) -> date:
    # FY starts on Apr 1
    return date(d.year if d.month >= 4 else d.year - 1, 4, 1)

def _norm(s: str) -> str:
    return (s or "").strip().upper()


# ────────────────────────────────────────────────
# Helpers: Stock (Closing, no date filter)
# ────────────────────────────────────────────────
def fetch_stock_closing(stage_to_fgs: Dict[str, Set[str]]
                        ) -> Tuple[Dict[Tuple[str, str], float], Dict[str, float]]:
    """
    Build closing stock at (FG, Stage) and FG levels.

    Enhancements:
    - Loads distinct dispatch-plan material names and normalizes them for direct SFG matching.
    - If an ERP stock row is of type 'Semi Finished Good' and its FGName matches any
      dispatch FG (raw, alias, or dispatch-normalized), allocate directly to that FG.
    - Otherwise, fall back to the scheduler-derived stage_to_fgs mapping.
    - Only accept Location in {'Solapur Approved Main Stores','WIP Store Approved'}.

    Returns:
        stock_stage_close: {(FG, StageName) -> qty}
        stock_fg_close:    {FG -> qty}
    """
    # ---------- 1) Dispatch FG universe (for robust direct SFG matching) ----------
    try:
        alpha_map = fetch_alpha_map()  # normalized raw -> alias
    except Exception:
        alpha_map = {}

    dispatch_fg_set: Set[str] = set()
    try:
        with connections[DISPATCH_PLAN_CONN].cursor() as cur:
            cur.execute(f"SELECT DISTINCT material_name FROM {DISPATCH_PLAN_TABLE};")
            rows = cur.fetchall()
        for (mat,) in rows:
            if not mat:
                continue
            raw_norm   = _norm(str(mat))
            disp_norm  = _disp_norm(str(mat))
            dispatch_fg_set.add(raw_norm)
            dispatch_fg_set.add(disp_norm)
            alias = alpha_map.get(raw_norm)
            if alias:
                dispatch_fg_set.add(_norm(alias))
    except Exception:
        dispatch_fg_set = set()

    # ---------- 2) ERP stock SQL (unchanged shape; widened @Fromdate per your snippet) ----------
    stock_sql = r"""
SET NOCOUNT ON;
DECLARE @Fromdate INT, @Todate INT;
SELECT @Fromdate = 20250101, @Todate = 20400106;

SELECT i.lId, i.sCode, i.sName, id.lTypId AS lItmTyp, it.sName AS sItmTyp, it.bStkSum,
       id.lUntRpt, id.dCnvRpt
INTO #ITMMST_Filtered
FROM ITMMST i
JOIN ITMDET id   ON id.lId = i.lId AND id.bStkUpd = 1 AND i.bDel = 0
JOIN ITMTYP it   ON it.lTypId = id.lTypId AND it.bStkUpd = 1
JOIN ITMTYPCMP x ON x.lTypId = it.lTypId AND x.lCompId = 27
WHERE it.sName IN ('Work in Progress','WIP FR','Intercut','Semi Finished Good','Finished Good');
CREATE CLUSTERED INDEX IX1 ON #ITMMST_Filtered (lItmTyp,lId);

SELECT DISTINCT d.lId, d.sName, t.bCompSel
INTO #DIMMST_Filtered
FROM DIMMST d
JOIN DIMTYP t ON t.lTypId = d.lTypId AND d.cTyp='S'
WHERE d.bStkVal = 1;
CREATE CLUSTERED INDEX IX2 ON #DIMMST_Filtered (lId);

SELECT lTypId,lStkTyp INTO #TXNTYP_Filtered
FROM TXNTYP WHERE lStkTyp < 2;
CREATE CLUSTERED INDEX IX3 ON #TXNTYP_Filtered (lTypId);

SELECT lId,sCode,sFmt INTO #UNTMST_Filtered FROM UNTMST;
CREATE CLUSTERED INDEX IX4 ON #UNTMST_Filtered (lId);

-- Opening balances (9 columns)
SELECT -1 AS lStkTyp, 0 AS lId, @Fromdate AS dtDocDate,
       dd.lItmTyp, dd.lItmId, dd.lUntId2, dd.lLocId,
       SUM(dd.dQtyStk) AS dQty, SUM(COALESCE(dd.dStkVal,0)) AS dVal
INTO #Opening
FROM TXNHDR h
JOIN #TXNTYP_Filtered tt ON tt.lTypId = h.lTypId
JOIN TXNDET dd ON dd.lId = h.lId
JOIN #ITMMST_Filtered i ON i.lId = dd.lItmId AND i.lItmTyp = dd.lItmTyp
JOIN #DIMMST_Filtered l ON l.lId = dd.lLocId
WHERE h.bDel = 0 AND dd.bDel = 0 AND dd.cFlag IN ('I','A')
  AND dd.lClosed<=0 AND h.lClosed<=0
  AND (h.lCompId=27 OR l.bCompSel=-1)
  AND h.dtDocDate < @Fromdate
GROUP BY dd.lItmTyp,dd.lItmId,dd.lUntId2,dd.lLocId
HAVING ABS(SUM(dd.dQtyStk))>0.0001;
CREATE CLUSTERED INDEX IX5 ON #Opening(lItmTyp,lItmId,lLocId,lUntId2);

-- Period transactions (keep 9 columns; omit dd.lLine to match #Opening)
SELECT tt.lStkTyp, h.lId, h.dtDocDate,
       dd.lItmTyp, dd.lItmId, dd.lUntId2, dd.lLocId,
       dd.dQtyStk AS dQty, COALESCE(dd.dStkVal,0) AS dVal
INTO #PeriodRaw
FROM TXNHDR h
JOIN #TXNTYP_Filtered tt ON tt.lTypId = h.lTypId
JOIN TXNDET dd ON dd.lId = h.lId
JOIN #ITMMST_Filtered i ON i.lId = dd.lItmId AND i.lItmTyp = dd.lItmTyp
JOIN #DIMMST_Filtered l ON l.lId = dd.lLocId
WHERE h.bDel=0 AND dd.bDel=0 AND dd.cFlag IN ('I','A')
  AND dd.lClosed<=0 AND h.lClosed<=0
  AND (h.lCompId=27 OR l.bCompSel=-1)
  AND h.dtDocDate BETWEEN @Fromdate AND @Todate
  AND NOT (dd.dQtyStk BETWEEN -0.0001 AND 0.0001);

-- Union with identical column lists and aggregate per (itm, loc, unit, date)
SELECT
    lStkTyp, lId, dtDocDate, lItmTyp, lItmId, lUntId2, lLocId,
    SUM(dQty) AS dQty, SUM(dVal) AS dVal
INTO #Combined
FROM (
    SELECT o.lStkTyp, o.lId, o.dtDocDate, o.lItmTyp, o.lItmId, o.lUntId2, o.lLocId, o.dQty, o.dVal
    FROM #Opening AS o
    UNION ALL
    SELECT r.lStkTyp, r.lId, r.dtDocDate, r.lItmTyp, r.lItmId, r.lUntId2, r.lLocId, r.dQty, r.dVal
    FROM #PeriodRaw AS r
) u
GROUP BY lStkTyp, lId, dtDocDate, lItmTyp, lItmId, lUntId2, lLocId;
CREATE CLUSTERED INDEX IX6 ON #Combined(lItmTyp,lItmId,lLocId,lUntId2,dtDocDate,lId);

-- Final select (includes sItmTyp so we can prefer SFG path)
SELECT i.sItmTyp,
       i.sName                        AS StageName,
       cf.FGName                      AS FGName,
       l.sName                        AS Location,
       CONVERT(VARCHAR,c.dtDocDate,105) AS txn_date,
       CAST(SUM(c.dQty) AS DECIMAL(21,3)) AS ClsQty,
       CAST(SUM(c.dVal) AS DECIMAL(21,2)) AS ClsVal
FROM #Combined c
JOIN #ITMMST_Filtered i ON i.lId=c.lItmId AND i.lItmTyp=c.lItmTyp
JOIN #DIMMST_Filtered l ON l.lId = c.lLocId
LEFT JOIN (
    SELECT icf.lTypId, icf.lId,
           MAX(CASE WHEN icf.sName='FG Name' THEN icf.sValue END) AS FGName
    FROM ITMCF icf
    GROUP BY icf.lTypId, icf.lId
) cf ON cf.lTypId=i.lItmTyp AND cf.lId=i.lId
GROUP BY i.sItmTyp, i.sName, cf.FGName, l.sName, CONVERT(VARCHAR,c.dtDocDate,105);
"""
    with connections[ERP_CONN].cursor() as cur:
        cur.execute(stock_sql)
        stk_rows = cur.fetchall()
        stk_cols = [c[0] for c in cur.description]

    # ---------- 3) Allocate with SFG-first, Location filter, then fallback by stage_to_fgs ----------
    c_typ   = stk_cols.index("sItmTyp")     # item type (e.g., 'Semi Finished Good')
    c_stage = stk_cols.index("StageName")
    c_fg    = stk_cols.index("FGName")
    c_loc   = stk_cols.index("Location")
    c_qty   = stk_cols.index("ClsQty")

    allowed_locations = {"SOLAPUR APPROVED MAIN STORES", "WIP STORE APPROVED"}

    stock_stage_close: Dict[Tuple[str, str], float] = {}

    for row in stk_rows:
        itm_typ    = (row[c_typ]   or "").strip()
        stage_name = (row[c_stage] or "").strip()
        fg_stock   = (row[c_fg]    or "").strip()
        loc_name   = (row[c_loc]   or "").strip()
        qty        = float(row[c_qty] or 0.0)

        if not stage_name or qty == 0.0:
            continue

        # LOCATION FILTER (apply to all paths)
        if _norm(loc_name) not in allowed_locations:
            continue

        # --- Preferred path: direct map if SFG row matches any dispatch FG (raw/alias/disp-norm)
        if fg_stock and itm_typ.lower() == "semi finished good":
            n_fg_raw   = _norm(fg_stock)
            n_fg_alias = _norm(alpha_map.get(n_fg_raw, fg_stock))
            n_fg_disp  = _disp_norm(fg_stock)

            if (n_fg_raw in dispatch_fg_set) or (n_fg_alias in dispatch_fg_set) or (n_fg_disp in dispatch_fg_set):
                stock_stage_close[(fg_stock, stage_name)] = stock_stage_close.get(
                    (fg_stock, stage_name), 0.0
                ) + qty
                continue  # done with this row

        # --- Fallback: stage→FG mapping from scheduler (respect Location filter already applied) ---
        n_stage = _norm(stage_name)
        possible = stage_to_fgs.get(n_stage, set())
        if not possible:
            continue

        chosen = None
        if fg_stock:
            n_fg_stock = _norm(fg_stock)
            for fg in possible:
                # match either raw or alias against the ERP FGName
                if _norm(fg) == n_fg_stock or _norm(alpha_map.get(_norm(fg), fg)) == n_fg_stock:
                    chosen = fg
                    break

        if chosen is None and len(possible) == 1:
            chosen = next(iter(possible))
        if chosen is None:
            continue

        stock_stage_close[(chosen, stage_name)] = stock_stage_close.get(
            (chosen, stage_name), 0.0
        ) + qty

    # ---------- 4) FG totals ----------
    stock_fg_close: Dict[str, float] = {}
    for (fg, _), q in stock_stage_close.items():
        stock_fg_close[fg] = stock_fg_close.get(fg, 0.0) + q

    return stock_stage_close, stock_fg_close

QC_PAGE_MAX_ROWS = 300  # keep it snappy on the big dashboard

def fetch_qc_tab_data(request):
    """
    Pulls stage-wise counts and a recent list of QC entries to embed in Reports → QC tab.
    Keeps assumptions minimal so it works with your current QCEntry model.

    Filters (optional; all via GET):
      ?qc_status=approved|approved_under_deviation|rejected|pending (multi allowed)
      ?qc_product=<alias or raw> (multi allowed; uses alpha_map)
      ?qc_stage=<stage text> (multi allowed; case-insensitive match)
    """
    alpha_map = fetch_alpha_map()  # already defined above
    norm = lambda s: (s or "").strip().upper()

    # Collect filters
    raw_status  = request.GET.getlist("qc_status")
    raw_prod    = request.GET.getlist("qc_product")
    raw_stage   = request.GET.getlist("qc_stage")

    sel_status = {norm(s) for s in raw_status if s}
    sel_prod   = {norm(p) for p in raw_prod if p}
    sel_stage  = {norm(st) for st in raw_stage if st}

    # Base queryset (keep it simple)
    qs = QCEntry.objects.select_related("product").all()

    # Status filter
    if sel_status:
        qs = qs.filter(decision_status__in=[s.lower() for s in sel_status])

    # Product filter (accept alias or raw)
    if sel_prod:
        # Build alias-aware OR
        from django.db.models import Q
        prod_q = Q()
        # We don't know your product model field names beyond product__name;
        # use product__name case-insensitively and also try matching aliases.
        for token in sel_prod:
            # match raw name OR alias name
            prod_q |= Q(product__name__iexact=token)
            # reverse-lookup: if token matches an alias, also match any raw that maps to it
            for raw_name, alias in alpha_map.items():
                if norm(alias) == token:
                    prod_q |= Q(product__name__iexact=raw_name)
        qs = qs.filter(prod_q)

    # Stage filter
    if sel_stage:
        from django.db.models import Q
        st_q = Q()
        for st in sel_stage:
            st_q |= Q(stage__iexact=st)
        qs = qs.filter(st_q)

    # Stage-wise counts (approved / variation / rejected / pending)
    stage_summary = (
        qs.values("product__name", "stage")
          .annotate(
              approved  = Count("id", filter=Q(decision_status="approved")),
              variation = Count("id", filter=Q(decision_status="approved_under_deviation")),
              rejected  = Count("id", filter=Q(decision_status="rejected")),
              pending   = Count("id", filter=Q(decision_status__in=["pending","in_progress","on_hold"]))
          )
          .order_by("product__name", "stage")
    )

    # Grand
    grand = {
        "approved":  sum(r["approved"]  for r in stage_summary),
        "variation": sum(r["variation"] for r in stage_summary),
        "rejected":  sum(r["rejected"]  for r in stage_summary),
        "pending":   sum(r["pending"]   for r in stage_summary),
    }
    grand["total"] = sum(grand.values())

    # Recent entries (lightweight columns only)
    recent = (
        qs.order_by("-id")  # newest first
          .values(
              "id",
              "product__name",
              "stage",
              "decision_status",
              "decision_remarks",   # if field missing, Django will raise; if you don't have it, remove this line
          )[:QC_PAGE_MAX_ROWS]
    )

    # Map product to alias for UI (without changing filters)
    def alias_of(name):
        n = norm(name)
        return alpha_map.get(n, name)

    summary_rows = []
    for r in stage_summary:
        summary_rows.append({
            "fg": alias_of(r["product__name"]),
            "stage": r["stage"],
            "approved": r["approved"],
            "variation": r["variation"],
            "rejected": r["rejected"],
            "pending": r["pending"],
            "total": (r["approved"]+r["variation"]+r["rejected"]+r["pending"]),
        })

    recent_rows = []
    for r in recent:
        recent_rows.append({
            "id": r["id"],
            "fg": alias_of(r["product__name"]),
            "stage": r["stage"],
            "status": (r["decision_status"] or "").replace("_", " ").title(),
            "remarks": r.get("decision_remarks", "") or "",
        })

    # For filter chip lists (distincts)
    prods = sorted({alias_of(n) for n in qs.values_list("product__name", flat=True)}, key=lambda s: s.upper())
    stages = sorted({(s or "").strip() for s in qs.values_list("stage", flat=True) if s}, key=lambda s: s.upper())

    return {
        "summary_rows": summary_rows,
        "recent_rows": recent_rows,
        "grand": grand,
        "filters": {
            "products": prods,
            "stages": stages,
            "selected": {
                "status": sorted(sel_status),
                "product": sorted(sel_prod),
                "stage": sorted(sel_stage),
            }
        }
    }

# ────────────────────────────────────────────────
# Helpers: Alpha map
# ────────────────────────────────────────────────
def fetch_alpha_map() -> Dict[str, str]:
    alpha_map: Dict[str, str] = {}
    with connections[DISPATCH_PLAN_CONN].cursor() as cur:
        cur.execute(f"SELECT product_name, alpha_code FROM {ALPHA_TABLE};")
        for pname, acode in cur.fetchall():
            alpha_map[_norm(pname)] = (acode or "").strip() or pname
    return alpha_map

# Map stock by both raw FG and alias-normalized FG, so dispatch rows can find it
def _build_stock_lookup(stock_fg_close: dict[str, float], alpha_map: dict[str, str]) -> dict[str, float]:
    """
    Returns a dict keyed by normalized FG names (both raw and alias) -> stock qty.
    This way, dispatch rows built from plan/actual keys (which may be normalized)
    can still match the stock captured from schedule-based FGs.
    """
    out: dict[str, float] = {}
    for fg, qty in (stock_fg_close or {}).items():
        n_raw = _norm(fg)
        n_alias = _norm(alpha_map.get(n_raw, fg))
        out[n_raw] = out.get(n_raw, 0.0) + float(qty or 0.0)
        out[n_alias] = out.get(n_alias, 0.0) + float(qty or 0.0)
    return out


# ────────────────────────────────────────────────
# Helpers: Utility mini-series (Power / Briquette / Water)
# ────────────────────────────────────────────────
def _fmt_label(dval) -> str:
    return dval.strftime("%d-%b") if isinstance(dval, (date, datetime)) else str(dval)

def fetch_power_series_range(start: date, end: date) -> Tuple[List[str], List[float]]:
    sql = """
        SELECT CONVERT(date, reading_date) AS rdate,
               CAST(total_kwh_e18_e22_e16 AS float) AS kwh
        FROM utility_power_readings
        WHERE reading_type='TOTAL POWER CONSUMPTION'
          AND CONVERT(date, reading_date) BETWEEN %s AND %s
        ORDER BY CONVERT(date, reading_date) ASC;
    """
    with connections[DEFAULT_CONN].cursor() as cur:
        cur.execute(sql, [start, end])
        rows = cur.fetchall()
    labels = [_fmt_label(r[0]) for r in rows]
    values = [float(r[1] or 0.0) for r in rows]
    return labels, values


# Utilities: Briquette — RANGE
def fetch_briquette_series_range(start: date, end: date) -> Tuple[List[str], List[float]]:
    sql = """
        SELECT CONVERT(date, reading_date) AS rdate,
               CAST(briquette_sb_3 AS float) AS briq
        FROM utility_records
        WHERE reading_type='BRIQUETTE'
          AND briquette_sb_3 IS NOT NULL
          AND CONVERT(date, reading_date) BETWEEN %s AND %s
        ORDER BY CONVERT(date, reading_date) ASC;
    """
    with connections[DEFAULT_CONN].cursor() as cur:
        cur.execute(sql, [start, end])
        rows = cur.fetchall()
    labels = [_fmt_label(r[0]) for r in rows]
    values = [float(r[1] or 0.0) for r in rows]
    return labels, values

# Utilities: Water (daily total) — RANGE with LAG
def fetch_water_series_range(start: date, end: date) -> Tuple[List[str], List[float]]:
    """
    MIDC meters are cumulative; compute daily consumption via LAG on date-aggregated maxima.
    First day in range may yield 0 due to lack of previous reading.
    """
    sql = """
        ;WITH bydate AS (
            SELECT CONVERT(date, reading_date) AS rdate,
                   MAX(CAST(midc_water_e_16 AS float)) AS e16,
                   MAX(CAST(midc_water_e_17 AS float)) AS e17,
                   MAX(CAST(midc_water_e_18 AS float)) AS e18,
                   MAX(CAST(midc_water_e_22 AS float)) AS e22
            FROM utility_records
            WHERE reading_type='MIDC reading'
              AND CONVERT(date, reading_date) BETWEEN %s AND %s
            GROUP BY CONVERT(date, reading_date)
        )
        SELECT rdate,
               (e16 - LAG(e16) OVER (ORDER BY rdate)) +
               (e17 - LAG(e17) OVER (ORDER BY rdate)) +
               (e18 - LAG(e18) OVER (ORDER BY rdate)) +
               (e22 - LAG(e22) OVER (ORDER BY rdate)) AS total_consumption
        FROM bydate
        ORDER BY rdate ASC;
    """
    with connections[DEFAULT_CONN].cursor() as cur:
        cur.execute(sql, [start, end])
        rows = cur.fetchall()
    labels, values = [], []
    for rdate_val, cons in rows:
        labels.append(_fmt_label(rdate_val))
        try: values.append(float(cons or 0.0))
        except Exception: values.append(0.0)
    return labels, values



# Utilities: SFR — RANGE with LAG
def fetch_sfr_series_range(start: date, end: date) -> Tuple[List[str], List[float]]:
    """
    SFR = (Δ sb_3_sub_fm_oc + deareator/1000) / briquette_sb_3
    Aggregate per date first, then LAG sb3 within the same range.
    """
    sql = """
        ;WITH bydate AS (
            SELECT CONVERT(date, reading_date) AS rdate,
                   MAX(CAST(sb_3_sub_fm_oc AS float)) AS sb3,
                   MAX(CAST(deareator      AS float)) AS deareator,
                   MAX(CAST(briquette_sb_3 AS float)) AS briq
            FROM dbo.utility_records
            WHERE (sb_3_sub_fm_oc IS NOT NULL OR deareator IS NOT NULL OR briquette_sb_3 IS NOT NULL)
              AND CONVERT(date, reading_date) BETWEEN %s AND %s
            GROUP BY CONVERT(date, reading_date)
        ),
        calc AS (
            SELECT rdate,
                   (sb3 - LAG(sb3) OVER (ORDER BY rdate)) AS sb3_cons,
                   (deareator / 1000.0)                   AS deareator_adj,
                   briq
            FROM bydate
        )
        SELECT rdate,
               ISNULL( (COALESCE(sb3_cons,0) + COALESCE(deareator_adj,0)) / NULLIF(briq,0), 0) AS sfr
        FROM calc
        ORDER BY rdate ASC;
    """
    with connections[DEFAULT_CONN].cursor() as cur:
        cur.execute(sql, [start, end])
        rows = cur.fetchall()
    labels, values = [], []
    for rdate_val, sfr in rows:
        labels.append(_fmt_label(rdate_val))
        try: values.append(float(sfr or 0.0))
        except Exception: values.append(0.0)
    return labels, values


def get_hse_context():
    """
    HSE (Lagging + Leading) strictly for the current fiscal year (Apr–Mar),
    with KPIs and separate lists for All/Open/Closed incidents.
    """
    today = timezone.localdate()
    fy_start = _fy_start(today)

    # FY filters only (no branch/location filtering)
    lag_qs = Lagging_Indicator.objects.filter(
        incident_date__gte=fy_start, incident_date__lte=today
    )
    lead_qs = LeadingRecords.objects.filter(
        observation_date__gte=fy_start, observation_date__lte=today
    )

    # KPIs
    kpi_total  = lag_qs.count()
    kpi_open   = lag_qs.filter(complience_status__iexact="Open").count()
    kpi_closed = lag_qs.filter(complience_status__iexact="Closed").count()

    vals = [
        "record_date","incident_date","incident_time","employee_type","department",
        "hse_lag_indicator","type_of_injury","injured_body_part","name_of_injured_person",
        "severity","likelihood","risk_factor","incident","immediate_action",
        "investigation_method","fact_about_men","fact_about_machine","fact_about_mother_nature",
        "fact_about_measurement","fact_about_method","fact_about_material","fact_about_history",
        "why_one","why_two","why_three","why_four","why_five",
        "direct_root_cause","indirect_root_cause","psm_failure",
        "date_resume_duty","mandays_lost","complience_status","complience_status_date",
        "physical_location__name","Contractor_name",
    ]

    lag_all    = list(lag_qs.values(*vals))
    lag_open   = list(lag_qs.filter(complience_status__iexact="Open").values(*vals))
    lag_closed = list(lag_qs.filter(complience_status__iexact="Closed").values(*vals))

    lead_rows = list(lead_qs.values(
        "observation_date","department","physical_location__name","leading_abnormality",
        "initiated_by","severity","likelihood","risk_factor","observation_description",
        "corrective_action","psl_member_name","responsible_person","root_cause","preventive_action",
        "target_date","status","remark"
    ))

    return {
        "kpis": {
            "fy_total": kpi_total,
            "open": kpi_open,
            "closed": kpi_closed,
            "fy_label": f"{fy_start.strftime('%d-%b-%Y')} to {today.strftime('%d-%b-%Y')}",
        },
        "lagging_all": lag_all,
        "lagging_open": lag_open,
        "lagging_closed": lag_closed,
        "leading": lead_rows,
    }

def fetch_hr_attendance_breakups(yday):
    """
    Return yesterday's DailyAttendance aggregations:
    - status wise (status_in_out)
    - shift wise  (shift_code)
    - department wise (department)
    - sub-department wise (sub_department)   # NEW
    Only for Branch = 'Solapur'
    """
    qs = DailyAttendance.objects.filter(
        attendance_date=yday,
        branch__iexact="Solapur"
    )

    by_status = (
        qs.values('status_in_out')
          .annotate(count=Count('id'))
          .order_by('-count', 'status_in_out')
    )
    by_shift = (
        qs.values('shift_code')
          .annotate(count=Count('id'))
          .order_by('-count', 'shift_code')
    )
    by_dept = (
        qs.values('department')
          .annotate(count=Count('id'))
          .order_by('-count', 'department')
    )
    # NEW: sub-department summary
    by_subdept = (
        qs.values('sub_department')
          .annotate(count=Count('id'))
          .order_by('-count', 'sub_department')
    )

    total = qs.count()

    def _norm_label(v, fallback):
        v = (v or '').strip()
        return v if v else fallback

    status_rows = [{'label': _norm_label(r['status_in_out'], '—'), 'count': r['count']} for r in by_status]
    shift_rows  = [{'label': _norm_label(r['shift_code'], '—'),     'count': r['count']} for r in by_shift]
    dept_rows   = [{'label': _norm_label(r['department'], '—'),     'count': r['count']} for r in by_dept]
    subdept_rows= [{'label': _norm_label(r['sub_department'], '—'), 'count': r['count']} for r in by_subdept]  # NEW

    return {
        'total': total,
        'status_rows': status_rows,
        'shift_rows':  shift_rows,
        'dept_rows':   dept_rows,
        'subdept_rows': subdept_rows,  # NEW
    }


def fetch_hr_dashboard(yday, branch="Solapur"):
    """
    HR snapshot for one day & branch with strict code rules:
      - Present  == exactly 'P | P'   (order-insensitive, i.e. two P tokens)
      - Absent   == exactly 'A | A'   (two A tokens)
      - WeeklyOff== all tokens 'WO'   (e.g. 'WO | WO' or single 'WO')
      - Holiday  == tokens subset of {'H','HD','HOLIDAY'}
      - Leave    == tokens subset of {'APL','L','LEAVE'} AND NO 'P'
      - OD       == any token 'OD'
    """
    import re, json, logging
    from collections import defaultdict
    from datetime import timedelta

    from django.db.models import Count, Sum, F, Q, Case, When, IntegerField

    log = logging.getLogger(__name__)

    # ---------- ultra-robust tokenization ----------
    _PIPE_ALTS = ["|", "｜", "¦", "│", "‖", "∣"]
    _WS_ALTS   = [
        "\u00A0", "\u1680", "\u2000", "\u2001", "\u2002", "\u2003", "\u2004",
        "\u2005", "\u2006", "\u2007", "\u2008", "\u2009", "\u200A", "\u202F",
        "\u205F", "\u3000", "\u200B", "\uFEFF"
    ]
    _TRANS = {ord(ch): " " for ch in _WS_ALTS}
    for alt in _PIPE_ALTS[1:]:
        _TRANS[ord(alt)] = "|"

    def _split_tokens(raw: str) -> list[str]:
        if raw is None:
            return []
        s = str(raw).translate(_TRANS).upper()
        s = re.sub(r"\s+", "", s)
        parts = s.split("|") if "|" in s else [s]
        parts = [re.sub(r"[^A-Z]", "", p) for p in parts]
        return [p for p in parts if p]

    HOLI_SET  = {"H", "HD", "HOLIDAY"}
    LEAVE_SET = {"APL", "L", "LEAVE"}

    def _classify_from_rows(rows):
        present = absent = leave = od = weekly_off = holiday = 0
        for r in rows:
            raw = r.get("status_in_out")
            c   = int(r.get("c") or 0)
            if c == 0:
                continue
            toks = _split_tokens(raw)
            if not toks:
                continue
            tset = set(toks)
            if tset == {"WO"}:
                weekly_off += c
            elif tset.issubset(HOLI_SET):
                holiday += c
            elif tset == {"A"} and len(toks) == 2:
                absent += c
            elif "OD" in tset:
                od += c
            elif tset.issubset(LEAVE_SET) and "P" not in tset:
                leave += c
            elif tset == {"P"} and len(toks) == 2:
                present += c
        return present, absent, leave, od, weekly_off, holiday

    # ---------- Attendance core ----------
    att_qs = DailyAttendance.objects.filter(attendance_date=yday, branch__iexact=branch)

    # Status distribution (classify from grouped rows for speed)
    stat_rows = list(att_qs.values("status_in_out").annotate(c=Count("id")))
    try:
        dbg = sorted(
            ({"raw": r["status_in_out"], "tokens": _split_tokens(r["status_in_out"]), "c": int(r["c"] or 0)}
             for r in stat_rows),
            key=lambda d: -d["c"]
        )[:10]
        log.debug("HR KPI code sample %s: %s", yday, dbg)
    except Exception:
        pass

    present, absent, leave_cnt, od_cnt, wo_cnt, hol_cnt = _classify_from_rows(stat_rows)

    # ---------- Headcount ----------
    hr_row = HRHeadcount.objects.filter(date=yday).first()
    headcount = (hr_row.total_employee if hr_row and hr_row.total_employee is not None
                 else att_qs.values("employee_code").distinct().count())

    # ---------- Late / Early-Go (DB-side, from DailyAttendance.Late_or_early) ----------
    late_early_agg = att_qs.aggregate(
        late_count=Sum(
            Case(
                When(Q(Late_or_early__istartswith="late"), then=1),
                default=0,
                output_field=IntegerField(),
            )
        ),
        early_go_count=Sum(
            Case(
                When(Q(Late_or_early__istartswith="early"), then=1),
                default=0,
                output_field=IntegerField(),
            )
        ),
    )
    late_count = int(late_early_agg.get("late_count") or 0)
    early_go_count = int(late_early_agg.get("early_go_count") or 0)

    # ---------- OT ----------
    ot_qs = OvertimeReport.objects.filter(attendance_date=yday, branch__iexact=branch)
    ot_minutes = ot_qs.aggregate(m=Sum("overtime_minutes"))["m"] or 0
    ot_emp = (ot_qs.exclude(overtime_minutes__isnull=True)
                  .exclude(overtime_minutes=0)
                  .values("employee_code").distinct().count())
    ot_hours = round(ot_minutes / 60.0, 1)

    # ---------- Requests ----------
    reg_by_status = list(
        AttendanceRegulation.objects
        .filter(attendance_date=yday, branch__iexact=branch)
        .values("request_status").annotate(count=Count("id")).order_by("-count")
    )
    od_by_status = list(
        On_Duty_Request.objects
        .filter(attendance_date=yday, branch__iexact=branch)
        .values("request_status").annotate(count=Count("id")).order_by("-count")
    )
    sl_by_status = list(
        ShortLeave.objects
        .filter(attendance_date=yday, branch__iexact=branch)
        .values("request_status").annotate(count=Count("id")).order_by("-count")
    )

    # ---------- Helpdesk ----------
    tickets_raised = Helpdesk_Ticket.objects.filter(
        raised_on__date=yday, branch__iexact=branch
    ).count()
    tickets_closed = Helpdesk_Ticket.objects.filter(
        closed_on__date=yday, branch__iexact=branch
    ).count()
    tickets_open_now = Helpdesk_Ticket.objects.filter(
        branch__iexact=branch
    ).exclude(status__iexact="Closed").count()

    # ---------- Check-in distribution ----------
    from collections import defaultdict as _dd
    ci_qs = DailyCheckIn.objects.filter(
        attendance_date=yday, branch__iexact=branch
    ).exclude(first_punch__isnull=True)
    buckets = _dd(int)
    for t in ci_qs.values_list("first_punch", flat=True):
        try:
            buckets[f"{int(t.hour):02d}:00"] += 1
        except Exception:
            continue
    ci_labels = sorted(buckets.keys())
    ci_values = [buckets[k] for k in ci_labels]

    # ---------- 14-day trend ----------
    from datetime import timedelta as _td
    start = yday - _td(days=13)
    trend_qs = DailyAttendance.objects.filter(
        attendance_date__range=(start, yday), branch__iexact=branch
    ).values("attendance_date", "status_in_out")

    by_date_rows = {}
    for r in trend_qs:
        d = r["attendance_date"]
        by_date_rows.setdefault(d, {})
        by_date_rows[d][r["status_in_out"]] = by_date_rows[d].get(r["status_in_out"], 0) + 1

    trend_dates = sorted(by_date_rows.keys())
    trend_labels, trend_present, trend_absent = [], [], []
    for d in trend_dates:
        rows = [{"status_in_out": raw, "c": cnt} for raw, cnt in by_date_rows[d].items()]
        p, a, _l, _od, _wo, _h = _classify_from_rows(rows)
        trend_labels.append(d.strftime("%d-%b"))
        trend_present.append(int(p))
        trend_absent.append(int(a))

    # ---------- Detail table (Employee) ----------
    # Alias the actual model field `Late_or_early` -> output key `late_early_go`
    hr_daily_rows_qs = (
        DailyAttendance.objects
        .filter(attendance_date=yday)  # (optionally also branch__iexact=branch)
        .annotate(late_early_go=F("Late_or_early"))
        .values(
            "employee_code",
            "full_name",
            "department",
            "designation",
            "grade",
            "shift_code",
            "sub_department",
            "branch",
            "status_in_out",
            "punch_in_punch_out_time",
            "late_early_go",
        )
        .order_by("department", "sub_department", "full_name")
    )
    hr_daily_rows_json = json.dumps(list(hr_daily_rows_qs), default=str)

    # =====================================================================
    #                       CONTRACT DASHBOARD  (NEW)
    #   Source: contract_employee_assignment (model: ContractEmployeeAssignment)
    #   Needs: Department / Location / Shift matrices + kpi count + raw rows
    # =====================================================================
    contract_count = 0
    contract_assignment_json = "[]"
    contract_matrices = {"by_department": [], "by_location": [], "by_shift": []}

    try:
        # Filter by day (use your branch filter here if table has a branch column)
        cbase = EmployeeAssignment.objects.filter(punch_date=yday)

        # KPI = unique workers assigned
        contract_count = cbase.values("employee_id").distinct().count()

        # Raw rows for client-side drill
        c_rows_qs = cbase.values(
            "punch_date",
            "department",
            "block_location",
            "shift",
            "punch_in",
            "punch_out",
            "assigned_date",
            "contractor_id",
            "employee_id",
            "is_reassigned",
        ).order_by("department", "block_location", "employee_id")
        contract_assignment_json = json.dumps(list(c_rows_qs), default=str)

        # Server-side matrices (handy if you want)
        by_dept = list(cbase.values(label=F("department")).annotate(count=Count("id")).order_by("-count", "label"))
        by_loc  = list(cbase.values(label=F("block_location")).annotate(count=Count("id")).order_by("-count", "label"))
        by_shift= list(cbase.values(label=F("shift")).annotate(count=Count("id")).order_by("-count", "label"))
        contract_matrices = {
            "by_department": by_dept,
            "by_location": by_loc,
            "by_shift": by_shift,
        }
    except Exception as e:
        log.exception("Contract dashboard build failed: %s", e)
        # fall back to zeros/empty JSON already set above
    
    # ---------- Final payload ----------
    return {
        "hr_daily_rows_json": hr_daily_rows_json,
        # Employee KPIs
        "kpis": {
            "headcount": headcount,
            "present": present,
            "absent": absent,
            "leave": leave_cnt,
            "od": od_cnt,
            "weekly_off": wo_cnt,
            "holiday": hol_cnt,
            "late": late_count,
            "early_go": early_go_count,
            "ot_emp": ot_emp,
            "ot_hours": ot_hours,
            "tickets_open": tickets_open_now,
            "tickets_raised": tickets_raised,
            "tickets_closed": tickets_closed,
        },
        "reg_by_status": reg_by_status,
        "od_by_status":  od_by_status,
        "sl_by_status":  sl_by_status,
        "checkin": {"labels": ci_labels, "values": ci_values, "total": sum(ci_values)},
        "top_late":  [],
        "top_early": [],
        "trend": {"labels": trend_labels, "present": trend_present, "absent": trend_absent},

        # ── Contract data for the Contract tab ──
        "contract_count": contract_count,                           # KPI tile
        "contract_assignment_json": contract_assignment_json,     # rows for JS
        "contract_matrices": contract_matrices,                     # optional server matrices
    }



from io import BytesIO
from django.http import HttpResponse
from openpyxl import Workbook

def _parse_ymd(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _approach_label(a: int) -> str:
    return {1: "FIFO", 3: "STAR"}.get(int(a or 0), "ROLL")

@login_required
def export_plan_batches_excel(request):
    """
    Export plan batches (Flask-identical structure) with TWO sheets:
      - AllData:    every fully-scheduled batch (no date filter), all IDs included
      - Filtered:   rows from AllData where batch_end ∈ [from, to] inclusive

    Optional filters:
      ?from=YYYY-MM-DD&to=YYYY-MM-DD   (used only for the Filtered sheet)
      ?fg_name=... (multi)             (limits schedules considered for both sheets)
    """
    # ---- range for Filtered sheet ----
    today = timezone.localdate()
    yday  = today - timedelta(days=1)

    raw_from = _parse_ymd(request.GET.get("from"))
    raw_to   = _parse_ymd(request.GET.get("to"))

    rng_to   = raw_to   or yday
    rng_from = raw_from or rng_to.replace(day=1)
    if rng_to > yday:
        rng_to = yday
    if rng_from > rng_to:
        rng_from, rng_to = rng_to, rng_from

    # datetimes (inclusive) for Filtered
    from_dt = datetime.combine(rng_from, datetime.min.time())
    to_dt   = datetime.combine(rng_to,   datetime.max.time())

    # ---- FG filter ----
    selected_fgs = set(s.strip() for s in request.GET.getlist("fg_name") if s and s.strip())

    # ---- load schedules ----
    schedules = load_plan_schedules([])
    if selected_fgs:
        schedules = [s for s in schedules if (s.product_id or "").strip() in selected_fgs]

    # ---- common header (exactly like Flask) ----
    headers = [
        "Schedule ID", "Doc No", "FG Name", "Stage", "Approach",
        "Batch #", "Gen Batch #", "Record Type",
        # run fields
        "Run #", "Equipment ID", "Std BCT (hrs)", "Wait Time (hrs)",
        "Run Start", "Run End", "Status", "Output Qty",
        "Batch Count",
        # material fields
        "Line Type", "Material Category", "Material Name",
        "Quantity", "Ratio", "Density", "Litre", "Include In Total",
    ]

    # We’ll build rows once (ALL data), then derive Filtered by date.
    all_rows = []  # each row is a tuple/list matching headers

    def _append_run_and_material_rows(ws_accumulator, sch, approach_lbl, batch, batch_count):
        """
        Build 'Run' rows and 'Material' rows for a batch, append to ws_accumulator (list of rows).
        Returns None (appends in place).
        """
        runs = batch.get("equipment_runs") or []
        for run_no, run in enumerate(runs, start=1):
            # Run row
            ws_accumulator.append([
                getattr(sch, "id", ""),
                getattr(sch, "doc_no", ""),
                getattr(sch, "product_id", ""),
                getattr(sch, "stage_name", ""),
                approach_lbl,
                batch.get("batch_no", ""),
                batch.get("generated_batch_number", ""),
                "Run",
                run_no,
                run.get("equipment_id", ""),
                run.get("std_bct", ""),
                run.get("wait_time", ""),
                run.get("start").strftime("%Y-%m-%d %H:%M:%S") if run.get("start") else "",
                run.get("end").strftime("%Y-%m-%d %H:%M:%S") if run.get("end") else "",
                run.get("status", ""),
                batch.get("output_quantity", 0.0),
                batch_count,
                "", "", "", "", "", "", "", "",
            ])

            # Material rows (materials + outputs + wastes)
            for ln in (batch.get("materials") or []) + (batch.get("outputs") or []) + (batch.get("wastes") or []):
                ws_accumulator.append([
                    getattr(sch, "id", ""),
                    getattr(sch, "doc_no", ""),
                    getattr(sch, "product_id", ""),
                    getattr(sch, "stage_name", ""),
                    approach_lbl,
                    batch.get("batch_no", ""),
                    batch.get("generated_batch_number", ""),
                    "Material",
                    run_no,
                    run.get("equipment_id", ""),
                    run.get("std_bct", ""),
                    run.get("wait_time", ""),
                    run.get("start").strftime("%Y-%m-%d %H:%M:%S") if run.get("start") else "",
                    run.get("end").strftime("%Y-%m-%d %H:%M:%S") if run.get("end") else "",
                    run.get("status", ""),
                    "",  # Output Qty blank on Material rows
                    batch_count,
                    ln.get("line_type", ""),
                    ln.get("material_category", ""),
                    ln.get("material_name", ""),
                    ln.get("quantity", ""),
                    ln.get("ratio", ""),
                    ln.get("density", ""),
                    ln.get("litre", ""),
                    ln.get("include_in_total", ""),
                ])

    # ---- build ALL rows (no date filter; still require fully scheduled like Flask) ----
    for sch in schedules:
        approach_lbl = _approach_label(getattr(sch, "scheduling_approach", 0))
        try:
            batches = generate_batch_report(sch)
        except Exception:
            batches = []

        # fully scheduled only (same as Flask)
        valid_batches = [
            b for b in batches
            if all((r.get("status") == "Scheduled") for r in (b.get("equipment_runs") or []))
        ]
        batch_count = len(valid_batches)

        for batch in valid_batches:
            _append_run_and_material_rows(all_rows, sch, approach_lbl, batch, batch_count)

    # ---- derive Filtered rows by batch_end within [from_dt, to_dt] ----
    # We need to look at the batch_end, so rebuild a quick index for batches per schedule.
    # To avoid regenerating, we’ll re-loop schedules once more but filter by date and then
    # build rows the same way.
    filtered_rows = []
    for sch in schedules:
        approach_lbl = _approach_label(getattr(sch, "scheduling_approach", 0))
        try:
            batches = generate_batch_report(sch)
        except Exception:
            batches = []

        fully_scheduled = [
            b for b in batches
            if all((r.get("status") == "Scheduled") for r in (b.get("equipment_runs") or []))
        ]
        in_range_batches = [
            b for b in fully_scheduled
            if isinstance(b.get("batch_end"), datetime) and (from_dt <= b["batch_end"] <= to_dt)
        ]
        batch_count = len(in_range_batches)

        for batch in in_range_batches:
            _append_run_and_material_rows(filtered_rows, sch, approach_lbl, batch, batch_count)

    # ---- write workbook: AllData + Filtered ----
    wb = Workbook()
    ws_all = wb.active
    ws_all.title = "AllData"
    ws_all.append(headers)
    for row in all_rows:
        ws_all.append(row)

    ws_fil = wb.create_sheet(title="Filtered")
    ws_fil.append(headers)
    for row in filtered_rows:
        ws_fil.append(row)

    # ---- stream back ----
    bio = BytesIO()
    wb.save(bio)
    bio.seek(0)

    fname = f"planning_report_all_data_and_filtered_{rng_from.isoformat()}_to_{rng_to.isoformat()}.xlsx"
    resp = HttpResponse(
        bio.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    resp["Content-Disposition"] = f'attachment; filename="{fname}"'
    return resp

# ─────────────────────────────────────────────────────────────────────
# EFFLUENT (KG) – Plan vs Actual for a date range + DEBUG printing
# ─────────────────────────────────────────────────────────────────────
def fetch_effluent_plan_actual_kg(
    mtd_start: date,
    yday: date,
    debug: bool = False,    # ← NEW
):
    """
    Effluent (KG) – Plan vs Actual for an arbitrary date range.
    Returns:
      (fg_mtd, fg_day, stg_mtd, stg_day, block_mtd, block_yday, yday_detail_rows, range_rows)
    """

    # ---- PLAN (scheduler) ----
    fg_mtd_plan: Dict[str, float] = {}
    fg_day_plan: Dict[str, float] = {}
    stg_mtd_plan: Dict[Tuple[str, str], float] = {}
    stg_day_plan: Dict[Tuple[str, str], float] = {}

    # NEW: plan batch-count dicts
    fg_mtd_plan_ct: Dict[str, int] = {}
    fg_day_plan_ct: Dict[str, int] = {}
    stg_mtd_plan_ct: Dict[Tuple[str, str], int] = {}
    stg_day_plan_ct: Dict[Tuple[str, str], int] = {}

    schedules = load_plan_schedules([])

    if debug:
        print("=" * 100)
        print(f"[EFF-DEBUG] PLAN: iterating schedules for {mtd_start} → {yday}")

    for sch in schedules:
        fg  = (sch.product_id or "").strip()
        stg = (sch.stage_name or "").replace("(STAGE-I)", "").strip()

        try:
            batches = generate_batch_report(sch)
        except Exception as e:
            if debug:
                print(f"[EFF-DEBUG] generate_batch_report FAILED for fg='{fg}', stage='{stg}': {e}")
            batches = []

        for b in batches:
            # keep batches that have at least one "Scheduled" run
            if not any(run.get("status") == "Scheduled" for run in b.get("equipment_runs", [])):
                continue

            be_dt = b.get("batch_end")
            try:
                be = be_dt.date() if isinstance(be_dt, datetime) else be_dt
            except Exception:
                continue

            if not (mtd_start <= be <= yday):
                continue

            # sum wastes for this batch (kg)
            plan_kg = 0.0
            for w in (b.get("wastes") or []):
                try:
                    plan_kg += float(w.get("quantity") or 0.0)
                except Exception:
                    continue

            if plan_kg == 0.0:
                if debug:
                    print(f"[EFF-DEBUG] PLAN row skipped (0 kg) fg='{fg}', stage='{stg}', be={be}")
                continue

            # FG & Stage RANGE totals
            fg_mtd_plan[fg] = fg_mtd_plan.get(fg, 0.0) + plan_kg
            sk = (fg, stg)
            stg_mtd_plan[sk] = stg_mtd_plan.get(sk, 0.0) + plan_kg

            # NEW: range batch counts
            fg_mtd_plan_ct[fg] = fg_mtd_plan_ct.get(fg, 0) + 1
            stg_mtd_plan_ct[sk] = stg_mtd_plan_ct.get(sk, 0) + 1

            # END-DAY (yday)
            if be == yday:
                fg_day_plan[fg] = fg_day_plan.get(fg, 0.0) + plan_kg
                stg_day_plan[sk] = stg_day_plan.get(sk, 0.0) + plan_kg

                # NEW: yday batch counts
                fg_day_plan_ct[fg] = fg_day_plan_ct.get(fg, 0) + 1
                stg_day_plan_ct[sk] = stg_day_plan_ct.get(sk, 0) + 1

            if debug:
                print(f"[EFF-DEBUG] PLAN row  fg='{fg}'  stage='{stg}'  be={be}  plan_kg={plan_kg:.3f}")

    # ---- ACTUAL aggregated (block-aware, in KG) ----
    agg_sql = """
        SELECT
            er.product_name,
            er.stage_name,
            CAST(er.record_date AS date)                         AS record_date,
            COALESCE(NULLIF(LTRIM(RTRIM(er.block)), ''), N'—')    AS block,
            SUM(COALESCE(eq.quantity_kg,0))                       AS qty_kg
        FROM effluent_qty eq
        JOIN effluent_records er ON er.id = eq.effluent_record_id
        WHERE CAST(er.record_date AS date) BETWEEN %s AND %s
        GROUP BY er.product_name, er.stage_name, CAST(er.record_date AS date), er.block;
    """

    fg_mtd_act: Dict[str, float] = {}
    fg_day_act: Dict[str, float] = {}
    stg_mtd_act: Dict[Tuple[str, str], float] = {}
    stg_day_act: Dict[Tuple[str, str], float] = {}
    block_mtd: Dict[str, float] = {}
    block_yday: Dict[str, float] = {}

    with connections[DEFAULT_CONN].cursor() as cur:
        cur.execute(agg_sql, [mtd_start, yday])
        rows = cur.fetchall()
        if debug:
            print("-" * 100)
            print(f"[EFF-DEBUG] ACTUAL-AGG rows (SUM(quantity_kg)) for {mtd_start} → {yday}: {len(rows)}")
        for fg, stg, rdate, block, qty_kg in rows:
            fgn = (fg or "").strip()
            stgn = (stg or "").replace("(STAGE-I)", "").strip()
            blk = (block or "—").strip() or "—"
            q = float(qty_kg or 0.0)

            # FG / Stage RANGE totals
            fg_mtd_act[fgn] = fg_mtd_act.get(fgn, 0.0) + q
            sk = (fgn, stgn)
            stg_mtd_act[sk] = stg_mtd_act.get(sk, 0.0) + q

            # END-DAY (yday) snapshot
            if rdate == yday:
                fg_day_act[fgn] = fg_day_act.get(fgn, 0.0) + q
                stg_day_act[sk] = stg_day_act.get(sk, 0.0) + q

            # Block aggregates
            block_mtd[blk] = block_mtd.get(blk, 0.0) + q
            if rdate == yday:
                block_yday[blk] = block_yday.get(blk, 0.0) + q

            if debug:
                print(f"[EFF-DEBUG] ACT row  fg='{fgn}'  stage='{stgn}'  date={rdate}  block='{blk}'  kg={q:.3f}")

    # ---- END-DAY (yday) DETAIL rows ----
    yday_sql = """
        SELECT
            CAST(er.record_date AS date)                         AS record_date,
            er.product_name,
            er.stage_name,
            er.batch_no,
            er.voucher_no,
            COALESCE(NULLIF(LTRIM(RTRIM(er.block)), ''), N'—')    AS block,
            eq.category,
            eq.effluent_nature,
            COALESCE(eq.plan_quantity,0)   AS plan_quantity,
            COALESCE(eq.actual_quantity,0) AS actual_quantity,
            COALESCE(eq.quantity_kg,0)     AS quantity_kg
        FROM effluent_qty eq
        JOIN effluent_records er ON er.id = eq.effluent_record_id
        WHERE CAST(er.record_date AS date) = %s;
    """
    yday_detail_rows: List[Dict] = []
    with connections[DEFAULT_CONN].cursor() as cur:
        cur.execute(yday_sql, [yday])
        cols = [c[0] for c in cur.description]
        yrows = cur.fetchall()
        if debug:
            print("-" * 100)
            print(f"[EFF-DEBUG] YDAY detail rows ({yday}): {len(yrows)} (showing up to 200)")
        for i, row in enumerate(yrows):
            r = {cols[j]: row[j] for j in range(len(cols))}
            yday_detail_rows.append(r)
            if debug and i < 200:
                print(f"[EFF-DEBUG] YDAY[{i+1:03}] {r}")

    # ---- RANGE DETAIL rows ----
    range_sql = """
        SELECT
            CAST(er.record_date AS date)                         AS record_date,
            er.product_name,
            er.stage_name,
            er.batch_no,
            er.voucher_no,
            COALESCE(NULLIF(LTRIM(RTRIM(er.block)), ''), N'—')    AS block,
            eq.category,
            eq.effluent_nature,
            COALESCE(eq.plan_quantity,0)   AS plan_quantity,
            COALESCE(eq.actual_quantity,0) AS actual_quantity,
            COALESCE(eq.quantity_kg,0)     AS quantity_kg
        FROM effluent_qty eq
        JOIN effluent_records er ON er.id = eq.effluent_record_id
        WHERE CAST(er.record_date AS date) BETWEEN %s AND %s;
    """
    range_rows: List[Dict] = []
    with connections[DEFAULT_CONN].cursor() as cur:
        cur.execute(range_sql, [mtd_start, yday])
        cols = [c[0] for c in cur.description]
        rrows = cur.fetchall()
        if debug:
            print("-" * 100)
            print(f"[EFF-DEBUG] RANGE detail rows ({mtd_start} → {yday}): {len(rrows)} (showing up to 200)")
        for i, row in enumerate(rrows):
            r = {cols[j]: row[j] for j in range(len(cols))}
            range_rows.append(r)
            if debug and i < 200:
                print(f"[EFF-DEBUG] RNG[{i+1:03}] {r}")

    # ---- ACTUAL batch counts (distinct batch_no) from RANGE detail rows ----
    fg_mtd_act_ct: Dict[str, int] = {}
    fg_day_act_ct: Dict[str, int] = {}
    stg_mtd_act_ct: Dict[Tuple[str, str], int] = {}
    stg_day_act_ct: Dict[Tuple[str, str], int] = {}

    seen_mtd_fg = set()          # (fg, batch_no)
    seen_mtd_stg = set()         # (fg, stage, batch_no)
    seen_day_fg = set()
    seen_day_stg = set()

    for r in range_rows:
        fg = (r.get("product_name") or "").strip()
        stg = (r.get("stage_name") or "").replace("(STAGE-I)", "").strip()
        bno = (r.get("batch_no") or "").strip()
        rdate = r.get("record_date")
        if not bno or not fg:
            continue

        k_fg = (fg, bno)
        k_stg = (fg, stg, bno)

        # Range (MTD/custom)
        if k_fg not in seen_mtd_fg:
            fg_mtd_act_ct[fg] = fg_mtd_act_ct.get(fg, 0) + 1
            seen_mtd_fg.add(k_fg)
        if k_stg not in seen_mtd_stg:
            stg_mtd_act_ct[(fg, stg)] = stg_mtd_act_ct.get((fg, stg), 0) + 1
            seen_mtd_stg.add(k_stg)

        # YDAY
        if rdate == yday:
            if k_fg not in seen_day_fg:
                fg_day_act_ct[fg] = fg_day_act_ct.get(fg, 0) + 1
                seen_day_fg.add(k_fg)
            if k_stg not in seen_day_stg:
                stg_day_act_ct[(fg, stg)] = stg_day_act_ct.get((fg, stg), 0) + 1
                seen_day_stg.add(k_stg)

    # ---- Pack into expected shapes (now with counts) ----
    fg_mtd: Dict[str, Dict[str, float]] = {}
    fg_day: Dict[str, Dict[str, float]] = {}
    all_fg_keys = set(fg_mtd_plan) | set(fg_day_plan) | set(fg_mtd_act) | set(fg_day_act)

    for fg in all_fg_keys:
        fg_mtd[fg] = {
            "plan":    fg_mtd_plan.get(fg, 0.0),
            "actual":  fg_mtd_act.get(fg, 0.0),
            "plan_ct": fg_mtd_plan_ct.get(fg, 0),
            "act_ct":  fg_mtd_act_ct.get(fg, 0),
        }
        fg_day[fg] = {
            "plan":    fg_day_plan.get(fg, 0.0),
            "actual":  fg_day_act.get(fg, 0.0),
            "plan_ct": fg_day_plan_ct.get(fg, 0),
            "act_ct":  fg_day_act_ct.get(fg, 0),
        }

    stg_mtd: Dict[Tuple[str, str], Dict[str, float]] = {}
    stg_day: Dict[Tuple[str, str], Dict[str, float]] = {}
    all_stg_keys = set(stg_mtd_plan) | set(stg_day_plan) | set(stg_mtd_act) | set(stg_day_act)

    for k in all_stg_keys:
        stg_mtd[k] = {
            "plan":    stg_mtd_plan.get(k, 0.0),
            "actual":  stg_mtd_act.get(k, 0.0),
            "plan_ct": stg_mtd_plan_ct.get(k, 0),
            "act_ct":  stg_mtd_act_ct.get(k, 0),
        }
        stg_day[k] = {
            "plan":    stg_day_plan.get(k, 0.0),
            "actual":  stg_day_act.get(k, 0.0),
            "plan_ct": stg_day_plan_ct.get(k, 0),
            "act_ct":  stg_day_act_ct.get(k, 0),
        }

    if debug:
        print("-" * 100)
        print("[EFF-DEBUG] PACKED TOTALS: FG MTD (plan/actual, ct)")
        for fg in sorted(fg_mtd.keys()):
            d = fg_mtd[fg]
            print(
                f"  FG='{fg}': "
                f"plan={d['plan']:.3f} ({d['plan_ct']})  "
                f"actual={d['actual']:.3f} ({d['act_ct']})"
            )
        print("-" * 100)
        print("[EFF-DEBUG] PACKED TOTALS: FG YDAY (plan/actual, ct)")
        for fg in sorted(fg_day.keys()):
            d = fg_day[fg]
            print(
                f"  FG='{fg}': "
                f"plan={d['plan']:.3f} ({d['plan_ct']})  "
                f"actual={d['actual']:.3f} ({d['act_ct']})"
            )
        print("=" * 100)

    return (
        fg_mtd, fg_day,
        stg_mtd, stg_day,
        block_mtd, block_yday,
        yday_detail_rows,
        range_rows,
    )


# top of file (near other utils)
import re

def _dom_key(s: str) -> str:
    """Uppercase, strip, and fold to a DOM-safe token for [data-*] attributes."""
    v = _norm(s)
    return re.sub(r'[^A-Z0-9]+', '-', v).strip('-') or 'X'


def build_effluent_rows_kg(
    fg_mtd, fg_day, stg_mtd, stg_day, alpha_map
) -> List[Dict]:
    fgs = sorted(
        {fg for fg in (set(fg_mtd.keys()) | set(fg_day.keys()))},
        key=lambda x: _norm(x)
    )
    eff_rows: List[Dict] = []
    for fg in fgs:
        alias = alpha_map.get(_norm(fg), fg)
        # NOW: include count fields with defaults
        p_mtd = fg_mtd.get(fg, {"plan": 0.0, "actual": 0.0, "plan_ct": 0, "act_ct": 0})
        p_day = fg_day.get(fg, {"plan": 0.0, "actual": 0.0, "plan_ct": 0, "act_ct": 0})

        # Collect stages for this FG
        stg_names = {
            stg for (f, stg) in stg_mtd.keys() if f == fg
        } | {
            stg for (f, stg) in stg_day.keys() if f == fg
        }
        parent_key = _dom_key(alias)  # <— consistent, alias-based

        stages = []
        for stg in sorted(stg_names, key=lambda s: _norm(s)):
            # Stage-level dicts also carry counts
            sm = stg_mtd.get((fg, stg), {"plan": 0.0, "actual": 0.0, "plan_ct": 0, "act_ct": 0})
            sd = stg_day.get((fg, stg), {"plan": 0.0, "actual": 0.0, "plan_ct": 0, "act_ct": 0})
            stages.append({
                "stage": stg,
                "prod_mtd": sm,
                "prod_day": sd,
                "parent_key": parent_key,   # <— ship to frontend
            })

        eff_rows.append({
            "fg": fg,
            "alias": alias,
            "row_key": parent_key,          # <— ship to frontend
            "prod_mtd": p_mtd,              # kg + ct
            "prod_day": p_day,              # kg + ct
            "stages": stages,
            "has_stages": bool(stages),
        })

    return eff_rows


def build_prod_grid(final_summary,
                    stage_breakdown,
                    months,
                    stock_fg_close,
                    stock_stage_close,
                    period):
    """
    Builds the production grid. For stock:
      - Prefer SFG (Semi Finished Good) stage stock by summing stock_stage_close[(FG, stage)]
        where the stage looks like SFG (endswith 'SFG' OR contains 'semi' & 'finished').
      - Fallback to stock_fg_close[FG] if no SFG stock is found.
    """
    def _cell_from(obj, mkey):
        d = obj.get(mkey, {})
        if period == "DAILY":
            return {
                "plan": float(d.get("plan", 0.0)),
                "est":  0.0,
                "act":  float(d.get("actual", 0.0)),
                "plan_ct": int(d.get("plan_ct", 0)),
                "est_ct":  0,
                "act_ct": int(d.get("act_ct", 0)),
                "wip":  float(d.get("wip", 0.0)),
                "var_pct": None,
            }
        else:
            est_val = float(d.get("estimated", 0.0))
            act_val = float(d.get("actual", 0.0))
            var_pct = ((act_val - est_val) / est_val * 100.0) if est_val else None
            est_ct_val = int(round(float(d.get("est_ct", 0.0) or 0.0)))
            return {
                "plan": float(d.get("plan", 0.0)),
                "est":  est_val,
                "act":  act_val,
                "plan_ct": int(d.get("plan_ct", 0)),
                "est_ct":  est_ct_val,
                "act_ct": int(d.get("act_ct", 0)),
                "wip":  float(d.get("wip", 0.0)),
                "var_pct": var_pct,
            }

    def _sum_cells(cells):
        plan =    sum(c["plan"]    for c in cells)
        est  =    sum(c["est"]     for c in cells)
        act  =    sum(c["act"]     for c in cells)
        plan_ct = sum(c["plan_ct"] for c in cells)
        est_ct  = sum(c["est_ct"]  for c in cells)
        act_ct  = sum(c["act_ct"]  for c in cells)
        wip     = sum(c["wip"]     for c in cells)
        var_pct = ((act - est) / est * 100.0) if (est and period != "DAILY") else None
        return {"plan": plan, "est": est, "act": act, "plan_ct": plan_ct, "est_ct": est_ct,
                "act_ct": act_ct, "wip": wip, "var_pct": var_pct}

    def _is_sfg_stage(stage: str) -> bool:
        s = _norm(stage)
        return s.endswith("SFG") or ("SEMI" in s and "FINISHED" in s)

    # Prefer SFG-stage stock (sum across all SFG-looking stages for the FG). Fallback to FG stock.
    def _stock_for_fg(fg_raw: str) -> float:
        sfg_sum = 0.0
        if stock_stage_close:
            for (kfg, kstage), q in stock_stage_close.items():
                if _norm(kfg) == _norm(fg_raw) and _is_sfg_stage(kstage):
                    try:
                        sfg_sum += float(q or 0.0)
                    except Exception:
                        pass
        if sfg_sum > 0.0:
            return sfg_sum
        return float(stock_fg_close.get(fg_raw, 0.0) or 0.0)

    alpha_map_local = fetch_alpha_map()
    fg_keys = sorted(
        ((alpha_map_local.get(_norm(fg), fg), fg) for fg in (final_summary.keys() | stage_breakdown.keys())),
        key=lambda x: (_norm(x[0]), _norm(x[1]))
    )

    rows = []
    g_totals = {"plan": 0.0, "est": 0.0, "act": 0.0, "wip": 0.0}
    g_stock = 0.0

    for _, fg in fg_keys:
        fs_map = final_summary.get(fg, {})
        fg_months = [_cell_from(fs_map, m) for m in months]

        # Default totals from summed months; allow __row_total__ override
        summed = _sum_cells(fg_months)
        rt = dict(fs_map.get("__row_total__", {}))

        fg_totals = {
            "plan": float(rt.get("plan", summed["plan"])),
            "est":  float(rt.get("estimated", summed["est"])),
            "act":  float(rt.get("actual", summed["act"])),
            "wip":  float(rt.get("wip", summed["wip"])),
        }
        fg_totals["var_pct"] = ((fg_totals["act"] - fg_totals["est"]) / fg_totals["est"] * 100.0) \
                               if (fg_totals["est"] and period != "DAILY") else None

        # ---- stage rows (with per-cell var_pct) ----
        st_rows = []
        for stg, meta in sorted((stage_breakdown.get(fg, {}) or {}).items(), key=lambda kv: _norm(kv[0])):
            mm = meta.get("months", {})
            st_months = [_cell_from(mm, m) for m in months]
            st_summed = _sum_cells(st_months)
            srt = dict(meta.get("__row_total__", {}))
            st_totals = {
                "plan": float(srt.get("plan", st_summed["plan"])),
                "est":  float(srt.get("estimated", st_summed["est"])),
                "act":  float(srt.get("actual", st_summed["act"])),
                "wip":  float(srt.get("wip", st_summed["wip"])),
            }
            st_totals["var_pct"] = ((st_totals["act"] - st_totals["est"]) / st_totals["est"] * 100.0) \
                                   if (st_totals["est"] and period != "DAILY") else None

            st_rows.append({
                "stage": stg,
                "type": meta.get("type") or "",
                "months": st_months,
                "totals": st_totals,
                "stock_close": float(stock_stage_close.get((fg, stg), 0.0)),
            })

        # FG-level stock: prefer SFG-stage sum, else FG stock
        fg_stock_close = _stock_for_fg(fg)

        # Your FG row reflection rule: if there is an SFG stage, use its plan/est/act/wip cells & totals for the FG row display.
        # (Stock still follows the SFG-preference above.)
        sfg_stage_row = next((sr for sr in st_rows if _is_sfg_stage(sr["stage"])), None)
        if sfg_stage_row:
            fg_months = sfg_stage_row["months"]
            fg_totals = sfg_stage_row["totals"]

        rows.append({
            "fg": fg,
            "alias": alpha_map_local.get(_norm(fg), fg),
            "months": fg_months,
            "totals": fg_totals,
            "stock_close": fg_stock_close,
            "has_stages": bool(st_rows),
            "stages": st_rows,
        })

        g_totals["plan"] += fg_totals["plan"]
        g_totals["est"]  += fg_totals["est"]
        g_totals["act"]  += fg_totals["act"]
        g_totals["wip"]  += fg_totals["wip"]
        g_stock          += fg_stock_close

    # Footer per-month totals (with var_pct)
    def _sum_cells_footer(cells):
        plan =    sum(c["plan"] for c in cells)
        est  =    sum(c["est"]  for c in cells)
        act  =    sum(c["act"]  for c in cells)
        wip  =    sum(c["wip"]  for c in cells)
        var_pct = ((act - est) / est * 100.0) if (est and period != "DAILY") else None
        return {"plan": plan, "est": est, "act": act, "wip": wip, "var_pct": var_pct}

    per_month = []
    for idx, _ in enumerate(months):
        pm_cells = [r["months"][idx] for r in rows]
        per_month.append(_sum_cells_footer(pm_cells))

    g_var_pct = ((g_totals["act"] - g_totals["est"]) / g_totals["est"] * 100.0) \
                if (g_totals["est"] and period != "DAILY") else None

    return {
        "period": period,
        "months": months,
        "rows": rows,
        "grand": {
            "per_month": per_month,
            "totals": {**g_totals, "var_pct": g_var_pct},
            "stock_total": g_stock
        },
    }


import csv
def compute_prod_pa_from_runs(
    schedules,
    start_dt: "datetime",
    end_dt: "datetime",
    period: str,   # "DAILY" | "MTD" | "YTD" | "CUSTOM"
):
    """
    Build plan vs actual/WIP tables for any selected date window.

    Monthly buckets (MTD/YTD/CUSTOM):
      - PLAN   (per month): full-month plan (all batches with batch_end in that month)
      - EST    (per month): plan Month-To-User-To-Date
            If month_end <= end_dt.date() -> EST = PLAN (full month)
            If month contains end_dt     -> EST = PLAN * (day(end_dt) / days_in_month)
      - ACTUAL (per month): actuals strictly from start_dt..end_dt

    DAILY buckets:
      - Same as before; plan/actual/WIP bucketed by day in [start_dt..end_dt].

    Returns:
      final_summary: { FG: { col_key: {plan,estimated,actual,wip,plan_ct,est_ct,act_ct}, "__row_total__": ... }, ... }
      stage_breakdown: { FG: { STAGE: { "type": "", "months": { col_key: {...} }, "__row_total__": ... }, ... }, ... }
      months: list[str]  # column labels (e.g., ["OCT-25","NOV-25"]) or per-day labels for DAILY, or ["YTD"]
    """
    import calendar
    from datetime import datetime, timedelta, date as _date
    from django.utils import timezone
    from django.db import connections

    # ---------------- helpers ----------------
    def _approach_label(v) -> str:
        try:
            v = int(v or 0)
        except Exception:
            v = 0
        return {1: "FIFO", 3: "STAR"}.get(v, "ROLL")

    def _to_local_naive(dt: datetime | None) -> datetime | None:
        if not dt:
            return None
        if isinstance(dt, datetime) and dt.tzinfo:
            return dt.astimezone(timezone.get_current_timezone()).replace(tzinfo=None)
        return dt if isinstance(dt, datetime) else None

    def _parse_any_dt(v):
        if isinstance(v, datetime):
            return _to_local_naive(v)
        if not v:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y%m%d", "%d-%m-%Y %H:%M:%S"):
            try:
                return datetime.strptime(str(v), fmt)
            except Exception:
                continue
        return None

    def _mon_key(dt: datetime) -> str:
        return dt.strftime("%b-%y").upper()

    def _day_key(dt: datetime) -> str:
        return dt.strftime("%d-%m-%y")

    def _days_in_month(d: _date) -> int:
        return calendar.monthrange(d.year, d.month)[1]

    def _month_bounds(d: _date) -> tuple[_date, _date]:
        first = _date(d.year, d.month, 1)
        last  = _date(d.year, d.month, _days_in_month(d))
        return first, last

    def _iter_month_starts(fr: _date, to: _date):
        cur = _date(fr.year, fr.month, 1)
        end = _date(to.year, to.month, 1)
        while cur <= end:
            yield cur
            if cur.month == 12:
                cur = _date(cur.year + 1, 1, 1)
            else:
                cur = _date(cur.year, cur.month + 1, 1)

    def _clean_stage(s: str) -> str:
        return (s or "").replace("(STAGE-I)", "").strip()

    p = (period or "MTD").upper()
    is_daily = (p == "DAILY")
    is_ytd   = (p == "YTD")

    # Window guards
    if end_dt < start_dt:
        start_dt, end_dt = end_dt, start_dt

    start_d = start_dt.date()
    end_d   = end_dt.date()

    # ------------- PLAN: flatten batches from schedules -------------
    flat_batches: list[dict] = []
    for sch in schedules:
        fg  = (getattr(sch, "product_id", "") or "").strip()
        stg = _clean_stage(getattr(sch, "stage_name", "") or "")
        typ = (getattr(sch, "type", "") or "").strip()
        app = _approach_label(getattr(sch, "scheduling_approach", 0))
        doc = getattr(sch, "doc_no", "")
        sid = getattr(sch, "id", None)
        try:
            batches = (generate_batch_report(sch) or [])
        except Exception:
            batches = []
        for b in batches:
            be = _to_local_naive(b.get("batch_end"))
            bs = _to_local_naive(b.get("batch_start"))
            if not isinstance(be, datetime):
                be = bs
            if not isinstance(be, datetime):
                continue
            flat_batches.append({
                "schedule_id": sid,
                "doc_no": doc,
                "fg": fg,
                "stage": stg,
                "type": typ,
                "approach": app,
                "batch_no": b.get("batch_no"),
                "gen_batch_no": b.get("generated_batch_number"),
                "qty": float(b.get("output_quantity") or 0.0),
                "end": be,
            })

    # Unique per (schedule_id, batch_no)
    seen: dict[tuple, dict] = {}
    for r in flat_batches:
        key = (r["schedule_id"], r["batch_no"])
        if key not in seen:
            seen[key] = r
    flat_batches = list(seen.values())

    # ------------- Column keys (buckets) -------------
    if is_daily:
        days = []
        cur = start_d
        while cur <= end_d:
            days.append(cur)
            cur += timedelta(days=1)
        cols_list = [_day_key(datetime(d.year, d.month, d.day)) for d in days]
    elif is_ytd:
        cols_list = ["YTD"]
    else:
        month_starts = list(_iter_month_starts(start_d, end_d))
        cols_list = [_mon_key(datetime(m.year, m.month, 1)) for m in month_starts]

    # ------------- Seed containers -------------
    final_summary: dict[str, dict] = {}
    stage_breakdown: dict[str, dict] = {}

    def _ensure_cell(container: dict, key: str) -> dict:
        return container.setdefault(
            key,
            {"plan": 0.0, "estimated": 0.0, "actual": 0.0, "wip": 0.0,
             "plan_ct": 0, "est_ct": 0.0, "act_ct": 0}
        )

    # ------------- Fill PLAN & EST (using user to-date) -------------
    if is_daily:
        # Daily: plan counts on the exact day of batch_end
        for b in flat_batches:
            be = b["end"].date()
            if not (start_d <= be <= end_d):
                continue
            ck = _day_key(b["end"])
            fgm = _ensure_cell(final_summary.setdefault(b["fg"], {}), ck)
            sb  = stage_breakdown.setdefault(b["fg"], {}).setdefault(b["stage"], {"type": b.get("type") or "", "months": {}})
            sm  = _ensure_cell(sb["months"], ck)

            fgm["plan"]      += b["qty"]
            fgm["plan_ct"]   += 1
            # For daily, estimated = plan for that day (no partial day estimate)
            fgm["estimated"] += b["qty"]
            fgm["est_ct"]    += 1.0

            sm["plan"]      += b["qty"]
            sm["plan_ct"]   += 1
            sm["estimated"] += b["qty"]
            sm["est_ct"]    += 1.0

    elif is_ytd:
        # YTD: single bucket "YTD"
        ytd_key = "YTD"
        # PLAN = sum of all batches whose batch_end month overlaps the fiscal window (we'll use start_dt..end_dt window)
        for b in flat_batches:
            be = b["end"].date()
            if not (start_d <= be <= end_d):
                continue
            fgm = _ensure_cell(final_summary.setdefault(b["fg"], {}), ytd_key)
            sb  = stage_breakdown.setdefault(b["fg"], {}).setdefault(b["stage"], {"type": b.get("type") or "", "months": {}})
            sm  = _ensure_cell(sb["months"], ytd_key)

            fgm["plan"]      += b["qty"]
            fgm["plan_ct"]   += 1
            sm["plan"]       += b["qty"]
            sm["plan_ct"]    += 1

        # EST = “to user’s end_dt” → since bucket is YTD over selected window, EST == PLAN (same window)
        for fg, mm in final_summary.items():
            cell = _ensure_cell(mm, ytd_key)
            # copy plan to estimated; est_ct mirrors plan_ct
            cell["estimated"] = cell["plan"]
            cell["est_ct"]    = float(cell["plan_ct"])
        for fg in stage_breakdown:
            for stg, info in stage_breakdown[fg].items():
                cell = _ensure_cell(info["months"], ytd_key)
                cell["estimated"] = cell["plan"]
                cell["est_ct"]    = float(cell["plan_ct"])
    else:
        # Monthly-style buckets (MTD/CUSTOM over multiple months)
        for m0 in _iter_month_starts(start_d, end_d):
            m_first, m_last = _month_bounds(m0)
            ck = _mon_key(datetime(m0.year, m0.month, 1))

            # PLAN: full month (all batches where batch_end falls within m_first..m_last)
            month_batches = [b for b in flat_batches if (m_first <= b["end"].date() <= m_last)]
            month_plan_sum_by_fg: dict[str, float] = {}
            month_plan_ct_by_fg:  dict[str, int]   = {}
            month_plan_sum_by_stg: dict[tuple[str,str], float] = {}
            month_plan_ct_by_stg:  dict[tuple[str,str], int]   = {}

            for b in month_batches:
                month_plan_sum_by_fg[b["fg"]] = month_plan_sum_by_fg.get(b["fg"], 0.0) + b["qty"]
                month_plan_ct_by_fg[b["fg"]]  = month_plan_ct_by_fg.get(b["fg"], 0) + 1
                key = (b["fg"], b["stage"])
                month_plan_sum_by_stg[key] = month_plan_sum_by_stg.get(key, 0.0) + b["qty"]
                month_plan_ct_by_stg[key]  = month_plan_ct_by_stg.get(key, 0) + 1

            # EST: Month-To-User-To-Date
            #   - If m_last <= end_d -> EST = PLAN (full month)
            #   - If m_first <= end_d < m_last -> proportion on that month
            if m_last <= end_d:
                est_factor = 1.0
            elif (m_first <= end_d <= m_last):
                est_factor = end_d.day / _days_in_month(end_d)
            else:
                est_factor = 0.0  # this month is after end_d (shouldn't happen due to range, but be safe)

            # Write FG cells
            for fg, p_sum in month_plan_sum_by_fg.items():
                fgm = _ensure_cell(final_summary.setdefault(fg, {}), ck)
                fgm["plan"]      += p_sum
                fgm["plan_ct"]   += month_plan_ct_by_fg.get(fg, 0)
                fgm["estimated"] += (p_sum * est_factor)
                # est_ct: scale count similarly; use min(plan_ct, estimated batches) but keep float
                fgm["est_ct"]    += month_plan_ct_by_fg.get(fg, 0) * est_factor

            # Write Stage cells
            for (fg, stg), p_sum in month_plan_sum_by_stg.items():
                sb = stage_breakdown.setdefault(fg, {}).setdefault(stg, {"type": "", "months": {}})
                sm = _ensure_cell(sb["months"], ck)
                sm["plan"]      += p_sum
                sm["plan_ct"]   += month_plan_ct_by_stg.get((fg, stg), 0)
                sm["estimated"] += (p_sum * est_factor)
                sm["est_ct"]    += month_plan_ct_by_stg.get((fg, stg), 0) * est_factor

    # ------------- ACTUAL from ERP (strictly start_dt..end_dt) -------------
    act_sql = r"""
        SELECT
            icf.sValue AS FG_Name,
            ITM.sName  AS Stage_Name,
            d.ddate    AS ISO_Date,
            SUM(CONVERT(DECIMAL(18,3), DET.dQty2)) AS Output_Quantity
        FROM txnhdr HDR
        JOIN TXNDET DET  ON HDR.lId = DET.lId AND DET.cFlag = 'I'
        JOIN ITMMST ITM  ON DET.lItmId = ITM.lId
        JOIN ITMCF  icf  ON DET.lItmId = icf.lId AND icf.sName='FG Name' AND icf.lLine=0
        CROSS APPLY (
            SELECT sval = CONVERT(varchar(50), HDR.dtDocDate)
        ) s
        CROSS APPLY (
            SELECT ddate = COALESCE(
                TRY_CONVERT(date, s.sval, 112),  -- yyyymmdd
                TRY_CONVERT(date, s.sval, 105),  -- dd-mm-yyyy
                TRY_CONVERT(date, s.sval, 103),  -- dd/mm/yyyy
                TRY_CONVERT(date, s.sval)        -- ISO-like
            )
        ) d
        WHERE HDR.ltypid IN (597,924,913,925,899,891)
          AND DET.lItmTyp <> 60
          AND HDR.lcompid  = 27
          AND HDR.bDel     = 0
          AND ITM.sName   <> 'MIX SOLVENT'
          AND d.ddate BETWEEN %s AND %s
        GROUP BY icf.sValue, ITM.sName, d.ddate;
    """
    with connections[ERP_CONN].cursor() as cur:
        cur.execute(act_sql, [start_d, end_d])
        act_rows = cur.fetchall()
        act_cols = [c[0].lower() for c in cur.description]

    a_fg    = act_cols.index("fg_name")
    a_stage = act_cols.index("stage_name")
    a_date  = act_cols.index("iso_date")
    a_qty   = act_cols.index("output_quantity")

    # Bucket Actual into the same column keys
    def _bucket_key_for_date(d: _date) -> str:
        if is_daily:
            return _day_key(datetime(d.year, d.month, d.day))
        elif is_ytd:
            return "YTD"
        else:
            return _mon_key(datetime(d.year, d.month, 1))

    for r in act_rows:
        fg  = (r[a_fg] or "").strip()
        stg = _clean_stage(r[a_stage] or "")
        d   = r[a_date]  # date
        q   = float(r[a_qty] or 0.0)
        ck  = _bucket_key_for_date(d)

        fgm = _ensure_cell(final_summary.setdefault(fg, {}), ck)
        fgm["actual"] += q
        fgm["act_ct"] += 1

        sb = stage_breakdown.setdefault(fg, {}).setdefault(stg, {"type": "", "months": {}})
        sm = _ensure_cell(sb["months"], ck)
        sm["actual"] += q
        sm["act_ct"] += 1

    # ------------- WIP (from dc_assets_inventory) using start_dt..end_dt -------------
    wip_sql = r"""
        SELECT
            out_item_id  AS FG_Name,
            out_stage    AS Stage_Name,
            TRY_CONVERT(datetime2, out_b_endtime)   AS EndAt,
            TRY_CONVERT(datetime2, out_b_starttime) AS StartAt,
            CONVERT(float, out_qty) AS Qty
        FROM dc_assets_inventory
        WHERE COALESCE(
                TRY_CONVERT(date, out_b_endtime),
                TRY_CONVERT(date, out_b_starttime)
              ) BETWEEN %s AND %s;
    """
    with connections[DEFAULT_CONN].cursor() as cur:
        cur.execute(wip_sql, [start_d, end_d])
        wip_rows = cur.fetchall()
        w_cols = [c[0].lower() for c in cur.description]

    w_fg    = w_cols.index("fg_name")
    w_stg   = w_cols.index("stage_name")
    w_end   = w_cols.index("endat")
    w_start = w_cols.index("startat")
    w_qty   = w_cols.index("qty")

    for r in wip_rows:
        fg  = (r[w_fg] or "").strip()
        stg = _clean_stage(r[w_stg] or "")
        t   = r[w_end] or r[w_start]
        if not isinstance(t, datetime):
            continue
        d   = t.date()
        ck  = _bucket_key_for_date(d)
        q   = float(r[w_qty] or 0.0)

        fgm = _ensure_cell(final_summary.setdefault(fg, {}), ck)
        fgm["wip"] += q

        sb = stage_breakdown.setdefault(fg, {}).setdefault(stg, {"type": "", "months": {}})
        sm = _ensure_cell(sb["months"], ck)
        sm["wip"] += q

    # ------------- Fill missing columns & row totals -------------
    for fg, mm in final_summary.items():
        # Ensure all requested columns exist
        for ck in cols_list:
            _ensure_cell(mm, ck)
        vals = [v for k, v in mm.items() if k != "__row_total__"]
        final_summary[fg]["__row_total__"] = {
            "plan":      sum(v["plan"]      for v in vals),
            "estimated": sum(v["estimated"] for v in vals),
            "actual":    sum(v["actual"]    for v in vals),
            "wip":       sum(v["wip"]       for v in vals),
            "plan_ct":   sum(v["plan_ct"]   for v in vals),
            "est_ct":    sum(v["est_ct"]    for v in vals),
            "act_ct":    sum(v["act_ct"]    for v in vals),
        }

    for fg in stage_breakdown:
        for stg, info in stage_breakdown[fg].items():
            for ck in cols_list:
                _ensure_cell(info["months"], ck)
            vals = list(info["months"].values())
            info["__row_total__"] = {
                "plan":      sum(v.get("plan", 0.0)      for v in vals),
                "estimated": sum(v.get("estimated", 0.0) for v in vals),
                "actual":    sum(v.get("actual", 0.0)    for v in vals),
                "wip":       sum(v.get("wip", 0.0)       for v in vals),
                "plan_ct":   sum(v.get("plan_ct", 0)     for v in vals),
                "est_ct":    sum(v.get("est_ct", 0.0)    for v in vals),
                "act_ct":    sum(v.get("act_ct", 0)      for v in vals),
            }

    months = cols_list if not is_ytd else ["YTD"]
    return final_summary, stage_breakdown, months



# ────────────────────────────────────────────────
# Dispatch: daily aggregations
# ────────────────────────────────────────────────
def _daterange(d1: date, d2: date):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)

def fetch_dispatch_plan_daily(rng_from: date, rng_to: date) -> dict[date, dict[str, float]]:
    """
    Returns {date: {FG_NAME_NORM: qty, ...}, ...} for plan rows within the range.
    """
    day_map: dict[date, dict[str, float]] = {}
    sql = f"""
        SELECT CAST(tentative_date AS date) AS pdate,
               RTRIM(LTRIM(material_name))  AS mat,
               CAST(SUM(qty) AS float)      AS qty
        FROM {DISPATCH_PLAN_TABLE}
        WHERE CAST(tentative_date AS date) BETWEEN %s AND %s
        GROUP BY CAST(tentative_date AS date), RTRIM(LTRIM(material_name));
    """
    with connections[DISPATCH_PLAN_CONN].cursor() as cur:
        cur.execute(sql, [rng_from, rng_to])
        for pdate, mat, qty in cur.fetchall():
            k = _norm(mat)
            day_map.setdefault(pdate, {})
            day_map[pdate][k] = day_map[pdate].get(k, 0.0) + float(qty or 0.0)
    # ensure empty days exist
    for d in _daterange(rng_from, rng_to):
        day_map.setdefault(d, {})
    return day_map

from datetime import date, datetime, timedelta
from django.db import connections

# ---------- Keys & labels (shared) ----------

def _disp_day_key(d: date) -> str:
    # Same format you render in the template for daily view
    return d.strftime("%d-%m-%y")

def _disp_mon_key(d: date) -> str:
    # Month header like JUN-25 (uppercase to match your other tables)
    return d.strftime("%b-%y").upper()


import re

def _disp_norm(s: str) -> str:
    """
    Normalize dispatch material names so plan & actual map to the same key.
    - Trim, uppercase
    - Unify weird spaces and dash variants
    - Collapse repeated spaces
    - Apply a few known ERP aliases (same as your SQL CASE)
    """
    if s is None:
        return ""

    v = str(s).upper().strip()

    # Unify dash/pipe lookalikes and whitespace
    v = v.replace("–", "-").replace("—", "-").replace("−", "-")
    v = v.replace("｜", "|").replace("¦", "|").replace("│", "|").replace("‖", "|").replace("∣", "|")
    v = re.sub(r"\s+", " ", v)               # collapse spaces
    v = re.sub(r"\s*-\s*", "-", v)           # tidy " - "
    v = re.sub(r"\s*\|\s*", "|", v)          # tidy " | "

    # Known ERP synonyms → canonical FG name
    if v in {"CANSOL", "DRP14/RM-15"}:
        return "ACETONCYANHYDRIN"

    return v


def _iter_month_starts(fr: date, to: date):
    """Yield the 1st of each month between fr..to inclusive."""
    cur = fr.replace(day=1)
    end = to.replace(day=1)
    while cur <= end:
        yield cur
        # add 1 month safely
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)

def _label_for_date(period: str, d: date) -> str:
    return _disp_day_key(d) if (period or "").upper() == "DAILY" else _disp_mon_key(d)

def _label_list(period: str, fr: date, to: date) -> list[str]:
    """Axis labels that exactly match _label_for_date bucketing."""
    p = (period or "MTD").upper()
    if p == "DAILY":
        n = (to - fr).days
        return [_disp_day_key(fr + timedelta(days=i)) for i in range(n + 1)]
    # else monthly buckets for MTD/YTD/CUSTOM
    return [_disp_mon_key(m) for m in _iter_month_starts(fr, to)]

def _label_for_date(period: str, d: date) -> str:
    return _disp_day_key(d) if (period or "").upper() == "DAILY" else _disp_mon_key(d)

# ---------- Dispatch: PLAN & ACTUAL series (per day) ----------
def fetch_dispatch_plan_series(fr: date, to: date) -> dict[date, dict[str, float]]:
    sql = f"""
        SELECT d.idate AS pdate,
               UPPER(RTRIM(LTRIM(dp.material_name))) AS mat,
               TRY_CONVERT(float, dp.qty)           AS qty
        FROM {DISPATCH_PLAN_TABLE} AS dp
        CROSS APPLY (
            -- Always stringify first; dp.tentative_date might be INT/DATE/TEXT
            SELECT sval = CONVERT(varchar(50), dp.tentative_date)
        ) AS s
        CROSS APPLY (
            SELECT idate = COALESCE(
                TRY_CONVERT(date, s.sval, 112),  -- yyyymmdd like 20250101
                TRY_CONVERT(date, s.sval, 105),  -- dd-mm-yyyy
                TRY_CONVERT(date, s.sval, 103),  -- dd/mm/yyyy
                TRY_CONVERT(date, s.sval)        -- last resort (already ISO-like)
            )
        ) AS d
        WHERE d.idate IS NOT NULL
          AND d.idate BETWEEN %s AND %s;
    """
    series: dict[date, dict[str, float]] = {}
    with connections[DISPATCH_PLAN_CONN].cursor() as cur:
        cur.execute(sql, [fr, to])
        for pdate, mat, qty in cur.fetchall():
            k = _disp_norm(mat)
            q = float(qty or 0.0)
            series.setdefault(pdate, {}).setdefault(k, 0.0)
            series[pdate][k] += q

    d = fr
    while d <= to:
        series.setdefault(d, {})
        d += timedelta(days=1)
    return series


def fetch_dispatch_actual_series(fr: date, to: date) -> dict[date, dict[str, float]]:
    sql = r"""
       SELECT
    UPPER(RTRIM(LTRIM(
        CASE
            WHEN ITM.sName IN ('CANSOL','DRP14/RM-15') THEN 'ACETONCYANHYDRIN'
            WHEN ITM.sName = 'F-ACID'                  THEN '4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE'
            ELSE ITM.sName
        END
    ))) AS mat,
    d.idate,
    SUM(DET.dQty) AS qty
FROM TXNHDR HDR
JOIN TXNDET DET ON DET.lId = HDR.lId AND DET.cFlag='I'
JOIN ITMMST ITM ON ITM.lId = DET.lItmId
JOIN ITMTYP ITP ON ITP.lTypId = DET.lItmTyp
CROSS APPLY (
    SELECT sval = CONVERT(varchar(50), HDR.dtDocDate)
) AS s
CROSS APPLY (
    SELECT idate = COALESCE(
        TRY_CONVERT(date, s.sval, 112),
        TRY_CONVERT(date, s.sval, 105),
        TRY_CONVERT(date, s.sval, 103),
        TRY_CONVERT(date, s.sval)
    )
) AS d
WHERE HDR.lTypId IN (409,497,498,499,500,504,650,654,824,825,826,827,828,829,939,940)
  AND HDR.bDel = 0 AND DET.bDel <> -2 AND HDR.lClosed = 0
  AND HDR.lCompId IN (27)
  AND ITP.sName = 'Finished Good'
  AND ITM.sName NOT IN ('SPENT HCL','ISONONANOYL CHLORIDE','URD')
  AND d.idate IS NOT NULL
  AND d.idate BETWEEN %s AND %s
GROUP BY
    UPPER(RTRIM(LTRIM(
        CASE
            WHEN ITM.sName IN ('CANSOL','DRP14/RM-15') THEN 'ACETONCYANHYDRIN'
            WHEN ITM.sName = 'F-ACID'                  THEN '4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE'
            ELSE ITM.sName
        END
    ))),
    d.idate;

    """
    series: dict[date, dict[str, float]] = {}
    with connections[ERP_CONN].cursor() as cur:
        cur.execute(sql, [fr, to])
        for mat, idate, qty in cur.fetchall():
            k = _disp_norm(mat)
            q = float(qty or 0.0)
            series.setdefault(idate, {}).setdefault(k, 0.0)
            series[idate][k] += q

    d = fr
    while d <= to:
        series.setdefault(d, {})
        d += timedelta(days=1)
    return series



def _daterange_days(fr: date, to: date):
    cur = fr
    while cur <= to:
        yield cur
        cur += timedelta(days=1)

def _full_outer_merge_series(plan_series: dict[date, dict[str, float]],
                             act_series: dict[date, dict[str, float]],
                             fr: date, to: date
                             ) -> tuple[dict[date, dict[str, float]], dict[date, dict[str, float]]]:
    """
    Make sure for every day in [fr..to], the union of FG keys from plan/act exists in both,
    filling missing values with 0.0. This simulates a FULL OUTER JOIN.
    """
    d = fr
    while d <= to:
        plan_series.setdefault(d, {})
        act_series.setdefault(d, {})
        # union of FGs for this date
        keys = set(plan_series[d].keys()) | set(act_series[d].keys())
        for k in keys:
            plan_series[d].setdefault(k, 0.0)
            act_series[d].setdefault(k, 0.0)
        d += timedelta(days=1)
    return plan_series, act_series

# ---------- Build period-aware dispatch grid ----------
def build_dispatch_grid(period: str,
                        fr: date,
                        to: date,
                        alpha_map: dict,
                        selected_fgs: list[str] | None = None,
                        stock_lookup: dict[str, float] | None = None,              # FG-level fallback
                        stock_stage_close: dict[tuple[str, str], float] | None = None  # NEW: per-(FG,Stage) stock
                        ):
    """
    Output (monthly modes: MTD/YTD/CUSTOM):
      {
        "months": [label...],  # month labels (e.g., JAN-25)
        "rows": [
          {
            "fg": raw_norm,
            "alias": alias,
            "cells": [
              {"plan":.., "est":.., "actual":.., "var_pct":..}, ...
            ],
            "stock_close": 123.45   # prefers SFG-stage stock; falls back to FG-level stock_lookup
          }
        ],
        "footer": {
          "months": [{"plan":..,"est":..,"actual":..,"var_pct":..}, ...]
        },
        "stock_total": 999.99
      }

    DAILY mode keeps the previous 2-field cells: {"plan","actual"} and still adds stock.
    """
    from django.utils import timezone
    import calendar

    def _is_sfg_stage(stage: str) -> bool:
        s = (_norm(stage) or "")
        return s.endswith("SFG") or ("SEMI" in s and "FINISHED" in s)

    def _stock_for_fg(fg_raw: str, fg_alias: str) -> float:
        """
        Prefer SFG-stage stock from stock_stage_close keyed by (FG, Stage).
        Match against either the raw or alias FG (normalized).
        Fallback to stock_lookup (which already maps raw/alias).
        """
        n_raw   = _norm(fg_raw)
        n_alias = _norm(fg_alias)

        # Prefer SFG sum if available
        if stock_stage_close:
            sfg_sum = 0.0
            for (kfg, kstage), q in stock_stage_close.items():
                nkfg = _norm(kfg)
                if nkfg == n_raw or nkfg == n_alias:
                    if _is_sfg_stage(kstage):
                        try:
                            sfg_sum += float(q or 0.0)
                        except Exception:
                            pass
            if sfg_sum > 0.0:
                return sfg_sum

        # Fallback to old FG-level lookup (already raw/alias aware)
        if stock_lookup:
            try:
                return float(
                    stock_lookup.get(n_alias, stock_lookup.get(n_raw, 0.0)) or 0.0
                )
            except Exception:
                return 0.0
        return 0.0

    p = (period or "MTD").upper()

    # ---------- DAILY stays the same ----------
    if p == "DAILY":
        plan_series, act_series = fetch_dispatch_series_full(fr, to)
        labels = _label_list(p, fr, to)

        # universe of FG keys
        all_mats: set[str] = set()
        for mats in plan_series.values(): all_mats |= set(mats.keys())
        for mats in act_series.values():  all_mats |= set(mats.keys())

        # optional FG filter (accept alias or raw)
        norm_sel: set[str] = set()
        if selected_fgs:
            norm_sel = {_norm(x) for x in selected_fgs}
            def _keep(k: str) -> bool:
                return (k in norm_sel) or (_norm(alpha_map.get(k, k)) in norm_sel)
            all_mats = {m for m in all_mats if _keep(m)}

        # bucketize to labels
        plan_bucket: dict[str, dict[str, float]] = {}
        act_bucket:  dict[str, dict[str, float]] = {}

        for d, mats in plan_series.items():
            b = _label_for_date(p, d)
            if b not in labels: continue
            for m, q in mats.items():
                if norm_sel:
                    alias = alpha_map.get(m, m)
                    if (m not in norm_sel) and (_norm(alias) not in norm_sel):
                        continue
                plan_bucket.setdefault(b, {}).setdefault(m, 0.0)
                plan_bucket[b][m] += float(q or 0.0)

        for d, mats in act_series.items():
            b = _label_for_date(p, d)
            if b not in labels: continue
            for m, q in mats.items():
                if norm_sel:
                    alias = alpha_map.get(m, m)
                    if (m not in norm_sel) and (_norm(alias) not in norm_sel):
                        continue
                act_bucket.setdefault(b, {}).setdefault(m, 0.0)
                act_bucket[b][m] += float(q or 0.0)

        ordered = sorted(((alpha_map.get(m, m), m) for m in all_mats),
                         key=lambda t: (_norm(t[0]), _norm(t[1])))
        rows: list[dict] = []
        for alias, raw in ordered:
            cells = []
            for lab in labels:
                pval = float(plan_bucket.get(lab, {}).get(raw, 0.0))
                aval = float(act_bucket.get(lab,  {}).get(raw, 0.0))
                cells.append({"plan": pval, "actual": aval})
            rows.append({"fg": raw, "alias": alias, "cells": cells})

        # attach stock per row (prefer SFG) and compute grand stock total
        stock_total = 0.0
        for r in rows:
            s = _stock_for_fg(r["fg"], r["alias"])
            r["stock_close"] = s
            stock_total += s

        footer_months = []
        for idx, _lab in enumerate(labels):
            tot_p = sum(r["cells"][idx]["plan"]   for r in rows) if rows else 0.0
            tot_a = sum(r["cells"][idx]["actual"] for r in rows) if rows else 0.0
            footer_months.append({"plan": tot_p, "actual": tot_a})

        return {
            "months": labels,
            "rows": rows,
            "footer": {"months": footer_months},
            "stock_total": stock_total,
        }

    # ---------- Monthly-style buckets (MTD / YTD / CUSTOM) ----------
    def _month_bounds(d: date) -> tuple[date, date]:
        first = d.replace(day=1)
        last_day = calendar.monthrange(d.year, d.month)[1]
        return first, d.replace(day=last_day)

    # axis labels for the UI (still based on fr..to months)
    month_starts = list(_iter_month_starts(fr, to))
    labels = [_disp_mon_key(m) for m in month_starts]
    if not labels:
        return {"months": [], "rows": [], "footer": {"months": []}, "stock_total": 0.0}

    first_month_start, _ = _month_bounds(month_starts[0])
    _, last_month_end    = _month_bounds(month_starts[-1])

    # cut-offs based on TODAY (not the selected 'to') for EST/Actual
    today_local = timezone.localdate()

    # PLAN needs full-month totals regardless of selection → fetch the entire span of months
    plan_full_series = fetch_dispatch_plan_series(first_month_start, last_month_end)

    # ACTUAL should be MTD to today (per month), so fetch up to min(last_month_end, today)
    act_series_end = min(last_month_end, today_local)
    act_mtd_series = fetch_dispatch_actual_series(first_month_start, act_series_end)

    # optional FG filter (alias-aware)
    norm_sel: set[str] = set()
    if selected_fgs:
        norm_sel = {_norm(x) for x in selected_fgs}

    # universe of FGs seen
    all_mats: set[str] = set()
    for mats in plan_full_series.values(): all_mats |= set(mats.keys())
    for mats in act_mtd_series.values():   all_mats |= set(mats.keys())

    if selected_fgs:
        def _keep(k: str) -> bool:
            return (k in norm_sel) or (_norm(alpha_map.get(k, k)) in norm_sel)
        all_mats = {m for m in all_mats if _keep(m)}

    # Pre-aggregate per-month buckets:
    #   - plan_month_bucket  : FULL month plan (always entire month)
    #   - est_month_bucket   : plan MTD → from month start to min(month_end, TODAY)
    #   - act_month_bucket   : actual MTD → from month start to min(month_end, TODAY)
    plan_month_bucket: dict[str, dict[str, float]] = {}
    est_month_bucket:  dict[str, dict[str, float]] = {}
    act_month_bucket:  dict[str, dict[str, float]] = {}

    def _accumulate_month(series_by_day: dict[date, dict[str, float]],
                          m_start: date, m_end: date, cut_end: date | None,
                          bucket: dict[str, dict[str, float]]):
        end_use = min(m_end, cut_end) if cut_end else m_end
        cur = m_start
        label = _disp_mon_key(m_start)
        while cur <= end_use:
            mats = series_by_day.get(cur, {}) or {}
            for m, q in mats.items():
                if selected_fgs:
                    alias = alpha_map.get(m, m)
                    if (m not in norm_sel) and (_norm(alias) not in norm_sel):
                        continue
                bucket.setdefault(label, {}).setdefault(m, 0.0)
                bucket[label][m] += float(q or 0.0)
            cur += timedelta(days=1)

    for m0 in month_starts:
        m_start, m_end = _month_bounds(m0)

        # PLAN = full month (no cut)
        _accumulate_month(plan_full_series, m_start, m_end, None, plan_month_bucket)

        # EST  = plan MTD (cut at TODAY, not the selected 'to')
        est_cut = min(m_end, today_local)
        _accumulate_month(plan_full_series, m_start, m_end, est_cut, est_month_bucket)

        # ACTUAL = actual MTD (cut at TODAY)
        act_cut = min(m_end, today_local)
        _accumulate_month(act_mtd_series, m_start, m_end, act_cut, act_month_bucket)

    # Build rows
    ordered = sorted(((alpha_map.get(m, m), m) for m in all_mats),
                     key=lambda t: (_norm(t[0]), _norm(t[1])))
    rows: list[dict] = []
    for alias, raw in ordered:
        cells = []
        for lab in labels:
            pval  = float((plan_month_bucket.get(lab, {}) or {}).get(raw, 0.0))
            eval_ = float((est_month_bucket.get(lab,  {}) or {}).get(raw, 0.0))
            aval  = float((act_month_bucket.get(lab,  {}) or {}).get(raw, 0.0))
            var_pct = ((aval - eval_) / eval_) if eval_ else 0.0
            cells.append({"plan": pval, "est": eval_, "actual": aval, "var_pct": var_pct})
        rows.append({"fg": raw, "alias": alias, "cells": cells})

    # Attach stock (prefer SFG) and compute grand stock total
    stock_total = 0.0
    for r in rows:
        s = _stock_for_fg(r["fg"], r["alias"])
        r["stock_close"] = s
        stock_total += s

    # Footer totals per month
    footer_months = []
    for idx, _lab in enumerate(labels):
        tot_p = sum(r["cells"][idx]["plan"]   for r in rows) if rows else 0.0
        tot_e = sum(r["cells"][idx]["est"]    for r in rows) if rows else 0.0
        tot_a = sum(r["cells"][idx]["actual"] for r in rows) if rows else 0.0
        tot_var = ((tot_a - tot_e) / tot_e) if tot_e else 0.0
        footer_months.append({"plan": tot_p, "est": tot_e, "actual": tot_a, "var_pct": tot_var})

    return {
        "months": labels,
        "rows": rows,
        "footer": {"months": footer_months},
        "stock_total": stock_total,
    }



def fetch_dispatch_series_full(fr: date, to: date) -> tuple[dict[date, dict[str, float]],
                                                            dict[date, dict[str, float]]]:
    """
    Returns (plan_series, act_series) where each is:
        { <date>: { <FG_NORMALIZED>: qty, ... }, ... }

    Follows the same pattern used elsewhere:
      - Reads PLAN via fetch_dispatch_plan_series(fr, to)
      - Reads ACTUAL via fetch_dispatch_actual_series(fr, to)
      - Performs a Python-side FULL OUTER merge so every date/material that
        appears on either side exists on both (missing -> 0.0).
    """
    # 1) Load daily per-FG series from each source
    plan_series = fetch_dispatch_plan_series(fr, to)   # {date: {FG->qty}}
    act_series  = fetch_dispatch_actual_series(fr, to) # {date: {FG->qty}}

    # 2) Ensure every day in range exists in both dicts
    for d in _daterange_days(fr, to):
        plan_series.setdefault(d, {})
        act_series.setdefault(d, {})

    # 3) Build the union of all materials seen in the window
    all_mats: set[str] = set()
    for d in _daterange_days(fr, to):
        for m in plan_series.get(d, ()): all_mats.add(m)
        for m in act_series.get(d, ()):  all_mats.add(m)

    # 4) For FULL OUTER semantics, ensure every (date, material) exists on both sides
    #    with a default 0.0 where missing.
    if all_mats:
        for d in _daterange_days(fr, to):
            p_row = plan_series.setdefault(d, {})
            a_row = act_series.setdefault(d, {})
            for m in all_mats:
                if m not in p_row:
                    p_row[m] = 0.0
                if m not in a_row:
                    a_row[m] = 0.0

    return plan_series, act_series

def _matches_selection(fg_raw, norm_selected, alpha_map):
    n_raw   = _norm(fg_raw)
    n_alias = _norm(alpha_map.get(n_raw, fg_raw))
    return (n_raw in norm_selected) or (n_alias in norm_selected)
from datetime import date
from dateutil.relativedelta import relativedelta

def month_floor(d: date) -> date:
    return date(d.year, d.month, 1)


def month_bins(from_date: date, to_date: date):
    """
    Inclusive month bins: [YYYY-MM, LABEL]
    LABEL is the SAME month (e.g., OCT-25).
    """
    start = month_floor(from_date)
    end   = month_floor(to_date)
    cur = start
    out = []
    while cur <= end:
        out.append({
            "key": cur.strftime("%Y-%m"),
            "label": cur.strftime("%b-%y").upper(),  # e.g., OCT-25
            "start": cur,
            "end": (cur + relativedelta(months=1)) - relativedelta(days=1),
        })
        cur = cur + relativedelta(months=1)
    return out

@login_required
def plan_vs_actual_daily(request):
    import json
    from datetime import datetime, timedelta
    from django.db.models import Q, Count, Sum, F
    from django.utils import timezone

    # ── enable profiler via ?profile=1
    prof = _Profiler(enabled=(request.GET.get("profile") == "1"))

    # ───────────────────────────────────────────────────────────────────
    # Base dates
    # ───────────────────────────────────────────────────────────────────
    today = timezone.localdate()
    yday  = today - timedelta(days=1)

    def _parse_ymd(s):
        if not s:
            return None
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None

    # HR attendance date (independent of production period)
    att_date = _parse_ymd(request.GET.get("att_date")) or yday
    att_date_iso = att_date.strftime("%Y-%m-%d")

    # Dates & Period  (MTD / YTD / DAILY / CUSTOM)
    raw_from_s = request.GET.get("from")
    raw_to_s   = request.GET.get("to")
    active_tab = (request.GET.get("tab") or "HSE").upper()
    raw_from   = _parse_ymd(raw_from_s)
    raw_to     = _parse_ymd(raw_to_s)

    period = (request.GET.get("period") or "MTD").upper()
    if period not in ("MTD", "YTD", "DAILY", "CUSTOM"):
        period = "MTD"

    user_typed_any_range = bool(raw_from_s or raw_to_s)

    # Default window: LAST MONTH start → YESTERDAY
    # (first day of previous month through yesterday)
    first_of_this_month = today.replace(day=1)
    prev_month_last_day = first_of_this_month - timedelta(days=1)
    default_from = prev_month_last_day.replace(day=1)
    base_to      = yday

    rng_to   = raw_to   or base_to
    rng_from = raw_from or default_from

    # Safety cap for future 'to' when no explicit typing
    if not user_typed_any_range and rng_to > yday:
        rng_to = yday

    if period == "DAILY":
        rng_from = rng_to
    elif user_typed_any_range:
        period = "CUSTOM"
        if raw_from and not raw_to:
            rng_to = yday
        if raw_to and not raw_from:
            rng_from = raw_to.replace(day=1)
    elif period == "MTD":
        # Keep 'MTD' semantics if user explicitly chose it.
        # Our default range already spans last+current month.
        # No override here so the default two-month view remains.
        pass
    elif period == "YTD":
        rng_from = _fy_start(rng_to)

    if rng_from > rng_to:
        rng_from, rng_to = rng_to, rng_from

    y_label     = rng_to.strftime("%d-%m-%y")
    range_label = f"{rng_from.strftime('%d-%m-%y')} → {y_label}"

    # ── Month header label for tables/cards (MMM-YY or MMM-YY → MMM-YY)
    month_label = _range_month_label(rng_from, rng_to, period)

    start_dt = datetime.combine(rng_from, datetime.min.time())
    end_dt   = datetime.combine(rng_to,   datetime.max.time())
    prof.mark("parsed dates & period")

    # ───────────────────────────────────────────────────────────────────
    # Quick links
    # ───────────────────────────────────────────────────────────────────
    mtd_start   = rng_to.replace(day=1)
    fy_start    = _fy_start(rng_to)
    cy_start    = rng_to.replace(month=1, day=1)
    last7_from  = rng_to - timedelta(days=6)
    last30_from = rng_to - timedelta(days=29)

    quick_links = {
        "yesterday": _build_url(request, **{"from": _to_ymd(rng_to),      "to": _to_ymd(rng_to),      "period": "DAILY"}),
        "last7":     _build_url(request, **{"from": _to_ymd(last7_from),  "to": _to_ymd(rng_to),      "period": "CUSTOM"}),
        "last30":    _build_url(request, **{"from": _to_ymd(last30_from), "to": _to_ymd(rng_to),      "period": "CUSTOM"}),
        "mtd":       _build_url(request, **{"from": _to_ymd(mtd_start),   "to": _to_ymd(rng_to),      "period": "MTD"}),
        "fytd":      _build_url(request, **{"from": _to_ymd(fy_start),    "to": _to_ymd(rng_to),      "period": "YTD"}),
        "cytd":      _build_url(request, **{"from": _to_ymd(cy_start),    "to": _to_ymd(rng_to),      "period": "CUSTOM"}),
        "custom":    _build_url(request, **{"period": "CUSTOM"}),
        "clear":     request.path,
    }

    # Filters / auth
    selected_fgs = request.GET.getlist("fg_name") or []
    user_groups  = list(request.user.groups.values_list("name", flat=True))
    is_superuser = bool(request.user.is_superuser)
    prof.mark("quick links & auth")

    # Alpha map
    alpha_map = fetch_alpha_map()
    prof.mark("fetch_alpha_map")

    # ───────────────────────────────────────────────────────────────────
    # PRODUCTION (Plan vs Actual)
    # ───────────────────────────────────────────────────────────────────
    prod_period = period if period in ("DAILY", "MTD", "YTD") else "MTD"
    schedules_all = load_plan_schedules([])
    prof.mark("load_plan_schedules")

    def _matches_selection(fg_raw, norm_selected, alpha_map):
        n_raw   = _norm(fg_raw)
        n_alias = _norm(alpha_map.get(n_raw, fg_raw))
        return (n_raw in norm_selected) or (n_alias in norm_selected)

    if selected_fgs:
        norm_sel = {_norm(x) for x in selected_fgs}
        schedules = [
            s for s in schedules_all
            if _matches_selection((s.product_id or "").strip(), norm_sel, alpha_map)
        ]
    else:
        schedules = schedules_all

    final_summary, stage_breakdown, months = compute_prod_pa_from_runs(
        schedules=schedules,
        start_dt=start_dt,
        end_dt=end_dt,
        period=prod_period,
    )
    prof.mark("compute_prod_pa_from_runs")

    # FG row should show ONLY SFG sums
    def _fg_from_sfg(stage_breakdown, months):
        out = {}
        for fg, stages in (stage_breakdown or {}).items():
            mm = {m: {"plan":0.0,"estimated":0.0,"actual":0.0,
                      "plan_ct":0,"est_ct":0,"act_ct":0} for m in months}
            for stg, info in stages.items():
                if not (stg or "").upper().endswith("SFG"):
                    continue
                for m in months:
                    cell = info["months"].get(m, {})
                    mm[m]["plan"]      += float(cell.get("plan", 0.0))
                    mm[m]["estimated"] += float(cell.get("estimated", 0.0))
                    mm[m]["actual"]    += float(cell.get("actual", 0.0))
                    mm[m]["plan_ct"]   += int(cell.get("plan_ct", 0))
                    mm[m]["est_ct"]    += int(cell.get("est_ct", 0))
                    mm[m]["act_ct"]    += int(cell.get("act_ct", 0))
            row_total = {
                "plan":      sum(v["plan"]      for v in mm.values()),
                "estimated": sum(v["estimated"] for v in mm.values()),
                "actual":    sum(v["actual"]    for v in mm.values()),
                "plan_ct":   sum(v["plan_ct"]   for v in mm.values()),
                "est_ct":    sum(v["est_ct"]    for v in mm.values()),
                "act_ct":    sum(v["act_ct"]    for v in mm.values()),
            }
            mm["__row_total__"] = row_total
            out[fg] = mm
        return out

    final_summary = _fg_from_sfg(stage_breakdown, months)
    prof.mark("aggregate SFG-only rows")

    # Stage→FG mapping for stock allocation
    stage_to_fgs = {}
    for sch in schedules:
        fg  = (sch.product_id or "").strip()
        stg = (sch.stage_name or "").replace("(STAGE-I)", "").strip()
        if fg and stg:
            stage_to_fgs.setdefault(_norm(stg), set()).add(fg)

    stock_stage_close, stock_fg_close = fetch_stock_closing(stage_to_fgs)
    prof.mark("fetch_stock_closing")

    prod_grid = build_prod_grid(
        final_summary=final_summary,
        stage_breakdown=stage_breakdown,
        months=months,
        stock_fg_close=stock_fg_close,
        stock_stage_close=stock_stage_close,
        period=prod_period,
    )
    prof.mark("build_prod_grid")

    if selected_fgs:
        norm_sel = {_norm(x) for x in selected_fgs}
        prod_grid["rows"] = [r for r in prod_grid["rows"] if _matches_selection(r["fg"], norm_sel, alpha_map)]
        # Recompute grand totals after row filter
        prod_grid["grand"]["stock_total"] = sum(r.get("stock_close", 0.0) for r in prod_grid["rows"])
        g_plan = sum(r["totals"].get("plan", 0.0) for r in prod_grid["rows"])
        g_act  = sum(r["totals"].get("act",  0.0) for r in prod_grid["rows"])
        prod_grid["grand"]["totals"]["plan"] = g_plan
        prod_grid["grand"]["totals"]["act"]  = g_act
        if prod_period == "MTD":
            g_est = sum(r["totals"].get("est",  0.0) for r in prod_grid["rows"])
            prod_grid["grand"]["totals"]["est"] = g_est
        else:
            prod_grid["grand"]["totals"]["est"] = 0.0
    prof.mark("filter prod_grid by FG (if any)")

    # Stock lookup for dispatch
    stock_lookup = {}
    for fg_raw, qty in (stock_fg_close or {}).items():
        n_raw   = _norm(fg_raw)
        alias   = alpha_map.get(n_raw, fg_raw)
        n_alias = _norm(alias)
        q = float(qty or 0.0)
        stock_lookup[n_raw]   = q
        stock_lookup[n_alias] = q

    # DISPATCH grid
    disp_grid = build_dispatch_grid(
        period=period if period in ("DAILY", "MTD", "YTD", "CUSTOM") else "MTD",
        fr=rng_from,
        to=rng_to,
        alpha_map=alpha_map,
        selected_fgs=selected_fgs or [],
        stock_lookup=stock_lookup,
        stock_stage_close=stock_stage_close,
    )
    prof.mark("build_dispatch_grid")

    # FG list(s) for dropdowns
    planned_fgs_raw  = {(s.product_id or "").strip() for s in schedules_all if (s.product_id or "").strip()}
    planned_aliases  = {alpha_map.get(_norm(fg), fg) for fg in planned_fgs_raw}
    grid_fgs_raw     = {r["fg"] for r in prod_grid.get("rows", [])}
    grid_aliases     = {alpha_map.get(_norm(fg), fg) for fg in grid_fgs_raw}
    dispatch_aliases = {r["alias"] for r in disp_grid.get("rows", [])}
    all_fg_list      = sorted(planned_aliases | grid_aliases | dispatch_aliases, key=_norm)

    fg_in_disp_now       = {r["alias"] for r in (disp_grid.get("rows") or [])}
    fg_available_alpha   = sorted(fg_in_disp_now, key=_norm)
    prof.mark("build FG dropdown lists")

    # Utilities (Power / Briquette / Water / SFR)
    power_labels, power_values = fetch_power_series_range(rng_from, rng_to)
    briq_labels,  briq_values  = fetch_briquette_series_range(rng_from, rng_to)
    water_labels, water_values = fetch_water_series_range(rng_from, rng_to)
    sfr_labels,   sfr_values   = fetch_sfr_series_range(rng_from, rng_to)
    prof.mark("fetch utilities (power/briq/water/sfr)")

    # Effluent (KG)
    debug = (request.GET.get("debug") == "1")
    (
        eff_fg_mtd, eff_fg_day,
        eff_stg_mtd, eff_stg_day,
        eff_block_mtd, eff_block_yday,
        eff_yday_rows, eff_range_rows,
    ) = fetch_effluent_plan_actual_kg(rng_from, rng_to, debug=debug)
    prof.mark("fetch effluent plan/actual (kg)")

    def _safe_f(x):
        try:
            return float(x or 0.0)
        except Exception:
            return 0.0

    def _with_alpha(rows_in):
        out = []
        for r in rows_in:
            prod = r.get("product_name")
            fg_alpha = alpha_map.get(_norm(prod or ""), prod)
            rr = dict(r)
            rr["fg"] = fg_alpha
            rr["product_name"] = fg_alpha
            rr["stage_name"] = rr.get("stage_name") or ""
            rr["batch_no"] = rr.get("batch_no") or ""
            rr["block"] = (rr.get("block") or "—").strip() or "—"
            rr["category"] = rr.get("category") or ""
            rr["effluent_nature"] = rr.get("effluent_nature") or ""
            rr["plan_quantity"] = _safe_f(rr.get("plan_quantity"))
            rr["actual_quantity"] = _safe_f(rr.get("actual_quantity"))
            rr["quantity_kg"] = _safe_f(rr.get("quantity_kg"))
            out.append(rr)
        return out

    eff_yday_rows_alpha  = _with_alpha(eff_yday_rows)
    eff_range_rows_alpha = _with_alpha(eff_range_rows)

    def _pa_by_dim(rows_in, dim_key: str):
        acc = {}
        seen = set()
        for r in rows_in:
            dim  = r.get(dim_key) or "—"
            bno  = (r.get("batch_no") or "").strip()
            plan = r.get("plan_quantity") or 0.0
            kg   = r.get("quantity_kg") or 0.0
            d = acc.setdefault(dim, {"plan": 0.0, "kg": 0.0})
            d["kg"] += kg
            k = (dim, bno or f"__{r.get('record_date')}_{r.get('product_name')}_{r.get('stage_name')}")
            if k not in seen:
                d["plan"] += plan
                seen.add(k)
        return acc

    def _dict_to_rows(d):
        rows_out = [{"label": k, "plan": round(v["plan"], 6), "kg": round(v["kg"], 6)} for k, v in d.items()]
        rows_out.sort(key=lambda x: (-x["kg"], _norm(x["label"])))
        return rows_out

    block_pa_mtd   = _pa_by_dim(eff_range_rows_alpha,  "block")
    block_pa_yday  = _pa_by_dim(eff_yday_rows_alpha,   "block")
    cat_pa_mtd     = _pa_by_dim(eff_range_rows_alpha,  "category")
    cat_pa_yday    = _pa_by_dim(eff_yday_rows_alpha,   "category")
    nature_pa_mtd  = _pa_by_dim(eff_range_rows_alpha,  "effluent_nature")
    nature_pa_yday = _pa_by_dim(eff_yday_rows_alpha,   "effluent_nature")

    eff_rows = build_effluent_rows_kg(
        eff_fg_mtd, eff_fg_day, eff_stg_mtd, eff_stg_day, alpha_map
    )
    prof.mark("effluent transform & groupings")

    # QC snapshot
    incoming_rows = get_incoming_rows_for(rng_to)   # same date the report uses

    qc_stage_status = (
        QCEntry.objects
        .values('product__name', 'stage')
        .annotate(
            approved  = Count('id', filter=Q(decision_status='approved')),
            variation = Count('id', filter=Q(decision_status='approved_under_deviation')),
            rejected  = Count('id', filter=Q(decision_status='rejected')),
        )
        .order_by('product__name', 'stage')
    )
    qc_grand_total = {
        'approved':  sum(item['approved']  for item in qc_stage_status),
        'variation': sum(item['variation'] for item in qc_stage_status),
        'rejected':  sum(item['rejected']  for item in qc_stage_status),
    }
    qc_grand_total['total'] = sum(qc_grand_total.values())
    prof.mark("QC snapshot")

    # HR & HSE  (use att_date)
    hr_att  = fetch_hr_attendance_breakups(att_date)
    hr_dash = fetch_hr_dashboard(att_date, branch="Solapur")
    hse     = get_hse_context()
    prof.mark("HR/HSE blocks")

    # Maintenance chips/table (same as your version)
    def _parse_ymd_or_none(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").date() if s else None
        except Exception:
            return None

    m_from_q = request.GET.get("m_from")
    m_to_q   = request.GET.get("m_to")
    m_from   = _parse_ymd_or_none(m_from_q)
    m_to     = _parse_ymd_or_none(m_to_q)

    if not m_from and not m_to:
        m_from = today
        m_to   = today
        mnt_active = "today"
    else:
        m_from = m_from or today
        m_to   = m_to or today
        if m_from > m_to:
            m_from, m_to = m_to, m_from
        this_monday = today - timedelta(days=today.weekday())
        if m_from == today and m_to == today:
            mnt_active = "today"
        elif m_from == (today + timedelta(days=1)) and m_to == (today + timedelta(days=1)):
            mnt_active = "tomorrow"
        elif m_from == this_monday and m_to == this_monday + timedelta(days=6):
            mnt_active = "thisweek"
        elif m_from == today.replace(day=1) and m_to == today:
            mnt_active = "mtd"
        elif m_from == _fy_start(today) and m_to == today:
            mnt_active = "fytd"
        else:
            mnt_active = ""

    mnt_qs = {
        "today":    _build_url(request, **{"m_from": _to_ymd(today),                 "m_to": _to_ymd(today)}),
        "tomorrow": _build_url(request, **{"m_from": _to_ymd(today + timedelta(days=1)),
                                           "m_to":   _to_ymd(today + timedelta(days=1))}),
        "thisweek": _build_url(request, **{"m_from": _to_ymd(today - timedelta(days=today.weekday())),
                                           "m_to":   _to_ymd(today - timedelta(days=today.weekday()) + timedelta(days=6))}),
        "mtd":      _build_url(request, **{"m_from": _to_ymd(today.replace(day=1)),  "m_to": _to_ymd(today)}),
        "fytd":     _build_url(request, **{"m_from": _to_ymd(_fy_start(today)),      "m_to": _to_ymd(today)}),
        "clear":    _build_url(request, **{"m_from": None, "m_to": None}),
    }
    mnt_label = f"{m_from.strftime('%d-%m-%y')} → {m_to.strftime('%d-%m-%y')}" if m_from != m_to else m_from.strftime('%d-%m-%y')

    mnt_qs_base = (
        MaintenanceSchedule.objects
        .annotate(eff_date=Coalesce('rescheduled_to', 'scheduled_date'))
        .filter(eff_date__range=(m_from, m_to))
        .order_by('equipment_id', 'scheduled_date')
    )
    mnt_rows = list(mnt_qs_base)
    mnt_total     = mnt_qs_base.count()
    mnt_scheduled = mnt_qs_base.filter(status=MaintenanceSchedule.STATUS_SCHEDULED).count()
    mnt_done      = mnt_qs_base.filter(status=MaintenanceSchedule.STATUS_DONE).count()
    mnt_postponed = mnt_qs_base.filter(status=MaintenanceSchedule.STATUS_POSTPONED).count()
    mnt_downtime_minutes = mnt_qs_base.aggregate(s=Sum('downtime_minutes'))['s'] or 0
    prof.mark("maintenance block")

    # ── finalize profiler rows for template + logs
    perf_rows, perf_total = prof.as_rows()
    prof.dump_to_log("Daily Operation — Profile")

    # Render
    resp = render(
        request,
        "reports/daily_operation.html",
        {
            # Dates / labels
            "period": period,
            "y_label": y_label,
            "mtd_label": range_label,
            "from_date": rng_from,
            "to_date": rng_to,
            "range_from": rng_from,
            "month_label": month_label,
            "range_to": rng_to,

            # HR attendance date (for date picker)
            "att_date_iso": att_date_iso,
            "att_date": att_date,

            # Filters / quick links
            "quick_links": quick_links,
            "all_fg_list": all_fg_list,
            "fg_available_alpha": fg_available_alpha,
            "selected_fgs": selected_fgs,

            # Production
            "prod_grid": prod_grid,

            # Dispatch
            "disp_grid": disp_grid,

            # Utilities
            "power_labels": power_labels,
            "power_values": power_values,
            "briq_labels": briq_labels,
            "briq_values": briq_values,
            "water_labels": water_labels,
            "water_values": water_values,
            "sfr_labels": sfr_labels,
            "sfr_values": sfr_values,

            # Effluent
            "eff_rows": eff_rows,
            "eff_yday_rows_json":  json.dumps(eff_yday_rows_alpha,  default=str),
            "eff_range_rows_json": json.dumps(eff_range_rows_alpha, default=str),
            "eff_block_pa_mtd":  _dict_to_rows(block_pa_mtd),
            "eff_block_pa_yday": _dict_to_rows(block_pa_yday),
            "eff_cat_pa_mtd":    _dict_to_rows(cat_pa_mtd),
            "eff_cat_pa_yday":   _dict_to_rows(cat_pa_yday),
            "eff_nat_pa_mtd":    _dict_to_rows(nature_pa_mtd),
            "eff_nat_pa_yday":   _dict_to_rows(nature_pa_yday),

            # QC
            "qc_incoming_rows": incoming_rows,
            "qc_stage_status": qc_stage_status,
            "qc_grand_total": qc_grand_total,

            # HR
            "hr_att": hr_att,
            "hr_dash": hr_dash,
            "hr_checkin_labels": json.dumps(hr_dash["checkin"]["labels"]),
            "hr_checkin_values": json.dumps(hr_dash["checkin"]["values"]),
            "hr_trend_labels":   json.dumps(hr_dash["trend"]["labels"]),
            "hr_trend_present":  json.dumps(hr_dash["trend"]["present"]),
            "hr_trend_absent":   json.dumps(hr_dash["trend"]["absent"]),
            "hr_daily_rows_json": hr_dash["hr_daily_rows_json"],

            # HSE
            "hse": hse,
            "hse_kpis": hse["kpis"],
            "hse_lag_all_json":    json.dumps(hse["lagging_all"],    default=str),
            "hse_lag_open_json":   json.dumps(hse["lagging_open"],   default=str),
            "hse_lag_closed_json": json.dumps(hse["lagging_closed"], default=str),
            "hse_leading_json":    json.dumps(hse["leading"],        default=str),

            # Maintenance
            "mnt_rows": mnt_rows,
            "mnt_kpi": {
                "total": mnt_total,
                "scheduled": mnt_scheduled,
                "done": mnt_done,
                "postponed": mnt_postponed,
            },
            "mnt_downtime_minutes": mnt_downtime_minutes,
            "mnt_from": m_from,
            "mnt_to": m_to,
            "mnt_label": mnt_label,
            "mnt_qs": mnt_qs,
            "mnt_active": mnt_active,

            # Auth
            "user_groups": user_groups,
            "is_superuser": is_superuser,

            "active_tab": active_tab,

            # Profiler data for UI
            "perf": {
                "enabled": prof.enabled,
                "rows": perf_rows,       # [{step, ms, cum_ms}, ...]
                "total_ms": perf_total,
            },

            # Needed by some templates
            "request": request,
        },
    )
    prof.mark("render context build")  # (for local debugging; not visible post-return)
    return resp



from datetime import datetime, timedelta
from io import BytesIO
from collections import defaultdict
from django.db import connections
from django.http import HttpResponse
from django.contrib.auth.decorators import login_required
import pandas as pd


@login_required
def export_prod_debug_excel(request):
    DB_ALIAS   = "production_scheduler"
    SCHED_TBL  = "production_schedule"
    LINES_TBL  = "production_schedule_lines"

    conn = connections[DB_ALIAS]

    # ---------- tiny helpers ----------
    def fetchall_dict(cur):
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def table_cols(table):
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COLUMN_NAME, DATA_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE LOWER(TABLE_NAME) = LOWER(%s)
            """, [table])
            return {r["COLUMN_NAME"]: r["DATA_TYPE"] for r in fetchall_dict(cur)}

    # ---------- load schedules (ALL IDs, no filtering) ----------
    sched_cols = table_cols(SCHED_TBL)
    need_sched = [
        "id","doc_no","transaction_date","type","bom_id","product_id",
        "stage_name","block","production_quantity","equipment_id",
        "equipment_capacity","bct_in_hrs","no_of_batches","batch_size",
        "start_date","end_date","wait_time","batch_number","scheduling_approach",
        "bom_name","last_edit_reason","revise","edit_date","edit_counts"
    ]
    sel_sched = [f"[{c}]" for c in need_sched if c in sched_cols]
    if not sel_sched:
        sel_sched = ["*"]  # fallback

    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(sel_sched)} FROM [{SCHED_TBL}]")
        schedules = fetchall_dict(cur)

    # ---------- load lines (ALL) ----------
    line_cols = table_cols(LINES_TBL)
    need_line = [
        "id","schedule_id","line_type","material_category","material_name",
        "quantity","ratio","density","litre","include_in_total","closed","closed_date",
        "equipment_id","std_bct","wait_time","equipment_type","capacity_size","moc_equipment","star","closed_reason",
    ]
    sel_lines = [f"[{c}]" for c in need_line if c in line_cols]
    if not sel_lines:
        sel_lines = ["*"]

    with conn.cursor() as cur:
        cur.execute(f"SELECT {', '.join(sel_lines)} FROM [{LINES_TBL}]")
        line_rows = fetchall_dict(cur)

    lines_by_sched = defaultdict(list)
    for r in line_rows:
        lines_by_sched[r.get("schedule_id")].append(r)

    # ---------- batch generator (inline) ----------
    def generate_batch_rows_for_sched(hdr, lines):
        from datetime import datetime as _dt, timedelta

        n_batches = int(float(hdr.get("no_of_batches") or 0))
        approach  = int(hdr.get("scheduling_approach") or 0)     # 0 ROLL, 1 FIFO, 3 STAR
        start_ts  = hdr.get("start_date")
        if start_ts and not isinstance(start_ts, _dt):
            try:
                start_ts = _dt.fromisoformat(str(start_ts))
            except Exception:
                start_ts = None

        # per-batch output qty from first output line
        out_ln = next((l for l in lines if str(l.get("line_type") or "").lower()=="output"), None)
        per_batch = round(float(out_ln.get("quantity") or 0)) if out_ln else 0

        def mapt(l):
            return dict(
                line_type        = l.get("line_type"),
                material_category= l.get("material_category"),
                material_name    = l.get("material_name"),
                quantity         = l.get("quantity"),
                ratio            = l.get("ratio"),
                density          = l.get("density"),
                litre            = l.get("litre"),
                include_in_total = l.get("include_in_total"),
            )
        mats = [mapt(l) for l in lines if str(l.get("line_type") or "").lower()=="input"]
        outs = [mapt(l) for l in lines if str(l.get("line_type") or "").lower()=="output"]
        wsts = [mapt(l) for l in lines if str(l.get("line_type") or "").lower()=="waste"]

        eq_lines = [l for l in lines if str(l.get("line_type") or "").lower()=="equipment"]
        if not eq_lines or n_batches < 1 or not start_ts:
            return []

        def truthy(v):
            return str(v or "").strip().lower() in ("1","true","yes","y","t")

        eq_state = [{
            "equipment_id": l.get("equipment_id"),
            "std":          float(l.get("std_bct") or 0.0),
            "wait":         float(l.get("wait_time") or 0.0),
            "next":         start_ts,
            "star":         truthy(l.get("star")),
            "closed_date":  l.get("closed_date")
        } for l in eq_lines]

        base = hdr.get("batch_number") or ""
        if len(base) >= 2 and base[-2:].isdigit():
            prefix, start_no = base[:-2], int(base[-2:])
            gen_num = lambda i: prefix + str(start_no + i).zfill(2)
        else:
            gen_num = lambda i: str(i).zfill(2)

        batches = []

        # FIFO
        if approach == 1:
            for i in range(1, n_batches + 1):
                cell = min(eq_state, key=lambda x: x["next"])
                st   = cell["next"]
                et   = st + timedelta(hours=cell["std"])
                cancel = (cell["closed_date"] and et > cell["closed_date"])
                status = "Cancelled" if cancel else "Scheduled"
                cell["next"] = et + timedelta(hours=cell["wait"])
                batches.append({
                    "batch_no":               i,
                    "generated_batch_number": gen_num(i),
                    "batch_start":            st,
                    "batch_end":              et,
                    "output_quantity":        per_batch,
                    "equipment_runs": [{
                        "equipment_id": cell["equipment_id"],
                        "std_bct":      cell["std"],
                        "wait_time":    cell["wait"],
                        "star":         cell["star"],
                        "start":        st,
                        "end":          et,
                        "status":       status
                    }],
                    "materials":              mats,
                    "outputs":                outs,
                    "wastes":                 wsts,
                })

        # ROLL
        elif approach == 0:
            pipeline = [dict(e) for e in eq_state]
            for i in range(1, n_batches + 1):
                runs, prev_end = [], None
                for cell in pipeline:
                    st = max(prev_end or cell["next"], cell["next"])
                    et = st + timedelta(hours=cell["std"])
                    cancel = (cell["closed_date"] and et > cell["closed_date"])
                    status = "Cancelled" if cancel else "Scheduled"
                    cell["next"] = et + timedelta(hours=cell["wait"])
                    if not cancel:
                        prev_end = cell["next"]
                    runs.append({
                        "equipment_id": cell["equipment_id"],
                        "std_bct":      cell["std"],
                        "wait_time":    cell["wait"],
                        "star":         cell["star"],
                        "start":        st,
                        "end":          et,
                        "status":       status
                    })
                batches.append({
                    "batch_no":               i,
                    "generated_batch_number": gen_num(i),
                    "batch_start":            runs[0]["start"],
                    "batch_end":              runs[-1]["end"],
                    "output_quantity":        per_batch,
                    "equipment_runs":         runs,
                    "materials":              mats,
                    "outputs":                outs,
                    "wastes":                 wsts,
                })

        # STAR
        elif approach == 3:
            stars = [e for e in eq_state if e["star"]][:2]
            if len(stars) < 2:
                need = 2 - len(stars)
                for e in eq_state:
                    if not e["star"] and need:
                        e["star"] = True
                        stars.append(e)
                        need -= 1
            A, B = stars[0], stars[1]
            B["next"] = start_ts + timedelta(hours=B["std"] / 2)
            for i in range(1, n_batches + 1):
                omit = B if (i & 1) else A
                seq  = [e for e in eq_state if e is not omit]
                runs, prev_end = [], None
                for cell in seq:
                    st = max(prev_end or cell["next"], cell["next"])
                    et = st + timedelta(hours=cell["std"])
                    cancel = (cell["closed_date"] and et >= cell["closed_date"])
                    status = "Cancelled" if cancel else "Scheduled"
                    cell["next"] = et + timedelta(hours=cell["wait"])
                    if not cancel:
                        prev_end = cell["next"]
                    runs.append({
                        "equipment_id": cell["equipment_id"],
                        "std_bct":      cell["std"],
                        "wait_time":    cell["wait"],
                        "star":         cell["star"],
                        "start":        st,
                        "end":          et,
                        "status":       status
                    })
                batches.append({
                    "batch_no":               i,
                    "generated_batch_number": gen_num(i),
                    "batch_start":            runs[0]["start"],
                    "batch_end":              runs[-1]["end"],
                    "output_quantity":        per_batch,
                    "equipment_runs":         runs,
                    "materials":              mats,
                    "outputs":                outs,
                    "wastes":                 wsts,
                })
        return batches

    def approach_label(a): return {1: "FIFO", 3: "STAR"}.get(int(a or 0), "ROLL")

    # ---------- flatten to ONE sheet, with your filter logic ----------
    headers = [
        "Schedule ID","Doc No","FG Name","Stage","Approach",
        "Batch #","Gen Batch #","Equipment ID",
        "Run #","Std BCT (hrs)","Wait Time (hrs)",
        "Run Start","Run End","Status",
        "Output Qty","Include In Sum","Counted Output Qty"
    ]
    rows = []

    for hdr in schedules:
        sid   = hdr.get("id")
        appr  = approach_label(hdr.get("scheduling_approach"))
        lines = lines_by_sched.get(sid, [])
        batches = generate_batch_rows_for_sched(hdr, lines)

        for b in batches:
            # keep only batches with at least one Scheduled run
            scheduled_runs = [r for r in b["equipment_runs"] if r["status"] == "Scheduled"]
            if not scheduled_runs:
                continue

            if appr in ("ROLL", "STAR"):
                # pick ONE equipment (first scheduled run) and count its batch once
                sel = scheduled_runs[0]
                rows.append([
                    sid, hdr.get("doc_no"), (hdr.get("product_id") or "").strip(),
                    hdr.get("stage_name"), appr,
                    b["batch_no"], b["generated_batch_number"], sel["equipment_id"],
                    1, sel["std_bct"], sel["wait_time"],
                    sel["start"], sel["end"], sel["status"],
                    b["output_quantity"], "Yes", b["output_quantity"]  # count once
                ])
                # (optional) if you also want to see other scheduled runs but not count them:
                for extra_idx, extra in enumerate(scheduled_runs[1:], start=2):
                    rows.append([
                        sid, hdr.get("doc_no"), (hdr.get("product_id") or "").strip(),
                        hdr.get("stage_name"), appr,
                        b["batch_no"], b["generated_batch_number"], extra["equipment_id"],
                        extra_idx, extra["std_bct"], extra["wait_time"],
                        extra["start"], extra["end"], extra["status"],
                        b["output_quantity"], "No", 0
                    ])
            else:
                # FIFO → include ALL scheduled runs; each contributes its batch qty
                for run_idx, run in enumerate(scheduled_runs, start=1):
                    rows.append([
                        sid, hdr.get("doc_no"), (hdr.get("product_id") or "").strip(),
                        hdr.get("stage_name"), appr,
                        b["batch_no"], b["generated_batch_number"], run["equipment_id"],
                        run_idx, run["std_bct"], run["wait_time"],
                        run["start"], run["end"], run["status"],
                        b["output_quantity"], "Yes", b["output_quantity"]
                    ])

    if not rows:
        rows.append([""] * len(headers))

    df = pd.DataFrame(rows, columns=headers)

    # enforce true datetimes
    df["Run Start"] = pd.to_datetime(df["Run Start"], errors="coerce")
    df["Run End"]   = pd.to_datetime(df["Run End"], errors="coerce")

    # ---------- write ONE worksheet ----------
    bio = BytesIO()
    with pd.ExcelWriter(
        bio, engine="xlsxwriter",
        datetime_format="yyyy-mm-dd hh:mm:ss",
        date_format="yyyy-mm-dd",
    ) as writer:
        df.to_excel(writer, sheet_name="Batch Report", index=False)

        wb = writer.book
        ws = writer.sheets["Batch Report"]

        header_fmt = wb.add_format({"bold": True, "bg_color": "#E2E8F0", "border": 1})
        dt_fmt     = wb.add_format({"num_format": "yyyy-mm-dd hh:mm:ss"})
        num3_fmt   = wb.add_format({"num_format": "0.000"})
        int0_fmt   = wb.add_format({"num_format": "0"})

        ws.set_row(0, None, header_fmt)
        ws.set_column("A:A", 12, int0_fmt)    # Schedule ID
        ws.set_column("B:D", 18)              # Doc/FG/Stage
        ws.set_column("E:E", 10)              # Approach
        ws.set_column("F:G", 12, int0_fmt)    # Batch numbers
        ws.set_column("H:H", 16)              # Equipment ID
        ws.set_column("I:I", 8, int0_fmt)     # Run #
        ws.set_column("J:K", 14, num3_fmt)    # Std/Wait
        ws.set_column("L:M", 20, dt_fmt)      # Run Start/End
        ws.set_column("N:N", 12)              # Status
        ws.set_column("O:O", 14, num3_fmt)    # Output Qty
        ws.set_column("P:P", 14)              # Include In Sum
        ws.set_column("Q:Q", 18, num3_fmt)    # Counted Output Qty
        ws.freeze_panes(1, 0)
        ws.autofilter(0, 0, len(df), len(df.columns)-1)

    bio.seek(0)
    resp = HttpResponse(
        bio.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    resp["Content-Disposition"] = (
        f'attachment; filename="prod_debug_scheduled_only_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx"'
    )
    return resp







# ═════════════════════════════════════════════════
# OPTIONAL standalone power chart page
# ═════════════════════════════════════════════════
@login_required
def power_trend(request):
    limit = max(1, min(int(request.GET.get("limit", 60)), 365))
    sql = f"""
        SELECT TOP {limit}
               CONVERT(date, reading_date)          AS rdate,
               CAST(total_kwh_e18_e22_e16 AS float) AS kwh
        FROM utility_power_readings
        WHERE reading_type='TOTAL POWER CONSUMPTION'
        ORDER BY reading_date DESC;
    """
    with connections[DEFAULT_CONN].cursor() as cur:
        cur.execute(sql)
        rows = list(reversed(cur.fetchall()))

    labels = [r[0].strftime("%d-%m-%y") if isinstance(r[0], (date, datetime)) else str(r[0]) for r in rows]
    values = [float(r[1] or 0.0) for r in rows]

    return render(
        request,
        "reports/power_trend.html",
        {
            "labels_json": json.dumps(labels),
            "series_json": json.dumps(values),
            "limit": limit,
            "table_rows": rows,
        },
    )