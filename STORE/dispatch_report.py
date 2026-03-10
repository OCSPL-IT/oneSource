# STORE/dispatch_report.py
from __future__ import annotations

import re
import calendar
from datetime import date, timedelta
from typing import Dict, Tuple, List, Set, Optional

from django.db import connections
from django.utils import timezone

# ────────────────────────────────────────────────
# DB aliases / tables
# ────────────────────────────────────────────────
DISPATCH_PLAN_CONN  = "production_scheduler"
DISPATCH_PLAN_TABLE = "dbo.dispatch_plans"
ALPHA_TABLE         = "dbo.alpha"

ERP_CONN            = "readonly_db"

# ────────────────────────────────────────────────
# STOCK settings (as per your SSMS query)
# ────────────────────────────────────────────────
STOCK_LOCATION_NAME = "Solapur Approved Main Stores"
STOCK_ITEMTYPES     = ("Semi Finished Good", "Finished Good")
STOCK_FROM_INT      = 20250101  # same as your query (opening from this date)

# ────────────────────────────────────────────────
# Normalizers / labels
# ────────────────────────────────────────────────
def _norm(s: str | None) -> str:
    return (s or "").strip().upper()

def _disp_day_key(d: date) -> str:
    return d.strftime("%d-%m-%y")

def _disp_mon_key(d: date) -> str:
    return d.strftime("%b-%y").upper()

def _iter_month_starts(fr: date, to: date):
    cur = fr.replace(day=1)
    end = to.replace(day=1)
    while cur <= end:
        yield cur
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)

def _label_for_date(period: str, d: date) -> str:
    p = (period or "MTD").upper()
    return _disp_day_key(d) if p == "DAILY" else _disp_mon_key(d)

def _label_list(period: str, fr: date, to: date) -> list[str]:
    p = (period or "MTD").upper()
    if p == "DAILY":
        n = (to - fr).days
        return [_disp_day_key(fr + timedelta(days=i)) for i in range(n + 1)]
    return [_disp_mon_key(m) for m in _iter_month_starts(fr, to)]

def _disp_norm(s: str | None) -> str:
    """
    Aggressive normalization so "same FG" matches across:
    - Alpha table
    - Dispatch plan table
    - ERP (FG Name / item name)
    """
    if s is None:
        return ""

    v = str(s).upper().strip()

    v = v.replace("–", "-").replace("—", "-").replace("−", "-")
    v = v.replace("｜", "|").replace("¦", "|").replace("│", "|").replace("‖", "|").replace("∣", "|")

    v = re.sub(r"\s+", " ", v)
    v = re.sub(r"\s*-\s*", "-", v)
    v = re.sub(r"\s*\|\s*", "|", v)

    # Synonyms -> canonical
    if v in {"CANSOL", "DRP14/RM-15"}:
        return "ACETONCYANHYDRIN"
    if v == "F-ACID":
        return "4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE"

    return v

def _round_qty(x: float) -> float:
    try:
        return float(round(float(x or 0.0), 0))
    except Exception:
        return 0.0

def _round_amt(x: float) -> float:
    try:
        return float(round(float(x or 0.0), 0))
    except Exception:
        return 0.0

def _round_rate(x: float) -> float:
    try:
        return float(round(float(x or 0.0), 2))
    except Exception:
        return 0.0

# ────────────────────────────────────────────────
# Alpha map (FG alias list)
# ────────────────────────────────────────────────
def fetch_alpha_map() -> Dict[str, str]:
    """
    Returns: {RAW_PRODUCT_NAME_UPPER: ALPHA_CODE_OR_FALLBACK}
    """
    alpha_map: Dict[str, str] = {}
    with connections[DISPATCH_PLAN_CONN].cursor() as cur:
        cur.execute(f"SELECT product_name, alpha_code FROM {ALPHA_TABLE};")
        for pname, acode in cur.fetchall():
            raw = _norm(pname)
            alpha_map[raw] = (acode or "").strip() or (pname or "")
    return alpha_map

# ────────────────────────────────────────────────
# STOCK: Using YOUR SSMS logic (CTE version)
# Stock = FG closing + SFG closing (grouped by FGKey)
# ────────────────────────────────────────────────
def _expand_stock_name_variants(keys: Set[str]) -> List[str]:
    """
    Expand keys so synonyms can match FGKey.
    """
    out: Set[str] = set()
    for k in keys:
        ck = _disp_norm(k)
        if not ck:
            continue
        if ck == "ACETONCYANHYDRIN":
            out |= {"ACETONCYANHYDRIN", "CANSOL", "DRP14/RM-15"}
        elif ck == "4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE":
            out |= {
                "F-ACID",
                "4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE",
            }
        else:
            out.add(ck)
    return sorted(out)

def fetch_stock_qty_map(material_keys: Set[str]) -> Dict[str, float]:
    """
    Returns: {CANONICAL_FGKEY: stock_qty}

    ✅ Uses same accounting logic you gave (Opening + Period + Combined)
    ✅ Location forced to 'Solapur Approved Main Stores'
    ✅ Includes 'Semi Finished Good' + 'Finished Good'
    ✅ FGKey = FG Name (ITMCF) else Item Name
    """
    if not material_keys:
        return {}

    today_int = int(timezone.localdate().strftime("%Y%m%d"))
    from_int = STOCK_FROM_INT
    to_int = today_int

    # Filter down to only keys we need (performance)
    key_list = _expand_stock_name_variants(material_keys)
    placeholders = ",".join(["%s"] * len(key_list))

    # CTE version of your temp-table query (safe in Django cursor)
    sql = f"""
    SET NOCOUNT ON;

    ;WITH ITMMST_Filtered AS (
        SELECT
            i.lId,
            i.sCode,
            i.sName,
            id.lTypId          AS lItmTyp,
            it.sName           AS sItmTyp,
            it.bStkSum,
            id.lUntRpt,
            id.dCnvRpt
        FROM ITMMST i
        JOIN ITMDET id   ON id.lId = i.lId AND id.bStkUpd = 1 AND i.bDel = 0
        JOIN ITMTYP it   ON it.lTypId = id.lTypId AND it.bStkUpd = 1
        JOIN ITMTYPCMP x ON x.lTypId = it.lTypId AND x.lCompId = 27
        WHERE it.sName IN (%s, %s)  -- Semi Finished Good + Finished Good
    ),
    DIMMST_Filtered AS (
        SELECT DISTINCT
            d.lId,
            d.sName,
            t.bCompSel
        FROM DIMMST d
        JOIN DIMTYP t ON t.lTypId = d.lTypId AND d.cTyp='S'
        WHERE d.bStkVal = 1
          AND LTRIM(RTRIM(d.sName)) = %s  -- Solapur Approved Main Stores
    ),
    TXNTYP_Filtered AS (
        SELECT lTypId, lStkTyp
        FROM TXNTYP
        WHERE lStkTyp < 2
    ),
    Opening AS (
        SELECT
            CAST(-1 AS int) AS lStkTyp,
            CAST(0  AS bigint) AS lId,
            CAST(%s AS int) AS dtDocDate,
            dd.lItmTyp,
            dd.lItmId,
            dd.lUntId2,
            dd.lLocId,
            SUM(dd.dQtyStk)              AS dQty,
            SUM(COALESCE(dd.dStkVal,0))  AS dVal
        FROM TXNHDR h
        JOIN TXNTYP_Filtered tt ON tt.lTypId = h.lTypId
        JOIN TXNDET dd           ON dd.lId    = h.lId
        JOIN ITMMST_Filtered i   ON i.lId     = dd.lItmId AND i.lItmTyp = dd.lItmTyp
        JOIN DIMMST_Filtered l   ON l.lId     = dd.lLocId
        WHERE h.bDel = 0 AND dd.bDel = 0 AND dd.cFlag IN ('I','A')
          AND dd.lClosed<=0 AND h.lClosed<=0
          AND (h.lCompId=27 OR l.bCompSel=-1)
          AND h.dtDocDate < %s
        GROUP BY dd.lItmTyp,dd.lItmId,dd.lUntId2,dd.lLocId
        HAVING ABS(SUM(dd.dQtyStk))>0.0001
    ),
    PeriodRaw AS (
        SELECT
            tt.lStkTyp,
            h.lId,
            h.dtDocDate,
            dd.lItmTyp,
            dd.lItmId,
            dd.lUntId2,
            dd.lLocId,
            dd.dQtyStk             AS dQty,
            COALESCE(dd.dStkVal,0) AS dVal
        FROM TXNHDR h
        JOIN TXNTYP_Filtered tt ON tt.lTypId = h.lTypId
        JOIN TXNDET dd           ON dd.lId    = h.lId
        JOIN ITMMST_Filtered i   ON i.lId     = dd.lItmId AND i.lItmTyp = dd.lItmTyp
        JOIN DIMMST_Filtered l   ON l.lId     = dd.lLocId
        WHERE h.bDel=0 AND dd.bDel=0 AND dd.cFlag IN ('I','A')
          AND dd.lClosed<=0 AND h.lClosed<=0
          AND (h.lCompId=27 OR l.bCompSel=-1)
          AND h.dtDocDate BETWEEN %s AND %s
          AND NOT (dd.dQtyStk BETWEEN -0.0001 AND 0.0001)
    ),
    Combined AS (
        SELECT
            lStkTyp, lId, dtDocDate, lItmTyp, lItmId, lUntId2, lLocId,
            SUM(dQty) AS dQty,
            SUM(dVal) AS dVal
        FROM (
            SELECT o.lStkTyp, o.lId, o.dtDocDate, o.lItmTyp, o.lItmId, o.lUntId2, o.lLocId, o.dQty, o.dVal
            FROM Opening o
            UNION ALL
            SELECT r.lStkTyp, r.lId, r.dtDocDate, r.lItmTyp, r.lItmId, r.lUntId2, r.lLocId, r.dQty, r.dVal
            FROM PeriodRaw r
        ) u
        GROUP BY lStkTyp, lId, dtDocDate, lItmTyp, lItmId, lUntId2, lLocId
    ),
    CF AS (
        SELECT
            icf.lTypId,
            icf.lId,
            MAX(CASE WHEN icf.sName='FG Name' THEN icf.sValue END) AS FGName
        FROM ITMCF icf
        GROUP BY icf.lTypId, icf.lId
    ),
    Final AS (
        SELECT
            COALESCE(NULLIF(LTRIM(RTRIM(cf.FGName)),''), i.sName) AS FGKey,
            SUM(c.dQty) AS ClosingQty
        FROM Combined c
        JOIN ITMMST_Filtered i ON i.lId=c.lItmId AND i.lItmTyp=c.lItmTyp
        JOIN DIMMST_Filtered l ON l.lId = c.lLocId
        LEFT JOIN CF cf         ON cf.lTypId=i.lItmTyp AND cf.lId=i.lId
        GROUP BY COALESCE(NULLIF(LTRIM(RTRIM(cf.FGName)),''), i.sName)
    )
    SELECT
        UPPER(LTRIM(RTRIM(FGKey))) AS FGKey,
        CAST(ClosingQty AS float)  AS ClosingQty
    FROM Final
    WHERE UPPER(LTRIM(RTRIM(FGKey))) IN ({placeholders});
    """

    params: List[object] = [
        STOCK_ITEMTYPES[0], STOCK_ITEMTYPES[1],
        STOCK_LOCATION_NAME,
        from_int, from_int,
        from_int, to_int,
        *key_list
    ]

    raw_map: Dict[str, float] = {}
    with connections[ERP_CONN].cursor() as cur:
        cur.execute(sql, params)
        for fgkey, qty in cur.fetchall():
            raw_map[_disp_norm(fgkey)] = float(qty or 0.0)

    # fold synonyms back into canonical keys
    out: Dict[str, float] = {}
    for k in material_keys:
        ck = _disp_norm(k)
        if ck == "ACETONCYANHYDRIN":
            out[ck] = (
                raw_map.get("ACETONCYANHYDRIN", 0.0)
                + raw_map.get("CANSOL", 0.0)
                + raw_map.get("DRP14/RM-15", 0.0)
            )
        elif ck == "4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE":
            out[ck] = (
                raw_map.get("F-ACID", 0.0)
                + raw_map.get("4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE", 0.0)
            )
        else:
            out[ck] = raw_map.get(ck, 0.0)

    return out

# ────────────────────────────────────────────────
# Dispatch: PLAN series loaders (per day)
# ────────────────────────────────────────────────
def fetch_dispatch_plan_series(fr: date, to: date) -> Dict[date, Dict[str, float]]:
    """
    PLAN: production_scheduler dbo.dispatch_plans
    Returns: {date: {mat: qty}}
    """
    sql = f"""
        ;WITH X AS (
            SELECT
                pdate = COALESCE(
                    TRY_CONVERT(date, dp.tentative_date),
                    TRY_CONVERT(date, dp.tentative_date, 23),
                    TRY_CONVERT(date, dp.tentative_date, 120),
                    TRY_CONVERT(date, dp.tentative_date, 121),
                    TRY_CONVERT(date, dp.tentative_date, 126),
                    TRY_CONVERT(date, dp.tentative_date, 112),
                    TRY_CONVERT(date, dp.tentative_date, 105),
                    TRY_CONVERT(date, dp.tentative_date, 103)
                ),
                mat = UPPER(LTRIM(RTRIM(COALESCE(dp.material_name,'')))),
                qty = TRY_CONVERT(float, REPLACE(CONVERT(varchar(50), dp.qty), ',', ''))
            FROM {DISPATCH_PLAN_TABLE} AS dp
        )
        SELECT X.pdate, X.mat, SUM(COALESCE(X.qty,0)) AS qty
        FROM X
        WHERE X.pdate IS NOT NULL
          AND X.pdate BETWEEN %s AND %s
          AND NULLIF(X.mat,'') IS NOT NULL
        GROUP BY X.pdate, X.mat;
    """
    series: Dict[date, Dict[str, float]] = {}
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

# ────────────────────────────────────────────────
# Dispatch: ACTUAL series loaders (per day) with qty + value + rate
# ────────────────────────────────────────────────
def fetch_dispatch_actual_series(fr: date, to: date) -> Dict[date, Dict[str, dict]]:
    """
    ACTUAL: ERP readonly_db TXNHDR/TXNDET
    Returns:
      {date: {mat: {"qty": float, "value": float, "rate": float}}}
    value = SUM(qty * rate)
    rate  = value / qty (avg rate)
    """
    sql = r"""
        ;WITH D AS (
            SELECT
                mat = UPPER(RTRIM(LTRIM(
                    CASE
                        WHEN ITM.sName IN ('CANSOL','DRP14/RM-15') THEN 'ACETONCYANHYDRIN'
                        WHEN ITM.sName = 'F-ACID' THEN '4-HYDROXY-3-(2,4,6-TRIMETHYLPHENYL)-1-OXASPIRO[4.4]NON-3-EN-2-ONE'
                        ELSE ITM.sName
                    END
                ))),
                idate = COALESCE(
                    TRY_CONVERT(date, CONVERT(varchar(50), HDR.dtDocDate), 112),
                    TRY_CONVERT(date, CONVERT(varchar(50), HDR.dtDocDate), 105),
                    TRY_CONVERT(date, CONVERT(varchar(50), HDR.dtDocDate), 103),
                    TRY_CONVERT(date, CONVERT(varchar(50), HDR.dtDocDate))
                ),
                qty  = TRY_CONVERT(float, DET.dQty),
                rate = TRY_CONVERT(float, DET.dRate)
            FROM TXNHDR HDR
            JOIN TXNDET DET ON DET.lId = HDR.lId AND DET.cFlag='I'
            JOIN ITMMST ITM ON ITM.lId = DET.lItmId
            JOIN ITMTYP ITP ON ITP.lTypId = DET.lItmTyp
            WHERE HDR.lTypId IN (409,497,498,499,500,504,650,654,824,825,826,827,828,829,939,940)
              AND HDR.bDel = 0 AND DET.bDel <> -2 AND HDR.lClosed = 0
              AND HDR.lCompId IN (27)
              AND ITP.sName = 'Finished Good'
              AND ITM.sName NOT IN ('SPENT HCL','ISONONANOYL CHLORIDE','URD')
        )
        SELECT
            mat,
            idate,
            SUM(COALESCE(qty,0))                             AS qty,
            SUM(COALESCE(qty,0) * COALESCE(rate,0))          AS value
        FROM D
        WHERE idate IS NOT NULL
          AND idate BETWEEN %s AND %s
        GROUP BY mat, idate;
    """

    out: Dict[date, Dict[str, dict]] = {}
    with connections[ERP_CONN].cursor() as cur:
        cur.execute(sql, [fr, to])
        for mat, idate, qty, value in cur.fetchall():
            k = _disp_norm(mat)
            q = float(qty or 0.0)
            v = float(value or 0.0)
            r = (v / q) if q else 0.0
            out.setdefault(idate, {})[k] = {"qty": q, "value": v, "rate": r}

    d = fr
    while d <= to:
        out.setdefault(d, {})
        d += timedelta(days=1)
    return out

def fetch_dispatch_series_full(fr: date, to: date) -> Tuple[
    Dict[date, Dict[str, float]],
    Dict[date, Dict[str, dict]]
]:
    plan_series = fetch_dispatch_plan_series(fr, to)
    act_series  = fetch_dispatch_actual_series(fr, to)

    d = fr
    while d <= to:
        plan_series.setdefault(d, {})
        act_series.setdefault(d, {})
        d += timedelta(days=1)

    all_mats: Set[str] = set()
    for mats in plan_series.values():
        all_mats |= set(mats.keys())
    for mats in act_series.values():
        all_mats |= set(mats.keys())

    if all_mats:
        d = fr
        while d <= to:
            p_row = plan_series.setdefault(d, {})
            a_row = act_series.setdefault(d, {})
            for m in all_mats:
                p_row.setdefault(m, 0.0)
                a_row.setdefault(m, {"qty": 0.0, "value": 0.0, "rate": 0.0})
            d += timedelta(days=1)

    return plan_series, act_series

# ────────────────────────────────────────────────
# Build Dispatch Grid (adds stock_qty + rate/value)
# ────────────────────────────────────────────────
def build_dispatch_grid(
    period: str,
    fr: date,
    to: date,
    alpha_map: Dict[str, str],
    selected_fgs: Optional[List[str]] = None,
):
    """
    DAILY cells:
      {"plan": qty, "actual": qty, "actual_value": amt, "actual_rate": avg}
    MONTHLY cells:
      {"plan": qty_full_month, "est": qty_mtd_plan, "actual": qty_mtd, "actual_value": amt_mtd, "actual_rate": avg_mtd}
    Stock:
      Stock = FG closing + SFG closing (Solapur Approved Main Stores)
    """
    p = (period or "MTD").upper()
    selected_fgs = selected_fgs or []
    norm_sel: Set[str] = {_norm(x) for x in selected_fgs} if selected_fgs else set()

    def _keep_key(k_norm: str) -> bool:
        if not norm_sel:
            return True
        alias = alpha_map.get(_norm(k_norm), k_norm)
        return (_norm(k_norm) in norm_sel) or (_norm(alias) in norm_sel)

    # ---------- DAILY ----------
    if p == "DAILY":
        plan_series, act_series = fetch_dispatch_series_full(fr, to)
        labels = _label_list(p, fr, to)

        all_mats: Set[str] = set()
        for mats in plan_series.values():
            all_mats |= set(mats.keys())
        for mats in act_series.values():
            all_mats |= set(mats.keys())

        all_mats = {m for m in all_mats if _keep_key(m)}

        stock_map = fetch_stock_qty_map(all_mats)

        plan_bucket: Dict[str, Dict[str, float]] = {}
        act_qty_bucket: Dict[str, Dict[str, float]] = {}
        act_val_bucket: Dict[str, Dict[str, float]] = {}

        for d0, mats in plan_series.items():
            lab = _label_for_date(p, d0)
            if lab not in labels:
                continue
            for m, q in (mats or {}).items():
                if not _keep_key(m):
                    continue
                plan_bucket.setdefault(lab, {}).setdefault(m, 0.0)
                plan_bucket[lab][m] += float(q or 0.0)

        for d0, mats in act_series.items():
            lab = _label_for_date(p, d0)
            if lab not in labels:
                continue
            for m, obj in (mats or {}).items():
                if not _keep_key(m):
                    continue
                q = float((obj or {}).get("qty", 0.0) or 0.0)
                v = float((obj or {}).get("value", 0.0) or 0.0)
                act_qty_bucket.setdefault(lab, {}).setdefault(m, 0.0)
                act_val_bucket.setdefault(lab, {}).setdefault(m, 0.0)
                act_qty_bucket[lab][m] += q
                act_val_bucket[lab][m] += v

        ordered = sorted(
            ((alpha_map.get(_norm(m), m), m) for m in all_mats),
            key=lambda t: (_norm(t[0]), _norm(t[1]))
        )

        rows: List[dict] = []
        for alias, raw in ordered:
            cells = []
            for lab in labels:
                pq = float(plan_bucket.get(lab, {}).get(raw, 0.0))
                aq = float(act_qty_bucket.get(lab, {}).get(raw, 0.0))
                av = float(act_val_bucket.get(lab, {}).get(raw, 0.0))
                ar = (av / aq) if aq else 0.0

                cells.append({
                    "plan": _round_qty(pq),
                    "actual": _round_qty(aq),
                    "actual_value": _round_amt(av),
                    "actual_rate": _round_rate(ar),
                })

            rows.append({
                "fg": raw,
                "alias": alias,
                "stock_qty": _round_qty(float(stock_map.get(_disp_norm(raw), 0.0))),
                "cells": cells,
            })

        footer = []
        for idx, _lab in enumerate(labels):
            tot_pq = sum(float(r["cells"][idx]["plan"]) for r in rows) if rows else 0.0
            tot_aq = sum(float(r["cells"][idx]["actual"]) for r in rows) if rows else 0.0
            tot_av = sum(float(r["cells"][idx]["actual_value"]) for r in rows) if rows else 0.0
            tot_ar = (tot_av / tot_aq) if tot_aq else 0.0
            footer.append({
                "plan": _round_qty(tot_pq),
                "actual": _round_qty(tot_aq),
                "actual_value": _round_amt(tot_av),
                "actual_rate": _round_rate(tot_ar),
            })

        footer_stock = sum(float(r.get("stock_qty", 0.0)) for r in rows) if rows else 0.0
        return {"months": labels, "rows": rows, "footer": {"months": footer, "stock_qty": _round_qty(footer_stock)}}

    # ---------- MONTHLY BUCKETS ----------
    labels = _label_list("MTD", fr, to)
    month_starts = list(_iter_month_starts(fr, to))
    if not month_starts:
        return {"months": [], "rows": [], "footer": {"months": [], "stock_qty": 0.0}}

    def _month_bounds(d0: date) -> Tuple[date, date]:
        first = d0.replace(day=1)
        last_day = calendar.monthrange(d0.year, d0.month)[1]
        return first, d0.replace(day=last_day)

    today_local = timezone.localdate()
    cut_end_global = min(to, today_local)  # ✅ EST/ACTUAL should be MTD only
    last_month_end = max(_month_bounds(m0)[1] for m0 in month_starts)  # ✅ PLAN full month

    plan_full_series = fetch_dispatch_plan_series(fr, last_month_end)
    act_mtd_series = fetch_dispatch_actual_series(fr, cut_end_global) if cut_end_global >= fr else {}

    plan_month_bucket: Dict[str, Dict[str, float]] = {}
    est_month_bucket:  Dict[str, Dict[str, float]] = {}
    act_qty_bucket:    Dict[str, Dict[str, float]] = {}
    act_val_bucket:    Dict[str, Dict[str, float]] = {}

    def _acc_plan(series_by_day: Dict[date, Dict[str, float]],
                  r_start: date, r_end: date,
                  month_label_date: date,
                  bucket: Dict[str, Dict[str, float]]):
        if r_end < r_start:
            return
        label = _disp_mon_key(month_label_date)
        cur = r_start
        while cur <= r_end:
            mats = series_by_day.get(cur, {}) or {}
            for m, q in mats.items():
                if not _keep_key(m):
                    continue
                bucket.setdefault(label, {}).setdefault(m, 0.0)
                bucket[label][m] += float(q or 0.0)
            cur += timedelta(days=1)

    def _acc_actual(series_by_day: Dict[date, Dict[str, dict]],
                    r_start: date, r_end: date,
                    month_label_date: date,
                    qty_bucket: Dict[str, Dict[str, float]],
                    val_bucket: Dict[str, Dict[str, float]]):
        if r_end < r_start:
            return
        label = _disp_mon_key(month_label_date)
        cur = r_start
        while cur <= r_end:
            mats = series_by_day.get(cur, {}) or {}
            for m, obj in mats.items():
                if not _keep_key(m):
                    continue
                q = float((obj or {}).get("qty", 0.0) or 0.0)
                v = float((obj or {}).get("value", 0.0) or 0.0)
                qty_bucket.setdefault(label, {}).setdefault(m, 0.0)
                val_bucket.setdefault(label, {}).setdefault(m, 0.0)
                qty_bucket[label][m] += q
                val_bucket[label][m] += v
            cur += timedelta(days=1)

    for m0 in month_starts:
        m_start, m_end = _month_bounds(m0)

        # ✅ PLAN = FULL month total
        plan_start = max(m_start, fr)
        plan_end   = m_end
        _acc_plan(plan_full_series, plan_start, plan_end, m_start, plan_month_bucket)

        # ✅ EST (MTD) = plan month start → min(today, to, month end)
        mtd_start = max(m_start, fr)
        mtd_end   = min(m_end, cut_end_global)
        _acc_plan(plan_full_series, mtd_start, mtd_end, m_start, est_month_bucket)

        # ✅ ACTUAL (MTD) = month start → same mtd_end
        _acc_actual(act_mtd_series, mtd_start, mtd_end, m_start, act_qty_bucket, act_val_bucket)

    all_mats: Set[str] = set()
    for mats in plan_month_bucket.values():
        all_mats |= set(mats.keys())
    for mats in est_month_bucket.values():
        all_mats |= set(mats.keys())
    for mats in act_qty_bucket.values():
        all_mats |= set(mats.keys())
    for mats in act_val_bucket.values():
        all_mats |= set(mats.keys())

    all_mats = {m for m in all_mats if _keep_key(m)}

    stock_map = fetch_stock_qty_map(all_mats)

    ordered = sorted(
        ((alpha_map.get(_norm(m), m), m) for m in all_mats),
        key=lambda t: (_norm(t[0]), _norm(t[1]))
    )

    rows: List[dict] = []
    for alias, raw in ordered:
        cells = []
        for lab in labels:
            pqty  = float((plan_month_bucket.get(lab, {}) or {}).get(raw, 0.0))  # FULL
            eqty  = float((est_month_bucket.get(lab,  {}) or {}).get(raw, 0.0))  # MTD plan
            aqty  = float((act_qty_bucket.get(lab, {}) or {}).get(raw, 0.0))     # MTD actual
            aval  = float((act_val_bucket.get(lab, {}) or {}).get(raw, 0.0))     # MTD value
            arate = (aval / aqty) if aqty else 0.0

            cells.append({
                "plan": _round_qty(pqty),
                "est": _round_qty(eqty),
                "actual": _round_qty(aqty),
                "actual_value": _round_amt(aval),
                "actual_rate": _round_rate(arate),
                "var_pct": 0.0,
            })

        rows.append({
            "fg": raw,
            "alias": alias,
            "stock_qty": _round_qty(float(stock_map.get(_disp_norm(raw), 0.0))),
            "cells": cells
        })

    footer = []
    for idx, _lab in enumerate(labels):
        tot_p  = sum(float(r["cells"][idx]["plan"]) for r in rows) if rows else 0.0
        tot_e  = sum(float(r["cells"][idx]["est"]) for r in rows) if rows else 0.0
        tot_aq = sum(float(r["cells"][idx]["actual"]) for r in rows) if rows else 0.0
        tot_av = sum(float(r["cells"][idx]["actual_value"]) for r in rows) if rows else 0.0
        tot_ar = (tot_av / tot_aq) if tot_aq else 0.0

        footer.append({
            "plan": _round_qty(tot_p),
            "est": _round_qty(tot_e),
            "actual": _round_qty(tot_aq),
            "actual_value": _round_amt(tot_av),
            "actual_rate": _round_rate(tot_ar),
            "var_pct": 0.0,
        })

    footer_stock = sum(float(r.get("stock_qty", 0.0)) for r in rows) if rows else 0.0
    return {"months": labels, "rows": rows, "footer": {"months": footer, "stock_qty": _round_qty(footer_stock)}}
