# REPORTS/data_access.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Iterable, Dict, Any
from datetime import datetime
from django.db import connections
from django.conf import settings


# ---- REAL table names (yours) ------------------------------------
SCHEMA = getattr(settings, "PRODSCHED_SCHEMA", "dbo")
HEADERS_TBL = f"[{SCHEMA}].[production_schedule]"
LINES_TBL   = f"[{SCHEMA}].[production_schedule_lines]"


@dataclass
class PlanLine:
    line_type: str
    material_category: Optional[str] = None
    material_name: Optional[str] = None
    quantity: float = 0.0
    ratio: float = 0.0
    density: float = 0.0
    litre: float = 0.0
    include_in_total: bool = True

    # equipment fields
    equipment_id: Optional[str] = None
    std_bct: float = 0.0         # hours
    wait_time: float = 0.0       # hours
    star: bool = False
    closed_date: Optional[datetime] = None


@dataclass
class PlanSchedule:
    """Lightweight schedule object for the batch generator."""
    id: int
    product_id: Optional[str]
    stage_name: Optional[str]
    start_date: datetime
    no_of_batches: int
    scheduling_approach: int      # 0 ROLL, 1 FIFO, 3 STAR
    batch_number: Optional[str]
    type: Optional[str]
    lines: List[PlanLine] = field(default_factory=list)


def _fetch_rows(sql: str, params: Iterable[Any] = ()) -> List[Dict[str, Any]]:
    with connections["production_scheduler"].cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in rows]


def load_plan_schedules(selected_fgs: Iterable[str] = ()) -> List[PlanSchedule]:
    """
    Load plan headers and lines from production_scheduler and assemble
    PlanSchedule objects with .lines populated.
    """
    # ---------- headers ----------
    fg_filter = ""
    params: list[Any] = []
    if selected_fgs:
        ph = ", ".join(["%s"] * len(selected_fgs))
        fg_filter = f"WHERE h.product_id IN ({ph})"
        params.extend(list(selected_fgs))

    hdr_sql = f"""
        SELECT
            h.id,
            h.product_id,
            h.stage_name,
            h.start_date,
            h.no_of_batches,
            h.batch_number,
            h.scheduling_approach,
            h.type
        FROM {HEADERS_TBL} AS h
        {fg_filter}
    """
    hdr_rows = _fetch_rows(hdr_sql, params)

    schedules: Dict[int, PlanSchedule] = {}
    for r in hdr_rows:
        schedules[r["id"]] = PlanSchedule(
            id=r["id"],
            product_id=(r.get("product_id") or "").strip() if r.get("product_id") else None,
            stage_name=(r.get("stage_name") or "").strip() if r.get("stage_name") else None,
            start_date=r["start_date"],
            no_of_batches=int(r.get("no_of_batches") or 0),
            scheduling_approach=int(r.get("scheduling_approach") or 0),
            batch_number=r.get("batch_number"),
            type=(r.get("type") or "").strip() if r.get("type") else None,
            lines=[],
        )

    if not schedules:
        return []

    # ---------- lines ----------
    id_ph = ", ".join(["%s"] * len(schedules))
    line_sql = f"""
        SELECT
            l.schedule_id,
            l.line_type,
            l.material_category,
            l.material_name,
            CONVERT(decimal(18,3), l.quantity)   AS quantity,
            CONVERT(decimal(18,5), l.ratio)      AS ratio,
            CONVERT(decimal(18,5), l.density)    AS density,
            CONVERT(decimal(18,3), l.litre)      AS litre,
            CAST(COALESCE(l.include_in_total, 1) AS bit) AS include_in_total,
            l.equipment_id,
            CONVERT(decimal(18,3), l.std_bct)    AS std_bct,
            CONVERT(decimal(18,3), l.wait_time)  AS wait_time,
            CAST(COALESCE(l.star, 0) AS bit)     AS star,
            l.closed_date
        FROM {LINES_TBL} AS l
        WHERE l.schedule_id IN ({id_ph})
    """
    line_rows = _fetch_rows(line_sql, list(schedules.keys()))

    for r in line_rows:
        sched = schedules.get(r["schedule_id"])
        if not sched:
            continue
        sched.lines.append(
            PlanLine(
                line_type=(r["line_type"] or "").strip(),
                material_category=r.get("material_category"),
                material_name=r.get("material_name"),
                quantity=float(r.get("quantity") or 0),
                ratio=float(r.get("ratio") or 0),
                density=float(r.get("density") or 0),
                litre=float(r.get("litre") or 0),
                include_in_total=bool(r.get("include_in_total")),
                equipment_id=r.get("equipment_id"),
                std_bct=float(r.get("std_bct") or 0),
                wait_time=float(r.get("wait_time") or 0),
                star=bool(r.get("star")),
                closed_date=r.get("closed_date"),
            )
        )

    # Only return schedules that have at least one equipment line
    return [s for s in schedules.values() if any(l.line_type == "equipment" for l in s.lines)]
