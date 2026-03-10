# REPORTS/utils.py
from __future__ import annotations
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List


def _as_datetime(v: Any) -> datetime | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    # try ISO / common str inputs
    try:
        return datetime.fromisoformat(str(v))
    except Exception:
        return None


def _truthy(v: Any) -> bool:
    return str(v or "").strip().lower() in ("1", "true", "yes", "y", "t")


def _get(obj: Any, name: str, default=None):
    """Get attribute/field from ORM object or dict."""
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _iter_lines(schedule: Any) -> Iterable[Dict[str, Any]]:
    """
    Yield line-dicts from either:
      • ORM schedule with .lines,
      • dict schedule with 'lines':[...].
    """
    lines = _get(schedule, "lines") or _get(schedule, "Lines") or []
    if not lines:
        return []
    out = []
    for l in lines:
        # normalize each line to dict
        if isinstance(l, dict):
            d = dict(l)
        else:
            d = {
                "line_type":        _get(l, "line_type"),
                "material_category":_get(l, "material_category"),
                "material_name":    _get(l, "material_name"),
                "quantity":         _get(l, "quantity"),
                "ratio":            _get(l, "ratio"),
                "density":          _get(l, "density"),
                "litre":            _get(l, "litre"),
                "include_in_total": _get(l, "include_in_total"),
                "equipment_id":     _get(l, "equipment_id"),
                "std_bct":          _get(l, "std_bct"),
                "wait_time":        _get(l, "wait_time"),
                "star":             _get(l, "star"),
                "closed_date":      _get(l, "closed_date"),
            }
        out.append(d)
    return out


def _normalize_header(schedule: Any) -> Dict[str, Any]:
    """
    Build a uniform header dict from ORM or dict schedule.
    """
    return {
        "no_of_batches":       _get(schedule, "no_of_batches", 0),
        "scheduling_approach": _get(schedule, "scheduling_approach", 0),
        "start_date":          _as_datetime(_get(schedule, "start_date")),
        "closed_date":         _as_datetime(_get(schedule, "closed_date")),
        "batch_number":        _get(schedule, "batch_number") or "",
    }


def _map_line_for_export(l: Dict[str, Any]) -> Dict[str, Any]:
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


def generate_batch_rows_from(header: Dict[str, Any], lines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Core generator used by exports and reports.
    Accepts a normalized header dict and list of line dicts.
    Returns: list of batch dicts (Scheduled-only batches).
    """
    n_batches = int(float(header.get("no_of_batches") or 0))
    approach  = int(header.get("scheduling_approach") or 0)  # 0 ROLL, 1 FIFO, 3 STAR
    start_ts  = header.get("start_date")
    plan_cut  = header.get("closed_date")  # plan-level cutoff (datetime or None)

    if not start_ts or n_batches < 1:
        return []

    # per-batch output qty from first 'output' line
    out_ln = next((l for l in lines if str(l.get("line_type") or "").lower() == "output"), None)
    per_batch = round(float(out_ln.get("quantity") or 0)) if out_ln else 0

    mats = [_map_line_for_export(l) for l in lines if str(l.get("line_type") or "").lower() == "input"]
    outs = [_map_line_for_export(l) for l in lines if str(l.get("line_type") or "").lower() == "output"]
    wsts = [_map_line_for_export(l) for l in lines if str(l.get("line_type") or "").lower() == "waste"]

    # equipment lanes
    eq_lines = [l for l in lines if str(l.get("line_type") or "").lower() == "equipment"]
    if not eq_lines:
        return []

    # normalize equipment state
    eq_state = [{
        "equipment_id": l.get("equipment_id"),
        "std":          float(l.get("std_bct") or 0.0),
        "wait":         float(l.get("wait_time") or 0.0),
        "next":         start_ts,
        "star":         _truthy(l.get("star")),
        "closed_date":  _as_datetime(l.get("closed_date")),
    } for l in eq_lines]

    # generated batch number helper
    base = header.get("batch_number") or ""
    if len(base) >= 2 and base[-2:].isdigit():
        prefix, start_no = base[:-2], int(base[-2:])
        gen_num = lambda i: prefix + str(start_no + i).zfill(2)
    else:
        gen_num = lambda i: str(i).zfill(2)

    batches: List[Dict[str, Any]] = []

    # ---------- FIFO ----------
    if approach == 1:
        for i in range(1, n_batches + 1):
            cell = min(eq_state, key=lambda x: x["next"])
            st   = cell["next"]
            et   = st + timedelta(hours=cell["std"])
            cancel = (
                (cell["closed_date"] and et > cell["closed_date"]) or
                (plan_cut and et > plan_cut)
            )
            status = "Cancelled" if cancel else "Scheduled"
            # always advance next (match legacy behavior)
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
                    "status":       status,
                }],
                "materials":              mats,
                "outputs":                outs,
                "wastes":                 wsts,
            })

    # ---------- ROLL ----------
    elif approach == 0:
        pipeline = [dict(e) for e in eq_state]
        for i in range(1, n_batches + 1):
            runs, prev_end = [], None
            for cell in pipeline:
                st = max(prev_end or cell["next"], cell["next"])
                et = st + timedelta(hours=cell["std"])
                cancel = (
                    (cell["closed_date"] and et > cell["closed_date"]) or
                    (plan_cut and et > plan_cut)
                )
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
                    "status":       status,
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

    # ---------- STAR ----------
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
                cancel = (
                    (cell["closed_date"] and et >= cell["closed_date"]) or
                    (plan_cut and et >= plan_cut)
                )
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
                    "status":       status,
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

    else:
        # unknown approach => treat as ROLL
        header["scheduling_approach"] = 0
        return generate_batch_rows_from(header, lines)

    # FINAL FILTER: keep only batches where ALL runs are Scheduled
    batches = [
        b for b in batches
        if all(r["status"] == "Scheduled" for r in b.get("equipment_runs", []))
    ]
    return batches


def generate_batch_report(schedule: Any) -> List[Dict[str, Any]]:
    """
    Compatibility wrapper:
      • If `schedule` is an ORM object with `.lines`, converts to dicts and calls core.
      • If `schedule` is a dict and contains 'lines', uses them directly.
    Returns list[dict] with the same schema as before.
    """
    # dict path
    if isinstance(schedule, dict):
        header = _normalize_header(schedule)
        lines  = list(_iter_lines(schedule))
        return generate_batch_rows_from(header, lines)

    # ORM path
    header = _normalize_header(schedule)
    # attach lines from ORM
    ldicts = []
    for l in (_get(schedule, "lines") or []):
        ldicts.append({
            "line_type":        _get(l, "line_type"),
            "material_category":_get(l, "material_category"),
            "material_name":    _get(l, "material_name"),
            "quantity":         _get(l, "quantity"),
            "ratio":            _get(l, "ratio"),
            "density":          _get(l, "density"),
            "litre":            _get(l, "litre"),
            "include_in_total": _get(l, "include_in_total"),
            "equipment_id":     _get(l, "equipment_id"),
            "std_bct":          _get(l, "std_bct"),
            "wait_time":        _get(l, "wait_time"),
            "star":             _get(l, "star"),
            "closed_date":      _get(l, "closed_date"),
        })
    return generate_batch_rows_from(header, ldicts)
