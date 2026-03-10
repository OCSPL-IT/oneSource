from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.contrib.auth.decorators import login_required
import json
from django.db.models import Sum, Count, Value, F ,FloatField,Q
from django.db.models.functions import Coalesce, Cast
from django.http import JsonResponse,HttpResponse
from django.db import connections
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import render, redirect,get_object_or_404
from django.contrib import messages
from django.utils.dateparse import parse_date
from .models import Downtime
from .forms import DowntimeForm,select_block, DEPARTMENT_CHOICES
import pandas as pd
from django.db.models import Sum,Count
from datetime import date, timedelta, datetime,time

# PRODUCTION/views.py
import json
import logging
from typing import List, Tuple, Dict, Any
from datetime import datetime
from zoneinfo import ZoneInfo

from django.db import connections
from django.shortcuts import render, redirect
from django.urls import reverse

from .models import DeptCategory, Downtime

logger = logging.getLogger(__name__)

SQL_CTE = r"""
WITH CFV AS (
    SELECT
        lid,
        MAX(CASE WHEN sName = 'Product Name' AND lLine = 0 THEN sValue END) AS ProductName,
        MAX(CASE WHEN sName = 'Batch No'     AND lLine = 0 THEN sValue END) AS BatchNo,
        MAX(CASE WHEN sName = 'Block'        AND lLine = 0 THEN sValue END) AS Block
    FROM txncf
    WHERE lLine = 0
      AND sName IN ('Product Name','Batch No','Block')
    GROUP BY lid
),
FGV AS (
    SELECT
        lId,
        sValue AS FGName,
        ROW_NUMBER() OVER (
            PARTITION BY lId
            ORDER BY
              CASE
                WHEN sValue = '12' AND lFieldNo = 10 THEN 0
                ELSE 1
              END,
              lFieldNo
        ) AS rn
    FROM ITMCF
    WHERE sName = 'FG Name'
      AND (sValue <> '12' OR (sValue = '12' AND lFieldNo = 10))
)
"""

SQL_WHERE = r"""
WHERE
    HDR.ltypid IN (664,717,718,719,720,721)
    AND DET.lItmTyp <> 63
    AND DET.bDel <> -2
    AND HDR.bDel <> 1
    AND DET.lClosed <> -2
    AND HDR.lClosed = 0
    AND HDR.lcompid = 27
"""

def _ro_conn():
    return connections["readonly_db"]

def _ps_conn():
    return connections["production_scheduler"]

def _fetch_erp_rows() -> List[Tuple[str, str, str, str]]:
    sql = SQL_CTE + r"""
    SELECT
        CFV.Block       AS Block,
        CFV.ProductName AS StageName,
        FGV.FGName      AS FGName,
        CFV.BatchNo     AS BatchNo
    FROM txnhdr AS HDR
    INNER JOIN TXNDET AS DET ON HDR.lId = DET.lId
    LEFT  JOIN CFV          ON CFV.lid = HDR.lId
    INNER JOIN ITMMST AS FM ON FM.sName = CFV.ProductName
    INNER JOIN ITMDET AS D2 ON D2.lId = FM.lId AND D2.lTypId = DET.lItmTyp
    LEFT  JOIN FGV          ON FGV.lId = FM.lId AND FGV.rn = 1
    """ + SQL_WHERE + r"""
      AND CFV.ProductName IS NOT NULL
      AND CFV.ProductName <> 'MIX SOLVENT'
      AND CFV.BatchNo IS NOT NULL
    """
    with _ro_conn().cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

def _fetch_equipment_rows():
    sql = r"""
    SELECT eq_id, block, capacity_size, capacity_unit, type_eq, moc_equipment
    FROM equipment
    WHERE block NOT IN ('JOB Work')
    """
    with _ps_conn().cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

def _fetch_bom_first_output_rows():
    sql = r"""
    WITH first_material AS (
        SELECT
            bom_id,
            material_name,
            quantity,
            ROW_NUMBER() OVER (PARTITION BY bom_id ORDER BY line_id ASC) AS rn
        FROM bom_lines
        WHERE line_type = 'output'
    )
    SELECT
        h.fg_name,
        h.stage_name,
        h.bom_id,
        fm.material_name,
        fm.quantity,
        e.std_bct
    FROM bom_headers h
    JOIN first_material fm
        ON h.bom_id = fm.bom_id AND fm.rn = 1
    LEFT JOIN bom_equipment e
        ON h.bom_id = e.bom_id
    """
    with _ps_conn().cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()

def _canon_block(b: str) -> str:
    if not b:
        return ""
    s = str(b).strip()
    if not s:
        return ""
    letter = s[0].upper()
    return letter if letter in {"A", "B", "C", "D", "E"} else ""

def _norm(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip().lower()

def _to_float_or_none(v: Any):
    try:
        return None if v is None else float(v)
    except Exception:
        return None

def _now_ist_for_datetime_local() -> str:
    ist_now = datetime.now(ZoneInfo("Asia/Kolkata"))
    return ist_now.strftime("%Y-%m-%dT%H:%M:%S")

@login_required
def downtime_form(request):
    """
    GET: render form with boot data
    POST: create Downtime record from form submission
    """
    if not request.user.has_perm('PRODUCTION.add_downtime'):
        messages.error(request, "You do not have permission to add downtime records.")
        return redirect('indexpage')

    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    # --- Build boot payload (same as before) ---
    erp_rows = _fetch_erp_rows()
    eq_rows  = _fetch_equipment_rows()
    bom_rows = _fetch_bom_first_output_rows()

    stages_set = set()
    fg_by_stage: Dict[str, str] = {}
    batches_by_stage_fg: Dict[str, set] = {}

    for _block, stage, fg, batch in erp_rows:
        if not stage:
            continue
        stages_set.add(stage)
        if fg and stage not in fg_by_stage:
            fg_by_stage[stage] = fg
        if fg and batch:
            key = f"{stage}||{fg}"
            batches_by_stage_fg.setdefault(key, set()).add(batch)

    stages = sorted(stages_set)
    batches_by_stage_fg_sorted = {k: sorted(v, reverse=True) for k, v in batches_by_stage_fg.items()}

    equipment_by_block: Dict[str, List[Dict[str, str]]] = {}
    for eq_id, block, capacity_size, capacity_unit, type_eq, moc_equipment in eq_rows:
        cb = _canon_block(block)
        if cb not in {"A", "B", "C", "D", "E"}:
            continue
        cap_size = "" if capacity_size is None else str(capacity_size)
        cap_unit = "" if capacity_unit is None else str(capacity_unit)
        t_eq     = "" if type_eq is None else str(type_eq)
        moc      = "" if moc_equipment is None else str(moc_equipment)
        label = f"{eq_id} — {cap_size} {cap_unit}".strip()
        extras = [x for x in (t_eq, moc) if x]
        if extras:
            label += f" ({', '.join(extras)})"
        equipment_by_block.setdefault(cb, []).append({
            "value": str(eq_id),
            "label": label,
            "block_display": str(block) if block is not None else "",
            "capacity_size": cap_size,
            "capacity_unit": cap_unit,
            "type_eq": t_eq,
            "moc_equipment": moc,
        })
    for cb in list(equipment_by_block.keys()):
        equipment_by_block[cb] = sorted(equipment_by_block[cb], key=lambda x: x["label"])

    bom_meta_by_stage_fg: Dict[str, Dict[str, Any]] = {}
    for fg_name, stage_name, bom_id, material_name, quantity, std_bct in bom_rows:
        s = (_norm(stage_name) or "")
        f = (_norm(fg_name) or "")
        if not s or not f:
            continue
        key = f"{s}||{f}"
        q = _to_float_or_none(quantity)
        bct = _to_float_or_none(std_bct)
        rec = bom_meta_by_stage_fg.setdefault(key, {"quantities": set(), "bcts": set()})
        if q is not None:
            rec["quantities"].add(int(round(q)))
        if bct is not None and bct > 0:
            rec["bcts"].add(int(round(bct)))
    for key, rec in bom_meta_by_stage_fg.items():
        rec["quantities"] = sorted(rec["quantities"])
        rec["bcts"] = sorted(rec["bcts"])

    # Dept / Category master
    dc_qs = DeptCategory.objects.filter(is_active=True).values_list("department", "category")
    departments = sorted({d for d, _ in dc_qs})
    categories_by_dept: Dict[str, List[str]] = {d: [] for d in departments}
    for d, c in dc_qs:
        categories_by_dept[d].append(c)
    for d in categories_by_dept:
        categories_by_dept[d].sort()

    blocks_display = ["A-Block", "B-Block", "C-Block", "D-Block", "E-Block"]
    block_display_to_key = {disp: _canon_block(disp) for disp in blocks_display}

    boot = {
        "blocks_display": blocks_display,
        "block_display_to_key": block_display_to_key,
        "equipment_by_block": equipment_by_block,
        "stages": stages,
        "fg_by_stage": fg_by_stage,
        "batches_by_stage_fg": batches_by_stage_fg_sorted,
        "bom_meta_by_stage_fg": bom_meta_by_stage_fg,
        "departments": departments,
        "categories_by_dept": categories_by_dept,
    }

    boot_json = json.dumps(boot, ensure_ascii=False).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    record_dt_default = _now_ist_for_datetime_local()

    # --- POST: Save a Downtime row ---
    if request.method == "POST":
        G = request.POST.get

        # Grab fields (strings first)
        idle               = (G("idle_flag") or "").strip() or None
        block              = (G("block") or "").strip() or None
        eqpt_id            = (G("equipment_id") or "").strip() or None
        eqpt_name          = (G("eqpt_name") or "").strip() or None
        stage_name         = (G("stage_name") or "").strip() or None
        product_name       = (G("product_name") or "").strip() or None  # FG
        batch_no           = (G("batch_no") or "").strip() or None
        downtime_dept      = (G("downtime_dept") or "").strip() or None
        downtime_category  = (G("downtime_category") or "").strip() or None
        reason             = (G("reason") or "").strip() or None

        # Dates/times
        date_str       = (G("record_dt") or "").strip()
        start_date_str = (G("start_date") or "").strip()
        end_date_str   = (G("end_date") or "").strip()
        start_time_str = (G("start_time") or "").strip()
        end_time_str   = (G("end_time") or "").strip()

        def _parse_date(s):
            try:
                return datetime.strptime(s, "%Y-%m-%d").date()
            except Exception:
                return None

        def _parse_time(s):
            # accept HH:MM or HH:MM:SS
            if not s:
                return None
            if len(s) == 5:
                s = s + ":00"
            try:
                return datetime.strptime(s, "%H:%M:%S").time()
            except Exception:
                return None

        def _parse_dtlocal(s):
            # e.g. 2025-08-06T13:45:12
            try:
                return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return None

        record_dt = _parse_dtlocal(date_str)
        date_for_row = (record_dt.date() if record_dt else None)

        s_date = _parse_date(start_date_str)
        e_date = _parse_date(end_date_str)
        s_time = _parse_time(start_time_str)
        e_time = _parse_time(end_time_str)

        # Numbers
        def _to_float(s):
            try:
                return float(s)
            except Exception:
                return None

        bom_qty        = _to_float(G("bom_qty"))
        bct            = _to_float(G("bom_bct"))
        duration_hours = _to_float(G("duration_hours"))
        total_loss     = _to_float(G("total_loss"))
        # derive loss/hr if needed on server
        loss_per_hr    = None
        if bom_qty is not None and bct not in (None, 0):
            try:
                loss_per_hr = round(bom_qty / bct)
            except Exception:
                loss_per_hr = None

        # Compute duration on server if missing
        if duration_hours is None and s_date and e_date and s_time and e_time:
            try:
                s_dt = datetime.combine(s_date, s_time)
                e_dt = datetime.combine(e_date, e_time)
                delta = (e_dt - s_dt).total_seconds() / 3600.0
                if delta >= 0:
                    duration_hours = round(delta, 2)
            except Exception:
                duration_hours = None

        # Compute total loss on server if missing
        if total_loss is None and loss_per_hr is not None and duration_hours is not None:
            total_loss = round(loss_per_hr * duration_hours)

        try:
            obj = Downtime.objects.create(
                date=date_for_row,
                idle=idle or "No",
                eqpt_id=eqpt_id,
                eqpt_name=eqpt_name,
                product_name=product_name,
                stage_name=stage_name,
                product_code=None,
                batch_no=batch_no,
                start_date=s_date,
                end_date=e_date,
                start_time=s_time,
                end_time=e_time,
                total_duration=duration_hours,
                block=block,
                downtime_dept=downtime_dept,
                downtime_category=downtime_category,
                reason=reason[:500] if reason else None,
                bom_qty=bom_qty,
                bct=bct,
                loss=total_loss,
            )
            messages.success(request, f"Downtime saved (ID #{obj.id}).")
            # Redirect-GET to avoid resubmission on refresh
            return redirect(request.path + "?ok=1")
        except Exception as exc:
            logger.exception("Failed to save downtime")
            messages.error(request, f"Failed to save record: {exc}")

    return render(
        request,
        "downtime/downtime_form.html",
        {
            "boot_json": boot_json,
            "record_dt_default": record_dt_default,
            'user_groups': user_groups,
            'is_superuser': is_superuser
        },
    )






@csrf_exempt
def search_equipment(request):
    """
    AJAX endpoint to search for equipment.
    Returns 'id' as the BOMItemCode (eqpt_id) and 'eqpt_name' for auto-filling the equipment name.
    Filters on Type='Equipment Master' and user input (term) against BOMItemCode or Name.
    """
    term = request.GET.get('term', '')
    like_param = f'%{term}%'

    sql_query = """
    WITH CTE_BOMDetails AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY det.lBomId, lSeqId) AS [Sr.No],
            TYP.sName AS [ItmType],
            MST.sName AS [ItemName],
            MST.sCode AS [ItemCode],
            BOM.dQty AS [Quantity],
            BOM.dRate AS [Rate],
            BOM.sCode AS [BOMCode],
            BOM.sName AS [BOMName],
            TYP1.sName AS [Type],
            MST1.sCode AS [BOMItemCode],
            MST1.sName AS [Name],
            CASE
                WHEN det.cFlag='P' THEN CAST(det.lUntId AS VARCHAR)
                ELSE u.sName
            END AS [Unit],
            BOM.cTyp AS [Based on],
            dPercentage AS [Percentage],
            CASE
                WHEN det.cFlag='P' THEN det.dQtyPrc
                ELSE det.dQty
            END AS [BOMQty],
            BOM.dCnv AS [BOMCnv],
            det.cFlag AS [cFlag],
            DSG.sCode AS [Resource Type],
            CASE
                WHEN st.lFieldNo=1 THEN BOM.svalue1
                WHEN st.lFieldNo=2 THEN BOM.svalue2
                WHEN st.lFieldNo=3 THEN BOM.svalue3
                WHEN st.lFieldNo=4 THEN BOM.svalue4
                WHEN st.lFieldNo=5 THEN BOM.svalue5
                WHEN st.lFieldNo=6 THEN BOM.svalue6
                WHEN st.lFieldNo=7 THEN BOM.svalue7
                WHEN st.lFieldNo=8 THEN BOM.svalue8
                WHEN st.lFieldNo=9 THEN BOM.svalue9
                WHEN st.lFieldNo=10 THEN BOM.svalue10
                ELSE ''
            END AS [Stock Parameter]
            FROM ITMBOMDET det
            INNER JOIN ITMBOM BOM ON det.lBomId = BOM.lBomId
            INNER JOIN ITMMST MST ON MST.lId = BOM.lId
            INNER JOIN ITMTYP TYP ON TYP.lTypId = BOM.lTypId
            LEFT JOIN ITMMST MST1 ON MST1.lId = det.lBomItm
            LEFT JOIN ITMDET DT ON det.lBomItm = DT.lId
            LEFT JOIN ITMTYP TYP1 ON TYP1.lTypId = DT.lTypId
            LEFT JOIN UNTMST u ON det.lUntId = u.lId
            LEFT OUTER JOIN DSGMST DSG ON DSG.lId = det.lResourceId
            LEFT JOIN STKPRM st ON st.lTypId = TYP.lTypId AND st.bBOM = 1
        )
        SELECT DISTINCT BOMItemCode, Name
        FROM CTE_BOMDetails
        WHERE [Type] = 'Equipment Master'
        AND (BOMItemCode LIKE %s OR Name LIKE %s)
        ORDER BY BOMItemCode;
    """


    with connections['readonly_db'].cursor() as cursor:
        cursor.execute(sql_query, [like_param, like_param])
        rows = cursor.fetchall()

    # Build JSON response in the format that django-select2 expects
    results = []
    for row in rows:
        bom_item_code = row[0]  # BOMItemCode
        eqpt_name = row[1]      # Name
        results.append({
            'id': bom_item_code,    # This will be stored in eqpt_id
            'text': bom_item_code,  # What the user sees in the dropdown
            'eqpt_name': eqpt_name  # Extra data to auto-fill eqpt_name
        })

    return JsonResponse({'results': results})


@csrf_exempt
def search_product(request):
    search_type = request.GET.get('search_type', 'fg_name')
    term = request.GET.get('term', '').strip()
    fg_filter = request.GET.get('fg_filter', '').strip()
    like_term = f"%{term.lower()}%"

    if search_type == 'fg_name':
        sql_query = """
            SELECT MIN(ITM.sCode) AS Output_Item_Code,
                   MIN(ITP.sName) AS Output_Item_Type,
                   ITMCF.sValue AS FG_Name
            FROM txnhdr HDR
            INNER JOIN TXNDET AS DET ON HDR.lId = DET.lId AND DET.cFlag = 'I'
            INNER JOIN ITMMST AS ITM ON DET.lItmId = ITM.lId
            INNER JOIN ITMTYP AS ITP ON ITP.lTypid = DET.lItmtyp
            INNER JOIN ITMCF AS ITMCF ON DET.lItmId = ITMCF.lId AND ITMCF.lFieldNo = 10 AND ITMCF.lLine = 0
            WHERE HDR.lTypid IN (597, 924, 913, 925, 899, 891)
              AND HDR.lCompid = 27
              AND HDR.bDel = 0
              AND ITP.sName NOT IN ('Finished Good')
              AND ITMCF.sValue LIKE %s
            GROUP BY ITMCF.sValue;
        """
    else:  # 'stage_name'
        sql_query = """
            SELECT DISTINCT
                   ITM.sCode AS Output_Item_Code,
                   ITP.sName AS Output_Item_Type,
                   ITM.sName AS Output_Item_Name,
                   ITMCF.sValue AS FG_Name
            FROM txnhdr HDR
            INNER JOIN TXNDET AS DET ON HDR.lId = DET.lId AND DET.cFlag = 'I'
            INNER JOIN ITMMST AS ITM ON DET.lItmId = ITM.lId
            INNER JOIN ITMTYP AS ITP ON ITP.lTypid = DET.lItmtyp
            INNER JOIN ITMCF AS ITMCF ON DET.lItmId = ITMCF.lId AND ITMCF.lFieldNo in(10,8) AND ITMCF.lLine = 0
            WHERE HDR.lTypid IN (597, 924, 913, 925, 899, 891)
              AND HDR.lCompid = 27
              AND HDR.bDel = 0
              AND ITP.sName NOT IN ('Finished Good')
              AND ITM.sName LIKE %s;
        """

    with connections['readonly_db'].cursor() as cursor:
        cursor.execute(sql_query, [like_term])
        rows = cursor.fetchall()

    results = []
    for row in rows:
        if search_type == 'fg_name':
            output_item_code, output_item_type, fg_name = row
            results.append({
                'id': fg_name,
                'text': fg_name,
                'product_code': output_item_code,
                'output_item_type': output_item_type
            })
        else:
            output_item_code, output_item_type, output_item_name, fg_name = row
            if fg_filter and fg_filter.lower() != fg_name.lower():
                continue
            results.append({
                'id': output_item_name,
                'text': output_item_name,
                'product_code': output_item_code,
                'fg_name': fg_name
            })

    return JsonResponse({'results': results})



@csrf_exempt
def search_batch(request):
    """
    AJAX endpoint for batch_no. 
    Filters by the selected stage_name and optionally by a typed term for partial searching.
    """
    stage_name = request.GET.get('stage_name', '')
    term = request.GET.get('term', '')  # user-typed text for partial matching
    like_term = f'%{term}%'

    sql_query = """
        SELECT DISTINCT [O/P Batch No]
        FROM (
            SELECT
                CASE HDR.ltypid
                    WHEN 664 THEN 'Fresh Batch BMR Issue'
                    WHEN 717 THEN 'Cleaning Batch BMR Issue'
                    WHEN 718 THEN 'Reprocess Batch BMR Issue'
                    WHEN 719 THEN 'Blending Batch BMR Issue'
                    WHEN 720 THEN 'Distillation Batch BMR Issue'
                    WHEN 721 THEN 'ETP Batch BMR Issue'
                    ELSE 'NA'
                END AS [BMR Issue Type],
                (SELECT sValue FROM txncf WHERE lid=HDR.lid AND sName='Product Name' AND lLine=0) AS [Product Name],
                (SELECT sValue FROM txncf WHERE lid=HDR.lid AND sName='Batch No' AND lLine=0) AS [O/P Batch No],
                (SELECT sValue FROM txncf WHERE lid=HDR.lid AND sName='Block' AND lLine=0) AS [Block],
                DET.lLine AS [Line No],
                ITP.sName AS [Item Type],
                ITM.sCode AS [Item Code],
                ITM.sName AS [Item Name],
                CONVERT(DECIMAL(18,3), DET.dQty2) AS [Batch Quantity]
            FROM txnhdr HDR
            INNER JOIN TXNDET AS DET ON HDR.lId = DET.lId
            INNER JOIN ITMMST AS ITM ON DET.lItmId = ITM.lId
            INNER JOIN ITMTYP AS ITP ON ITP.lTypid = DET.lItmtyp
            INNER JOIN UNTMST AS UOM ON DET.lUntId = UOM.lId
            WHERE HDR.ltypid IN (664,717,718,719,720,721)
                AND DET.lItmTyp <> 63
                AND DET.bDel <> -2
                AND HDR.bDel <> 1
                AND DET.lClosed <> -2
                AND HDR.lClosed = 0
                AND HDR.lcompid = 27
                AND CONVERT(DATE, CAST(HDR.dtDocDate AS CHAR(8)), 112)
                    BETWEEN '2025-04-01' AND GETDATE()
        ) AS batch_data
        WHERE [Product Name] = %s
          AND [O/P Batch No] LIKE %s
    """

    with connections['readonly_db'].cursor() as cursor:
        # Pass stage_name and the partial term for batch no
        cursor.execute(sql_query, [stage_name, like_term])
        rows = cursor.fetchall()

    results = []
    for row in rows:
        batch_no = row[0]
        results.append({
            'id': batch_no,
            'text': batch_no
        })

    return JsonResponse({'results': results})



@csrf_exempt
def get_bom_details(request):
    """
    POST endpoint that receives JSON with 'eqpt_id' and 'stage_name'.
    Returns:
        - 'bct' -> BOMQty from the query
        - 'bom_qty' -> Quantity from the query
    """
    if request.method != 'POST':
        return JsonResponse({"error": "Invalid request method"}, status=405)

    try:
        data = json.loads(request.body.decode("utf-8"))
        eqpt_id = data.get('eqpt_id')      # BOMItemCode
        stage_name = data.get('stage_name')  # ItemName

        if not eqpt_id or not stage_name:
            return JsonResponse({"error": "Missing eqpt_id or stage_name"}, status=400)

        # The same CTE you used, but now with placeholders for eqpt_id and stage_name
        sql_query = """
            WITH CTE_BOMDetails AS (
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY det.lBomId, lSeqId) AS [Sr.No], 
                    TYP.sName AS [ItmType],
                    MST.sName AS [ItemName],
                    MST.sCode AS [ItemCode],
                    BOM.dQty AS [Quantity],
                    BOM.dRate AS [Rate],
                    BOM.sCode AS [BOMCode],
                    BOM.sName AS [BOMName],
                    TYP1.sName AS [Type],
                    MST1.sCode AS [BOMItemCode],
                    MST1.sName AS [Name],
                    CASE 
                        WHEN det.cFlag='P' THEN CAST(det.lUntId AS VARCHAR)
                        ELSE u.sName
                    END AS [Unit],
                    BOM.cTyp AS [Based on],
                    dPercentage AS [Percentage],
                    CASE 
                        WHEN det.cFlag='P' THEN det.dQtyPrc
                        ELSE det.dQty
                    END AS [BOMQty],
                    BOM.dCnv AS [BOMCnv],
                    det.cFlag AS [cFlag],
                    DSG.sCode AS [Resource Type],
                    CASE 
                        WHEN st.lFieldNo=1 THEN BOM.svalue1
                        WHEN st.lFieldNo=2 THEN BOM.svalue2
                        WHEN st.lFieldNo=3 THEN BOM.svalue3
                        WHEN st.lFieldNo=4 THEN BOM.svalue4
                        WHEN st.lFieldNo=5 THEN BOM.svalue5
                        WHEN st.lFieldNo=6 THEN BOM.svalue6
                        WHEN st.lFieldNo=7 THEN BOM.svalue7
                        WHEN st.lFieldNo=8 THEN BOM.svalue8
                        WHEN st.lFieldNo=9 THEN BOM.svalue9
                        WHEN st.lFieldNo=10 THEN BOM.svalue10
                        ELSE ''
                    END AS [Stock Parameter]
                FROM ITMBOMDET det
                INNER JOIN ITMBOM BOM ON det.lBomId = BOM.lBomId
                INNER JOIN ITMMST MST ON MST.lId = BOM.lId
                INNER JOIN ITMTYP TYP ON TYP.lTypId = BOM.lTypId
                LEFT JOIN ITMMST MST1 ON MST1.lId = det.lBomItm
                LEFT JOIN ITMDET DT ON det.lBomItm = DT.lId
                LEFT JOIN ITMTYP TYP1 ON TYP1.lTypId = DT.lTypId
                LEFT JOIN UNTMST u ON det.lUntId = u.lId
                LEFT OUTER JOIN DSGMST DSG ON DSG.lId = det.lResourceId
                LEFT JOIN STKPRM st ON st.lTypId = TYP.lTypId AND st.bBOM = 1
            )
            SELECT 
                Quantity,      -- to populate BOM Qty field
                BOMQty         -- to populate BCT field
            FROM CTE_BOMDetails
            WHERE [Type] = 'Equipment Master'
              AND BOMItemCode = %s
              AND ItemName = %s
            ORDER BY [Sr.No];
        """

        with connections['readonly_db'].cursor() as cursor:
            cursor.execute(sql_query, [eqpt_id, stage_name])
            row = cursor.fetchone()

        if row:
            # row[0] = Quantity, row[1] = BOMQty
            quantity = float(row[0]) if row[0] else 0
            bct = float(row[1]) if row[1] else 0
            return JsonResponse({
                "bom_qty": quantity,  # For the "BOM Qty" field
                "bct": bct           # For the "BCT" field
            })
        else:
            return JsonResponse({"error": "No matching records found"}, status=404)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@login_required
def view_downtime(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('PRODUCTION.view_downtime'):
        messages.error(request, "You do not have permission to add downtime records.")
        return redirect('indexpage')

    # Permissions
    can_edit_downtime = request.user.has_perm('PRODUCTION.change_downtime')
    can_delete_downtime = request.user.has_perm('PRODUCTION.delete_downtime')
    can_add_downtime = request.user.has_perm('PRODUCTION.add_downtime')
    can_view_downtime = request.user.has_perm('PRODUCTION.view_downtime')

    # Grab filter inputs from GET parameters
    date_filter = request.GET.get('date_filter', '')
    eqpt_id_filter = request.GET.get('eqpt_id_filter', '')
    product_name_filter = request.GET.get('product_name_filter', '')
    downtime_dept_filter = request.GET.get('downtime_dept_filter', '')
    block_filter = request.GET.get('block_filter', '')

    # Base queryset
    downtimes = Downtime.objects.all().order_by('-id')  # ordering by date (modify as needed)

    # Apply filters if provided
    if date_filter:
        parsed_date = parse_date(date_filter)
        if parsed_date:
            downtimes = downtimes.filter(date=parsed_date)
    if eqpt_id_filter:
        downtimes = downtimes.filter(eqpt_id__icontains=eqpt_id_filter)
    if product_name_filter:
        downtimes = downtimes.filter(product_name__icontains=product_name_filter)
    if downtime_dept_filter:
        downtimes = downtimes.filter(downtime_dept__icontains=downtime_dept_filter)
    if block_filter:
        downtimes = downtimes.filter(block__icontains=block_filter)

    # Apply pagination: 10 records per page
    paginator = Paginator(downtimes, 10)
    page = request.GET.get('page')
    try:
        downtimes = paginator.page(page)
    except PageNotAnInteger:
        downtimes = paginator.page(1)
    except EmptyPage:
        downtimes = paginator.page(paginator.num_pages)

    context = {
        'downtimes': downtimes,
        'date_filter': date_filter,
        'eqpt_id_filter': eqpt_id_filter,
        'block_filter': block_filter,
        'downtime_dept_filter': downtime_dept_filter,
        'product_name_filter': product_name_filter,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'can_edit_downtime': can_edit_downtime,
        'can_delete_downtime': can_delete_downtime,
        'can_add_downtime': can_add_downtime,
        'can_view_downtime': can_view_downtime,
        'select_block': select_block,
        'DEPARTMENT_CHOICES': DEPARTMENT_CHOICES,
    }
    return render(request, 'downtime/view_downtime.html', context)



@login_required
def view_downtime_detail(request, pk):
    user_groups = request.user.groups.values_list('name', flat=True)  # Check if the user is in HR group
    is_superuser = request.user.is_superuser

    """ View vehicle details (Permission Required: PRODUCTION.view_downtime) """
    if not request.user.has_perm('PRODUCTION.view_downtime'):
        messages.error(request, "You do not have permission to view vehicle records.")
        return redirect('indexpage')
    """
    Displays the details of a single Downtime record.
    """
    downtime = get_object_or_404(Downtime, pk=pk)
    return render(request, 'downtime/view_downtime_detail.html', 
                  {'downtime': downtime,'user_groups':user_groups,'is_superuser':is_superuser})


@login_required
def edit_downtime(request, pk):
    """
    Edit using the *same UI* as downtime_form (stage/fg/batch search, bom qty/bct, loss/hr, etc.).
    Pre-populates via INITIAL json. Saves with the same server-side derivations as create.
    """
    downtime_obj = get_object_or_404(Downtime, pk=pk)

    if not request.user.has_perm('PRODUCTION.change_downtime'):
        messages.error(request, "You do not have permission to edit downtime records.")
        return redirect('indexpage')

    user_groups  = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    # ---- Build the SAME boot payload used by downtime_form ----
    erp_rows = _fetch_erp_rows()
    eq_rows  = _fetch_equipment_rows()
    bom_rows = _fetch_bom_first_output_rows()

    stages_set = set()
    fg_by_stage = {}
    batches_by_stage_fg = {}

    for _block, stage, fg, batch in erp_rows:
        if not stage:
            continue
        stages_set.add(stage)
        if fg and stage not in fg_by_stage:
            fg_by_stage[stage] = fg
        if fg and batch:
            key = f"{stage}||{fg}"
            batches_by_stage_fg.setdefault(key, set()).add(batch)

    stages = sorted(stages_set)
    batches_by_stage_fg_sorted = {k: sorted(v, reverse=True) for k, v in batches_by_stage_fg.items()}

    equipment_by_block = {}
    for eq_id, block, capacity_size, capacity_unit, type_eq, moc_equipment in eq_rows:
        cb = _canon_block(block)
        if cb not in {"A", "B", "C", "D", "E"}:
            continue
        cap_size = "" if capacity_size is None else str(capacity_size)
        cap_unit = "" if capacity_unit is None else str(capacity_unit)
        t_eq     = "" if type_eq is None else str(type_eq)
        moc      = "" if moc_equipment is None else str(moc_equipment)
        label = f"{eq_id} — {cap_size} {cap_unit}".strip()
        extras = [x for x in (t_eq, moc) if x]
        if extras:
            label += f" ({', '.join(extras)})"
        equipment_by_block.setdefault(cb, []).append({
            "value": str(eq_id),
            "label": label,
            "block_display": str(block) if block is not None else "",
            "capacity_size": cap_size,
            "capacity_unit": cap_unit,
            "type_eq": t_eq,
            "moc_equipment": moc,
        })
    for cb in list(equipment_by_block.keys()):
        equipment_by_block[cb] = sorted(equipment_by_block[cb], key=lambda x: x["label"])

    bom_meta_by_stage_fg = {}
    for fg_name, stage_name, bom_id, material_name, quantity, std_bct in bom_rows:
        s = (_norm(stage_name) or "")
        f = (_norm(fg_name) or "")
        if not s or not f:
            continue
        key = f"{s}||{f}"
        q   = _to_float_or_none(quantity)
        bct = _to_float_or_none(std_bct)
        rec = bom_meta_by_stage_fg.setdefault(key, {"quantities": set(), "bcts": set()})
        if q is not None:
            rec["quantities"].add(int(round(q)))
        if bct is not None and bct > 0:
            rec["bcts"].add(int(round(bct)))
    for key, rec in bom_meta_by_stage_fg.items():
        rec["quantities"] = sorted(rec["quantities"])
        rec["bcts"] = sorted(rec["bcts"])

    dc_qs = DeptCategory.objects.filter(is_active=True).values_list("department", "category")
    departments = sorted({d for d, _ in dc_qs})
    categories_by_dept = {d: [] for d in departments}
    for d, c in dc_qs:
        categories_by_dept[d].append(c)
    for d in categories_by_dept:
        categories_by_dept[d].sort()

    blocks_display = ["A-Block", "B-Block", "C-Block", "D-Block", "E-Block"]
    block_display_to_key = {disp: _canon_block(disp) for disp in blocks_display}

    boot = {
        "blocks_display": blocks_display,
        "block_display_to_key": block_display_to_key,
        "equipment_by_block": equipment_by_block,
        "stages": stages,
        "fg_by_stage": fg_by_stage,
        "batches_by_stage_fg": batches_by_stage_fg_sorted,
        "bom_meta_by_stage_fg": bom_meta_by_stage_fg,
        "departments": departments,
        "categories_by_dept": categories_by_dept,
    }
    boot_json = json.dumps(boot, ensure_ascii=False).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")

    # Default record_dt (used by the datetime-local control)
    def _now_ist_for_datetime_local():
        ist_now = datetime.now(ZoneInfo("Asia/Kolkata"))
        return ist_now.strftime("%Y-%m-%dT%H:%M:%S")
    record_dt_default = _now_ist_for_datetime_local()

    # ---- Build INITIAL payload from the existing object (for prefill) ----
    def _fmt_time(t):
        return t.strftime("%H:%M:%S") if t else ""
    def _fmt_date(d):
        return d.isoformat() if d else ""

    initial = {
        "id": downtime_obj.id,
        "idle": downtime_obj.idle or "No",
        "block": downtime_obj.block or "",
        "eqpt_id": downtime_obj.eqpt_id or "",
        "eqpt_name": downtime_obj.eqpt_name or "",
        "stage_name": downtime_obj.stage_name or "",
        "product_name": downtime_obj.product_name or "",
        "batch_no": downtime_obj.batch_no or "",
        "start_date": _fmt_date(downtime_obj.start_date),
        "end_date": _fmt_date(downtime_obj.end_date),
        "start_time": _fmt_time(downtime_obj.start_time),
        "end_time": _fmt_time(downtime_obj.end_time),
        "duration_hours": float(downtime_obj.total_duration) if downtime_obj.total_duration is not None else "",
        "bom_qty": int(downtime_obj.bom_qty) if downtime_obj.bom_qty not in (None, "") else "",
        "bct": int(downtime_obj.bct) if downtime_obj.bct not in (None, "") else "",
        "loss": int(downtime_obj.loss) if downtime_obj.loss not in (None, "") else "",
        "downtime_dept": downtime_obj.downtime_dept or "",
        "downtime_category": downtime_obj.downtime_category or "",
        "reason": downtime_obj.reason or "",
        "date": _fmt_date(downtime_obj.date),
        # record_dt for the control: combine date + start_time if available
        "record_dt": (
            f"{_fmt_date(downtime_obj.date)}T{_fmt_time(downtime_obj.start_time or time(0,0,0))}"
            if downtime_obj.date else record_dt_default
        ),
    }
    initial_json = json.dumps(initial, ensure_ascii=False).replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")

    # ---- POST: update the object (same derivations as create) ----
    if request.method == "POST":
        G = request.POST.get

        idle               = (G("idle_flag") or "").strip() or None
        block              = (G("block") or "").strip() or None
        eqpt_id            = (G("equipment_id") or "").strip() or None
        eqpt_name          = (G("eqpt_name") or "").strip() or None
        stage_name         = (G("stage_name") or "").strip() or None
        product_name       = (G("product_name") or "").strip() or None
        batch_no           = (G("batch_no") or "").strip() or None
        downtime_dept      = (G("downtime_dept") or "").strip() or None
        downtime_category  = (G("downtime_category") or "").strip() or None
        reason             = (G("reason") or "").strip() or None

        date_str       = (G("record_dt") or "").strip()
        start_date_str = (G("start_date") or "").strip()
        end_date_str   = (G("end_date") or "").strip()
        start_time_str = (G("start_time") or "").strip()
        end_time_str   = (G("end_time") or "").strip()

        def _parse_date(s):
            try: return datetime.strptime(s, "%Y-%m-%d").date()
            except: return None

        def _parse_time(s):
            if not s: return None
            if len(s) == 5: s = s + ":00"
            try: return datetime.strptime(s, "%H:%M:%S").time()
            except: return None

        def _parse_dtlocal(s):
            try: return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S")
            except: return None

        record_dt = _parse_dtlocal(date_str)
        date_for_row = (record_dt.date() if record_dt else downtime_obj.date)

        s_date = _parse_date(start_date_str)
        e_date = _parse_date(end_date_str)
        s_time = _parse_time(start_time_str)
        e_time = _parse_time(end_time_str)

        def _to_float(s):
            try: return float(s)
            except: return None

        bom_qty        = _to_float(G("bom_qty"))
        bct            = _to_float(G("bom_bct"))
        duration_hours = _to_float(G("duration_hours"))
        total_loss     = _to_float(G("total_loss"))

        # server derives loss/hr, duration, total loss if missing
        loss_per_hr = None
        if bom_qty is not None and bct not in (None, 0):
            try: loss_per_hr = round(bom_qty / bct)
            except: loss_per_hr = None

        if duration_hours is None and s_date and e_date and s_time and e_time:
            try:
                s_dt = datetime.combine(s_date, s_time)
                e_dt = datetime.combine(e_date, e_time)
                delta = (e_dt - s_dt).total_seconds() / 3600.0
                if delta >= 0:
                    duration_hours = round(delta, 2)
            except:
                duration_hours = None

        if total_loss is None and loss_per_hr is not None and duration_hours is not None:
            total_loss = round(loss_per_hr * duration_hours)

        try:
            downtime_obj.date              = date_for_row
            downtime_obj.idle              = idle or "No"
            downtime_obj.eqpt_id           = eqpt_id
            downtime_obj.eqpt_name         = eqpt_name
            downtime_obj.product_name      = product_name
            downtime_obj.stage_name        = stage_name
            downtime_obj.product_code      = None
            downtime_obj.batch_no          = batch_no
            downtime_obj.start_date        = s_date
            downtime_obj.end_date          = e_date
            downtime_obj.start_time        = s_time
            downtime_obj.end_time          = e_time
            downtime_obj.total_duration    = duration_hours
            downtime_obj.block             = block
            downtime_obj.downtime_dept     = downtime_dept
            downtime_obj.downtime_category = downtime_category
            downtime_obj.reason            = (reason[:500] if reason else None)
            downtime_obj.bom_qty           = bom_qty
            downtime_obj.bct               = bct
            downtime_obj.loss              = total_loss
            downtime_obj.save()

            messages.success(request, f"Downtime updated (ID #{downtime_obj.id}).")
            return redirect('view_downtime')
        except Exception as exc:
            logger.exception("Failed to update downtime")
            messages.error(request, f"Failed to update record: {exc}")

    return render(
        request,
        "downtime/edit_downtime.html",  # template below
        {
            "boot_json": boot_json,
            "initial_json": initial_json,
            "record_dt_default": record_dt_default,
            "user_groups": user_groups,
            "is_superuser": is_superuser,
        },
    )

@login_required
def delete_downtime(request, pk):
    user_groups = request.user.groups.values_list('name', flat=True)  # Check if the user is in HR group
    is_superuser = request.user.is_superuser

    """ View downtime details (Permission Required: PRODUCTION.delete_downtime) """
    if not request.user.has_perm('PRODUCTION.delete_downtime'):
        messages.error(request, "You do not have permission to delete vehicle records.")
        return redirect('indexpage')
    """
    Deletes a Downtime record.
    For GET requests, shows a confirmation page.
    For POST requests, deletes the record and redirects to the listing page.
    """
    downtime = get_object_or_404(Downtime, pk=pk)
    if request.method == 'POST':
        downtime.delete()
        messages.success(request, "Downtime record deleted successfully!")
        return redirect('view_downtime')
    # Render a confirmation page for GET requests
    return render(request, 'downtime/confirm_delete.html',
              {'downtime': downtime, 'user_groups': user_groups, 'is_superuser': is_superuser})


@login_required
def export_downtime_excel(request):
    if not request.user.has_perm('PRODUCTION.view_downtime'):
        messages.error(request, "You do not have permission to export downtime records.")
        return redirect('indexpage')

    # Filters
    date_filter = request.GET.get('date_filter', '')
    eqpt_id_filter = request.GET.get('eqpt_id_filter', '')
    block_filter = request.GET.get('block_filter', '')
    downtime_dept_filter = request.GET.get('downtime_dept_filter', '')
    product_name_filter = request.GET.get('product_name_filter', '')

    downtimes = Downtime.objects.all().order_by('-id')

    if date_filter:
        parsed_date = parse_date(date_filter)
        if parsed_date:
            downtimes = downtimes.filter(date=parsed_date)
    if eqpt_id_filter:
        downtimes = downtimes.filter(eqpt_id__icontains=eqpt_id_filter)
    if block_filter:
        downtimes = downtimes.filter(block__icontains=block_filter)
    if downtime_dept_filter:
        downtimes = downtimes.filter(downtime_dept__icontains=downtime_dept_filter)
    if product_name_filter:
        downtimes = downtimes.filter(product_name__icontains=product_name_filter)

    # Prepare DataFrame
    data = downtimes.values(
        'id', 'date', 'eqpt_id', 'eqpt_name', 'product_name', 'product_code', 'batch_no',
        'stage_name', 'block', 'downtime_dept', 'downtime_category', 'reason',
        'start_date', 'start_time', 'end_date', 'end_time', 'total_duration', 'bom_qty', 'bct', 'loss'
    )

    df = pd.DataFrame(data)

    # Rename columns for better presentation
    df.rename(columns={
        'id': 'ID',
        'date': 'Date',
        'eqpt_id': 'Equipment ID',
        'eqpt_name': 'Equipment Name',
        'product_name': 'Product Name',
        'product_code': 'Product Code',
        'batch_no': 'Batch No',
        'stage_name': 'Stage Name',
        'block': 'Block',
        'downtime_dept': 'Downtime Department',
        'downtime_category': 'Downtime Category',
        'reason': 'Reason',
        'start_date': 'Start Date',
        'start_time': 'Start Time',
        'end_date': 'End Date',
        'end_time': 'End Time',
        'total_duration': 'Total Duration (hrs)',
        'bom_qty': 'BOM Qty',
        'bct': 'BCT',
        'loss': 'Loss (Kg)'
    }, inplace=True)

    # Generate Excel in-memory
    response = HttpResponse(
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = 'attachment; filename=downtime_records.xlsx'

    with pd.ExcelWriter(response, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Downtime', index=False)

    return response

########################################################################################################
# PRODUCTION/views.py
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.serializers.json import DjangoJSONEncoder
from django.db.models import Sum, Q
from django.http import JsonResponse
from django.shortcuts import render, redirect
from django.utils.dateparse import parse_date

from .models import Downtime, DowntimeCriticalEquip
from django.db import connections

# --- small helpers (unchanged) ---
def _to_iso_date(d): return d.isoformat() if d else None
def _to_iso_time(t): return t.strftime("%H:%M:%S") if t else None
def _to_number(x):
    if x is None: return None
    try: return float(x)
    except Exception:
        try: return int(x)
        except Exception: return None

def _parse_date_any(s: str):
    if not s: return None
    s = s.strip()
    d = parse_date(s)
    if d: return d
    try:
        dd, mm, yyyy = s.split("/")
        return parse_date(f"{yyyy}-{mm}-{dd}")
    except Exception:
        return None

def _alpha_maps():
    name2code, code2names = {}, {}
    with connections['production_scheduler'].cursor() as cur:
        cur.execute("SELECT product_name, alpha_code FROM alpha")
        for prod, code in cur.fetchall():
            p = (prod or "").strip()
            c = (code or "").strip()
            if not p or not c:
                continue
            name2code[p.lower()] = c
            code2names.setdefault(c, set()).add(p)
    return name2code, code2names


@login_required
def downtime_report(request):
    """
    JSON when Accept includes application/json or ?json=1.
    Adds equipment-level metrics in payload['equip_stats'] with:
      - bct (max), op_qty (avg), loss_per_hr (= op_qty / bct)

    Also adds FG-level SFG Output in payload['fg_sfg_output']:
      - key: alpha_code (if available) else product_name
      - value: max(bom_qty) for SFG stage, using data BEFORE stage/eqpt filters
    """
    if not request.user.has_perm('PRODUCTION.view_downtime'):
        if "application/json" in request.headers.get("Accept", "") or request.GET.get("json") == "1":
            return JsonResponse({"error": "No permission to view downtime records."}, status=403)
        messages.error(request, "You do not have permission to view downtime records.")
        return redirect('indexpage')

    wants_json = ("application/json" in request.headers.get("Accept", "")) or (request.GET.get("json") == "1")
    if not wants_json:
        return render(request, "downtime/downtime_report.html")

    # ---------- params ----------
    p_from  = (request.GET.get("from") or "").strip()
    p_to    = (request.GET.get("to") or "").strip()
    sel_prod_codes = [s.strip() for s in request.GET.getlist("product") if s.strip()]
    sel_stages     = [s.strip() for s in request.GET.getlist("stage")   if s.strip()]
    sel_eqpts      = [s.strip() for s in request.GET.getlist("eqpt")    if s.strip()]
    debug_mode     = (request.GET.get("debug") or "").strip().lower()
    try:
        debug_limit = int(request.GET.get("debug_limit", "500"))
    except Exception:
        debug_limit = 500

    name2code, code2names = _alpha_maps()

    # ---------- base queryset ----------
    qs = Downtime.objects.all().order_by("id")
    total_before_filters = qs.count()

    d_from = _parse_date_any(p_from)
    d_to   = _parse_date_any(p_to)

    # robust date/overlap filter
    if d_from or d_to:
        q = Q()
        if d_from and d_to:
            overlap = Q(start_date__isnull=False, end_date__isnull=False,
                        start_date__lte=d_to, end_date__gte=d_from)
            has_date_in = Q(date__isnull=False)
            if d_from: has_date_in &= Q(date__gte=d_from)
            if d_to:   has_date_in &= Q(date__lte=d_to)
            only_start_in = Q(start_date__isnull=False, end_date__isnull=True)
            if d_from: only_start_in &= Q(start_date__gte=d_from)
            if d_to:   only_start_in &= Q(start_date__lte=d_to)
            only_end_in = Q(start_date__isnull=True, end_date__isnull=False)
            if d_from: only_end_in &= Q(end_date__gte=d_from)
            if d_to:   only_end_in &= Q(end_date__lte=d_to)
            q = overlap | has_date_in | only_start_in | only_end_in
        elif d_from:
            q = (
                Q(date__isnull=False, date__gte=d_from) |
                Q(end_date__isnull=False, end_date__gte=d_from) |
                Q(start_date__isnull=False, end_date__isnull=True, start_date__gte=d_from)
            )
        else:
            q = (
                Q(date__isnull=False, date__lte=d_to) |
                Q(start_date__isnull=False, start_date__lte=d_to) |
                Q(start_date__isnull=True, end_date__isnull=False, end_date__lte=d_to)
            )
        qs = qs.filter(q)

    after_date_count = qs.count()

    # ---------- product filter ----------
    if sel_prod_codes:
        wanted_names = set()
        for code in sel_prod_codes:
            if code in code2names:
                wanted_names.update(code2names[code])
            else:
                wanted_names.add(code)
        if wanted_names:
            qs = qs.filter(product_name__in=list(wanted_names))

    # keep a copy *before* stage/eqpt filters – used for SFG Output
    qs_for_sfg = qs

    # ---------- stage/eqpt filters ----------
    if sel_stages:
        qs = qs.filter(stage_name__in=sel_stages)
    if sel_eqpts:
        qs = qs.filter(eqpt_id__in=sel_eqpts)

    after_all_filters = qs.count()

    # ---------- critical equipment ----------
    crit_qs = DowntimeCriticalEquip.objects.all()
    if d_from: crit_qs = crit_qs.filter(to_date__gte=d_from)
    if d_to:   crit_qs = crit_qs.filter(from_date__lte=d_to)
    if sel_prod_codes:
        selected_names = set()
        for c in sel_prod_codes:
            if c in code2names: selected_names.update(code2names[c])
            else: selected_names.add(c)
        if selected_names:
            crit_qs = crit_qs.filter(fg__in=list(selected_names))
    if sel_stages: crit_qs = crit_qs.filter(stage__in=sel_stages)
    if sel_eqpts:  crit_qs = crit_qs.filter(eqp__in=sel_eqpts)

    critical, critical_set = [], set()
    for c in crit_qs.iterator():
        fg_name = (c.fg or "").strip()
        fg_code = name2code.get(fg_name.lower())
        fg_key  = fg_code or fg_name
        item = {
            "fg": fg_key,
            "stage": (c.stage or "").strip(),
            "eqp": (c.eqp or "").strip(),
            "from": c.from_date.isoformat() if c.from_date else None,
            "to":   c.to_date.isoformat()   if c.to_date   else None,
        }
        critical.append(item)
        critical_set.add((fg_key, item["stage"], item["eqp"]))

    fgs_in_rows = set(qs.values_list("product_name", flat=True))
    fgs_with_critical = set()
    for fg_key, _stg, _eqp in critical_set:
        if fg_key in code2names:
            fgs_with_critical.update(code2names[fg_key])
        else:
            fgs_with_critical.add(fg_key)
    no_critical_fgs = sorted([fg for fg in fgs_in_rows if fg and fg not in fgs_with_critical])

    # ---------- options ----------
    prod_names = qs.exclude(product_name__isnull=True).values_list("product_name", flat=True).distinct()
    prod_codes = set((name2code.get((pn or "").strip().lower()) or pn or "") for pn in prod_names)
    products_opt = sorted({p for p in prod_codes if p})

    stages_opt = list(qs.exclude(stage_name__isnull=True).values_list("stage_name", flat=True).distinct().order_by("stage_name"))
    eqpts_opt  = list(qs.exclude(eqpt_id__isnull=True).values_list("eqpt_id", flat=True).distinct().order_by("eqpt_id"))

    # ---------- totals ----------
    sums = qs.aggregate(sum_duration=Sum("total_duration"), sum_loss=Sum("loss"))
    sum_duration = float(sums["sum_duration"] or 0)
    sum_loss = float(sums["sum_loss"] or 0)

    # ---------- serialize rows ----------
    rows = []
    for dt in qs.iterator():
        pn = dt.product_name or ""
        code = name2code.get(pn.strip().lower())
        rows.append({
            "id": dt.id,
            "date": _to_iso_date(dt.date),
            "idle": dt.idle or "No",
            "eqpt_id": dt.eqpt_id,
            "eqpt_name": dt.eqpt_name,
            "product_name": pn,
            "alpha_code": code or pn,
            "product_code": dt.product_code,
            "batch_no": dt.batch_no,
            "stage_name": dt.stage_name,
            "block": dt.block,
            "downtime_dept": dt.downtime_dept,
            "downtime_category": dt.downtime_category,
            "reason": dt.reason,
            "start_date": _to_iso_date(dt.start_date),
            "start_time": _to_iso_time(dt.start_time),
            "end_date": _to_iso_date(dt.end_date),
            "end_time": _to_iso_time(dt.end_time),
            "total_duration": _to_number(dt.total_duration),
            "bom_qty": _to_number(dt.bom_qty),
            "bct": _to_number(dt.bct),
            "loss": _to_number(dt.loss),
        })

    # ---------- equipment-level metrics (BCT, O/P QTY, Stage Loss/hr) ----------
    from collections import defaultdict
    grp = defaultdict(lambda: {"bcts": [], "boms": []})

    def _key(row):
        # use alpha_code if present, else product_name (to match the frontend grouping)
        fg = (row.get("alpha_code") or row.get("product_name") or "").strip()
        st = (row.get("stage_name") or "").strip()
        eq = (row.get("eqpt_id") or "").strip()
        return (fg, st, eq)

    for r in rows:
        bct = r.get("bct")
        bom = r.get("bom_qty")
        if bct and float(bct) > 0:
            grp[_key(r)]["bcts"].append(float(bct))
        if bom and float(bom) > 0:
            grp[_key(r)]["boms"].append(float(bom))

    def _avg(vals):
        return (sum(vals) / len(vals)) if vals else 0.0

    equip_stats = []
    for (fg, stage, eqp), buckets in grp.items():
        max_bct = max(buckets["bcts"]) if buckets["bcts"] else 0.0           # BCT = MAX
        op_qty  = _avg(buckets["boms"])                                      # O/P QTY = AVG
        loss_hr = (op_qty / max_bct) if max_bct > 0 else 0.0                 # Stage Loss/hr
        equip_stats.append({
            "fg": fg,
            "stage": stage,
            "eqp": eqp,
            "bct": round(max_bct, 2),
            "op_qty": round(op_qty, 2),
            "loss_per_hr": round(loss_hr, 2),
            "bct_str": f"{max_bct:.2f}",
            "op_qty_str": f"{op_qty:.2f}",
            "loss_per_hr_str": f"{loss_hr:.2f}",
        })

    # ---------- FG-level SFG Output (independent of stage/eqpt filters) ----------
    fg_sfg_output = {}
    sfg_qs = qs_for_sfg.filter(stage_name__icontains="sfg")
    for dt in sfg_qs.iterator():
        pn = dt.product_name or ""
        code = name2code.get(pn.strip().lower())
        fg_key = code or pn
        val = _to_number(dt.bom_qty)
        if val is None:
            continue
        v = float(val)
        # take max SFG output per FG (you can change to avg if needed)
        existing = fg_sfg_output.get(fg_key)
        if existing is None or v > existing:
            fg_sfg_output[fg_key] = v

    # ---------- debug ----------
    if debug_mode == "print":
        import json as _json
        logger.warning("DT-REPORT DEBUG PRINT >>> filters=%s", {
            "from": p_from, "to": p_to,
            "products": sel_prod_codes, "stages": sel_stages, "eqpts": sel_eqpts,
            "counts": {"total_before": total_before_filters,
                       "after_date": after_date_count,
                       "after_all": after_all_filters}
        })
        for r in rows:
            logger.warning("DT-ROW %s", _json.dumps(r, ensure_ascii=False))
        for s in equip_stats:
            logger.warning("DT-EQPT %s", _json.dumps(s, ensure_ascii=False))
        logger.warning("DT-FG-SFG-OUTPUT %s", _json.dumps(fg_sfg_output, ensure_ascii=False))
        return JsonResponse({
            "printed_rows": len(rows),
            "printed_equip_stats": len(equip_stats),
            "printed_fg_sfg_output": len(fg_sfg_output)
        })

    payload = {
        "count": len(rows),
        "sum_duration": sum_duration,
        "sum_loss": sum_loss,
        "rows": rows,
        "equip_stats": equip_stats,        # per (FG, Stage, Eqpt) metrics
        "fg_sfg_output": fg_sfg_output,    # NEW: FG → SFG Output
        "options": {"products": products_opt, "stages": stages_opt, "eqpts": eqpts_opt},
        "critical": critical,
        "no_critical_fgs": no_critical_fgs,
    }

    if debug_mode in {"1", "all"}:
        sample = rows if debug_mode == "all" else rows[:min(debug_limit, 200)]
        payload["debug"] = {
            "filters": {"from": p_from, "to": p_to,
                        "products": sel_prod_codes, "stages": sel_stages, "eqpts": sel_eqpts},
            "counts": {"total_before": total_before_filters,
                       "after_date": after_date_count,
                       "after_all": after_all_filters},
            "critical_count": len(critical),
            "debug_rows": sample,
            "equip_stats_sample": equip_stats[:min(debug_limit, 200)],
            "fg_sfg_output": fg_sfg_output,
        }

    return JsonResponse(payload, encoder=DjangoJSONEncoder, safe=False)
