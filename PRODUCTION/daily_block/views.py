# daily_block/views.py
from datetime import time,datetime
from io import StringIO
from pprint import pprint
from django.contrib import messages
from django.contrib.admin.views.decorators import staff_member_required
from django.contrib.auth.decorators import login_required
from django.core.management import call_command
from django.db.models import Count, OuterRef, Subquery, Q
from django.http import JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.utils.text import slugify
from django.views.decorators.http import require_http_methods
from .forms import ( HeaderForm, BLOCK_CHOICES, AssetsFS)
from .models import *
from django.core.paginator import Paginator
from decimal import Decimal
import json
import logging
from django.db import transaction
from django.db import connections
from django.views.decorators.http import require_GET
from django.shortcuts import render
from django.utils.dateparse import parse_date
from django.db.models.functions import Substr, Cast
from django.db import transaction, IntegrityError
from django.db.models import Max, Q, Sum, Avg, Count, F, Case, When, IntegerField
from celery.result import AsyncResult
from .tasks import sync_erp_task # Import the new task
import csv
from django.http import JsonResponse, HttpResponse
from django.shortcuts import render
from .utils import _parse_date
from datetime import date as _date
from io import BytesIO
import xlsxwriter
import io
from contextlib import redirect_stdout


# Initialize custom logger
logger = logging.getLogger('custom_logger')


# ───────────────────────────────────────────────────────────────

# ───────────────────────────────────────────────────────────────
#  SMALL LOOK‑UP HELPER
# ───────────────────────────────────────────────────────────────
def _static_context() -> dict:
    """Tiny helper that returns small, cache‑friendly look‑ups."""
    return {
        "block_list":          [c[0] for c in BLOCK_CHOICES],
        "manpower_categories": (
            "Operators",
            "Supervisors",
            "Technicians",
            "Contract Workers",
        ),
        "stage_list":     ["Stage-1", "Stage-2", "Stage-3", "Stage-4"],
        "effluent_types": ["Acidic", "Basic", "Neutral", "Sodium Cyanide Effluent", "3‑CHP Effluent", "Ammonium Chloride Effluent", "Sulphuric > 50 % Effluent", "Sulphuric ≤ 50 % Effluent", "Residue"],
        "water_types":    ["Process", "Cooling Tower", "Boiler"],
        "scrub_types":    ["Scrubber HCl 30‑32 %", "Scrubber Basic Effluent", "Scrubber Acidic Effluent", "QC Effluent", "Drainage Effluent", "Dyke Effluent", "PCO Cleaning / Cleaning Effluent", "Ejector Effluent", "Blow‑down Effluent (Cooling Tower)", "Scrubber NOx Effluent"],
        "equip_fields":   ["status", "remarks", "downtime_hrs"],
        "stock_fg_objs": [
            type("FG", (), {"product_id": "FG-001", "product_name": "API X"}),
            type("FG", (), {"product_id": "FG-002", "product_name": "API Y"}),
        ],
    }


# ───────────────────────────────────────────────────────────────
#  AJAX HELPERS
# ───────────────────────────────────────────────────────────────
@login_required
def stage_list_api(request):
    stages = list(ERPBOMDetail.objects.values_list("stage_name", flat=True))
    return JsonResponse(stages, safe=False)


@login_required
def stage_detail_api(request):
    stage = request.GET.get("stage")
    try:
        detail = ERPBOMDetail.objects.get(stage_name=stage)
        return JsonResponse(
            {
                "stage":     detail.stage_name,
                "fg_name":   detail.fg_name,
                "equipment": detail.equipment,
            }
        )
    except ERPBOMDetail.DoesNotExist:
        return JsonResponse({"error": "Not found"}, status=404)





    


##########  To fetch equipment from production schedular  = #####
@login_required
def get_equipment_api(request):
    """
    Returns a JSON list of equipment codes from the 'equipment' table
    in the production_scheduler database, filtered by the selected block,
    using a direct database connection.
    """
    block_display_name = request.GET.get('block')
    if not block_display_name:
        return JsonResponse([], safe=False)

    try:
        # Step 1: Get the block code (e.g., "A-Block") from the display name
        block_obj = Block.objects.get(display_name=block_display_name)
        block_code = block_obj.code

        equipment_codes = []
        # Step 2: Use a direct connection to the 'production_scheduler' database
        #
        # ▼▼▼ THIS IS THE CORRECTED LINE ▼▼▼
        with connections['production_scheduler'].cursor() as cursor:
            #
            # ▲▲▲ THE FIX IS HERE ▲▲▲

            # Step 3: Write and execute a raw SQL query.
            sql_query = "SELECT eq_id FROM equipment WHERE block = %s ORDER BY eq_id"
            cursor.execute(sql_query, [block_code])
            
            # Step 4: Fetch all results.
            rows = cursor.fetchall()
            
            # Step 5: Convert the list of tuples into a simple list of strings
            equipment_codes = [row[0] for row in rows]

        return JsonResponse(equipment_codes, safe=False)

    except Block.DoesNotExist:
        return JsonResponse([], safe=False)
    except Exception as e:
        print(f"An error occurred in get_equipment_api: {e}")
        return JsonResponse({'error': 'Failed to retrieve equipment data.'}, status=500)



@login_required
def get_batch_numbers_api(request):
    """
    Returns a JSON list of batch numbers based on the product (FG Name)
    associated with a given stage.
    """
    stage_name = request.GET.get('stage')
    if not stage_name:
        return JsonResponse([], safe=False)

    try:
        # Find the product (FG Name) for the given stage
        bom_detail = ERPBOMDetail.objects.get(stage_name=stage_name)
        product_to_filter = bom_detail.fg_name
    except ERPBOMDetail.DoesNotExist:
        return JsonResponse([], safe=False)

    # Fetch ALL distinct batch numbers for that product
    batch_numbers = list(
        BmrIssue.objects
        .filter(fg_name=product_to_filter)
        .values_list('op_batch_no', flat=True)
        .order_by('-op_batch_no')  # Show most recent first in search
        .distinct()
    )
    return JsonResponse(batch_numbers, safe=False)






@login_required
@require_GET
def get_bom_details_by_stage_api(request):
    """
    Fetch BOM details (input/output/waste/equipment) for a stage.
    If multiple BOMHeader rows exist for the same stage, the 'equip'
    query param is required and is matched against fixed_equipment_id.
    """
    stage_name = (request.GET.get('stage') or '').strip()
    equip = (request.GET.get('equip') or '').strip()  # selected Out Equipment

    if not stage_name:
        return JsonResponse({'error': 'Stage parameter is required'}, status=400)

    qs = BOMHeader.objects.filter(stage_name=stage_name)

    if not qs.exists():
        return JsonResponse({'error': f'BOM not found for stage "{stage_name}"'}, status=404)

    # If exactly one BOM, use it
    header = None
    if qs.count() == 1:
        header = qs.first()
    else:
        # Multiple BOMs for this stage → require equipment
        if not equip:
            return JsonResponse(
                {
                    'error': 'Multiple BOMs for this stage; equipment selection required.',
                    'needs_equipment': True
                },
                status=409
            )

        try:
            header = qs.filter(fixed_equipment_id=equip).first() or \
                    qs.filter(equipments__equipment_ref=equip).distinct().first()
        except Exception as ex:
            return JsonResponse({'error': f'Query error: {ex}'}, status=500)

        if header is None:
            return JsonResponse(
                {
                    'error': f'No BOM matches stage "{stage_name}" with equipment "{equip}".'
                },
                status=404
            )

    # Serialize lines
    inputs = list(
        header.lines.filter(line_type='input').values(
            'material_category', 'material_name', 'quantity', 'litre', 'density', 'ratio'
        )
    )
    outputs = list(
        header.lines.filter(line_type='output').values(
            'material_category', 'material_name', 'quantity', 'litre', 'density', 'ratio'
        )
    )
    waste = list(
        header.lines.filter(line_type='waste').values(
            'material_category', 'material_name', 'quantity', 'litre', 'density', 'ratio'
        )
    )
    equipment = list(
        header.equipments.all().values(
            'equipment_type', 'moc_equipment', 'capacity_size', 'equipment_ref', 'std_bct', 'wait_time'
        )
    )

    return JsonResponse({
        'inputs': inputs,
        'outputs': outputs,
        'waste': waste,
        'equipment': equipment,
    })



@login_required
def get_all_bom_equipment_api(request):
    """
    Fetches all unique equipment details from the BOMEquipment table in the
    ProductionScheduler database. This is used to populate dropdowns for
    new, user-added equipment rows in the daily check form.
    """
    try:
        # The db_router automatically points this read to 'production_scheduler'
        equipment = list(BOMEquipment.objects.values(
            'equipment_type',
            'moc_equipment',
            'capacity_size',
            'equipment_ref',
            'std_bct',
            'wait_time'
        ).distinct())
        return JsonResponse(equipment, safe=False)
    except Exception as e:
        # A simple logging statement for debugging on the server
        print(f"Error in get_all_bom_equipment_api: {e}")
        return JsonResponse({'error': 'An internal server error occurred.'}, status=500)





def _alloc_next_txn(prefix="BLPRD"):
    fy_two = timezone.now().year % 100
    base = f"{prefix}/{fy_two:02d}/"
    # Extract the last 6 chars as integer and get MAX
    # transaction_number format: PREFIX/YY/######  -> fixed-width numeric suffix
    suffix_len = 6
    qs = (DailyCheckHeader.objects
          .filter(transaction_number__startswith=base)
          .annotate(num=Cast(Substr("transaction_number", len(base)+1, suffix_len), models.IntegerField()))
          .aggregate(mx=Max("num")))
    next_seq = (qs["mx"] or 0) + 1
    return f"{base}{next_seq:06d}"



@login_required
def daily_check_entry(request, pk=None):
    """Create or edit a Daily-Check, handling asset/inventory and BOM child rows."""
    instance = get_object_or_404(DailyCheckHeader, pk=pk) if pk else None

    # --- POST (SAVING) LOGIC ---
    if request.method == "POST":
        header_form = HeaderForm(request.POST or None, instance=instance)
        if "transaction_number" in header_form.fields:
            del header_form.fields["transaction_number"]
            
        if header_form.is_valid():
            header = header_form.save(commit=False)
            header.prepared_by = request.user
            if not instance:
                header.transaction_number = _alloc_next_txn()
            header.save()

            AssetsInventory.objects.filter(daily_check=header).delete()

            o = 0
            while any(k.startswith(f"output-{o}-") for k in request.POST):
                if request.POST.get(f"output-{o}-product") or request.POST.get(f"output-{o}-qty"):
                    out_start_str = request.POST.get(f"output-{o}-starttime")
                    out_end_str = request.POST.get(f"output-{o}-endtime")
                    out_start_dt = datetime.fromisoformat(out_start_str) if out_start_str else None
                    out_end_dt = datetime.fromisoformat(out_end_str) if out_end_str else None
                    out_bct = None
                    if out_start_dt and out_end_dt and out_end_dt > out_start_dt:
                        duration = out_end_dt - out_start_dt
                        out_bct = Decimal(duration.total_seconds() / 3600).quantize(Decimal("0.01"))
                    out_status_val = "Completed" if out_end_dt else "Online"

                    output_row = AssetsInventory.objects.create(
                        daily_check=header, row_flag="OUTPUT",
                        batch_size=request.POST.get(f"output-{o}-batch_size") or None,
                        out_stage=request.POST.get(f"output-{o}-stage", ""),
                        out_item_id=request.POST.get(f"output-{o}-product", ""),
                        out_bno=request.POST.get(f"output-{o}-bno", ""),
                        out_equip=request.POST.get(f"output-{o}-equip", ""),
                        out_b_starttime=out_start_dt,
                        out_b_endtime=out_end_dt,
                        out_bct=out_bct,
                        out_qty=request.POST.get(f"output-{o}-qty") or None,
                        out_remarks=request.POST.get(f"output-{o}-remarks", ""),
                        out_status=out_status_val,
                    )
                    
                    i_idx = 0
                    while f"bom-input-{o}-{i_idx}-material_name" in request.POST:
                        material_name = request.POST.get(f"bom-input-{o}-{i_idx}-material_name", "")
                        if material_name:
                            DailyCheckBOMInput.objects.create(output_row=output_row, material_category=request.POST.get(f"bom-input-{o}-{i_idx}-category",""), material_name=material_name, actual_qty=request.POST.get(f"bom-input-{o}-{i_idx}-actual_qty") or None, quantity=request.POST.get(f"bom-input-{o}-{i_idx}-qty") or None, litre=request.POST.get(f"bom-input-{o}-{i_idx}-litre") or None, density=request.POST.get(f"bom-input-{o}-{i_idx}-density") or None, ratio=request.POST.get(f"bom-input-{o}-{i_idx}-ratio") or None)
                        i_idx += 1

                    out_idx = 0
                    while f"bom-output-{o}-{out_idx}-material_name" in request.POST:
                        material_name = request.POST.get(f"bom-output-{o}-{out_idx}-material_name", "")
                        if material_name:
                            DailyCheckBOMOutput.objects.create(output_row=output_row, material_category=request.POST.get(f"bom-output-{o}-{out_idx}-category",""), material_name=material_name, actual_qty=request.POST.get(f"bom-output-{o}-{out_idx}-actual_qty") or None, quantity=request.POST.get(f"bom-output-{o}-{out_idx}-qty") or None, litre=request.POST.get(f"bom-output-{o}-{out_idx}-litre") or None, density=request.POST.get(f"bom-output-{o}-{out_idx}-density") or None, ratio=request.POST.get(f"bom-output-{o}-{out_idx}-ratio") or None)
                        out_idx += 1
                    
                    w_idx = 0
                    while f"bom-waste-{o}-{w_idx}-name" in request.POST:
                        waste_name = request.POST.get(f"bom-waste-{o}-{w_idx}-name", "")
                        if waste_name:
                            DailyCheckBOMWaste.objects.create(output_row=output_row, waste_type=request.POST.get(f"bom-waste-{o}-{w_idx}-type",""), waste_name=waste_name, actual_qty=request.POST.get(f"bom-waste-{o}-{w_idx}-actual_qty") or None, quantity=request.POST.get(f"bom-waste-{o}-{w_idx}-qty") or None, litre=request.POST.get(f"bom-waste-{o}-{w_idx}-litre") or None, density=request.POST.get(f"bom-waste-{o}-{w_idx}-density") or None, ratio=request.POST.get(f"bom-waste-{o}-{w_idx}-ratio") or None)
                        w_idx += 1

                    eq_idx = 0
                    while f"bom-equipment-{o}-{eq_idx}-id" in request.POST:
                        eq_id = request.POST.get(f"bom-equipment-{o}-{eq_idx}-id", "")
                        if eq_id:
                            start_str=request.POST.get(f"bom-equipment-{o}-{eq_idx}-starttime"); end_str=request.POST.get(f"bom-equipment-{o}-{eq_idx}-endtime"); start_dt=datetime.fromisoformat(start_str) if start_str else None; end_dt=datetime.fromisoformat(end_str) if end_str else None; actual_bct=None
                            if start_dt and end_dt and end_dt > start_dt:
                                duration = end_dt - start_dt; actual_bct=Decimal(duration.total_seconds()/3600).quantize(Decimal("0.01"))
                            DailyCheckBOMEquipment.objects.create(output_row=output_row, equipment_type=request.POST.get(f"bom-equipment-{o}-{eq_idx}-type",""), moc=request.POST.get(f"bom-equipment-{o}-{eq_idx}-moc",""), capacity=request.POST.get(f"bom-equipment-{o}-{eq_idx}-capacity",""), equipment_id=eq_id, std_bct=request.POST.get(f"bom-equipment-{o}-{eq_idx}-bct") or None, wait_time=request.POST.get(f"bom-equipment-{o}-{eq_idx}-waittime") or None, starttime=start_dt, endtime=end_dt, actual_bct=actual_bct)
                        eq_idx += 1
                o += 1
            return redirect("daily_checks:report_detail", pk=header.pk)
        else:
            errors_json = json.loads(header_form.errors.as_json())
            print("DEBUG: Header Form Errors:", json.dumps(errors_json, indent=2))
    
    # --- GET (DISPLAYING) LOGIC ---
    next_txn_display = instance.transaction_number if instance else _alloc_next_txn()
    header_form = HeaderForm(instance=instance, initial={"transaction_number": next_txn_display})

    output_rows = []
    if instance:
        all_asset_rows = AssetsInventory.objects.filter(daily_check=instance).prefetch_related(
            'bom_inputs', 'bom_outputs', 'bom_waste', 'bom_equipment' 
        ).order_by('pk')

        for row in all_asset_rows:
            if row.row_flag == "OUTPUT":
                output_rows.append({
                    "stage": row.out_stage, "product": row.out_item_id,
                    "bno": row.out_bno, "equip": row.out_equip,
                    "batch_size": row.batch_size,
                    "starttime": row.out_b_starttime, "endtime": row.out_b_endtime,
                    "bct": row.out_bct, "qty": row.out_qty, "remarks": row.out_remarks,
                    "inputs": list(row.bom_inputs.all().values()),
                    "outputs": list(row.bom_outputs.all().values()),
                    "waste": list(row.bom_waste.all().values()),
                    "equipment": list(row.bom_equipment.all().values()),
                })
    
    bom_qs = ERPBOMDetail.objects.values("stage_name", "fg_name")
    all_bom_stages_qs = ERPBOMDetail.objects.values_list("stage_name", flat=True).distinct()
    sfg_fg_item_types = ['Finished Good', 'Semi Finished Good', 'WIP FR','Work in Progress']
    sfg_fg_relevant_stages = BmrIssue.objects.filter(item_type__in=sfg_fg_item_types).values_list('item_name', flat=True).distinct()
    output_stage_list = sorted(list(all_bom_stages_qs.filter(stage_name__in=sfg_fg_relevant_stages)))
    stage_list = sorted(list(all_bom_stages_qs))
    BLOCK_CHOICES = Block.objects.values_list('display_name', 'display_name')
    block_items = BlockItemMaster.objects.values('item_type', 'product_name')
    all_products = [{'item_type': bi['item_type'] or '', 'item_name': bi['product_name'] or ''} for bi in block_items]

    # Custom JSON converter for complex data types
    def json_converter(o):
        if isinstance(o, (datetime,)):
            return o.isoformat()
        if isinstance(o, Decimal):
            return str(o)

    context = {
        "dr_form": header_form,
        "next_transaction": next_txn_display,
        "now": timezone.now(),
        "BLOCK_CHOICES": BLOCK_CHOICES,
        "output_rows": output_rows,
        "output_rows_json": json.dumps(output_rows, default=json_converter), # *** THE CRUCIAL FIX ***
        "output_stage_list": output_stage_list,
        "stage_list": stage_list,
        "bom_mapping": list(bom_qs),
        "all_products": all_products,
        "all_equipment_json": json.dumps(list(BOMEquipment.objects.values('equipment_type', 'moc_equipment', 'capacity_size', 'equipment_ref', 'std_bct', 'wait_time').distinct())),
    }

    return render(request, "daily_block/entry_form.html", context)



@login_required
def daily_check_edit(request, pk):
    """Create or edit a Daily-Check, handling asset/inventory and BOM child rows."""
    instance = get_object_or_404(DailyCheckHeader, pk=pk) if pk else None

    # --- POST (SAVING) LOGIC ---
    if request.method == "POST":
        header_form = HeaderForm(request.POST or None, instance=instance)
        if "transaction_number" in header_form.fields:
            del header_form.fields["transaction_number"]
            
        if header_form.is_valid():
            header = header_form.save(commit=False)
            header.prepared_by = request.user
            if not instance:
                header.transaction_number = _alloc_next_txn()
            header.save()

            AssetsInventory.objects.filter(daily_check=header).delete()

            o = 0
            while any(k.startswith(f"output-{o}-") for k in request.POST):
                if request.POST.get(f"output-{o}-product") or request.POST.get(f"output-{o}-qty"):
                    out_start_str = request.POST.get(f"output-{o}-starttime")
                    out_end_str = request.POST.get(f"output-{o}-endtime")
                    out_start_dt = datetime.fromisoformat(out_start_str) if out_start_str else None
                    out_end_dt = datetime.fromisoformat(out_end_str) if out_end_str else None
                    out_bct = None
                    if out_start_dt and out_end_dt and out_end_dt > out_start_dt:
                        duration = out_end_dt - out_start_dt
                        out_bct = Decimal(duration.total_seconds() / 3600).quantize(Decimal("0.01"))
                    out_status_val = "Completed" if out_end_dt else "Online"

                    output_row = AssetsInventory.objects.create(
                        daily_check=header, row_flag="OUTPUT",
                        batch_size=request.POST.get(f"output-{o}-batch_size") or None,
                        out_stage=request.POST.get(f"output-{o}-stage", ""),
                        out_item_id=request.POST.get(f"output-{o}-product", ""),
                        out_bno=request.POST.get(f"output-{o}-bno", ""),
                        out_equip=request.POST.get(f"output-{o}-equip", ""),
                        out_b_starttime=out_start_dt,
                        out_b_endtime=out_end_dt,
                        out_bct=out_bct,
                        out_qty=request.POST.get(f"output-{o}-qty") or None,
                        out_remarks=request.POST.get(f"output-{o}-remarks", ""),
                        out_status=out_status_val,
                    )
                    
                    i_idx = 0
                    while f"bom-input-{o}-{i_idx}-material_name" in request.POST:
                        material_name = request.POST.get(f"bom-input-{o}-{i_idx}-material_name", "")
                        if material_name:
                            DailyCheckBOMInput.objects.create(output_row=output_row, material_category=request.POST.get(f"bom-input-{o}-{i_idx}-category",""), material_name=material_name, actual_qty=request.POST.get(f"bom-input-{o}-{i_idx}-actual_qty") or None, quantity=request.POST.get(f"bom-input-{o}-{i_idx}-qty") or None, litre=request.POST.get(f"bom-input-{o}-{i_idx}-litre") or None, density=request.POST.get(f"bom-input-{o}-{i_idx}-density") or None, ratio=request.POST.get(f"bom-input-{o}-{i_idx}-ratio") or None)
                        i_idx += 1

                    out_idx = 0
                    while f"bom-output-{o}-{out_idx}-material_name" in request.POST:
                        material_name = request.POST.get(f"bom-output-{o}-{out_idx}-material_name", "")
                        if material_name:
                            DailyCheckBOMOutput.objects.create(output_row=output_row, material_category=request.POST.get(f"bom-output-{o}-{out_idx}-category",""), material_name=material_name, actual_qty=request.POST.get(f"bom-output-{o}-{out_idx}-actual_qty") or None, quantity=request.POST.get(f"bom-output-{o}-{out_idx}-qty") or None, litre=request.POST.get(f"bom-output-{o}-{out_idx}-litre") or None, density=request.POST.get(f"bom-output-{o}-{out_idx}-density") or None, ratio=request.POST.get(f"bom-output-{o}-{out_idx}-ratio") or None)
                        out_idx += 1
                    
                    w_idx = 0
                    while f"bom-waste-{o}-{w_idx}-name" in request.POST:
                        waste_name = request.POST.get(f"bom-waste-{o}-{w_idx}-name", "")
                        if waste_name:
                            DailyCheckBOMWaste.objects.create(output_row=output_row, waste_type=request.POST.get(f"bom-waste-{o}-{w_idx}-type",""), waste_name=waste_name, actual_qty=request.POST.get(f"bom-waste-{o}-{w_idx}-actual_qty") or None, quantity=request.POST.get(f"bom-waste-{o}-{w_idx}-qty") or None, litre=request.POST.get(f"bom-waste-{o}-{w_idx}-litre") or None, density=request.POST.get(f"bom-waste-{o}-{w_idx}-density") or None, ratio=request.POST.get(f"bom-waste-{o}-{w_idx}-ratio") or None)
                        w_idx += 1

                    eq_idx = 0
                    while f"bom-equipment-{o}-{eq_idx}-id" in request.POST:
                        eq_id = request.POST.get(f"bom-equipment-{o}-{eq_idx}-id", "")
                        if eq_id:
                            start_str=request.POST.get(f"bom-equipment-{o}-{eq_idx}-starttime"); end_str=request.POST.get(f"bom-equipment-{o}-{eq_idx}-endtime"); start_dt=datetime.fromisoformat(start_str) if start_str else None; end_dt=datetime.fromisoformat(end_str) if end_str else None; actual_bct=None
                            if start_dt and end_dt and end_dt > start_dt:
                                duration = end_dt - start_dt; actual_bct=Decimal(duration.total_seconds()/3600).quantize(Decimal("0.01"))
                            DailyCheckBOMEquipment.objects.create(output_row=output_row, equipment_type=request.POST.get(f"bom-equipment-{o}-{eq_idx}-type",""), moc=request.POST.get(f"bom-equipment-{o}-{eq_idx}-moc",""), capacity=request.POST.get(f"bom-equipment-{o}-{eq_idx}-capacity",""), equipment_id=eq_id, std_bct=request.POST.get(f"bom-equipment-{o}-{eq_idx}-bct") or None, wait_time=request.POST.get(f"bom-equipment-{o}-{eq_idx}-waittime") or None, starttime=start_dt, endtime=end_dt, actual_bct=actual_bct)
                        eq_idx += 1
                o += 1
            return redirect("daily_checks:report_detail", pk=header.pk)
        else:
            errors_json = json.loads(header_form.errors.as_json())
            print("DEBUG: Header Form Errors:", json.dumps(errors_json, indent=2))
    
    # --- GET (DISPLAYING) LOGIC ---
    next_txn_display = instance.transaction_number if instance else _alloc_next_txn()
    header_form = HeaderForm(instance=instance, initial={"transaction_number": next_txn_display})

    output_rows = []
    if instance:
        all_asset_rows = AssetsInventory.objects.filter(daily_check=instance).prefetch_related(
            'bom_inputs', 'bom_outputs', 'bom_waste', 'bom_equipment' 
        ).order_by('pk')

        for row in all_asset_rows:
            if row.row_flag == "OUTPUT":
                output_rows.append({
                    "stage": row.out_stage, "product": row.out_item_id,
                    "bno": row.out_bno, "equip": row.out_equip,
                    "batch_size": row.batch_size,
                    "starttime": row.out_b_starttime, "endtime": row.out_b_endtime,
                    "bct": row.out_bct, "qty": row.out_qty, "remarks": row.out_remarks,
                    "inputs": list(row.bom_inputs.all().values()),
                    "outputs": list(row.bom_outputs.all().values()),
                    "waste": list(row.bom_waste.all().values()),
                    "equipment": list(row.bom_equipment.all().values()),
                })
    
    bom_qs = ERPBOMDetail.objects.values("stage_name", "fg_name")
    all_bom_stages_qs = ERPBOMDetail.objects.values_list("stage_name", flat=True).distinct()
    sfg_fg_item_types = ['Finished Good', 'Semi Finished Good', 'WIP FR','Work in Progress']
    sfg_fg_relevant_stages = BmrIssue.objects.filter(item_type__in=sfg_fg_item_types).values_list('item_name', flat=True).distinct()
    output_stage_list = sorted(list(all_bom_stages_qs.filter(stage_name__in=sfg_fg_relevant_stages)))
    stage_list = sorted(list(all_bom_stages_qs))
    BLOCK_CHOICES = Block.objects.values_list('display_name', 'display_name')
    block_items = BlockItemMaster.objects.values('item_type', 'product_name')
    all_products = [{'item_type': bi['item_type'] or '', 'item_name': bi['product_name'] or ''} for bi in block_items]
    # Custom JSON converter for complex data types
    def json_converter(o):
        if isinstance(o, (datetime,)):
            return o.isoformat()
        if isinstance(o, Decimal):
            return str(o)
    context = {
        "dr_form": header_form,
        "next_transaction": next_txn_display,
        "now": timezone.now(),
        "BLOCK_CHOICES": BLOCK_CHOICES,
        "output_rows": output_rows,
        "output_rows_json": json.dumps(output_rows, default=json_converter), # *** THE CRUCIAL FIX ***
        "output_stage_list": output_stage_list,
        "stage_list": stage_list,
        "bom_mapping": list(bom_qs),
        "all_products": all_products,
        "all_equipment_json": json.dumps(list(BOMEquipment.objects.values('equipment_type', 'moc_equipment', 'capacity_size', 'equipment_ref', 'std_bct', 'wait_time').distinct())),
    }
    return render(request, "daily_block/edit_form.html", context)





@login_required
def daily_block_list(request):
    user_name = request.user.username if request.user.is_authenticated else "anonymous"
    active_tab = request.GET.get('tab', 'online')

    logger.info(
        "[DailyBlock][LIST][OPEN] user=%s tab=%s raw_params=%s",
        user_name, active_tab, dict(request.GET)
    )

    # Base queryset depending on tab
    if active_tab == 'online':
        reports = DailyCheckHeader.objects.filter(
            asset_rows__row_flag='OUTPUT',
            asset_rows__out_status='Online'
        ).distinct().order_by("-updated_at", "-pk")
    else:  # completed
        reports = DailyCheckHeader.objects.exclude(
            asset_rows__row_flag='OUTPUT',
            asset_rows__out_status='Online'
        ).order_by("-updated_at", "-pk")

    # Annotate first OUTPUT row values (per report)
    first_output = AssetsInventory.objects.filter(
        daily_check=OuterRef('pk'),
        row_flag='OUTPUT'
    ).order_by('id')

    reports = reports.annotate(
        out_stage_anno=Subquery(first_output.values('out_stage')[:1]),
        out_item_id_anno=Subquery(first_output.values('out_item_id')[:1]),
        out_bno_anno=Subquery(first_output.values('out_bno')[:1]),
    )

    # Filters
    block_filter = request.GET.get('block', '')
    from_date_filter = request.GET.get('from_date', '')
    to_date_filter = request.GET.get('to_date', '')

    if block_filter:
        reports = reports.filter(block__display_name=block_filter)

    if from_date_filter:
        from_date = parse_date(from_date_filter)
        if from_date:
            reports = reports.filter(report_dt__date__gte=from_date)

    if to_date_filter:
        to_date = parse_date(to_date_filter)
        if to_date:
            reports = reports.filter(report_dt__date__lte=to_date)

    # ? Pagination ONLY for completed tab
    page_obj = None
    if active_tab == 'completed':
        paginator = Paginator(reports, 10)  # 10 per page
        page_number = request.GET.get('page')
        page_obj = paginator.get_page(page_number)
        reports = page_obj

    all_blocks = Block.objects.all().order_by('display_name')
    draft_pending = "draft_entry" in request.session

    context = {
        "reports": reports,
        "page_obj": page_obj,
        "active_tab": active_tab,
        "all_blocks": all_blocks,
        "filter_block": block_filter,
        "filter_from_date": from_date_filter,
        "filter_to_date": to_date_filter,
        "draft_pending": draft_pending,
        "now": timezone.now(),
    }

    return render(request, "daily_block/daily_list.html", context)




@login_required
def daily_report_detail(request, pk):
    report = get_object_or_404(DailyCheckHeader, pk=pk)
    # ----- PERMISSION CHECK (simple style like your example) -----
    if not request.user.has_perm('daily_block.view_dailycheckheader'):
        messages.error(request, "You do not have permission to view Daily Block report details.")
        logger.warning( f"User '{request.user.username}' tried to view Daily Check Report (ID={pk}) without permission.")
        return redirect('indexpage')
    # -------------------------------------------------------------
    logger.info(f"User '{request.user.username}' accessed Daily Check Report Detail (ID={pk})." )
    # Pre-fetch related rows
    output_rows = report.asset_rows.filter(row_flag="OUTPUT").prefetch_related(
        'bom_inputs', 'bom_outputs', 'bom_waste', 'bom_equipment')
    return render(request, "daily_block/report_detail.html", {
        "report": report, "output_rows": output_rows, })
    

@login_required
def delete_daily_check(request, pk):
    # ---- PERMISSION CHECK (Your Required Format) ----
    if not request.user.has_perm('daily_block.delete_dailycheckheader'):
        messages.error(request, "You do not have permission to delete Daily Check records.")
        logger.warning(
            f"User '{request.user.username}' tried to delete Daily Check Report (ID={pk}) without permission." )
        return redirect('daily_checks:daily_block_list')
    # -------------------------------------------------
    report = get_object_or_404(DailyCheckHeader, pk=pk)
    try:
        report_identifier = report.transaction_number or f"report from {report.report_dt:%d-%m-%Y}"
        report.delete()
        logger.info(f"Daily Check Report (ID={pk}, Ref='{report_identifier}') deleted successfully by user '{request.user.username}'." )
        messages.success(request, f"Successfully deleted report '{report_identifier}'.")
    except Exception as e:
        logger.error(
            f"Error deleting Daily Check Report (ID={pk}) by user '{request.user.username}': {e}"
        )
        messages.error(request, "An unexpected error occurred while trying to delete the report.")
    return redirect('daily_checks:daily_block_list')

    
    


# ----------------------- Production Dashboard --------------------------------

@login_required
def daily_dashboard_page(request):
    logger.info("User=%s accessed Daily Block Dashboard", request.user.username)
    today = _date.today()
    first_of_month = today.replace(day=1)
    ctx = {
        "default_from": first_of_month.isoformat(),
        "default_to": today.isoformat(),
    }
    return render(request, "daily_block/daily_block_dashboard.html", ctx)


@login_required
def production_dashboard_data(request):
    dfrom = _parse_date(request.GET.get("from"))
    dto = _parse_date(request.GET.get("to"))
    q = (request.GET.get("q") or "").strip()
    # Interactive filters
    f_item = (request.GET.get("item") or "").strip()
    f_stage = (request.GET.get("stage") or "").strip()
    f_block = (request.GET.get("block") or "").strip()
    f_status = (request.GET.get("status") or "").strip()
    # --- Queryset Building ---
    initial_qs = AssetsInventory.objects.filter(
        row_flag="OUTPUT"
    ).select_related("daily_check__block", "daily_check__prepared_by")

    if dfrom and dto:
        initial_qs = initial_qs.filter(out_b_starttime__date__range=[dfrom, dto])

    if q:
        initial_qs = initial_qs.filter(
            Q(out_item_id__icontains=q)
            | Q(out_stage__icontains=q)
            | Q(out_bno__icontains=q)
            | Q(out_equip__icontains=q)
            | Q(daily_check__block__display_name__icontains=q)
        ).distinct()
    qs_for_secondary_cards = initial_qs
    if f_item:
        qs_for_secondary_cards = qs_for_secondary_cards.filter(out_item_id=f_item)

    qs_for_status_counts = qs_for_secondary_cards
    if f_stage:
        qs_for_status_counts = qs_for_status_counts.filter(out_stage=f_stage)
    if f_block:
        qs_for_status_counts = qs_for_status_counts.filter(
            daily_check__block__display_name=f_block
        )
    final_qs = qs_for_status_counts
    if f_status == "Completed":
        final_qs = final_qs.filter(
            out_b_starttime__isnull=False, out_b_endtime__isnull=False
        )
    elif f_status == "Online":
        final_qs = final_qs.filter(
            Q(out_b_starttime__isnull=True) | Q(out_b_endtime__isnull=True)
        )
    # --- Data Aggregation ---
    status_counts = qs_for_status_counts.aggregate(
        completed_count=Count(
            Case(
                When(
                    out_b_starttime__isnull=False,
                    out_b_endtime__isnull=False,
                    then=1,
                )
            )
        ),
        online_count=Count(
            Case(
                When(
                    Q(out_b_starttime__isnull=True)
                    | Q(out_b_endtime__isnull=True),
                    then=1,
                )
            )
        ),
    )
    by_status = {
        "completed": status_counts.get("completed_count", 0),
        "online": status_counts.get("online_count", 0),
    }
    totals = final_qs.aggregate(
        total_batches=Count("out_bno", distinct=True),
        total_qty=Sum("out_qty"),
        avg_bct=Avg("out_bct"),
    )
    totals["total_qty"] = totals.get("total_qty") or 0
    totals["avg_bct"] = totals.get("avg_bct") or 0
    by_item = [
        {"item": r["out_item_id"] or "(blank)"}
        for r in initial_qs.values("out_item_id")
        .annotate(count=Count("id"))
        .order_by("-count", "out_item_id")
    ]
    by_stage = [
        {"stage": r["out_stage"] or "(blank)"}
        for r in qs_for_secondary_cards.values("out_stage")
        .annotate(count=Count("id"))
        .order_by("-count", "out_stage")
    ]
    by_block = [
        {"block": r["block_name"] or "(blank)"}
        for r in qs_for_secondary_cards.values(
            block_name=F("daily_check__block__display_name")
        )
        .annotate(count=Count("id"))
        .order_by("-count", "block_name")
    ]
    # --- Equipment-wise as before ---
    equipment_qs = DailyCheckBOMEquipment.objects.filter(
        output_row__in=final_qs.values_list("id", flat=True)
    )
    by_equipment = [
        {
            "equipment_id": r["equipment_id"] or "(blank)",
            "std_bct": r["avg_std_bct"] or 0,
            "actual_bct": r["avg_actual_bct"] or 0,
        }
        for r in equipment_qs.values("equipment_id")
        .annotate(avg_std_bct=Avg("std_bct"), avg_actual_bct=Avg("actual_bct"))
        .order_by("equipment_id")
    ]
    # === NEW: BOM input / output / waste summaries ===
    output_ids = list(final_qs.values_list("id", flat=True))

    input_qs = DailyCheckBOMInput.objects.filter(output_row_id__in=output_ids)
    by_input = [
        {
            "material_name": r["material_name"] or "(blank)",
            "quantity": r["qty_sum"] or 0,
            "actual_qty": r["actual_sum"] or 0,
        }
        for r in input_qs.values("material_name")
        .annotate(
            qty_sum=Sum("quantity"),
            actual_sum=Sum("actual_qty"),
        )
        .order_by("material_name")
    ]
    output_qs = DailyCheckBOMOutput.objects.filter(output_row_id__in=output_ids)
    by_output = [
        {
            "material_name": r["material_name"] or "(blank)",
            "quantity": r["qty_sum"] or 0,
            "actual_qty": r["actual_sum"] or 0,
        }
        for r in output_qs.values("material_name")
        .annotate(
            qty_sum=Sum("quantity"),
            actual_sum=Sum("actual_qty"),
        )
        .order_by("material_name")
    ]
    waste_qs = DailyCheckBOMWaste.objects.filter(output_row_id__in=output_ids)
    by_waste = [
        {
            "waste_name": r["waste_name"] or "(blank)",
            "quantity": r["qty_sum"] or 0,
            "actual_qty": r["actual_sum"] or 0,
        }
        for r in waste_qs.values("waste_name")
        .annotate(
            qty_sum=Sum("quantity"),
            actual_sum=Sum("actual_qty"),
        )
        .order_by("waste_name")
    ]
    # UPDATED: by_batch query to include updated_at
    by_batch_raw = (
        final_qs.order_by("-daily_check__updated_at", "-id")
        .values(
            "out_item_id",
            "out_stage",
            "out_bno",
            block_name=F("daily_check__block__display_name"),
            prepared_by_name=F("daily_check__prepared_by__username"),
            updated_at=F("daily_check__updated_at"),
        )[:100]
    )
    by_batch = [
        {
            "item": r["out_item_id"] or "",
            "stage": r["out_stage"] or "",
            "bno": r["out_bno"] or "",
            "block": r["block_name"] or "",
            "prepared_by": r["prepared_by_name"] or "",
            "updated_at": r["updated_at"].isoformat() if r["updated_at"] else "",
        }
        for r in by_batch_raw
    ]
    return JsonResponse(
        {
            "totals": totals,
            "by_item": by_item,
            "by_stage": by_stage,
            "by_block": by_block,
            "by_status": by_status,
            "by_batch": by_batch,
            "by_equipment": by_equipment,
            "by_input": by_input,    # NEW
            "by_output": by_output,  # NEW
            "by_waste": by_waste,    # NEW
        },
        safe=False,
    )


@login_required
def export_production_data_to_excel(request):
    logger.info("User=%s Download Daily Block Records", request.user.username)

    dfrom = _parse_date(request.GET.get("from"))
    dto   = _parse_date(request.GET.get("to"))
    q     = (request.GET.get("q") or "").strip()
    f_item   = (request.GET.get("item") or "").strip()
    f_stage  = (request.GET.get("stage") or "").strip()
    f_block  = (request.GET.get("block") or "").strip()
    f_status = (request.GET.get("status") or "").strip()

    # ---- Queryset (same logic as before) ----
    qs = AssetsInventory.objects.filter(
        row_flag="OUTPUT"
    ).select_related("daily_check__block", "daily_check__prepared_by")

    if dfrom and dto:
        qs = qs.filter(out_b_starttime__date__range=[dfrom, dto])
    if q:
        qs = qs.filter(
            Q(out_item_id__icontains=q)
            | Q(out_stage__icontains=q)
            | Q(out_bno__icontains=q)
            | Q(out_equip__icontains=q)
            | Q(daily_check__block__display_name__icontains=q)
        ).distinct()
    if f_item:
        qs = qs.filter(out_item_id=f_item)
    if f_stage:
        qs = qs.filter(out_stage=f_stage)
    if f_block:
        qs = qs.filter(daily_check__block__display_name=f_block)
    if f_status == "Completed":
        qs = qs.filter(out_b_starttime__isnull=False, out_b_endtime__isnull=False)
    elif f_status == "Online":
        qs = qs.filter(
            Q(out_b_starttime__isnull=True) | Q(out_b_endtime__isnull=True)  )

    data_rows = qs.order_by("daily_check__report_dt").values_list("out_item_id", "out_stage",F("daily_check__block__display_name"),
        "out_bno", "out_equip", "out_b_starttime", "out_b_endtime", "out_qty", "out_bct","batch_size",
        F("daily_check__prepared_by__username"), F("daily_check__created_at"), F("daily_check__updated_at"), )

    # ---- Prepare Excel in memory ----
    output = BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    worksheet = workbook.add_worksheet("Daily Block")

    # Formats
    title_fmt = workbook.add_format({"bold": True, "font_size": 16, "align": "center", "valign": "vcenter" })
    header_fmt = workbook.add_format({ "bold": True,"bg_color": "#D9E1F2","border": 1  })
    datetime_fmt = workbook.add_format({"num_format": "yyyy-mm-dd hh:mm:ss" })
    text_fmt = workbook.add_format()
    number_fmt = workbook.add_format()
    # ---- Title row ----
    header = ["Product Name","Stage Name", "Block","Batch No", "Equipment ID","Start Time","End Time",
        "Output Quantity","BCT", "Batch Size", "Created By",  "created_at", "Updated At", ]
    # Merge across all header columns for title
    worksheet.merge_range(0, 0, 0, len(header) - 1,"Production Daily Block Report", title_fmt)
    # Blank row after title
    # Header row at row index 2
    for col, col_name in enumerate(header):
        worksheet.write(2, col, col_name, header_fmt)
    # Optional: set column widths a bit nicer
    worksheet.set_column(0, 0, 18)   # out_item_id
    worksheet.set_column(1, 2, 20)   # stage, block
    worksheet.set_column(3, 4, 18)   # bno, equip
    worksheet.set_column(7, 7, 30)    # Output Quantity (wider so full text shows)
    worksheet.set_column(5, 7, 22)   # start, end, qty
    worksheet.set_column(8, 12, 20)  # rest
    # Helper to convert aware -> naive local datetime
    def to_naive_local(dt):
        if not dt:
            return None
        local = timezone.localtime(dt)   # settings.TIME_ZONE
        return local.replace(tzinfo=None)
    # indices of datetime columns in the header
    dt_indices = {5, 6, 11, 12}
    row_idx = 3  # data starts after title + blank + header
    for row in data_rows:
        for col_idx, value in enumerate(row):

            # Datetime columns
            if col_idx in dt_indices:
                dt_val = to_naive_local(value)
                if dt_val:
                    worksheet.write_datetime(row_idx, col_idx, dt_val, datetime_fmt)
                else:
                    worksheet.write(row_idx, col_idx, "", text_fmt)
            # Numeric values
            elif isinstance(value, (int, float, Decimal)):
                # xlsxwriter can handle Decimal but casting is safe
                worksheet.write_number(row_idx, col_idx, float(value), number_fmt)
            # Everything else as text
            else:
                worksheet.write(row_idx, col_idx, value if value is not None else "", text_fmt)
        row_idx += 1
    workbook.close()
    output.seek(0)
    # ---- HTTP response ----
    response = HttpResponse( output.read(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",)
    response["Content-Disposition"] = (
        'attachment; filename="production_daily_block_report.xlsx"' )
    return response




@login_required
def trigger_erp_sync(request):
    """
    Runs sync_erp_block immediately (NO Celery).
    Returns output as JSON.
    """
    if request.method != "POST":
        return JsonResponse({'error': 'Invalid request method. Please use POST.'}, status=405)
    try:
        f = io.StringIO()
        with redirect_stdout(f):
            call_command('sync_erp_block')

        output = f.getvalue()
        return JsonResponse({'status': 'SUCCESS', 'result': output}, status=200)

    except Exception as e:
        return JsonResponse({'status': 'FAILURE', 'error': str(e)}, status=500)
