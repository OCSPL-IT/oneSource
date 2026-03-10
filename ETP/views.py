from django.shortcuts import render, redirect,get_object_or_404
from django.forms import inlineformset_factory
from django.contrib import messages
from .models import *
from .forms import *
from django.db import connections
from django.http import JsonResponse,HttpResponse
from django.views.decorators.csrf import csrf_exempt
import json
from django.contrib.auth.decorators import login_required, permission_required
from django.views.decorators.http import require_POST
import logging
from django.core.paginator import Paginator
from django.db.models import Sum
from datetime import datetime, timedelta,date,time
from django.db.models.functions import TruncMonth
from django.utils.dateparse import parse_date
from django.utils.timezone import now
from django.db.models import Q
import xlsxwriter
import io
from calendar import monthrange
from collections import defaultdict
import re
from .models import ProductionSchedule  # via your ReadOnlyDBRouter
from urllib.parse import urlencode
from django.db import transaction
from datetime import datetime, time as dtime
from datetime import date, datetime as dt, time as dtime
from .constants import *
from io import BytesIO
from django.urls import reverse
from .storage_tank_reporting import  build_effluent_report_range
from ETP.MEE.models import *
from django.db.models import Sum, Avg, Value, DecimalField
from django.db.models.functions import Coalesce
from django.forms.models import inlineformset_factory, BaseInlineFormSet



logger = logging.getLogger('custom_logger')


#Get FG Name
def get_products(request):
    search_term = request.GET.get('term', '').lower()

    query = """
    SELECT DISTINCT
       ITMCF.sValue AS [FG_Name]
FROM   txnhdr  AS HDR
JOIN   TXNDET  AS DET   ON DET.lId   = HDR.lId
JOIN   ITMMST  AS ITM   ON ITM.lId   = DET.lItmId          -- (kept for completeness)
JOIN   ITMTYP  (NOLOCK) AS ITP  ON ITP.lTypId = DET.lItmTyp
JOIN   ITMCF   AS ITMCF
           ON  ITMCF.lId      = DET.lItmId
          AND  ITMCF.lFieldNo = 10          -- FG name CF-field
          AND  ITMCF.lLine    = 0
WHERE  HDR.lTypId     IN (664,717,718,719,720,721)  -- required Txn types
  AND  HDR.lCompId    = 27
  AND  HDR.bDel       = 0                           -- header not deleted
  AND  DET.bDel      <> -2                          -- detail not deleted
  AND  DET.lClosed   <> -2                          -- detail not closed
  AND  DET.lItmTyp   <> 63                          -- exclude item-type 63
  AND  ITP.sName    NOT IN ('WIP FR','Intercut')    -- exclude certain item-types
  AND  HDR.dtDocDate >= '20250101'                  -- date filter (yyyymmdd)
  AND ( SELECT sValue
         FROM  txncf
        WHERE  lid    = HDR.lid
          AND  sName  = 'Product Name'
          AND  lLine  = 0
      ) <> 'MIX SOLVENT';
    """

    with connections['readonly_db'].cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    # Remove duplicates, sort with match priority
    seen = set()
    results = []
    for row in rows:
        fg_name = row[0]
        if fg_name and fg_name not in seen:
            seen.add(fg_name)
            results.append(fg_name)

    # Sort: matches with search_term earlier in string come first
    if search_term:
        results.sort(key=lambda name: name.lower().find(search_term) if search_term in name.lower() else float('inf'))

    # Return as Select2 format
    return JsonResponse({'results': [{'id': name, 'text': name} for name in results]})


# Get Stage Name -------------------------------------------------------------
def get_stage_names(request):
    """
    Returns DISTINCT Product-/Stage-names (txncf.'Product Name')
    that belong to the FG selected in the first dropdown.
    The extra joins / filters are identical to get_products().
    """
    term     = request.GET.get("term", "").lower()          # what the user typed
    fg_name  = request.GET.get("product_name", "")          # FG selected above

    sql = """
    SELECT  DISTINCT                       -- one row per stage
            (SELECT sValue
               FROM  txncf
              WHERE  lid   = HDR.lid
                AND  sName = 'Product Name'
                AND  lLine = 0)            AS [Stage_Name]
    FROM        txnhdr  AS HDR
    JOIN        txndet  AS DET   ON DET.lId    = HDR.lId
    JOIN        itmmst  AS ITM   ON ITM.lId    = DET.lItmId       -- kept
    JOIN        itmtyp  (NOLOCK) AS ITP   ON ITP.lTypId = DET.lItmTyp
    JOIN        itmcf   AS ITMCF
                   ON  ITMCF.lId      = DET.lItmId
                  AND  ITMCF.lFieldNo = 10   -- FG name custom field
                  AND  ITMCF.lLine    = 0
    WHERE       HDR.lTypId  IN (664,717,718,719,720,721)
      AND       HDR.lCompId = 27
      AND       HDR.bDel    = 0
      AND       DET.bDel   <> -2
      AND       DET.lClosed <> -2
      AND       DET.lItmTyp <> 63
      AND       ITP.sName  NOT IN ('WIP FR','Intercut')
      AND       HDR.dtDocDate >= '20250101'
      AND       ITMCF.sValue = %s                     -- FG filter
      AND      (SELECT sValue
                  FROM  txncf
                 WHERE  lid   = HDR.lid
                   AND  sName = 'Product Name'
                   AND  lLine = 0) <> 'MIX SOLVENT'
    ORDER BY    [Stage_Name];
    """

    with connections["readonly_db"].cursor() as cur:
        cur.execute(sql, [fg_name])
        stages = [row[0] for row in cur.fetchall() if row[0]]   # flatten

    # ------- deduplicate & apply search-term priority -----------------------
    unique   = list(dict.fromkeys(stages))          # preserves order, removes dupes

    if term:
        # move matches (containing the user's term) to the front
        unique.sort(
            key=lambda x: (x.lower().find(term) if term in x.lower() else float("inf"), x)
        )

    return JsonResponse(
        {"results": [{"id": s, "text": s} for s in unique]}
    )



# ---------------------------------------------------------------------------
# Get Batch-Nos ( “O/P Batch No” custom-field )
# ---------------------------------------------------------------------------
@csrf_exempt
def get_batch_nos(request):
    """Return distinct Batch-numbers for the selected FG & Stage."""
    term          = request.GET.get("term", "").lower()       # live search
    fg_name       = request.GET.get("product_name", "")       # FG selected first
    stage_name    = request.GET.get("stage_name", "")         # ’Product Name’ (=Stage)

    sql = """
    SELECT  DISTINCT
            ( SELECT sValue
                FROM  txncf
               WHERE  lid   = HDR.lid
                 AND  sName = 'Batch No'
                 AND  lLine = 0
            )  AS Batch_No
    FROM        txnhdr  AS HDR
    JOIN        txndet  AS DET   ON DET.lId    = HDR.lId
    JOIN        itmmst  AS ITM   ON ITM.lId    = DET.lItmId      -- (kept)
    JOIN        itmtyp  (NOLOCK) AS ITP   ON ITP.lTypId = DET.lItmTyp
    JOIN        itmcf   AS ITMCF
               ON  ITMCF.lId      = DET.lItmId
              AND  ITMCF.lFieldNo = 10      -- FG name CF
              AND  ITMCF.lLine    = 0
    WHERE       HDR.lTypId  IN (664,717,718,719,720,721)
      AND       HDR.lCompId = 27
      AND       HDR.bDel    = 0
      AND       DET.bDel   <> -2
      AND       DET.lClosed <> -2
      AND       DET.lItmTyp <> 63
      AND       ITP.sName  NOT IN ('WIP FR','Intercut')
      AND       HDR.dtDocDate >= '20250101'
      AND       ITMCF.sValue = %s                           -- FG filter
      AND      ( SELECT sValue
                   FROM  txncf
                  WHERE  lid   = HDR.lid
                    AND  sName = 'Product Name'
                    AND  lLine = 0 ) = %s                   -- Stage filter
      AND      ( SELECT sValue
                   FROM  txncf
                  WHERE  lid   = HDR.lid
                    AND  sName = 'Product Name'
                    AND  lLine = 0 ) <> 'MIX SOLVENT'
    ORDER BY    Batch_No;
    """

    with connections["readonly_db"].cursor() as cur:
        cur.execute(sql, [fg_name, stage_name])
        batch_list = [row[0] for row in cur.fetchall() if row[0]]

    # remove duplicates kept by DISTINCT (safety) and apply live-search priority
    seen, results = set(), []
    for b in batch_list:
        if b not in seen:
            seen.add(b)
            results.append(b)

    if term:
        results.sort(
            key=lambda x: (x.lower().find(term) if term in x.lower() else float("inf"), x)
        )

    return JsonResponse(
        {"results": [{"id": b, "text": b} for b in results]}
    )



# ---------------------------------------------------------------------------
# Voucher-number for a given FG  + Stage  + Batch-No
# ---------------------------------------------------------------------------
@csrf_exempt
def get_voucher_details_by_batch(request):
    data        = json.loads(request.body.decode("utf-8") or "{}")
    fg_name     = data.get("product_name", "")
    stage_name  = data.get("stage_name", "")
    batch_no    = data.get("batch_no",   "")

    sql = """
    SELECT TOP 1 HDR.sDocNo                                          -- voucher #
    FROM   txnhdr  AS HDR
    JOIN   txndet  AS DET   ON DET.lId    = HDR.lId
    JOIN   itmtyp  (NOLOCK) ITP ON ITP.lTypId = DET.lItmTyp
    JOIN   itmcf   AS ITMCF
           ON ITMCF.lId      = DET.lItmId
          AND ITMCF.lFieldNo = 10
          AND ITMCF.lLine    = 0
    WHERE  HDR.lTypId    IN (664,717,718,719,720,721)
      AND  HDR.lCompId   = 27
      AND  HDR.bDel      = 0
      AND  DET.bDel     <> -2
      AND  DET.lClosed  <> -2
      AND  DET.lItmTyp  <> 63
      AND  ITP.sName   NOT IN ('WIP FR','Intercut')
      AND  HDR.dtDocDate >= '20250101'
      AND  ITMCF.sValue   = %s                                   -- FG name
      AND (SELECT sValue FROM txncf WHERE lid=HDR.lid AND sName='Product Name' AND lLine=0) = %s
      AND (SELECT sValue FROM txncf WHERE lid=HDR.lid AND sName='Batch No'   AND lLine=0) = %s
      AND (SELECT sValue FROM txncf WHERE lid=HDR.lid AND sName='Product Name' AND lLine=0) <> 'MIX SOLVENT';
    """

    with connections["readonly_db"].cursor() as cur:
        cur.execute(sql, [fg_name, stage_name, batch_no])
        rec = cur.fetchone()

    if rec:
        return JsonResponse({"voucher_no": rec[0]})
    return JsonResponse({"error": "Not found"}, status=404)




@csrf_exempt
@require_POST
def get_effluent_qty_details(request):
    if request.headers.get('Content-Type') == 'application/json':
        data = json.loads(request.body)
    else:
        data = request.POST

    product_name = data.get('product_name')
    stage_name = data.get('stage_name')
    batch_no = data.get('batch_no')

    # print("🔍 Incoming Request:")
    # print(f"   ➤ Product Name: {product_name}")
    # print(f"   ➤ Stage Name:   {stage_name}")
    # print(f"   ➤ Batch No:     {batch_no}")

    if not product_name or not stage_name or not batch_no:
        # print("❌ Missing one or more required parameters.")
        return JsonResponse({
            'error': 'Missing parameters: product_name, stage_name and batch_no are required.'
        }, status=400)

    query = """
        SELECT
            bl.material_category AS category,
            bl.material_name     AS effluent_nature,
            bl.quantity          AS plan_quantity,
            bl.density           AS density
        FROM bom_headers bh
        JOIN bom_lines bl ON bh.bom_id = bl.bom_id
        WHERE bh.fg_name = %s
          AND bh.stage_name = %s
          AND bl.line_type = 'waste'
          AND bl.material_category = 'process'
    """

    try:
        with connections['production_scheduler'].cursor() as cursor:
            cursor.execute(query, [product_name, stage_name])
            rows = cursor.fetchall()
            # print(f"✅ Query executed successfully. Rows fetched: {len(rows)}")
    except Exception as e:
        # print(f"🔥 Error during DB fetch: {str(e)}")
        return JsonResponse({'error': 'Database query failed'}, status=500)

    if not rows:
        # print("⚠️ No records found for effluent quantity.")
        return JsonResponse({'error': 'No matching production schedule lines found.'}, status=404)

    results = []
    for row in rows:
        # print(f"📦 Row: {row}")
        results.append({
            'category': row[0],
            'effluent_nature': row[1],
            'plan_quantity': row[2],
            'density':        row[3],
        })

    return JsonResponse({'data': results})





def add_effluent_record(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    if not request.user.has_perm('ETP.add_effluentrecord'):
        logger.warning(f"Unauthorized add attempt by {request.user.username}")
        messages.error(request, "You do not have permission to add Effluent records.")
        return redirect('indexpage')
    
    EffluentQtyFormSet = inlineformset_factory(
        EffluentRecord, EffluentQty, form=EffluentQtyForm,
        extra=1, can_delete=True )
    
    FORMSET_PREFIX = "effluentqty_set" 

    if request.method == 'POST':
        logger.debug("🔄 Received POST request")
        logger.debug(f"📦 POST Data: {request.POST.dict()}")

        record_form = EffluentRecordForm(request.POST)
        formset = EffluentQtyFormSet(request.POST, prefix=FORMSET_PREFIX)

        if record_form.is_valid():
            logger.debug("✅ Record form is valid")
        else:
            logger.error(f"❌ Record form errors: {record_form.errors.as_json()}")

        if formset.is_valid():
            logger.debug("✅ Formset is valid")
        else:
            for i, f in enumerate(formset.forms):
                if f.errors:
                    logger.error(f"  - Row {i} Errors: {f.errors.as_json()}")

        if record_form.is_valid() and formset.is_valid():
            effluent_record = record_form.save()
            formset.instance = effluent_record
            formset.save()
            logger.info("✅ Effluent record and quantities saved successfully.")
            messages.success(request, "Effluent record added successfully!")
            return redirect('view_effluent_records')
        else:
            logger.warning("⚠️ Form validation failed. Rendering form with errors.")
            messages.error(request, "Please correct the errors below.")
    else:
        record_form = EffluentRecordForm()
        formset = EffluentQtyFormSet(prefix=FORMSET_PREFIX)

    return render(request, 'etp/add_effluent_record.html', {
        'record_form': record_form,
        'formset': formset,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
    })



@login_required
def effluent_records_list(request):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('ETP.view_effluentrecord'):
        logger.warning(f"Unauthorized view attempt by user: {request.user.username}")
        messages.error(request, "You do not have permission to View Effluent records.")
        return redirect('indexpage')

    today = date.today()
    first_day = today.replace(day=1)
    last_day = (first_day + timedelta(days=32)).replace(day=1) - timedelta(days=1)

    # Filters
    product_name = request.GET.get('product_name', '').strip()
    stage_name = request.GET.get('stage_name', '').strip()
    block = request.GET.get('block', '').strip()
    category = request.GET.get('category', '').strip()

    from_str = request.GET.get('from_date', '')
    to_str = request.GET.get('to_date', '')
    from_date = parse_date(from_str) if from_str else first_day
    to_date = parse_date(to_str) if to_str else last_day

    logger.info(f"User {request.user.username} filtering effluent records from {from_date} to {to_date}, "
                f"product_name='{product_name}', stage_name='{stage_name}', block='{block}', category='{category}'")

    # Query
    records = EffluentQty.objects.select_related('effluent_record') \
        .filter(effluent_record__record_date__range=(from_date, to_date)) \
        .order_by('-effluent_record__record_date', '-effluent_record__id')

    if product_name:
        records = records.filter(effluent_record__product_name__icontains=product_name)
    if stage_name:
        records = records.filter(effluent_record__stage_name__icontains=stage_name)
    if block:
        records = records.filter(effluent_record__block=block)
    if category:
        records = records.filter(category=category)

    totals = records.aggregate(
        total_plan_quantity=Sum('plan_quantity'),
        total_actual_quantity=Sum('actual_quantity'),
        total_quantity_kg=Sum('quantity_kg')
    )

    paginator = Paginator(records, 10)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)

    all_blocks = EffluentQty.objects.values_list('effluent_record__block', flat=True).distinct()
    all_categories = EffluentQty.objects.values_list('category', flat=True).distinct()

    filter_params = {
        'from_date': from_str or '',
        'to_date': to_str or '',
        'product_name': product_name,
        'stage_name': stage_name,
        'block': block,
        'category': category,
    }
    filter_query = urlencode({k: v for k, v in filter_params.items() if v})

    logger.debug(f"Returned {page_obj.paginator.count} effluent records, page {page_number or 1}")

    return render(request, 'etp/view_effluent_records.html', {
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'page_obj': page_obj,
        'all_blocks': all_blocks,
        'all_categories': all_categories,
        'totals': totals,
        'filters': {
            'product_name': product_name,
            'stage_name': stage_name,
            'block': block,
            'category': category,
            'from_date': from_date.strftime('%Y-%m-%d'),
            'to_date': to_date.strftime('%Y-%m-%d'),
        },
        'filter_query': filter_query,
    })


class EffluentQtyBaseFormSet(BaseInlineFormSet):
    """
    Custom formset so the auto-added PK field 'id' is NOT required,
    and any 'id' errors are ignored.

    This is safe here because:
    - For existing rows, the hidden id is always present in the HTML.
    - For new rows, leaving id blank simply means "create a new row".
    """

    def add_fields(self, form, index):
        super().add_fields(form, index)
        pk_name = self.model._meta.pk.name  # usually "id"
        if pk_name in form.fields:
            form.fields[pk_name].required = False

    def clean(self):
        # run default validation first
        super().clean()

        # then strip any "id is required" style errors
        pk_name = self.model._meta.pk.name  # usually "id"
        for form in self.forms:
            if pk_name in form.errors:
                # drop ONLY the pk errors; keep others
                form.errors.pop(pk_name, None)


@login_required
def edit_effluent_record(request, pk):
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm('ETP.change_effluentrecord'):
        logger.warning(f"Unauthorized add attempt by {request.user.username}")
        messages.error(request, "You do not have permission to update Effluent records.")
        return redirect('indexpage')

    record = get_object_or_404(EffluentRecord, id=pk)

    EffluentQtyFormSet = inlineformset_factory(
        EffluentRecord,
        EffluentQty,
        form=EffluentQtyForm,
        formset=EffluentQtyBaseFormSet,  # ⬅️ use our custom base
        extra=0,
        can_delete=True,
    )
    FORMSET_PREFIX = "effluentqty_set"

    if request.method == 'POST':
        record_form = EffluentRecordForm(request.POST, instance=record)
        formset = EffluentQtyFormSet(request.POST, instance=record, prefix=FORMSET_PREFIX)

        if record_form.is_valid() and formset.is_valid():
            record_form.save()
            formset.save()
            messages.success(request, "Effluent record updated successfully!")
            return redirect('view_effluent_records')
        else:
            logger.error(
                "Effluent edit failed | header_errors=%s | formset_errors=%s | formset_non_form_errors=%s",
                record_form.errors,
                [f.errors for f in formset.forms],
                formset.non_form_errors(),
            )
            messages.error(request, "Please correct the errors below.")
    else:
        record_form = EffluentRecordForm(instance=record)
        formset = EffluentQtyFormSet(instance=record, prefix=FORMSET_PREFIX)

        # Fetch densities from BOM for each nature
        fg_name = record.product_name
        stage_name = record.stage_name
        query = """
            SELECT material_name, density
            FROM bom_lines bl
            JOIN bom_headers bh ON bl.bom_id = bh.bom_id
            WHERE bh.fg_name = %s AND bh.stage_name = %s AND bl.line_type = 'waste'
        """
        density_map = {}
        with connections['production_scheduler'].cursor() as cursor:
            cursor.execute(query, [fg_name, stage_name])
            for mat_name, density in cursor.fetchall():
                density_map[mat_name] = density or 0

        # Inject density into each form’s initial data
        for form in formset:
            nature = form.initial.get('effluent_nature')
            form.initial['density'] = density_map.get(nature, 0)

    return render(
        request,
        'etp/edit_effluent_record.html',
        {
            'user_groups': user_groups,
            'is_superuser': is_superuser,
            'record_form': record_form,
            'formset': formset,
            'record': record,
            'edit_mode': True,
        },
    )



@login_required
def delete_effluent_qty(request, qty_id):
    if not request.user.has_perm('ETP.delete_effluentrecord'):
        logger.warning(f"Unauthorized add attempt by {request.user.username}")
        messages.error(request, "You do not have permission to delete Effluent records.")
        return redirect('indexpage')
    qty = get_object_or_404(EffluentQty, id=qty_id)

    # Check related record
    eff_record = qty.effluent_record
    sibling_count = EffluentQty.objects.filter(effluent_record=eff_record).count()

    if request.method == 'POST':
        qty.delete()
        msg = "Effluent Record deleted successfully."

        # If it was the only one, delete the parent record
        if sibling_count == 1:
            eff_record.delete()
            msg = "Effluent record deleted successfully."

        messages.success(request, msg)
        return redirect('view_effluent_records')

    messages.error(request, "Invalid request.")
    return redirect('view_effluent_records')



@login_required
def download_effluent_excel(request):
    today = datetime.today().date()
    first_day = today.replace(day=1)
    last_day = (first_day.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

    product_name = request.GET.get('product_name', '').strip()
    stage_name = request.GET.get('stage_name', '').strip()
    block = request.GET.get('block', '').strip()
    category = request.GET.get('category', '').strip()
    from_str = request.GET.get('from_date', '')
    to_str = request.GET.get('to_date', '')

    from_date = parse_date(from_str) if from_str else first_day
    to_date = parse_date(to_str) if to_str else last_day

    qs = EffluentQty.objects.select_related('effluent_record') \
        .filter(effluent_record__record_date__range=(from_date, to_date)) \
        .order_by('-effluent_record__record_date')

    if product_name:
        qs = qs.filter(effluent_record__product_name__icontains=product_name)
    if stage_name:
        qs = qs.filter(effluent_record__stage_name__icontains=stage_name)
    if block:
        qs = qs.filter(effluent_record__block=block)
    if category:
        qs = qs.filter(category=category)

    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {'in_memory': True})
    ws = wb.add_worksheet("Effluent Records")

    header_fmt = wb.add_format({'bold': True, 'bg_color': '#D9E1F2', 'border': 1, 'align': 'center'})
    date_fmt = wb.add_format({'num_format': 'dd/mm/yyyy', 'align': 'center'})
    text_fmt = wb.add_format({'align': 'left'})
    num_fmt = wb.add_format({'num_format': '#,##0.00', 'align': 'right'})

    headers = [
        "Sr. No", "Record Date", "Product Name", "Stage Name", "Category",
        "Block", "Effluent Nature", "Plan Quantity (Kg)", "Actual Quantity (Kl)", "Quantity (Kg)"
    ]
    for col, header in enumerate(headers):
        ws.write(0, col, header, header_fmt)

    ws.set_column('A:A', 8)
    ws.set_column('B:B', 12)
    ws.set_column('C:C', 25)
    ws.set_column('D:D', 25)
    ws.set_column('E:E', 12)
    ws.set_column('F:F', 12)
    ws.set_column('G:G', 25)
    ws.set_column('H:J', 18)

    row = 1
    for idx, rec in enumerate(qs, start=1):
        eff = rec.effluent_record
        ws.write_number(row, 0, idx)
        ws.write_datetime(row, 1, datetime.combine(eff.record_date, time()), date_fmt)
        ws.write_string(row, 2, eff.product_name or '', text_fmt)
        ws.write_string(row, 3, eff.stage_name or '', text_fmt)
        ws.write_string(row, 4, rec.category or '', text_fmt)
        ws.write_string(row, 5, eff.block or '', text_fmt)
        ws.write_string(row, 6, rec.effluent_nature or '', text_fmt)
        ws.write_number(row, 7, rec.plan_quantity or 0.0, num_fmt)
        ws.write_number(row, 8, rec.actual_quantity or 0.0, num_fmt)
        ws.write_number(row, 9, rec.quantity_kg or 0.0, num_fmt)
        row += 1

    wb.close()
    output.seek(0)

    response = HttpResponse(
        output.read(),
        content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
    response['Content-Disposition'] = 'attachment; filename="effluent_records.xlsx"'
    return response


def parse_batch_count(s):
    try:
        inside = s.split('(')[1].split(')')[0]  # "34 / 5,273"
        batch_count = inside.split('/')[0].strip()
        return int(batch_count.replace(',', ''))
    except Exception:
        return 0


def generate_batch_rows(schedule):
    """
    Produce one dict per batch with:
      - batch_no, generated_batch_number
      - batch_start, batch_end
      - output_quantity
      - equipment_runs: list of {
            equipment_id, std_bct, wait_time, star, start, end, status
        }
      - materials, outputs, wastes: full lists of schedule lines

    Runs whose start >= their closed_date or the schedule’s closed_date
    are marked "Cancelled" (but still emitted in the CSV).
    """
    from datetime import datetime as _dt, timedelta as _td

    # ─── 0) Determine plan-level cutoff if any ─────────────────────────
    plan_cutoff = getattr(schedule, "closed_date", None)
    if plan_cutoff and not isinstance(plan_cutoff, _dt):
        plan_cutoff = _dt.combine(plan_cutoff, _dt.min.time())

    # ─── 1) Basic header values ───────────────────────────────────────
    n_batches = int(schedule.no_of_batches or 0)
    approach  = int(schedule.scheduling_approach or 0)
    start_ts  = schedule.start_date
    if not isinstance(start_ts, _dt):
        start_ts = _dt.combine(start_ts, _dt.min.time())

    # ─── 2) Per-batch output ──────────────────────────────────────────
    lines = schedule.lines.all()
    out_ln = next((l for l in lines if l.line_type == "output"), None)
    per_batch = round(float(out_ln.quantity or 0)) if out_ln else 0

    # ─── 3) Pull materials/outputs/wastes once ────────────────────────
    mats = [
        dict(
            line_type="input",
            material_category=l.material_category,
            material_name=l.material_name,
            quantity=l.quantity,
            ratio=l.ratio,
            density=l.density,
            litre=l.litre,
            include_in_total=l.include_in_total
        )
        for l in lines if l.line_type == "input"
    ]
    outs = [
        dict(
            line_type="output",
            material_category=l.material_category,
            material_name=l.material_name,
            quantity=l.quantity,
            ratio=l.ratio,
            density=l.density,
            litre=l.litre
        )
        for l in lines if l.line_type == "output"
    ]
    wsts = [
        dict(
            line_type="waste",
            material_category=l.material_category,
            material_name=l.material_name,
            quantity=l.quantity,
            ratio=l.ratio,
            density=l.density,
            litre=l.litre
        )
        for l in lines if l.line_type == "waste"
    ]

    # ─── 4) Build equipment state ─────────────────────────────────────
    eq_lines = [l for l in lines if l.line_type == "equipment"]
    if not eq_lines or n_batches < 1:
        return []

    eq_state = []
    for l in eq_lines:
        eq_state.append({
            "equipment_id": l.equipment_id,
            "std":          float(l.std_bct or 0),
            "wait":         float(l.wait_time or 0),
            "next":         start_ts,
            "star":         bool(getattr(l, "star", False)),
            "closed_date":  l.closed_date
        })

    # ─── helper to build generated_batch_number ───────────────────────
    def gen_num(i):
        base = schedule.batch_number or ""
        if len(base) >= 2 and base[-2:].isdigit():
            prefix, start = base[:-2], int(base[-2:])
            return prefix + str(start + i).zfill(2)
        return str(i).zfill(2)

    batches = []

    # ─── FIFO ─────────────────────────────────────────────────────────
    if approach == 1:
        for i in range(1, n_batches + 1):
            cell = min(eq_state, key=lambda x: x["next"])
            st   = cell["next"]
            et   = st + _td(hours=cell["std"])

            is_cancel = False
            if cell["closed_date"] and st >= cell["closed_date"]:
                is_cancel = True
            if plan_cutoff and st >= plan_cutoff:
                is_cancel = True
            status = "Cancelled" if is_cancel else "Scheduled"

            if status == "Scheduled":
                cell["next"] = et + _td(hours=cell["wait"])

            runs = [{
                "equipment_id": cell["equipment_id"],
                "std_bct":      cell["std"],
                "wait_time":    cell["wait"],
                "star":         cell["star"],
                "start":        st,
                "end":          et,
                "status":       status
            }]

            batches.append({
                "batch_no":               i,
                "generated_batch_number": gen_num(i),
                "batch_start":            st,
                "batch_end":              et,
                "output_quantity":        per_batch,
                "equipment_runs":         runs,
                "materials":              mats,
                "outputs":                outs,
                "wastes":                 wsts,
            })

    # ─── ROLL ─────────────────────────────────────────────────────────
    elif approach == 0:
        pipeline = [dict(e) for e in eq_state]
        for i in range(1, n_batches + 1):
            runs = []
            prev = None
            for cell in pipeline:
                st = prev if prev and prev > cell["next"] else cell["next"]
                et = st + _td(hours=cell["std"])

                is_cancel = False
                if cell["closed_date"] and st >= cell["closed_date"]:
                    is_cancel = True
                if plan_cutoff and st >= plan_cutoff:
                    is_cancel = True
                status = "Cancelled" if is_cancel else "Scheduled"

                if status == "Scheduled":
                    cell["next"] = et + _td(hours=cell["wait"])
                    prev = cell["next"]

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

    # ─── STAR ─────────────────────────────────────────────────────────
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
        B["next"] = start_ts + _td(hours=B["std"] / 2)

        for i in range(1, n_batches + 1):
            omit = B if (i & 1) else A
            seq  = [e for e in eq_state if e is not omit]
            runs, prev = [], None

            for cell in seq:
                st = prev if prev and prev > cell["next"] else cell["next"]
                et = st + _td(hours=cell["std"])

                is_cancel = False
                if cell["closed_date"] and st >= cell["closed_date"]:
                    is_cancel = True
                if plan_cutoff and st >= plan_cutoff:
                    is_cancel = True
                status = "Cancelled" if is_cancel else "Scheduled"

                if status == "Scheduled":
                    cell["next"] = et + _td(hours=cell["wait"])
                    prev = cell["next"]

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

    # ─── fallback ─ treat unknown as ROLL ─────────────────────────────
    else:
        schedule.scheduling_approach = 0
        return generate_batch_rows(schedule)

    return batches


def indian_number_format(num):
    try:
        num = float(num)
    except:
        return num
    s = f"{num:.2f}"
    if "." in s:
        whole, dec = s.split(".")
    else:
        whole, dec = s, ""
    if len(whole) > 3:
        last3 = whole[-3:]
        other = whole[:-3]
        other = __import__("re").sub(r"(\d)(?=(\d\d)+$)", r"\1,", other)
        whole = other + "," + last3
    return whole if dec == "00" else whole + "." + dec


def safe_round(val):
    try:
        return round(float(val))
    except:
        return val




@login_required
def effluent_plan_actual_report(request):
    user_groups = request.user.groups.values_list('name', flat=True)  # Check if the user is in STORE group
    is_superuser = request.user.is_superuser
    # 1) parse filters & dates
    today = date.today()
    year, last_mon = today.year, today.month

    fg_filter = request.GET.get("fg_name", "").strip()
    period    = request.GET.get("period", "monthly").strip().lower()
    from_str  = request.GET.get("from_date", "").strip()
    to_str    = request.GET.get("to_date", "").strip()

    def parse_date(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except:
            return None

    # 2) build buckets & bucket_for
    if period == "daily":
        start = parse_date(from_str) or today.replace(day=1)
        end   = parse_date(to_str)   or today
        buckets = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        bucket_label_items = [(d, d.strftime("%d-%b")) for d in buckets]
        bucket_for = lambda d: d

    elif period == "weekly":
        start = date(year,1,1)
        end   = date(year,last_mon, monthrange(year,last_mon)[1])
        buckets, labels = [], {}
        d = start - timedelta(days=start.weekday())
        while d <= end:
            buckets.append(d)
            labels[d] = f"W{d.isocalendar()[1]} ({d.strftime('%d-%b')})"
            d += timedelta(days=7)
        bucket_label_items = [(b, labels[b]) for b in buckets]
        bucket_for = lambda d: d - timedelta(days=d.weekday())

    elif period == "fortnightly":
        start = date(year,1,1)
        end   = date(year,last_mon, monthrange(year,last_mon)[1])
        buckets, labels = [], {}
        d = start.replace(day=1)
        while d <= end:
            buckets.append(d)
            labels[d] = f"{d.strftime('%b')} 1-15"
            d2 = d.replace(day=16)
            buckets.append(d2)
            labels[d2] = f"{d2.strftime('%b')} 16-{monthrange(d.year,d.month)[1]}"
            d = d.replace(month=d.month+1 if d.month<12 else 1,
                          year=d.year   if d.month<12 else d.year+1)
        bucket_label_items = [(b, labels[b]) for b in buckets]
        bucket_for = lambda d: d.replace(day=1) if d.day<=15 else d.replace(day=16)

    else:  # monthly
        start = date(year,1,1)
        end   = date(year,last_mon, monthrange(year,last_mon)[1])
        buckets, labels = [], {}
        d = start.replace(day=1)
        while d <= end:
            buckets.append(d.month)
            labels[d.month] = d.strftime('%b')
            d = d.replace(month=d.month+1 if d.month<12 else 1,
                          year=d.year   if d.month<12 else d.year+1)
        bucket_label_items = [(m, labels[m]) for m in buckets]
        bucket_for = lambda d: d.month

    # 3) aggregate actuals
    actual_qs = EffluentQty.objects.select_related("effluent_record")\
        .filter(effluent_record__record_date__range=(start, end))
    if fg_filter:
        actual_qs = actual_qs.filter(effluent_record__product_name=fg_filter)

    report_data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {
        "plan": 0.0, "actual": 0.0, "batches": set()
    })))
    for qty in actual_qs:
        rec = qty.effluent_record
        b   = bucket_for(rec.record_date)
        if b not in buckets: continue
        report_data[rec.product_name][rec.stage_name][b]["actual"] += qty.quantity_kg
        if rec.batch_no:
            report_data[rec.product_name][rec.stage_name][b]["batches"].add(rec.batch_no)

    # 4) aggregate planned
    sched_qs = ProductionSchedule.objects.using("production_scheduler")\
        .filter(start_date__date__lte=end, end_date__date__gte=start)
    if fg_filter:
        sched_qs = sched_qs.filter(product_id=fg_filter)

    planned_qty   = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    planned_count = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    for sch in sched_qs:
        for batch in generate_batch_rows(sch):
            if not any(run["status"]=="Scheduled" for run in batch["equipment_runs"]):
                continue
            bkey = bucket_for(batch["batch_end"].date())
            if bkey not in buckets: continue
            for w in batch["wastes"]:
                planned_qty[sch.product_id][sch.stage_name][bkey] += w["quantity"]
            planned_count[sch.product_id][sch.stage_name][bkey] += 1

    for fg, stages in planned_qty.items():
        for stg, bm in stages.items():
            for b, val in bm.items():
                report_data[fg][stg][b]["plan"] = val
                report_data[fg][stg][b]["planned_batches"] = planned_count[fg][stg][b]

    def fmt_cell(value, batches):
        v = safe_round(value)
        s = indian_number_format(v)
        if batches:
            avg = safe_round(value / batches)
            s = f"{s} ({batches} / {indian_number_format(avg)})"
        return s

    fg_rows = []
    grand_plan = defaultdict(float)
    grand_act  = defaultdict(float)
    grand_batches = defaultdict(int)

    for fg, stages in report_data.items():
        fg_plan_total = fg_act_total = 0
        fg_plan_vals = []
        fg_act_vals  = []
        fg_plan_counts = []
        fg_act_counts = []

        for b in buckets:
            tp = sum(stages[s][b]["plan"]   for s in stages if b in stages[s])
            ta = sum(stages[s][b]["actual"] for s in stages if b in stages[s])
            pb = sum(stages[s][b].get("planned_batches",0) for s in stages if b in stages[s])
            ab = len(set().union(*(stages[s][b]["batches"] for s in stages if b in stages[s])))

            fg_plan_counts.append(pb)
            fg_act_counts.append(ab)
            fg_plan_total += tp
            fg_act_total  += ta
            grand_plan[b] += tp
            grand_act[b] += ta
            grand_batches[b] |= ab

            fg_plan_vals.append({
                "display": fmt_cell(tp, pb),
                "batch_count": pb,
            })
            fg_act_vals.append({
                "display": fmt_cell(ta, ab),
                "batch_count": ab,
            })

        fg_total_plan_s = fmt_cell(fg_plan_total, sum(fg_plan_counts))
        fg_total_act_s  = fmt_cell(fg_act_total,  sum(fg_act_counts))

        stages_list = []
        for stg, bm in stages.items():
            st_plan_vals = []
            st_act_vals  = []
            st_plan_counts = []
            st_act_counts = []

            for b in buckets:
                p = bm[b]["plan"] if b in bm else 0
                a = bm[b]["actual"] if b in bm else 0
                pb = bm[b].get("planned_batches",0)
                ab = len(bm[b]["batches"]) if b in bm else 0

                st_plan_counts.append(pb)
                st_act_counts.append(ab)
                st_plan_vals.append({
                    "display": fmt_cell(p, pb),
                    "batch_count": pb,
                })
                st_act_vals.append({
                    "display": fmt_cell(a, ab),
                    "batch_count": ab,
                })

            stage_total_plan_s = fmt_cell(sum(p["batch_count"] for p in st_plan_vals), sum(st_plan_counts))
            stage_total_act_s  = fmt_cell(sum(a["batch_count"] for a in st_act_vals),  sum(st_act_counts))

            stages_list.append({
                "stage": stg,
                "month_pairs": list(zip(st_plan_vals, st_act_vals)),
                "total_plan": stage_total_plan_s,
                "total_act" : stage_total_act_s,
            })

        fg_rows.append({
            "fg": fg,
            "month_pairs": list(zip(fg_plan_vals, fg_act_vals)),
            "total_plan": fg_total_plan_s,
            "total_act": fg_total_act_s,
            "stages": stages_list,
        })

    grand_pairs = []
    grand_tp = sum(grand_plan.values())
    grand_ta = sum(grand_act.values())
    grand_pb = sum(planned_count[fg][stg][b] 
                   for fg in report_data for stg in report_data[fg] for b in buckets)
    grand_ab = sum(grand_batches[b] for b in buckets)

    grand_total_plan_s = fmt_cell(grand_tp, grand_pb)
    grand_total_act_s  = fmt_cell(grand_ta, grand_ab)

    grand_month_pairs = [
        [fmt_cell(grand_plan[b], sum(planned_count[fg][stg][b] for fg in report_data for stg in report_data[fg])),
         fmt_cell(grand_act[b],  grand_batches[b])]
        for b in buckets
    ]

    grand_totals = {
        "month_pairs": grand_month_pairs,
        "total_plan":  grand_total_plan_s,
        "total_act":   grand_total_act_s,
    }

    return render(request, "etp/effluent_report_plan_actual.html", {
        "fg_list":             EffluentRecord.objects.values_list("product_name", flat=True).distinct(),
        "fg_filter":           fg_filter,
        "period":              period,
        "from_date":           from_str if period=="daily" else "",
        "to_date":             to_str   if period=="daily" else "",
        "bucket_label_items":  bucket_label_items,
        "fg_rows":             fg_rows,
        "grand_totals":        grand_totals,
        'user_groups':         user_groups,
        'is_superuser':        is_superuser,
    })









#---------------------------------------------------------------------------------------------------------------------
#####  Below is the general effluent #####################





@login_required
def add_general_effluent(request):
    user_groups = request.user.groups.values_list('name', flat=True)  # Check if the user is in STORE group
    is_superuser = request.user.is_superuser
    # ✅ Add Permission Check
    if not request.user.has_perm('ETP.add_generaleffluent'):
        logger.warning(f"Unauthorized add attempt by {request.user.username}")
        messages.error(request, "You do not have permission to add General Effluent records.")
        return redirect('indexpage')
    
    if request.method == 'POST':
        form = GeneralEffluentForm(request.POST)
        if form.is_valid():
            instance = form.save()
            logger.info(f"GeneralEffluent record added by {request.user.username}: ID {instance.id}")
            messages.success(request, "Effluent record added successfully.")
            return redirect('view_general_effluent')  # Or redirect to another page
        else:
            messages.error(request, "Please correct the errors below.")
            logger.warning(f"Invalid form submitted by {request.user.username}: {form.errors}")
    else:
        form = GeneralEffluentForm()

    return render(request, 'etp/add_general_effluent.html', {
        'form': form,
        'user_groups': user_groups,
        'is_superuser': is_superuser
    })



@login_required
def view_general_effluent_records(request):
    user_groups  = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser 

    if not request.user.has_perm('ETP.view_generaleffluent'):
        logger.warning(f"Unauthorized view attempt by {request.user.username}")
        messages.error(request, "You do not have permission to view General Effluent records.")
        return redirect('indexpage')

    # 🔍 Read filters
    selected_location    = request.GET.get('location', '')
    selected_nature      = request.GET.get('effluent_nature', '')
    selected_period      = request.GET.get('period', 'Monthly')
    start_str            = request.GET.get('start_date', '')
    end_str              = request.GET.get('end_date', '')

    # 📄 Base queryset (most recent first)
    qs = GeneralEffluent.objects.all().order_by('-record_date')

    # 📅 Apply period filter
    today = date.today()
    if selected_period == 'Weekly':
        start_dt = today - timedelta(days=7)
        end_dt   = today
    elif selected_period == 'Custom':
        try:
            start_dt = datetime.strptime(start_str, '%Y-%m-%d').date()
        except ValueError:
            start_dt = today.replace(day=1)
        try:
            end_dt = datetime.strptime(end_str, '%Y-%m-%d').date()
        except ValueError:
            end_dt = today
    else:  # Monthly
        start_dt = today.replace(day=1)
        end_dt   = today

    qs = qs.filter(record_date__gte=start_dt, record_date__lte=end_dt)

    # 🔍 Apply location & nature filters
    if selected_location:
        qs = qs.filter(location=selected_location)
    if selected_nature:
        qs = qs.filter(effluent_nature=selected_nature)

    # ➕ total quantity
    total_quantity = qs.aggregate(total=Sum('actual_quantity'))['total'] or 0.0

    # 🔄 distinct filter options
    locations         = GeneralEffluent.objects.values_list('location', flat=True).distinct().order_by('location')
    effluent_natures  = GeneralEffluent.objects.values_list('effluent_nature', flat=True).distinct().order_by('effluent_nature')

    # 📑 pagination
    paginator   = Paginator(qs, 10)
    page_number = request.GET.get('page')
    page_obj    = paginator.get_page(page_number)

    return render(request, 'etp/view_general_effluent.html', {
        'page_obj':          page_obj,
        'user_groups':       user_groups,
        'is_superuser':      is_superuser,
        'locations':         locations,
        'effluent_natures':  effluent_natures,
        'total_quantity':    total_quantity,
        # pass filter state back to template
        'selected_location':   selected_location,
        'selected_nature':     selected_nature,
        'selected_period':     selected_period,
        'start_date':          start_dt.isoformat(),
        'end_date':            end_dt.isoformat(),
    })



@login_required
def edit_general_effluent(request, pk):
    instance = get_object_or_404(GeneralEffluent, pk=pk)
    """ Edit a GeneralEffluent entry (Permission Required: ETP.change_generaleffluent) """
    if not request.user.has_perm('ETP.change_generaleffluent'):
        logger.warning(f"Unauthorized edit attempt by {request.user.username} on effluent ID {pk}")
        messages.error(request, "You do not have permission to edit General Effluent records.")
        return redirect('indexpage')
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if request.method == 'POST':
        form = GeneralEffluentForm(request.POST, instance=instance)
        if form.is_valid():
            form.save()
            logger.info(f"GeneralEffluent record updated by {request.user.username}: ID {instance.id}")
            messages.success(request, "Effluent record updated successfully.")
            return redirect('view_general_effluent')  # or wherever you want
        else:
            messages.error(request, "Please correct the errors below.")
            logger.warning(f"Invalid edit form by {request.user.username}: {form.errors}")
    else:
        form = GeneralEffluentForm(instance=instance)

    return render(request, 'etp/edit_general_effluent.html', {
        'form': form,
        'user_groups': user_groups,
        'is_superuser': is_superuser,
        'instance': instance
    })



@login_required
def delete_general_effluent(request, pk):
    record = get_object_or_404(GeneralEffluent, pk=pk)  
    """ Delete a GeneralEffluent entry (Permission Required: ETP.delete_generaleffluent) """
    if not request.user.has_perm('ETP.delete_generaleffluent'):
        logger.warning(f"Unauthorized delete attempt by {request.user.username} on effluent ID {record.id}")
        messages.error(request, "You do not have permission to delete General Effluent records.")
        return redirect('indexpage')

    if request.method == 'POST':
        record.delete()
        messages.success(request, "General Effluent record deleted successfully.")
        return redirect('view_general_effluent')

    messages.error(request, "Invalid request.")
    return redirect('view_general_effluent')


@login_required
def general_effluent_charts(request):
    user_groups  = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    # 1) Pull filter params (defaults: period="mtd", no location/nature)
    period         = request.GET.get('period', 'mtd')
    from_date_str  = request.GET.get('from_date', '')
    to_date_str    = request.GET.get('to_date', '')
    location_filt  = request.GET.get('location', '').strip()
    nature_filt    = request.GET.get('effluent_nature', '').strip()

    # 2) Build base queryset and apply date range
    qs = GeneralEffluent.objects.all()
    today = date.today()

    if period == 'weekly':
        start_date = today - timedelta(days=7)
        end_date   = today
    elif period == 'fortnightly':
        start_date = today - timedelta(days=15)
        end_date   = today
    elif period == 'custom' and from_date_str and to_date_str:
        # parse_date returns a date or None
        d1 = parse_date(from_date_str)
        d2 = parse_date(to_date_str)
        if d1 and d2:
            start_date, end_date = d1, d2
        else:
            # fallback to MTD if parse fails
            start_date = today.replace(day=1)
            end_date   = today
    else:  # "mtd" or any invalid
        start_date = today.replace(day=1)
        end_date   = today

    qs = qs.filter(record_date__gte=start_date, record_date__lte=end_date)

    # 3) Optional location / nature filters
    if location_filt:
        qs = qs.filter(location=location_filt)
    if nature_filt:
        qs = qs.filter(effluent_nature=nature_filt)

    # 4) Aggregate for charts
    loc_agg = (
        qs.values('location')
          .annotate(total=Sum('actual_quantity'))
          .order_by('location')
    )
    loc_labels = [row['location'] or 'Unknown' for row in loc_agg]
    loc_data   = [row['total'] for row in loc_agg]

    nature_agg = (
        qs.values('effluent_nature')
          .annotate(total=Sum('actual_quantity'))
          .order_by('effluent_nature')
    )
    nature_labels = [row['effluent_nature'] or 'Unknown' for row in nature_agg]
    nature_data   = [row['total'] for row in nature_agg]
    # in your view, before render(...)
    ALL_LOCATIONS = (
        GeneralEffluent.objects
        .values_list("location", flat=True)
        .distinct()
        .order_by("location")
    )
    ALL_NATURES = (
        GeneralEffluent.objects
        .values_list("effluent_nature", flat=True)
        .distinct()
        .order_by("effluent_nature")
    )
    # 5) Render, passing back filter state
    return render(request, 'etp/general_effluent_charts.html', {
        'loc_labels':        json.dumps(loc_labels),
        'loc_data':          json.dumps(loc_data),
        'nature_labels':     json.dumps(nature_labels),
        'nature_data':       json.dumps(nature_data),
        'user_groups':       user_groups,
        'is_superuser':      is_superuser,

        # pass these so your template can pre-fill the form controls:
        'period':            period,
        'from_date':         from_date_str,
        'to_date':           to_date_str,
        'location_filter':   location_filt,
        'nature_filter':     nature_filt,
        "ALL_LOCATIONS": ALL_LOCATIONS,
        "ALL_NATURES": ALL_NATURES,
    })






@login_required
def api_effluent_received(request):
    """
    GET params:
      - date: YYYY-MM-DD
      - nature: string (must exactly match effluent_nature in your data)
    Returns JSON with general_total, production_total, combined_total.
    """
    d_str = request.GET.get("date")
    nature = request.GET.get("nature")

    if not d_str or not nature:
        return JsonResponse({"error": "Missing 'date' or 'nature'."}, status=400)

    try:
        d = datetime.strptime(d_str, "%Y-%m-%d").date()
    except ValueError:
        return JsonResponse({"error": "Invalid date format. Use YYYY-MM-DD."}, status=400)

    # 1) Sum from GeneralEffluent for that day/nature
    general_total = (
        GeneralEffluent.objects
        .filter(record_date=d, effluent_nature=nature)
        .aggregate(total=Sum("actual_quantity"))["total"] or 0
    )

    # 2) Sum from EffluentQty (joined by EffluentRecord.record_date) for that day/nature
    production_qs = EffluentQty.objects.filter(
        effluent_record__record_date=d,
        effluent_nature=nature
        )

    if nature.strip().lower() == "basic":
        production_qs = production_qs.exclude(
            effluent_record__product_name__iexact="N,N DI ISO PROPYL ETHYL AMINE"
        )

    production_total = production_qs.aggregate(total=Sum("actual_quantity"))["total"] or 0

    combined_total = float(general_total) + float(production_total)

    return JsonResponse({
        "date": d_str,
        "nature": nature,
        "general_total": float(general_total),
        "production_total": float(production_total),
        "combined_total": round(combined_total, 2),
    })


@login_required
def primary_treat_create(request):
    user_groups  = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    if not request.user.has_perm('ETP.add_primarytreatmenteffluent'):
        logger.warning(f"Unauthorized add attempt by {request.user.username}")
        messages.error(request, "You do not have permission to add Primary treatment Effluent records.")
        return redirect('indexpage')
    
    if request.method == "POST":
        form = PrimaryTreatmentEffluentForm(request.POST)
        if form.is_valid():
            try:
                with transaction.atomic():
                    effluent_record = form.save()
                    chemical_names = request.POST.getlist('chemical_name')
                    chemical_qtys = request.POST.getlist('chemical_qty')

                    for i, name in enumerate(chemical_names):
                        qty_str = chemical_qtys[i]
                        if qty_str and float(qty_str) > 0:
                            PrimaryTreatmentChemical.objects.create(
                                effluent_record=effluent_record,
                                chemical_name=name,
                                quantity=qty_str
                            )
                
                messages.success(request, "Primary Treatment Effluent record saved successfully.")
                return redirect("pte_create")
            except Exception as e:
                logger.error(f"Error during primary treatment record save: {e}")
                messages.error(request, "An error occurred while saving. Please check your inputs and try again.")
        else:
            messages.error(request, "Please correct the errors in the main form below.")
    else:
        form = PrimaryTreatmentEffluentForm()

    # ===== THIS IS THE FIX =====
    # Create the list of chemicals in the format the template expects.
    # For a new record, all quantities will be None.
    all_chemicals_data = []
    for value, name in CHEMICAL_USED_CHOICES:
        all_chemicals_data.append({
            'value': value,
            'name': name,
            'quantity': None,  # Always None for a new record
        })

    return render(request, "etp/primary_treat_form.html", {
        "form": form,
        # Pass the correctly formatted list with the expected name 'all_chemicals'
        "all_chemicals": all_chemicals_data, 
        "active_link": "primary_treatment_effluent",
        'user_groups': user_groups,
        'is_superuser': is_superuser,
    })
    
    
@login_required    
def primary_treat_list(request):
    user_groups  = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    if not request.user.has_perm('ETP.view_primarytreatmenteffluent'):
        logger.warning(f"Unauthorized View attempt by {request.user.username}")
        messages.error(request, "You do not have permission to view Primary treatment Effluent records.")
        return redirect('indexpage')
    
    # Use prefetch_related for performance efficiency
    qs = PrimaryTreatmentEffluent.objects.all().prefetch_related('chemicals_used').order_by("-date")

    from_date = request.GET.get("from_date", "") or ""
    to_date   = request.GET.get("to_date", "") or ""
    nature    = request.GET.get("effluent_nature", "") or ""
    chem      = request.GET.get("chemical_used", "") or ""

    # If no filters applied → default to current month
    if not (from_date or to_date or nature or chem):
        today = date.today()
        from_date = today.replace(day=1).isoformat()
        to_date = today.isoformat()

    if from_date:
        d = parse_date(from_date)
        if d:
            qs = qs.filter(date__gte=d)
    if to_date:
        d = parse_date(to_date)
        if d:
            qs = qs.filter(date__lte=d)
    if nature:
        qs = qs.filter(effluent_nature__icontains=nature)
    
    # UPDATED: Correct filtering for related chemical model
    if chem:
        qs = qs.filter(chemicals_used__chemical_name__icontains=chem).distinct()

    # Totals on the filtered set
    totals = qs.aggregate(
        total_received=Sum("effluent_received"),
        total_neutralized=Sum("effluent_neutralized"),
    )

    paginator = Paginator(qs, 25)  # page size
    page_num = request.GET.get("page")
    items = paginator.get_page(page_num)

    context = {
        'user_groups':   user_groups,
        'is_superuser':  is_superuser,
        "items": items,
        "from_date": from_date,
        "to_date": to_date,
        "effluent_nature": nature,
        "chemical_used": chem,
        "totals": {
            "received": round(float(totals["total_received"] or 0), 2),
            "neutralized": round(float(totals["total_neutralized"] or 0), 2),
        },
        "active_link": "primary_treatment_effluent",
    }
    return render(request, "etp/primary_treat_list.html", context)


@login_required
def primary_treat_edit(request, pk):
    user_groups  = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser
    if not request.user.has_perm('ETP.change_primarytreatmenteffluent'):
        logger.warning(f"Unauthorized edit attempt by {request.user.username} on PTE record {pk}")
        messages.error(request, "You do not have permission to update Primary treatment Effluent records.")
        return redirect('indexpage')
    
    # Get the main record instance you want to edit
    obj = get_object_or_404(PrimaryTreatmentEffluent, pk=pk)

    if request.method == "POST":
        form = PrimaryTreatmentEffluentForm(request.POST, instance=obj)
        if form.is_valid():
            try:
                # Use a transaction to ensure all database changes succeed or none do
                with transaction.atomic():
                    # Save the changes to the main record (date, nature, etc.)
                    effluent_record = form.save()

                    # 1. Delete all previously saved chemicals for this record.
                    effluent_record.chemicals_used.all().delete()

                    # 2. Get the new chemical data from the submitted form.
                    chemical_names = request.POST.getlist('chemical_name')
                    chemical_qtys = request.POST.getlist('chemical_qty')

                    # 3. Create new chemical records for each selected chemical.
                    for i, name in enumerate(chemical_names):
                        qty_str = chemical_qtys[i]
                        if qty_str and float(qty_str) > 0:
                            PrimaryTreatmentChemical.objects.create(
                                effluent_record=effluent_record,
                                chemical_name=name,
                                quantity=qty_str
                            )
                
                messages.success(request, "Record updated successfully.")
                return redirect("pte_list")
            except Exception as e:
                logger.error(f"Error updating PTE record {pk}: {e}")
                messages.error(request, "An unexpected error occurred during the update. Please try again.")
        else:
            messages.error(request, "Please correct the errors in the form below.")
    else:
        # This is for the initial GET request (when the page first loads)
        form = PrimaryTreatmentEffluentForm(instance=obj)

    # Prepare the chemical data for the template
    # Get a dictionary of chemicals that are already saved for this specific record
    saved_chemicals_dict = {c.chemical_name: c.quantity for c in obj.chemicals_used.all()}

    # Build a list that the template can easily loop through. For each possible
    # chemical, it will include whether it's saved and what its quantity is.
    all_chemicals_with_data = []
    for value, name in CHEMICAL_USED_CHOICES:
        all_chemicals_with_data.append({
            'value': value,
            'name': name,
            'quantity': saved_chemicals_dict.get(value)  # Will be the quantity or None
        })

    return render(request, "etp/primary_treat_form.html", {
        'user_groups':   user_groups,
        'is_superuser':  is_superuser,
        "form": form,
        # Pass the prepared chemical list to the template
        "all_chemicals": all_chemicals_with_data, 
        "active_link": "primary_treatment_effluent",
    })


@require_POST
def primary_treat_delete(request, pk):
    """Delete a PrimaryTreatmentEffluent record (POST only)."""
    obj = get_object_or_404(PrimaryTreatmentEffluent, pk=pk)
    obj.delete()
    messages.success(request, "Record deleted successfully.")
    return redirect("pte_list")



@login_required
def primary_treat_excel(request):
    """Download filtered PrimaryTreatmentEffluent as an Excel file with grouped rows."""
    
    # ── Filtering Logic (No changes needed here) ──────────────────────────────
    qs = PrimaryTreatmentEffluent.objects.all().prefetch_related('chemicals_used').order_by("-date")

    from_date = request.GET.get("from_date", "") or ""
    to_date   = request.GET.get("to_date", "") or ""
    nature    = request.GET.get("effluent_nature", "") or ""
    chem      = request.GET.get("chemical_used", "") or ""

    if from_date:
        d = parse_date(from_date)
        if d:
            qs = qs.filter(date__gte=d)
    if to_date:
        d = parse_date(to_date)
        if d:
            qs = qs.filter(date__lte=d)
    if nature:
        qs = qs.filter(effluent_nature__icontains=nature)
    if chem:
        qs = qs.filter(chemicals_used__chemical_name__icontains=chem).distinct()

    # ── Calculate totals on the final queryset ─────────────────────────────────
    totals = qs.aggregate(
        total_received=Sum("effluent_received"),
        total_neutralized=Sum("effluent_neutralized"),
    )

    # ── Build workbook with new grouped format ────────────────────────────────
    ts = now().strftime("%Y%m%d_%H%M%S")
    filename = f"PTE_Grouped_Report_{from_date or 'all'}_{to_date or 'all'}_{ts}.xlsx"

    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {"in_memory": True})
    ws = wb.add_worksheet("PTE_Report")

    # Formats (Added formats with vertical alignment for merged cells)
    fmt_title = wb.add_format({"bold": True, "font_size": 16, "align": "center", "valign": "vcenter", "bg_color": "#D9E1F2"})
    fmt_head  = wb.add_format({"bold": True, "bg_color": "#E5F0FF", "border": 1, "align": "center", "valign": "vcenter"})
    # Format for single-row cells
    fmt_text   = wb.add_format({"border": 1}) 
    # Formats for merged cells, aligned to the top
    fmt_text_top = wb.add_format({"border": 1, "valign": "top"})
    fmt_date_top = wb.add_format({"num_format": "dd-mm-yyyy", "border": 1, "valign": "top"})
    fmt_num_top  = wb.add_format({"num_format": "0.00", "border": 1, "valign": "top"})
    # Formats for the totals row
    fmt_total_hdr = wb.add_format({"bold": True, "bg_color": "#FFF7CC", "border": 1, "align": "right"})
    fmt_total     = wb.add_format({"bold": True, "bg_color": "#FFF7CC", "num_format": "0.00", "border": 1})

    headers = ["Date", "Effluent Nature", "Effluent Received (KL)", "Effluent Neutralized (KL)", "Chemicals Used (Name & Qty)"]
    last_col = len(headers) - 1

    ws.merge_range(0, 0, 0, last_col, "Primary Treatment Effluent Report", fmt_title)
    for c, h in enumerate(headers):
        ws.write(2, c, h, fmt_head)

    # Data rows with merging logic
    row = 3
    col_widths = [len(h) for h in headers]

    for obj in qs:
        chemicals = list(obj.chemicals_used.all())
        # A record will span as many rows as it has chemicals, or 1 if it has none
        rowspan = len(chemicals) if chemicals else 1

        # Use merge_range for the main data columns
        # merge_range(first_row, first_col, last_row, last_col, data, format)
        if rowspan > 1:
            ws.merge_range(row, 0, row + rowspan - 1, 0, datetime.combine(obj.date, dtime.min), fmt_date_top)
            ws.merge_range(row, 1, row + rowspan - 1, 1, obj.effluent_nature or "", fmt_text_top)
            ws.merge_range(row, 2, row + rowspan - 1, 2, float(obj.effluent_received or 0), fmt_num_top)
            ws.merge_range(row, 3, row + rowspan - 1, 3, float(obj.effluent_neutralized or 0), fmt_num_top)
        else:
            # If only one row, no need to merge, just write
            ws.write_datetime(row, 0, datetime.combine(obj.date, dtime.min), fmt_date_top)
            ws.write(row, 1, obj.effluent_nature or "", fmt_text_top)
            ws.write_number(row, 2, float(obj.effluent_received or 0), fmt_num_top)
            ws.write_number(row, 3, float(obj.effluent_neutralized or 0), fmt_num_top)

        # Write each chemical into its own row within the block
        if chemicals:
            for i, chem_obj in enumerate(chemicals):
                chemical_string = f"{chem_obj.chemical_name} ({chem_obj.quantity:.2f})"
                ws.write(row + i, 4, chemical_string, fmt_text)
                col_widths[4] = max(col_widths[4], len(chemical_string))
        else:
            # If no chemicals, write a placeholder in the single row
            ws.write(row, 4, "—", fmt_text)

        # Update column widths for main data
        col_widths[1] = max(col_widths[1], len(obj.effluent_nature or ""))
        
        # Move the row pointer down by the number of rows this record took up
        row += rowspan

    # Totals row
    ws.merge_range(row, 0, row, 1, "Totals", fmt_total_hdr)
    ws.write_number(row, 2, float(totals['total_received'] or 0), fmt_total)
    ws.write_number(row, 3, float(totals['total_neutralized'] or 0), fmt_total)
    ws.write(row, 4, "", fmt_total) 

    # Set final column widths
    caps = [15, 40, 22, 26, 45]
    for i, w in enumerate(col_widths):
        ws.set_column(i, i, min(w + 3, caps[i]))

    wb.close()
    output.seek(0)

    return HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# ==========================================================================================
# ========   Below code for Effluent Dashboard  ========================================


# --- helper to safely parse dates ---
def _parse_date(s, default=None):
    if not s:
        return default
    try:
        return parse_date(s) or default
    except Exception:
        return default


@login_required
def effluent_dashboard(request):
    logger.info("User=%s accessed Effuent Dashboard", request.user.username)
    products = (EffluentRecord.objects.exclude(product_name__isnull=True)
                .exclude(product_name__exact="")
                .values_list("product_name", flat=True)
                .distinct().order_by("product_name"))

    stages = (EffluentRecord.objects.exclude(stage_name__isnull=True)
              .exclude(stage_name__exact="")
              .values_list("stage_name", flat=True)
              .distinct().order_by("stage_name"))

    blocks = (EffluentRecord.objects.exclude(block__isnull=True)
              .exclude(block__exact="")
              .values_list("block", flat=True)
              .distinct().order_by("block"))

    batches = (EffluentRecord.objects.exclude(batch_no__isnull=True)
               .exclude(batch_no__exact="")
               .values_list("batch_no", flat=True)
               .distinct().order_by("batch_no"))

    natures = (EffluentQty.objects.exclude(effluent_nature__isnull=True)
               .exclude(effluent_nature__exact="")
               .values_list("effluent_nature", flat=True)
               .distinct().order_by("effluent_nature"))

    context = {
        "products": products,
        "stages": stages,
        "blocks": blocks,
        "batches": batches,
        "natures": natures,
        "default_from": date.today().replace(day=1).isoformat(),
        "default_to": date.today().isoformat(),
    }
    return render(request, "effluent/effluent_dashboard_tables.html", context)


@login_required
def effluent_dashboard_data(request):
    """
    JSON API for Effluent Dashboard (AJAX).
    Returns totals, summary tables, and detail rows.
    """
    dfrom   = _parse_date(request.GET.get("from"))
    dto     = _parse_date(request.GET.get("to"))
    product = (request.GET.get("product") or "").strip()
    stage   = (request.GET.get("stage") or "").strip()
    block   = (request.GET.get("block") or "").strip()
    nature  = (request.GET.get("nature") or "").strip()
    batch   = (request.GET.get("batch") or "").strip()
    q       = (request.GET.get("q") or "").strip()

    qs = EffluentQty.objects.select_related("effluent_record").all()

    # date filter
    if dfrom and dto:
        qs = qs.filter(effluent_record__record_date__range=[dfrom, dto])
    elif dfrom:
        qs = qs.filter(effluent_record__record_date__gte=dfrom)
    elif dto:
        qs = qs.filter(effluent_record__record_date__lte=dto)

    # direct filters
    if product:
        qs = qs.filter(effluent_record__product_name=product)
    if stage:
        qs = qs.filter(effluent_record__stage_name=stage)
    if block:
        qs = qs.filter(effluent_record__block=block)
    if nature:
        qs = qs.filter(effluent_nature=nature)
    if batch:
        qs = qs.filter(effluent_record__batch_no=batch)

    # free search
    if q:
        qs = qs.filter(
            Q(effluent_record__product_name__icontains=q) |
            Q(effluent_record__stage_name__icontains=q)   |
            Q(effluent_record__batch_no__icontains=q)     |
            Q(effluent_record__voucher_no__icontains=q)   |
            Q(effluent_record__block__icontains=q)        |
            Q(effluent_nature__icontains=q)               |
            Q(category__icontains=q)
        )

    # KPIs
    totals = qs.aggregate(
        total_plan=Sum("plan_quantity"),
        total_actual=Sum("actual_quantity"),
        total_kg=Sum("quantity_kg"),
    )

    # --- SUMMARY TABLES ---
    by_product_qs = (qs.values("effluent_record__product_name")
                       .annotate(plan=Sum("plan_quantity"),
                                 actual=Sum("actual_quantity"),
                                 kg=Sum("quantity_kg"))
                       .order_by("-actual"))
    by_product = [{
        "product": r["effluent_record__product_name"] or "(blank)",
        "plan": float(r["plan"] or 0),
        "actual": float(r["actual"] or 0),
        "kg": float(r["kg"] or 0),
    } for r in by_product_qs]

    by_stage_qs = (qs.values("effluent_record__stage_name")
                     .annotate(plan=Sum("plan_quantity"),
                               actual=Sum("actual_quantity"),
                               kg=Sum("quantity_kg"))
                     .order_by("-actual"))
    by_stage = [{
        "stage": r["effluent_record__stage_name"] or "(blank)",
        "plan": float(r["plan"] or 0),
        "actual": float(r["actual"] or 0),
        "kg": float(r["kg"] or 0),
    } for r in by_stage_qs]

    by_block_qs = (qs.values("effluent_record__block")
                     .annotate(plan=Sum("plan_quantity"),
                               actual=Sum("actual_quantity"),
                               kg=Sum("quantity_kg"))
                     .order_by("-actual"))
    by_block = [{
        "block": r["effluent_record__block"] or "(blank)",
        "plan": float(r["plan"] or 0),
        "actual": float(r["actual"] or 0),
        "kg": float(r["kg"] or 0),
    } for r in by_block_qs]

    by_nature_qs = (qs.values("effluent_nature")
                      .annotate(plan=Sum("plan_quantity"),
                                actual=Sum("actual_quantity"),
                                kg=Sum("quantity_kg"))
                      .order_by("-actual"))
    by_nature = [{
        "nature": r["effluent_nature"] or "(blank)",
        "plan": float(r["plan"] or 0),
        "actual": float(r["actual"] or 0),
        "kg": float(r["kg"] or 0),
    } for r in by_nature_qs]

    by_batch_qs = (qs.values("effluent_record__batch_no")
                     .annotate(plan=Sum("plan_quantity"),
                               actual=Sum("actual_quantity"),
                               kg=Sum("quantity_kg"))
                     .order_by("-actual"))
    by_batch = [{
        "batch": r["effluent_record__batch_no"] or "(blank)",
        "plan": float(r["plan"] or 0),
        "actual": float(r["actual"] or 0),
        "kg": float(r["kg"] or 0),
    } for r in by_batch_qs]

    # --- DETAIL ROWS (latest 500 for UI) ---
    rows_qs = (qs.values(
        "effluent_record__record_date",
        "effluent_record__product_name",
        "effluent_record__stage_name",
        "effluent_record__batch_no",
        "effluent_record__voucher_no",
        "effluent_record__block",
        "effluent_nature",
    ).annotate(
        plan=Sum("plan_quantity"),
        actual=Sum("actual_quantity"),
        kg=Sum("quantity_kg"),
    ).order_by("-effluent_record__record_date")[:500])

    table_rows = [{
        "date": r["effluent_record__record_date"].isoformat() if r["effluent_record__record_date"] else "",
        "product": r["effluent_record__product_name"] or "",
        "stage": r["effluent_record__stage_name"] or "",
        "batch_no": r["effluent_record__batch_no"] or "",
        "voucher_no": r["effluent_record__voucher_no"] or "",
        "block": r["effluent_record__block"] or "",
        "nature": r["effluent_nature"] or "",
        "plan": float(r["plan"] or 0),
        "actual": float(r["actual"] or 0),
        "kg": float(r["kg"] or 0),
    } for r in rows_qs]

    return JsonResponse({
        "totals": {
            "plan": float(totals["total_plan"] or 0),
            "actual": float(totals["total_actual"] or 0),
            "kg": float(totals["total_kg"] or 0),
        },
        "by_product": by_product,
        "by_stage": by_stage,
        "by_block": by_block,
        "by_nature": by_nature,
        "by_batch": by_batch,
        "table": table_rows,
    }, safe=False)


@login_required
def effluent_dashboard_export(request):
    """
    Export current Detail table to Excel applying the same filters.
    """
    dfrom   = _parse_date(request.GET.get("from"))
    dto     = _parse_date(request.GET.get("to"))
    product = (request.GET.get("product") or "").strip()
    stage   = (request.GET.get("stage") or "").strip()
    block   = (request.GET.get("block") or "").strip()
    nature  = (request.GET.get("nature") or "").strip()
    batch   = (request.GET.get("batch") or "").strip()
    q       = (request.GET.get("q") or "").strip()

    qs = EffluentQty.objects.select_related("effluent_record").all()

    if dfrom and dto:
        qs = qs.filter(effluent_record__record_date__range=[dfrom, dto])
    elif dfrom:
        qs = qs.filter(effluent_record__record_date__gte=dfrom)
    elif dto:
        qs = qs.filter(effluent_record__record_date__lte=dto)

    if product:
        qs = qs.filter(effluent_record__product_name=product)
    if stage:
        qs = qs.filter(effluent_record__stage_name=stage)
    if block:
        qs = qs.filter(effluent_record__block=block)
    if nature:
        qs = qs.filter(effluent_nature=nature)
    if batch:
        qs = qs.filter(effluent_record__batch_no=batch)
    if q:
        qs = qs.filter(
            Q(effluent_record__product_name__icontains=q) |
            Q(effluent_record__stage_name__icontains=q)   |
            Q(effluent_record__batch_no__icontains=q)     |
            Q(effluent_record__voucher_no__icontains=q)   |
            Q(effluent_record__block__icontains=q)        |
            Q(effluent_nature__icontains=q)               |
            Q(category__icontains=q)
        )

    LIMIT = 50000
    rows = (qs.values(
        "effluent_record__record_date",
        "effluent_record__product_name",
        "effluent_record__stage_name",
        "effluent_record__batch_no",
        "effluent_record__voucher_no",
        "effluent_record__block",
        "effluent_nature",
        "plan_quantity",
        "actual_quantity",
        "quantity_kg",
    ).order_by("-effluent_record__record_date")[:LIMIT])

    ts = now().strftime("%Y%m%d_%H%M%S")
    filename = f"Effluent_Detail_{ts}.xlsx"

    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {"in_memory": True})
    ws = wb.add_worksheet("Detail")

    fmt_head = wb.add_format({"bold": True, "bg_color": "#E5F0FF", "border": 1, "align": "center", "valign": "vcenter"})
    fmt_text = wb.add_format({"border": 1})
    fmt_num  = wb.add_format({"border": 1, "num_format": "0.00"})
    fmt_date = wb.add_format({"border": 1, "num_format": "dd-mm-yyyy"})

    headers = ["Date", "Product", "Stage", "Batch No", "Voucher No", "Block", "Nature", "Plan", "Actual", "Kg"]
    for c, h in enumerate(headers):
        ws.write(0, c, h, fmt_head)

    r = 1
    for row in rows:
        d = row["effluent_record__record_date"]
        if d:
            ws.write_datetime(r, 0, dt.combine(d, dtime.min), fmt_date)
        else:
            ws.write(r, 0, "", fmt_text)

        ws.write(r, 1, row["effluent_record__product_name"] or "", fmt_text)
        ws.write(r, 2, row["effluent_record__stage_name"] or "", fmt_text)
        ws.write(r, 3, row["effluent_record__batch_no"] or "", fmt_text)
        ws.write(r, 4, row["effluent_record__voucher_no"] or "", fmt_text)
        ws.write(r, 5, row["effluent_record__block"] or "", fmt_text)
        ws.write(r, 6, row["effluent_nature"] or "", fmt_text)
        ws.write_number(r, 7, float(row["plan_quantity"] or 0), fmt_num)
        ws.write_number(r, 8, float(row["actual_quantity"] or 0), fmt_num)
        ws.write_number(r, 9, float(row["quantity_kg"] or 0), fmt_num)
        r += 1

    widths = [12, 18, 18, 14, 14, 12, 16, 12, 12, 12]
    for i, w in enumerate(widths):
        ws.set_column(i, i, w)

    wb.close()
    output.seek(0)

    return HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# views.py

from collections import defaultdict
from django.db.models import Sum, Q
# assumes you already have: from django.utils.timezone import now
# and for export: from datetime import datetime as dt, time as dtime
# and xlsxwriter/io imported in the file elsewhere

from .models import EffluentQty, GeneralEffluent  # <-- make sure this import exists


@login_required
def effluent_dashboard_data_kl(request):
    """
    KL-only JSON including BOTH:
      • EffluentQty (actual_quantity)
      • GeneralEffluent (actual_quantity)
    Mappings for GeneralEffluent:
      product="General", stage="General Effluent",
      date=record_date, block=location, nature=effluent_nature
    """
    dfrom   = _parse_date(request.GET.get("from"))
    dto     = _parse_date(request.GET.get("to"))
    product = (request.GET.get("product") or "").strip()
    stage   = (request.GET.get("stage") or "").strip()
    block   = (request.GET.get("block") or "").strip()
    nature  = (request.GET.get("nature") or "").strip()
    batch   = (request.GET.get("batch") or "").strip()
    q       = (request.GET.get("q") or "").strip()

    # ---------- Base query: process-linked effluent ----------
    qs = EffluentQty.objects.select_related("effluent_record").all()

    # Date filter
    if dfrom and dto:
        qs = qs.filter(effluent_record__record_date__range=[dfrom, dto])
    elif dfrom:
        qs = qs.filter(effluent_record__record_date__gte=dfrom)
    elif dto:
        qs = qs.filter(effluent_record__record_date__lte=dto)

    # Direct filters
    if product:
        qs = qs.filter(effluent_record__product_name=product)
    if stage:
        qs = qs.filter(effluent_record__stage_name=stage)
    if block:
        qs = qs.filter(effluent_record__block=block)
    if nature:
        qs = qs.filter(effluent_nature=nature)
    if batch:
        qs = qs.filter(effluent_record__batch_no=batch)

    # Free search
    if q:
        qs = qs.filter(
            Q(effluent_record__product_name__icontains=q) |
            Q(effluent_record__stage_name__icontains=q)   |
            Q(effluent_record__batch_no__icontains=q)     |
            Q(effluent_record__voucher_no__icontains=q)   |
            Q(effluent_record__block__icontains=q)        |
            Q(effluent_nature__icontains=q)               |
            Q(category__icontains=q)
        )

    # ---------- GeneralEffluent query ----------
    gqs = GeneralEffluent.objects.all()

    # Date filter (record_date)
    if dfrom and dto:
        gqs = gqs.filter(record_date__range=[dfrom, dto])
    elif dfrom:
        gqs = gqs.filter(record_date__gte=dfrom)
    elif dto:
        gqs = gqs.filter(record_date__lte=dto)

    # Map filters:
    # product -> must equal "General" to include general rows when product filter present
    if product:
        if product.lower() != "general":
            gqs = gqs.none()
    # stage -> must equal "General Effluent" to include when stage filter present
    if stage:
        if stage.lower() != "general effluent":
            gqs = gqs.none()
    # block -> maps to location
    if block:
        gqs = gqs.filter(location=block)
    # nature -> maps to effluent_nature
    if nature:
        gqs = gqs.filter(effluent_nature=nature)
    # batch filter does not apply to GeneralEffluent (there is no batch), so exclude all if user filters by batch
    if batch:
        gqs = gqs.none()

    # Free search for general
    if q:
        # match on location/block or effluent_nature, and allow "general" keyword to bring these in
        gqs = gqs.filter(
            Q(location__icontains=q) |
            Q(effluent_nature__icontains=q) |
            Q(effluent_nature__isnull=True, location__isnull=True)  # harmless extension
        )
        # If user searches "general", we won't exclude — current OR logic already allows it.

    # ---------- KPIs ----------
    main_total_kl = qs.aggregate(v=Sum("actual_quantity"))["v"] or 0
    gen_total_kl  = gqs.aggregate(v=Sum("actual_quantity"))["v"] or 0
    total_kl      = float(main_total_kl + gen_total_kl)

    # ---------- Summaries: build maps then → sorted lists ----------
    # Product
    prod_map = defaultdict(float)
    for r in (qs.values("effluent_record__product_name")
                .annotate(kl=Sum("actual_quantity"))
                .order_by()):
        prod_map[(r["effluent_record__product_name"] or "(blank)")] += float(r["kl"] or 0)
    if gen_total_kl:
        prod_map["General"] += float(gen_total_kl)
    by_product = sorted(
        [{"product": k, "kl": v} for k, v in prod_map.items()],
        key=lambda x: -x["kl"]
    )

    # Stage
    stage_map = defaultdict(float)
    for r in (qs.values("effluent_record__stage_name")
                .annotate(kl=Sum("actual_quantity"))
                .order_by()):
        stage_map[(r["effluent_record__stage_name"] or "(blank)")] += float(r["kl"] or 0)
    if gen_total_kl:
        stage_map["General Effluent"] += float(gen_total_kl)
    by_stage = sorted(
        [{"stage": k, "kl": v} for k, v in stage_map.items()],
        key=lambda x: -x["kl"]
    )

    # Block (block for main, location for general)
    block_map = defaultdict(float)
    for r in (qs.values("effluent_record__block")
                .annotate(kl=Sum("actual_quantity"))
                .order_by()):
        block_map[(r["effluent_record__block"] or "(blank)")] += float(r["kl"] or 0)
    for r in (gqs.values("location")
                .annotate(kl=Sum("actual_quantity"))
                .order_by()):
        block_map[(r["location"] or "(blank)")] += float(r["kl"] or 0)
    by_block = sorted(
        [{"block": k, "kl": v} for k, v in block_map.items()],
        key=lambda x: -x["kl"]
    )

    # Nature
    nature_map = defaultdict(float)
    for r in (qs.values("effluent_nature")
                .annotate(kl=Sum("actual_quantity"))
                .order_by()):
        nature_map[(r["effluent_nature"] or "(blank)")] += float(r["kl"] or 0)
    for r in (gqs.values("effluent_nature")
                .annotate(kl=Sum("actual_quantity"))
                .order_by()):
        nature_map[(r["effluent_nature"] or "(blank)")] += float(r["kl"] or 0)
    by_nature = sorted(
        [{"nature": k, "kl": v} for k, v in nature_map.items()],
        key=lambda x: -x["kl"]
    )

    # Batch (no batch in general → unchanged from main)
    by_batch = [
        {
            "batch": r["effluent_record__batch_no"] or "(blank)",
            "kl": float(r["kl"] or 0),
        }
        for r in (
            qs.values("effluent_record__batch_no")
              .annotate(kl=Sum("actual_quantity"))
              .order_by("-kl")
        )
    ]

    # ---------- Detail rows (combine main + general, sort desc by date, top 500) ----------
    main_rows = (
        qs.values(
            "effluent_record__record_date",
            "effluent_record__product_name",
            "effluent_record__stage_name",
            "effluent_record__batch_no",
            "effluent_record__voucher_no",
            "effluent_record__block",
            "effluent_nature",
        )
        .annotate(kl=Sum("actual_quantity"))
        .order_by("-effluent_record__record_date")
    )

    table_rows = [{
        "date": r["effluent_record__record_date"].isoformat() if r["effluent_record__record_date"] else "",
        "product": r["effluent_record__product_name"] or "",
        "stage": r["effluent_record__stage_name"] or "",
        "batch_no": r["effluent_record__batch_no"] or "",
        "voucher_no": r["effluent_record__voucher_no"] or "",
        "block": r["effluent_record__block"] or "",
        "nature": r["effluent_nature"] or "",
        "kl": float(r["kl"] or 0),
    } for r in main_rows]

    gen_rows = gqs.values("record_date", "location", "effluent_nature", "actual_quantity").order_by("-record_date")
    for r in gen_rows:
        d = r["record_date"]
        table_rows.append({
            "date": d.isoformat() if d else "",
            "product": "General",
            "stage": "General Effluent",
            "batch_no": "",
            "voucher_no": "",
            "block": r["location"] or "",
            "nature": r["effluent_nature"] or "",
            "kl": float(r["actual_quantity"] or 0),
        })

    # Sort by date desc (ISO sorts correctly) and cap 500
    table_rows.sort(key=lambda x: x["date"] or "", reverse=True)
    table_rows = table_rows[:500]

    return JsonResponse({
        "totals": {"kl": total_kl},
        "by_product": by_product,
        "by_stage": by_stage,
        "by_block": by_block,
        "by_nature": by_nature,
        "by_batch": by_batch,
        "table": table_rows,
    }, safe=False)


@login_required
def effluent_dashboard_export_kl(request):
    """
    Export KL detail with BOTH datasets merged (no Plan, no Kg):
      • EffluentQty  -> KL = actual_quantity
      • GeneralEffluent -> KL = actual_quantity
    """
    dfrom   = _parse_date(request.GET.get("from"))
    dto     = _parse_date(request.GET.get("to"))
    product = (request.GET.get("product") or "").strip()
    stage   = (request.GET.get("stage") or "").strip()
    block   = (request.GET.get("block") or "").strip()
    nature  = (request.GET.get("nature") or "").strip()
    batch   = (request.GET.get("batch") or "").strip()
    q       = (request.GET.get("q") or "").strip()

    # Main qs
    qs = EffluentQty.objects.select_related("effluent_record").all()

    if dfrom and dto:
        qs = qs.filter(effluent_record__record_date__range=[dfrom, dto])
    elif dfrom:
        qs = qs.filter(effluent_record__record_date__gte=dfrom)
    elif dto:
        qs = qs.filter(effluent_record__record_date__lte=dto)

    if product:
        qs = qs.filter(effluent_record__product_name=product)
    if stage:
        qs = qs.filter(effluent_record__stage_name=stage)
    if block:
        qs = qs.filter(effluent_record__block=block)
    if nature:
        qs = qs.filter(effluent_nature=nature)
    if batch:
        qs = qs.filter(effluent_record__batch_no=batch)
    if q:
        qs = qs.filter(
            Q(effluent_record__product_name__icontains=q) |
            Q(effluent_record__stage_name__icontains=q)   |
            Q(effluent_record__batch_no__icontains=q)     |
            Q(effluent_record__voucher_no__icontains=q)   |
            Q(effluent_record__block__icontains=q)        |
            Q(effluent_nature__icontains=q)               |
            Q(category__icontains=q)
        )

    # General qs
    gqs = GeneralEffluent.objects.all()

    if dfrom and dto:
        gqs = gqs.filter(record_date__range=[dfrom, dto])
    elif dfrom:
        gqs = gqs.filter(record_date__gte=dfrom)
    elif dto:
        gqs = gqs.filter(record_date__lte=dto)

    if product and product.lower() != "general":
        gqs = gqs.none()
    if stage and stage.lower() != "general effluent":
        gqs = gqs.none()
    if block:
        gqs = gqs.filter(location=block)
    if nature:
        gqs = gqs.filter(effluent_nature=nature)
    if batch:
        gqs = gqs.none()
    if q:
        gqs = gqs.filter(
            Q(location__icontains=q) |
            Q(effluent_nature__icontains=q)
        )

    LIMIT = 50000

    # Pull main detail (aggregated by doc date & dims)
    main_rows = (qs.values(
        "effluent_record__record_date",
        "effluent_record__product_name",
        "effluent_record__stage_name",
        "effluent_record__batch_no",
        "effluent_record__voucher_no",
        "effluent_record__block",
        "effluent_nature",
    ).annotate(kl=Sum("actual_quantity"))
     .order_by("-effluent_record__record_date")[:LIMIT])

    # Pull general (raw rows)
    gen_rows = gqs.values(
        "record_date", "location", "effluent_nature", "actual_quantity"
    ).order_by("-record_date")[:LIMIT]

    # Merge to a single in-memory table
    merged = []
    for r in main_rows:
        merged.append({
            "date": r["effluent_record__record_date"],
            "product": r["effluent_record__product_name"] or "",
            "stage": r["effluent_record__stage_name"] or "",
            "batch_no": r["effluent_record__batch_no"] or "",
            "voucher_no": r["effluent_record__voucher_no"] or "",
            "block": r["effluent_record__block"] or "",
            "nature": r["effluent_nature"] or "",
            "kl": float(r["kl"] or 0),
        })
    for r in gen_rows:
        merged.append({
            "date": r["record_date"],
            "product": "General",
            "stage": "General Effluent",
            "batch_no": "",
            "voucher_no": "",
            "block": r["location"] or "",
            "nature": r["effluent_nature"] or "",
            "kl": float(r["actual_quantity"] or 0),
        })

    # Sort desc by date and limit
    merged.sort(key=lambda x: (x["date"] or dt.min.date()), reverse=True)
    merged = merged[:LIMIT]

    # ---- Excel build ----
    ts = now().strftime("%Y%m%d_%H%M%S")
    filename = f"Effluent_KL_{ts}.xlsx"

    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {"in_memory": True})
    ws = wb.add_worksheet("KL Detail")

    fmt_head = wb.add_format({"bold": True, "bg_color": "#E5F0FF", "border": 1, "align": "center", "valign": "vcenter"})
    fmt_text = wb.add_format({"border": 1})
    fmt_num  = wb.add_format({"border": 1, "num_format": "0.00"})
    fmt_date = wb.add_format({"border": 1, "num_format": "dd-mm-yyyy"})

    headers = ["Date", "Product", "Stage", "Batch No", "Voucher No", "Block", "Nature", "KL"]
    for c, h in enumerate(headers):
        ws.write(0, c, h, fmt_head)

    r = 1
    for row in merged:
        d = row["date"]
        if d:
            ws.write_datetime(r, 0, dt.combine(d, dtime.min), fmt_date)
        else:
            ws.write(r, 0, "", fmt_text)

        ws.write(r, 1, row["product"], fmt_text)
        ws.write(r, 2, row["stage"], fmt_text)
        ws.write(r, 3, row["batch_no"], fmt_text)
        ws.write(r, 4, row["voucher_no"], fmt_text)
        ws.write(r, 5, row["block"], fmt_text)
        ws.write(r, 6, row["nature"], fmt_text)
        ws.write_number(r, 7, row["kl"], fmt_num)
        r += 1

    widths = [12, 18, 18, 14, 14, 12, 16, 12]
    for i, w in enumerate(widths):
        ws.set_column(i, i, w)

    wb.close()
    output.seek(0)

    return HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# --- helpers for HW filters (place near _parse_date) ---
def _is_blank_token(v: str) -> bool:
    return isinstance(v, str) and v.strip().lower() == "(blank)"

def _eq_or_blank(qs, field: str, value: str):
    """
    If value == "(blank)", filter for NULL or '' in the given field.
    Otherwise, equality filter. If value falsy, return qs unchanged.
    """
    if not value:
        return qs
    if _is_blank_token(value):
        return qs.filter(Q(**{f"{field}__isnull": True}) | Q(**{field: ""}))
    return qs.filter(**{field: value})


# ---------- REPLACE: Hazardous Waste JSON for the dashboard ----------
@login_required
def effluent_dashboard_data_hw(request):
    """
    JSON for Hazardous Waste tab.
    Filters:
      - from, to (date range)
      - q (free text)
      - hw_transporter, hw_type, hw_method, hw_facility (dimension filters from clicks)
    Cards: quantity_mt (MT), transportation_cost (₹), total_cost (₹)
    Groups: transporter_name, type_of_waste, disposal_method, disposal_facility
    Detail: latest 500 rows
    """
    dfrom = _parse_date(request.GET.get("from"))
    dto   = _parse_date(request.GET.get("to"))
    q     = (request.GET.get("q") or "").strip()

    # NEW: dimension filters coming from clickable summaries
    f_transporter = (request.GET.get("hw_transporter") or "").strip()
    f_type        = (request.GET.get("hw_type") or "").strip()
    f_method      = (request.GET.get("hw_method") or "").strip()
    f_facility    = (request.GET.get("hw_facility") or "").strip()

    qs = HazardousWaste.objects.all()

    # Date filters
    if dfrom and dto:
        qs = qs.filter(date__range=[dfrom, dto])
    elif dfrom:
        qs = qs.filter(date__gte=dfrom)
    elif dto:
        qs = qs.filter(date__lte=dto)

    # Free search
    if q:
        qs = qs.filter(
            Q(challan_no__icontains=q) |
            Q(manifest_no__icontains=q) |
            Q(transporter_name__icontains=q) |
            Q(vehicle_registration_numbers__icontains=q) |
            Q(type_of_waste__icontains=q) |
            Q(waste_category__icontains=q) |
            Q(disposal_method__icontains=q) |
            Q(disposal_facility__icontains=q)
        )

    # APPLY CLICK FILTERS (these must narrow *all* summaries + detail)
    qs = _eq_or_blank(qs, "transporter_name",  f_transporter)
    qs = _eq_or_blank(qs, "type_of_waste",     f_type)
    qs = _eq_or_blank(qs, "disposal_method",   f_method)
    qs = _eq_or_blank(qs, "disposal_facility", f_facility)

    # KPIs
    agg = qs.aggregate(
        qty=Sum("quantity_mt"),
        tcost=Sum("transportation_cost"),
        tot=Sum("total_cost"),
    )

    # Summaries (reflect current filters because qs is already narrowed)
    def _group(values_key):
        data = (
            qs.values(values_key)
              .annotate(
                  qty=Sum("quantity_mt"),
                  tcost=Sum("transportation_cost"),
                  tot=Sum("total_cost"),
              )
              .order_by("-tot", values_key)
        )
        label_key = {
            "transporter_name": "transporter",
            "type_of_waste": "waste_type",
            "disposal_method": "method",
            "disposal_facility": "facility",
        }[values_key]
        out = []
        for r in data:
            label = r.get(values_key) or "(blank)"
            out.append({
                label_key: label,
                "qty": float(r.get("qty") or 0),
                "tcost": float(r.get("tcost") or 0),
                "tot": float(r.get("tot") or 0),
            })
        return out

    by_transporter = _group("transporter_name")
    by_type        = _group("type_of_waste")
    by_method      = _group("disposal_method")
    by_facility    = _group("disposal_facility")

    # Detail (latest 500)
    details_qs = (
        qs.order_by("-date", "-id")
          .values(
              "date",
              "challan_no",
              "manifest_no",
              "transporter_name",
              "vehicle_registration_numbers",
              "type_of_waste",
              "waste_category",
              "quantity_mt",
              "disposal_rate_rs_per_mt",
              "transportation_cost",
              "total_cost",
              "disposal_method",
              "disposal_facility",
              "license_valid_upto",
          )[:500]
    )
    details = []
    for r in details_qs:
        details.append({
            "date": r["date"].isoformat() if r["date"] else "",
            "challan_no": r["challan_no"] or "",
            "manifest_no": r["manifest_no"] or "",
            "transporter_name": r["transporter_name"] or "",
            "vehicle_registration_numbers": r["vehicle_registration_numbers"] or "",
            "type_of_waste": r["type_of_waste"] or "",
            "waste_category": r["waste_category"] or "",
            "quantity_mt": float(r["quantity_mt"] or 0),
            "disposal_rate_rs_per_mt": float(r["disposal_rate_rs_per_mt"] or 0),
            "transportation_cost": float(r["transportation_cost"] or 0),
            "total_cost": float(r["total_cost"] or 0),
            "disposal_method": r["disposal_method"] or "",
            "disposal_facility": r["disposal_facility"] or "",
            "license_valid_upto": r["license_valid_upto"].isoformat() if r["license_valid_upto"] else "",
        })

    return JsonResponse({
        "totals": {
            "quantity_mt": float(agg["qty"] or 0),
            "transportation_cost": float(agg["tcost"] or 0),
            "total_cost": float(agg["tot"] or 0),
        },
        "by_transporter": by_transporter,
        "by_type": by_type,
        "by_method": by_method,
        "by_facility": by_facility,
        "table": details,
    }, safe=False)


# ---------- REPLACE: Excel export for Hazardous Waste ----------
@login_required
def effluent_dashboard_export_hw(request):
    dfrom = _parse_date(request.GET.get("from"))
    dto   = _parse_date(request.GET.get("to"))
    q     = (request.GET.get("q") or "").strip()

    # same filters as JSON
    f_transporter = (request.GET.get("hw_transporter") or "").strip()
    f_type        = (request.GET.get("hw_type") or "").strip()
    f_method      = (request.GET.get("hw_method") or "").strip()
    f_facility    = (request.GET.get("hw_facility") or "").strip()

    qs = HazardousWaste.objects.all()

    if dfrom and dto:
        qs = qs.filter(date__range=[dfrom, dto])
    elif dfrom:
        qs = qs.filter(date__gte=dfrom)
    elif dto:
        qs = qs.filter(date__lte=dto)

    if q:
        qs = qs.filter(
            Q(challan_no__icontains=q) |
            Q(manifest_no__icontains=q) |
            Q(transporter_name__icontains=q) |
            Q(vehicle_registration_numbers__icontains=q) |
            Q(type_of_waste__icontains=q) |
            Q(waste_category__icontains=q) |
            Q(disposal_method__icontains=q) |
            Q(disposal_facility__icontains=q)
        )

    # APPLY CLICK FILTERS (export must match on-screen)
    qs = _eq_or_blank(qs, "transporter_name",  f_transporter)
    qs = _eq_or_blank(qs, "type_of_waste",     f_type)
    qs = _eq_or_blank(qs, "disposal_method",   f_method)
    qs = _eq_or_blank(qs, "disposal_facility", f_facility)

    rows = qs.order_by("-date", "-id").values(
        "date", "challan_no", "manifest_no", "transporter_name",
        "vehicle_registration_numbers", "type_of_waste", "waste_category",
        "quantity_mt", "disposal_rate_rs_per_mt", "transportation_cost",
        "total_cost", "disposal_method", "disposal_facility",
        "license_valid_upto"
    )[:50000]

    ts = now().strftime("%Y%m%d_%H%M%S")
    filename = f"Hazardous_Waste_{ts}.xlsx"

    output = io.BytesIO()
    wb = xlsxwriter.Workbook(output, {"in_memory": True})
    ws = wb.add_worksheet("Hazardous Waste")

    fmt_head = wb.add_format({"bold": True, "bg_color": "#E5F0FF", "border": 1, "align": "center"})
    fmt_text = wb.add_format({"border": 1})
    fmt_num  = wb.add_format({"border": 1, "num_format": "0.00"})
    fmt_date = wb.add_format({"border": 1, "num_format": "dd-mm-yyyy"})

    headers = [
        "Date","Challan","Manifest","Transporter","Vehicle Nos","Waste Type","Category",
        "Qty (MT)","Rate (₹/MT)","Transport (₹)","Total (₹)","Method","Facility","License Upto"
    ]
    for c,h in enumerate(headers): ws.write(0,c,h,fmt_head)

    r = 1
    for row in rows:
        d = row["date"]
        if d:
            ws.write_datetime(r,0, dt.combine(d, dtime.min), fmt_date)
        else:
            ws.write(r,0,"",fmt_text)
        ws.write(r,1, row["challan_no"] or "", fmt_text)
        ws.write(r,2, row["manifest_no"] or "", fmt_text)
        ws.write(r,3, row["transporter_name"] or "", fmt_text)
        ws.write(r,4, row["vehicle_registration_numbers"] or "", fmt_text)
        ws.write(r,5, row["type_of_waste"] or "", fmt_text)
        ws.write(r,6, row["waste_category"] or "", fmt_text)
        ws.write_number(r,7, float(row["quantity_mt"] or 0), fmt_num)
        ws.write_number(r,8, float(row["disposal_rate_rs_per_mt"] or 0), fmt_num)
        ws.write_number(r,9, float(row["transportation_cost"] or 0), fmt_num)
        ws.write_number(r,10, float(row["total_cost"] or 0), fmt_num)
        ws.write(r,11, row["disposal_method"] or "", fmt_text)
        ws.write(r,12, row["disposal_facility"] or "", fmt_text)
        ld = row["license_valid_upto"]
        if ld:
            ws.write_datetime(r,13, dt.combine(ld, dtime.min), fmt_date)
        else:
            ws.write(r,13,"",fmt_text)
        r += 1

    widths = [12,12,12,22,18,28,10,10,12,14,14,14,26,14]
    for i,w in enumerate(widths): ws.set_column(i,i,w)

    wb.close()
    output.seek(0)
    return HttpResponse(
        output.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )


# --- helper to parse int safely
def _parse_int(s):
    try:
        return int(str(s).strip())
    except Exception:
        return None


@login_required
def effluent_dashboard_data_tanks(request):
    from datetime import date as _date
    """
    JSON for 'Effluent Tanks' tab.

    Inputs (GET):
      from, to  -> YYYY-MM-DD
      tank      -> optional tank id (to focus the table)

    Output:
      cards: [{tank_id, tank, capacity, available}]   # 'available' = available_space on 'to' date
      table: [{tank_id, tank, date, opening, inlet, consume, closing, available_space, capacity}]
    """
    dfrom = _parse_date(request.GET.get("from"))
    dto   = _parse_date(request.GET.get("to"))
    tank_id = _parse_int(request.GET.get("tank"))

    # sane defaults (1st of month to today) if not supplied
    today = _date.today()
    if not dto:
        dto = today
    if not dfrom:
        dfrom = dto.replace(day=1)

    tank_ids = [tank_id] if tank_id else None
    report = build_effluent_report_range(dfrom, dto, tank_ids)

    # Cards: one per tank, 'available' is available_space on dto (or last computed date)
    cards = []
    for row in report["rows"]:
        cells = row["cells"]
        on_date = next((c for c in cells if c["date"] == dto), cells[-1] if cells else None)
        cards.append({
            "tank_id": row["tank"].id,
            "tank": row["tank"].name,
            "capacity": float(row["capacity"]),
            "available": float(on_date["available_space"] if on_date else 0.0),
        })

    # Detail table: all days in range
    table = []
    for row in report["rows"]:
        cap = float(row["capacity"])
        for c in row["cells"]:
            table.append({
                "tank_id": row["tank"].id,
                "tank": row["tank"].name,
                "date": c["date"].isoformat(),
                "opening": float(c["opening"]),
                "inlet": float(c["inlet"]),
                "consume": float(c["consume"]),
                "closing": float(c["closing"]),
                "available_space": float(c["available_space"]),
                "capacity": cap,
            })

    # sort table by date asc within tank
    table.sort(key=lambda r: (r["tank"].lower(), r["date"]))

    return JsonResponse({"cards": cards, "table": table}, safe=False)


@login_required
def effluent_dashboard_data_primary(request):
    from datetime import date as _date

    dfrom = _parse_date(request.GET.get("from"))
    dto   = _parse_date(request.GET.get("to"))
    today = _date.today()
    if not dto:
        dto = today
    if not dfrom:
        dfrom = dto.replace(day=1)

    # optional filter coming from UI pill
    nature_filter = (request.GET.get("pte_nature") or request.GET.get("nature") or "").strip()

    qs = PrimaryTreatmentEffluent.objects.filter(date__range=[dfrom, dto])

    totals = qs.aggregate(
        received=Sum("effluent_received"),
        neutralized=Sum("effluent_neutralized"),
    )

    by_nature = list(
        qs.values("effluent_nature")
          .annotate(
              received=Sum("effluent_received"),
              neutralized=Sum("effluent_neutralized"),
          )
          .order_by("effluent_nature")
    )

    # chemicals, narrowed by selected effluent nature if provided
    chem_qs = PrimaryTreatmentChemical.objects.filter(
        effluent_record__date__range=[dfrom, dto]
    )
    if nature_filter:
        chem_qs = chem_qs.filter(effluent_record__effluent_nature=nature_filter)

    by_chemical = list(
        chem_qs.values("chemical_name")
               .annotate(qty=Sum("quantity"))
               .order_by("chemical_name")
    )

    def f(x): return float(x or 0)

    payload = {
        "totals": {
            "received": f(totals["received"]),
            "neutralized": f(totals["neutralized"]),
        },
        "selected_nature": nature_filter,  # (optional, handy on the front-end)
        "by_nature": [
            {
                "effluent_nature": r["effluent_nature"],
                "received": f(r["received"]),
                "neutralized": f(r["neutralized"]),
            } for r in by_nature
        ],
        "by_chemical": [
            {"chemical_name": r["chemical_name"], "qty": f(r["qty"])}
            for r in by_chemical
        ],
    }
    return JsonResponse(payload)


def _parse_iso_date(s: str):
    """Parse 'YYYY-MM-DD' to date or return None."""
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def _get_mee_sub(cat_name: str, sub_name: str):
    """Get MEE subcategory id for given category & subcategory names (case-insensitive)."""
    try:
        sub = MEEReadingSubCategory.objects.get(
            category__name__iexact=cat_name,
            name__iexact=sub_name,
        )
        return sub.id
    except MEEReadingSubCategory.DoesNotExist:
        return None


@login_required
def effluent_dashboard_data_mee(request):
    """
    JSON for MEE tab.

    Payload:
      totals: {
        effluent_feed_stripper: float avg per day,
        steam_consume_total:    float avg per day,
        steam_economy_plant:    float avg per day,
        total_downtime:         float sum (hrs over all days)
      }
      by_date: [
        {
          date: "YYYY-MM-DD",
          effluent_feed_stripper: float,
          steam_consume_total:    float,
          steam_economy_plant:    float,
          total_downtime:         float
        },
        ...
      ]
    """

    # ---- date range ----
    from_str = request.GET.get("from") or ""
    to_str   = request.GET.get("to") or ""

    today = date.today()
    default_from = today.replace(day=1)
    default_to = today

    from_date = _parse_iso_date(from_str) or default_from
    to_date   = _parse_iso_date(to_str) or default_to

    # ---- subcategory IDs ----
    eff_feed_strip_id   = _get_mee_sub("Effluent Feed", "Stripper")
    steam_cons_total_id = _get_mee_sub("Steam Consume", "Total")
    # fallback: if TOTAL not configured, use Stripper so at least some value shows
    if not steam_cons_total_id:
        steam_cons_total_id = _get_mee_sub("Steam Consume", "Stripper")

    steam_eco_plant_id  = _get_mee_sub("Steam Economy", "Plant")

    logger.info(
        "MEE subs: eff_strip=%s, steam_total=%s, steam_plant=%s",
        eff_feed_strip_id, steam_cons_total_id, steam_eco_plant_id,
    )

    sub_ids = [
        sid for sid in [eff_feed_strip_id, steam_cons_total_id, steam_eco_plant_id]
        if sid
    ]
    if not sub_ids:
        # Nothing configured – return empty payload
        return JsonResponse(
            {
                "totals": {
                    "effluent_feed_stripper": 0.0,
                    "steam_consume_total": 0.0,
                    "steam_economy_plant": 0.0,
                    "total_downtime": 0.0,
                },
                "by_date": [],
            }
        )

    # ---- base structure per date ----
    rows_by_date = defaultdict(
        lambda: {
            "effluent_feed_stripper": 0.0,
            "steam_consume_total": 0.0,
            "steam_economy_plant": 0.0,
            "total_downtime": 0.0,
        }
    )

    # ---- fetch readings (numerical KPIs) ----
    qs = (
        MEEDailyReading.objects
        .filter(
            reading_date__gte=from_date,
            reading_date__lte=to_date,
            subcategory_id__in=sub_ids,
        )
        .select_related("subcategory")
        .order_by("-reading_date")
    )

    for r in qs:
        try:
            v = float(r.value or 0)
        except (TypeError, ValueError):
            v = 0.0

        d = r.reading_date
        row = rows_by_date[d]

        if r.subcategory_id == eff_feed_strip_id:
            row["effluent_feed_stripper"] += v
        elif r.subcategory_id == steam_cons_total_id:
            row["steam_consume_total"] += v
        elif r.subcategory_id == steam_eco_plant_id:
            row["steam_economy_plant"] += v

    # ---- fetch downtimes and sum hours per date ----
    dt_qs = (
        MEEDowntime.objects
        .select_related("reading")
        .filter(
            reading__reading_date__gte=from_date,
            reading__reading_date__lte=to_date,
        )
        .order_by("reading__reading_date", "downtime_start")
    )

    for dt in dt_qs:
        d = dt.reading.reading_date
        row = rows_by_date[d]

        # be defensive in case some old rows have null downtime_hours
        if dt.downtime_hours is not None:
            try:
                hrs = float(dt.downtime_hours)
            except (TypeError, ValueError):
                hrs = 0.0
        else:
            # fallback compute
            dur = dt._compute_duration()
            hrs = float(dur) if dur is not None else 0.0

        row["total_downtime"] += hrs

    # ---- compute totals (averages for KPIs, sum for downtime) ----
    eff_vals   = []
    steam_vals = []
    se_vals    = []
    total_dt   = 0.0

    for row in rows_by_date.values():
        eff_vals.append(float(row["effluent_feed_stripper"] or 0.0))
        steam_vals.append(float(row["steam_consume_total"] or 0.0))
        se_vals.append(float(row["steam_economy_plant"] or 0.0))
        total_dt += float(row["total_downtime"] or 0.0)

    totals = {
        "effluent_feed_stripper": sum(eff_vals) / len(eff_vals) if eff_vals else 0.0,
        "steam_consume_total":    sum(steam_vals) / len(steam_vals) if steam_vals else 0.0,
        "steam_economy_plant":    sum(se_vals)   / len(se_vals)   if se_vals   else 0.0,
        "total_downtime":         total_dt,
    }

    # ---- build by_date list ----
    by_date = []
    for d in sorted(rows_by_date.keys()):
        row = rows_by_date[d]
        by_date.append(
            {
                "date": d.strftime("%Y-%m-%d"),
                "effluent_feed_stripper": row["effluent_feed_stripper"],
                "steam_consume_total":    row["steam_consume_total"],
                "steam_economy_plant":    row["steam_economy_plant"],
                "total_downtime":         row["total_downtime"],
            }
        )

    return JsonResponse({"totals": totals, "by_date": by_date})





def effluent_dashboard_data_atfd(request):
    # ✅ IMPORTANT: don't redirect to login page for fetch() calls
    if not request.user.is_authenticated:
        return JsonResponse({"detail": "Authentication required"}, status=401)

    from_str = request.GET.get("from") or ""
    to_str   = request.GET.get("to") or ""

    today = date.today()
    default_from = today.replace(day=1)
    default_to   = today

    from_date = _parse_iso_date(from_str) or default_from
    to_date   = _parse_iso_date(to_str) or default_to

    downtime_field = DecimalField(max_digits=12, decimal_places=2)

    qs = (
        ATFDReading.objects
        .filter(reading_date__gte=from_date, reading_date__lte=to_date)
        .annotate(
            total_downtime=Coalesce(
                Sum("downtimes__downtime_hours"),
                Value(0),
                output_field=downtime_field,
            )
        )
        .order_by("-reading_date")
    )

    # ✅ averages should not get affected by downtime joins
    agg = qs.aggregate(
        avg_effluent_feed=Avg("effluent_feed", distinct=True),
        avg_steam_consume=Avg("steam_consume", distinct=True),
        avg_steam_economy=Avg("steam_economy", distinct=True),
        total_downtime=Coalesce(
            Sum("downtimes__downtime_hours"),
            Value(0),
            output_field=downtime_field,
        ),
    )

    def f(x): return float(x or 0)

    by_date = [{
        "date": r.reading_date.strftime("%Y-%m-%d") if r.reading_date else "",
        "effluent_feed": f(r.effluent_feed),
        "steam_consume": f(r.steam_consume),
        "total_downtime": f(r.total_downtime),
    } for r in qs]

    return JsonResponse({
        "totals": {
            "avg_effluent_feed": f(agg["avg_effluent_feed"]),
            "avg_steam_consume": f(agg["avg_steam_consume"]),
            "avg_steam_economy": f(agg["avg_steam_economy"]),
            "total_downtime": f(agg["total_downtime"]),
        },
        "by_date": by_date,
    })


@login_required
def effluent_dashboard_data_mass_balance(request):
    from_str = request.GET.get("from") or ""
    to_str   = request.GET.get("to") or ""

    today = date.today()
    default_from = today.replace(day=1)
    default_to   = today

    from_date = _parse_iso_date(from_str) or default_from
    to_date   = _parse_iso_date(to_str) or default_to

    eff_natures = ["Basic", "Acidic Aq. Layer", "Sodium Cyanide Effluent"]

    pte_qs = (
        PrimaryTreatmentEffluent.objects
        .filter(
            date__gte=from_date,
            date__lte=to_date,
            effluent_nature__in=eff_natures,
        )
        .values("date")
        .annotate(
            received=Sum("effluent_received"),
            neutralized=Sum("effluent_neutralized"),
        )
        .order_by("date")
    )
    pte_map = {r["date"]: r for r in pte_qs}

    eff_strip_id = _get_mee_sub("Effluent Feed", "Stripper")
    mee_map = {}
    if eff_strip_id:
        mee_qs = (
            MEEDailyReading.objects
            .filter(
                reading_date__gte=from_date,
                reading_date__lte=to_date,
                subcategory_id=eff_strip_id,
            )
            .values("reading_date")
            .annotate(feed=Sum("value"))
            .order_by("reading_date")
        )
        mee_map = {r["reading_date"]: r for r in mee_qs}

    atfd_qs = (
        ATFDReading.objects
        .filter(reading_date__gte=from_date, reading_date__lte=to_date)
        .values("reading_date")
        .annotate(feed=Sum("effluent_feed"))
        .order_by("reading_date")
    )
    atfd_map = {r["reading_date"]: r for r in atfd_qs}

    all_dates = set()
    all_dates.update(pte_map.keys())
    all_dates.update(mee_map.keys())
    all_dates.update(atfd_map.keys())

    if not all_dates:
        return JsonResponse({
            "totals": {
                "avg_effluent_received": 0.0,
                "avg_effluent_neutralized": 0.0,
                "avg_effluent_treated": 0.0,
                "avg_effluent_deviation": 0.0,
            },
            "grand_totals": {
                "effluent_received": 0.0,
                "effluent_neutralized": 0.0,
                "stripper_feed": 0.0,
                "atfd_feed": 0.0,
                "total_feed": 0.0,
                "effluent_deviation": 0.0,
            },
            "by_date": [],
        })

    rows = []
    rec_vals, neu_vals, treat_vals, dev_vals = [], [], [], []

    # ✅ NEW: grand totals (sum)
    sum_rec = sum_neu = sum_strip = sum_atfd = sum_total = sum_dev = 0.0

    for d in sorted(all_dates):
        rec   = float(pte_map.get(d, {}).get("received") or 0)
        neu   = float(pte_map.get(d, {}).get("neutralized") or 0)
        strip = float(mee_map.get(d, {}).get("feed") or 0)
        atfd  = float(atfd_map.get(d, {}).get("feed") or 0)

        total_feed = strip + atfd
        deviation  = total_feed - rec

        rec_vals.append(rec)
        neu_vals.append(neu)
        treat_vals.append(total_feed)
        dev_vals.append(deviation)

        # ✅ accumulate totals
        sum_rec   += rec
        sum_neu   += neu
        sum_strip += strip
        sum_atfd  += atfd
        sum_total += total_feed
        sum_dev   += deviation

        rows.append({
            "date": d.strftime("%Y-%m-%d"),
            "effluent_received": rec,
            "effluent_neutralized": neu,
            "stripper_feed": strip,
            "atfd_feed": atfd,
            "total_feed": total_feed,
            "effluent_deviation": deviation,
        })

    def _avg(values):
        return sum(values) / len(values) if values else 0.0

    totals = {
        "avg_effluent_received": _avg(rec_vals),
        "avg_effluent_neutralized": _avg(neu_vals),
        "avg_effluent_treated": _avg(treat_vals),
        "avg_effluent_deviation": _avg(dev_vals),
    }

    # ✅ NEW: totals row data
    grand_totals = {
        "effluent_received": sum_rec,
        "effluent_neutralized": sum_neu,
        "stripper_feed": sum_strip,
        "atfd_feed": sum_atfd,
        "total_feed": sum_total,
        "effluent_deviation": sum_dev,
    }

    return JsonResponse({
        "totals": totals,
        "grand_totals": grand_totals,  # ✅ send to UI
        "by_date": rows
    })



# ---------------------------------------------------------------------------


@login_required
def api_transporter_vehicles(request):
    """Return vehicle list for the given transporter name."""
    try:
        name = (request.GET.get("name") or "").strip()
        logger.debug(f"🚚 API:transporter_vehicles — user={request.user.username} name='{name}' raw_params={request.GET.dict()}")
        vehicles = TRANSPORTER_VEHICLES.get(name, [])
        found = bool(vehicles)
        if "Other" not in vehicles:
            vehicles = [*vehicles, "Other"] if vehicles else ["Other"]
        logger.info(
            "✅ API:transporter_vehicles — "
            f"name='{name}' found={found} count={len(vehicles)} vehicles_sample={vehicles[:3]}" )
        return JsonResponse({"transporter": name, "vehicles": vehicles})
    except Exception:
        logger.exception(f"💥 API:transporter_vehicles failed user={request.user.username}")
        return JsonResponse({"error": "Internal error"}, status=500)


@login_required
def api_disposal_rates(request):
    """
    GET params: facility, waste, qty
    Returns: {rate: float|None, transport: float|None}
    """
    try:
        fac   = (request.GET.get("facility") or "").strip()
        waste = (request.GET.get("waste") or "").strip()
        qty_s = (request.GET.get("qty") or "").strip()

        qty = None
        try:
            qty = float(qty_s) if qty_s != "" else None
        except Exception:
            qty = None

        logger.debug(
            "🏭 API:disposal_rates — "
            f"user={request.user.username} facility='{fac}' waste='{waste}' qty='{qty_s}' raw_params={request.GET.dict()}"
        )

        rate = transport = None
        matched = False

        if fac in FACILITY_WASTE_RATES:
            row = FACILITY_WASTE_RATES[fac].get(waste)
            if row:
                matched = True
                rate = row.get("rate")
                transport = row.get("transport")

        # ✅ NEW RULE: Only for MEPL facility, transport depends on qty
        if fac == MEPL_FACILITY and qty is not None:
            transport = MEPL_TRANSPORT_GT_15 if qty > MEPL_QTY_THRESHOLD else MEPL_TRANSPORT_LT_EQ_15

        if matched:
            logger.info(
                "✅ API:disposal_rates — "
                f"facility='{fac}' waste='{waste}' qty={qty} rate={rate} transport={transport}"
            )
        else:
            logger.warning(
                "⚠️ API:disposal_rates — no exact match "
                f"facility='{fac}' waste='{waste}' qty={qty}"
            )

        return JsonResponse({"facility": fac, "waste": waste, "rate": rate, "transport": transport})

    except Exception:
        logger.exception(f"💥 API:disposal_rates failed user={request.user.username}")
        return JsonResponse({"error": "Internal error"}, status=500)


@login_required
def hazardous_waste_create(request):
    # (Optional) mirror context you used in add_effluent_record
    user_groups = request.user.groups.values_list('name', flat=True)
    is_superuser = request.user.is_superuser

    if not request.user.has_perm("ETP.add_hazardouswaste"):
        logger.warning(f"Unauthorized add attempt by {request.user.username}")
        messages.error(request, "You do not have permission to add Hazardous Waste records.")
        return redirect("indexpage")

    if request.method == "POST":
        logger.debug("🔄 Received POST request")
        # Safe to log keys/values if they don’t include sensitive data
        logger.debug(f"📦 POST Data: {request.POST.dict()}")

        form = HazardousWasteForm(request.POST)

        if form.is_valid():
            logger.debug("✅ HazardousWasteForm is valid")
        else:
            logger.error(f"❌ HazardousWasteForm errors: {form.errors.as_json()}")

        if form.is_valid():
            try:
                with transaction.atomic():
                    obj = form.save()
                logger.info("✅ Hazardous Waste record saved successfully.")
                logger.debug(
                    f"🆔 Saved object details: user={request.user.username} obj_id={getattr(obj, 'id', None)}"
                )
                messages.success(request, "Hazardous Waste record created.")
                return redirect("hazardous_waste_list")
            except Exception:
                logger.exception(
                    f"💥 Save failed for user={request.user.username} — transaction rolled back"
                )
                messages.error(request, "Could not save record due to an internal error.")
        else:
            logger.warning("⚠️ Form validation failed. Rendering form with errors.")
            messages.error(request, "Please correct the errors below.")
    else:
        logger.debug("📥 GET request — rendering empty HazardousWasteForm")
        form = HazardousWasteForm()

    return render(request,"etp/hazardous_waste_form.html",{"form": form,"user_groups": user_groups,"is_superuser": is_superuser,},)



# ──────────────────────────────────────────────────────────────
# Internal helper to apply the same filters for both endpoints
# ──────────────────────────────────────────────────────────────
def _filtered_hw_queryset(request):
    qs = HazardousWaste.objects.all().order_by("-date", "-id")

    from_date   = (request.GET.get("from_date") or "").strip()
    to_date     = (request.GET.get("to_date") or "").strip()
    challan     = (request.GET.get("challan_no") or "").strip()
    transporter = (request.GET.get("transporter_name") or "").strip()
    manifest    = (request.GET.get("manifest_no") or "").strip()
    facility    = (request.GET.get("disposal_facility") or "").strip()
    method      = (request.GET.get("disposal_method") or "").strip()

    # Raw filter snapshot
    logger.debug("🔎 HW:list — incoming filters "
                 f"from_date={from_date} to_date={to_date} challan_no={challan} "
                 f"transporter={transporter} manifest_no={manifest} "
                 f"facility={facility} method={method}")

    # Default to current month if nothing chosen
    if not (from_date or to_date or challan or transporter or manifest or facility or method):
        today = date.today()
        from_date = today.replace(day=1).isoformat()
        to_date = today.isoformat()
        logger.debug(f"📅 HW:list — no filters; defaulting to current month window {from_date}..{to_date}")

    # Apply filters
    if from_date:
        d = parse_date(from_date)
        if d:
            qs = qs.filter(date__gte=d)
        else:
            logger.error(f"❌ HW:list — invalid from_date={from_date}")
    if to_date:
        d = parse_date(to_date)
        if d:
            qs = qs.filter(date__lte=d)
        else:
            logger.error(f"❌ HW:list — invalid to_date={to_date}")
    if challan:
        qs = qs.filter(challan_no__icontains=challan)
    if transporter:
        qs = qs.filter(transporter_name__icontains=transporter)
    if manifest:
        qs = qs.filter(manifest_no__icontains=manifest)
    if facility:
        qs = qs.filter(disposal_facility__icontains=facility)
    if method:
        qs = qs.filter(disposal_method=method)

    # Snapshot of result size (DB hit)
    count = qs.count()
    logger.debug(f"📊 HW:list — queryset size after filters: {count}")

    # Build totals on filtered set
    totals = qs.aggregate(
        total_qty=Sum("quantity_mt"),
        total_transport=Sum("transportation_cost"),
        total_amount=Sum("total_cost"),
    )
    totals_ctx = {
        "qty": round(float(totals["total_qty"] or 0), 3),
        "transport": round(float(totals["total_transport"] or 0), 2),
        "amount": round(float(totals["total_amount"] or 0), 2),
    }

    logger.info("✅ HW:list — totals "
                f"count={count} qty={totals_ctx['qty']} transport={totals_ctx['transport']} "
                f"amount={totals_ctx['amount']} window={from_date}..{to_date} "
                f"method={method or '-'}")

    filters_ctx = {
        "from_date": from_date,
        "to_date": to_date,
        "challan_no": challan,
        "transporter_name": transporter,
        "manifest_no": manifest,
        "disposal_facility": facility,
        "disposal_method": method,
    }

    return qs, filters_ctx, totals_ctx


# ──────────────────────────────────────────────────────────────
# LIST VIEW (only listing + pagination)
# ──────────────────────────────────────────────────────────────
@login_required
def hazardous_waste_list(request):
    if not request.user.has_perm("ETP.view_hazardouswaste"):
        logger.warning(f"Unauthorized list attempt by {request.user.username}")
        messages.error(request, "You do not have permission to view Hazardous Waste records.")
        return redirect("indexpage")

    qs, filters_ctx, totals_ctx = _filtered_hw_queryset(request)

    paginator = Paginator(qs, 25)
    page_param = request.GET.get("page")
    items = paginator.get_page(page_param)

    logger.debug("📄 HW:list — pagination "
                 f"page_req={page_param or 1} per_page=25 total_items={paginator.count} "
                 f"total_pages={paginator.num_pages} page_obj_count={len(items.object_list)}")

    method_choices = HazardousWaste._meta.get_field("disposal_method").choices

    ctx = {
        "items": items,
        "method_choices": method_choices,
        "totals": totals_ctx,
        **filters_ctx,
    }
    return render(request, "etp/hazardous_waste_list.html", ctx)

# ──────────────────────────────────────────────────────────────
# EXCEL EXPORT (xlsxwriter) – only downloads file
# ──────────────────────────────────────────────────────────────
@login_required
def hazardous_waste_export_xlsx(request):
    if not request.user.has_perm("ETP.view_hazardouswaste"):
        logger.warning(f"Unauthorized export attempt by {request.user.username}")
        messages.error(request, "You do not have permission to export Hazardous Waste records.")
        return redirect("indexpage")

    try:
        logger.debug("🧾 HW:export — building filtered queryset")
        qs, filters_ctx, totals_ctx = _filtered_hw_queryset(request)

        # Snapshot filters + totals + size
        count = qs.count()
        from_s = filters_ctx.get("from_date") or "all"
        to_s   = filters_ctx.get("to_date") or "all"
        logger.info(
            "✅ HW:export — ready "
            f"user={request.user.username} count={count} "
            f"qty={totals_ctx['qty']} transport={totals_ctx['transport']} amount={totals_ctx['amount']} "
            f"window={from_s}..{to_s}"
        )

        output = BytesIO()
        wb = xlsxwriter.Workbook(output, {"in_memory": True})
        ws = wb.add_worksheet("HazardousWaste")

        # ──────────────────────────────
        # Formatting styles
        # ──────────────────────────────
        title_fmt = wb.add_format({"bold": True, "font_size": 16, "align": "center", "valign": "vcenter"})
        head_fmt  = wb.add_format({"bold": True, "border": 1, "align": "center", "bg_color": "#D9E1F2"})
        num3      = wb.add_format({"num_format": "0.000", "border": 1})
        money2    = wb.add_format({"num_format": "0.00", "border": 1})
        text      = wb.add_format({"border": 1})
        datefmt   = wb.add_format({"num_format": "dd/mm/yyyy", "border": 1})

        # ──────────────────────────────
        # Title
        # ──────────────────────────────
        title = "Hazardous Waste Transport and Disposal Detail"
        ws.merge_range("A1:L1", title, title_fmt)
        ws.set_row(0, 25)

        # ──────────────────────────────
        # Headers
        # ──────────────────────────────
        headers = [
            "Date", "DC Challan No", "Manifest No", "Transporter",
            "Vehicle Registration", "Type of Waste", "Waste Category", "Disposal Method",
            "Disposal Facility", "Qty (MT)", "Disposal Rate (Rs/MT)",
            "Transportation Cost", "Total Cost",
        ]
        header_row = 2
        for c, h in enumerate(headers):
            ws.write(header_row, c, h, head_fmt)

        # ──────────────────────────────
        # Data rows
        # ──────────────────────────────
        row = header_row + 1
        written = 0
        for r in qs:
            ws.write_datetime(row, 0, r.date, datefmt)
            ws.write(row, 1,  r.challan_no or "", text)
            ws.write(row, 2,  r.manifest_no or "", text)
            ws.write(row, 3,  r.transporter_name or "", text)
            ws.write(row, 4,  r.vehicle_registration_numbers or "", text)
            ws.write(row, 5,  r.type_of_waste or "", text)
            ws.write(row, 6,  r.waste_category or "", text)
            ws.write(row, 7,  r.disposal_method or "", text)
            ws.write(row, 8,  r.disposal_facility or "", text)
            ws.write_number(row, 9,  float(r.quantity_mt or 0), num3)
            ws.write_number(row, 10, float(r.disposal_rate_rs_per_mt or 0), money2)
            ws.write_number(row, 11, float(r.transportation_cost or 0),    money2)
            ws.write_number(row, 12, float(r.total_cost or 0),             money2)
            row += 1
            written += 1

        logger.debug(f"🧮 HW:export — wrote {written} data rows")

        # ──────────────────────────────
        # Totals row
        # ──────────────────────────────
        ws.write(row, 8, "Totals:", head_fmt)
        ws.write_number(row, 9,  totals_ctx["qty"],       num3)
        ws.write_blank(row, 10, None,                     head_fmt)
        ws.write_number(row, 11, totals_ctx["transport"], money2)
        ws.write_number(row, 12, totals_ctx["amount"],    money2)

        # ──────────────────────────────
        # Column widths
        # ──────────────────────────────
        widths = [12, 14, 14, 28, 22, 30, 16, 20, 28, 10, 16, 18, 14]  # 13 cols (A..M)
        for i, w in enumerate(widths):
            ws.set_column(i, i, w)

        ws.freeze_panes(header_row + 1, 0)

        wb.close()
        output.seek(0)

        # ──────────────────────────────
        # Response
        # ──────────────────────────────
        filename = f"HazardousWaste_{from_s}_{to_s}.xlsx"
        logger.info(f"📤 HW:export — sending file filename={filename} bytes={output.getbuffer().nbytes}")

        resp = HttpResponse(
            output.read(),
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        resp["Content-Disposition"] = f'attachment; filename="{filename}"'
        return resp

    except Exception:
        logger.exception(f"💥 HW:export — failed for user={request.user.username}")
        messages.error(request, "Could not export records due to an internal error.")
        return redirect("hazardous_waste_list")



@login_required
def hazardous_waste_edit(request, pk):
    obj = get_object_or_404(HazardousWaste, pk=pk)

    # Permission to view (same behavior as your original)
    if not request.user.has_perm("ETP.view_hazardouswaste"):
        logger.warning(f"Unauthorized edit view attempt by {request.user.username} obj_id={obj.id}")
        messages.error(request, "You do not have permission to view Hazardous Waste records.")
        return redirect("indexpage")

    if request.method == "POST":
        logger.debug(f"🔄 HW:edit — POST received by {request.user.username} obj_id={obj.id}")
        logger.debug(f"📦 POST Data: {request.POST.dict()}")

        form = HazardousWasteForm(request.POST, instance=obj)

        if form.is_valid():
            logger.debug("✅ HazardousWasteForm (edit) is valid")
            try:
                with transaction.atomic():
                    form.save()
                logger.info(f"✅ HW:edit success user={request.user.username} obj_id={obj.id}")
                messages.success(request, "Hazardous Waste record updated.")
                return redirect("hazardous_waste_list")
            except Exception:
                logger.exception(f"💥 HW:edit save failed user={request.user.username} obj_id={obj.id}")
                messages.error(request, "Could not save record due to an internal error.")
        else:
            logger.error(f"❌ HazardousWasteForm (edit) errors: {form.errors.as_json()}")
            messages.error(request, "Please correct the errors below.")
    else:
        logger.debug(f"📥 HW:edit — GET render by {request.user.username} obj_id={obj.id}")
        form = HazardousWasteForm(instance=obj)

    return render(request, "etp/hazardous_waste_form.html", {"form": form, "obj": obj})


@login_required
@require_POST
def hazardous_waste_delete(request, pk: int):
    """
    POST-only delete with permission check.
    """
    if not request.user.has_perm("ETP.delete_hazardouswaste"):
        logger.warning(f"Unauthorized delete attempt by {request.user.username} obj_id={pk}")
        messages.error(request, "You do not have permission to delete Hazardous Waste records.")
        return redirect("hazardous_waste_list")

    logger.debug(f"🗑️ HW:delete — POST received by {request.user.username} obj_id={pk}")

    obj = get_object_or_404(HazardousWaste, pk=pk)

    try:
        # keep a tiny snapshot for audit before deletion
        snapshot = {
            "id": obj.id,
            "date": getattr(obj, "date", None),
            "challan_no": getattr(obj, "challan_no", None),
            "manifest_no": getattr(obj, "manifest_no", None),
            "qty_mt": float(getattr(obj, "quantity_mt", 0) or 0),
        }
        obj.delete()
        logger.info(f"✅ HW:delete success user={request.user.username} snapshot={snapshot}")
        messages.success(request, "Record deleted successfully.")
    except Exception:
        logger.exception(f"💥 HW:delete failed user={request.user.username} obj_id={pk}")
        messages.error(request, "Could not delete the record due to an internal error.")

    return redirect("hazardous_waste_list")






# ====================================================================================================    
# ───────────────────────────────────────────────────────────────────
# Below code is for Effluent storage tank
# ───────────────────────────────────────────────────────────────────


def _parse_month(qs_val: str | None):
    if not qs_val:
        return None
    for fmt in ("%Y-%m", "%Y-%m-%d"):
        try:
            return datetime.strptime(qs_val, fmt).date().replace(day=1)
        except Exception:
            continue
    return None


@login_required
def opening_balance_form(request):
    if request.method == "POST":
        form = OpeningBalanceBulkForm(request.POST)
        if form.is_valid():
            month = form.save()
            messages.success(request, f"Opening balances saved for {month:%b %Y}.")
            return redirect(f"{reverse('opening_balance_form')}?month={month:%Y-%m}")
        messages.error(request, "Please fix the errors below.")
    else:
        form = OpeningBalanceBulkForm()
        # Default month = current month 1st
        default_month = date.today().replace(day=1)
        selected_month = _parse_month(request.GET.get("month")) or default_month

        # Set the month input's initial value
        form.fields["month"].initial = selected_month

        # Prefill each tank field if a record exists for that month
        for tank in EffluentTank.objects.all().order_by("name"):
            ob = EffluentOpeningBalance.objects.filter(
                tank=tank, month=selected_month
            ).first()
            if ob:
                form.fields[f"tank_{tank.id}"].initial = ob.opening_balance

    return render(request, "etp/opening_balance_form.html", {"form": form})



def _month_bounds_today():
    today = date.today()
    first = today.replace(day=1)
    if first.month == 12:
        last = first.replace(year=first.year + 1, month=1) - (first - first)  # temp
        last = last.replace(day=1) - (date(1,1,2) - date(1,1,1))  # simpler below
    # simpler, portable:
    if first.month == 12:
        last = date(first.year, 12, 31)
    else:
        last = first.replace(month=first.month + 1, day=1) - (date(2000,1,2) - date(2000,1,1))
    return first, last

@login_required
def effluent_tank_report(request):
    # Defaults: current month 1st → last day; all tanks
    m_first, m_last = _month_bounds_today()

    def _parse_ymd(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None

    from_s = request.GET.get("from")
    to_s   = request.GET.get("to")
    tank_s = request.GET.get("tank")    # tank id or empty

    fr = _parse_ymd(from_s) or m_first
    to = _parse_ymd(to_s)   or m_last

    tank_ids = None
    selected_tank = None
    if tank_s:
        try:
            selected_tank = EffluentTank.objects.get(pk=int(tank_s))
            tank_ids = [selected_tank.id]
        except Exception:
            selected_tank = None
            tank_ids = None

    ctx = build_effluent_report_range(fr, to, tank_ids)
    ctx.update({
        "filters": {
            "from": fr, "to": to,
            "tank": selected_tank.id if selected_tank else "",
            "tanks": EffluentTank.objects.all().order_by("name"),
        },
        "show_admin_panel": True,
    })
    return render(request, "ETP/etp_storage_tank_report.html", ctx)