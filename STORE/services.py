# STORE/services.py
from datetime import date
from decimal import Decimal
from typing import Iterable, Optional

from django.conf import settings
from django.db import connections, transaction

from .models import GrnLineCache, IssueLineCache, RackAllocation, RackIssue

# ────────────────────────────────────────────────
# Rack-to-Rack transfer (allocation move)
# ────────────────────────────────────────────────
from decimal import Decimal
from typing import Optional

from django.db import transaction
from django.db.models import F, Sum

from .models import Rack, Pallet, RackAllocation

class TransferError(Exception):
    pass


@transaction.atomic
def transfer_allocations(
    *,
    item_code: str,
    batch_no: str | None,
    from_rack_code: str,
    to_rack_code: str,
    qty: Decimal,
    from_pallet_number: Optional[str] = None,
    to_pallet_number: Optional[str] = None,
) -> list[dict]:
    """
    Move 'qty' (<= available balance) of allocations for item_code[/batch]
    from one rack/pallet to another, FIFO by GRN date then allocation time.

    Returns a list of transfer fragments:
      [{"grn_id": <id>, "moved": Decimal("...")}]

    Raises TransferError on validation problems.
    """
    if qty is None or Decimal(qty) <= 0:
        raise TransferError("Quantity must be greater than zero.")

    try:
        src_rack = Rack.objects.get(code=from_rack_code, is_active=True)
    except Rack.DoesNotExist:
        raise TransferError(f"Source rack '{from_rack_code}' not found or inactive.")

    try:
        dst_rack = Rack.objects.get(code=to_rack_code, is_active=True)
    except Rack.DoesNotExist:
        raise TransferError(f"Destination rack '{to_rack_code}' not found or inactive.")

    src_pallet = None
    if from_pallet_number:
        try:
            src_pallet = Pallet.objects.get(rack=src_rack, number=from_pallet_number, is_active=True)
        except Pallet.DoesNotExist:
            raise TransferError(f"Source pallet '{from_pallet_number}' not found on rack {from_rack_code}.")

    # Destination pallet: create on demand if named but not found (non-breaking)
    dst_pallet = None
    if to_pallet_number:
        dst_pallet, _ = Pallet.objects.get_or_create(
            rack=dst_rack, number=to_pallet_number, defaults={"is_active": True}
        )

    # Collect source allocations (FIFO) — and lock for update to avoid races
    q = (
        RackAllocation.objects
        .select_for_update()
        .filter(rack=src_rack, grn__item_code=item_code)
        .select_related("grn", "rack", "pallet")
        .order_by("grn__doc_date", "created_at", "pk")
    )
    if batch_no:
        q = q.filter(grn__batch_no=batch_no)
    if src_pallet:
        q = q.filter(pallet=src_pallet)

    # Check total available balance on the source
    total_available = Decimal("0.000")
    for a in q:
        bal = a.balance_qty
        if bal > 0:
            total_available += bal

    if total_available <= 0:
        raise TransferError("No available balance on source.")
    if Decimal(qty) - total_available > Decimal("1e-9"):
        raise TransferError(f"Insufficient balance. Available: {total_available:.3f}")

    need = Decimal(qty)
    fragments: list[dict] = []

    for alloc in q:
        if need <= 0:
            break
        bal = alloc.balance_qty
        if bal <= 0:
            continue

        take = bal if bal <= need else need

        # Destination allocation for the *same* GRN line at dest rack/pallet
        dst_alloc, _ = RackAllocation.objects.get_or_create(
            grn=alloc.grn, rack=dst_rack, pallet=dst_pallet,
            defaults={"allocated_qty": Decimal("0.000")},
        )

        # Reduce source allocated_qty by 'take'
        alloc.allocated_qty = (alloc.allocated_qty - take).quantize(Decimal("0.001"))
        if alloc.allocated_qty < 0:
            # Should not happen because we cap by balance, but guard anyway
            raise TransferError("Computed negative allocated quantity on source.")
        alloc.save(update_fields=["allocated_qty"])

        # Increase destination allocated_qty by 'take'
        dst_alloc.allocated_qty = (dst_alloc.allocated_qty + take).quantize(Decimal("0.001"))
        dst_alloc.save(update_fields=["allocated_qty"])

        fragments.append({"grn_id": alloc.grn_id, "moved": take})
        need -= take

    return fragments

# ────────────────────────────────────────────────
# Single source of truth for ERP alias
# ────────────────────────────────────────────────
def get_erp_alias() -> str:
    desired = getattr(settings, "DB_READONLY_NAME", None)
    for alias, db in settings.DATABASES.items():
        name = db.get("NAME")
        if desired and name == desired:
            return alias
        if name == "eresOCSPL":
            return alias
    return "default"

# ────────────────────────────────────────────────
# GRN SYNC – parameterized (no DECLAREs)
# (Only change here: join to ITMTYP and filter to 3 types)
# ────────────────────────────────────────────────
SQL_GRN = r"""
SELECT
    %s AS CompanyID,
    %s AS YearID,
    GRNHDR.sDocNo AS DocNo,
    CONVERT(date, CONVERT(varchar(8), GRNHDR.dtDocDate)) AS DocDate,
    ISNULL(SUPP.sCode,'') AS SupplierCode,
    ISNULL(SUPP.sName,'') AS SupplierName,
    ITM.sCode  AS ItemCode,
    ITM.sName  AS ItemName,
    -- keep same projection of PR item type (no flow change)
    (SELECT sName FROM ITMTYP WITH (NOLOCK) WHERE ITMTYP.lTypid = PRDET.lItmtyp) AS PRItemType,
    COALESCE(
      NULLIF(LTRIM(RTRIM((
        SELECT TOP 1 c.sValue
        FROM txncf c
        WHERE c.lid = GRNHDR.lid
          AND c.lLine = GRNDET.lLine
          AND UPPER(REPLACE(REPLACE(REPLACE(c.sName,' ',''),'.',''),':','')) IN ('BATCHNO','BATCHNUMBER','BATCH')
      ))), ''),
      NULLIF(LTRIM(RTRIM((
        SELECT TOP 1 c2.sValue
        FROM txncf c2
        WHERE c2.lid = GRNHDR.lid
          AND c2.lLine = 0
          AND UPPER(REPLACE(REPLACE(REPLACE(c2.sName,' ',''),'.',''),':','')) IN ('BATCHNO','BATCHNUMBER','BATCH')
      ))), ''),
      NULLIF(LTRIM(RTRIM(GRNDET.sValue1)),''),
      ''
    ) AS BatchNo,
    ISNULL(UNT.sCode,'') AS UOM,
    CONVERT(decimal(18,3), ISNULL(GRNDET.dQty2, ISNULL(GRNDET.dQty, 0))) AS Qty,
    '' AS RM_PM,
    '' AS Warehouse,
    CAST(%s AS varchar(10)) + '-' +
    CAST(%s AS varchar(10)) + '-' +
    GRNHDR.sDocNo + '-' +
    CAST(GRNDET.lLine AS varchar(10)) AS ERPLineID
FROM txnhdr PRHDR WITH (NOLOCK)
INNER JOIN TXNDET PRDET WITH (NOLOCK) ON PRHDR.lId = PRDET.lId
-- NEW: bind PR line to its item type for filtering
INNER JOIN ITMTYP PRITP WITH (NOLOCK) ON PRITP.lTypid = PRDET.lItmtyp
LEFT JOIN TXNDET PODET WITH (NOLOCK)
  ON PRHDR.lid = PODET.llnkdocid
 AND PODET.lLnkLine = PRDET.lLine
 AND PODET.LTYPID IN (400,509,520,524,750,751,752,753,754,755,756,757,758,759,760,761,762,763,764,765,766,767,768,769,956)
LEFT JOIN txnhdr POHDR WITH (NOLOCK)
  ON POHDR.lid = PODET.lId
 AND POHDR.LTYPID IN (400,509,520,524,750,751,752,753,754,755,756,757,758,759,760,761,762,763,764,765,766,767,768,769,956)
 AND POHDR.bDel <> 1
 AND PODET.lClosed <> -2
LEFT JOIN BUSMST SUPP WITH (NOLOCK) ON POHDR.lAccId1 = SUPP.lId
LEFT JOIN TXNDET GRNDET WITH (NOLOCK)
  ON PODET.lid = GRNDET.llnkdocid
 AND GRNDET.lLnkLine = PODET.lLine
 AND GRNDET.LTYPID IN (164,540,548,861,790,791,792,793,794,795,796,797,798,921,801,802,809,808,807,805,804,803,841,842,844,850,845,
                        851,852,932,868,867,854,853,932,958,528)
LEFT JOIN txnhdr GRNHDR WITH (NOLOCK)
  ON GRNHDR.lid = GRNDET.lId
 AND GRNHDR.LTYPID IN (164,540,548,861,790,791,792,793,794,795,796,797,798,921,801,802,809,808,807,805,804,803,841,842,844,850,845,
                        851,852,932,868,867,854,853,932,958,528)
 AND GRNHDR.bDel <> 1
 AND GRNDET.lClosed <> -2
LEFT JOIN ITMMST ITM  WITH (NOLOCK) ON GRNDET.lItmId  = ITM.lId
LEFT JOIN UNTMST UNT  WITH (NOLOCK) ON GRNDET.lUntId2 = UNT.lId
WHERE
    PRHDR.lTypId IN (402,679,680,681,682,683,684,685,686,687,688,689)
    AND PRHDR.bDel <> 1
    AND PRDET.lClosed <> -2
    -- NEW: restrict to only the 3 item types
    AND PRITP.sName IN ('Key Raw Material', 'Packing Material', 'Raw Material')
    AND CONVERT(date, CONVERT(varchar(8), GRNHDR.dtDocDate)) BETWEEN %s AND %s
ORDER BY GRNHDR.sDocNo, GRNDET.lLine;
"""

# ────────────────────────────────────────────────
# MATERIAL ISSUE SYNC – parameterized (already filtered)
# ────────────────────────────────────────────────
SQL_ISSUE = r"""
SELECT
    %s AS CompanyID,
    %s AS YearID,
    HDR.sDocNo AS IssueNo,
    CONVERT(date, CONVERT(varchar(8), HDR.dtDocDate)) AS IssueDate,
    ISNULL((SELECT TOP 1 sValue FROM txncf WHERE lid = HDR.lid AND sName = 'Block' AND lLine = 0),'') AS CostCenter,
    ITM.sCode AS ItemCode,
    ITM.sName AS ItemName,
    ITP.sName AS IssueItemType,
    COALESCE(
      NULLIF(LTRIM(RTRIM((
        SELECT TOP 1 c.sValue
        FROM txncf c
        WHERE c.lid = HDR.lid
          AND c.lLine = DET.lLine
          AND UPPER(REPLACE(REPLACE(REPLACE(c.sName,' ',''),'.',''),':','')) IN ('BATCHNO','BATCHNUMBER','BATCH')
      ))), ''),
      NULLIF(LTRIM(RTRIM((
        SELECT TOP 1 c2.sValue
        FROM txncf c2
        WHERE c2.lid = HDR.lid
          AND c2.lLine = 0
          AND UPPER(REPLACE(REPLACE(REPLACE(c2.sName,' ',''),'.',''),':','')) IN ('BATCHNO','BATCHNUMBER','BATCH')
      ))), ''),
      NULLIF(LTRIM(RTRIM(DET.sValue1)),''),
      ''
    ) AS BatchNo,
    ISNULL(UOM.sCode, '') AS UOM,
    CONVERT(decimal(18,3), DET.dQty) AS Qty,
    '' AS Warehouse,
    CAST(%s AS varchar(10)) + '-' +
    CAST(%s AS varchar(10)) + '-' +
    HDR.sDocNo + '-' +
    CAST(DET.lLine AS varchar(10)) AS ERPLineID
FROM txnhdr HDR
INNER JOIN TXNTYP tp ON HDR.lTypId = tp.lTypId
INNER JOIN TXNDET DET ON HDR.lId = DET.lId
INNER JOIN ITMMST ITM ON DET.lItmId = ITM.lId
INNER JOIN ITMTYP ITP ON ITP.lTypid = DET.lItmtyp
INNER JOIN UNTMST UOM ON DET.lUntId2 = UOM.lId
WHERE
    HDR.lTypId IN (431,880,875,887,989,183,883,666,727,728,729,730,731,979,987,992,1021,1024)
    AND DET.bDel <> -2
    AND HDR.bDel <> 1
    AND DET.lClosed <> -2
    AND HDR.lClosed = 0
    AND ITP.sName IN ('Key Raw Material', 'Packing Material', 'Raw Material')
    AND CONVERT(date, CONVERT(varchar(8), HDR.dtDocDate)) BETWEEN %s AND %s
ORDER BY HDR.sDocNo, DET.lLine;
"""

def _rows(cursor):
    cols = [c[0] for c in cursor.description]
    for r in cursor.fetchall():
        yield dict(zip(cols, r))

@transaction.atomic
def sync_grn(from_date: date, to_date: date, company_id: int = 27, year_id: int = 7, alias: Optional[str] = None) -> int:
    processed = 0
    erp_alias = alias or get_erp_alias()
    with connections[erp_alias].cursor() as c:
        c.execute(SQL_GRN, [company_id, year_id, company_id, year_id, from_date, to_date])
        for row in _rows(c):
            rm_pm = "PM" if "pack" in (row.get("ItemName") or "").lower() else "RM"
            GrnLineCache.objects.update_or_create(
                erp_line_id=row["ERPLineID"],
                defaults=dict(
                    company_id=row["CompanyID"],
                    year_id=row["YearID"],
                    doc_no=row["DocNo"],
                    doc_date=row["DocDate"],
                    supplier_code=str(row.get("SupplierCode") or ""),
                    supplier_name=row.get("SupplierName") or "",
                    item_code=str(row.get("ItemCode") or ""),
                    item_name=row.get("ItemName") or "",
                    pr_item_type=row.get("PRItemType") or "",
                    batch_no=row.get("BatchNo") or "",
                    uom=row.get("UOM") or "",
                    qty=row.get("Qty") or 0,
                    rm_pm=rm_pm,
                    warehouse=row.get("Warehouse") or "",
                ),
            )
            processed += 1
    return processed

@transaction.atomic
def sync_issues(from_date: date, to_date: date, company_id: int = 27, year_id: int = 7, alias: Optional[str] = None) -> int:
    processed = 0
    erp_alias = alias or get_erp_alias()
    with connections[erp_alias].cursor() as c:
        c.execute(SQL_ISSUE, [company_id, year_id, company_id, year_id, from_date, to_date])
        for row in _rows(c):
            IssueLineCache.objects.update_or_create(
                erp_line_id=row["ERPLineID"],
                defaults=dict(
                    company_id=row["CompanyID"],
                    year_id=row["YearID"],
                    issue_no=row["IssueNo"],
                    issue_date=row["IssueDate"],
                    cost_center=row.get("CostCenter") or "",
                    item_code=str(row["ItemCode"]),
                    item_name=row.get("ItemName") or "",
                    issue_item_type=row.get("IssueItemType") or "",
                    batch_no=row.get("BatchNo") or "",
                    uom=row.get("UOM") or "",
                    qty=row.get("Qty") or 0,
                    warehouse=row.get("Warehouse") or "",
                ),
            )
            processed += 1
    return processed

# ────────────────────────────────────────────────
# FIFO allocator
# ────────────────────────────────────────────────
def apply_issue_fifo(issue: IssueLineCache, prefer_rack_code: Optional[str] = None) -> list[RackIssue]:
    need = Decimal(issue.qty)
    if need <= 0:
        return []
    q = (
        RackAllocation.objects
        .filter(grn__item_code=issue.item_code)
        .select_related("grn", "rack")
        .order_by("grn__doc_date", "created_at", "pk")
    )
    if issue.batch_no:
        q = q.filter(grn__batch_no=issue.batch_no)
    if prefer_rack_code:
        prefer = q.filter(rack__code=prefer_rack_code)
        others = q.exclude(rack__code=prefer_rack_code)
        allocations: Iterable[RackAllocation] = list(prefer) + list(others)
    else:
        allocations = q
    out: list[RackIssue] = []
    for alloc in allocations:
        bal = alloc.balance_qty
        if bal <= 0:
            continue
        take = min(bal, need)
        out.append(RackIssue(issue=issue, allocation=alloc, qty=take))
        need -= take
        if need <= 0:
            break
    with transaction.atomic():
        RackIssue.objects.bulk_create(out)
    return out
