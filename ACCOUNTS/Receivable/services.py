# ACCOUNTS/services.py

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal

from django.core.cache import cache
from django.db import connections
from django.db.models import Q
from django.utils import timezone

from ACCOUNTS.Receivable.models import ReceivableSnapshotRow


# =============================================================================
# Configuration
# =============================================================================

BASE_START_DATE = date(2025, 4, 1)

# Detail table size cap (prevents HTML render slowdown on huge snapshots)
MAX_DETAIL_ROWS = 2000

# Cache TTLs
CACHE_RAW_TTL = 1800          # 30 minutes
CACHE_DASHBOARD_TTL = 300     # 5 minutes


# =============================================================================
# Company Groups
# =============================================================================

COMPANY_GROUPS = {
    "OCSPL": [
        "OC Specialities Private Limited - Solapur",
        "OC Specialities Private Limited - Mumbai",
        "OC Specialities Private Limited - GJ",
        "OC Specialities Private Limited - Warehouse (AKOLEKATHI)",
        "OC Specialities Private Limited - Solapur (Unit II)",
        "OC Specialities Private Limited - WAREHOUSE (F-36 MIDC, CHINCHOLI)",
    ],
    "OCCHEM": [
        "OC Specialities Chemicals Private Limited",
        "OC Specialities Chemicals Private Limited -MH",
        "OC Specialities Chemicals Private Limited -GJ",
        "OC Specialities Chemicals Private Limited -AP",
    ],
}


def get_company_group(company_name: str) -> str:
    """
    Map full company name from ERP to logical group:
      - 'OCSPL'
      - 'OCCHEM'
      - 'OTHER'

    Safe behavior:
      - Exact match using COMPANY_GROUPS lists
      - Fallback substring match (helps if ERP name varies slightly)
    """
    if not company_name:
        return "OTHER"

    cn = str(company_name).strip()
    for group, names in COMPANY_GROUPS.items():
        if cn in names:
            return group

    # Fallbacks
    n = cn.lower()
    if "oc special" in n or "ocspl" in n:
        return "OCSPL"
    if "oc specialities chemicals" in n or "oc chem" in n or "occhem" in n:
        return "OCCHEM"

    return "OTHER"


# =============================================================================
# NOTE: ERP Receivables SQL
# (kept here because your sync command imports RECEIVABLES_SQL from this module)
# =============================================================================

RECEIVABLES_SQL = r"""
-- (SQL unchanged; kept exactly as you provided)
Declare @decimal decimal(21,18)
DECLARE @Status varchar(10) = 'ALL';   -- 'ALL' / 'CLEAN' / 'OVERDUE' (set from UI)
DECLARE @AsOfDate date = CAST(GETDATE() AS date);

DECLARE @IncludeClosed bit = CASE WHEN UPPER(@Status)='ALL' THEN 1 ELSE 0 END;

select @decimal=lDigDec from CMPNY c inner join CURMST cm on c.lCurrId=cm.lid where c.lId=27;

with vTxnHdr as (
    Select
        d.lTypId,
        dt.sName,
        dt.lFinTyp,
        d.lId,
        d.lClosed,
        d.bDel,
        d.dtDocDate as dtDueDate,
        d.dtDocDate,
        d.dTotal,
        d.sPrefix,
        d.lDocNo,
        d.sExtNo,
        d.sDocNo,
        d.lCurrId,
        d.lPayTrmId,
        d.dCurrCnv,
        (case when dt.bEmpDet > 0 then da.lEmpId Else d.lEmpId End) as lEmpId,
        da.lLine,
        0 as lSubLine,
        da.lAccId,
        da.lAstId,
        d.lCompId,
        da.lPrjId,
        da.lDimId,
        d.sNarr as sNarr1,
        da.sNarr,
        da.dAmtDr,
        da.dAmtCr,
        da.dOtstndAmt,
        da.dRate,
        d.lLocId,
        case when isnull(g.lPrevId,0)=0 then isnull(g.lId,0) else isnull(g.lPrevId,0) end as GEoPrevId
    from TXNTYP as dt
    inner join TXNHDR as d
        on dt.lTypId=d.lTypId
       and dt.lFinTyp<2
       and (dt.bComp=0 and dt.bPrjDet=0 and dt.bProfitCenter=0)
    inner join TXNACC as da on d.lId=da.lId
    left join BUSMST bm on da.lAccId=bm.lAccId and da.lLine>0 and bm.bDel = 0
    left join BUSADD ba on bm.lId = ba.lId and ba.bDefault=1
    left join GEOLOC as g on d.lLocId = g.lId
    where d.bDel=0
      and da.bDel=0
      and d.dtDocDate<=CONVERT(VARCHAR(8), GETDATE(), 112)

    Union All

    Select
        d.lTypId,
        dt.sName,
        dt.lFinTyp,
        d.lId,
        d.lClosed,
        d.bDel,
        d.dtDocDate as dtDueDate,
        d.dtDocDate,
        d.dTotal,
        d.sPrefix,
        d.lDocNo,
        d.sExtNo,
        d.sDocNo,
        d.lCurrId,
        d.lPayTrmId,
        d.dCurrCnv,
        (case when dt.bEmpDet > 0 then da.lEmpId Else d.lEmpId End) as lEmpId,
        ds.lLine,
        ds.lSubLine,
        da.lAccId,
        da.lAstId,
        ds.lCompId,
        ds.lPrjId,
        ds.lDimId,
        d.sNarr as sNarr1,
        da.sNarr,
        ds.dAmtDr,
        ds.dAmtCr,
        ds.dOtstndAmt,
        da.dRate,
        d.lLocId,
        case when isnull(g.lPrevId,0)=0 then isnull(g.lId,0) else isnull(g.lPrevId,0) end as GEoPrevId
    from TXNTYP as dt
    inner join TXNHDR as d
        on dt.lTypId=d.lTypId
       and not (dt.bComp=0 and dt.bPrjDet=0 and dt.bProfitCenter=0)
    inner join TXNACC as da on d.lId=da.lId
    inner join TXNACCSUB as ds
        on da.lId=ds.lId
       and da.lLine=ds.lLine
       and ds.cTyp='P'
       and ds.bDel=0
    left join BUSMST bm on da.lAccId=bm.lAccId and da.lLine>0 and bm.bDel = 0
    left join BUSADD ba on bm.lId = ba.lId and ba.bDefault=1
    left join GEOLOC as g on d.lLocId = g.lId
    where d.bDel=0
      and da.bDel=0
      and d.dtDocDate<=CONVERT(VARCHAR(8), GETDATE(), 112)
)
Select distinct d.*
into #TXNACC
from vTxnHdr as d
inner join ACCMST as a on d.lAccId=a.lId
where (d.dAmtDr+d.dAmtCr>0)
  and d.lCompId in (3,4,27,28,40,93,7,8,9,25,26)
  and (d.lClosed<=0 OR @IncludeClosed=1)
;

Update #TXNACC
set lLocId = ba.lLocId
from #TXNACC t
inner join BUSMST b on t.lAccId=b.lAccId
inner join BUSADD ba on ba.lid =b.lId and ba.bDefault=1
where t.lFinTyp<2 and t.lFinTyp not in (-1,-2)
;

Update #TXNACC
set GEoPrevId=a.GEoPrevId1
from (
    select
        case when isnull(g.lPrevId,0)=0 then isnull(g.lId,0) else isnull(g.lPrevId,0) end as GEoPrevId1,
        d.lId,
        d.lLocId
    from #TXNACC d
    left join GEOLOC as g on d.lLocId = g.lId
) a
where a.lId=#TXNACC.lId
  and a.lLocId=#TXNACC.lLocId
  and isnull(#TXNACC.GEoPrevId,0)=0
;

Select distinct
    d.lTypId,
    cu.sName as CurType,
    lFinTyp,
    d.sName,
    d.dCurrCnv,
    c1.sName CurType1,
    isnull(
        case when not d.lCurrId = 0 then
            (Select top 1 cd.dCurrCnv
             From CURDET cd
             where cd.lid=1
               and lCurrId = 0
               and dtWefDate<d.dtDocDate
             order by dtWefDate desc)
        else d.dCurrCnv end,
    1) as CurRate,
    d.lId,
    d.sPrefix,
    d.lDocNo as TrnNo,
    d.sExtNo,
    d.sDocNo as sDocument,
    Convert(VarChar(20),d.dtDocDate) as mDocDate,
    Convert(VarChar(20), d.dtDueDate) as mDueDate,
    d.dtDueDate,
    d.dtDocDate,
    d.dTotal,
    d.bDel,
    d.lClosed,
    pt.sName as PayTrm,
    isnull(pt.dValue,0) as PayTrmDays,
    cm.lId as lCompId,
    cm.sRemarks as CompanyName,
    Geo.sName as Location,
    Geo1.sName as Location1,
    d.sNarr as Narration
into #TXNHDR
from #TXNACC as d
inner join CURMST cu on d.lCurrId = cu.lId
inner join CMPNY as cm on d.lCompId=cm.lId
inner join CURMST as c1 on cm.lCurrId=c1.lId
inner join ACCMST as a on d.lAccId=a.lId
inner join ACCTYP act on  act.lTypId=a.lTypId
left join GEOLOC Geo on  d.lLocId=Geo.lId
left join GEOLOC Geo1 on  d.GEoPrevId=Geo1.lId
left join PayTrm as pt on d.lPayTrmId=pt.lId
where d.bDel=0
  and cm.lId in (3,4,27,28,40,93,7,8,9,25,26)
  and act.cLdgTyp='C'
;

Select
    d.lId,
    isnull(Max(Case when cf.sName='Customer PO Date' then sValue Else '' End),'') as [Customer PO Date],
    isnull(Max(Case when cf.sName='Customer PO No.' then sValue Else '' End),'') as [Customer PO No.],
    isnull(Max(Case when cf.sName='Item Name' then sValue Else '' End),'') as [Item Name],
    isnull(Max(Case when cf.sName='Destination' then sValue Else '' End),'') as [Destination],
    isnull(Max(Case when cf.sName='Bank Details' then sValue Else '' End),'') as [Bank Details]
into #vTXNCF
from #TXNHDR as d
inner join TXNCF as cf on d.lId=cf.lId and cf.lLine=0
Group by d.lId
;

With vBusCon as (
    Select
        ROW_NUMBER ()over(Partition by lid order by lid) as Rowno,
        bc.*
    From BUSCNT bc
)
, vAcc as (
    Select distinct
        a.lId,
        a.sCode,
        a.sName,
        act.cAccTyp,
        c.sName as GroupName,
        max(dCreditLimit) dCreditLimit,
        max(dCreditDay) dCreditDay,
        act.cLdgTyp,
        isnull(
            Case when ba.lMSMETyp=1 then 'Micro'
                 when ba.lMSMETyp=2 then 'Small'
                 when ba.lMSMETyp=3 then 'Medium'
            end,'') as sMSME,
        case when isnull(Max(bc.sName),'')='' then isnull(Max(ad.sName),'') else isnull(Max(bc.sName),'') end  as ConName,
        case when isnull(Max(bc.sMobile),'')='' then  isnull(Max(ad.sMobile),'') else isnull(Max(bc.sMobile),'') end as ConMobile
    from ACCMST as a
    inner join ACCTYP act on  act.lTypId=a.lTypId
    left join BUSMST b on b.lAccId=a.lId
    left join [BUSADD] ad on ad.lId =b.lId and ad.bDefault=1
    left join vBusCon bc on b.lid=bc.lid and Rowno=1
    left join (
        Select Max(lMSMETyp)lMSMETyp,lId From BUSADD Group BY lId
    )as ba on b.lId=ba.lId
    inner join COAMST c on a.lCoaId=c.lId
    where act.cLdgTyp='C'
    Group by a.lId, a.sCode, a.sName,act.cAccTyp,act.cLdgTyp,ba.lMSMETyp,c.sName
)
Select * into #vACC from vACC
;

With vTxnCf1 as (
    Select
        d.lId,
        Max(Case when cf.sName like '%Reference No.%' or cf.sName like '%Invoice No.%' then sValue Else '' End) as RefNo,
        Max(Case when cf.sName like '%Reference Dt.%' or cf.sName like '%Invoice Dt.%' then sValue Else '' End) as RefDate
    from #TXNHDR as d
    inner join TXNCF as cf
        on d.lId=cf.lId
    where not sValue=''
      and cf.lLine=0
      and (cf.sName like '%Reference Dt.%'
           or cf.sName like '%Invoice Dt.%'
           or cf.sName like '%Reference No.%'
           or cf.sName like '%Invoice No.%')
      and d.bDel=0
    Group by d.lId
)
Select * into #vTXNCF1 from vTXNCf1
;

-- ============================================================
-- ✅ NEW: Instrument Number (Cheque/Instrument No from TXNCF)
-- ============================================================
With vTxnCfInst as (
    Select
        d.lId,
        Max(
            Case
                when cf.sName like '%Instrument No%'
                  or cf.sName like '%Instrument Number%'
                  or cf.sName like '%Cheque No%'
                  or cf.sName like '%Chq No%'
                  or cf.sName like '%Chq%No%'
                then sValue
                else ''
            End
        ) as [Instrument No]
    from #TXNHDR as d
    inner join TXNCF as cf
        on d.lId = cf.lId
    where isnull(sValue,'') <> ''
      and cf.lLine = 0
      and d.bDel = 0
    group by d.lId
)
Select * into #vTXNCF_INST from vTxnCfInst
;

with vTxnAcc as (
    select distinct
        d.lId,
        d.lTypId,
        d.lLine,
        d.lAccId,
        a.GroupName,
        d.dAmtCr,
        d.dAmtDr,
        d.dOtstndAmt,
        (d.dRate * d.dCurrCnv) as dRate,
        dCreditLimit CrLimit,
        dCreditDay as CrDays,

        CASE
            WHEN duex.DueDt < @AsOfDate THEN DATEDIFF(DAY, duex.DueDt, @AsOfDate)
            ELSE 0
        END AS OverdueDays,

        CONVERT(varchar(8), duex.DueDt, 112) AS OverdueDate

    from #TXNACC as d
    inner join #vAcc a on a.lid=d.lAccId
    inner join #TXNHDR as dh on d.lid = dh.lid

    CROSS APPLY (
        SELECT DATEADD(
            DAY,
            ISNULL(dh.PayTrmDays,0),
            CONVERT(date, CONVERT(varchar(8), dh.dtDocDate), 112)
        ) AS DueDt
    ) duex

    left outer join #vTxnCf1 cf on d.lId=cf.lId
)
, vTxnSett as (
    Select
        d.lId,
        d.lLine,
        case when max(d.dAmtDr)>0 then
            isnull(Sum(ds.dAdjAmtDr-ds.dAdjAmtcr),0)
        else
            isnull(Sum(ds.dAdjAmtCr-ds.dAdjAmtDr),0)
        end as dAmt,
        case when max(d.dAmtDr)>0 then
            case when d.lFinTyp in (-5,-6,-7,-8) then
                Sum((ds.dAdjAmtDr-ds.dAdjAmtCr)/d.dCurrCnv)
            else
                Sum((ds.dAdjAmtDr-ds.dAdjAmtCr)/ds.dCurrCnv)
            end
        else
            case when d.lFinTyp in (-5,-6,-7,-8) then
                Sum((ds.dAdjAmtCr-ds.dAdjAmtDr)/d.dCurrCnv)
            else
                Sum((ds.dAdjAmtCr-ds.dAdjAmtDr)/ds.dCurrCnv)
            end
        end dAmtFC,
        isnull(ds.bSystem,0) bSystem
    from #TXNACC d
    inner join #vAcc a on a.lid=d.lAccId
    left join (
        Select
            ds.dAdjAmtDr,
            ds.dAdjAmtcr,
            ds.lId,
            ds.lLine,
            ds.bSystem,
            ds.lAccId,
            d.dCurrCnv
        From #TXNACC d
        inner join TXNFINSET as ds
            on ds.lId=d.lId
           and d.lLine=ds.lLine
           and d.bDel=0
           and (d.lClosed<=0 OR @IncludeClosed=1)
        inner join TXNHDR as dh on ds.lRefId=dh.lId
        inner join ACCMST as a on d.lAccId=a.lId
        inner join ACCTYP as at on a.lTypId=at.lTypId and not at.bDocAdj=0
        inner join TxnTyp t2 on t2.lTypId=ds.lRefTypId and t2.lFinTyp<2
        where dh.bDel=0
          and dh.lClosed<=0
          and dh.dtDocDate<=CONVERT(VARCHAR(8), GETDATE(), 112)
          and dh.lCompId in (3,4,27,28,40,93,7,8,9,25,26)
    ) as ds
        on d.lId=ds.lId
       and d.lLine=ds.lLine
       and d.lAccId=ds.lAccId
    and d.lCompId in (3,4,27,28,40,93,7,8,9,25,26)
    Group by d.lId, d.dAmtDr, d.lLine,d.lFinTyp,ds.bSystem
)
, vSumm as (
    -- vSumm unchanged (kept exactly as you provided)
    select
        lFinTyp,
        d.lTypId,
        dtDocDate,
        da.lAccId,
        a.sCode As AccCode,
        a.sName as AccName,
        da.GroupName,
        a.sMSME,
        d.sName as TrnTyp,
        d.lid as lid,
        d.sDocument as TrnNo,
        isnull((RefNo),'')RefNo,
        isnull((RefDate),'') RefDate,
        Convert(VarChar,CONVERT(datetime, convert(varchar(10), d.mDocDate)),106)  as TrnDate,
        d.dtDueDate as DueDate,
        CurType,
        CurType1,
        CurRate,
        CrLimit,
        CrDays,
        OverdueDays,
        Convert(VarChar,CONVERT(datetime, convert(varchar(10), OverdueDate)),106) as OverdueDate,
        sum( da.dAmtCr) as dAmtCr,
        sum(da.dAmtDr) as dAmtDr,
        isnull(sum((da.dAmtDr)+(da.dAmtCr)),0) AS BillAmt,
        abs(isnull(sum(ds.dAmt),0)) PaidAmt,
        case when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
               or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
             then (isnull(sum(ds.dAmt),0))-(isnull(sum((da.dAmtDr)+(da.dAmtCr)),0))
             else isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)-(isnull(sum(ds.dAmt),0))
        end as  BillOSAmt,
        0 UnAdjAmt,
        case when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
               or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
             then (isnull(sum(ds.dAmt),0))-(isnull(sum((da.dAmtDr)+(da.dAmtCr)),0))
             else isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)-(isnull(sum(ds.dAmt),0))
        end AS OsAmt,
        sum( da.dAmtCr)/CurRate as dAmtCr_FC,
        sum(da.dAmtDr)/CurRate as dAmtDr_FC,
        (isnull(sum((da.dAmtDr)+(da.dAmtCr)),0))/CurRate AS BillAmt_FC,
        abs(isnull(sum(dAmtFC),0)) AS PaidAmt_FC,
        case when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
               or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
             then(isnull(sum(ds.dAmtFC),0))-(isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)/CurRate)
             else isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)/CurRate-(isnull(sum(ds.dAmtFC),0))
        end as  BillOSAmt_FC,
        0  AS UnAdjAmt_FC,
        case when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
               or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
             then (isnull(sum(ds.dAmtFC),0))-(isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)/CurRate)
             else isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)/CurRate-(isnull(sum(ds.dAmtFC),0))
        end AS OsAmt_FC,
        isnull(ds.bSystem,0) bSystem,
        PayTrm,
        PayTrmDays,
        d.CompanyName,
        d.lCompId,
        a.ConName as ConName,
        a.ConMobile as ConMobile,
        d.Location,
        d.Location1,
        d.Narration
    from #TXNHDR as d
    inner join vTxnAcc as da on d.lId=da.lId
    inner join #vACC as a on da.lAccId=a.lId
    inner join vTxnSett as ds on da.lId=ds.lId and da.lLine=ds.lLine
    left join #vTxnCf1 as cf on d.lId=cf.lId
    where lFinTyp in (-1,-2)
      and da.lLine=0
    Group by
        lFinTyp,
        d.lTypId,
        dtDocDate,
        da.lAccId,
        a.sCode,
        a.sName,
        da.GroupName,
        a.sMSME,
        d.sName,
        d.lid,
        d.sDocument,
        RefNo,
        RefDate,
        d.mDocDate,
        d.dtDueDate,
        CurType,
        CurType1,
        CurRate,
        CrLimit,
        CrDays,
        OverdueDays,
        OverdueDate,
        bSystem,
        PayTrm,
        PayTrmDays,
        cLdgTyp,
        d.CompanyName,
        d.lCompId,
        a.ConName,
        a.ConMobile,
        d.Location,
        d.Location1,
        d.Narration

    Union All

    -- reminder: your second UNION ALL block remains unchanged
    select
        lFinTyp,
        d.lTypId,
        dtDocDate,
        da.lAccId,
        a.sCode As AccCode,
        a.sName as AccName,
        da.GroupName,
        a.sMSME,
        d.sName as TrnTyp,
        d.lid as lid,
        d.sDocument as TrnNo,
        isnull((case when lFinTyp =-13 then cf1.RefNo else cf.RefNo end),'')RefNo,
        isnull((case when lFinTyp =-13 then cf1.RefDate else cf.RefDate end),'') RefDate,
        Convert(VarChar,CONVERT(datetime, convert(varchar(10), d.mDocDate)),106)  as TrnDate,
        d.dtDueDate as DueDate,
        CurType,
        CurType1,
        CurRate,
        CrLimit,
        CrDays,
        OverdueDays,
        Convert(VarChar,CONVERT(datetime, convert(varchar(10), OverdueDate)),106) as OverdueDate,
        sum( da.dAmtCr) as dAmtCr,
        sum(da.dAmtDr) as dAmtDr,
        case
            when lFinTyp =-13 and not Max(da.dRate)=0 then Max(da.dRate)
            when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
              or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
              then 0
            else isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)
        end AS BillAmt,
        abs(isnull(sum(ds.dAmt),0)-(case
            when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
               or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
               then isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)
            else 0 end)
        ) PaidAmt,
        case when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
               or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
             then (isnull(sum(ds.dAmt),0))-(isnull(sum((da.dAmtDr)+(da.dAmtCr)),0))
             else isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)-(isnull(sum(ds.dAmt),0))
        end as  BillOSAmt,
        case when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
               or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
             then  isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)-(isnull(sum(ds.dAmt),0))
             else 0
        end AS UnAdjAmt,
        case when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
               or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
             then (isnull(sum(ds.dAmt),0))-(isnull(sum((da.dAmtDr)+(da.dAmtCr)),0))
             else isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)-(isnull(sum(ds.dAmt),0))
        end AS OsAmt,
        sum( da.dAmtCr)/CurRate as dAmtCr_FC,
        sum(da.dAmtDr)/CurRate as dAmtDr_FC,
        case
            when lFinTyp =-13 and not Max(da.dRate)=0 then Max(da.dRate)
            when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
              or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
              then 0
            else isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)
        end/CurRate AS BillAmt_FC,
        abs(
            isnull(sum(ds.dAmtFC),0)-(case
                when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
                   or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
                   then isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)
                else 0 end)/CurRate
        ) as PaidAmt_FC,
        case when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
               or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
             then(isnull(sum(ds.dAmtFC),0))-(isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)/CurRate)
             else isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)/CurRate-(isnull(sum(ds.dAmtFC),0))
        end as  BillOSAmt_FC,
        case when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
               or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
             then  isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)/CurRate-(isnull(sum(ds.dAmtFC),0))
             else 0
        end AS UnAdjAmt_FC,
        case when (cLdgTyp='S' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)>0)
               or (cLdgTyp='C' and  isnull(sum((da.dAmtDr)-(da.dAmtCr)),0)<0)
             then (isnull(sum(ds.dAmtFC),0))-(isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)/CurRate)
             else isnull(sum((da.dAmtDr)+(da.dAmtCr)),0)/CurRate-(isnull(sum(ds.dAmtFC),0))
        end AS OsAmt_FC,
        isnull(ds.bSystem,0) bSystem,
        PayTrm,
        PayTrmDays,
        d.CompanyName,
        d.lCompId,
        a.ConName as ConName,
        a.ConMobile as ConMobile,
        d.Location,
        d.Location1,
        d.Narration
    from #TXNHDR as d
    inner join vTxnAcc as da on d.lId=da.lId
    inner join #vACC as a on da.lAccId=a.lId
    inner join vTxnSett as ds on da.lId=ds.lId and da.lLine=ds.lLine
    left join #vTxnCf1 as cf on ds.lId=cf.lId and not lFinTyp=-13
    left join #vTxnCf1 as cf1 on d.lId=cf1.lId and lFinTyp=-13
    Where lFinTyp not in (-1,-2)
    Group by
        lFinTyp,
        d.lTypId,
        dtDocDate,
        da.lAccId,
        a.sCode,
        a.sName,
        da.GroupName,
        a.sMSME,
        d.sName,
        d.lid,
        d.sDocument,
        cf1.RefNo,
        cf.RefNo,
        cf.RefDate,
        cf1.RefDate,
        d.mDocDate,
        d.dtDueDate,
        CurType,
        CurType1,
        CurRate,
        CrLimit,
        CrDays,
        OverdueDays,
        OverdueDate,
        bSystem,
        PayTrm,
        PayTrmDays,
        cLdgTyp,
        d.CompanyName,
        d.lCompId,
        a.ConName,
        a.ConMobile,
        d.Location,
        d.Location1,
        d.Narration
)
, vSumm2 as (
    select
        row_number() over (Order by AccName,lAccId,dtDocDate,lid) as myRow,
        *
    from vSumm
    where bSystem = 0
      and (
            UPPER(@Status) = 'ALL'
            OR (UPPER(@Status) = 'OVERDUE' AND abs(OsAmt) > 0.0001 AND OverdueDays > 0)
            OR (UPPER(@Status) = 'CLEAN'  AND abs(OsAmt) > 0.0001 AND OverdueDays = 0)
          )
)

Select
    lFinTyp,
    row_number() over (Partition by lAccId Order by AccName,lAccId) as TmpRow,
    myRow,
    lAccId,
    AccCode,
    AccName,
    GroupName,
    sMSME,
    da.lTypId,
    da.lid,
    TrnTyp,
    TrnNo,
    TrnDate,
    [Customer PO Date],
    [Customer PO No.],
    [Item Name],
    [Destination],
    [Bank Details],
    isnull(inst.[Instrument No],'') as [Instrument No],   -- ✅ NEW
    RefNo,
    RefDate,
    PayTrm,
    PayTrmDays,
    DueDate,
    CurType,
    CurType1,
    CurRate,
    BillAmt,
    PaidAmt,
    case when abs(BillOSAmt) <0.0001 then 0 else BillOSAmt end as BillOSAmt,
    CrLimit,
    CrDays,
    OverdueDays,
    OverdueDate,
    UnAdjAmt,
    case when abs(OsAmt) <0.0001 then 0 else OsAmt end as OsAmt,
    BillAmt_FC,
    PaidAmt_FC,
    case when abs(BillOSAmt_FC) <0.0001 then 0 else BillOSAmt_FC end as BillOSAmt_FC,
    UnAdjAmt_FC,
    case when abs(OsAmt_FC) <0.0001 then 0 else OsAmt_FC end as OsAmt_FC,
    CompanyName,
    lCompId,
    ConName,
    ConMobile,
    Location,
    Location1,
    Narration
into #temp
from vSumm2 da
left join #vTXNCF as cf on da.lId=cf.lId
left join #vTXNCF_INST as inst on da.lId=inst.lId   -- ✅ NEW
;

Select
    TmpRow,
    myRow,
    lAccId,
    lTypId,
    #temp.lid,
    CompanyName as  [Company Name],
    lCompId,
    AccCode As [Party Code],
    AccName as [Party Name],
    GroupName,
    ConName as [ConName],
    ConMobile as [ConMobile],
    TrnTyp as [Trans Type],
    TrnNo as [Trans No],
    TrnDate [Trans Date],
    CONVERT(
        varchar(11),
        DATEADD(
            DAY,
            ISNULL(PayTrmDays,0),
            CONVERT(date, TrnDate, 106)
        ),
        106
    ) as [Due Date],
    RefNo [Ref No],
    RefDate [Ref Date],
    [Customer PO Date],
    [Customer PO No.],
    [Item Name],
    [Destination],
    [Bank Details],
    [Instrument No],                 -- ✅ NEW
    PayTrm  as [Payment Term],
    PayTrmDays  as [Payment Term Days],
    Location,
    Location1,
    CurType1 as [Currency Code],
    1 as [Conversion Rate],
    BillAmt as [Bill Amt],
    PaidAmt as [Paid Amt],
    BillOSAmt as [Bill OS Amt],
    CrLimit as [Credit Limit],
    0 As [Credit Limit Days],
    OverdueDate as [Overdue Date],
    OverdueDays as [Overdue Days],
    UnAdjAmt as [Unadjustment Amt],
    OsAmt as [Outstanding Amt],
    Narration,
    isnull((SELECT COUNT(lAccId) FROM #temp HAVING COUNT(lAccId) > 1),0) as lRecordCount
From #temp
where myRow > 0
order by myRow,TmpRow,lAccId
;

Drop Table if Exists #temp,#TXNHDR,#vTXNCF,#vACC,#TXNACC,#vTXNCF1,#vTXNCF_INST
"""

# =============================================================================
# Date / Decimal helpers
# =============================================================================

def _to_decimal(val, default=Decimal("0")):
    try:
        s = str(val or "").replace(",", "").strip()
        return Decimal(s) if s else default
    except Exception:
        return default


def _parse_ui_date(value: str):
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _parse_sql_display_date(value: str):
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    for fmt in (
        "%d %b %Y",    # 08 Dec 2025
        "%d-%b-%Y",    # 08-Dec-2025
        "%d %B %Y",    # 08 December 2025
        "%Y-%m-%d",    # 2025-12-08
        "%d/%m/%Y",    # 08/12/2025
        "%Y%m%d",      # 20251208
    ):
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _parse_any_date(s):
    if not s:
        return None
    return _parse_sql_display_date(s) or _parse_ui_date(s)


def _to_date_obj(v):
    if not v:
        return None
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, datetime):
        return v.date()
    s = str(v).strip()
    return _parse_any_date(s)


def _norm(s):
    return str(s or "").strip().lower()


def _bill_key(party_code, invoice_no):
    return f"{(party_code or '').strip()}||{(invoice_no or '').strip()}"


# =============================================================================
# Snapshot date resolver
# =============================================================================

def _latest_snapshot_date():
    """
    Prefer dedicated service if present, else compute from snapshot table.
    """
    try:
        from ACCOUNTS.Receivable.services.receivables_sync import latest_snapshot_date as fn
        d = fn()
        if d:
            return d
    except Exception:
        pass

    return (
        ReceivableSnapshotRow.objects.order_by("-snapshot_date")
        .values_list("snapshot_date", flat=True)
        .first()
    )


# =============================================================================
# Snapshot fetch (FAST): values() + iterator()
#   - Avoids reading large JSON raw for entire dataset
#   - Adds internal key "_pk" for optional raw lookup only for detail rows
# =============================================================================

def _values_row_to_erp_dict(v: dict) -> dict:
    r = {
        "_pk": v.get("id"),

        "Company Name": v.get("company_name") or "",
        "Party Code": v.get("party_code") or "",
        "Party Name": v.get("party_name") or "",
        "Trans Type": v.get("trans_type") or "",
        "Trans No": v.get("trans_no") or "",

        "Trans Date": v.get("trans_date_display") or "",
        "Due Date": v.get("due_date_display") or "",
        "Overdue Date": v.get("overdue_date_display") or "",

        "Bill Amt": v.get("bill_amt") or 0,
        "Paid Amt": v.get("paid_amt") or 0,
        "Outstanding Amt": v.get("outstanding_amt") or 0,

        "Item Name": v.get("item_name") or "",
        "Location": v.get("location") or "",

        "lid": v.get("erp_lid"),
        "lAccId": v.get("erp_acc_id"),
        "lCompId": v.get("erp_comp_id"),
        "lTypId": v.get("erp_typ_id"),
    }

    # Cached parsed dates
    r["_trans_dt"] = _parse_any_date(r.get("Trans Date") or "")
    r["_due_dt"] = _parse_any_date(r.get("Due Date") or "")
    r["_overdue_dt"] = _parse_any_date(r.get("Overdue Date") or "")
    r["_effective_dt"] = r["_overdue_dt"] or r["_due_dt"] or r["_trans_dt"]

    # Normalized strings
    r["_pn_norm"] = (r.get("Party Name") or "").strip().lower()
    r["_pc_norm"] = (r.get("Party Code") or "").strip().lower()
    r["_cn_norm"] = (r.get("Company Name") or "").strip().lower()

    return r


def fetch_receivables_raw_from_snapshot(
    *,
    as_of_date=None,
    snapshot_date=None,
    include_all=False,
    customer: str = "",
    company: str = "",
):
    """
    Returns list[dict] compatible with your existing dict-based code.

    - include_all=False -> OPEN rows only (Outstanding != 0)
    - customer/company are optional DB-side reducers (icontains)
    """
    snap = snapshot_date or _latest_snapshot_date()
    if not snap:
        return []

    qs = ReceivableSnapshotRow.objects.filter(snapshot_date=snap)

    # DB-side reducers
    cust = (customer or "").strip()
    comp = (company or "").strip()
    if cust:
        qs = qs.filter(party_name__icontains=cust)
    if comp:
        qs = qs.filter(company_name__icontains=comp)

    # OPEN vs ALL (DB-side)
    if not include_all:
        qs = qs.exclude(outstanding_amt=0)

    qs = qs.values(
        "id",
        "company_name", "party_code", "party_name",
        "trans_type", "trans_no",
        "trans_date_display", "due_date_display", "overdue_date_display",
        "bill_amt", "paid_amt", "outstanding_amt",
        "item_name", "location",
        "erp_lid", "erp_acc_id", "erp_comp_id", "erp_typ_id",
    )

    out = []
    for v in qs.iterator(chunk_size=5000):
        r = _values_row_to_erp_dict(v)

        if not include_all:
            os_amt = _to_decimal(r.get("Outstanding Amt") or 0)
            if abs(os_amt) <= Decimal("0.0001"):
                continue

        out.append(r)

    return out


# =============================================================================
# Row preparation (inplace) - no change to visible keys; internal keys only
# =============================================================================

_INTERNAL_ROW_KEYS = (
    "_pn_norm", "_pc_norm", "_cn_norm",
    "_trans_dt", "_due_dt", "_overdue_dt", "_effective_dt"
)


def _row_prepare(row: dict) -> dict:
    if "_pn_norm" in row:
        return row

    row["_pn_norm"] = (row.get("Party Name") or "").strip().lower()
    row["_pc_norm"] = (row.get("Party Code") or "").strip().lower()
    row["_cn_norm"] = (row.get("Company Name") or "").strip().lower()

    row["_trans_dt"] = _parse_any_date(row.get("Trans Date") or "")
    row["_due_dt"] = _parse_any_date(row.get("Due Date") or row.get("DueDate") or "")
    row["_overdue_dt"] = _parse_any_date(row.get("Overdue Date") or "")

    row["_effective_dt"] = row["_overdue_dt"] or row["_due_dt"] or row["_trans_dt"]
    return row


def _prepare_rows_inplace(rows):
    for r in rows:
        _row_prepare(r)
    return rows


# =============================================================================
# Internal transfer detection (same concept, faster)
# =============================================================================

INTERNAL_TRANSFER_CODES = {
    "BARNH",
    "OCSPL",
}

INTERNAL_TRANSFER_KEYWORDS = [
    "OC SPECIALITIES",
    "INTERNAL TRANSFER",
    "INTER COMPANY",
    "INTER-COMPANY",
    "INTERCO",
    "BRANCH TRANSFER",
    "BRANCH XFER",
]


def _is_internal_transfer_party(row) -> bool:
    # prefer prepared norms if present
    name_u = (row.get("_pn_norm") or (row.get("Party Name") or "").strip().upper()).upper()
    code_u = (row.get("_pc_norm") or (row.get("Party Code") or "").strip().upper()).upper()
    grp_u = (row.get("GroupName") or row.get("Group Name") or "").strip().upper()

    if code_u in INTERNAL_TRANSFER_CODES:
        return True
    if code_u.startswith("BARNH"):
        return True

    for kw in INTERNAL_TRANSFER_KEYWORDS:
        if kw in code_u or kw in name_u or kw in grp_u:
            return True

    return False


# =============================================================================
# Fetch receivables with caching
# =============================================================================

def fetch_receivables_raw_all():
    """
    Snapshot-backed ALL rows (includes fully adjusted where Outstanding=0).
    Used by receipts/weekly analysis functions.
    """
    snap = _latest_snapshot_date()
    cache_key = f"receivables_raw_all::{snap}"

    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    data = fetch_receivables_raw_from_snapshot(snapshot_date=snap, include_all=True)
    _prepare_rows_inplace(data)

    cache.set(cache_key, data, CACHE_RAW_TTL)
    return data


def fetch_receivables_raw():
    """
    Snapshot-backed OPEN-only rows (Outstanding != 0), excluding internal parties.
    """
    snap = _latest_snapshot_date()
    cache_key = f"receivables_raw_open_external::{snap}"

    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    data = fetch_receivables_raw_from_snapshot(snapshot_date=snap, include_all=False)
    _prepare_rows_inplace(data)

    data = [r for r in data if not _is_internal_transfer_party(r)]

    cache.set(cache_key, data, CACHE_RAW_TTL)
    return data


# =============================================================================
# Dashboard filters
# =============================================================================

def _compute_overdue_days(row, as_of=None):
    if as_of is None:
        as_of = timezone.localdate()

    dd = row.get("_overdue_dt") or row.get("_due_dt") or row.get("_trans_dt")
    if not dd:
        due_str = (
            row.get("Overdue Date")
            or row.get("DueDate")
            or row.get("mDueDate")
            or row.get("Due Date")
            or row.get("Trans Date")
            or ""
        )
        dd = _parse_any_date(due_str)

    if not dd:
        return 0

    return max((as_of - dd).days, 0)


def _apply_filters(
    rows,
    customer=None,
    aging=None,
    overdue=None,
    company_group=None,
    company=None,
    as_of_date=None,
):
    if as_of_date is None:
        as_of_date = timezone.localdate()

    _prepare_rows_inplace(rows)

    customer_norm = (customer or "").strip().lower()
    company_norm = (company or "").strip().lower()
    cg_filter = (company_group or "").strip()

    overdue_norm = str(overdue or "").strip().lower()
    aging_norm = str(aging or "").strip()

    def match_aging(od: int, bucket: str) -> bool:
        try:
            d = int(od or 0)
        except Exception:
            d = 0

        if not bucket:
            return True

        if bucket == "Not due":
            return d <= 0
        if bucket == "0-30 days":
            return 0 < d <= 30
        if bucket == "31-60 days":
            return 30 < d <= 60
        if bucket == "61-90 days":
            return 60 < d <= 90
        if bucket == "91-120 days":
            return 90 < d <= 120
        if bucket == "121-180 days":
            return 120 < d <= 180
        if bucket == ">180 days":
            return d > 180
        return True

    out = []
    for r in rows:
        if _is_internal_transfer_party(r):
            continue

        # company substring filter (DB-side may already reduce, keep safe)
        if company_norm:
            if company_norm not in (r.get("_cn_norm") or ""):
                continue

        # company group filter
        if cg_filter and cg_filter != "ALL":
            cg = get_company_group(r.get("Company Name") or "")
            if cg != cg_filter:
                continue

        # customer filter (contains)
        if customer_norm:
            if customer_norm not in (r.get("_pn_norm") or ""):
                continue

        od = _compute_overdue_days(r, as_of_date)

        if overdue_norm == "overdue" and od <= 0:
            continue
        if overdue_norm == "not_overdue" and od > 0:
            continue

        if aging_norm and aging_norm.upper() not in ("ALL",):
            if not match_aging(od, aging_norm):
                continue

        r["Overdue Days"] = od
        r["days_overdue"] = od
        out.append(r)

    return out


# =============================================================================
# Dashboard context (FAST) + caching + raw lookup only for detail rows
# =============================================================================

def build_receivable_dashboard_context(filters):
    customer = (filters.get("customer") or "")
    aging = (filters.get("aging") or "")
    overdue = (filters.get("overdue") or "")
    company = (filters.get("company") or "")
    company_group = (filters.get("company_group") or "")

    # Status mapping (kept)
    overdue_norm = str(overdue or "").strip().lower()
    if not overdue_norm:
        status_ui = str(filters.get("status") or "").strip().lower()
        if status_ui in ("clean", "not overdue", "not_overdue", "not-overdue"):
            overdue = "not_overdue"
        elif status_ui in ("overdue",):
            overdue = "overdue"
        else:
            overdue = ""

    from_dt = _parse_ui_date(filters.get("from_date") or "")
    to_dt = _parse_ui_date(filters.get("to_date") or "")

    if not from_dt:
        from_dt = BASE_START_DATE

    as_of_date = to_dt or timezone.localdate()

    # all_mode logic (kept)
    show = str(filters.get("show") or "").strip().lower()
    include_all = str(filters.get("include_all") or "").strip().lower() in ("1", "true", "yes", "y")
    all_mode = include_all or (show in ("all",)) or (str(aging).strip().upper() == "ALL") or (str(overdue).strip().upper() == "ALL")

    snap = _latest_snapshot_date()

    # Dashboard cache key
    cache_key = "rcv_dash::" + hashlib.md5(
        json.dumps(
            {
                "snap": str(snap),
                "filters": {
                    "customer": customer,
                    "aging": aging,
                    "overdue": overdue,
                    "company": company,
                    "company_group": company_group,
                    "from_date": str(from_dt) if from_dt else "",
                    "to_date": str(to_dt) if to_dt else "",
                    "show": show,
                    "include_all": include_all,
                },
                "as_of": str(as_of_date),
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()

    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    # Fetch snapshot rows (DB-side reduced by customer/company, no JSON raw)
    raw_rows = fetch_receivables_raw_from_snapshot(
        snapshot_date=snap,
        include_all=all_mode,               # ALL when needed, else OPEN only
        as_of_date=as_of_date,
        customer=customer,
        company=company,
    )

    # Apply remaining filters (aging/overdue/group)
    rows = _apply_filters(
        raw_rows,
        customer=customer,
        aging=aging,
        overdue=overdue,
        company=company,
        company_group=company_group,
        as_of_date=as_of_date,
    )

    dec = Decimal
    total_bill = dec("0")
    total_paid = dec("0")
    total_os = dec("0")

    aging_buckets = {
        "0-30 days": dec("0"),
        "31-60 days": dec("0"),
        "61-90 days": dec("0"),
        "91-120 days": dec("0"),
        "121-180 days": dec("0"),
        ">180 days": dec("0"),
    }

    customer_out = defaultdict(Decimal)
    monthly_out = defaultdict(Decimal)
    detail_rows = []

    today = as_of_date
    week_start_current = today - timedelta(days=today.weekday())
    week_end_current = week_start_current + timedelta(days=6)

    prev_week_start = week_start_current - timedelta(days=7)
    prev_week_end = week_start_current - timedelta(days=1)

    prev_week_received = dec("0")
    this_week_incoming = []
    this_week_incoming_total = dec("0")

    # Track pk for detail raw lookup (only top N)
    detail_pks = []

    for r in rows:
        doc_date = r.get("_effective_dt")
        trn_dt = r.get("_trans_dt")

        period_doc_date = doc_date
        if from_dt and period_doc_date and period_doc_date < from_dt:
            period_doc_date = from_dt

        if from_dt and (period_doc_date is None or period_doc_date < from_dt):
            continue
        if to_dt and (period_doc_date is None or period_doc_date > to_dt):
            continue

        bill = _to_decimal(r.get("Bill Amt") or 0, default=dec("0"))
        paid = _to_decimal(r.get("Paid Amt") or 0, default=dec("0"))
        os_amt = _to_decimal(r.get("Outstanding Amt") or 0, default=dec("0"))

        total_bill += bill
        total_paid += paid
        total_os += os_amt

        od = _compute_overdue_days(r, today)

        if 0 < od <= 30:
            aging_buckets["0-30 days"] += os_amt
        elif 30 < od <= 60:
            aging_buckets["31-60 days"] += os_amt
        elif 60 < od <= 90:
            aging_buckets["61-90 days"] += os_amt
        elif 90 < od <= 120:
            aging_buckets["91-120 days"] += os_amt
        elif 120 < od <= 180:
            aging_buckets["121-180 days"] += os_amt
        elif od > 180:
            aging_buckets[">180 days"] += os_amt

        party_name = r.get("Party Name") or "Unknown"
        customer_out[party_name] += os_amt

        if period_doc_date:
            month_key = date(period_doc_date.year, period_doc_date.month, 1)
            monthly_out[month_key] += os_amt

        if trn_dt and prev_week_start <= trn_dt <= prev_week_end:
            prev_week_received += paid

        if doc_date and week_start_current <= doc_date <= week_end_current and os_amt > 0:
            this_week_incoming.append({
                "company_name": r.get("Company Name"),
                "party_code": r.get("Party Code"),
                "party_name": party_name,
                "invoice_number": r.get("Trans No") or "",
                "due_date": doc_date,
                "due_date_display": doc_date.strftime("%d-%b-%Y"),
                "outstanding_amt": os_amt,
            })
            this_week_incoming_total += os_amt

        if os_amt <= 0:
            status = "CLOSED"
        elif paid == 0:
            status = "OPEN"
        else:
            status = "PARTIAL"

        company_name = r.get("Company Name") or ""
        cg = get_company_group(company_name)

        due_display = (
            r.get("Overdue Date")
            or r.get("Due Date")
            or r.get("DueDate")
            or r.get("Trans Date")
            or ""
        )

        # Limit detail rendering (major speed improvement)
        if len(detail_rows) < MAX_DETAIL_ROWS:
            pk = r.get("_pk")
            if pk:
                detail_pks.append(pk)

            detail_rows.append({
                "_pk": pk,
                "company_name": company_name,
                "party_code": r.get("Party Code"),
                "party_name": party_name,

                "bill_amt": bill,
                "paid_amt": paid,
                "outstanding_amt": os_amt,
                "os_amt": os_amt,

                "trans_no": r.get("Trans No"),
                "trans_date": r.get("Trans Date"),

                "overdue_days": od,
                "days_overdue": od,
                "overdue_date": due_display,

                "payment_term": r.get("Payment Term"),
                "item_name": r.get("Item Name"),
                "location": r.get("Location"),
                "company_group": cg,

                # Will be filled from raw lookup (if available)
                "customer_po_date": "",
                "customer_po_no": "",

                "customer_name": party_name,
                "invoice_number": r.get("Trans No") or "",
                "invoice_date": r.get("Trans Date") or "",
                "due_date": due_display or "",
                "invoice_amount": bill,
                "received_amount": paid,
                "balance_amount": os_amt,
                "status": status,
            })

    # Fill PO fields from raw JSON only for detail rows displayed
    if detail_pks:
        raw_map = dict(
            ReceivableSnapshotRow.objects.filter(id__in=detail_pks)
            .values_list("id", "raw")
        )
        for dr in detail_rows:
            pk = dr.get("_pk")
            raw = raw_map.get(pk) or {}
            dr["customer_po_date"] = raw.get("Customer PO Date") or ""
            dr["customer_po_no"] = raw.get("Customer PO No.") or raw.get("Customer PO No") or ""
            dr.pop("_pk", None)

    this_week_incoming.sort(key=lambda x: (x["due_date"], (x["party_name"] or "")))

    total_os_crore = (total_os / dec("10000000")) if total_os else dec("0")
    summary = {
        "total_invoiced": total_bill,
        "total_received": total_paid,
        "total_outstanding": total_os,
        "total_bill_amt": total_bill,
        "total_paid_amt": total_paid,
        "total_os_amt": total_os,
        "total_os": total_os,
        "total_outstanding_crore": total_os_crore,
        "total_os_crore": total_os_crore,
    }

    aging_data = [{"aging_bucket": k, "bucket_label": k, "outstanding": v} for k, v in aging_buckets.items()]
    customer_data = [
        {"customer_name": n, "party_name": n, "outstanding": a, "outstanding_amount": a}
        for n, a in sorted(customer_out.items(), key=lambda kv: kv[1], reverse=True)[:20]
    ]
    monthly_data = [
        {"month": m, "month_label": m.strftime("%b %Y"), "outstanding": amt}
        for m, amt in sorted(monthly_out.items())
    ]

    result = {
        "summary": summary,
        "aging_data": aging_data,
        "customer_data": customer_data,
        "monthly_data": monthly_data,
        "previous_week_received": prev_week_received,
        "this_week_incoming": this_week_incoming,
        "this_week_incoming_total": this_week_incoming_total,
        "receivables": detail_rows,
        "as_of_date": as_of_date,
        "all_mode": all_mode,
    }

    cache.set(cache_key, result, CACHE_DASHBOARD_TTL)
    return result

# =============================================================================
# Payments / Targets Helpers (DB-Optimized for Weekly Targets)
# =============================================================================

from decimal import Decimal
from datetime import date

from django.db.models import Q, Sum
from django.utils import timezone

from ACCOUNTS.Receivable.models import ReceivableSnapshotRow
from ACCOUNTS.Receivable.services.receivables_sync import latest_snapshot_date


# -----------------------------
# small helpers
# -----------------------------
def _pick(row, *keys, default=""):
    for k in keys:
        v = row.get(k)
        if v is not None and str(v).strip() != "":
            return v
    return default


def _truthy(v):
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "pdc")


def _looks_like_cheque_mode(mode: str) -> bool:
    m = (mode or "").strip().lower()
    return ("cheque" in m) or ("chq" in m) or ("pdc" in m)


def _get_trans_date(row):
    dt = row.get("_trans_dt")
    if dt is not None:
        return dt
    return _parse_any_date(row.get("Trans Date") or "")


def _get_paid_amount(row):
    return (
        row.get("Paid Amt") if row.get("Paid Amt") is not None else
        row.get("Paid Amount") if row.get("Paid Amount") is not None else
        row.get("Received Amt") if row.get("Received Amt") is not None else
        row.get("Received Amount") if row.get("Received Amount") is not None else 0
    )


def _get_due_display_and_date(row):
    due_display = (
        row.get("Due Date")
        or row.get("Overdue Date")
        or row.get("DueDate")
        or row.get("Trans Date")
        or ""
    )
    return due_display, _parse_any_date(due_display)


def _bill_key(party_code, invoice_no):
    return f"{(party_code or '').strip().upper()}||{(invoice_no or '').strip().upper()}"


def _has_field(model, field_name: str) -> bool:
    try:
        model._meta.get_field(field_name)
        return True
    except Exception:
        return False


# -----------------------------
# Company Group -> DB Q() filter
# (matches your get_company_group() logic)
# -----------------------------
def _company_group_q(company_group: str) -> Q:
    cg = (company_group or "ALL").strip().upper()

    q_ocspl = Q(company_name__icontains="oc special") | Q(company_name__icontains="ocspl")
    q_occhem = Q(company_name__icontains="occhem") | Q(company_name__icontains="oc chem")

    if cg in ("", "ALL"):
        return Q()  # no filter
    if cg == "OCSPL":
        return q_ocspl
    if cg == "OCCHEM":
        return q_occhem
    if cg == "OTHER":
        return ~(q_ocspl | q_occhem)

    # unknown group -> no filter (safe)
    return Q()


def _snapshot_qs(snapshot_date=None):
    snap = snapshot_date or latest_snapshot_date()
    if not snap:
        return ReceivableSnapshotRow.objects.none()
    return ReceivableSnapshotRow.objects.filter(snapshot_date=snap)


def _get_all_rows_fallback():
    fn = globals().get("fetch_receivables_raw_all")
    if callable(fn):
        return fn()
    return fetch_receivables_raw()


# =============================================================================
# FAST: Paid lookup for week (DB aggregation)
# =============================================================================
def build_paid_lookup_for_period(company_group="ALL", *, start_date=None, end_date=None, snapshot_date=None):
    """
    Returns dict: {"PARTYCODE||INVNO": Decimal(total_paid_in_period)}
    FAST path (preferred): DB aggregation on ReceivableSnapshotRow
    Fallback path: python scan of raw rows (slow)
    """
    start_dt = _to_date_obj(start_date)
    end_dt = _to_date_obj(end_date)

    # DB path only if we have required columns
    has_paid = _has_field(ReceivableSnapshotRow, "paid_amt")
    has_party = _has_field(ReceivableSnapshotRow, "party_code")
    has_inv = _has_field(ReceivableSnapshotRow, "trans_no") or _has_field(ReceivableSnapshotRow, "trans_no_display")  # safe
    has_trn_dt = _has_field(ReceivableSnapshotRow, "trans_date")

    if has_paid and has_party and has_inv and has_trn_dt:
        qs = _snapshot_qs(snapshot_date).filter(_company_group_q(company_group))
        if start_dt and end_dt:
            qs = qs.filter(trans_date__range=(start_dt, end_dt))
        elif start_dt:
            qs = qs.filter(trans_date__gte=start_dt)
        elif end_dt:
            qs = qs.filter(trans_date__lte=end_dt)

        qs = qs.filter(paid_amt__gt=0)

        # group by party_code + trans_no
        agg = (
            qs.values("party_code", "trans_no")
              .annotate(paid_total=Sum("paid_amt"))
        )

        out = {}
        for r in agg.iterator(chunk_size=5000):
            key = _bill_key(r.get("party_code"), r.get("trans_no"))
            out[key] = (r.get("paid_total") or Decimal("0"))
        return out

    # -----------------
    # Fallback (slow)
    # -----------------
    rows = _get_all_rows_fallback()

    lookup = {}
    for r in rows:
        cg = get_company_group(r.get("Company Name") or "")
        if company_group and company_group != "ALL" and cg != company_group:
            continue

        trn_dt = _get_trans_date(r)
        if not trn_dt:
            continue
        if start_dt and trn_dt < start_dt:
            continue
        if end_dt and trn_dt > end_dt:
            continue

        paid = _to_decimal(_get_paid_amount(r) or 0)
        if paid <= 0:
            continue

        party_code = (r.get("Party Code") or "").strip()
        invoice_no = (r.get("Trans No") or "").strip()
        key = _bill_key(party_code, invoice_no)

        lookup[key] = lookup.get(key, Decimal("0")) + paid

    return lookup


# =============================================================================
# FAST: All receipts rows for week (DB filter + values)
# =============================================================================
def get_received_rows_for_period(company_group="ALL", *, start_date=None, end_date=None, snapshot_date=None):
    """
    Returns list of dicts (same shape you were using) for receipts in period.
    FAST path: DB filtered rows on paid_amt + trans_date.
    Fallback: python scan of raw rows (slow).
    """
    start_dt = _to_date_obj(start_date)
    end_dt = _to_date_obj(end_date)

    has_paid = _has_field(ReceivableSnapshotRow, "paid_amt")
    has_trn_dt = _has_field(ReceivableSnapshotRow, "trans_date")
    has_cols = all(_has_field(ReceivableSnapshotRow, f) for f in [
        "company_name", "party_code", "party_name", "trans_no"
    ])

    # DB path
    if has_paid and has_trn_dt and has_cols:
        qs = _snapshot_qs(snapshot_date).filter(_company_group_q(company_group))
        if start_dt and end_dt:
            qs = qs.filter(trans_date__range=(start_dt, end_dt))
        elif start_dt:
            qs = qs.filter(trans_date__gte=start_dt)
        elif end_dt:
            qs = qs.filter(trans_date__lte=end_dt)

        qs = qs.filter(paid_amt__gt=0)

        # Pull only needed columns (fast)
        # trans_date_display may exist; if not, you can format trans_date in template
        fields = ["company_name", "party_code", "party_name", "trans_no", "paid_amt"]
        if _has_field(ReceivableSnapshotRow, "trans_date_display"):
            fields.append("trans_date_display")
        else:
            fields.append("trans_date")

        data = list(qs.values(*fields).order_by("party_name", "trans_no")[:200000])

        out = []
        for r in data:
            out.append({
                "company_name": r.get("company_name") or "",
                "party_code": r.get("party_code") or "",
                "party_name": r.get("party_name") or "",
                "invoice_no": r.get("trans_no") or "",
                "trans_date": r.get("trans_date_display") or (r.get("trans_date").strftime("%d-%b-%Y") if r.get("trans_date") else ""),
                "paid_amount": r.get("paid_amt") or Decimal("0"),
                # keep compatibility
                "bill_amount": Decimal("0"),
                "outstanding_amount": Decimal("0"),
                "bill_key": _bill_key(r.get("party_code"), r.get("trans_no")),
            })
        return out

    # Fallback (slow)
    start_dt = _to_date_obj(start_date)
    end_dt = _to_date_obj(end_date)

    rows = _get_all_rows_fallback()

    out = []
    for r in rows:
        cg = get_company_group(r.get("Company Name") or "")
        if company_group and company_group != "ALL" and cg != company_group:
            continue

        trn_dt = _get_trans_date(r)
        if not trn_dt:
            continue
        if start_dt and trn_dt < start_dt:
            continue
        if end_dt and trn_dt > end_dt:
            continue

        paid = _to_decimal(_get_paid_amount(r) or 0)
        if paid <= 0:
            continue

        out.append({
            "company_name": r.get("Company Name") or "",
            "party_code": r.get("Party Code") or "",
            "party_name": r.get("Party Name") or "",
            "invoice_no": r.get("Trans No") or "",
            "trans_date": r.get("Trans Date") or "",
            "paid_amount": paid,
            "bill_amount": _to_decimal(r.get("Bill Amt") or 0),
            "outstanding_amount": _to_decimal(r.get("Outstanding Amt") or 0),
            "bill_key": _bill_key(r.get("Party Code"), r.get("Trans No")),
        })

    out.sort(key=lambda x: ((x.get("party_name") or ""), (x.get("invoice_no") or "")))
    return out


# =============================================================================
# OPTIONAL SPEEDUP: Open bills for week (DB filter if due_date/outstanding columns exist)
# =============================================================================
def get_open_bills_for_party(party_code=None, party_name=None, company_group="ALL", from_date=None, to_date=None, snapshot_date=None):
    """
    OPEN bills selection for week.

    If your snapshot model has:
      - outstanding_amt (Decimal)
      - due_date (Date)
      - party_code / party_name / company_name / trans_no
    this becomes DB-fast.

    Otherwise fallback to existing fetch_receivables_raw() scan (slow).
    """
    pc = _norm(party_code)
    pn = _norm(party_name)

    from_dt = _to_date_obj(from_date)
    to_dt = _to_date_obj(to_date)

    has_os = _has_field(ReceivableSnapshotRow, "outstanding_amt")
    has_due = _has_field(ReceivableSnapshotRow, "due_date")
    has_cols = all(_has_field(ReceivableSnapshotRow, f) for f in [
        "company_name", "party_code", "party_name", "trans_no", "bill_amt"
    ])

    if has_os and has_due and has_cols:
        qs = _snapshot_qs(snapshot_date).filter(_company_group_q(company_group))
        qs = qs.filter(outstanding_amt__gt=Decimal("0.0001"))

        # weekly logic: exclude only if due_date AFTER week_end
        if to_dt:
            qs = qs.filter(due_date__lte=to_dt)

        # party filter
        if pc:
            qs = qs.filter(party_code__istartswith=pc)  # supports prefix search like your earlier logic
        elif pn:
            qs = qs.filter(party_name__icontains=pn)

        qs = qs.order_by("due_date", "trans_no")

        out = []
        for o in qs.iterator(chunk_size=5000):
            # if due_date earlier than week_start, show week_start as effective
            due_dt_effective = o.due_date
            if from_dt and due_dt_effective and due_dt_effective < from_dt:
                due_dt_effective = from_dt

            out.append({
                "company_name": o.company_name or "",
                "party_code": o.party_code or "",
                "party_name": o.party_name or "",
                "invoice_no": o.trans_no or "",
                "invoice_date": getattr(o, "trans_date_display", "") or "",
                "due_date": getattr(o, "due_date_display", "") or (o.due_date.strftime("%d-%b-%Y") if o.due_date else ""),
                "due_date_dt": due_dt_effective,
                "bill_amount": (o.bill_amt or Decimal("0")),
                "outstanding_amount": (o.outstanding_amt or Decimal("0")),
            })

        # keep same sort output
        out.sort(key=lambda x: (x["due_date_dt"] or date.max, (x["invoice_no"] or "")))
        return out

    # -----------------
    # Fallback (slow)
    # -----------------
    rows = fetch_receivables_raw()  # OPEN external only

    codes = set()
    if pc:
        for r in rows:
            codes.add(_norm(r.get("Party Code")))
    has_exact_code = pc in codes if pc else False

    out = []
    for r in rows:
        cg = get_company_group(r.get("Company Name") or "")
        if company_group and company_group != "ALL" and cg != company_group:
            continue

        r_pc = _norm(r.get("Party Code"))
        r_pn = _norm(r.get("Party Name"))

        if pc:
            if has_exact_code:
                if r_pc != pc:
                    continue
            else:
                if not r_pc.startswith(pc):
                    continue
        elif pn:
            if pn not in r_pn:
                continue

        os_amt = _to_decimal(r.get("Outstanding Amt") or 0)
        if abs(os_amt) <= Decimal("0.0001"):
            continue

        due_display, due_dt = _get_due_display_and_date(r)
        if to_dt and (due_dt is None or due_dt > to_dt):
            continue

        due_dt_effective = due_dt
        if from_dt and due_dt and due_dt < from_dt:
            due_dt_effective = from_dt

        bill_amt = _to_decimal(r.get("Bill Amt") or 0)

        out.append({
            "company_name": r.get("Company Name") or "",
            "party_code": r.get("Party Code") or "",
            "party_name": r.get("Party Name") or "",
            "invoice_no": r.get("Trans No") or "",
            "invoice_date": r.get("Trans Date") or "",
            "due_date": due_display,
            "due_date_dt": due_dt_effective,
            "bill_amount": bill_amt,
            "outstanding_amount": os_amt,
        })

    out.sort(key=lambda x: (x["due_date_dt"] or date.max, (x["invoice_no"] or "")))
    return out


def get_open_bills_for_period(company_group="ALL", from_date=None, to_date=None, *, start_date=None, end_date=None, party_code=None, party_name=None):
    if start_date is None and from_date is not None:
        start_date = from_date
    if end_date is None and to_date is not None:
        end_date = to_date

    return get_open_bills_for_party(
        party_code=party_code,
        party_name=party_name,
        company_group=company_group or "ALL",
        from_date=start_date,
        to_date=end_date,
    )


def get_open_parties_for_period(company_group="ALL", start_date=None, end_date=None):
    rows = get_open_bills_for_period(
        company_group=company_group,
        start_date=start_date,
        end_date=end_date,
        party_code=None,
        party_name=None,
    )

    seen = set()
    out = []
    for r in rows:
        code = (r.get("party_code") or "").strip()
        name = (r.get("party_name") or "").strip()
        if not code and not name:
            continue
        key = (code.upper(), name.lower())
        if key in seen:
            continue
        seen.add(key)
        out.append({"party_code": code, "party_name": name})

    out.sort(key=lambda x: (x["party_name"].lower(), x["party_code"].upper()))
    return out


# =============================================================================
# Receivable entries for period (kept as-is; still python scan)
# NOTE: If this is slow, we should store cheque/pdc fields into model columns at sync time.
# =============================================================================
def get_receivable_entries_for_period(company_group="ALL", *, start_date=None, end_date=None):
    start_dt = _to_date_obj(start_date)
    end_dt = _to_date_obj(end_date)

    rows = fetch_receivables_raw_all()

    out = []
    for r in rows:
        cg = get_company_group(r.get("Company Name") or "")
        if company_group and company_group != "ALL" and cg != company_group:
            continue

        trn_dt = _get_trans_date(r)
        if not trn_dt:
            continue
        if start_dt and trn_dt < start_dt:
            continue
        if end_dt and trn_dt > end_dt:
            continue

        pay_mode = str(_pick(r, "Pay Mode", "Payment Mode", "Instrument Type", "Mode", default="")).strip()
        cheque_no = str(_pick(r, "Cheque No", "Chq No", "ChequeNo", "Instrument No", "InstrumentNo", "Ref No", default="")).strip()
        cheque_date = str(_pick(r, "Cheque Date", "Chq Date", "ChequeDate", "Instrument Date", "InstrumentDate", default="")).strip()

        is_pdc = _truthy(_pick(r, "Is PDC", "PDC", "PDC Flag", default=""))
        pdc_date = str(_pick(r, "PDC Date", "Pdc Date", "PDCDate", default="")).strip()

        received_amt = _to_decimal(_pick(r, "Paid Amt", "Received Amt", "Receipt Amount", default=0))
        pdc_amount = _to_decimal(_pick(r, "PDC Amount", "Cheque Amount", "Instrument Amount", default=0))

        is_cheque_entry = bool(cheque_no) or bool(cheque_date) or is_pdc or _looks_like_cheque_mode(pay_mode)
        if (received_amt <= 0) and (not is_cheque_entry):
            continue

        receipt_no = str(_pick(r, "Receipt No", "Voucher No", "Bank Receipt No", "ReceiptNo", default="")).strip()
        if not receipt_no:
            receipt_no = str(_pick(r, "Trans No", default="")).strip()

        out.append({
            "company_name": r.get("Company Name") or "",
            "party_code": r.get("Party Code") or "",
            "party_name": r.get("Party Name") or "",
            "receipt_no": receipt_no,
            "receipt_date": r.get("Trans Date") or "",
            "pay_mode": pay_mode or "-",
            "cheque_no": cheque_no or "-",
            "cheque_date": cheque_date or "-",
            "is_pdc": bool(is_pdc),
            "pdc_date": pdc_date or "-",
            "received_amount": (received_amt if received_amt > 0 else None),
            "pdc_amount": (pdc_amount if pdc_amount > 0 else None),
        })

    out.sort(key=lambda x: ((x.get("party_name") or ""), (x.get("receipt_no") or "")))
    return out
# =============================================================================