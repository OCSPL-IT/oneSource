# ACCOUNTS/services/receivables_service.py
"""
Receivables service layer (ERP SQL + row preparation helpers).

This module exists because your project has BOTH:
  - ACCOUNTS/services.py        (a module)
  - ACCOUNTS/services/          (a package)

Python will always import the package when you do: `import ACCOUNTS.services`,
so anything used by management commands MUST live inside ACCOUNTS/services/.

Sync command should import from here:
    from ACCOUNTS.services.receivables_service import RECEIVABLES_SQL, _prepare_rows_inplace, _is_internal_transfer_party
"""

from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List, Optional

# Company group mapping used for fast DB filtering
# Fill with your exact company_name values that exist in ReceivableSnapshotRow.company_name
COMPANY_GROUPS = {
    "OCSPL": [
        # "OC Specialities Pvt. Ltd.",
        # "OC Specialities",
    ],
    "OCCHEM": [
        # "OC Chem",
        # "OC Chemicals",
    ],
}

# ---------------------------------------------------------------------
# ERP Receivables SQL (Working version + ONLY Instrument No added)
# ---------------------------------------------------------------------
RECEIVABLES_SQL = r"""
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
Select * into #vTXNCF1 from vTxnCf1
;

-- ============================================================
-- ✅ ONLY ADD: Instrument Number (Cheque/Instrument No) from TXNCF
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
    isnull(inst.[Instrument No],'') as [Instrument No],   -- ✅ ONLY ADD
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
left join #vTXNCF_INST as inst on da.lId=inst.lId     -- ✅ ONLY ADD
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
    [Instrument No],                 -- ✅ ONLY ADD
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

# ---------------------------------------------------------------------
# Helpers for date parsing used by row preparation
# ---------------------------------------------------------------------
def _parse_ui_date(value: str) -> Optional[date]:
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


def _parse_sql_display_date(value: str) -> Optional[date]:
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None

    formats = [
        "%d %b %Y",    # 08 Dec 2025
        "%d-%b-%Y",    # 08-Dec-2025
        "%d %B %Y",    # 08 December 2025
        "%Y-%m-%d",    # 2025-12-08
        "%d/%m/%Y",    # 08/12/2025
        "%Y%m%d",      # 20251208
    ]

    for fmt in formats:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _parse_any_date(s: str) -> Optional[date]:
    if not s:
        return None
    return _parse_sql_display_date(s) or _parse_ui_date(s)


# ---------------------------------------------------------------------
# Row normalization (same behavior as your current code)
# ---------------------------------------------------------------------
_INTERNAL_ROW_KEYS = (
    "_pn_norm", "_pc_norm", "_grp_norm", "_cn_norm",
    "_trans_dt", "_due_dt", "_overdue_dt", "_effective_dt",
)


def _row_prepare(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Attach normalized fields + parsed dates to a raw ERP row.
    Does NOT change visible output keys; only adds internal keys.
    """
    get = row.get

    party_name = (get("Party Name") or "")
    party_code = (get("Party Code") or "")
    group_name = (get("GroupName") or get("Group Name") or "")
    company_name = (get("Company Name") or "")

    row["_pn_norm"] = party_name.strip().lower()
    row["_pc_norm"] = party_code.strip().lower()
    row["_grp_norm"] = group_name.strip().upper()
    row["_cn_norm"] = company_name.strip()

    trans_str = (get("Trans Date") or "")
    od_str = (get("Overdue Date") or "")
    due_str = (get("Due Date") or get("DueDate") or "")

    trans_dt = _parse_any_date(trans_str) if trans_str else None
    overdue_dt = _parse_any_date(od_str) if od_str else None
    due_dt = _parse_any_date(due_str) if due_str else None

    row["_trans_dt"] = trans_dt
    row["_overdue_dt"] = overdue_dt
    row["_due_dt"] = due_dt
    row["_effective_dt"] = overdue_dt or trans_dt

    return row


def _prepare_rows_inplace(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Prepare all rows only once.
    """
    for r in rows:
        if "_pn_norm" not in r:
            _row_prepare(r)
    return rows


# ---------------------------------------------------------------------
# Internal transfer filtering (same behavior as your current code)
# ---------------------------------------------------------------------
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


def _is_internal_transfer_party(row: Dict[str, Any]) -> bool:
    name = (row.get("_pn_norm") or (row.get("Party Name") or "").strip().upper()).upper()
    code = (row.get("_pc_norm") or (row.get("Party Code") or "").strip().upper()).upper()
    group = (row.get("_grp_norm") or (row.get("GroupName") or row.get("Group Name") or "").strip().upper()).upper()

    if code in INTERNAL_TRANSFER_CODES:
        return True
    if code.startswith("BARNH"):
        return True

    for kw in INTERNAL_TRANSFER_KEYWORDS:
        if kw in code or kw in name or kw in group:
            return True

    return False


__all__ = [
    "RECEIVABLES_SQL",
    "_prepare_rows_inplace",
    "_is_internal_transfer_party",
    "_parse_ui_date",
    "_parse_sql_display_date",
    "_parse_any_date",
]
