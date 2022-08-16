
with
  last_po_req as (
    select 
      max(ee.CREATION_DT) as creation_dt,
      ee.EVENT_ID, 
      ee.EVENT_DB_ID,
    from `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.evt_event` as ee
    where ee.EVENT_STATUS_CD like 'PRPOREQ'
    group by 
      ee.EVENT_ID,
      ee.EVENT_DB_ID
  ),

  purchase_tat as (
    select 
      po.po_creation_dt,
      rp.CREATION_DT as pr_creation_dt,
      date_diff(po.po_creation_dt, rp.CREATION_DT, day) as tat,
      extract(isoweek from po.po_creation_dt) as wk,
      rp.req_type_cd,
      ee.EVENT_SDESC as part_request,
      po.po_code,
      row_number() over (partition by po.po_id, po.po_db_id order by po.po_creation_dt desc, pl.po_ln_creation_dt desc, rp.CREATION_DT asc) as rn
    from `sp-ed-opssupplyanalytics-dev.supply_mxi.po_header_helper` as po
    left join  `sp-ed-opssupplyanalytics-dev.supply_mxi.po_line_helper` as pl
      on po.po_id = pl.po_id
      and po.po_db_id = pl.po_db_id
    left join `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.req_part` as rp
      on rp.PO_DB_ID = po.po_db_id
      and rp.PO_ID = po.po_id
      and rp.PO_LINE_ID = pl.po_line_id
    left join `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.evt_event` as ee
      on ee.EVENT_ID = rp.REQ_PART_ID
      and ee.EVENT_DB_ID = rp.REQ_PART_DB_ID
    left join `sp-ed-opssupplyanalytics-dev.supply_mxi.temp_bp_team` as team
      on po.po_bp_owner_code = team.bp_code
    where extract(year from po.po_creation_dt) = 2022
      and po.po_type like 'PURCHASE'
      and po.po_status not in ('POCANCEL')
      and team.team in ('COMPRAS PROGRAMADAS')
      and rp.req_type_cd like 'STOCK'
  )


select 
  wk,
  sum(tat) as tat_total_days,
  count(tat) as tat_records,
  avg(tat) as avg_tat,
from purchase_tat
where rn = 1
  and pr_creation_dt is not null
group by wk

