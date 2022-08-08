with 
  tat_reparos as (
    select distinct
      po.po_code,
      extract(isoweek from dt.first_issued_dt) as week,
      date_diff(dt.first_issued_dt, po.po_creation_dt, day) as tat,
      po.po_creation_dt,
      dt.first_issued_dt,
      pl.part_no_oem,
      pl.pl_part_no_sdesc,
      po.vendor_cd,
      po.vendor_name,
    from `sp-ed-opssupplyanalytics-dev.supply_mxi.po_header_helper` as po
    left join `sp-ed-opssupplyanalytics-dev.supply_mxi.po_line_helper` as pl
      on po.po_id = pl.po_id
      and po.po_db_id = pl.po_db_id
    left join `sp-ed-opssupplyanalytics-dev.supply_mxi.evt_helper_dt` as dt
      on po.po_code = dt.po_code
    left join `sp-ed-opssupplyanalytics-dev.supply_mxi.temp_bp_team` as team
      on po.po_bp_owner_code = team.bp_code
    where extract(year from dt.first_issued_dt) = 2022
      and po.po_type like 'REPAIR'
      and po.po_status not in ('POCANCEL')
      and team.team in ('REPAROS')
  )


select *
from tat_reparos
