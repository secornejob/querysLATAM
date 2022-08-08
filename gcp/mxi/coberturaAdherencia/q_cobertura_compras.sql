select 
  po.po_code,
  po.vendor_cd,
  po.vendor_name,
  po.po_type,
  date(po.po_creation_dt) as po_creation_dt,
  pl.po_line_id,
  pl.part_no_oem,
  pl.pl_part_no_sdesc,
  po.po_bp_owner_code,
  po.auth_status_cd,
  po.po_auth_flow_cd,
  date (evt.first_auth_dt) as first_auth_dt,
  date(evt.first_issued_dt) as first_issued_dt

from `sp-ed-opssupplyanalytics-dev.supply_mxi.po_header_helper` as po
left join `sp-ed-opssupplyanalytics-dev.supply_mxi.po_line_helper` as pl
  on pl.po_code = po.po_code
left join `sp-ed-opssupplyanalytics-dev.supply_mxi.evt_helper_dt` as evt
  on evt.po_code = po.po_code
left join `sp-ed-opssupplyanalytics-dev.supply_mxi.temp_bp_team` as team
  on team.bp_code = po.po_bp_owner_code
where date(po.po_creation_dt) between '2022-01-01' and '2022-07-30'
  and po.po_type like 'PURCHASE'
  and pl.part_no_oem is not null
  and team.team in ('COMPRAS PROGRAMADAS', 'REPAROS')
