
select 
  extract(isoweek from po.po_creation_dt) as week,
  count(*) as cantidad_MXI
from `sp-ed-opssupplyanalytics-dev.supply_mxi.po_header_helper` as po
left join  `sp-ed-opssupplyanalytics-dev.supply_mxi.po_line_helper` as pl
  on po.po_id = pl.po_id
  and po.po_db_id = pl.po_db_id
left join `sp-ed-opssupplyanalytics-dev.supply_mxi.temp_bp_team` as team
  on po.po_bp_owner_code = team.bp_code
where extract(year from po.po_creation_dt) = 2022
  and po.po_type like 'PURCHASE'
  and po.po_status not in ('POCANCEL')
  and team.team in ('COMPRAS PROGRAMADAS')
group by extract(isoweek from po.po_creation_dt)

