
select 
  extract (isoweek from pl.po_created_dt) as semana,
  count(*) as cantidad
from `sp-ed-opssupplyanalytics-dev.supply_sap.po_line_helper` as pl
where extract(year from pl.po_created_dt) = 2022
  and pl.po_purchase_group like 'P%'
  and pl.po_doc_type in ('ZPAM', 'ZAOG', 'ZDAF', 'ZPAA')
  and pl.po_ln_deletion_indicator not like 'L'
group by extract (isoweek from pl.po_created_dt)
