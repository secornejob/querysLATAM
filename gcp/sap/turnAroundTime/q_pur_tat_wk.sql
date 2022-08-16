with 
  tat_compras as (
    select
      extract (isoweek from pl.po_created_dt) as semana,
      date_diff(pl.po_created_dt, pl.req_created_dt, day) as tat,
      pl.po_created_dt,
      pl.req_created_dt,
      pl.req_number,
    from `sp-ed-opssupplyanalytics-dev.supply_sap.po_line_helper` as pl
    where extract(year from pl.po_created_dt) = 2022
      and pl.po_purchase_group like 'P%'
      and pl.po_doc_type in ('ZPAM', 'ZAOG', 'ZDAF', 'ZPAA')
      and pl.po_ln_deletion_indicator not like 'L'
      and pl.req_created_dt is not null
  )

select 
  semana,
  sum(tat) as tat_total_days,
  count(tat) as tat_records,
  avg(tat) as avg_tat,
from tat_compras
group by semana
