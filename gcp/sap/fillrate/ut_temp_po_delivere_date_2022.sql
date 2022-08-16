create or replace table `sp-ed-opssupplyanalytics-dev.supply_sap.temp_po_delivere_date_2022` as 

with 
  principal as (
    SELECT 
      po_number,
      po_created_by,
      po_created_dt,
      po_ln_number,
      po_ln_shor_text,
      po_ln_qty	,
      po_ln_net_price,
      po_ln_net_worth,
      po_ln_material,
      po_doc_type,
      po_vendor_code, 
      po_vendor_name,
      cast(po.po_ln_delivery_dt as date format 'YYYYMMDD') as po_ln_delivery_dt,
      po.po_ln_delivered_qty,
      po.po_ln_delivered_qty * (po_ln_net_worth/po_ln_qty) as po_ln_deliveried_net_worth
    FROM `sp-ed-opssupplyanalytics-dev.supply_sap.po_line_helper` as po
    where po.po_purchase_group like 'P%'
      and po.po_ln_delivery_dt is not null
      and po.po_ln_delivery_dt like '2022%'
      -- and cast(po.po_ln_delivery_dt as date format 'YYYYMMDD') between '2022-01-01' and current_date()
      and po.po_doc_type not in ('ZPT')
      and po.po_ln_deletion_indicator not like 'L'
      and po.po_ln_qty >= 1
  )


select *,
  case when po_ln_delivery_dt >= current_date() - 1 then 'OnTime' else 'Delay' end as po_dt_status,
  case when po_ln_delivered_qty = po_ln_qty then 'cerrada' else 'abierta' end as po_ln_status,
  case when po_ln_delivered_qty = 0 then 0 else 1 end as po_delivered_flag,
  1 as po_flag
from principal

