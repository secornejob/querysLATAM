with 
  po_ln_helper as (
    select 
      po.po_number,
      po.po_ln_number,
      po.req_number,
      po.po_ln_material,
      po.po_purchase_group,
      po.po_doc_type,
      case when po.po_ln_qty <= po.po_ln_delivered_qty then 'open' else 'close' end as po_status
    from `sp-ed-opssupplyanalytics-dev.supply_sap.po_line_helper` as po
  )

select 
  eban.BANFN as req_number,
  eban.ERNAM as req_created_by,
  eban.BADAT as req_created_dt,
  eban.FRGDT as req_released_dt,
  eban.LFDAT as req_delivery_dt,
  eban.MATNR as req_part_number,
  eban.EKGRP as req_purchase_group,
  eban.EBELN as req_po_number,
  case 
    when po.po_purchase_group like 'A%' then 'AOG_Desk'
    when po.po_purchase_group like 'E%' then 'Check'
    when po.po_purchase_group like 'M%' then 'Major_Parts'
    when po.po_purchase_group like 'P%' then 'Compras_Planejadas'
    when po.po_purchase_group like 'Q%' then 'unificado_com_E'
    when po.po_purchase_group like 'R%' then 'Reparos'
    when po.po_purchase_group like 'S%' then 'Motores'
    when po.po_purchase_group like 'J%' then 'Projetos'
    else 'otros'
  end as po_purchase_group,
  po.po_number,
  po.po_status,
  zs.ppn,
  po_doc_type
FROM `dlakedomain-prod-20dl.maintenance_brownfield_vwt_us.eban` eban
inner join `sp-ed-opssupplyanalytics-dev.supply_sap.temp_pn_purchase_zero_stock` zs
  on zs.pn = eban.MATNR
left join po_ln_helper as po
  on po.po_number = eban.EBELN
  and cast(po.po_ln_number as int) = cast(eban.EBELP as int)
where eban.LOEKZ not like 'L'
  and eban.LFDAT like '202%'
