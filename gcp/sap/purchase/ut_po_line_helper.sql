truncate table `sp-ed-opssupplyanalytics-dev.supply_sap.po_line_helper`;
insert into `sp-ed-opssupplyanalytics-dev.supply_sap.po_line_helper`

with 
  ekko as (
    select distinct
      ekko.EBELN,
      ekko.ERNAM,
      ekko.BEDAT,
      ekko.BSART,
      ekko.LIFNR,
      ekko.ZTERM,
      ekko.EKORG,
      ekko.EKGRP,
      ekko.WAERS,
      ekko.ZZACK,
    from `dlakedomain-prod-20dl.maintenance_brownfield_vwt_us.ekko` as ekko -- encabezado PO
    -- where ekko.EBELN like '0000358023'
  ),
  
  mbew as (
    select distinct 
      mbew.MATNR,
      mbew.BKLAS,
    from `dlakedomain-prod-20dl.maintenance_brownfield_vwt_us.mbew` as mbew
  ),

  po_line_helper as (
    select distinct
      ekko.EBELN as po_number,
      ekko.ERNAM as po_created_by,
      ekko.BEDAT as po_created_dt,
      ekko.BSART as po_doc_type,
      ekko.LIFNR as po_vendor_code,
      lfa1.NAME1 as po_vendor_name,
      ekko.ZTERM as po_term_of_payments,
      ekko.EKORG as po_purchase_org,
      ekko.EKGRP as po_purchase_group,
      ekko.WAERS as po_currency,
      ekko.ZZACK as po_ack,
      ekpo.EBELP as po_ln_number,
      ekpo.LOEKZ as po_ln_deletion_indicator,
      ekpo.MATNR as po_ln_material,
      ekpo.TXZ01 as po_ln_shor_text,
      ekpo.MATKL as po_ln_material_group,
      ekpo.WERKS as po_ln_location,
      ekpo.MENGE as po_ln_qty,
      ekpo.MEINS as po_ln_unit,
      ekpo.NETPR as po_ln_net_price,
      ekpo.NETWR as po_ln_net_worth,
      eket.EINDT as po_ln_delivery_dt,
      eket.WEMNG as po_ln_delivered_qty,
      eban.BANFN as req_number,
      eban.BNFPO as req_ln,
      eban.MATNR as req_material,
      eban.MFRNR as req_manufacturer,
      eban.MPROF as req_manufacturer_part_profile,
      eban.MENGE as req_qty,
      eban.ERNAM as req_created_by,
      eban.BADAT as req_created_dt,
      eban.FRGDT as req_released_dt,
      eban.LFDAT as req_delivery_dt,
      eban.WERKS as req_location,
      eban.BSART as req_doc_type,
      mbew.BKLAS as mt_valuation_class,
      current_datetime("America/Santiago") as created_at,
      current_datetime("America/Santiago") as updated_at,

    from ekko -- encabezado PO
    left join `dlakedomain-prod-20dl.maintenance_brownfield_vwt_us.ekpo` as ekpo -- linea PO
      on ekpo.EBELN = ekko.EBELN -- po_number
    left join `dlakedomain-prod-20dl.maintenance_brownfield_vwt_us.eban` as eban -- requisiciones
      on eban.EBELN = ekpo.EBELN -- po_number
      and cast(eban.EBELP as int) = cast(ekpo.EBELP as int) -- po_ln_number
    left join `dlakedomain-prod-20dl.maintenance_brownfield_vwt_us.lfa1` as lfa1 -- proveedores
      on lfa1.LIFNR = ekko.LIFNR -- po_vendor_code
    left join `dlakedomain-prod-20dl.maintenance_brownfield_vwt_us.eket` as eket -- recepciones
      on eket.EBELN = ekpo.EBELN -- po_number
      and cast(eket.EBELP as int) = cast(ekpo.EBELP as int) -- po_ln_number
    left join mbew
      on mbew.MATNR = ekpo.MATNR
    -- where (
    --     ekko.EKGRP like 'P%' -- po_purchase_group
    --     or ekko.EKGRP like 'R%' -- po_purchase_group
    --   )
  )


select *
from po_line_helper

