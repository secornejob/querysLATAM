create or replace table `sp-ed-opssupplyanalytics-dev.supply_mxi.po_line_helper` as

select 
  po.po_id,
  po.po_db_id,
  po.po_code, 
  pl.po_line_id,
  pl.part_no_id,
  pl.part_no_db_id,
  pn.PART_NO_OEM as part_no_oem,
  case 
    when pn.PART_NO_SDESC is not null then pn.PART_NO_SDESC
    else pl.line_ldesc
  end as pl_part_no_sdesc,
  pl.ORDER_QT as pl_order_qty,
  pl.RECEIVED_QT as pl_received_qty,
  pl.QTY_UNIT_CD as pl_unit_cd,
  pl.UNIT_PRICE as po_ln_unit_price,
  pl.LINE_PRICE as po_ln_line_price,
  pl.PROMISE_BY_DT as po_ln_promise_by_dt,
  pl.ORIG_PROMISE_BY_DT as po_ln_orig_promise_by_dt,
  pl.RETURN_BY_DT as po_ln_return_by_dt,
  pl.CREATION_DT as po_ln_creation_dt,

from `sp-ed-opssupplyanalytics-dev.supply_mxi.po_header_helper` as po
left join `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.po_line` as pl
  on po.po_id = pl.po_id
  and po.po_db_id = pl.po_db_id
left join `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.eqp_part_no` as pn
  on pl.PART_NO_ID = pn.PART_NO_ID
  and pl.PART_NO_DB_ID = pn.PART_NO_DB_ID