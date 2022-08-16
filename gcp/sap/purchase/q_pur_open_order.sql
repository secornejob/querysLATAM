SELECT *
FROM `sp-ed-opssupplyanalytics-dev.supply_sap.po_line_helper` 
where po_ln_deletion_indicator not in ('L')
  and po_ln_qty	> po_ln_delivered_qty
  and po_doc_type	not in ('ZPT')
  and po_purchase_group like 'P%'