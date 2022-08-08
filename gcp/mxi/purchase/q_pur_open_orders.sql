SELECT *
FROM `sp-ed-opssupplyanalytics-dev.supply_mxi.po_header_helper` ph
left join `sp-ed-opssupplyanalytics-dev.supply_mxi.po_line_helper` pl
  on ph.po_id = pl.po_id
  and ph.po_db_id = pl.po_db_id
where po_type like 'PURCHASE'
  and po_status not in ('POCLOSED', 'POCANCEL', 'PORECEIVED')
  -- and po_status like 'PORECEIVED'
order by po_creation_dt desc