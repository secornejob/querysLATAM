
with
  last_po_req as (
    select 
      max(ee.CREATION_DT) as creation_dt,
      ee.EVENT_ID, 
      ee.EVENT_DB_ID,
    from `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.evt_event` as ee
    where ee.EVENT_STATUS_CD like 'PRPOREQ'
      and extract(year from ee.CREATION_DT) = 2022
    group by 
      ee.EVENT_ID,
      ee.EVENT_DB_ID
  ),

  part_request as (
    select 
      extract(isoweek from rp.CREATION_DT) as req_week,
      count(*) as cantidad
    from `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.req_part` as rp
    left join `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.evt_event` as ee
      on ee.EVENT_ID = rp.REQ_PART_ID
      and ee.EVENT_DB_ID = rp.REQ_PART_DB_ID
    inner join last_po_req f  
      on f.event_id = ee.event_id
      and f.event_db_id = ee.event_db_id
    where rp.REQ_TYPE_CD like 'STOCK'
      and extract(year from rp.CREATION_DT) = 2022
      and ee.event_status_cd not in ('PRCANCEL')
    group by extract(isoweek from rp.CREATION_DT)
  )


select *
from part_request