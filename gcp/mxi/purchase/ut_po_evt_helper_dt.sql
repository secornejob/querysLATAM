create or replace table `sp-ed-opssupplyanalytics-dev.supply_mxi.evt_helper_dt` as

with
  evt_stage_helper as (
    select 
      case 
        when upper(evt.stage_note) like '%ACK%' or evt.event_status_cd = 'POACKNOWLEDGED' then 'ACK'
        when upper(evt.stage_note) like '%FUP%' then 'FUP'
        when evt.event_status_cd in ('POISSUED', 'POAUTH', 'PRAVAIL', 'PRONORDER', 'PRPOREQ', 'PRCANCEL') then evt.event_status_cd
      end as event_description,
      evt.stage_dt,
      evt.event_id,
      evt.event_db_id
    from `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.evt_stage` as evt
  ),

  event_helper as (
    select 
      po.po_code,
      po.po_type,
      po.po_status,
      evt.event_description,
      evt.stage_dt
    from `sp-ed-opssupplyanalytics-dev.supply_mxi.po_header_helper` as po
    left join evt_stage_helper as evt
      on po.po_id = evt.event_id
      and po.po_db_id = evt.event_db_id
    where extract(year from po.po_creation_dt) = 2022
      and evt.event_description is not null
  ),

  first_dt as (
    select 
      po_code,
      POAUTH as first_auth_dt,
      POISSUED as first_issued_dt,
      ACK as first_ack_dt,
      FUP as first_fup_dt
    from event_helper
    pivot (
      min(stage_dt) for event_description in ('POISSUED', 'ACK', 'POAUTH', 'FUP')
    ) as pvt
  ),

  last_dt as (
    select 
      po_code,
      POAUTH as last_auth_dt,
      POISSUED as last_issued_dt,
      ACK as last_ack_dt,
      FUP as last_fup_dt
    from event_helper
    pivot (
      max(stage_dt) for event_description in ('POISSUED', 'ACK', 'POAUTH', 'FUP')
    ) as pvt
  )


select 
  fd.po_code,
  fd.first_auth_dt,
  ld.last_auth_dt,
  fd.first_issued_dt,
  ld.last_issued_dt,
  fd.first_ack_dt,
  ld.last_ack_dt,
  fd.first_fup_dt,
  ld.last_fup_dt
from first_dt as fd
left join last_dt as ld
  on fd.po_code = ld.po_code