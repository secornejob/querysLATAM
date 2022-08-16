create or replace table `sp-ed-opssupplyanalytics-dev.supply_mxi.test_shipment_dt` as

with
  shipment_temp as (
    select 
      event_id,
      event_db_id,
      stage_gdt,
      event_status_cd,
    from `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.evt_stage`
    where EVENT_STATUS_CD in ('IXINTR', 'IXCMPLT', 'IXPEND')
  ),

  shipment_dt as (
    select
      event_id,
      event_db_id,
      IXINTR as shipment_in_transit_dt,
      IXCMPLT as shipment_complete_dt,
      IXPEND as shipment_pending_dt,
    from shipment_temp
    pivot (
      max (stage_gdt) for event_status_cd in ('IXINTR', 'IXCMPLT', 'IXPEND')
    ) as pvt
  ),

  shipment_type as (
    select 
      sd.event_id as shipment_id,
      sd.event_db_id as shipment_db_id,
      ss.po_id,
      ss.po_db_id,
      sd.shipment_in_transit_dt,
      sd.shipment_complete_dt,
      sd.shipment_pending_dt,
      case 
        when ss.shipment_type_cd in ('PURCHASE', 'REPAIR') then 'inbound'
        when ss.shipment_type_cd in ('SENDXCHG', 'SENDREP') then 'outbound'
      end as shipment_type_cd,
      ss.creation_dt as shipment_creation_dt
    from shipment_dt sd
    left join `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.ship_shipment` as ss
    on ss.SHIPMENT_ID = sd.event_id
      and ss.SHIPMENT_DB_ID = sd.event_db_id
    where ss.shipment_type_cd in ('SENDXCHG', 'PURCHASE', 'SENDREP', 'REPAIR')
  ),

  shipment_type_complete_dt as (
    select 
      po_id,
      po_db_id,
      inbound as inbound_shipment_complete_dt,
      outbound as outbound_shipment_complete_dt
    from (
      select 
        po_id,
        po_db_id,
        shipment_complete_dt,
        shipment_type_cd
      from shipment_type
    ) as a
    pivot (
      max(shipment_complete_dt) for shipment_type_cd in ('outbound', 'inbound')
    ) as pvt
  ),

  shipment_type_in_transit_dt as (
    select 
      po_id,
      po_db_id,
      inbound as inbound_shipment_in_transit_dt,
      outbound as outbound_shipment_in_transit_dt
    from (
      select 
        po_id,
        po_db_id,
        shipment_in_transit_dt,
        shipment_type_cd
      from shipment_type
    ) as a
    pivot (
      max(shipment_in_transit_dt) for shipment_type_cd in ('outbound', 'inbound')
    ) as pvt
  ),

  shipment_type_pending_dt as (
    select 
      po_id,
      po_db_id,
      inbound as inbound_shipment_pending_dt,
      outbound as outbound_shipment_pending_dt
    from (
      select 
        po_id,
        po_db_id,
        shipment_pending_dt,
        shipment_type_cd
      from shipment_type
    ) as a
    pivot (
      max(shipment_pending_dt) for shipment_type_cd in ('outbound', 'inbound')
    ) as pvt
  )


select 
  po.*,
  sc.inbound_shipment_complete_dt,
  sc.outbound_shipment_complete_dt,
  st.inbound_shipment_in_transit_dt,
  st.outbound_shipment_in_transit_dt,
  sp.inbound_shipment_pending_dt,
  sp.outbound_shipment_pending_dt,
from `sp-ed-opssupplyanalytics-dev.supply_mxi.po_header_helper` as po
left join shipment_type_complete_dt as sc
  on po.po_id = sc.po_id
  and po.po_db_id = sc.po_db_id
left join shipment_type_in_transit_dt st
  on po.po_id = st.po_id
  and po.po_db_id = st.po_db_id
left join shipment_type_pending_dt sp
  on po.po_id = sp.po_id
  and po.po_db_id = sp.po_db_id
-- where po.po_code = 'P0607031'

