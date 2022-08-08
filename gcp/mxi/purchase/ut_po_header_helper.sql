
-- truncate table `sp-ed-opssupplyanalytics-dev.supply_mxi.po_header_helper`;
-- insert into `sp-ed-opssupplyanalytics-dev.supply_mxi.po_header_helper`

create or replace table `sp-ed-opssupplyanalytics-dev.supply_mxi.po_header_helper` as

with po_header as (
  select 
    po.po_id,
    po.po_db_id,
    evt.EVENT_SDESC as po_code,
    po.PO_TYPE_CD as po_type,
    evt.EVENT_STATUS_CD as po_status,
    ov.VENDOR_CD as vendor_cd,
    ov.VENDOR_NAME as vendor_name,
    hr.HR_ID as po_bp_owner_id,
    hr.HR_DB_ID as po_bo_owner_db_id,
    hr.HR_CD as po_bp_owner_code,
    po.AUTH_STATUS_CD as auth_status_cd,
    po.PO_AUTH_FLOW_CD as po_auth_flow_cd,
    po.CURRENCY_CD as po_currency_cd,
    po.creation_dt as po_creation_dt,

  from `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.po_header` as po
  left join `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.evt_event` as evt
    on po.PO_ID = evt.EVENT_ID
    and po.PO_DB_ID = evt.EVENT_DB_ID
  left join `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.org_vendor` as ov
    on ov.VENDOR_ID = po.VENDOR_ID
    and ov.VENDOR_DB_ID = po.VENDOR_DB_ID
  left join `bc-te-dlake-prod-21x6.stg_cloud_ora_exmro_mro_us.org_hr` as hr
    on po.CONTACT_HR_ID = hr.HR_ID
    and po.CONTACT_HR_DB_ID = hr.HR_DB_ID
  where evt.EVENT_SDESC is not null
    -- and po.PO_TYPE_CD in ('REPAIR', 'PURCHASE', 'EXCHANGE')
)

select distinct *
from po_header
