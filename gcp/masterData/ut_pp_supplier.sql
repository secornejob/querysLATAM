create or replace table `sp-ed-opssupplyanalytics-dev.master_data_sap.pp_supplier` as 

with 
  supplier_master_data as (
    select 
      array_to_string([
        nullif(lfa1.NAME1, " "), 
        nullif(lfa1.NAME2, " ")
      ], " ") as supplier_name,
      -- lfa1.NAME3 as supplier_name_3,
      -- lfa1.NAME4 as supplier_name_4,
      lfa1.LIFNR as supplier_bp,
      array_to_string([
        nullif(lfa1.STRAS, " "), 
        nullif(lfa1.MCOD3, " "),
        nullif(lfa1.LAND1, " ")
      ], " - ") as supplier_address,
      lfa1.ADRNR as supplier_comercial_contact,
      nullif(array_to_string([
        nullif(lfa1.TELF1, " "), 
        nullif(lfa1.TELF2, " ")
      ], " - "), "") as supplier_phone_number,
      nullif(array_to_string([
        nullif(lfa1.STCD1, " "), 
        nullif(lfa1.STCD2, " "), 
        nullif(lfa1.STCD3, " "), 
        nullif(lfa1.STCD4, " "), 
        nullif(lfa1.STCD5, " "), 
        nullif(lfa1.STCD6, " ")
      ], ", "), "") as supplier_tax_number,
      nullif(lfa1.BEGRU, " ") as supplier_auth_group,
      nullif(array_to_string(
        array(
          select lower(adr6.SMTP_ADDR) 
          from `dlakedomain-prod-20dl.finance_brownfield_vwt_us.adr6` as adr6
          where lfa1.ADRNR = adr6.ADDRNUMBER
        ), 
        " / "
      ), "") as supplier_email,
      case 
        when upper(lfa1.STKZN) like 'X'and upper(lfa1.LAND1) like 'BR' then 1 
        else 0 
      end as br_clients_filter,
      nullif(lfa1.LAND1, " ") as bp_country,
      nullif(lfa1.STKZN, " ") as bp_persona,
    from `dlakedomain-prod-20dl.maintenance_brownfield_vwt_us.lfa1` as lfa1
    inner join `sp-ed-opssupplyanalytics-dev.master_data_sap.pp_bp_filter` as bp
      on bp.bp = lfa1.LIFNR 
  )

select *
from supplier_master_data as sd
where sd.br_clients_filter = 0

