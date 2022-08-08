create or replace table `sp-ed-opssupplyanalytics-dev.master_data_sap.pp_supplier_company` as 

with 
  company as (
    select 
      bp.supplier_bp,
      nullif(lfb1.AD_HASH, " ") company_email,
      array_to_string([
        nullif(lfb1.BUKRS, " "),
        nullif(t001.BUTXT, " ")
      ], ": ") as company_code,
      nullif(array_to_string([
        nullif(lfb1.ZTERM, " "),
        concat(nullif(t052.ZTAG1, " "), " días")
      ], ": "), " ") as company_payment_terms,
      nullif(lfb1.ZWELS, " ") private_company_payment_methods,
    from `sp-ed-opssupplyanalytics-dev.master_data_sap.pp_supplier` as bp
    left join `dlakedomain-prod-20dl.maintenance_brownfield_vwt_us.lfb1` as lfb1
      on lfb1.LIFNR = bp.supplier_bp
    left join `dlakedomain-prod-20dl.maintenance_brownfield_vwt_us.t052` as t052
      on lfb1.ZTERM = t052.ZTERM
    left join `dlakedomain-prod-20dl.finance_brownfield_vwt_us.t001` as t001
      on lfb1.BUKRS = t001.BUKRS
  )

select *
from company
