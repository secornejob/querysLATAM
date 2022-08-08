create or replace table `sp-ed-opssupplyanalytics-dev.master_data_sap.pp_bp_filter` as 

with 
  payments_filter as (
    select distinct bp_proveedor as bp,
    from `ed-cu-dscanalytics-dev.CxP.autoconsulta_preview_v2`
    where fecha_descarga = current_date()
      and C11 not like '%PRE%' -- filtrar todos los PRE CH11
      and (
        -- fecha contabilizacion ultimos 180 dias
        fecha_contabilizacion between current_date() - 180 and current_date() 
        -- fecha pago ultimos 360 dias
        or fecha_pago between current_date() - 360 and current_date()
      )
  ),

  account_filter as (
    select distinct lfb1.LIFNR as bp
    from `dlakedomain-prod-20dl.maintenance_brownfield_vwt_us.lfb1` as lfb1
    where lfb1.AKONT like '2112007001' -- cuenta contable funcionario
  )

select pf.bp
from payments_filter pf
left join account_filter af
  on pf.bp = af.bp
left join `sp-ed-opssupplyanalytics-dev.master_data_sap.pp_bp_worker_filter` as wf -- BP ex-funcionarios
  on wf.bp_text = pf.bp
where af.bp is null -- filtro cuenta contable funcionario
  and wf.bp_text is null -- filtro ex-funcionario

  