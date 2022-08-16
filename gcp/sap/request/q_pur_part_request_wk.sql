
with 
	req_part as (
		select 
			extract(isoweek from cast(eban.BADAT as date format 'YYYYMMDD')) as req_week,
		from `dlakedomain-prod-20dl.maintenance_brownfield_vwt_us.eban` as eban
		where eban.BADAT like '2022%'
			and eban.	EKGRP like 'P%'
			and BSART not in ('ZRT', 'ZNA1')
  )

select req_week, count(*) as cta
from req_part
group by req_week