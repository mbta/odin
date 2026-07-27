DROP VIEW IF EXISTS cubic_reports.wsp611_operation_performance;
CREATE VIEW cubic_reports.wsp611_operation_performance
AS
(SELECT
  DT_WSP611_OP_MON_PERF_DED.run_date,
  DT_WSP611_OP_MON_PERF_DED.KPI_ID,
  DT_WSP611_OP_MON_PERF_DED.KPI_NAME,
  DT_WSP611_OP_MON_PERF_DED.LOCATION_CATEGORY,
  DT_WSP611_OP_MON_PERF_DED.TOT_QTY AS total_quantity,
  DT_WSP611_OP_MON_PERF_DED.AVG_RESOLUTION AS average_resolution,
  DT_WSP611_OP_MON_PERF_DED.CURE_PERIOD,
  DT_WSP611_OP_MON_PERF_DED.UNITS,
  DT_WSP611_OP_MON_PERF_DED.RECURRENCE_PERIODS,
  DT_WSP611_OP_MON_PERF_DED.DEDUCTION AS deduction_amount
  --DT_WSP611_OP_MON_PERF_DED.ORDER0,
  --DT_WSP611_OP_MON_PERF_DED.ORDER1,
  --DT_WSP611_OP_MON_PERF_DED.ORDER2,
  --DT_WSP611_OP_MON_PERF_DED.ORDER3
FROM
  ( 
  select  
rtrim(substr(s1.kpi_id,2,2),'abcd-')::int as order0,
s1.run_date,
  length(substr(s1.kpi_id,1,4)) as order1,
  substr(s1.kpi_id,2,3) as order2,
  cast(ltrim(substr(s1.kpi_id,4,6),'abcd-') AS float) as order3,
s1.kpi_id,kpi_name,location_category,tot_qty,round(avg_resolution,1) as avg_resolution,cure_period,units,recurrence_periods,
case when (location_category in ('A','B','C','D', 'PTT') and s2.deduction is not null) then s2.deduction else s1.deduction end deduction
from
((select 
dd.month_desc || '-' || dd.YEAR AS run_date
,ks.kpi_id
,kpi.kpi_name
,kpi.units
,sum(kpi_value)/100 as deduction
FROM cubic_ods.edw_kpi_summary_by_day ks
inner join cubic_ods.edw_date_dimension dd
on ks.transit_day_key = dd.date_key
inner join cubic_ods.edw_kpi kpi
on ks.kpi_id = kpi.kpi_id  and COALESCE(grouped,'xxx') not like 'sum%'
where dd.month_desc = 'February' AND dd.YEAR = 2026
--dd.dtm >= '2025-08-01 00:00:00.000'
--AND dd.dtm <= '2025-08-31 23:59:59.000'
and metric_category_id = 8
group by ks.kpi_id,kpi_name,kpi_type,units,dd.month_desc,dd.YEAR) s1 --S1 IS WORKING BY ITSELF
	left join
(select kpi_id,
case when location_category in ('A','B','C','D') then location_category
when kpi_id in ('P1-18','P2-19.1','P2-19.2','P2-20','P3-21') and failure_level = 999 then 'PTT'
else location_category end location_category,
sum(1) as tot_qty,avg(performance_time_basis) as avg_resolution,
max(base_cure_period) as cure_period, sum(recurrence_count) as Recurrence_periods,
sum(kpi_value)/100 as deduction
from cubic_ods.edw_kpi_detail_events_by_day kpi_detail_events_by_day
inner join cubic_ods.edw_date_dimension dd
on  kpi_detail_events_by_day.transit_day_key = dd.date_key
where dd.month_desc = 'February' AND dd.YEAR = 2026
--dd.dtm >= '2025-08-01 00:00:00.000'
--AND dd.dtm <= '2025-08-31 23:59:59.000'
group by kpi_id,
case when location_category in ('A','B','C','D') then location_category
when kpi_id in ('P1-18','P2-19.1','P2-19.2','P2-20','P3-21') and failure_level = 999 then 'PTT'
else location_category end) s2 --S2 IS WORKING BY ITSELF
on s1.kpi_id = s2.kpi_id)
order by rtrim(substr(s1.kpi_id,2,2),'abcd-')::int,
length(substr(s1.kpi_id,1,4)),substr(s1.kpi_id,2,3),
cast(ltrim(substr(s1.kpi_id,4,6),'abcd-') AS float),location_category
  )  DT_WSP611_OP_MON_PERF_DED
) 
