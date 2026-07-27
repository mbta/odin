DROP VIEW IF EXISTS cubic_reports.wsp611_system_availability;
CREATE VIEW cubic_reports.wsp611_system_availability
AS
(SELECT
  DT_WSP611_SA_MON_PERF_DED.run_date,
  DT_WSP611_SA_MON_PERF_DED.KPI_ID,
  DT_WSP611_SA_MON_PERF_DED.KPI_NAME,
  DT_WSP611_SA_MON_PERF_DED.TOTAL_QTY,
  DT_WSP611_SA_MON_PERF_DED.MEASURED AS measured_value,
  DT_WSP611_SA_MON_PERF_DED.UNITS,
  round(CAST (DT_WSP611_SA_MON_PERF_DED.THRESHOLD as numeric),2) AS threshold_value,
  round(CAST(DT_WSP611_SA_MON_PERF_DED.KPI_VALUE AS numeric),2) AS kpi_value,
  DT_WSP611_SA_MON_PERF_DED.BAND,
  DT_WSP611_SA_MON_PERF_DED.DEDUCTION AS SIM,
  --DT_WSP611_SA_MON_PERF_DED.ORDER0,
  --DT_WSP611_SA_MON_PERF_DED.ORDER1,
  --DT_WSP611_SA_MON_PERF_DED.ORDER2
FROM
  ( 
  with --date_range as
--(select date_key,dd.month_desc || '-' || dd.YEAR AS run_date from cubic_ods.edw_date_dimension dd
--  where dd.month_desc = 'March' AND dd.YEAR = 2025), --SELECT MONTH AND YEAR
base as
        (SELECT ks.kpi_id,kpi_name,kpi.kpi_type,units,metric_category_id,base_qty,grouped,dd.month_desc || '-' || dd.YEAR AS run_date,
                SUM(kpi_value) AS measured,
                SUM(kpi_quantity) AS total_qty,
                MAX(CASE WHEN kpi_quantity > 0 THEN ks.transit_day_key ELSE 0 END) AS last_day_key
         FROM cubic_ods.edw_kpi_summary_by_day ks
         INNER JOIN cubic_ods.edw_date_dimension dd ON dd.date_key = ks.transit_day_key
         INNER JOIN cubic_ods.edw_kpi kpi ON ks.kpi_id = kpi.kpi_id and deduction_basis_id is null
                    and (metric_category_id != 8 or metric_category_id is null)
         WHERE dd.month_desc = 'January' AND dd.YEAR = 2026 --SELECT RUN DATE AND YEAR
         GROUP BY ks.kpi_id,kpi_name,kpi_type,units,metric_category_id,base_qty,grouped,run_date
        ),                           
        child as
        (SELECT kpi_id,
            case when grouped like 'sum%' then v_sum
                 when grouped like 'max%' then v_max
                 when grouped like 'min%' then v_min
                 else v_sum
            end measured,Q.run_date,
            case when grouped like '%/sum' then q_sum
                 when grouped like '%/avg' then q_avg
                 else q_sum
            end total_qty
        from
        (SELECT k.kpi_id,k.grouped,b.run_date,
           sum(measured) as v_sum,
           max(measured) as v_max,
           min(measured) as v_min,
           sum(total_qty) as q_sum,
           avg(total_qty) as q_avg
         from base b
         join cubic_ods.edw_kpi k on k.kpi_id = b.grouped
         group by k.kpi_id,k.grouped,b.run_date)Q),         
        parent as 
        (SELECT b.kpi_id,kpi_name,kpi_type,units,metric_category_id,base_qty,b.run_date,
            case when substr(b.grouped,1,3) in ('sum','max','min') then coalesce(c.measured,b.measured) else b.measured end measured,
            case when substr(b.grouped,1,3) in ('sum','max','min') then coalesce(c.total_qty,b.total_qty) else b.total_qty end total_qty,
            last_day_key 
         from base b
         left join child c on c.kpi_id = b.kpi_id
         )
  select 
  rtrim(substr(kg.kpi_id,2,2),'abcd-')::int as order0,run_date,
  length(kg.kpi_id) as order1,
  kg.kpi_id as order2,
kg.kpi_id,kg.kpi_name,total_qty,measured,kg.units,
         case when kg.kpi_type = '%' and kg.kpi_minimum = 0 then kg.kpi_maximum/1000::float
              when kg.kpi_type = '%' then kg.kpi_minimum/1000::float
              when kg.kpi_minimum = 0 then kg.kpi_maximum::float
              else kg.kpi_minimum::float
         end as threshold,
         case when kg.kpi_type = '%' then round(kpi/1000,3)
                else round(kg.kpi)
         end as kpi_value,
         case when metric_category_id = 9 and kg.total_qty = 0 then band_a
                          else kt.kpi_band
                     end as band,
         case when metric_category_id = 9 and kg.total_qty = 0 then          
                  case when kt.kpi_deduction_type = '%' then round(deduction_a/1000,3)
                        else kt.kpi_deduction_value
                  end
              else 
                  case when kt.kpi_deduction_type = '%' then round(kt.kpi_deduction_value/1000,3)
                        else kt.kpi_deduction_value
                  end
              end as deduction
from
(select s2.kpi_id,kpi_name,kpi_type,metric_category_id,total_qty,measured,units, kt1.kpi_minimum,kt1.kpi_maximum,run_date,
         case when kpi_type in ('TOT', 'Each Day', 'Days')  then measured
                when total_qty > 0 and  kpi_type = '%'  and metric_category_id in (2,4,8) then Round((1 - measured/total_qty) * 100000,0)
                when total_qty > 0 and  kpi_type = '%'  then Round((measured/total_qty) * 100000,0)
                when total_qty > 0 and  kpi_type = 'AVG'  then measured/total_qty
                when kpi_type = '%' and kt1.kpi_minimum = 0 then 0
                when kpi_type = '%' then 100000
                else 0
         end kpi,
         kt1.kpi_band as band_a,
         kt1.kpi_deduction_value as deduction_a 
from
(select s1.kpi_id,kpi_name,run_date,
            Case when substr(kpi_type,1,1) = '%' then '%' else kpi_type end kpi_type,
            metric_category_id,
            units,
            Case when kpi_type = '%Last' then KS1.KPI_QUANTITY
                    when kpi_type = '%Base' then base_qty
                    else total_qty
            end total_qty,
            Case when kpi_type = '%Last' then KS1.KPI_VALUE else measured end measured
 from
(select kpi_id,kpi_name,kpi_type,units,metric_category_id,base_qty,measured,total_qty,last_day_key,run_date from parent) s1
left join cubic_ods.edw_kpi_summary_by_day ks1
on s1.kpi_id = ks1.kpi_id and s1.last_day_key = ks1.transit_day_key
) s2
left join cubic_ods.edw_kpi_target kt1
on s2.kpi_id = kt1.kpi_id
and kt1.kpi_band = 'A'
) kg
left join cubic_ods.edw_kpi_target kt
on kg.kpi_id = kt.kpi_id
 and kpi between kt.kpi_minimum and kt.kpi_maximum
order by rtrim(substr(kg.kpi_id,2,2),'abcd-')::INT,length(kg.kpi_id),kg.kpi_id) DT_WSP611_SA_MON_PERF_DED
)
