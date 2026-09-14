DROP VIEW IF EXISTS cubic_reports.wc700_comp_a;
CREATE VIEW cubic_reports.wc700_comp_a
AS (
WITH FAREREV_PROD_SALES_SUMMARY AS (
SELECT p.settlement_day_key,
p.operating_day_key,
p.payment_type_key,
m.txn_channel_display,
m.sales_channel_display,
SUM(COALESCE(transit_value,0) + COALESCE(benefit_value,0) + COALESCE(bankcard_payment_value,0) 
+ COALESCE(one_account_value,0)) AS stored_value,
SUM(COALESCE(pass_cost,0)) AS pass_cost,
SUM(COALESCE(enablement_fee,0)) AS enablement_fee,
SUM(COALESCE(replacement_fee, 0)) AS replacement_fee,
SUM(COALESCE(transit_value,0) + COALESCE(benefit_value,0) + COALESCE(bankcard_payment_value,0) 
	+ COALESCE(one_account_value,0) + COALESCE(pass_cost,0) + COALESCE(enablement_fee,0) 
	+ COALESCE(replacement_fee, 0)) AS total_fare_revenue
FROM fares_data_repository.cubic_ods.edw_payment_summary p
	JOIN fares_data_repository.cubic_ods.edw_txn_channel_map m ON m.txn_source = p.txn_source 
	AND m.sales_channel_key = p.sales_channel_key 
	AND m.payment_type_key = p.payment_type_key
WHERE m.txn_group = 'Product Sales'
GROUP BY
p.settlement_day_key,
p.operating_day_key,
p.payment_type_key,
m.txn_channel_display,
m.sales_channel_display
)
SELECT
'WC700',
OPERATING_DATE_DIMENSION.DTM AS operating_day,
SETTLEMENT_DATE_DIMENSION.DTM AS settlement_day,
strptime(CAST(FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY AS VARCHAR), '%Y%m%d') AS due_day,
CASE
	WHEN FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY > FAREREV_PROD_SALES_SUMMARY.SETTLEMENT_DAY_KEY
	THEN '<' || strftime(strptime(CAST(FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY AS VARCHAR), '%Y%m%d'),'%d-%b-%y')
	ELSE
'  ' || strftime(strptime(CAST(FAREREV_PROD_SALES_SUMMARY.SETTLEMENT_DAY_KEY AS VARCHAR), '%Y%m%d'),'%d-%b-%y')
END AS due_day_grouping_display,
CASE
	WHEN FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY >FAREREV_PROD_SALES_SUMMARY.SETTLEMENT_DAY_KEY
	THEN strptime(CAST(FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY AS VARCHAR), '%Y%m%d') - interval'1 day'
	ELSE strptime(CAST(FAREREV_PROD_SALES_SUMMARY.SETTLEMENT_DAY_KEY AS VARCHAR), '%Y%m%d')
END AS due_day_for_group_sorting,
FAREREV_PROD_SALES_SUMMARY.TXN_CHANNEL_DISPLAY,
FAREREV_PROD_SALES_SUMMARY.SALES_CHANNEL_DISPLAY,
SUM(FAREREV_PROD_SALES_SUMMARY.STORED_VALUE/100) AS stored_value,
SUM(FAREREV_PROD_SALES_SUMMARY.PASS_COST/100) AS pass_cost,
SUM(FAREREV_PROD_SALES_SUMMARY.ENABLEMENT_FEE/100) AS enablement_fee,
COALESCE(SUM(FAREREV_PROD_SALES_SUMMARY.REPLACEMENT_FEE/100),0) AS replacement_fee,
SUM(FAREREV_PROD_SALES_SUMMARY.TOTAL_FARE_REVENUE/100) AS total_fare_revenue
FROM
FAREREV_PROD_SALES_SUMMARY
	JOIN fares_data_repository.cubic_ods.edw_date_dimension SETTLEMENT_DATE_DIMENSION ON SETTLEMENT_DATE_DIMENSION.DATE_KEY=FAREREV_PROD_SALES_SUMMARY.SETTLEMENT_DAY_KEY
	JOIN fares_data_repository.cubic_ods.edw_date_dimension OPERATING_DATE_DIMENSION ON OPERATING_DATE_DIMENSION.DATE_KEY=FAREREV_PROD_SALES_SUMMARY.OPERATING_DAY_KEY
	JOIN fares_data_repository.cubic_ods.edw_fare_revenue_report_schedule FAREREV_REPORT_SCHEDULE ON FAREREV_PROD_SALES_SUMMARY.OPERATING_DAY_KEY = FAREREV_REPORT_SCHEDULE.COMP_OPERATING_DAY_KEY 
GROUP BY
'WC700', 
OPERATING_DATE_DIMENSION.DTM, 
SETTLEMENT_DATE_DIMENSION.DTM, 
due_day,
due_day_grouping_display,
due_day_for_group_sorting,
FAREREV_PROD_SALES_SUMMARY.TXN_CHANNEL_DISPLAY,
FAREREV_PROD_SALES_SUMMARY.SALES_CHANNEL_DISPLAY
)
