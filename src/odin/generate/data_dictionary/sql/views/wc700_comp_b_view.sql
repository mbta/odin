DROP VIEW IF EXISTS cubic_reports.wc700_comp_b;
CREATE VIEW cubic_reports.wc700_comp_b
AS (
WITH FAREREV_PAYG_TRIP_SUMMARY AS (
SELECT p.settlement_day_key,
p.operating_day_key,
p.payment_type_key,
m.txn_channel_display,
m.sales_channel_display,
SUM(payment_value) AS total_fare_revenue
FROM cubic_ods.edw_payment_summary p
	JOIN cubic_ods.edw_txn_channel_map m ON m.txn_source = p.txn_source
	AND m.sales_channel_key = p.sales_channel_key
	AND m.payment_type_key = p.payment_type_key
WHERE m.txn_group = 'Open Payment Trips'
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
WHEN FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY > FAREREV_PAYG_TRIP_SUMMARY.SETTLEMENT_DAY_KEY
	THEN '<' || strftime(strptime(CAST(FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY AS VARCHAR), '%Y%m%d'),'%d-%b-%y')
	ELSE '  ' || strftime(strptime(CAST(FAREREV_PAYG_TRIP_SUMMARY.SETTLEMENT_DAY_KEY AS VARCHAR), '%Y%m%d'),'%d-%b-%y')
END AS due_day_grouping_display,
CASE
	WHEN FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY > FAREREV_PAYG_TRIP_SUMMARY.SETTLEMENT_DAY_KEY
	THEN strptime(CAST(FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY AS VARCHAR), '%Y%m%d') - interval'1 day'
	ELSE strptime(CAST(FAREREV_PAYG_TRIP_SUMMARY.SETTLEMENT_DAY_KEY AS VARCHAR), '%Y%m%d')
END AS due_day_grouping_for_sorting,
FAREREV_PAYG_TRIP_SUMMARY.TXN_CHANNEL_DISPLAY,
FAREREV_PAYG_TRIP_SUMMARY.SALES_CHANNEL_DISPLAY,
SUM(FAREREV_PAYG_TRIP_SUMMARY.TOTAL_FARE_REVENUE/100) AS total_fare_revenue
FROM
FAREREV_PAYG_TRIP_SUMMARY
	JOIN cubic_ods.edw_date_dimension SETTLEMENT_DATE_DIMENSION ON SETTLEMENT_DATE_DIMENSION.DATE_KEY=FAREREV_PAYG_TRIP_SUMMARY.SETTLEMENT_DAY_KEY
	JOIN cubic_ods.edw_date_dimension OPERATING_DATE_DIMENSION ON OPERATING_DATE_DIMENSION.DATE_KEY=FAREREV_PAYG_TRIP_SUMMARY.OPERATING_DAY_KEY
	JOIN cubic_ods.edw_fare_revenue_report_schedule FAREREV_REPORT_SCHEDULE ON FAREREV_PAYG_TRIP_SUMMARY.OPERATING_DAY_KEY = FAREREV_REPORT_SCHEDULE.COMP_OPERATING_DAY_KEY
GROUP BY
'WC700',
operating_day,
settlement_day,
due_day,
due_day_grouping_display,
due_day_grouping_for_sorting,
FAREREV_PAYG_TRIP_SUMMARY.TXN_CHANNEL_DISPLAY,
FAREREV_PAYG_TRIP_SUMMARY.SALES_CHANNEL_DISPLAY
)
