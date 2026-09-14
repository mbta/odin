DROP VIEW IF EXISTS cubic_reports.wc700_comp_d;
CREATE VIEW cubic_reports.wc700_comp_d
AS (
WITH FAREREV_REFUND_SUMMARY AS (
SELECT p.settlement_day_key,
p.operating_day_key,
p.payment_type_key,
m.txn_channel_display,
m.sales_channel_display,
r.reason_name,
SUM(payment_value) AS refund_value,
SUM(payment_value) AS total_fare_revenue
FROM fares_data_repository.cubic_ods.edw_payment_summary p
	JOIN fares_data_repository.cubic_ods.edw_txn_channel_map m ON m.txn_source = p.txn_source
		AND m.sales_channel_key = p.sales_channel_key
		AND m.payment_type_key = p.payment_type_key
	LEFT JOIN fares_data_repository.cubic_ods.edw_reason_dimension r ON r.reason_key = p.reason_key
WHERE m.txn_group = 'Direct Refunds Applied'
GROUP BY
p.settlement_day_key,
p.operating_day_key,
p.payment_type_key,
m.txn_channel_display,
m.sales_channel_display,
r.reason_name
)
SELECT
'WC700',
OPERATING_DATE_DIMENSION.DTM AS operating_day,
SETTLEMENT_DATE_DIMENSION.DTM AS settlement_day,
strptime(CAST(FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY AS VARCHAR), '%Y%m%d') AS due_day,
CASE
	WHEN FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY > FAREREV_REFUND_SUMMARY.SETTLEMENT_DAY_KEY
	THEN '<' || strftime(strptime(CAST(FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY AS VARCHAR), '%Y%m%d'),'%d-%b-%y')
	ELSE '  ' || strftime(strptime(CAST(FAREREV_REFUND_SUMMARY.SETTLEMENT_DAY_KEY AS VARCHAR), '%Y%m%d'),'%d-%b-%y')
END AS due_day_grouping_display,
CASE
	WHEN FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY > FAREREV_REFUND_SUMMARY.SETTLEMENT_DAY_KEY
	THEN strptime(CAST(FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY AS VARCHAR), '%Y%m%d') - interval'1 day'
	ELSE strptime(CAST(FAREREV_REFUND_SUMMARY.SETTLEMENT_DAY_KEY AS VARCHAR), '%Y%m%d')
END AS due_day_grouping_for_sorting,
FAREREV_REFUND_SUMMARY.TXN_CHANNEL_DISPLAY,
FAREREV_REFUND_SUMMARY.SALES_CHANNEL_DISPLAY,
FAREREV_REFUND_SUMMARY.REASON_NAME,
PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_NAME,
(SUM(FAREREV_REFUND_SUMMARY.REFUND_VALUE/100)) AS refund_value,
(SUM(FAREREV_REFUND_SUMMARY.TOTAL_FARE_REVENUE/100)) AS total_fare_revenue
FROM
FAREREV_REFUND_SUMMARY
	JOIN fares_data_repository.cubic_ods.edw_fare_revenue_report_schedule FAREREV_REPORT_SCHEDULE ON FAREREV_REFUND_SUMMARY.OPERATING_DAY_KEY = FAREREV_REPORT_SCHEDULE.COMP_OPERATING_DAY_KEY
	RIGHT JOIN fares_data_repository.cubic_ods.edw_payment_type_dimension PAYMENT_TYPE_DIMENSION ON FAREREV_REFUND_SUMMARY.PAYMENT_TYPE_KEY = PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_KEY
	JOIN fares_data_repository.cubic_ods.edw_date_dimension OPERATING_DATE_DIMENSION ON OPERATING_DATE_DIMENSION.DATE_KEY=FAREREV_REFUND_SUMMARY.OPERATING_DAY_KEY
	JOIN fares_data_repository.cubic_ods.edw_date_dimension SETTLEMENT_DATE_DIMENSION ON SETTLEMENT_DATE_DIMENSION.DATE_KEY=FAREREV_REFUND_SUMMARY.SETTLEMENT_DAY_KEY
GROUP BY
'WC700',
operating_day,
settlement_day,
due_day,
due_day_grouping_display,
due_day_grouping_for_sorting,
FAREREV_REFUND_SUMMARY.TXN_CHANNEL_DISPLAY,
FAREREV_REFUND_SUMMARY.SALES_CHANNEL_DISPLAY,
FAREREV_REFUND_SUMMARY.REASON_NAME,
PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_NAME
)
