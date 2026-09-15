DROP VIEW IF EXISTS cubic_reports.wc700_comp_c;
CREATE VIEW cubic_reports.wc700_comp_c
AS (
WITH FAREREV_RECOVERY_TXN_V AS (
SELECT
CAST ('C' AS CHAR(1)) AS computation_type,
od.dtm AS operating_date,
t.transaction_dtm,
pd.dtm AS posting_date,
sd.dtm AS settlement_date,
st.service_type_name,
COALESCE(sp.stop_point_name, rd.route_name) AS location,
t.device_id,
t.source_table_uk AS transaction_id,
t.tap_id,
t.trip_id,
rc.rider_class_name,
fp.fare_prod_name,
t.fare_rule_description,
t.recovery_txn_type,
t.incident_id,
t.supervening_event,
t.minimum_fare_charge AS recovery_calculation_amount,
t.operating_day_key,
t.settlement_day_key,
t.posting_day_key
FROM cubic_ods.edw_farerev_recovery_txn t
	JOIN cubic_ods.edw_date_dimension od ON od.date_key = t.operating_day_key
	JOIN cubic_ods.edw_date_dimension pd ON pd.date_key = t.posting_day_key
	JOIN cubic_ods.edw_date_dimension sd ON sd.date_key = t.settlement_day_key
	LEFT JOIN cubic_ods.edw_service_type_dimension st ON st.service_type_id = t.service_type_id
	LEFT JOIN cubic_ods.edw_stop_point_dimension sp ON sp.stop_point_key = t.stop_point_key
	LEFT JOIN cubic_ods.edw_route_dimension rd ON rd.route_key = t.route_key
	LEFT JOIN cubic_ods.edw_rider_class_dimension rc ON rc.rider_class_id = t.rider_class_id
	LEFT JOIN cubic_ods.edw_fare_product_dimension fp ON fp.fare_prod_key = t.fare_prod_key
		AND fp.monetary_inst_type_id = 2
),
FAREREV_RECOVERY_SUMMARY AS (
SELECT
settlement_day_key,
operating_day_key,
rider_class_name,
fare_prod_name,
service_type_name,
fare_rule_description,
recovery_txn_type,
SUM(recovery_calculation_amount) AS recovery_calculation_amount
FROM farerev_recovery_txn_v
GROUP BY
settlement_day_key,
operating_day_key,
rider_class_name,
fare_prod_name,
service_type_name,
fare_rule_description,
recovery_txn_type
)
SELECT
'WC700',
OPERATING_DATE_DIMENSION.DTM AS operating_day,
SETTLEMENT_DATE_DIMENSION.DTM AS settlement_day,
strptime(CAST(FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY AS VARCHAR), '%Y%m%d') AS due_day,
CASE
	WHEN FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY > FAREREV_RECOVERY_SUMMARY.SETTLEMENT_DAY_KEY
	THEN '<' || strftime(strptime(CAST(FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY AS VARCHAR), '%Y%m%d'),'%d-%b-%y')
	ELSE '  ' || strftime(strptime(CAST(FAREREV_RECOVERY_SUMMARY.SETTLEMENT_DAY_KEY AS VARCHAR), '%Y%m%d'),'%d-%b-%y')
END AS due_day_grouping_display,
CASE
	WHEN FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY > FAREREV_RECOVERY_SUMMARY.SETTLEMENT_DAY_KEY
	THEN strptime(CAST(FAREREV_REPORT_SCHEDULE.DUE_DAY_KEY AS VARCHAR), '%Y%m%d') - interval'1 day'
	ELSE strptime(CAST(FAREREV_RECOVERY_SUMMARY.SETTLEMENT_DAY_KEY AS VARCHAR), '%Y%m%d')
END AS due_day_grouping_for_sorting,
FAREREV_RECOVERY_SUMMARY.RIDER_CLASS_NAME,
FAREREV_RECOVERY_SUMMARY.FARE_PROD_NAME AS passes_used,
FAREREV_RECOVERY_SUMMARY.SERVICE_TYPE_NAME,
FAREREV_RECOVERY_SUMMARY.FARE_RULE_DESCRIPTION,
FAREREV_RECOVERY_SUMMARY.RECOVERY_TXN_TYPE AS reason_code,
SUM(FAREREV_RECOVERY_SUMMARY.RECOVERY_CALCULATION_AMOUNT)/100 AS total_fare_revenue
FROM FAREREV_RECOVERY_SUMMARY
	JOIN cubic_ods.edw_fare_revenue_report_schedule FAREREV_REPORT_SCHEDULE ON FAREREV_RECOVERY_SUMMARY.OPERATING_DAY_KEY = FAREREV_REPORT_SCHEDULE.COMP_OPERATING_DAY_KEY
	JOIN cubic_ods.edw_date_dimension SETTLEMENT_DATE_DIMENSION ON SETTLEMENT_DATE_DIMENSION.DATE_KEY=FAREREV_RECOVERY_SUMMARY.SETTLEMENT_DAY_KEY
	JOIN cubic_ods.edw_date_dimension OPERATING_DATE_DIMENSION ON OPERATING_DATE_DIMENSION.DATE_KEY=FAREREV_RECOVERY_SUMMARY.OPERATING_DAY_KEY
GROUP BY
'WC700',
operating_day,
settlement_day,
due_day,
due_day_grouping_display,
due_day_grouping_for_sorting,
FAREREV_RECOVERY_SUMMARY.RIDER_CLASS_NAME,
passes_used,
FAREREV_RECOVERY_SUMMARY.SERVICE_TYPE_NAME,
FAREREV_RECOVERY_SUMMARY.FARE_RULE_DESCRIPTION,
reason_code
)
