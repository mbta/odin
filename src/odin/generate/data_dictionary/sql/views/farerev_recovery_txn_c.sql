DROP VIEW IF EXISTS cubic_reports.farerev_recovery_txn_c;
CREATE VIEW cubic_reports.farerev_recovery_txn_c
AS
WITH edw_farerev_recovery_txn_v AS (
SELECT
    CAST('C' AS VARCHAR) AS computation_type,
    od.dtm AS operating_date,
    t.transaction_dtm,
    pd.dtm AS posting_date,
    sd.dtm AS settlement_date,
    st.service_type_name,
    COALESCE(sp.stop_point_name, rd.route_name) AS "location",
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
FROM
    fares_data_repository.cubic_ods.edw_farerev_recovery_txn AS t
INNER JOIN fares_data_repository.cubic_ods.edw_date_dimension AS od ON
    ((od.date_key = t.operating_day_key))
INNER JOIN fares_data_repository.cubic_ods.edw_date_dimension AS pd ON
    ((pd.date_key = t.posting_day_key))
INNER JOIN fares_data_repository.cubic_ods.edw_date_dimension AS sd ON
    ((sd.date_key = t.settlement_day_key))
LEFT JOIN fares_data_repository.cubic_ods.edw_service_type_dimension AS st ON
    ((st.service_type_id = t.service_type_id))
LEFT JOIN fares_data_repository.cubic_ods.edw_stop_point_dimension AS sp ON
    ((sp.stop_point_key = t.stop_point_key))
LEFT JOIN fares_data_repository.cubic_ods.edw_route_dimension AS rd ON
    ((rd.route_key = t.route_key))
LEFT JOIN fares_data_repository.cubic_ods.edw_rider_class_dimension AS rc ON
    ((rc.rider_class_id = t.rider_class_id))
LEFT JOIN fares_data_repository.cubic_ods.edw_fare_product_dimension AS fp ON
    (((fp.fare_prod_key = t.fare_prod_key)
        AND (fp.monetary_inst_type_id = 2)))),
EDW_farerev_recovery_txn_c AS (
SELECT
    rs.due_day_key AS report_due_day_key,
    CAST('N' AS VARCHAR) AS addendum,
    t.*
FROM
    fares_data_repository.cubic_ods.edw_fare_revenue_report_schedule AS rs
INNER JOIN edw_farerev_recovery_txn_v AS t ON
    (((t.operating_day_key = rs.comp_operating_day_key)
        AND (t.settlement_day_key <= rs.comp_max_settlement_day_key)))),
farerev_recovery_txn_c AS (
SELECT
    report_due_day_key,
    addendum,
    computation_type,
    operating_date,
    transaction_dtm,
    posting_date,
    settlement_date,
    service_type_name,
    "location",
    device_id,
    transaction_id,
    tap_id,
    trip_id,
    rider_class_name,
    fare_prod_name,
    fare_rule_description,
    recovery_txn_type,
    incident_id,
    supervening_event,
    round((recovery_calculation_amount / 100), 2) AS recovery_calculation_amount,
    operating_day_key,
    settlement_day_key,
    posting_day_key
FROM
    EDW_farerev_recovery_txn_c)
SELECT * FROM farerev_recovery_txn_c;
