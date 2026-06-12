DROP VIEW IF EXISTS cubic_reports.farerev_payg_trip_txn_a_v2;
CREATE VIEW cubic_reports.farerev_payg_trip_txn_a_v2 AS
WITH EDW_FAREREV_PAYG_TRIP_TXN AS (
    SELECT * FROM cubic_reports.edw_farerev_payg_trip_txn
),
EDW_FARE_REVENUE_REPORT_SCHEDULE_A AS(
SELECT
    DISTINCT s.due_day_key,
    s.adden_max_operating_day_key,
    s.adden_min_settlement_day_key,
    s.adden_max_settlement_day_key,
    s.deposit_due_day_key
FROM
    cubic_ods.edw_fare_revenue_report_schedule AS s),
EDW_farerev_payg_trip_txn_a AS(
SELECT
    rs.due_day_key AS report_due_day_key,
    CAST('Y' AS VARCHAR) AS addendum,
    t.*
FROM
    EDW_FARE_REVENUE_REPORT_SCHEDULE_A AS rs
INNER JOIN EDW_FAREREV_PAYG_TRIP_TXN AS t ON
    (((t.operating_day_key <= rs.adden_max_operating_day_key)
        AND (t.settlement_day_key BETWEEN rs.adden_min_settlement_day_key AND rs.adden_max_settlement_day_key)))),
farerev_payg_trip_txn_a AS (
SELECT
    report_due_day_key,
    addendum,
    txn_channel_display,
    computation_type,
    operating_date,
    transaction_dtm,
    posting_date,
    settlement_date,
    CASE WHEN ((operator_name = 'Subway')) THEN ('Transit') ELSE operator_name END AS transit_mode_name,
    device_id,
    dw_transaction_id,
    transit_account_id,
    patron_trip_id,
    tap_id,
    fare_rule_description,
    transfer_sequence_nbr,
    payment_type_name,
    bin,
    round((fare_revenue / 100), 2) AS fare_revenue,
    extension_charge_reason,
    retrieval_ref_nbr,
    operating_day_key,
    settlement_day_key,
    posting_day_key
FROM
    EDW_farerev_payg_trip_txn_a)
SELECT *
FROM farerev_payg_trip_txn_a;
