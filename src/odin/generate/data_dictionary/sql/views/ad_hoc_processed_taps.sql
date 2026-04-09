DROP VIEW IF EXISTS cubic_reports.ad_hoc_processed_taps;
CREATE VIEW cubic_reports.ad_hoc_processed_taps
AS
SELECT
    tap_id
    ,transaction_dtm
    ,device_id
    ,DEVICE_PREFIX
    ,token_id
    ,TRANSIT_ACCOUNT_ID
    ,OPERATOR_ID
    ,TRANSACTION_ID
    ,tap_status_id
    ,tap_status_desc
    ,unmatched_flag
    ,trip_id
    ,sector_id
    ,voidable_until_dtm
    ,dw_transaction_id
    ,source_inserted_dtm
FROM cubic_ods.edw_abp_tap
;
