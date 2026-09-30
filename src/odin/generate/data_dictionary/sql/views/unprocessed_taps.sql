DROP VIEW IF EXISTS cubic_reports.unprocessed_taps;
CREATE VIEW cubic_reports.unprocessed_taps;
AS
SELECT
    REPROCESS_LOG_ID,
    CLIENT_INFO,
    HTTP_METHOD,
    SERVER_INFO,
    HTTP_URL,
    DEVICE,
    REQUEST_TYPE,
    REQUEST_ID,
    REQUEST_TIMESTAMP,
    REQUEST,
    TRIM(string_split(request,',"')[12],'"') AS tap_status_id,
    ADDITIONAL_INFO,
    INSERTED_SERVER_NAME,
    UPDATED_SERVER_NAME,
    SOURCE_INSERTED_DTM,
    SOURCE_UPDATED_DTM,
    STAGING_INSERTED_DTM,
    STAGING_UPDATED_DTM,
    EDW_INSERTED_DTM,
    EDW_UPDATED_DTM,
    STATUS_FLAG,
    JOB_ID
FROM cubic_ods.edw_abp_reprocess_log r
