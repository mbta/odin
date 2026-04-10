DROP VIEW IF EXISTS cubic_reports.wc700_comp_d;
CREATE VIEW cubic_reports.wc700_comp_d
AS
SELECT
    ps.settlement_day_key
    ,ps.operating_day_key
    ,ps.payment_type_key
    ,tcm.txn_channel_display
    ,tcm.sales_channel_display
    ,rd.reason_name
    ,SUM(ps.payment_value)/100 as refund_value
FROM
    cubic_ods.edw_payment_summary ps
JOIN
    cubic_ods.edw_txn_channel_map tcm
    ON
        tcm.txn_source = ps.txn_source
        AND tcm.sales_channel_key = ps.sales_channel_key
        AND tcm.payment_type_key = ps.payment_type_key
LEFT JOIN
    cubic_ods.edw_reason_dimension rd
    ON
        rd.reason_key = ps.reason_key
WHERE
    tcm.txn_group = 'Direct Refunds Applied'
GROUP BY
    ps.settlement_day_key
    ,ps.operating_day_key
    ,ps.payment_type_key
    ,tcm.txn_channel_display
    ,tcm.sales_channel_display
    ,rd.reason_name
ORDER BY
    ps.operating_day_key desc
    ,ps.settlement_day_key desc
;
