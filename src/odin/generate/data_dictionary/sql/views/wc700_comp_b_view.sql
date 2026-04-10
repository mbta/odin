DROP VIEW IF EXISTS cubic_reports.wc700_comp_b;
CREATE VIEW cubic_reports.wc700_comp_b
AS
SELECT
    ps.settlement_day_key
    ,ps.operating_day_key
    ,ps.payment_type_key
    ,tcm.txn_channel_display
    ,tcm.sales_channel_display
    ,SUM(COALESCE(payment_value,0))/100 AS total_fare_revenue
FROM
    cubic_ods.edw_payment_summary ps
JOIN
    cubic_ods.edw_txn_channel_map tcm
    ON
        tcm.txn_source = ps.txn_source
        AND tcm.sales_channel_key = ps.sales_channel_key
        AND tcm.payment_type_key = ps.payment_type_key
WHERE
    tcm.txn_group = 'Open Payment Trips'
GROUP BY
    ps.settlement_day_key
    ,ps.operating_day_key
    ,ps.payment_type_key
    ,tcm.txn_channel_display
    ,tcm.sales_channel_display
ORDER BY
    operating_day_key desc
    ,settlement_day_key desc
;
