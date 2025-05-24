AD_HOC_VIEW = """
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
"""

COMP_A_VIEW = """
    DROP VIEW IF EXISTS cubic_reports.wc700_comp_a;
    CREATE VIEW cubic_reports.wc700_comp_a
    AS
    SELECT
        ps.settlement_day_key
        ,ps.operating_day_key
        ,ps.payment_type_key
        ,tcm.txn_channel_display
        ,tcm.sales_channel_display
        ,SUM(COALESCE(transit_value,0)
         + COALESCE(benefit_value,0)
         + COALESCE(bankcard_payment_value,0)
         + COALESCE(one_account_value,0))/100 AS stored_value
        ,SUM(COALESCE(pass_cost,0))/100 AS pass_cost
        ,SUM(COALESCE(enablement_fee,0))/100 AS enablement_fee
        ,SUM(COALESCE(transit_value,0)
         + COALESCE(benefit_value,0)
         + COALESCE(bankcard_payment_value,0)
         + COALESCE(one_account_value,0)
         + COALESCE(pass_cost,0)
         + COALESCE(enablement_fee,0)
         + COALESCE(replacement_fee, 0))/100 AS total_fare_revenue
    FROM
        cubic_ods.edw_payment_summary ps
    JOIN
        cubic_ods.edw_txn_channel_map tcm
        ON
            tcm.txn_source = ps.txn_source
            AND tcm.sales_channel_key = ps.sales_channel_key
            AND tcm.payment_type_key = ps.payment_type_key
    WHERE
        tcm.txn_group = 'Product Sales'
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
"""


COMP_B_VIEW = """
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
"""


COMP_D_VIEW = """
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
"""
