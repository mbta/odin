# These SQL statements/reports are sourced from the Cubic SAP platform.
# Query outputs are validated by MBTA finance analysts to confirm that the correct
# results are being produced.

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

COMP_C_VIEW = """
    DROP VIEW IF EXISTS cubic_reports.wc700_comp_c;
    CREATE VIEW cubic_reports.wc700_comp_c
    AS
    SELECT
        t.settlement_day_key
        ,t.operating_day_key
        ,rc.rider_class_name
        ,fp.fare_prod_name
        ,t.service_type_id
        ,t.fare_rule_description
        ,t.recovery_txn_type
        ,sum(t.minimum_fare_charge) / 100 AS recovery_calculation_amount
    FROM
        cubic_ods.edw_farerev_recovery_txn t
    LEFT JOIN
        cubic_ods.edw_rider_class_dimension rc
        ON
            rc.rider_class_id = t.rider_class_id
    LEFT JOIN
        cubic_ods.edw_fare_product_dimension fp
        ON
            fp.fare_prod_key = t.fare_prod_key
    WHERE
        fp.monetary_inst_type_id = 2
        AND t.minimum_fare_charge IS NOT NULL
    GROUP BY
        t.operating_day_key
        ,t.settlement_day_key
        ,t.service_type_id
        ,rc.rider_class_name
        ,fp.fare_prod_name
        ,t.fare_rule_description
        ,t.recovery_txn_type
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


LATE_TAP_ADJUSTMENT = """
    SELECT
        s.settlement_day_key
        ,s.operating_day_key
        ,u.patron_trip_id
        ,u.trip_price_count
        ,tp.is_reversal
        ,u.token_id
        ,s.purse_load_id
        ,-tp.stored_value / 100 AS uncollectible_amount
        ,tp.transaction_dtm
        ,u.transit_account_id
        ,tm.travel_mode_name
    FROM
        cubic_ods.edw_sale_transaction s
    JOIN
        cubic_ods.edw_trip_payment tp
        ON
            tp.purse_load_id = s.purse_load_id
    JOIN
        cubic_ods.edw_patron_trip t
        ON
            t.patron_trip_id = tp.patron_trip_id
    JOIN
        cubic_ods.edw_use_transaction u
        ON
            u.patron_trip_id = tp.patron_trip_id
            AND u.trip_price_count = tp.trip_price_count
    LEFT JOIN
        cubic_ods.edw_transaction_history en
        ON
            en.dw_transaction_id = t.dw_entry_txn_id
    LEFT JOIN
        cubic_ods.edw_travel_mode_dimension tm
        ON
            tm.travel_mode_id = en.travel_mode_id
    WHERE
        s.sale_type_key = 26
        AND u.value_changed <> 0
        AND
        (
            (
                tp.je_is_fare_adjustment = 0
                AND u.ride_type_key <> 24
                AND tp.is_reversal = 1
                AND u.fare_due < 0
            )
            OR
            (
                tp.je_is_fare_adjustment = 0
                AND u.ride_type_key <> 24
                AND tp.is_reversal = 0
                AND u.fare_due > 0
            )
            OR (tp.je_is_fare_adjustment = 1 AND u.ride_type_key = 24)
        )
    ORDER BY
        s.operating_day_key desc
        ,s.settlement_day_key desc
"""


COMP_B_ADDENDUM = """
    SELECT
        ut.dw_transaction_id
        ,ut.transit_account_id
        ,ut.operating_day_key as operating_date
        ,ut.posting_day_key as posting_date
        ,ut.settlement_day_key as settlement_date
        ,ut.transaction_dtm
        ,th.transit_mode_name
        ,ut.patron_trip_id
        ,tr.fare_rule_description
        ,ut.transfer_sequence_nbr
        ,cd.bin
        ,-(tp.bankcard_value
            + case when tp.bankcard_payment_id is not null then tp.uncollectible_amount else 0 end
            + case when s.purse_load_id is not null then tp.stored_value else 0 end
        ) as fare_revenue
        ,case when ut.transfer_sequence_nbr > 0 and ut.fare_due <> 0 then 'Transfer' else null end
        as extension_charge_reason
        ,ut.retrieval_ref_nbr
    FROM
        cubic_ods.edw_use_transaction ut
    JOIN cubic_ods.edw_patron_trip tr
        ON tr.patron_trip_id = ut.patron_trip_id AND tr.source = ut.source
    JOIN cubic_ods.edw_trip_payment tp
        ON
            tp.patron_trip_id = ut.patron_trip_id
            AND tp.source = ut.source
            AND tp.trip_price_count = ut.trip_price_count
    JOIN cubic_ods.edw_card_dimension cd
        ON cd.card_key = ut.card_key
    LEFT JOIN cubic_ods.edw_sale_transaction s
        ON s.purse_load_id = tp.purse_load_id
    LEFT JOIN cubic_ods.edw_transaction_history th
        ON th.dw_transaction_id = ut.dw_transaction_id
    WHERE
        s.sale_type_key = 26
        and (
                (
                    tp.journal_entry_type_id <> 131
                    AND tp.is_reversal = 1
                    AND ut.txn_status_key in (36, 59)
                )
                or (
                    tp.journal_entry_type_id <> 131
                    AND tp.is_reversal = 0
                    AND ut.txn_status_key not in (36, 59, 50)
                )
                or (tp.journal_entry_type_id = 131 and ut.ride_type_key = 24)
            )
        and (
            ut.bankcard_payment_value <> 0
            or ut.uncollectible_amount <> 0
            or ut.value_changed <> 0
        )
"""
