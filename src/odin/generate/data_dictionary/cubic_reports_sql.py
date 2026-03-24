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

WC700_COMP_A_VIEW = """
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


WC700_COMP_B_VIEW = """
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

WC700_COMP_C_VIEW = """
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


WC700_COMP_D_VIEW = """
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


WC320_LATE_TAP_ADJUSTMENT = """
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
# This report is being published with the following tables missing from the data repository:
# - DISTRIBUTOR_DIMENSION
# - ACTIVITY_TYPE_DIMENSION
#
# ACTIVITY_TYPE_DIMENSION may be equivalent to ACTIVITY_CODE_DIMENSTION (which is available from
# incoming, but not in the data repository)
# This report is essentially a supplemented version of the CCH_AFC_TRANSACTION table...
# TODO: The name of the CASE statement rows is currently unknown.
WC231_CLEARING_HOUSE = """
    DROP VIEW IF EXISTS cubic_reports.wc231_clearing_house;
    CREATE VIEW cubic_reports.wc231_clearing_house
    AS
    SELECT
        CCH_STAGE_CATEGORY.CATEGORY_NAME,
        EDW_CCH_AFC_TRANSACTION.DEVICE_ID,
        case
            WHEN EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_NAME IS NULL
            AND EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION IS NOT NULL THEN 'Transit Value'
            ELSE EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_NAME
        END as fare_prod_name,
        EDW_CCH_AFC_TRANSACTION.LOAD_TYPE_ID,
        EDW_PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_NAME,
        EDW_ROUTE_DIMENSION.ROUTE_DESIGNATOR_DESC,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_DTM,
        EDW_CCH_AFC_TRANSACTION.ADMINISTRATIVE_FEE / 100 as administrative_fee,
        EDW_CCH_AFC_TRANSACTION.BONUS_ADDED / 100 as bonus_added,
        EDW_CCH_AFC_TRANSACTION.DEPOSIT / 100 as deposit,
        EDW_CCH_AFC_TRANSACTION.REPLACEMENT_FEE / 100 as replacement_fee,
        (
            CASE
                WHEN EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'U'
                THEN EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION + EDW_CCH_AFC_TRANSACTION.BENEFIT_VALUE
                  + EDW_CCH_AFC_TRANSACTION.BANKCARD_PAYMENT_VALUE
                WHEN EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'S'
                THEN EDW_CCH_AFC_TRANSACTION.NET_VALUE
                ELSE EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION
            END
        ) / 100 as transaction_value,
        EDW_CCH_AFC_TRANSACTION.MONEY_PAID / 100 as money_paid,
        EDW_CCH_AFC_TRANSACTION.NET_MONEY_PAID / 100 as net_money_paid,
        EDW_CCH_AFC_TRANSACTION.NET_VALUE / 100 as net_value,
        EDW_CCH_AFC_TRANSACTION.OVERPAY / 100 as overpay,
        EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION / 100 as sv_transaction,
        EDW_CCH_AFC_TRANSACTION.UNDERPAY / 100 as underpay,
        EDW_CCH_AFC_TRANSACTION.BENEFIT_VALUE / 100 as benefit_value,
        EDW_CCH_AFC_TRANSACTION.BANKCARD_PAYMENT_VALUE / 100 as bankcard_payment_value,
        CASE
            WHEN (
                EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'U'
                AND EDW_CCH_AFC_TRANSACTION.PASS_USE_COUNT = 1
            )
            OR EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID IN ('S', 'O', 'M')
            THEN EDW_CCH_AFC_TRANSACTION.PASS_COST / 100
            ELSE 0
        END as pass_cost,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_ID,
        EDW_CCH_AFC_TRANSACTION.DW_TRANSACTION_ID,
        EDW_FARE_PRODUCT_DIMENSION.MONETARY_INST_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.SHIPPING_FEE / 100 as shipping_fee,
        FINANCIALLY_RESP_BE_DIMENSION.BUSINESS_ENTITY_NAME,
        EDW_BUSINESS_ENTITY_DIMENSION.BUSINESS_ENTITY_NAME,
        EDW_CCH_AFC_TRANSACTION.PAYMENT_AMOUNT / 100 as payment_amount,
        CCH_STAGE_CATEGORY.TRANSACTION_CATEGORY,
        COALESCE(EDW_CCH_AFC_TRANSACTION.REFUND_FEE / 100, 0) as refund_fee,
        EDW_CCH_AFC_TRANSACTION.PASS_LOAD_AMT_PAID_BY_BV / 100 as pass_load_amt_paid_by_bv,
        EDW_CCH_AFC_TRANSACTION.PASS_LOAD_AMT_NOT_PAID_BY_BV / 100 as pass_load_amt_not_paid_by_bv,
        EDW_SALE_TYPE_DIMENSION.SALE_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.ONE_ACCOUNT_VALUE / 100 as one_account_value,
        EDW_CCH_AFC_TRANSACTION.UNCOLLECTIBLE_AMOUNT / 100 as uncollectible_amount,
        CCH_STAGE_REPROCESS_ACTION.SHORT_DESC,
        EDW_CCH_AFC_TRANSACTION.USER_REF_1,
        EDW_CCH_AFC_TRANSACTION.USER_REF_2,
        EDW_CCH_AFC_TRANSACTION.USER_REF_3,
        EDW_CCH_AFC_TRANSACTION.MERCHANT_NUMBER,
        EDW_CREDIT_CARD_TYPE_DIMENSION.CREDIT_CARD_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.DEVICE_ID,
        EDW_CCH_AFC_TRANSACTION.CAPTURE_DATE,
        EDW_CCH_AFC_TRANSACTION.BANK_SETTLEMENT_DATE,
        EDW_TRANSACTION_ORIGIN_DIMENSION.TRANSACTION_ORIGIN_NAME,
        CCH_STAGE_CATEGORIZATION_RULE.RULE_ID,
        CCH_STAGE_CATEGORIZATION_RULE.RULE_NAME,
        CCH_STAGE_TRANSACTION_TYPE.DESCRIPTION,
        COALESCE(EDW_CCH_AFC_TRANSACTION.CARD_FEE / 100, 0) as card_fee,
        EDW_CCH_AFC_TRANSACTION.PASS_USE_COUNT,
        EDW_CCH_AFC_TRANSACTION.ACTIVITY_TYPE_ID,
        EDW_CCH_AFC_TRANSACTION.BOOKING_PREPAID_VALUE / 100 as booking_prepaid_value,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_REF_NBR,
        EDW_MEDIA_TYPE_DIMENSION.MEDIA_TYPE_NAME,
        EDW_RIDER_CLASS_DIMENSION.RIDER_CLASS_NAME,
        CCH_STAGE_TRANSACTION_TYPE.TRANSACTION_TYPE_ID,
        COALESCE(EDW_CCH_AFC_TRANSACTION.DISCOUNT_APPLIED / 100, 0) AS discount_applied,
        EDW_PURSE_TYPE_DIMENSION.PURSE_NAME,
        EDW_CCH_AFC_TRANSACTION.RESTRICTED_PURSE_VALUE / 100 as restricted_purse_value,
        EDW_CCH_AFC_TRANSACTION.RETRIEVAL_REF_NBR,
        EDW_CCH_AFC_TRANSACTION.TRANSIT_ACCOUNT_ID,
        COALESCE(EDW_CCH_AFC_TRANSACTION.REFUNDABLE_PURSE_VALUE / 100, 0) as refundable_purse_value,
        COALESCE(EDW_CCH_AFC_TRANSACTION.ROUNDING_AMOUNT / 100, 0) AS rounding_amount,
        COALESCE(EDW_CCH_AFC_TRANSACTION.PREPAID_BANKCARD_VALUE / 100, 0) AS prepaid_bankcard_value,
        COALESCE(EDW_CCH_AFC_TRANSACTION.POST_PAY_AMOUNT / 100, 0) AS post_pay_amount,
        BOARDING_STOP_DIMENSION.STOP_POINT_DISPLAY_DESC AS boarding_stop_point_display_desc,
        ALIGHTING_STOP_DIMENSION.STOP_POINT_DISPLAY_DESC AS alighting_stop_point_display_desc,
        EDW_CCH_AFC_TRANSACTION.ENABLEMENT_FEE / 100 as enablement_fee,
        EDW_CCH_AFC_TRANSACTION.PAYMENT_TRANSIT_ACCOUNT_ID,
        EDW_CCH_AFC_TRANSACTION.OPERATING_DATE,
        EDW_CCH_AFC_TRANSACTION.PASS_ID,
        EDW_CCH_AFC_TRANSACTION.ORDER_NBR,
        EDW_CCH_AFC_TRANSACTION.LINE_ITEM_NBR,
        ACTIVITY_TYPE_DIMENSION.SOURCE_ACTIVITY_TYPE_NAME,
        EDW_OPERATOR_DIMENSION.OPERATOR_NAME,
        EDW_CCH_AFC_TRANSACTION.USE_TYPE,
        EDW_DATE_DIMENSION.YEAR,
        EDW_DATE_DIMENSION.DTM,
        DISTRIBUTOR_DIMENSION.ORG_NAME
    FROM
        cubic_ods.EDW_CCH_AFC_TRANSACTION
    JOIN cubic_ods.CCH_STAGE_CATEGORY
        ON CCH_STAGE_CATEGORY.TRANSACTION_CATEGORY = EDW_CCH_AFC_TRANSACTION.TRANSACTION_CATEGORY
    LEFT JOIN cubic_ods.EDW_FARE_PRODUCT_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.FARE_PROD_KEY = EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_KEY
    LEFT JOIN cubic_ods.EDW_PAYMENT_TYPE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.PAYMENT_TYPE_KEY = EDW_PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_KEY
    LEFT JOIN cubic_ods.EDW_ROUTE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.ROUTE_KEY = EDW_ROUTE_DIMENSION.ROUTE_KEY
    LEFT JOIN cubic_ods.EDW_BUSINESS_ENTITY_DIMENSION
        ON EDW_BUSINESS_ENTITY_DIMENSION.BUSINESS_ENTITY_ID
            = EDW_CCH_AFC_TRANSACTION.BUSINESS_ENTITY_ID
    LEFT JOIN cubic_ods.EDW_BUSINESS_ENTITY_DIMENSION FINANCIALLY_RESP_BE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.SETTLEMENT_BUSINESS_ENTITY_ID
            = FINANCIALLY_RESP_BE_DIMENSION.BUSINESS_ENTITY_KEY
    LEFT JOIN cubic_ods.EDW_SALE_TYPE_DIMENSION
        ON EDW_SALE_TYPE_DIMENSION.SALE_TYPE_KEY = EDW_CCH_AFC_TRANSACTION.SALE_TYPE_KEY
    LEFT JOIN cubic_ods.CCH_STAGE_REPROCESS_ACTION
        ON CCH_STAGE_REPROCESS_ACTION.REPROCESS_ACTION_ID
            = EDW_CCH_AFC_TRANSACTION.PROCESSING_TYPE_ID
    LEFT JOIN cubic_ods.EDW_CREDIT_CARD_TYPE_DIMENSION
        ON EDW_CREDIT_CARD_TYPE_DIMENSION.CPA_CREDIT_CARD_TYPE_ID
            = EDW_CCH_AFC_TRANSACTION.CREDIT_CARD_TYPE_ID
    LEFT JOIN cubic_ods.EDW_TRANSACTION_ORIGIN_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.TRANSACTION_ORIGIN_ID
            = EDW_TRANSACTION_ORIGIN_DIMENSION.TRANSACTION_ORIGIN_KEY
    LEFT JOIN cubic_ods.CCH_STAGE_CATEGORIZATION_RULE
        ON CCH_STAGE_CATEGORIZATION_RULE.RULE_ID = EDW_CCH_AFC_TRANSACTION.CATEGORIZATION_RULE_ID
    LEFT JOIN cubic_ods.CCH_STAGE_TRANSACTION_TYPE
        ON CCH_STAGE_TRANSACTION_TYPE.TRANSACTION_TYPE_ID
            = EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID
    LEFT JOIN cubic_ods.EDW_MEDIA_TYPE_DIMENSION
        ON EDW_MEDIA_TYPE_DIMENSION.MEDIA_TYPE_ID = EDW_CCH_AFC_TRANSACTION.MEDIA_TYPE_ID
    LEFT JOIN cubic_ods.EDW_RIDER_CLASS_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.RIDER_CLASS_ID = EDW_RIDER_CLASS_DIMENSION.RIDER_CLASS_ID
    LEFT JOIN cubic_ods.EDW_PURSE_TYPE_DIMENSION
        ON EDW_PURSE_TYPE_DIMENSION.PURSE_SKU = EDW_CCH_AFC_TRANSACTION.PURSE_SKU
    LEFT JOIN cubic_ods.EDW_OPERATOR_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.OPERATOR_ID = EDW_OPERATOR_DIMENSION.OPERATOR_ID
    LEFT JOIN cubic_ods.EDW_DATE_DIMENSION
        ON EDW_DATE_DIMENSION.DTM = EDW_CCH_AFC_TRANSACTION.SUMMARY_PERIOD
    LEFT JOIN cubic_ods.DISTRIBUTOR_ORDER
        ON DISTRIBUTOR_ORDER.DISTRIBUTOR_ORDER_KEY = EDW_CCH_AFC_TRANSACTION.TRANSACTION_ID
    LEFT JOIN cubic_ods.DISTRIBUTOR_DIMENSION
        ON DISTRIBUTOR_DIMENSION.DISTRIBUTOR_KEY = DISTRIBUTOR_ORDER.DISTRIBUTOR_KEY
    LEFT JOIN cubic_ods.ACTIVITY_TYPE_DIMENSION
        ON ACTIVITY_TYPE_DIMENSION.ACTIVITY_TYPE_KEY = EDW_CCH_AFC_TRANSACTION.ACTIVITY_TYPE_ID
    LEFT JOIN cubic_ods.EDW_STOP_POINT_DIMENSION BOARDING_STOP_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.BOARDING_STOP_KEY = BOARDING_STOP_DIMENSION.STOP_POINT_KEY
    LEFT JOIN cubic_ods.EDW_STOP_POINT_DIMENSION ALIGHTING_STOP_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.ALIGHTING_STOP_KEY = ALIGHTING_STOP_DIMENSION.STOP_POINT_KEY
    ;
"""


WA160 = """
    DROP VIEW IF EXISTS cubic_reports.wa160;
    CREATE VIEW cubic_reports.wa160
    AS
    SELECT
        strptime(posting_day_key::varchar,'%Y%m%d')::date as posting_date,
        strptime(settlement_day_key::varchar,'%Y%m%d')::date as settlement_date,
        strptime(transit_day_key::varchar,'%Y%m%d')::date as transit_date,
        strptime(ut.operating_day_key::varchar,'%Y%m%d')::date as operating_date,
        ut.transaction_dtm,
        ut.source_inserted_dtm,
        voided_dtm,
        opd.operator_name,
        fpd.fare_prod_name,
        ut.transit_account_id,
        ut.serial_nbr,
        ut.pass_use_count,
        coalesce(ut.pass_cost, 0)::double / 100 as pass_cost,
        coalesce(ut.value_changed, 0)::double / 100 as value_changed,
        coalesce(ut.booking_prepaid_value, 0)::double / 100 as booking_prepaid_value,
        coalesce(ut.benefit_value, 0)::double / 100 as benefit_value,
        coalesce(ut.bankcard_payment_value, 0)::double / 100 as bankcard_payment_value,
        coalesce(ut.merchant_service_fee, 0)::double / 100 as merchant_service_fee,
        (
            coalesce(ut.pass_cost, 0)
            + coalesce(ut.value_changed, 0)
            + coalesce(ut.booking_prepaid_value, 0)
            + coalesce(ut.benefit_value, 0)
            + coalesce(ut.bankcard_payment_value,0)
            + coalesce(ut.merchant_service_fee, 0)
        )::double / 100 as total_use_cost,
        transaction_status_name,
        fpuld.fare_prod_users_list_name,
        trip_price_count,
        ut.bus_id,
        txnsd.txn_status_name,
        paygo_ride_count,
        ride_count,
        transaction_id,
        transfer_flag,
        transfer_sequence_nbr,
        tad.account_status_name,
        dw_transaction_id,
        ut.token_id,
        pass_id,
        ut.pg_card_id,
        mtd.media_type_name,
        purse_name,
        patron_trip_id,
        retrieval_ref_nbr,
        txnsd.successful_use_flag,
        ut.facility_id,
        tap_id,
        rtd.ride_type_name,
        calculated_fare,
        coalesce(fpd.rider_class_name, tad.rider_class_name) as rider_class_name,
        coalesce(emd.external_ref, emd.customer_member_id) as external_ref,
        coalesce(one_account_value, 0) as one_account_value,
        coalesce(ut.restricted_purse_value, 0) as restricted_purse_value,
        coalesce(ut.refundable_purse_value, 0) as refundable_purse_value,
        coalesce(ut.uncollectible_amount, 0)::double / 100 as uncollectible_amount
    FROM
        cubic_ods.edw_use_transaction ut
    LEFT JOIN
        cubic_ods.edw_fare_product_dimension fpd on ut.fare_prod_key = fpd.fare_prod_key
    LEFT JOIN
        cubic_ods.edw_member_dimension emd on ut.transit_account_id = emd.transit_account_id
    LEFT JOIN
        cubic_ods.edw_operator_dimension opd on ut.operator_key = opd.operator_key
    LEFT JOIN
        cubic_ods.edw_card_dimension cardd on ut.card_key = cardd.card_key
    LEFT JOIN
        cubic_ods.edw_ride_type_dimension rtd on ut.ride_type_key = rtd.ride_type_key
    LEFT JOIN
        cubic_ods.edw_txn_status_dimension txnsd on ut.txn_status_key = txnsd.txn_status_key
    LEFT JOIN
        cubic_ods.edw_media_type_dimension mtd on ut.media_type_key = mtd.media_type_key
    LEFT JOIN
        cubic_ods.edw_transit_account_dimension tad
            on cardd.transit_account_key = tad.transit_account_key
    LEFT JOIN
        cubic_ods.edw_fare_prod_users_list_dimension fpuld
            on fpuld.fare_prod_users_list_key = fpd.fare_prod_users_list_key
    ;
"""

AD_HOC_JOURNAL_ENTRIES = """
    DROP VIEW IF EXISTS cubic_reports.ad_hoc_journal_entries;
    CREATE VIEW cubic_reports.ad_hoc_journal_entries
    AS
    SELECT
    accounting_date,
    voucher,
    ledger_account,
    txn_currency_debit_amount,
    txn_currency_credit_amount,
    external_account_name,
    description,
    split_part(description, '|', 1) as transaction_category,
    split_part(description, '|', 2) as apportionment_rule,
    split_part(description, '|', 4) as operating_date,
    split_part(description, '|', 5) as fare_instrument_id
    FROM
        cubic_ods.edw_fnp_general_jrnl_account_entry
    WHERE
        ledger_name = 'MBTA'
    ;
"""

WC231_PASS_ID_ADHOC = """
    DROP VIEW IF EXISTS cubic_reports.wc231_pass_id_adhoc;
    CREATE VIEW cubic_reports.wc231_pass_id_adhoc
    AS
    SELECT
        CCH_STAGE_CATEGORY.CATEGORY_NAME,
        EDW_CCH_AFC_TRANSACTION.DEVICE_ID,
        case
            WHEN EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_NAME IS NULL
            AND EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION IS NOT NULL THEN 'Transit Value'
            ELSE EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_NAME
        END as fare_prod_name,
        EDW_CCH_AFC_TRANSACTION.LOAD_TYPE_ID,
        EDW_PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_NAME,
        EDW_ROUTE_DIMENSION.ROUTE_DESIGNATOR_DESC,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_DTM,
        EDW_CCH_AFC_TRANSACTION.ADMINISTRATIVE_FEE / 100 as administrative_fee,
        EDW_CCH_AFC_TRANSACTION.BONUS_ADDED / 100 as bonus_added,
        EDW_CCH_AFC_TRANSACTION.DEPOSIT / 100 as deposit,
        EDW_CCH_AFC_TRANSACTION.REPLACEMENT_FEE / 100 as replacement_fee,
        (
            CASE
                WHEN EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'U'
                THEN EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION + EDW_CCH_AFC_TRANSACTION.BENEFIT_VALUE
                  + EDW_CCH_AFC_TRANSACTION.BANKCARD_PAYMENT_VALUE
                WHEN EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'S'
                THEN EDW_CCH_AFC_TRANSACTION.NET_VALUE
                ELSE EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION
            END
        ) / 100,
        EDW_CCH_AFC_TRANSACTION.MONEY_PAID / 100 as money_paid,
        EDW_CCH_AFC_TRANSACTION.NET_MONEY_PAID / 100 as net_money_paid,
        EDW_CCH_AFC_TRANSACTION.NET_VALUE / 100 as net_value,
        EDW_CCH_AFC_TRANSACTION.OVERPAY / 100 as overpay,
        EDW_CCH_AFC_TRANSACTION.SV_TRANSACTION / 100 as sv_transaction,
        EDW_CCH_AFC_TRANSACTION.UNDERPAY / 100 as underpay,
        EDW_CCH_AFC_TRANSACTION.BENEFIT_VALUE / 100 as benefit_value,
        EDW_CCH_AFC_TRANSACTION.BANKCARD_PAYMENT_VALUE / 100 as bankcard_payment_value,
        CASE
            WHEN (
                EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID = 'U'
                AND EDW_CCH_AFC_TRANSACTION.PASS_USE_COUNT = 1
            )
            OR EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID IN ('S', 'O', 'M')
            THEN EDW_CCH_AFC_TRANSACTION.PASS_COST / 100
            ELSE 0
        END,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_ID,
        EDW_CCH_AFC_TRANSACTION.DW_TRANSACTION_ID,
        EDW_FARE_PRODUCT_DIMENSION.MONETARY_INST_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.SHIPPING_FEE / 100 as shipping_fee,
        FINANCIALLY_RESP_BE_DIMENSION.BUSINESS_ENTITY_NAME,
        EDW_BUSINESS_ENTITY_DIMENSION.BUSINESS_ENTITY_NAME,
        EDW_CCH_AFC_TRANSACTION.PAYMENT_AMOUNT / 100 as payment_amount,
        CCH_STAGE_CATEGORY.TRANSACTION_CATEGORY,
        COALESCE(EDW_CCH_AFC_TRANSACTION.REFUND_FEE / 100, 0) as refund_fee,
        -- DISTRIBUTOR_DIMENSION.ORG_NAME,
        EDW_CCH_AFC_TRANSACTION.PASS_LOAD_AMT_PAID_BY_BV / 100 as pass_load_amt_paid_by_bv,
        EDW_CCH_AFC_TRANSACTION.PASS_LOAD_AMT_NOT_PAID_BY_BV / 100 as pass_load_amt_not_paid_by_bv,
        EDW_SALE_TYPE_DIMENSION.SALE_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.ONE_ACCOUNT_VALUE / 100 as one_account_value,
        EDW_CCH_AFC_TRANSACTION.UNCOLLECTIBLE_AMOUNT / 100 as uncollectible_amount,
        CCH_STAGE_REPROCESS_ACTION.SHORT_DESC,
        EDW_CCH_AFC_TRANSACTION.USER_REF_1,
        EDW_CCH_AFC_TRANSACTION.USER_REF_2,
        EDW_CCH_AFC_TRANSACTION.USER_REF_3,
        EDW_CCH_AFC_TRANSACTION.MERCHANT_NUMBER,
        EDW_CREDIT_CARD_TYPE_DIMENSION.CREDIT_CARD_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.DEVICE_ID,
        EDW_CCH_AFC_TRANSACTION.CAPTURE_DATE,
        EDW_CCH_AFC_TRANSACTION.BANK_SETTLEMENT_DATE,
        EDW_TRANSACTION_ORIGIN_DIMENSION.TRANSACTION_ORIGIN_NAME,
        CCH_STAGE_CATEGORIZATION_RULE.RULE_ID,
        CCH_STAGE_CATEGORIZATION_RULE.RULE_NAME,
        CCH_STAGE_TRANSACTION_TYPE.DESCRIPTION,
        COALESCE(EDW_CCH_AFC_TRANSACTION.CARD_FEE / 100, 0) as card_fee,
        EDW_CCH_AFC_TRANSACTION.PASS_USE_COUNT,
        -- ACTIVITY_TYPE_DIMENSION.SOURCE_ACTIVITY_TYPE_NAME,
        EDW_CCH_AFC_TRANSACTION.ACTIVITY_TYPE_ID,
        EDW_CCH_AFC_TRANSACTION.BOOKING_PREPAID_VALUE / 100 as booking_prepaid_value,
        EDW_CCH_AFC_TRANSACTION.TRANSACTION_REF_NBR,
        EDW_MEDIA_TYPE_DIMENSION.MEDIA_TYPE_NAME,
        EDW_RIDER_CLASS_DIMENSION.RIDER_CLASS_NAME,
        CCH_STAGE_TRANSACTION_TYPE.TRANSACTION_TYPE_ID,
        COALESCE(EDW_CCH_AFC_TRANSACTION.DISCOUNT_APPLIED / 100, 0) AS discount_applied,
        EDW_PURSE_TYPE_DIMENSION.PURSE_NAME,
        EDW_CCH_AFC_TRANSACTION.RESTRICTED_PURSE_VALUE / 100 as restricted_purse_value,
        EDW_CCH_AFC_TRANSACTION.RETRIEVAL_REF_NBR,
        EDW_CCH_AFC_TRANSACTION.TRANSIT_ACCOUNT_ID,
        ut.pass_id,
        COALESCE(EDW_CCH_AFC_TRANSACTION.REFUNDABLE_PURSE_VALUE / 100, 0) as refundable_purse_value,
        EDW_CCH_AFC_TRANSACTION.ENABLEMENT_FEE / 100 as enablement_fee
    FROM
        cubic_ods.EDW_CCH_AFC_TRANSACTION
    JOIN cubic_ods.CCH_STAGE_CATEGORY
        ON CCH_STAGE_CATEGORY.TRANSACTION_CATEGORY = EDW_CCH_AFC_TRANSACTION.TRANSACTION_CATEGORY
    LEFT JOIN cubic_ods.EDW_FARE_PRODUCT_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.FARE_PROD_KEY = EDW_FARE_PRODUCT_DIMENSION.FARE_PROD_KEY
    LEFT JOIN cubic_ods.EDW_PAYMENT_TYPE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.PAYMENT_TYPE_KEY = EDW_PAYMENT_TYPE_DIMENSION.PAYMENT_TYPE_KEY
    LEFT JOIN cubic_ods.EDW_ROUTE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.ROUTE_KEY = EDW_ROUTE_DIMENSION.ROUTE_KEY
    LEFT JOIN cubic_ods.EDW_BUSINESS_ENTITY_DIMENSION
        ON EDW_BUSINESS_ENTITY_DIMENSION.BUSINESS_ENTITY_ID
            = EDW_CCH_AFC_TRANSACTION.BUSINESS_ENTITY_ID
    LEFT JOIN cubic_ods.EDW_BUSINESS_ENTITY_DIMENSION FINANCIALLY_RESP_BE_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.SETTLEMENT_BUSINESS_ENTITY_ID
            = FINANCIALLY_RESP_BE_DIMENSION.BUSINESS_ENTITY_KEY
    -- LEFT JOIN DISTRIBUTOR_DIMENSION
    --     ON DISTRIBUTOR_DIMENSION.DISTRIBUTOR_KEY = EDW_CCH_AFC_TRANSACTION.TRANSACTION_ID
    LEFT JOIN cubic_ods.EDW_SALE_TYPE_DIMENSION
        ON EDW_SALE_TYPE_DIMENSION.SALE_TYPE_KEY = EDW_CCH_AFC_TRANSACTION.SALE_TYPE_KEY
    LEFT JOIN cubic_ods.CCH_STAGE_REPROCESS_ACTION
        ON CCH_STAGE_REPROCESS_ACTION.REPROCESS_ACTION_ID
            = EDW_CCH_AFC_TRANSACTION.PROCESSING_TYPE_ID
    LEFT JOIN cubic_ods.EDW_CREDIT_CARD_TYPE_DIMENSION
        ON EDW_CREDIT_CARD_TYPE_DIMENSION.CPA_CREDIT_CARD_TYPE_ID
            = EDW_CCH_AFC_TRANSACTION.CREDIT_CARD_TYPE_ID
    LEFT JOIN cubic_ods.EDW_TRANSACTION_ORIGIN_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.TRANSACTION_ORIGIN_ID
            = EDW_TRANSACTION_ORIGIN_DIMENSION.TRANSACTION_ORIGIN_KEY
    LEFT JOIN cubic_ods.CCH_STAGE_CATEGORIZATION_RULE
        ON CCH_STAGE_CATEGORIZATION_RULE.RULE_ID = EDW_CCH_AFC_TRANSACTION.CATEGORIZATION_RULE_ID
    LEFT JOIN cubic_ods.CCH_STAGE_TRANSACTION_TYPE
        ON CCH_STAGE_TRANSACTION_TYPE.TRANSACTION_TYPE_ID
            = EDW_CCH_AFC_TRANSACTION.TRANSACTION_TYPE_ID
    -- LEFT JOIN ACTIVITY_TYPE_DIMENSION
    --     ON ACTIVITY_TYPE_DIMENSION.ACTIVITY_TYPE_KEY = EDW_CCH_AFC_TRANSACTION.ACTIVITY_TYPE_ID
    LEFT JOIN cubic_ods.EDW_MEDIA_TYPE_DIMENSION
        ON EDW_MEDIA_TYPE_DIMENSION.MEDIA_TYPE_ID = EDW_CCH_AFC_TRANSACTION.MEDIA_TYPE_ID
    LEFT JOIN cubic_ods.EDW_RIDER_CLASS_DIMENSION
        ON EDW_CCH_AFC_TRANSACTION.RIDER_CLASS_ID = EDW_RIDER_CLASS_DIMENSION.RIDER_CLASS_ID
    LEFT JOIN cubic_ods.EDW_PURSE_TYPE_DIMENSION
        ON EDW_PURSE_TYPE_DIMENSION.PURSE_SKU = EDW_CCH_AFC_TRANSACTION.PURSE_SKU
    LEFT JOIN cubic_ods.edw_use_transaction ut
        ON ut.transaction_id = EDW_CCH_AFC_TRANSACTION.transaction_id
    ;
"""


COMP_A_TXN_A = """
    CREATE VIEW cubic_reports.farerev_payg_trip_txn_a AS
    WITH EDW_FAREREV_PAYG_TRIP_TXN AS
    (
    SELECT
        m.txn_channel_display,
        CAST('B' AS VARCHAR) AS computation_type,
        strptime(CAST(ut.operating_day_key AS VARCHAR), '%Y%m%d') AS operating_date,
        ut.transaction_dtm,
        strptime(CAST(ut.posting_day_key AS VARCHAR), '%Y%m%d') AS posting_date,
        strptime(CAST(ut.settlement_day_key AS VARCHAR), '%Y%m%d') AS settlement_date,
        th.transit_mode_name,
        ut.device_id,
        ut.dw_transaction_id,
        ut.transit_account_id,
        ut.patron_trip_id,
        NULL AS tap_id,
        tr.fare_rule_description,
        ut.transfer_sequence_nbr,
        pt.payment_type_name,
        cd.bin,
        -(sum(((tp.bankcard_value + CASE WHEN (((tp.bankcard_payment_id IS NOT NULL) OR (mt.payg_flag = 1))) THEN (tp.uncollectible_amount) ELSE 0 END) + CASE WHEN (((s.purse_load_id IS NOT NULL) AND (mt.payg_flag = 1))) THEN (tp.stored_value) ELSE 0 END))) AS fare_revenue,
        CASE
            WHEN (((ut.transfer_sequence_nbr > 0)
            AND (ut.fare_due != 0))) THEN ('Transfer')
            ELSE NULL
        END AS extension_charge_reason,
        bcp.retrieval_ref_nbr,
        pt.payment_type_key,
        ut.operating_day_key,
        ut.settlement_day_key,
        ut.posting_day_key
    FROM
        cubic_ods.edw_use_transaction AS ut
    INNER JOIN cubic_ods.edw_patron_trip AS tr ON
        (((tr.patron_trip_id = ut.patron_trip_id)
            AND (tr."source" = ut."source")))
    INNER JOIN cubic_ods.edw_trip_payment AS tp ON
        (((tp.patron_trip_id = tr.patron_trip_id)
            AND (tp."source" = tr."source")
                AND (tp.trip_price_count = ut.trip_price_count)
                    AND (((tp.je_is_fare_adjustment = 0)
                        AND (ut.ride_type_key != 24)
                            AND (tp.is_reversal = 1)
                                AND (ut.fare_due < 0))
                        OR ((tp.je_is_fare_adjustment = 0)
                            AND (ut.ride_type_key != 24)
                                AND (tp.is_reversal = 0)
                                    AND (ut.fare_due > 0))
                            OR ((tp.je_is_fare_adjustment = 1)
                                AND (ut.ride_type_key = 24)))))
    LEFT JOIN cubic_ods.edw_sale_transaction AS s ON
        (((s.purse_load_id = tp.purse_load_id)
            AND (s.sale_type_key = 26)))
    LEFT JOIN cubic_ods.edw_sale_transaction AS bcp ON
        (((bcp.bankcard_payment_id = tp.bankcard_payment_id)
            AND (bcp.sale_type_key = 20)
                AND (bcp.bankcard_payment_type_key IN (1, 2, 3))))
    INNER JOIN (
        SELECT
            payment_type_name,
            payment_type_key
        FROM
            cubic_ods.edw_payment_type_dimension
        WHERE
            (payment_type_key = 2)) AS pt ON
        ((1 = 1))
    LEFT JOIN cubic_ods.edw_card_dimension AS cd ON
        ((cd.card_key = ut.card_key))
    INNER JOIN cubic_ods.edw_media_type_dimension AS mt ON
        ((mt.media_type_key = ut.media_type_key))
    INNER JOIN (
        SELECT
            txn_channel_display
        FROM
            cubic_ods.edw_txn_channel_map
        WHERE
            ((txn_source = 'UseTxn')
                AND (sales_channel_key = 8)
                    AND (payment_type_key = 2))) AS m ON
        ((1 = 1))
    LEFT JOIN cubic_ods.edw_transaction_history AS th ON
        ((th.dw_transaction_id = ut.dw_transaction_id))
    WHERE
        ((ut.bankcard_payment_value != 0)
            OR (ut.uncollectible_amount != 0)
                OR (ut.value_changed != 0))
    GROUP BY
        m.txn_channel_display,
        ut.transaction_dtm,
        th.transit_mode_name,
        ut.device_id,
        ut.dw_transaction_id,
        ut.transit_account_id,
        ut.patron_trip_id,
        tr.fare_rule_description,
        ut.transfer_sequence_nbr,
        pt.payment_type_name,
        cd.bin,
        CASE
            WHEN (((ut.transfer_sequence_nbr > 0)
                AND (ut.fare_due != 0))) THEN ('Transfer')
            ELSE NULL
        END,
        bcp.retrieval_ref_nbr,
        pt.payment_type_key,
        ut.operating_day_key,
        ut.settlement_day_key,
        ut.posting_day_key
    HAVING
        (sum(((tp.bankcard_value + CASE WHEN (((tp.bankcard_payment_id IS NOT NULL) OR (mt.payg_flag = 1))) THEN (tp.uncollectible_amount) ELSE 0 END) + CASE WHEN (((s.purse_load_id IS NOT NULL) AND (mt.payg_flag = 1))) THEN (tp.stored_value) ELSE 0 END)) != 0)
    UNION ALL (
    SELECT
    m.txn_channel_display,
    CAST('B' AS VARCHAR) AS computation_type,
    strptime(CAST(s.operating_day_key AS VARCHAR), '%Y%m%d') AS operating_date,
    s.transaction_dtm,
    strptime(CAST(s.posting_day_key AS VARCHAR), '%Y%m%d') AS posting_date,
    strptime(CAST(s.settlement_day_key AS VARCHAR), '%Y%m%d') AS settlement_date,
    th.transit_mode_name,
    s.device_id,
    s.dw_transaction_id,
    s.transit_account_id,
    NULL AS patron_trip_id,
    s.tap_id,
    NULL AS fare_rule_description,
    NULL AS transfer_sequence_nbr,
    pt.payment_type_name,
    cd.bin,
    sp.payment_value AS fare_revenue,
    NULL AS extension_charge_reason,
    sp.retrieval_ref_nbr,
    pt.payment_type_key,
    s.operating_day_key,
    s.settlement_day_key,
    s.posting_day_key
    FROM
    cubic_ods.edw_sale_transaction AS s
    INNER JOIN cubic_ods.edw_sale_txn_payment AS sp ON
    (((sp.dw_transaction_id = s.dw_transaction_id)
        AND (sp.transaction_dtm = s.transaction_dtm)))
    INNER JOIN cubic_ods.edw_media_type_dimension AS mt ON
    ((mt.media_type_key = s.media_type_key))
    INNER JOIN cubic_ods.edw_txn_channel_map AS m ON
    (((m.txn_source = 'UseTxn')
        AND (m.sales_channel_key = 14)
            AND (m.payment_type_key = CASE
                WHEN ((s.reason_key IN (990951, 990961))) THEN (2)
                ELSE 4
            END)))
    INNER JOIN cubic_ods.edw_payment_type_dimension AS pt ON
    ((pt.payment_type_key = m.payment_type_key))
    LEFT JOIN cubic_ods.edw_read_transaction AS rt ON
    ((rt.tap_id = s.tap_id))
    LEFT JOIN cubic_ods.edw_transaction_history AS th ON
    ((th.dw_transaction_id = rt.dw_transaction_id))
    LEFT JOIN cubic_ods.edw_card_dimension AS cd ON
    ((cd.card_key = rt.card_key))
    WHERE
    ((s.operating_day_key IS NOT NULL)
        AND (s.sale_type_key = 22)
            AND (s.reason_key IN (990951, 990961, 990952, 990962))
                AND (mt.payg_flag = 1)))),
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
        transit_mode_name,
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
"""

COMP_A_TXN_C = """
    CREATE VIEW cubic_reports.farerev_payg_trip_txn_c AS
    WITH EDW_FAREREV_PAYG_TRIP_TXN AS
    (
    SELECT
        m.txn_channel_display,
        CAST('B' AS VARCHAR) AS computation_type,
        strptime(CAST(ut.operating_day_key AS VARCHAR), '%Y%m%d') AS operating_date,
        ut.transaction_dtm,
        strptime(CAST(ut.posting_day_key AS VARCHAR), '%Y%m%d') AS posting_date,
        strptime(CAST(ut.settlement_day_key AS VARCHAR), '%Y%m%d') AS settlement_date,
        th.transit_mode_name,
        ut.device_id,
        ut.dw_transaction_id,
        ut.transit_account_id,
        ut.patron_trip_id,
        NULL AS tap_id,
        tr.fare_rule_description,
        ut.transfer_sequence_nbr,
        pt.payment_type_name,
        cd.bin,
        -(sum(((tp.bankcard_value + CASE WHEN (((tp.bankcard_payment_id IS NOT NULL) OR (mt.payg_flag = 1))) THEN (tp.uncollectible_amount) ELSE 0 END) + CASE WHEN (((s.purse_load_id IS NOT NULL) AND (mt.payg_flag = 1))) THEN (tp.stored_value) ELSE 0 END))) AS fare_revenue,
        CASE
            WHEN (((ut.transfer_sequence_nbr > 0)
            AND (ut.fare_due != 0))) THEN ('Transfer')
            ELSE NULL
        END AS extension_charge_reason,
        bcp.retrieval_ref_nbr,
        pt.payment_type_key,
        ut.operating_day_key,
        ut.settlement_day_key,
        ut.posting_day_key
    FROM
        cubic_ods.edw_use_transaction AS ut
    INNER JOIN cubic_ods.edw_patron_trip AS tr ON
        (((tr.patron_trip_id = ut.patron_trip_id)
            AND (tr."source" = ut."source")))
    INNER JOIN cubic_ods.edw_trip_payment AS tp ON
        (((tp.patron_trip_id = tr.patron_trip_id)
            AND (tp."source" = tr."source")
                AND (tp.trip_price_count = ut.trip_price_count)
                    AND (((tp.je_is_fare_adjustment = 0)
                        AND (ut.ride_type_key != 24)
                            AND (tp.is_reversal = 1)
                                AND (ut.fare_due < 0))
                        OR ((tp.je_is_fare_adjustment = 0)
                            AND (ut.ride_type_key != 24)
                                AND (tp.is_reversal = 0)
                                    AND (ut.fare_due > 0))
                            OR ((tp.je_is_fare_adjustment = 1)
                                AND (ut.ride_type_key = 24)))))
    LEFT JOIN cubic_ods.edw_sale_transaction AS s ON
        (((s.purse_load_id = tp.purse_load_id)
            AND (s.sale_type_key = 26)))
    LEFT JOIN cubic_ods.edw_sale_transaction AS bcp ON
        (((bcp.bankcard_payment_id = tp.bankcard_payment_id)
            AND (bcp.sale_type_key = 20)
                AND (bcp.bankcard_payment_type_key IN (1, 2, 3))))
    INNER JOIN (
        SELECT
            payment_type_name,
            payment_type_key
        FROM
            cubic_ods.edw_payment_type_dimension
        WHERE
            (payment_type_key = 2)) AS pt ON
        ((1 = 1))
    LEFT JOIN cubic_ods.edw_card_dimension AS cd ON
        ((cd.card_key = ut.card_key))
    INNER JOIN cubic_ods.edw_media_type_dimension AS mt ON
        ((mt.media_type_key = ut.media_type_key))
    INNER JOIN (
        SELECT
            txn_channel_display
        FROM
            cubic_ods.edw_txn_channel_map
        WHERE
            ((txn_source = 'UseTxn')
                AND (sales_channel_key = 8)
                    AND (payment_type_key = 2))) AS m ON
        ((1 = 1))
    LEFT JOIN cubic_ods.edw_transaction_history AS th ON
        ((th.dw_transaction_id = ut.dw_transaction_id))
    WHERE
        ((ut.bankcard_payment_value != 0)
            OR (ut.uncollectible_amount != 0)
                OR (ut.value_changed != 0))
    GROUP BY
        m.txn_channel_display,
        ut.transaction_dtm,
        th.transit_mode_name,
        ut.device_id,
        ut.dw_transaction_id,
        ut.transit_account_id,
        ut.patron_trip_id,
        tr.fare_rule_description,
        ut.transfer_sequence_nbr,
        pt.payment_type_name,
        cd.bin,
        CASE
            WHEN (((ut.transfer_sequence_nbr > 0)
                AND (ut.fare_due != 0))) THEN ('Transfer')
            ELSE NULL
        END,
        bcp.retrieval_ref_nbr,
        pt.payment_type_key,
        ut.operating_day_key,
        ut.settlement_day_key,
        ut.posting_day_key
    HAVING
        (sum(((tp.bankcard_value + CASE WHEN (((tp.bankcard_payment_id IS NOT NULL) OR (mt.payg_flag = 1))) THEN (tp.uncollectible_amount) ELSE 0 END) + CASE WHEN (((s.purse_load_id IS NOT NULL) AND (mt.payg_flag = 1))) THEN (tp.stored_value) ELSE 0 END)) != 0)
    UNION ALL (
    SELECT
    m.txn_channel_display,
    CAST('B' AS VARCHAR) AS computation_type,
    strptime(CAST(s.operating_day_key AS VARCHAR), '%Y%m%d') AS operating_date,
    s.transaction_dtm,
    strptime(CAST(s.posting_day_key AS VARCHAR), '%Y%m%d') AS posting_date,
    strptime(CAST(s.settlement_day_key AS VARCHAR), '%Y%m%d') AS settlement_date,
    th.transit_mode_name,
    s.device_id,
    s.dw_transaction_id,
    s.transit_account_id,
    NULL AS patron_trip_id,
    s.tap_id,
    NULL AS fare_rule_description,
    NULL AS transfer_sequence_nbr,
    pt.payment_type_name,
    cd.bin,
    sp.payment_value AS fare_revenue,
    NULL AS extension_charge_reason,
    sp.retrieval_ref_nbr,
    pt.payment_type_key,
    s.operating_day_key,
    s.settlement_day_key,
    s.posting_day_key
    FROM
    cubic_ods.edw_sale_transaction AS s
    INNER JOIN cubic_ods.edw_sale_txn_payment AS sp ON
    (((sp.dw_transaction_id = s.dw_transaction_id)
        AND (sp.transaction_dtm = s.transaction_dtm)))
    INNER JOIN cubic_ods.edw_media_type_dimension AS mt ON
    ((mt.media_type_key = s.media_type_key))
    INNER JOIN cubic_ods.edw_txn_channel_map AS m ON
    (((m.txn_source = 'UseTxn')
        AND (m.sales_channel_key = 14)
            AND (m.payment_type_key = CASE
                WHEN ((s.reason_key IN (990951, 990961))) THEN (2)
                ELSE 4
            END)))
    INNER JOIN cubic_ods.edw_payment_type_dimension AS pt ON
    ((pt.payment_type_key = m.payment_type_key))
    LEFT JOIN cubic_ods.edw_read_transaction AS rt ON
    ((rt.tap_id = s.tap_id))
    LEFT JOIN cubic_ods.edw_transaction_history AS th ON
    ((th.dw_transaction_id = rt.dw_transaction_id))
    LEFT JOIN cubic_ods.edw_card_dimension AS cd ON
    ((cd.card_key = rt.card_key))
    WHERE
    ((s.operating_day_key IS NOT NULL)
        AND (s.sale_type_key = 22)
            AND (s.reason_key IN (990951, 990961, 990952, 990962))
                AND (mt.payg_flag = 1)))),
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
    EDW_farerev_payg_trip_txn_c AS(
    SELECT
        rs.due_day_key AS report_due_day_key,
        CAST('N' AS VARCHAR) AS addendum,
        t.*
    FROM
        cubic_ods.edw_fare_revenue_report_schedule AS rs
    INNER JOIN EDW_FAREREV_PAYG_TRIP_TXN AS t ON
        (((t.operating_day_key = rs.comp_operating_day_key)
            AND (t.settlement_day_key <= rs.comp_max_settlement_day_key)))),
    farerev_payg_trip_txn_c AS(
    SELECT
        report_due_day_key,
        addendum,
        txn_channel_display,
        computation_type,
        operating_date,
        transaction_dtm,
        posting_date,
        settlement_date,
        transit_mode_name,
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
        EDW_farerev_payg_trip_txn_c)
    SELECT *
    FROM farerev_payg_trip_txn_c;
"""

COMP_B_TXN_A = """
    CREATE VIEW cubic_reports.farerev_prod_sales_txn_a
    AS
    with EDW_FAREREV_PROD_SALES_TXN AS (
    SELECT
        m.txn_channel_display,
        m.sales_channel_display,
        CAST('A' AS VARCHAR) AS computation_type,
        od.dtm AS operating_date,
        o.order_dtm AS transaction_dtm,
        CAST(li.source_inserted_dtm AS DATE) AS posting_date,
        sd.dtm AS settlement_date,
        dd.facility_name,
        dd.device_id,
        p.dw_transaction_id AS transaction_id,
        CAST(NULL AS VARCHAR) AS group_account_id,
        li.subsystem_account_ref AS transit_account_id,
        o.order_nbr,
        1 AS quantity,
        li.line_item_sequence AS order_line_item_sequence,
        pt.payment_type_name,
        CASE
            WHEN ((p.payment_type_key = 1)) THEN (p.payment_amount)
            ELSE 0
        END AS cash_received,
        CASE
            WHEN (((p.payment_type_key = 1)
            AND (li.line_item_type = 'Cash Overpayment'))) THEN (-((SELECT COALESCE(payment_amount, 0) FROM cubic_ods.edw_patron_order_payment WHERE ((dw_patron_order_line_item_id = p.dw_patron_order_line_item_id) AND (retrieval_ref_nbr = 'cash returned')))))
            ELSE 0
        END AS cash_returned,
        CASE
            WHEN ((p.payment_type_key = 1)) THEN (CASE
                WHEN ((li.line_item_type = 'Cash Overpayment')) THEN ((p.payment_amount + (
                SELECT
                    COALESCE(payment_amount, 0)
                FROM
                    cubic_ods.edw_patron_order_payment
                WHERE
                    ((dw_patron_order_line_item_id = p.dw_patron_order_line_item_id)
                        AND (retrieval_ref_nbr = 'cash returned')))))
                ELSE p.payment_amount
            END)
            ELSE 0
        END AS net_cash,
        COALESCE(li.item_total_discount_amount, 0) AS discount_amount,
        ((((COALESCE(p.transit_value, 0) + COALESCE(p.benefit_value, 0)) + COALESCE(p.refundable_purse_value, 0)) + COALESCE(p.pass_cost, 0)) + COALESCE(p.enablement_fee, 0)) AS fare_revenue,
        CASE
            WHEN (((fp.fare_prod_name IS NULL)
                AND ((p.transit_value != 0)
                    OR (p.benefit_value != 0)
                        OR (p.refundable_purse_value != 0)))) THEN ('Stored Value')
            ELSE fp.fare_prod_name
        END AS fare_product,
        COALESCE(p.transit_value, 0) AS transit_value,
        COALESCE(p.benefit_value, 0) AS benefit_value,
        COALESCE(p.refundable_purse_value, 0) AS refundable_purse_value,
        ((COALESCE(p.transit_value, 0) + COALESCE(p.benefit_value, 0)) + COALESCE(p.refundable_purse_value, 0)) AS total_stored_value,
        COALESCE(p.pass_cost, 0) AS pass_cost,
        COALESCE(p.enablement_fee, 0) AS enablement_fee,
        COALESCE(p.replacement_fee, 0) AS replacement_fee,
        COALESCE(p.product_value, 0) AS product_value,
        (COALESCE(p.payment_amount, 0) - (((((((((COALESCE(p.pass_cost, 0) + COALESCE(p.transit_value, 0)) + COALESCE(p.benefit_value, 0)) + COALESCE(p.refundable_purse_value, 0)) + COALESCE(p.enablement_fee, 0)) + COALESCE(p.replacement_fee, 0)) + COALESCE(p.shipping_fee, 0)) + COALESCE(p.card_fee, 0)) + COALESCE(p.deposit_value, 0)) + COALESCE(p.administrative_fee, 0))) AS overpayment,
        0 AS retail_commission_amount,
        p.retrieval_ref_nbr,
        li.line_item_status,
        li.line_item_type,
        rd.reason_name,
        pt.payment_type_key,
        li.operating_day_key,
        p.settlement_day_key
    FROM
        cubic_ods.edw_patron_order_payment AS p
    INNER JOIN cubic_ods.edw_patron_order_line_item AS li ON
        ((li.dw_patron_order_line_item_id = p.dw_patron_order_line_item_id))
    INNER JOIN cubic_ods.edw_patron_order AS o ON
        (((o.dw_patron_order_id = li.dw_patron_order_id)
            AND (lower(o.order_type) !~~ '%refund%')))
    INNER JOIN cubic_ods.edw_txn_channel_map AS m ON
        (((m.txn_group = 'Product Sales')
            AND (m.txn_source IN ('SalesOrder', 'RefundOrder'))
                AND (m.sales_channel_key = o.sales_channel_key)
                    AND (m.payment_type_key = p.payment_type_key)))
    INNER JOIN cubic_ods.edw_date_dimension AS od ON
        ((od.date_key = li.operating_day_key))
    INNER JOIN cubic_ods.edw_date_dimension AS sd ON
        ((sd.date_key = p.settlement_day_key))
    LEFT JOIN cubic_ods.edw_device_dimension AS dd ON
        ((dd.device_key = o.device_key))
    INNER JOIN cubic_ods.edw_payment_type_dimension AS pt ON
        ((pt.payment_type_key = p.payment_type_key))
    LEFT JOIN cubic_ods.edw_fare_product_dimension AS fp ON
        ((fp.fare_prod_key = li.fare_prod_key))
    LEFT JOIN cubic_ods.edw_reason_dimension AS rd ON
        ((rd.reason_key = li.reason_key))
    WHERE
        ((COALESCE(p.retrieval_ref_nbr, '.') != 'cash returned')
            AND ((p.transaction_category != -9)
                OR (o.order_type IN ('Sale', 'Refund'))))
    UNION ALL (
    SELECT
    m.txn_channel_display,
    m.sales_channel_display,
    CAST('A' AS VARCHAR) AS computation_type,
    od.dtm AS operating_date,
    st.transaction_dtm,
    pd.dtm AS posting_date,
    sd.dtm AS settlement_date,
    dd.facility_name,
    dd.device_id,
    st.dw_transaction_id AS transaction_id,
    cd.fin_customer_id AS group_account_id,
    CAST(st.transit_account_id AS VARCHAR) AS transit_account_id,
    po.order_nbr,
    1 AS quantity,
    li.line_item_sequence AS order_line_item_sequence,
    pt.payment_type_name,
    CASE
        WHEN ((sp.payment_type_key = 1)) THEN (sp.payment_value)
        ELSE 0
    END AS cash_received,
    0 AS cash_returned,
    CASE
        WHEN ((sp.payment_type_key = 1)) THEN (sp.payment_value)
        ELSE 0
    END AS net_cash,
    COALESCE(st.discount_amount, 0) AS discount_amount,
    ((((COALESCE(sp.value_changed, 0) + COALESCE(sp.benefit_value, 0)) + COALESCE(sp.refundable_purse_value, 0)) + COALESCE(sp.pass_cost, 0)) + COALESCE(sp.enablement_fee, 0)) AS fare_revenue,
    CASE
        WHEN (((fp.fare_prod_name IS NULL)
            AND ((sp.value_changed != 0)
                OR (sp.benefit_value != 0)
                    OR (sp.refundable_purse_value != 0)))) THEN ('Stored Value')
        ELSE fp.fare_prod_name
    END AS fare_product,
    COALESCE(sp.value_changed, 0) AS transit_value,
    COALESCE(sp.benefit_value, 0) AS benefit_value,
    COALESCE(sp.refundable_purse_value, 0) AS refundable_purse_value,
    ((COALESCE(sp.value_changed, 0) + COALESCE(sp.benefit_value, 0)) + COALESCE(sp.refundable_purse_value, 0)) AS total_stored_value,
    COALESCE(sp.pass_cost, 0) AS pass_cost,
    COALESCE(sp.enablement_fee, 0) AS enablement_fee,
    COALESCE(sp.replacement_fee, 0) AS replacement_fee,
    COALESCE(sp.net_value, 0) AS product_value,
    (COALESCE(sp.payment_value, 0) - (((((((((COALESCE(sp.pass_cost, 0) + COALESCE(sp.value_changed, 0)) + COALESCE(sp.benefit_value, 0)) + COALESCE(sp.refundable_purse_value, 0)) + COALESCE(sp.enablement_fee, 0)) + COALESCE(sp.replacement_fee, 0)) + COALESCE(sp.shipping_fee, 0)) + COALESCE(sp.card_fee, 0)) + COALESCE(sp.deposit_value, 0)) + COALESCE(sp.administrative_fee, 0))) AS overpayment,
    (COALESCE(st.product_commission_amount, 0) + COALESCE(st.fee_commission_amount, 0)) AS retail_commission_amount,
    sp.retrieval_ref_nbr,
    li.line_item_status,
    li.line_item_type,
    rd.reason_name,
    pt.payment_type_key,
    st.operating_day_key,
    st.settlement_day_key
    FROM
    cubic_ods.edw_sale_transaction AS st
    INNER JOIN cubic_ods.edw_sale_txn_payment AS sp ON
    (((sp.dw_transaction_id = st.dw_transaction_id)
        AND (sp.transaction_dtm = st.transaction_dtm)))
    LEFT JOIN cubic_ods.edw_patron_order_line_item AS li ON
    ((li.dw_patron_order_line_item_id = st.dw_patron_order_line_item_id))
    LEFT JOIN cubic_ods.edw_patron_order AS po ON
    ((po.dw_patron_order_id = li.dw_patron_order_id))
    INNER JOIN cubic_ods.edw_txn_channel_map AS m ON
    (((m.txn_group = 'Product Sales')
        AND (m.sales_channel_key = st.sales_channel_key)
            AND (m.payment_type_key = sp.payment_type_key)
                AND ((m.txn_source IN ('SalesTxn', 'RefundTxn'))
                    OR ((m.txn_source = 'SalesOrder')
                        AND (li.line_item_type = 'Refund Cash Overpayment')))))
    INNER JOIN cubic_ods.edw_date_dimension AS od ON
    ((od.date_key = st.operating_day_key))
    INNER JOIN cubic_ods.edw_date_dimension AS sd ON
    ((sd.date_key = st.settlement_day_key))
    INNER JOIN cubic_ods.edw_date_dimension AS pd ON
    ((pd.date_key = st.posting_day_key))
    LEFT JOIN cubic_ods.edw_device_dimension AS dd ON
    ((dd.device_key = st.device_key))
    INNER JOIN cubic_ods.edw_payment_type_dimension AS pt ON
    ((pt.payment_type_key = sp.payment_type_key))
    LEFT JOIN cubic_ods.edw_fare_product_dimension AS fp ON
    ((fp.fare_prod_key = st.fare_prod_key))
    LEFT JOIN cubic_ods.edw_reason_dimension AS rd ON
    ((rd.reason_key = st.reason_key))
    LEFT JOIN cubic_ods.edw_customer_dimension AS cd ON
    ((cd.customer_key = st.customer_key))
    WHERE
    (st.sale_type_key IN (21, 30, 31, 23)))),
    EDW_FARE_REVENUE_REPORT_SCHEDULE_A AS(
    SELECT
        DISTINCT s.due_day_key,
        s.adden_max_operating_day_key,
        s.adden_min_settlement_day_key,
        s.adden_max_settlement_day_key,
        s.deposit_due_day_key
    FROM
        cubic_ods.edw_fare_revenue_report_schedule AS s),
    EDW_farerev_prod_sales_txn_a AS (
    SELECT
        rs.due_day_key AS report_due_day_key,
        CAST('Y' AS VARCHAR) AS addendum,
        t.*
    FROM
        EDW_FARE_REVENUE_REPORT_SCHEDULE_A AS rs
    INNER JOIN EDW_FAREREV_PROD_SALES_TXN AS t ON
        (((t.operating_day_key <= rs.adden_max_operating_day_key)
            AND (t.settlement_day_key BETWEEN rs.adden_min_settlement_day_key AND rs.adden_max_settlement_day_key)))),
    farerev_prod_sales_txn_a AS (
    SELECT DISTINCT 
        report_due_day_key,
        addendum,
        txn_channel_display,
        sales_channel_display,
        computation_type,
        operating_date,
        transaction_dtm,
        posting_date,
        settlement_date,
        facility_name,
        device_id,
        transaction_id,
        group_account_id,
        transit_account_id,
        order_nbr,
        quantity,
        order_line_item_sequence,
        payment_type_name,
        round((cash_received / 100), 2) AS cash_received,
        round((cash_returned / 100), 2) AS cash_returned,
        round((net_cash / 100), 2) AS net_cash,
        round((discount_amount / 100), 2) AS discount_amount,
        round((fare_revenue / 100), 2) AS fare_revenue,
        fare_product,
        round((transit_value / 100), 2) AS transit_value,
        round((benefit_value / 100), 2) AS benefit_value,
        round((refundable_purse_value / 100), 2) AS refundable_purse_value,
        round((total_stored_value / 100), 2) AS total_stored_value,
        round((pass_cost / 100), 2) AS pass_cost,
        round((enablement_fee / 100), 2) AS enablement_fee,
        round((replacement_fee / 100), 2) AS replacement_fee,
        round((product_value / 100), 2) AS product_value,
        round((overpayment / 100), 2) AS overpayment,
        round((CAST(retail_commission_amount AS NUMERIC) / 100), 2) AS retail_commission_amount,
        retrieval_ref_nbr,
        line_item_status,
        line_item_type,
        reason_name,
        operating_day_key,
        settlement_day_key
    FROM
        EDW_farerev_prod_sales_txn_a)
    SELECT * FROM farerev_prod_sales_txn_a;
"""

COMP_B_TXN_C = """
    CREATE VIEW cubic_reports.farerev_prod_sales_txn_c
    AS
    with EDW_FAREREV_PROD_SALES_TXN AS (
    SELECT
        m.txn_channel_display,
        m.sales_channel_display,
        CAST('A' AS VARCHAR) AS computation_type,
        od.dtm AS operating_date,
        o.order_dtm AS transaction_dtm,
        CAST(li.source_inserted_dtm AS DATE) AS posting_date,
        sd.dtm AS settlement_date,
        dd.facility_name,
        dd.device_id,
        p.dw_transaction_id AS transaction_id,
        CAST(NULL AS VARCHAR) AS group_account_id,
        li.subsystem_account_ref AS transit_account_id,
        o.order_nbr,
        1 AS quantity,
        li.line_item_sequence AS order_line_item_sequence,
        pt.payment_type_name,
        CASE
            WHEN ((p.payment_type_key = 1)) THEN (p.payment_amount)
            ELSE 0
        END AS cash_received,
        CASE
            WHEN (((p.payment_type_key = 1)
            AND (li.line_item_type = 'Cash Overpayment'))) THEN (-((SELECT COALESCE(payment_amount, 0) FROM cubic_ods.edw_patron_order_payment WHERE ((dw_patron_order_line_item_id = p.dw_patron_order_line_item_id) AND (retrieval_ref_nbr = 'cash returned')))))
            ELSE 0
        END AS cash_returned,
        CASE
            WHEN ((p.payment_type_key = 1)) THEN (CASE
                WHEN ((li.line_item_type = 'Cash Overpayment')) THEN ((p.payment_amount + (
                SELECT
                    COALESCE(payment_amount, 0)
                FROM
                    cubic_ods.edw_patron_order_payment
                WHERE
                    ((dw_patron_order_line_item_id = p.dw_patron_order_line_item_id)
                        AND (retrieval_ref_nbr = 'cash returned')))))
                ELSE p.payment_amount
            END)
            ELSE 0
        END AS net_cash,
        COALESCE(li.item_total_discount_amount, 0) AS discount_amount,
        ((((COALESCE(p.transit_value, 0) + COALESCE(p.benefit_value, 0)) + COALESCE(p.refundable_purse_value, 0)) + COALESCE(p.pass_cost, 0)) + COALESCE(p.enablement_fee, 0)) AS fare_revenue,
        CASE
            WHEN (((fp.fare_prod_name IS NULL)
                AND ((p.transit_value != 0)
                    OR (p.benefit_value != 0)
                        OR (p.refundable_purse_value != 0)))) THEN ('Stored Value')
            ELSE fp.fare_prod_name
        END AS fare_product,
        COALESCE(p.transit_value, 0) AS transit_value,
        COALESCE(p.benefit_value, 0) AS benefit_value,
        COALESCE(p.refundable_purse_value, 0) AS refundable_purse_value,
        ((COALESCE(p.transit_value, 0) + COALESCE(p.benefit_value, 0)) + COALESCE(p.refundable_purse_value, 0)) AS total_stored_value,
        COALESCE(p.pass_cost, 0) AS pass_cost,
        COALESCE(p.enablement_fee, 0) AS enablement_fee,
        COALESCE(p.replacement_fee, 0) AS replacement_fee,
        COALESCE(p.product_value, 0) AS product_value,
        (COALESCE(p.payment_amount, 0) - (((((((((COALESCE(p.pass_cost, 0) + COALESCE(p.transit_value, 0)) + COALESCE(p.benefit_value, 0)) + COALESCE(p.refundable_purse_value, 0)) + COALESCE(p.enablement_fee, 0)) + COALESCE(p.replacement_fee, 0)) + COALESCE(p.shipping_fee, 0)) + COALESCE(p.card_fee, 0)) + COALESCE(p.deposit_value, 0)) + COALESCE(p.administrative_fee, 0))) AS overpayment,
        0 AS retail_commission_amount,
        p.retrieval_ref_nbr,
        li.line_item_status,
        li.line_item_type,
        rd.reason_name,
        pt.payment_type_key,
        li.operating_day_key,
        p.settlement_day_key
    FROM
        cubic_ods.edw_patron_order_payment AS p
    INNER JOIN cubic_ods.edw_patron_order_line_item AS li ON
        ((li.dw_patron_order_line_item_id = p.dw_patron_order_line_item_id))
    INNER JOIN cubic_ods.edw_patron_order AS o ON
        (((o.dw_patron_order_id = li.dw_patron_order_id)
            AND (lower(o.order_type) !~~ '%refund%')))
    INNER JOIN cubic_ods.edw_txn_channel_map AS m ON
        (((m.txn_group = 'Product Sales')
            AND (m.txn_source IN ('SalesOrder', 'RefundOrder'))
                AND (m.sales_channel_key = o.sales_channel_key)
                    AND (m.payment_type_key = p.payment_type_key)))
    INNER JOIN cubic_ods.edw_date_dimension AS od ON
        ((od.date_key = li.operating_day_key))
    INNER JOIN cubic_ods.edw_date_dimension AS sd ON
        ((sd.date_key = p.settlement_day_key))
    LEFT JOIN cubic_ods.edw_device_dimension AS dd ON
        ((dd.device_key = o.device_key))
    INNER JOIN cubic_ods.edw_payment_type_dimension AS pt ON
        ((pt.payment_type_key = p.payment_type_key))
    LEFT JOIN cubic_ods.edw_fare_product_dimension AS fp ON
        ((fp.fare_prod_key = li.fare_prod_key))
    LEFT JOIN cubic_ods.edw_reason_dimension AS rd ON
        ((rd.reason_key = li.reason_key))
    WHERE
        ((COALESCE(p.retrieval_ref_nbr, '.') != 'cash returned')
            AND ((p.transaction_category != -9)
                OR (o.order_type IN ('Sale', 'Refund'))))
    UNION ALL (
    SELECT
    m.txn_channel_display,
    m.sales_channel_display,
    CAST('A' AS VARCHAR) AS computation_type,
    od.dtm AS operating_date,
    st.transaction_dtm,
    pd.dtm AS posting_date,
    sd.dtm AS settlement_date,
    dd.facility_name,
    dd.device_id,
    st.dw_transaction_id AS transaction_id,
    cd.fin_customer_id AS group_account_id,
    CAST(st.transit_account_id AS VARCHAR) AS transit_account_id,
    po.order_nbr,
    1 AS quantity,
    li.line_item_sequence AS order_line_item_sequence,
    pt.payment_type_name,
    CASE
        WHEN ((sp.payment_type_key = 1)) THEN (sp.payment_value)
        ELSE 0
    END AS cash_received,
    0 AS cash_returned,
    CASE
        WHEN ((sp.payment_type_key = 1)) THEN (sp.payment_value)
        ELSE 0
    END AS net_cash,
    COALESCE(st.discount_amount, 0) AS discount_amount,
    ((((COALESCE(sp.value_changed, 0) + COALESCE(sp.benefit_value, 0)) + COALESCE(sp.refundable_purse_value, 0)) + COALESCE(sp.pass_cost, 0)) + COALESCE(sp.enablement_fee, 0)) AS fare_revenue,
    CASE
        WHEN (((fp.fare_prod_name IS NULL)
            AND ((sp.value_changed != 0)
                OR (sp.benefit_value != 0)
                    OR (sp.refundable_purse_value != 0)))) THEN ('Stored Value')
        ELSE fp.fare_prod_name
    END AS fare_product,
    COALESCE(sp.value_changed, 0) AS transit_value,
    COALESCE(sp.benefit_value, 0) AS benefit_value,
    COALESCE(sp.refundable_purse_value, 0) AS refundable_purse_value,
    ((COALESCE(sp.value_changed, 0) + COALESCE(sp.benefit_value, 0)) + COALESCE(sp.refundable_purse_value, 0)) AS total_stored_value,
    COALESCE(sp.pass_cost, 0) AS pass_cost,
    COALESCE(sp.enablement_fee, 0) AS enablement_fee,
    COALESCE(sp.replacement_fee, 0) AS replacement_fee,
    COALESCE(sp.net_value, 0) AS product_value,
    (COALESCE(sp.payment_value, 0) - (((((((((COALESCE(sp.pass_cost, 0) + COALESCE(sp.value_changed, 0)) + COALESCE(sp.benefit_value, 0)) + COALESCE(sp.refundable_purse_value, 0)) + COALESCE(sp.enablement_fee, 0)) + COALESCE(sp.replacement_fee, 0)) + COALESCE(sp.shipping_fee, 0)) + COALESCE(sp.card_fee, 0)) + COALESCE(sp.deposit_value, 0)) + COALESCE(sp.administrative_fee, 0))) AS overpayment,
    (COALESCE(st.product_commission_amount, 0) + COALESCE(st.fee_commission_amount, 0)) AS retail_commission_amount,
    sp.retrieval_ref_nbr,
    li.line_item_status,
    li.line_item_type,
    rd.reason_name,
    pt.payment_type_key,
    st.operating_day_key,
    st.settlement_day_key
    FROM
    cubic_ods.edw_sale_transaction AS st
    INNER JOIN cubic_ods.edw_sale_txn_payment AS sp ON
    (((sp.dw_transaction_id = st.dw_transaction_id)
        AND (sp.transaction_dtm = st.transaction_dtm)))
    LEFT JOIN cubic_ods.edw_patron_order_line_item AS li ON
    ((li.dw_patron_order_line_item_id = st.dw_patron_order_line_item_id))
    LEFT JOIN cubic_ods.edw_patron_order AS po ON
    ((po.dw_patron_order_id = li.dw_patron_order_id))
    INNER JOIN cubic_ods.edw_txn_channel_map AS m ON
    (((m.txn_group = 'Product Sales')
        AND (m.sales_channel_key = st.sales_channel_key)
            AND (m.payment_type_key = sp.payment_type_key)
                AND ((m.txn_source IN ('SalesTxn', 'RefundTxn'))
                    OR ((m.txn_source = 'SalesOrder')
                        AND (li.line_item_type = 'Refund Cash Overpayment')))))
    INNER JOIN cubic_ods.edw_date_dimension AS od ON
    ((od.date_key = st.operating_day_key))
    INNER JOIN cubic_ods.edw_date_dimension AS sd ON
    ((sd.date_key = st.settlement_day_key))
    INNER JOIN cubic_ods.edw_date_dimension AS pd ON
    ((pd.date_key = st.posting_day_key))
    LEFT JOIN cubic_ods.edw_device_dimension AS dd ON
    ((dd.device_key = st.device_key))
    INNER JOIN cubic_ods.edw_payment_type_dimension AS pt ON
    ((pt.payment_type_key = sp.payment_type_key))
    LEFT JOIN cubic_ods.edw_fare_product_dimension AS fp ON
    ((fp.fare_prod_key = st.fare_prod_key))
    LEFT JOIN cubic_ods.edw_reason_dimension AS rd ON
    ((rd.reason_key = st.reason_key))
    LEFT JOIN cubic_ods.edw_customer_dimension AS cd ON
    ((cd.customer_key = st.customer_key))
    WHERE
    (st.sale_type_key IN (21, 30, 31, 23)))),
    EDW_farerev_prod_sales_txn_c AS
    (SELECT
        rs.due_day_key AS report_due_day_key,
        CAST('N' AS VARCHAR) AS addendum,
        t.*
    FROM
        cubic_ods.edw_fare_revenue_report_schedule AS rs
    INNER JOIN EDW_FAREREV_PROD_SALES_TXN AS t ON
        (((t.operating_day_key = rs.comp_operating_day_key)
            AND (t.settlement_day_key <= rs.comp_max_settlement_day_key)))),
    farerev_prod_sales_txn_c AS
    (SELECT DISTINCT
        report_due_day_key,
        addendum,
        txn_channel_display,
        sales_channel_display,
        computation_type,
        operating_date,
        transaction_dtm,
        posting_date,
        settlement_date,
        facility_name,
        device_id,
        transaction_id,
        group_account_id,
        transit_account_id,
        order_nbr,
        quantity,
        order_line_item_sequence,
        payment_type_name,
        round((cash_received / 100), 2) AS cash_received,
        round((cash_returned / 100), 2) AS cash_returned,
        round((net_cash / 100), 2) AS net_cash,
        round((discount_amount / 100), 2) AS discount_amount,
        round((fare_revenue / 100), 2) AS fare_revenue,
        fare_product,
        round((transit_value / 100), 2) AS transit_value,
        round((benefit_value / 100), 2) AS benefit_value,
        round((refundable_purse_value / 100), 2) AS refundable_purse_value,
        round((total_stored_value / 100), 2) AS total_stored_value,
        round((pass_cost / 100), 2) AS pass_cost,
        round((enablement_fee / 100), 2) AS enablement_fee,
        round((replacement_fee / 100), 2) AS replacement_fee,
        round((product_value / 100), 2) AS product_value,
        round((overpayment / 100), 2) AS overpayment,
        round((CAST(retail_commission_amount AS NUMERIC) / 100), 2) AS retail_commission_amount,
        retrieval_ref_nbr,
        line_item_status,
        line_item_type,
        reason_name,
        operating_day_key,
        settlement_day_key
    FROM
        EDW_farerev_prod_sales_txn_c)
    SELECT * FROM farerev_prod_sales_txn_c;
"""
