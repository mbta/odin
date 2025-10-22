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
    DROP VIEW IF EXISTS cubic_reports.wc_231_clearing_house;
    DROP VIEW IF EXISTS cubic_reports.wc_321_clearing_house;
    CREATE VIEW cubic_reports.wc_231_clearing_house
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
    DROP VIEW IF EXISTS cubic_reports.wc_231_pass_id_adhoc;
    CREATE VIEW cubic_reports.wc_231_pass_id_adhoc
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
