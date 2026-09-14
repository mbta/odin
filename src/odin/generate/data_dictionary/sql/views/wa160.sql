DROP VIEW IF EXISTS cubic_reports.use_txns_wa160;
CREATE VIEW cubic_reports.use_txns_wa160
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
    (CAST(COALESCE(ut.pass_cost, 0) AS DOUBLE) / 100) AS pass_cost,
    (CAST(COALESCE(ut.value_changed, 0) AS DOUBLE) / 100) AS value_changed,
    (CAST(COALESCE(ut.booking_prepaid_value, 0) AS DOUBLE) / 100) AS booking_prepaid_value,
    (CAST(COALESCE(ut.benefit_value, 0) AS DOUBLE) / 100) AS benefit_value,
    (CAST(COALESCE(ut.bankcard_payment_value, 0) AS DOUBLE) / 100) AS bankcard_payment_value,
    (CAST(COALESCE(ut.merchant_service_fee, 0) AS DOUBLE) / 100) AS merchant_service_fee,
    (CAST((((((COALESCE(ut.pass_cost, 0) + COALESCE(ut.value_changed, 0)) + COALESCE(ut.booking_prepaid_value, 0)) + COALESCE(ut.benefit_value, 0)) + COALESCE(ut.bankcard_payment_value, 0)) + COALESCE(ut.merchant_service_fee, 0)) AS DOUBLE) / 100) AS total_use_cost,
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
    COALESCE(fpd.rider_class_name, tad.rider_class_name) AS rider_class_name,
    COALESCE(one_account_value, 0) AS one_account_value,
    COALESCE(ut.restricted_purse_value, 0) AS restricted_purse_value,
    COALESCE(ut.refundable_purse_value, 0) AS refundable_purse_value,
    (CAST(COALESCE(ut.uncollectible_amount, 0) AS DOUBLE) / 100) AS uncollectible_amount,
    tad.is_registered,
    (CAST(COALESCE(ut.discount_applied, 0) AS DOUBLE) / 100) AS discount_amount,
    (CAST(COALESCE(ut.post_pay_amount, 0) AS DOUBLE) / 100) AS post_pay_amount,
    CASE
        WHEN (((ut.TRANSFER_FLAG = 2)
        AND ((ut.MULTI_RIDE_ID IS NULL)
        OR (RIDE_COUNT <= 1)))) THEN ('TRANSFER')
        WHEN (((ut.TRANSFER_FLAG = 2)
        AND ((ut.MULTI_RIDE_ID IS NOT NULL)
        OR (RIDE_COUNT > 1)))) THEN ('MULTI-RIDE TRANSFER')
        WHEN (((ut.TRANSFER_FLAG != 2)
        AND ((ut.MULTI_RIDE_ID IS NOT NULL)
        OR (RIDE_COUNT > 1)))) THEN ('MULTI-RIDE')
        ELSE NULL
    END AS transfer_or_multiride
FROM
    cubic_delta.edw_use_transaction AS ut
LEFT JOIN cubic_ods.edw_fare_product_dimension AS fpd ON
    ((ut.fare_prod_key = fpd.fare_prod_key))
LEFT JOIN cubic_ods.edw_operator_dimension AS opd ON
    ((ut.operator_key = opd.operator_key))
LEFT JOIN cubic_ods.edw_card_dimension AS cardd ON
    ((ut.card_key = cardd.card_key))
LEFT JOIN cubic_ods.edw_ride_type_dimension AS rtd ON
    ((ut.ride_type_key = rtd.ride_type_key))
LEFT JOIN cubic_ods.edw_txn_status_dimension AS txnsd ON
    ((ut.txn_status_key = txnsd.txn_status_key))
LEFT JOIN cubic_ods.edw_media_type_dimension AS mtd ON
    ((ut.media_type_key = mtd.media_type_key))
LEFT JOIN cubic_ods.edw_transit_account_dimension AS tad ON
    ((cardd.transit_account_key = tad.transit_account_key))
LEFT JOIN cubic_ods.edw_fare_prod_users_list_dimension AS fpuld ON
    ((fpuld.fare_prod_users_list_key = fpd.fare_prod_users_list_key));
