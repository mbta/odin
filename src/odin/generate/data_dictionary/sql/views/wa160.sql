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
