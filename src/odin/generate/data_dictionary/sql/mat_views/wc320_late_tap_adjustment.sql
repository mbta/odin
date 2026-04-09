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
