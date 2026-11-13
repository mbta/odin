WITH agg AS (
    SELECT
        ut.operating_day_key,
        ut.settlement_day_key,
        ut.posting_day_key,
        ut.transaction_dtm,
        ut.device_id,
        ut.dw_transaction_id,
        ut.transit_account_id,
        ut.patron_trip_id,
        ut.transfer_sequence_nbr,
        ut.operator_key,            -- carried through -> th.operator_name
        ut.card_key,                -- carried through -> cd.bin
        bcp.retrieval_ref_nbr,
        tr.fare_rule_description,
        CASE
            WHEN (((ut.transfer_sequence_nbr > 0)
            AND (ut.fare_due != 0))) THEN ('Transfer')
            ELSE NULL
        END AS extension_charge_reason,
        sum(((tp.bankcard_value + CASE WHEN (((tp.bankcard_payment_id IS NOT NULL) OR (mt.payg_flag = 1))) THEN (tp.uncollectible_amount) ELSE 0 END) + CASE WHEN (((s.purse_load_id IS NOT NULL) AND (mt.payg_flag = 1))) THEN (tp.stored_value) ELSE 0 END)) AS fare_revenue_raw
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
    INNER JOIN cubic_ods.edw_media_type_dimension AS mt ON
        ((mt.media_type_key = ut.media_type_key))
    WHERE
        ((ut.bankcard_payment_value != 0)
            OR (ut.uncollectible_amount != 0)
                OR (ut.value_changed != 0))
    GROUP BY ALL
    HAVING
        (sum(((tp.bankcard_value + CASE WHEN (((tp.bankcard_payment_id IS NOT NULL) OR (mt.payg_flag = 1))) THEN (tp.uncollectible_amount) ELSE 0 END) + CASE WHEN (((s.purse_load_id IS NOT NULL) AND (mt.payg_flag = 1))) THEN (tp.stored_value) ELSE 0 END)) != 0)
)
SELECT
    m.txn_channel_display,
    CAST('B' AS VARCHAR) AS computation_type,
    strptime(CAST(agg.operating_day_key AS VARCHAR), '%Y%m%d') AS operating_date,
    agg.transaction_dtm,
    strptime(CAST(agg.posting_day_key AS VARCHAR), '%Y%m%d') AS posting_date,
    strptime(CAST(agg.settlement_day_key AS VARCHAR), '%Y%m%d') AS settlement_date,
    th.operator_name,
    agg.device_id,
    agg.dw_transaction_id,
    agg.transit_account_id,
    agg.patron_trip_id,
    NULL AS tap_id,
    agg.fare_rule_description,
    agg.transfer_sequence_nbr,
    pt.payment_type_name,
    cd.bin,
    -(agg.fare_revenue_raw) AS fare_revenue,
    agg.extension_charge_reason,
    agg.retrieval_ref_nbr,
    pt.payment_type_key,
    agg.operating_day_key,
    agg.settlement_day_key,
    agg.posting_day_key
FROM
    agg
LEFT JOIN cubic_ods.edw_card_dimension AS cd ON
    ((cd.card_key = agg.card_key))
LEFT JOIN cubic_ods.edw_operator_dimension AS th ON
    ((th.operator_key = agg.operator_key))
CROSS JOIN (
    SELECT
        payment_type_name,
        payment_type_key
    FROM
        cubic_ods.edw_payment_type_dimension
    WHERE
        (payment_type_key = 2)) AS pt
CROSS JOIN (
    SELECT
        txn_channel_display
    FROM
        cubic_ods.edw_txn_channel_map
    WHERE
        ((txn_source = 'UseTxn')
            AND (sales_channel_key = 8)
                AND (payment_type_key = 2))) AS m
UNION ALL (
SELECT
m.txn_channel_display,
CAST('B' AS VARCHAR) AS computation_type,
strptime(CAST(s.operating_day_key AS VARCHAR), '%Y%m%d') AS operating_date,
s.transaction_dtm,
strptime(CAST(s.posting_day_key AS VARCHAR), '%Y%m%d') AS posting_date,
strptime(CAST(s.settlement_day_key AS VARCHAR), '%Y%m%d') AS settlement_date,
th.operator_name,
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
LEFT JOIN cubic_ods.edw_operator_dimension AS th ON
((th.operator_key = rt.operator_key))
LEFT JOIN cubic_ods.edw_card_dimension AS cd ON
((cd.card_key = rt.card_key))
WHERE
((s.operating_day_key IS NOT NULL)
    AND (s.sale_type_key = 22)
        AND (s.reason_key IN (990951, 990961, 990952, 990962))
            AND (mt.payg_flag = 1)))
