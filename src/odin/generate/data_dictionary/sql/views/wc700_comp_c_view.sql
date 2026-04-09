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
