DROP VIEW IF EXISTS cubic_reports.farerev_prod_sales_txn_c;
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
