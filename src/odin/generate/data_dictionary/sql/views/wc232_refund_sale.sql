CREATE OR REPLACE VIEW cubic_reports.wc232_refund_sale AS 
(WITH UNSETTLED_REFUND_SALE AS (
SELECT s.settlement_state,
       s.posting_day_key,
       s.ready_for_settlement_dtm,
       op.operator_name,
       s.transaction_dtm,
       s.transaction_id,
       s.device_id,
       pt.payment_type_name,
       fp.fare_prod_name,
       s.deposit_value,
       s.pass_cost,
       s.transit_value,
       s.benefit_value,
       s.one_account_value,
       s.orig_sale_amt_paid_by_bv,
       s.orig_sale_amt_paid_by_non_bv,
       s.order_nbr,
       s.transit_account_id,
       s.serial_nbr,
       s.oa_account_id,
       r.reason_code,
       r.reason_name,
       s.pass_id,
       s.dw_transaction_id,
       s.operator_id,
       s.fare_prod_key,
       s.payment_type_key,
       s.reason_key,
       s.booking_prepaid_value,
       s.purse_sku,
       prstd.purse_name,
       s.restricted_purse_value,
       s.prepaid_bankcard_value,
       s.refund_fee,
       s.retrieval_ref_nbr,
       s.payment_transit_account_id
  FROM fares_data_repository.cubic_ods.edw_unsettled_sale s
       LEFT JOIN fares_data_repository.cubic_ods.edw_operator_dimension op ON op.operator_id = s.operator_id
       LEFT JOIN fares_data_repository.cubic_ods.edw_fare_product_dimension fp ON fp.fare_prod_key = s.fare_prod_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_payment_type_dimension pt ON pt.payment_type_key = s.payment_type_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_reason_dimension r ON r.reason_key = s.reason_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_purse_type_dimension prstd ON prstd.purse_sku = s.purse_sku
 WHERE s.sale_type_key = 24)
SELECT
  UNSETTLED_REFUND_SALE.SETTLEMENT_STATE,
  UNSETTLED_REFUND_SALE.POSTING_DAY_KEY,
  UNSETTLED_REFUND_SALE.READY_FOR_SETTLEMENT_DTM,
  UNSETTLED_REFUND_SALE.OPERATOR_NAME,
  UNSETTLED_REFUND_SALE.TRANSACTION_DTM,
  UNSETTLED_REFUND_SALE.TRANSACTION_ID,
  UNSETTLED_REFUND_SALE.DEVICE_ID,
  UNSETTLED_REFUND_SALE.PAYMENT_TYPE_NAME,
  UNSETTLED_REFUND_SALE.FARE_PROD_NAME,
  COALESCE(UNSETTLED_REFUND_SALE.DEPOSIT_VALUE,0)/100 AS deposit_value,
  COALESCE(UNSETTLED_REFUND_SALE.PASS_COST,0)/100 AS pass_cost,
  COALESCE(UNSETTLED_REFUND_SALE.TRANSIT_VALUE,0)/100 AS transit_value,
  COALESCE(UNSETTLED_REFUND_SALE.BENEFIT_VALUE,0)/100 AS benefit_value,
  COALESCE(UNSETTLED_REFUND_SALE.ORIG_SALE_AMT_PAID_BY_BV,0)/100 AS orig_sale_amt_paid_by_bv,
  COALESCE(UNSETTLED_REFUND_SALE.ORIG_SALE_AMT_PAID_BY_NON_BV,0)/100 AS orig_sale_amt_paid_by_non_bv,
  UNSETTLED_REFUND_SALE.ORDER_NBR,
  UNSETTLED_REFUND_SALE.TRANSIT_ACCOUNT_ID,
  UNSETTLED_REFUND_SALE.PURSE_NAME,
  COALESCE(UNSETTLED_REFUND_SALE.RESTRICTED_PURSE_VALUE,0)/100 AS restricted_purse_value,
  COALESCE(UNSETTLED_REFUND_SALE.REFUND_FEE/100,0) AS refund_fee,
  UNSETTLED_REFUND_SALE.RETRIEVAL_REF_NBR,
  COALESCE(UNSETTLED_REFUND_SALE.PREPAID_BANKCARD_VALUE/100,0) AS prepaid_bankcard_value,
  UNSETTLED_REFUND_SALE.PAYMENT_TRANSIT_ACCOUNT_ID
FROM
  UNSETTLED_REFUND_SALE)
