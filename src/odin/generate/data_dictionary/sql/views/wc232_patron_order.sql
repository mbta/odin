CREATE OR REPLACE VIEW cubic_reports.wc232_patron_order AS (SELECT
  eupo.POSTING_DAY_KEY,
  eupo.READY_FOR_SETTLEMENT_DTM,
  eupo.ORDER_ORIGIN,
  eupo.TRANSACTION_DTM,
  eupo.ORDER_NBR,
  eupo.LINE_ITEM_NBR,
  eupo.ORDER_STATUS,
  efpd.FARE_PROD_NAME,
  pt.PAYMENT_TYPE_NAME,
  COALESCE(eupo.DEPOSIT_VALUE,0)/100 AS deposit_value,
  COALESCE(eupo.PASS_COST,0)/100 AS pass_cost,
  COALESCE(eupo.TRANSIT_VALUE,0)/100 AS transit_value,
  COALESCE(eupo.BENEFIT_VALUE,0)/100 AS benefit_value,
  COALESCE(eupo.ADMINISTRATIVE_FEE,0)/100 AS administrative_fee,
  COALESCE(eupo.REPLACEMENT_FEE,0)/100 AS replacement_fee,
  COALESCE(eupo.SHIPPING_FEE,0)/100 AS shipping_fee,
  COALESCE(eupo.REFUND_FEE,0)/100 AS refund_fee,
  eupo.TRANSIT_ACCOUNT_ID,
  eupo.SERIAL_NBR,
  eod.OPERATOR_NAME,
  erd.REASON_CODE,
  erd.REASON_NAME,
  eupo.PASS_ID,
  eupo.SETTLEMENT_STATE,
  COALESCE(eupo.ONE_ACCOUNT_VALUE,0)/100 AS one_account_value,
  COALESCE(eupo.DISCOUNT_AMOUNT/100,0) AS discount_amount,
  eptd.PURSE_NAME,
  COALESCE(eupo.RESTRICTED_PURSE_VALUE,0)/100 AS restricted_purse_value,
  eupo.RETRIEVAL_REF_NBR,
  COALESCE(eupo.REFUNDABLE_PURSE_VALUE,0)/100 AS refundable_purse_value,
  COALESCE(eupo.PREPAID_BANKCARD_VALUE/100,0) AS prepaid_bankcard_value,
  eupo.PAYMENT_TRANSIT_ACCOUNT_ID
FROM cubic_ods.edw_unsettled_patron_order eupo
	LEFT JOIN cubic_ods.edw_reason_dimension erd ON eupo.reason_key = erd.reason_key
	LEFT JOIN cubic_ods.edw_operator_dimension eod ON eupo.operator_key = eod.operator_key
	LEFT JOIN cubic_ods.edw_fare_product_dimension efpd ON eupo.fare_prod_key = efpd.fare_prod_key
	LEFT JOIN cubic_ods.edw_purse_type_dimension eptd ON eupo.purse_sku = eptd.purse_sku
	LEFT JOIN cubic_ods.edw_payment_type_dimension pt ON eupo.payment_type_key = pt.payment_type_id
)
