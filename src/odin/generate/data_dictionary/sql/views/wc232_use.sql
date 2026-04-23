CREATE OR REPLACE VIEW cubic_reports.wc232_use AS (SELECT DISTINCT
  euu.SETTLEMENT_STATE,
  COALESCE(euu.TRANSIT_VALUE,0)/100 AS transit_value,
  euu.TRANSIT_ACCOUNT_ID,
  euu.PASS_ID,
  eod.OPERATOR_NAME,
  efpd.FARE_PROD_NAME,
  euu.DEVICE_ID,
  euu.SERIAL_NBR,
  euu.PASS_USE_COUNT,
  euu.POSTING_DAY_KEY,
  COALESCE(euu.BENEFIT_VALUE,0)/100 AS benefit_value,
  COALESCE(euu.BANKCARD_PAYMENT_VALUE,0)/100 AS bankcard_payment_value,
  COALESCE(euu.PASS_COST,0)/100 AS pass_cost,
  euu.READY_FOR_SETTLEMENT_DTM,
  euu.TRANSACTION_DTM,
  COALESCE(euu.ONE_ACCOUNT_VALUE,0)/100 AS one_account_amount,
  COALESCE(euu.UNCOLLECTIBLE_AMOUNT,0)/100 AS uncollectible_amount,
  eptd.PURSE_NAME,
  COALESCE(euu.RESTRICTED_PURSE_VALUE,0)/100 AS restricted_purse_value,
  COALESCE(euu.POST_PAY_AMOUNT/100,0) AS post_pay_amount,
  euu.RETRIEVAL_REF_NBR,
  COALESCE(euu.REFUNDABLE_PURSE_VALUE,0)/100 AS refundable_purse_value,
  COALESCE(euu.PREPAID_BANKCARD_VALUE/100,0) AS prepaid_bankcard_value
FROM fares_data_repository.cubic_ods.edw_unsettled_use euu
	LEFT JOIN fares_data_repository.cubic_ods.edw_operator_dimension eod ON euu.operator_id = eod.operator_id
	LEFT JOIN fares_data_repository.cubic_ods.edw_fare_product_dimension efpd ON euu.fare_prod_key = efpd.fare_prod_key 
	LEFT JOIN fares_data_repository.cubic_ods.edw_purse_type_dimension eptd ON euu.purse_sku = eptd.purse_sku
	)
