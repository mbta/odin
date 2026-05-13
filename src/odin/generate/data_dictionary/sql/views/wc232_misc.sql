CREATE OR REPLACE VIEW cubic_reports.wc232_misc AS (
SELECT
  UNSETTLED_MISC.SETTLEMENT_STATE,
  strptime(UNSETTLED_MISC.POSTING_DAY_KEY::varchar,'%Y%m%d') AS posting_day_key,
  UNSETTLED_MISC.READY_FOR_SETTLEMENT_DTM,
  UNSETTLED_MISC.TXN_DESC,
  eod.OPERATOR_NAME,
  eptd.PAYMENT_TYPE_NAME,
  efpd.FARE_PROD_NAME,
  UNSETTLED_MISC.TRANSIT_ACCOUNT_ID,
  UNSETTLED_MISC.PASS_ID,
  COALESCE(UNSETTLED_MISC.PASS_COST,0)/100 AS pass_cust,
  UNSETTLED_MISC.PASS_USE_COUNT
FROM
 	cubic_ods.edw_unsettled_misc UNSETTLED_MISC
 		LEFT JOIN cubic_ods.edw_fare_product_dimension efpd ON UNSETTLED_MISC.fare_prod_key = efpd.fare_prod_key 
		LEFT JOIN cubic_ods.edw_operator_dimension eod ON UNSETTLED_MISC.operator_id = eod.operator_id 
		LEFT JOIN cubic_ods.edw_payment_type_dimension eptd ON UNSETTLED_MISC.payment_type_key = eptd.payment_type_key 
)
;
