CREATE OR REPLACE VIEW cubic_reports.wc232_sale AS (
SELECT
us.SETTLEMENT_STATE,
us.DEVICE_ID,
efpd.FARE_PROD_NAME,
eod.OPERATOR_NAME,
eptd.PAYMENT_TYPE_NAME,
us.TRANSACTION_DTM,
COALESCE(us.ADMINISTRATIVE_FEE,0)/100 AS administrative_fee,
COALESCE(us.DEPOSIT_VALUE,0)/100 AS deposit_value,
COALESCE(us.REPLACEMENT_FEE,0)/100 AS replacement_fee,
COALESCE(us.OVERPAY,0)/100 AS overpay,
COALESCE(us.TRANSIT_VALUE,0)/100 AS transit_value,
COALESCE(us.UNDERPAY,0)/100 AS underpay,
COALESCE(us.BENEFIT_VALUE,0)/100 AS benefit_value,
COALESCE(us.BANKCARD_PAYMENT_VALUE,0)/100 AS bankcard_payment_value,
COALESCE(us.PASS_COST,0)/100 AS pass_cost,
us.TRANSACTION_ID,
COALESCE(us.SHIPPING_FEE,0)/100 AS shipping_fee,
estd.SALE_TYPE_NAME,
us.READY_FOR_SETTLEMENT_DTM,
us.POSTING_DAY_KEY,
us.ORDER_NBR,
us.TRANSIT_ACCOUNT_ID,
erd.REASON_CODE,
us.PASS_ID,
COALESCE(us.ONE_ACCOUNT_VALUE,0)/100 AS one_account_value,
COALESCE(us.UNCOLLECTIBLE_AMOUNT,0)/100 AS uncollectible_amount,
COALESCE(us.DISCOUNT_AMOUNT/100,0) AS discount_amount,
ptd.PURSE_NAME,
COALESCE(us.RESTRICTED_PURSE_VALUE,0)/100 AS restricted_purse_value,
us.RETRIEVAL_REF_NBR,
COALESCE(us.REFUNDABLE_PURSE_VALUE,0)/100 AS refundable_purse_value,
COALESCE(us.PREPAID_BANKCARD_VALUE/100,0) AS prepaid_bankcard_value,
us.PAYMENT_TRANSIT_ACCOUNT_ID
FROM
	cubic_ods.edw_unsettled_sale us
		LEFT JOIN cubic_ods.edw_fare_product_dimension efpd ON us.fare_prod_key = efpd.fare_prod_key 
		LEFT JOIN cubic_ods.edw_operator_dimension eod ON us.operator_id = eod.operator_id 
		LEFT JOIN cubic_ods.edw_payment_type_dimension eptd ON us.payment_type_key = eptd.payment_type_key 
		LEFT JOIN cubic_ods.edw_sale_type_dimension estd ON us.sale_type_key = estd.sale_type_key 
		LEFT JOIN cubic_ods.edw_reason_dimension erd ON us.reason_key = erd.reason_key 
		LEFT JOIN cubic_ods.edw_purse_type_dimension ptd ON us.purse_sku = ptd.purse_sku 
WHERE us.device_id IS NOT NULL
)
;
