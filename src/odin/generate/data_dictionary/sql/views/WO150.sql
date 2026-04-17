DROP VIEW IF EXISTS cubic_reports.wo150;
CREATE VIEW cubic_reports.wo150
AS
WITH CSR_PATRON_ORDER_DETAIL AS (SELECT o.order_dtm AS order_date,
       strptime(CAST(py.settlement_day_key AS VARCHAR),'%Y%m%d') AS sales_settlement_date,
       py.settlement_day_key,
       o.order_origin,
       o.device_key,
       o.merchant_id,
       o.retrieval_ref_nbr,
       o.order_nbr AS order_number,
       o.line_item_count,
       o.order_total_product_value,
       o.order_total_deposit_value,
       o.order_total_replacement_fee + o.order_total_administrative_fee + o.order_total_shipping_fee + o.order_total_card_fee + o.order_total_enablement_fee AS order_total_fees_value,
       o.order_total_value,
       py.shipping_fee,
       o.order_total_sales_tax,
       o.order_total_prepaid_amount,
       o.order_total_postpaid_amount,
       o.order_status,
       CASE ot.order_type_enum
         WHEN 'TravelCorrection' THEN li.travel_operator_id
         ELSE COALESCE(odli.operator_id, od.operator_id)
       END AS operator_id,
       CASE WHEN o.order_type = 'Refund' THEN rr.reason_name ELSE rd.reason_name END AS reason,
       o.created_by_user_id,
       o.order_completed_dtm,
       o.source,
       li.line_item_sequence,
       li.line_item_type,
       li.card_type,
       CASE
         WHEN li.card_key IS NULL AND li.subsystem_account_ref IS NOT NULL AND li.subsystem_enum IS NOT NULL THEN
           li.subsystem_account_ref||'('||SUBSTR(li.subsystem_enum,1,1)||')'
         WHEN tad.transit_account_id IS NOT NULL THEN
           CAST(tad.transit_account_id AS VARCHAR)||'(A)'
         WHEN cd.serial_nbr IS NOT NULL THEN
           cd.serial_nbr||'(C)'
         WHEN li.oa_account_id IS NOT NULL THEN
           CAST(li.oa_account_id AS VARCHAR)||'(O)'
         ELSE
           NULL
       END AS card_or_account_number,
       COALESCE(tad.transit_account_id,CAST(li.subsystem_account_ref AS BIGINT)) AS transit_account_id,
       tad.source AS transit_account_source,
       cd.serial_nbr,
       li.oa_account_id,
       CASE
         WHEN li.product_group = 'Not Applicable' THEN li.product_group
         ELSE COALESCE(fpd.fare_prod_name, li.line_item_name, li.product_group, 'Unknown')
       END AS product_description,
       li.product_group,
       ptd.payment_type_name AS payment_type,
       py.product_value,
       py.pass_cost,
       py.transit_value,
       py.benefit_value,
       py.deposit_value,
       py.replacement_fee,
       py.administrative_fee,
       py.shipping_fee AS line_item_shipping_fee,
       py.card_fee AS line_item_card_fee,
       py.sales_tax AS line_item_sales_tax,
       py.replacement_fee AS line_item_replacement_fee,
       py.enablement_fee AS line_item_enablement_fee,
       py.one_account_value AS line_item_one_account_value,
       py.prepaid_amount AS line_item_prepaid_amount,
       py.postpaid_amount AS line_item_postpaid_amount,
       py.rounding_amount AS line_item_rounding_amount,
       py.payment_amount AS line_item_total_value,
       py.discount_amount,
       py.promotion_id,
       py.promotion_sponsor_key AS promotion_sponsor_id,
       py.refund_fee,
       li.line_item_status,
       fpd.fare_prod_key,
       COALESCE(li.ticket_or_book_id, CAST(li.pass_id AS VARCHAR)) AS pass_id, --passid is BIGINT
       fpuld.fare_prod_users_list_name,
       'PII' AS patron_name,
       o.legacy_account_id AS legacy_account_number,
       o.legacy_card_nbr AS legacy_card_number,
       fpd.sku passsku,
       fpd.fare_prod_id AS fare_instrument_id,
       li.purse_type,
       li.grouping_id,
       li.line_item_nbr AS order_detail_id,
       o.order_nbr AS order_id,
       o.order_type,
       CASE WHEN o.order_type = 'Refund' THEN rr.reason_code ELSE rd.reason_code END AS reason_code,
       CASE WHEN o.order_type = 'Refund' THEN COALESCE(o.refund_reason_key, li.reason_key) ELSE o.reason_key END AS reason_key,
       o.order_day_key AS order_date_key,
       ftd.fee_type_key,
       (SELECT SUM(payment_amount) FROM fares_data_repository.cubic_ods.edw_patron_order_payment
         WHERE dw_patron_order_id=o.dw_patron_order_id ) AS cr_db_amount, --AND payment_type_key=c_payment_type_credit)
       o.collection_status,
       o.collection_date,
       o.submittal_count,
       CASE WHEN o.review_approved_rejected = 'Approved' THEN o.review_assigned_to_user_id ELSE NULL END AS approved_by_user_id,
       o.review_approved_dtm AS approved_dtm,
       COALESCE(o.notes, li.notes) AS adjustment_notes,
       bill.first_name AS bill_to_first_name,
       bill.last_name AS bill_to_last_name,
       py.card_fee_count AS line_item_card_fee_count,
       li.operating_day_key,
       o.sales_channel_key,
       o.approval_user_id,
       li.travel_transaction_id,
       li.travel_presence_id,
       li.line_item_name,
       cd.token_id,
       li.line_item_amount,
       li.autoload_enroll_action,
       COALESCE(li.pg_card_id, cd.pg_card_id) AS pg_card_id,
       COALESCE(fpd.rider_class_name, rc.rider_class_name, tad.rider_class_name) AS rider_class_name,
       cd.media_type_name,
       li.xfer_to_subsystem_account_ref,
       py.purse_sku,
       COALESCE(prstd.purse_name, li.subsystem_purse_restriction) AS purse_name,
       py.restricted_purse_value,
       py.refundable_purse_value,
       py.prepaid_bankcard_value,
       o.location_id,
       be.business_entity_name,
       be.address_1 AS business_entity_address_1,
       be.address_2 AS business_entity_address_2,
       be.city AS business_entity_city,
       be.state AS business_entity_state,
       be.country AS business_entity_country,
       be.postal_code AS business_entity_postal_code,
       py.ready_for_settlement_dtm,
       strptime(CAST(py.ready_for_settlement_day_key AS VARCHAR),'%Y%m%d') AS ready_for_settlement_date,
       os.ready_for_settlement_flag,
       os.no_financial_impact_flag,
       ot.include_on_reports_flag,
       ot.send_to_cch_flag,
       ot.cch_feed_sale_type_key,
       li.dw_patron_order_id,
       li.dw_patron_order_line_item_id,
       py.dw_patron_order_payment_id,
       py.dw_transaction_id,
       py.transaction_category,
       CASE WHEN py.subsystem_enum = 'ABP' THEN py.subsystem_account_ref END AS payment_transit_account_id,
       o.employee_key,
       o.inserted_user_id,
       o.inserted_first_name,
       o.inserted_last_name,
       o.updated_origin,
       o.updated_user_id,
       o.updated_first_name,
       o.updated_last_name,
       li.line_item_refund_fee,
       li.external_order_reference,
       li.origin_stop_point_id,
       li.origin_stop_point_desc,
       li.destination_stop_point_id,
       li.destination_stop_point_desc,
       li.media_sku,
       li.issue_media_bus_trip_id,
       li.issue_media_stop_point_id,
       li.issue_media_route_id,
       CASE WHEN li.xfer_to_subsystem_account_ref IS NOT NULL THEN li.subsystem_account_ref END AS xfer_from_subsystem_acct_ref,
       li.replaced_travel_token_id,
       CASE WHEN li.replaced_travel_token_id IS NOT NULL THEN li.travel_token_id END AS replacement_travel_token_id,
       o.source_inserted_dtm,
       o.source_updated_dtm,
       o.staging_inserted_dtm,
       o.staging_updated_dtm,
       o.edw_inserted_dtm,
       o.edw_updated_dtm
  FROM fares_data_repository.cubic_ods.edw_patron_order o
       LEFT JOIN fares_data_repository.cubic_ods.edw_patron_order_line_item li ON o.dw_patron_order_id = li.dw_patron_order_id
       LEFT JOIN fares_data_repository.cubic_ods.edw_patron_order_payment py ON li.dw_patron_order_line_item_id = py.dw_patron_order_line_item_id AND li.dw_patron_order_id = py.dw_patron_order_id
       LEFT JOIN fares_data_repository.cubic_ods.edw_operator_dimension od ON o.operator_key = od.operator_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_operator_dimension odli ON li.responsible_operator_key = odli.operator_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_fare_product_dimension fpd ON li.fare_prod_key = fpd.fare_prod_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_fare_prod_users_list_dimension fpuld ON fpd.fare_prod_users_list_key = fpuld.fare_prod_users_list_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_reason_dimension rd ON o.reason_key = rd.reason_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_reason_dimension rr ON COALESCE(o.refund_reason_key, li.reason_key) = rr.reason_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_payment_type_dimension ptd ON py.payment_type_key = ptd.payment_type_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_card_dimension cd ON li.card_key = cd.card_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_transit_account_dimension tad ON cd.transit_account_key = tad.transit_account_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_fee_type_dimension ftd ON li.fee_type_key = ftd.fee_type_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_patron_order_status_dimension os ON os.source = o.source AND os.order_status_name = o.order_status
       LEFT JOIN fares_data_repository.cubic_ods.edw_patron_order_type_dimension ot ON ot.source = o.source AND ot.order_type_name = o.order_type
       LEFT JOIN fares_data_repository.cubic_ods.edw_contact_dimension bill ON bill.contact_key = o.bill_to_contact_key
       LEFT JOIN fares_data_repository.cubic_ods.edw_rider_class_dimension rc ON rc.rider_class_id = li.account_rider_class_id
       LEFT JOIN fares_data_repository.cubic_ods.edw_purse_type_dimension prstd ON prstd.purse_sku = li.purse_sku
       LEFT JOIN fares_data_repository.cubic_ods.edw_business_entity_dimension be ON be.business_entity_key = CAST(o.location_id AS INT) AND TRANSLATE(o.location_id, 'x0123456789', 'x') IS NULL
 WHERE (ot.include_on_reports_flag = 1 OR ot.send_to_cch_flag = 1)
   AND (   (NOT (o.order_type = 'Refund' AND o.source = 'PIVOTAL'))
        OR (o.review_approved_rejected = 'Approved' AND COALESCE(o.refund_is_opt_out,0) = 0)
       )
),
CSR_USER AS (
SELECT NULL AS USER_ID,
          NULL AS LOGIN_NAME,
          NULL AS RN_DESCRIPTOR,
          NULL AS RN_CREATE_DATE,
          NULL AS RN_CREATE_USER,
          NULL AS RN_EDIT_DATE,
          NULL AS RN_EDIT_USER,
          NULL AS EMAIL_ADDRESS,
          NULL AS LAN_SEQUENCE,
          NULL AS MOBILE_SEQUENCE,
          NULL AS KEY_PREFIX,
          NULL AS DESKTOP_BINARY,
          NULL AS DATABASE_IDENT,
          NULL AS SOURCE_DSM_USERID,
          NULL AS REPLICA_NUMBER,
          NULL AS LAST_SYNC_DATE,
          NULL AS LAST_REPLICATE_DATE,
          NULL AS LAST_CONFIGURE_DATE,
          NULL AS SOURCE_PREFIX,
          NULL AS OUTLOOK_SYNC,
          NULL AS OUTLOOK_SYNC_USER_ID,
          NULL AS OUTLOOK_SYNC_RUNNING,
          NULL AS OUTLOOK_SYNC_STARTUP,
          NULL AS LANGUAGE_ID,
          NULL AS PREFERENCES,
          NULL AS MSMQ_ADDRESS,
          NULL AS TRANSPORT,
          NULL AS REMOTEMESSAGELEVEL,
          NULL AS SEARCH_TYPE_DEFAULT,
          NULL AS FAVORITES_TREE_BINARY,
          NULL AS FAVORITES_BAR_DOCKSTATE_BINARY,
          NULL AS WAM_PREFERENCES,
          NULL AS SYNCSTREAM_INBOX_TOKEN,
          NULL AS MAXSYNCMAILSIZE,
          NULL AS CAL_PREFERENCES,
          NULL AS CAL_SETTINGS,
          NULL AS SHORTCUTS_UPDATED,
          NULL AS COMPUTER,
          NULL AS LKEY,
          NULL AS MLKEY,
          NULL AS ULD,
          NULL AS USER_TYPE,
          NULL AS IS_LOCKED,
          NULL AS DW_USERS_ID,
          NULL AS DW_INSERTED_DTM,
          NULL AS DW_UPDATED_DTM
),
CSR_EMPLOYEE AS (
		SELECT
		NULL AS	EMPLOYEE_ID,
		CAST(NULL AS VARCHAR) AS	RN_DESCRIPTOR,
		NULL AS	RN_CREATE_DATE,
		NULL AS	RN_CREATE_USER,
		NULL AS	RN_EDIT_DATE,
		NULL AS	RN_EDIT_USER,
		NULL AS	FIRST_NAME,
		NULL AS	LAST_NAME,
		NULL AS	EXTENSION,
		NULL AS	DEPARTMENT,
		NULL AS	 RN_EMPLOYEE_USER_ID,
		NULL AS	ADDRESS_1,
		NULL AS	ADDRESS_2,
		NULL AS	ADDRESS_3,
		NULL AS	COUNTRY,
		NULL AS	ZIP,
		NULL AS	WORK_PHONE,
		NULL AS	WORK_FAX,
		NULL AS	WORK_EMAIL,
		NULL AS	START_DATE,
		NULL AS	END_DATE,
		NULL AS	BIRTHDAY,
		NULL AS	GENDER,
		NULL AS	LOGIN_SCRIPT,
		NULL AS	 TERRITORY_ID,
		NULL AS	STATE_,
		NULL AS	CITY,
		NULL AS	 REGIONAL_MANAGER_ID,
		NULL AS	TITLE,
		NULL AS	HOME_PHONE,
		NULL AS	HOME_FAX,
		NULL AS	PAGER,
		NULL AS	CELL_PHONE,
		NULL AS	HOME_EMAIL,
		NULL AS	 REGION_ID,
		NULL AS	ACTIVE,
		NULL AS	JOB_TITLE,
		NULL AS	EMPLOYEE_NAME_SOUNDEX,
		NULL AS	POSSIBLE_DUPLICATE,
		NULL AS	PIN,
		NULL AS	NUMBER_,
		NULL AS	SICK_DAYS,
		NULL AS	VACATION_DAYS,
		NULL AS	PHOTO,
		NULL AS	CALL_CAPTURE_SEARCH_IN,
		NULL AS	COMMENTS,
		NULL AS	START_DATE_TEXT,
		NULL AS	WEB_EDITED,
		NULL AS	EXTERNAL_USER_NAME,
		NULL AS	REPORTS_TO,
		NULL AS	REPORTS_TO_USER_ID,
		NULL AS	SUPPORT_MARKETING_PROJECT_ID,
		NULL AS	SUPPORT_TEAM_ID,
		NULL AS	SUPPORT_ALARM_FILE,
		NULL AS	REIMBURSEMENT_CURRENCY_ID,
		NULL AS	DELTA_REPORTS_TO_ID,
		NULL AS	SALES_NOTIFICATION,
		NULL AS	ORDER_NOTIFICATION,
		NULL AS	ACTIVITY_MEETING_NOTIFICATION,
		NULL AS	ACTIVITY_GENERAL_NOTIFICATION,
		NULL AS	TIME_ZONE_ID,
		NULL AS	COMPANY_NOTIFICATION,
		NULL AS	CONTACT_NOTIFICATION,
		NULL AS	NEW_PARTNER_NOTIFICATION,
		NULL AS	TIMER_ON,
		NULL AS	DUTY_MANAGER,
		NULL AS	ESCALATION_NOTIFICATION,
		NULL AS	UPDATE_NOTIFICATION,
		NULL AS	CLIENT,
		NULL AS	UNSUBSCRIBE_FLAG,
		NULL AS	BOUNCE_COUNT,
		NULL AS	EMAIL_TYPE,
		NULL AS	LOGIN_,
		NULL AS	DW_EMPLOYEE_ID,
		NULL AS	DW_INSERTED_DTM,
		NULL AS	DW_UPDATED_DTM
)
SELECT
  'WO150',
  CSR_PATRON_ORDER_DETAIL.ORDER_STATUS,
  COALESCE(CSR_PATRON_ORDER_DETAIL.REASON,'N/A') AS reason,
  COALESCE(case when upper(CSR_PATRON_ORDER_DETAIL.PURSE_TYPE) = 'PRETAX PURSE' then 'Benefit Value' else CSR_PATRON_ORDER_DETAIL.PURSE_TYPE end,'Undefined') AS purse_type,
  COALESCE((CSR_PATRON_ORDER_DETAIL.PRODUCT_VALUE) / 100,0) AS product_value,
  COALESCE(CSR_PATRON_ORDER_DETAIL.LINE_ITEM_COUNT,0) AS line_item_count,
  COALESCE(OPERATOR_DIMENSION.OPERATOR_NAME,'Undefined') AS operator_name,
  CSR_PATRON_ORDER_DETAIL.ORDER_DATE,
  CSR_PATRON_ORDER_DETAIL.CARD_OR_ACCOUNT_NUMBER::VARCHAR AS card_or_account_number,
  CSR_PATRON_ORDER_DETAIL.ORDER_NUMBER,
  CASE WHEN 
(CSR_PATRON_ORDER_DETAIL.INSERTED_FIRST_NAME || CSR_PATRON_ORDER_DETAIL.INSERTED_LAST_NAME) IS NULL then
(CSR_PATRON_ORDER_DETAIL.INSERTED_FIRST_NAME || ' ' || CSR_PATRON_ORDER_DETAIL.INSERTED_LAST_NAME) ELSE 
(COALESCE(CSR_PATRON_ORDER_DETAIL.INSERTED_USER_ID,CSR_EMPLOYEE.FIRST_NAME || ' ' || CSR_EMPLOYEE.LAST_NAME)) END AS inserted_by
,
  CSR_PATRON_ORDER_DETAIL.APPROVED_DTM 
,
  CSR_PATRON_ORDER_DETAIL.ORDER_COMPLETED_DTM,
  FARE_PRODUCT_DIMENSION.FARE_PROD_NAME,
  DT_REASON_DIMENSION.REASON_TYPE,
  COALESCE(CSR_PATRON_ORDER_DETAIL.PASS_COST,0)/100 AS pass_cost,
  COALESCE(CSR_PATRON_ORDER_DETAIL.TRANSIT_VALUE,0)/100 AS transit_value,
  COALESCE(CSR_PATRON_ORDER_DETAIL.BENEFIT_VALUE,0)/100 AS benefit_value,
  CSR_PATRON_ORDER_DETAIL.ADJUSTMENT_NOTES,
  CSR_PATRON_ORDER_DETAIL.ORDER_ORIGIN,
  COALESCE(CSR_PATRON_ORDER_DETAIL.APPROVAL_USER_ID,CSR_EMPLOYEE_APPROVED_USER.RN_DESCRIPTOR) AS approval_user_id,
  CSR_PATRON_ORDER_DETAIL.SALES_SETTLEMENT_DATE,
  CASE  
WHEN ((CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION IS NOT NULL)) 
    THEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE || ' (' || CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION || ')' 
ELSE CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE
END AS line_item_type,
  CSR_PATRON_ORDER_DETAIL.PG_CARD_ID,
  CSR_PATRON_ORDER_DETAIL.RIDER_CLASS_NAME,
  CSR_PATRON_ORDER_DETAIL.MEDIA_TYPE_NAME,
  CSR_PATRON_ORDER_DETAIL.PURSE_NAME,
  COALESCE(CSR_PATRON_ORDER_DETAIL.RESTRICTED_PURSE_VALUE/100,0) AS restricted_purse_value,
    CASE
    WHEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE   IN ('Transit Account Trip Fare Adjustment', 'Transit Account Trip Void', 'Transit Account Tap Void','Transit Account Trip Correction')
    THEN (CSR_PATRON_ORDER_DETAIL.LINE_ITEM_AMOUNT /100)
    ELSE 0
  END AS line_item_amount,
  CSR_PATRON_ORDER_DETAIL.PRODUCT_DESCRIPTION,
  COALESCE(CSR_PATRON_ORDER_DETAIL.REFUNDABLE_PURSE_VALUE,0)/100 AS refundable_purse_value,
  COALESCE(CSR_PATRON_ORDER_DETAIL.PREPAID_BANKCARD_VALUE/100,0) AS prepaid_bankcard_value,
  CSR_PATRON_ORDER_DETAIL.RETRIEVAL_REF_NBR
FROM
  CSR_PATRON_ORDER_DETAIL
  LEFT JOIN fares_data_repository.cubic_ods.edw_fare_product_dimension FARE_PRODUCT_DIMENSION ON CSR_PATRON_ORDER_DETAIL.FARE_PROD_KEY = FARE_PRODUCT_DIMENSION.fare_prod_key
  LEFT JOIN CSR_USER ON CSR_PATRON_ORDER_DETAIL.CREATED_BY_USER_ID = CSR_USER.USER_ID
  LEFT JOIN CSR_EMPLOYEE ON CSR_USER.USER_ID = CSR_EMPLOYEE.RN_EMPLOYEE_USER_ID
  JOIN fares_data_repository.cubic_ods.edw_operator_dimension OPERATOR_DIMENSION ON CSR_PATRON_ORDER_DETAIL.OPERATOR_ID=OPERATOR_DIMENSION.OPERATOR_ID
  JOIN fares_data_repository.cubic_ods.edw_date_dimension DATE_DIMENSION ON CSR_PATRON_ORDER_DETAIL.ORDER_DATE_KEY=DATE_DIMENSION.DATE_KEY
  LEFT JOIN CSR_EMPLOYEE CSR_EMPLOYEE_APPROVED_USER ON CSR_PATRON_ORDER_DETAIL.APPROVED_BY_USER_ID = CSR_EMPLOYEE_APPROVED_USER.EMPLOYEE_ID
  LEFT JOIN (Select * from fares_data_repository.cubic_ods.edw_reason_dimension) DT_REASON_DIMENSION ON DT_REASON_DIMENSION.REASON_KEY = CSR_PATRON_ORDER_DETAIL.REASON_KEY
WHERE
    (
     (
      CASE  
WHEN ((CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION IS NOT NULL)) 
    THEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE || ' (' || CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION || ')' 
ELSE CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE
END  LIKE  '%Adjustment%'
      OR
      CASE  
WHEN ((CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION IS NOT NULL)) 
    THEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE || ' (' || CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION || ')' 
ELSE CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE
END  IN  ( 'Pass Removal','Pass Conversion','Transit Account Trip Correction','Transit Account Trip Void','Transit Account Tap Void'  )
     )
     OR
     (
      CASE  
WHEN ((CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION IS NOT NULL)) 
    THEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE || ' (' || CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION || ')' 
ELSE CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE
END  LIKE  '%Refund%'
      AND
      CASE  
WHEN ((CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION IS NOT NULL)) 
    THEN CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE || ' (' || CSR_PATRON_ORDER_DETAIL.AUTOLOAD_ENROLL_ACTION || ')' 
ELSE CSR_PATRON_ORDER_DETAIL.LINE_ITEM_TYPE
END  <>  'Refund Issue Media'
     )
    );
