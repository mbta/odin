from typing import Dict, List
from odin.utils.instance import get_odin_instance
from odin.utils.logger import ProcessLog


_ODIN_INSTANCE = get_odin_instance()
assert _ODIN_INSTANCE in {"alpha", "beta", "gamma", "delta"}


HISTORY_JOB_IND = 0
LEGACY_FACT_JOB_IND = 1
DELTA_FACT_JOB_IND = 2

TABLE_MANIFEST: Dict[str, List[str | None]] = {
    "CCH_STAGE.CATEGORIZATION_RULE": ["alpha", "alpha", None],
    "CCH_STAGE.CATEGORY": ["alpha", "alpha", None],
    "CCH_STAGE.REPROCESS_ACTION": ["alpha", "alpha", None],
    "CCH_STAGE.TRANSACTION_TYPE": ["alpha", "alpha", None],
    "EC_STAGE.METRIC_HISTORY": ["alpha", "alpha", None],
    "EDW.ABP_REPROCESS_LOG": ["alpha", "alpha", None],
    "EDW.ABP_TAP": ["alpha", "alpha", None],
    "EDW.ABP_TRANSIT_ACCOUNT_X_TOKEN": ["alpha", "alpha", None],
    "EDW.ACCOUNT_BALANCE_BY_DAY": ["alpha", "alpha", None],
    "EDW.ACCOUNT_TRANSIT_LIABILITY": ["alpha", "alpha", None],
    "EDW.BE_INVOICE_STATUS_DIMENSION": ["alpha", "alpha", None],
    "EDW.BNFT_INVOICE_STATUS_DIMENSION": ["alpha", "alpha", None],
    "EDW.BUSINESS_ENTITY_DIMENSION": ["alpha", "alpha", None],
    "EDW.CARD_ACTION": ["alpha", "alpha", None],
    "EDW.CARD_ACTION_REASON_DIMENSION": ["alpha", "alpha", None],
    "EDW.CARD_ACTION_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.CARD_DIMENSION": ["alpha", "alpha", None],
    "EDW.CASHBOX_EVENT_DIMENSION": ["alpha", "alpha", None],
    "EDW.CCH_ACCOUNT": ["alpha", "alpha", None],
    "EDW.CCH_AFC_TRANSACTION": ["delta", None, "delta"],
    "EDW.CCH_APPORTION_RULE": ["alpha", "alpha", None],
    "EDW.CCH_APPORTION_RULE_MAP": ["alpha", "alpha", None],
    "EDW.CCH_CATEGORY": ["alpha", "alpha", None],
    "EDW.CCH_GL_SUMMARY_IMPORT": ["alpha", "alpha", None],
    "EDW.CCH_REPROCESS_ACTION": ["alpha", "alpha", None],
    "EDW.CCH_RULES_SET": ["alpha", "alpha", None],
    "EDW.CCH_RULE_MULTIPLIER_TYPE": ["alpha", "alpha", None],
    "EDW.CHGBK_ACTIVITY_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.CITATION": ["alpha", "alpha", None],
    "EDW.CONTACT_DIMENSION": ["alpha", "alpha", None],
    "EDW.CREDIT_CARD_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.CUSTOMER_DIMENSION": ["alpha", "alpha", None],
    "EDW.DATE_DIMENSION": ["alpha", "alpha", None],
    "EDW.DEVICE_CURRENT_SW_CONFIG": ["alpha", "alpha", None],
    "EDW.DEVICE_DIMENSION": ["alpha", "alpha", None],
    "EDW.DEVICE_END_OF_DAY_MSG_COUNT": ["alpha", "alpha", None],
    "EDW.DEVICE_EVENT": ["alpha", "alpha", None],
    "EDW.DEVICE_LAST_STATE": ["gamma", "gamma", None],
    "EDW.EMPLOYEE_DIMENSION": ["alpha", "alpha", None],
    "EDW.EVENT_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.FACILITY_DIMENSION": ["alpha", "alpha", None],
    "EDW.FAREREV_RECOVERY_TXN": ["alpha", "alpha", None],
    "EDW.FARE_PRODUCT_DIMENSION": ["alpha", "alpha", None],
    "EDW.FARE_PRODUCT_INSTANCE": ["alpha", "alpha", None],
    "EDW.FARE_PROD_USERS_LIST_DIMENSION": ["alpha", "alpha", None],
    "EDW.FARE_REVENUE_REPORT_SCHEDULE": ["alpha", "alpha", None],
    "EDW.FEE_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.FNP_GENERAL_JRNL_ACCOUNT_ENTRY": ["alpha", "alpha", None],
    "EDW.FRAUD_ALERT_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.FRAUD_SUMMARY_BY_ACCOUNT": ["alpha", "alpha", None],
    "EDW.FRAUD_SUMMARY_BY_DAY": ["alpha", "alpha", None],
    "EDW.FRM_BANK_FEE_SUMMARY": ["alpha", "alpha", None],
    "EDW.FRM_BANK_FEE_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.FRM_CRDB_ACQ_BANK_FEE": ["alpha", "alpha", None],
    "EDW.FRM_CRDB_ACQ_BANK_FEE_DETAIL": ["alpha", "alpha", None],
    "EDW.FRM_CRDB_CHGBK_ACTIVITY": ["alpha", "alpha", None],
    "EDW.FRM_CRDB_CHGBK_CASE": ["alpha", "alpha", None],
    "EDW.FRM_CRDB_CHGBK_MASTER": ["alpha", "alpha", None],
    "EDW.FRM_CRDB_RECON_SYSCONF_ACQCONF": ["alpha", "alpha", None],
    "EDW.FRM_MERCHANT_MAPPING": ["alpha", "alpha", None],
    "EDW.FRM_SRC_CRDB_ACQUIRER_CHGBK": ["alpha", "alpha", None],
    "EDW.FRM_SRC_CRDB_ACQUIRER_CONF": ["alpha", "alpha", None],
    "EDW.GL_SUM_REC_STAT_DIMENSION": ["alpha", "alpha", None],
    "EDW.IFDM_FRAUD_ALERT": ["alpha", "alpha", None],
    "EDW.IFDM_FRAUD_ALERT_ACTION": ["alpha", "alpha", None],
    "EDW.JOURNAL_ENTRY": ["alpha", "alpha", None],
    "EDW.KPI": ["alpha", "alpha", None],
    "EDW.KPI_AGENCY_MAP": ["alpha", "alpha", None],
    "EDW.KPI_AVAILABILITY_EVENT": ["alpha", "alpha", None],
    "EDW.KPI_DETAIL_EVENTS_BY_DAY": ["alpha", "alpha", None],
    "EDW.KPI_MONTHLY_SLDC": ["alpha", "alpha", None],
    "EDW.KPI_OPERATING_DAY_SCHEDULE": ["alpha", "alpha", None],
    "EDW.KPI_RULE": ["alpha", "alpha", None],
    "EDW.KPI_SUMMARY_BY_DAY": ["alpha", "alpha", None],
    "EDW.KPI_TARGET": ["alpha", "alpha", None],
    "EDW.MEDIA_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.MEMBER_DIMENSION": ["alpha", "alpha", None],
    "EDW.OPERATOR_DIMENSION": ["alpha", "alpha", None],
    "EDW.PAL_CONFIRMATION": ["alpha", "alpha", None],
    "EDW.PASS_LIAB_EVENT_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.PATRON_ORDER": ["alpha", "alpha", None],
    "EDW.PATRON_ORDER_LINE_ITEM": ["alpha", "alpha", None],
    "EDW.PATRON_ORDER_PAYMENT": ["alpha", "alpha", None],
    "EDW.PATRON_ORDER_STATUS_DIMENSION": ["alpha", "alpha", None],
    "EDW.PATRON_ORDER_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.PATRON_TRIP": ["alpha", "alpha", None],
    "EDW.PATRONAGE_SUMMARY": ["alpha", "beta", None],
    "EDW.PAYMENT_SUMMARY": ["alpha", "alpha", None],
    "EDW.PAYMENT_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.PROCESS_COST_SUMMARY": ["alpha", "alpha", None],
    "EDW.PROC_COST_CHGBK_DTL": ["alpha", "alpha", None],
    "EDW.PROC_COST_PG_LOSS_DTL": ["alpha", "alpha", None],
    "EDW.PURSE_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.READ_TRANSACTION": ["alpha", "alpha", None],
    "EDW.REASON_DIMENSION": ["alpha", "alpha", None],
    "EDW.REVENUE_LOSS_ASSESSMENT": ["alpha", "alpha", None],
    "EDW.RIDER_CLASS_DIMENSION": ["alpha", "alpha", None],
    "EDW.RIDE_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.ROUTE_DIMENSION": ["alpha", "alpha", None],
    "EDW.SALE_TRANSACTION": ["gamma", None, "gamma"],
    "EDW.SALES_CHANNEL_DIMENSION": ["alpha", "alpha", None],
    "EDW.SALES_SUMMARY_BY_DAY": ["alpha", "alpha", None],
    "EDW.SALE_TXN_PAYMENT": ["alpha", "alpha", None],
    "EDW.SALE_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.SERVICE_TYPE_DIMENSION": ["alpha", "alpha", None],
    "EDW.STOP_POINT_DIMENSION": ["alpha", "alpha", None],
    "EDW.SVN_INCIDENT": ["alpha", "alpha", None],
    "EDW.SVN_TASK": ["alpha", "alpha", None],
    "EDW.SVN_U_FS_EVENT_CODE": ["alpha", "alpha", None],
    "EDW.SVN_U_FS_FAULTY_ITEMS": ["alpha", "alpha", None],
    "EDW.SVN_U_FS_FAULT_CODES": ["alpha", "alpha", None],
    "EDW.SVN_U_FS_RPIR_CODE_RT_CAUSE_ID": ["alpha", "alpha", None],
    "EDW.SVN_U_KPI_LEVEL": ["alpha", "alpha", None],
    "EDW.SVN_WM_ORDER": ["alpha", "alpha", None],
    "EDW.SVN_WM_TASK": ["alpha", "alpha", None],
    "EDW.TAP_USAGE_SUMMARY": ["alpha", "alpha", None],
    "EDW.TIME_PERIOD_DIMENSION": ["alpha", "alpha", None],
    "EDW.TOKEN_HISTORY": ["alpha", "alpha", None],
    "EDW.TRANSACTION_ORIGIN_DIMENSION": ["alpha", "alpha", None],
    "EDW.TRANSIT_ACCOUNT_BALANCE": ["alpha", "alpha", None],
    "EDW.TRANSIT_ACCOUNT_DIMENSION": ["alpha", "alpha", None],
    "EDW.TRAVEL_MODE_DIMENSION": ["alpha", "alpha", None],
    "EDW.TRIP_PAYMENT": ["alpha", "alpha", None],
    "EDW.TXN_CHANNEL_MAP": ["alpha", "alpha", None],
    "EDW.TXN_STATUS_DIMENSION": ["alpha", "alpha", None],
    "EDW.UNSETTLED_CCA_CASH_COUNT": ["alpha", "alpha", None],
    "EDW.UNSETTLED_CRDB_ACQ_CONF": ["delta", None, "delta"],
    "EDW.UNSETTLED_CRDB_CHGBK": ["alpha", "alpha", None],
    "EDW.UNSETTLED_CRDB_SYS_CONF": ["alpha", "alpha", None],
    "EDW.UNSETTLED_DEVICE_CASH_STC": ["alpha", "alpha", None],
    "EDW.UNSETTLED_MISC": ["alpha", "alpha", None],
    "EDW.UNSETTLED_PATRON_ORDER": ["alpha", "alpha", None],
    "EDW.UNSETTLED_SALE": ["alpha", "alpha", None],
    "EDW.UNSETTLED_USE": ["alpha", "alpha", None],
    "EDW.USE_TRANSACTION": ["delta", None, "delta"],
    "EDW.VEHICLE_TRIP": ["alpha", "alpha", None],
    "EDW.BUS_DIMENSION": ["alpha", "alpha", None],
    "EDW.DAILY_CASH_BALANCE_SUMMARY": ["alpha", "alpha", None],
    # "EDW.DAILY_POS_CASH_BALANCE_SUMMARY": ["alpha", "alpha", None],
    # "EDW.FNP_PARSED_MANUAL_JOURNAL": ["alpha", "alpha", None],
    "EDW.COMPONENT_MAINT_COUNT": ["alpha", "alpha", None],
    "DW_MAIN.DW_SETTING": ["alpha", "alpha", None],
}


CUBIC_HISTORY_TABLES = [
    t for t, inst in TABLE_MANIFEST.items() if inst[HISTORY_JOB_IND] == _ODIN_INSTANCE
]

CUBIC_ODS_TABLES = [
    t for t, inst in TABLE_MANIFEST.items() if inst[LEGACY_FACT_JOB_IND] == _ODIN_INSTANCE
]

CUBIC_ODS_DELTA_TABLES = [
    t for t, inst in TABLE_MANIFEST.items() if inst[DELTA_FACT_JOB_IND] == _ODIN_INSTANCE
]

# Add tables here if Cubic exports them with escapeChar='"', which is done to support CLOB data.
# All other tables do not define escape characters.
CUBIC_QLIK_ESCAPED_QUOTE_TABLES = {
    "EDW.ABP_REPROCESS_LOG",
}


def log_cubic_table_manifest():
    """Output a log showing Cubic table lists for the current instance."""
    ProcessLog(process="table_manifest", auto_start=False).complete(
        history_tables=",".join(CUBIC_HISTORY_TABLES),
        ods_tables=",".join(CUBIC_ODS_TABLES),
        delta_tables=",".join(CUBIC_ODS_DELTA_TABLES),
    )
