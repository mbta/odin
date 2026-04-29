import os

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import CUBIC_ODS_FACT_DATA
from odin.utils.logger import ProcessLog

TABLES_TO_DELETE: list[str] = [
    "EDW.PAYMENT_SUMMARY",
    "EDW.PATRON_ORDER_PAYMENT",
    "EDW.UNSETTLED_CCA_CASH_COUNT",
    "EDW.UNSETTLED_DIST_ORDER",
    "EDW.DATE_DIMENSION",
    "EDW.FARE_PROD_USERS_LIST_DIMENSION",
    "EDW.FARE_PRODUCT_DIMENSION",
    "EDW.MEDIA_TYPE_DIMENSION",
    "EDW.OPERATOR_DIMENSION",
    "EDW.RIDE_TYPE_DIMENSION",
    "EDW.ROUTE_DIMENSION",
    "EDW.STOP_POINT_DIMENSION",
    "EDW.TRANSIT_ACCOUNT_DIMENSION",
    "EDW.TXN_STATUS_DIMENSION",
    "EDW.CARD_DIMENSION",
    "EDW.DEVICE_DIMENSION",
    "EDW.PAYMENT_TYPE_DIMENSION",
    "EDW.EVENT_TYPE_DIMENSION",
    "EDW.BUSINESS_ENTITY_DIMENSION",
    "EDW.SALE_TYPE_DIMENSION",
    "EDW.CREDIT_CARD_TYPE_DIMENSION",
    "EDW.TRANSACTION_ORIGIN_DIMENSION",
    "EDW.CHGBK_ACTIVITY_TYPE_DIMENSION",
    "EDW.RIDER_CLASS_DIMENSION",
    "EDW.PURSE_TYPE_DIMENSION",
    "EDW.CASHBOX_EVENT_DIMENSION",
    "EDW.PASS_LIAB_EVENT_TYPE_DIMENSION",
    "EDW.MEMBER_DIMENSION",
    "EDW.REASON_DIMENSION",
    "EDW.TRAVEL_MODE_DIMENSION",
    "EDW.FACILITY_DIMENSION",
    "EDW.CUSTOMER_DIMENSION",
    "EDW.SERVICE_TYPE_DIMENSION",
    "EDW.BE_INVOICE_STATUS_DIMENSION",
    "EDW.BNFT_INVOICE_STATUS_DIMENSION",
    "EDW.EMPLOYEE_DIMENSION",
    "EDW.PATRON_ORDER_STATUS_DIMENSION",
    "EDW.PATRON_ORDER_TYPE_DIMENSION",
    "EDW.CONTACT_DIMENSION",
    "EDW.SALES_CHANNEL_DIMENSION",
    "EDW.FRM_BANK_FEE_TYPE_DIMENSION",
    "EDW.FEE_TYPE_DIMENSION",
]


def migration() -> None:
    """
    Delete all files under
    s3://<springboard>/odin/data/cubic/ods/<table>/
    for each table in TABLES_TO_DELETE.
    """
    log = ProcessLog("odin_migration", migration="dev_0013")
    failures: dict[str, int] = {}

    for table in TABLES_TO_DELETE:
        prefix = os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_FACT_DATA, table, "")
        remaining = delete_objects([obj.path for obj in list_objects(prefix)])
        if remaining:
            failures[prefix] = len(remaining)

    log.add_metadata(
        tables_attempted=len(TABLES_TO_DELETE),
        tables_failed=len(failures),
        failure_details=str(failures) if failures else "none",
    )
    log.complete()

    assert not failures, f"Failed to delete objects for prefixes: {failures}"
