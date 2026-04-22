import os

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import CUBIC_ODS_FACT_DATA
from odin.utils.logger import ProcessLog

TABLES_TO_DELETE: list[str] = [
    "EDW.UNSETTLED_CRDB_ACQ_CONF",
    "EDW.UNSETTLED_CRDB_CHGBK",
    "EDW.UNSETTLED_CRDB_SYS_CONF",
    "EDW.UNSETTLED_DEVICE_CASH_STC",
    "EDW.UNSETTLED_MISC",
]


def migration() -> None:
    """
    ODIN DEV Migration 0012.

    April 22, 2026

    Delete all files under
    s3://<springboard>/odin/data/cubic/ods/<table>/
    for each table in TABLES_TO_DELETE.
    """
    log = ProcessLog("odin_migration", migration="dev_0012")
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
