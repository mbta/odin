import os

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import CUBIC_ODS_FACT_DATA
from odin.utils.logger import ProcessLog

TABLES_TO_DELETE: list[str] = [
    "EDW.UNSETTLED_PATRON_ORDER",
    "EDW.UNSETTLED_SALE",
    "EDW.UNSETTLED_USE",
]


def migration() -> None:
    """
    ODIN PROD Migration 0011.

    April 15, 2026

    Delete all files under
    s3://<springboard>/odin/data/cubic/ods/<table>/
    for each table in TABLES_TO_DELETE.
    """
    log = ProcessLog("migration_0011")
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
