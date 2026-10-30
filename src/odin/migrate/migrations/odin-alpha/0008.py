import os

from odin.utils.aws.s3 import delete_objects, object_exists
from odin.utils.locations import CUBIC_ODS_FACT_STATUS, DATA_SPRINGBOARD
from odin.utils.logger import ProcessLog


def migration() -> None:
    """Delete leftover ODS status file for Parquet EDW.TRIP_PAYMENT table"""
    status_path = os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_FACT_STATUS, "EDW.TRIP_PAYMENT.json")
    log = ProcessLog("odin_migration", migration="alpha_0008", status_path=status_path)

    if not object_exists(status_path):
        log.complete(object_found=False)
        return
    else:
        log.add_metadata(object_found=True)

    delete_failures = delete_objects([status_path])
    if delete_failures:
        exception = AssertionError(f"Failed to delete status file: {delete_failures}")
        log.add_metadata(delete_failures=str(delete_failures))
        log.failed(exception)
        raise exception

    log.complete()
