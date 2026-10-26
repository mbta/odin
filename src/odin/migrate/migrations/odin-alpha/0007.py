import os

from odin.utils.aws.s3 import delete_objects
from odin.utils.aws.s3 import object_exists
from odin.utils.locations import CUBIC_ODS_FACT_STATUS
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.logger import ProcessLog


def migration() -> None:
    """Delete leftover ODS status file for deprecated EDW.COMPONENT_MAINT_COUNT table"""
    status_path = os.path.join(
        DATA_SPRINGBOARD, CUBIC_ODS_FACT_STATUS, "EDW.COMPONENT_MAINT_COUNT.json"
    )
    log = ProcessLog("odin_migration", migration="alpha_0007", status_path=status_path)

    if not object_exists(status_path):
        log.complete(not_found=True)
        return

    delete_failures = delete_objects([status_path])
    if delete_failures:
        exception = AssertionError(f"Failed to delete status file: {delete_failures}")
        log.add_metadata(delete_failures=str(delete_failures))
        log.failed(exception)
        raise exception

    log.complete()
