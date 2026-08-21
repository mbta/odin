import os

from odin.utils.aws.s3 import delete_objects, object_exists
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import CUBIC_ODS_DELTA_STATUS
from odin.utils.logger import ProcessLog


def migration() -> None:
    """Delete log files from legacy ODS tables that have been moved over to the delta system."""
    target_tables = ["EDW.DEVICE_LAST_STATE"]
    log = ProcessLog(
        "odin_migration", migration="alpha_dev_0006", target_tables=", ".join(target_tables)
    )
    target_paths = [
        os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_DELTA_STATUS, x) + ".json" for x in target_tables
    ]
    print(target_paths)
    paths = [x for x in target_paths if object_exists(x)]
    if not all(paths):
        exception = AssertionError("Could not find status tables")
        log.add_metadata(not_found=", ".join([x for x in target_paths if not object_exists(x)]))
        log.failed(exception)
        raise exception

    delete_failures = delete_objects(paths)

    if delete_failures:
        exception = AssertionError("Failed to delete status files")
        log.add_metadata(source_delete_failures=str(delete_failures))
        log.failed(exception)
        raise exception

    log.complete()
