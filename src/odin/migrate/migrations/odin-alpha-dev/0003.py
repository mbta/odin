import os

from odin.utils.aws.s3 import copy_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.aws.s3 import list_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_DATA
from odin.utils.locations import MASABI_TEMP
from odin.utils.logger import ProcessLog


def migration() -> None:
    """
    Back up the current live Masabi dataset into the temporary prefix.

    Deletes every object under MASABI_TEMP (clearing any previous backup),
    then copies the live MASABI_DATA dataset into it. Paired with the 0004
    migration that replaces MASABI_DATA with the backfill: MASABI_TEMP is left
    holding the original data as a rollback point if the new data is bad.
    """
    data_prefix = os.path.join(DATA_SPRINGBOARD, MASABI_DATA, "")
    temp_prefix = os.path.join(DATA_SPRINGBOARD, MASABI_TEMP, "")
    log = ProcessLog(
        "odin_migration",
        migration="alpha_dev_0003",
        data_prefix=data_prefix,
        temp_prefix=temp_prefix,
    )

    # Clear any previous backup first so stale files cannot survive the copy.
    delete_failures = delete_objects([obj.path for obj in list_objects(temp_prefix)])
    if delete_failures:
        exception = AssertionError(f"Failed to clear backup prefix {temp_prefix}")
        log.add_metadata(delete_failures=str(delete_failures))
        log.failed(exception)
        raise exception

    data_objects = [obj.path for obj in list_objects(data_prefix)]
    copy_jobs = [(obj, obj.replace(data_prefix, temp_prefix, 1)) for obj in data_objects]
    copy_failures = copy_objects(copy_jobs)

    log.add_metadata(
        objects_copied=len(copy_jobs),
        objects_failed=len(copy_failures),
        failure_details=str(copy_failures) if copy_failures else "none",
    )

    if copy_failures:
        exception = AssertionError(f"Failed to back up live objects into {temp_prefix}")
        log.failed(exception)
        raise exception

    log.complete()
