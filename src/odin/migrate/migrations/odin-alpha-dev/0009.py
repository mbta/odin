import os

from odin.utils.aws.s3 import delete_objects
from odin.utils.aws.s3 import list_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_BACKFILL
from odin.utils.locations import MASABI_TEMP
from odin.utils.logger import ProcessLog


def migration() -> None:
    """
    Clear out temporary storage from the previous backfill, for the new backfill.

    Deletes every object under MASABI_BACKFILL and MASABI_TEMPORARY; these had been
    the previous backfill data and the backup of the pre-previous-backfill data.
    """
    temp_prefix = os.path.join(DATA_SPRINGBOARD, MASABI_TEMP, "")
    backfill_prefix = os.path.join(DATA_SPRINGBOARD, MASABI_BACKFILL, "")

    log = ProcessLog(
        "odin_migration",
        migration="alpha_dev_0009",
        backfill_prefix=backfill_prefix,
        temp_prefix=temp_prefix,
    )

    temp_files = [obj.path for obj in list_objects(temp_prefix)]
    backfill_files = [obj.path for obj in list_objects(backfill_prefix)]
    assert temp_files, f"Expected to find files in {temp_prefix} but found none"
    assert backfill_files, f"Expected to find files in {backfill_prefix} but found none"

    delete_failures = delete_objects(temp_files + backfill_files)
    if delete_failures:
        exception = AssertionError(f"Failed to clear backup prefix {temp_prefix}")
        log.add_metadata(delete_failures=str(delete_failures))
        log.failed(exception)
        raise exception

    log.complete()
