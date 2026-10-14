import os

from odin.utils.aws.s3 import delete_objects
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import copy_objects
from odin.utils.aws.s3 import s3_folder
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_BACKFILL
from odin.utils.locations import MASABI_DATA 
from odin.utils.logger import ProcessLog


def migration() -> None:
    """
    Clear out temporary storage from the previous backfill, for the new backfill.

    Deletes every object under MASABI_BACKFILL and MASABI_TEMPORARY; these had been
    the previous backfill data and the backup of the pre-previous-backfill data.
    """
    backfill_prefix = s3_folder(os.path.join(DATA_SPRINGBOARD, MASABI_BACKFILL))
    data_prefix = s3_folder(os.path.join(DATA_SPRINGBOARD, MASABI_DATA))

    log = ProcessLog(
        "odin_migration",
        migration="alpha_0002",
        backfill_prefix=backfill_prefix,
        data_prefix=data_prefix
    )

    backfill_files = [obj.path for obj in list_objects(backfill_prefix)]
    data_files = [obj.path for obj in list_objects(data_prefix)]
    print(f"Found {len(backfill_files)} files in {backfill_prefix}")
    print(f"Found {len(data_files)} files in {data_prefix}")

    delete_failures = delete_objects(data_files)
    if delete_failures:
        exception = AssertionError(f"Failed to clear data path: {delete_failures}")
        log.add_metadata(delete_failures=str(delete_failures))
        log.failed(exception)
        raise exception

    copy_jobs = [(src, src.replace(backfill_prefix, data_prefix)) for src in backfill_files]
    copy_failures = copy_objects(copy_jobs)
    if copy_failures:
        exception = AssertionError(f"Failed to copy backfill to data path: {copy_failures}")
        log.add_metadata(copy_failures=str(copy_failures))
        log.failed(exception)
        raise exception

    log.complete()
