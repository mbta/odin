import os

from odin.utils.aws.s3 import delete_objects
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import s3_folder
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_BACKFILL
from odin.utils.locations import MASABI_TEMP
from odin.utils.logger import ProcessLog


def migration() -> None:
    """Clear out backfill and temporary Masabi paths."""
    backfill_prefix = s3_folder(os.path.join(DATA_SPRINGBOARD, MASABI_BACKFILL))
    temp_prefix = s3_folder(os.path.join(DATA_SPRINGBOARD, MASABI_TEMP))

    log = ProcessLog(
        "odin_migration",
        migration="alpha_0005",
        backfill_prefix=backfill_prefix,
        temp_prefix=temp_prefix,
    )

    backfill_files = [obj.path for obj in list_objects(backfill_prefix)]
    temp_files = [obj.path for obj in list_objects(temp_prefix)]
    print(f"Found {len(backfill_files)} files in {backfill_prefix}")
    print(f"Found {len(list_objects(temp_prefix))} files in {temp_prefix}")

    delete_failures = delete_objects(temp_files + backfill_files)
    if delete_failures:
        exception = AssertionError(f"Failed to clear data path: {delete_failures}")
        log.add_metadata(delete_failures=str(delete_failures))
        log.failed(exception)
        raise exception

    log.complete()
