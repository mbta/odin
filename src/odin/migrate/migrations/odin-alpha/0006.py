import os

from odin.utils.aws.s3 import delete_objects
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import s3_folder
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_STATUS
from odin.utils.logger import ProcessLog


def migration() -> None:
    """Clear out Masabi backfill status objects."""
    status_prefix = s3_folder(os.path.join(DATA_SPRINGBOARD, MASABI_STATUS))

    log = ProcessLog(
        "odin_migration",
        migration="alpha_0006",
        status_prefix=status_prefix
    )

    status_files = [obj.path for obj in list_objects(status_prefix) if '_backfill' in obj.path]
    print(f"Found {len(status_files)} files in {status_prefix}")
    assert len(status_files) > 0, f"No backfill status files found in {status_prefix}"

    delete_failures = delete_objects(status_files)
    if delete_failures:
        exception = AssertionError(f"Failed to clear data path: {delete_failures}")
        log.add_metadata(delete_failures=str(delete_failures))
        log.failed(exception)
        raise exception

    log.complete()
