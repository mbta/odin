import os

from odin.utils.aws.s3 import copy_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.aws.s3 import list_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_DATA
from odin.utils.locations import MASABI_BACKFILL
from odin.utils.logger import ProcessLog


def migration() -> None:
    """
    Replace the live Masabi dataset with the historical backfill.

    Deletes every object under MASABI_DATA, copies the MASABI_BACKFILL dataset
    into its place (preserving the per-table object layout), then deletes the
    backfill source to complete the move.
    """
    data_prefix = os.path.join(DATA_SPRINGBOARD, MASABI_DATA, "")
    backfill_prefix = os.path.join(DATA_SPRINGBOARD, MASABI_BACKFILL, "")
    log = ProcessLog(
        "odin_migration",
        migration="alpha_prod_0004",
        data_prefix=data_prefix,
        backfill_prefix=backfill_prefix,
    )

    backfill_objects = [obj.path for obj in list_objects(backfill_prefix)]
    # Refuse to wipe live data if the backfill is missing/empty.
    if not backfill_objects:
        exception = AssertionError(f"No backfill objects found under {backfill_prefix}; aborting.")
        log.failed(exception)
        raise exception

    # Delete the live dataset first so stale part files cannot survive the copy.
    delete_failures = delete_objects([obj.path for obj in list_objects(data_prefix)])
    if delete_failures:
        exception = AssertionError(f"Failed to delete live objects under {data_prefix}")
        log.add_metadata(delete_failures=str(delete_failures))
        log.failed(exception)
        raise exception

    copy_jobs = [(obj, obj.replace(backfill_prefix, data_prefix, 1)) for obj in backfill_objects]
    copy_failures = copy_objects(copy_jobs)

    log.add_metadata(
        objects_copied=len(copy_jobs),
        objects_failed=len(copy_failures),
        failure_details=str(copy_failures) if copy_failures else "none",
    )

    if copy_failures:
        exception = AssertionError(f"Failed to copy backfill objects into {data_prefix}")
        log.failed(exception)
        raise exception

    # Copy succeeded: remove the backfill source to complete the move.
    source_delete_failures = delete_objects(backfill_objects)
    if source_delete_failures:
        exception = AssertionError(f"Failed to delete backfill source under {backfill_prefix}")
        log.add_metadata(source_delete_failures=str(source_delete_failures))
        log.failed(exception)
        raise exception

    log.complete()
