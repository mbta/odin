import os

from odin.utils.aws.s3 import copy_objects
from odin.utils.aws.s3 import list_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_DATA
from odin.utils.locations import MASABI_TEMP
from odin.utils.logger import ProcessLog


def migration() -> None:
    """
    Copy Masabi public-table data into the temporary prefix.

    This preserves the existing object layout while duplicating the current
    MASABI public-data snapshot under the temporary path.
    """
    source_prefix = os.path.join(DATA_SPRINGBOARD, MASABI_DATA, "")
    destination_prefix = os.path.join(DATA_SPRINGBOARD, MASABI_TEMP, "")
    log = ProcessLog(
        "odin_migration",
        migration="alpha_prod_0001",
        source_prefix=source_prefix,
        destination_prefix=destination_prefix,
    )

    source_objects = [obj.path for obj in list_objects(source_prefix)]
    copy_jobs = [(obj, obj.replace(source_prefix, destination_prefix, 1)) for obj in source_objects]
    failures = copy_objects(copy_jobs)

    log.add_metadata(
        objects_attempted=len(copy_jobs),
        objects_failed=len(failures),
        failure_details=str(failures) if failures else "none",
    )

    if failures:
        exception = AssertionError(f"Failed to copy objects for source prefix: {source_prefix}")
        log.failed(exception)
        raise exception

    log.complete()
