import os

from odin.utils.aws.s3 import copy_objects
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import s3_folder
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_DATA
from odin.utils.locations import MASABI_TEMP
from odin.utils.logger import ProcessLog


def migration() -> None:
    """Copy current Masabi data into backup directory, prior to moving in backfill data."""
    api_prefix = s3_folder(os.path.join(DATA_SPRINGBOARD, MASABI_DATA))
    temp_prefix = s3_folder(os.path.join(DATA_SPRINGBOARD, MASABI_TEMP))

    log = ProcessLog(
        "odin_migration",
        migration="alpha_0002",
        api_prefix=api_prefix,
        temp_prefix=temp_prefix,
    )

    temp_files = [obj.path for obj in list_objects(temp_prefix)]
    api_files = [obj.path for obj in list_objects(api_prefix)]
    assert api_files, f"No files found in {api_prefix}"
    assert not temp_files, f"Temporary prefix {temp_prefix} is not empty"

    copy_jobs = [
        (obj.path, obj.path.replace(api_prefix, temp_prefix)) for obj in list_objects(api_prefix)
    ]
    copy_failures = copy_objects(copy_jobs)
    if copy_failures:
        exception = AssertionError(f"Failed to copy Masabi data to temp prefix: {temp_prefix}")
        log.add_metadata(copy_failures=str(copy_failures))
        log.failed(exception)
        raise exception

    log.complete()
