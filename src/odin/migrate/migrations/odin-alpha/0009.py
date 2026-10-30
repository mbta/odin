import os

from odin.utils.aws.s3 import delete_objects
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import s3_folder
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_DATA
from odin.utils.logger import ProcessLog


def migration() -> None:
    """Start regeneration of Masabi table retail.ticket_refunds"""
    tr_data_prefix = s3_folder(os.path.join(DATA_SPRINGBOARD, MASABI_DATA, "retail.ticket_refunds"))

    log = ProcessLog("odin_migration", migration="alpha_0008", tr_data_prefix=tr_data_prefix)

    tr_files = [obj.path for obj in list_objects(tr_data_prefix)]
    print(f"Found {len(tr_files)} files in {tr_data_prefix}")

    delete_failures = delete_objects(tr_files)
    if delete_failures:
        exception = AssertionError(f"Failed to clear data path: {delete_failures}")
        log.add_metadata(delete_failures=str(delete_failures))
        log.failed(exception)
        raise exception

    log.complete()
