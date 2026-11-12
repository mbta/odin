import os

from odin.utils.aws.s3 import delete_objects, object_exists
from odin.utils.locations import CUBIC_ODS_FACT_DATA, DATA_SPRINGBOARD
from odin.utils.logger import ProcessLog


def migration() -> None:
    """Delete empty parquet file for EC_STAGE.METRIC_HISTORY"""
    parquet_path = os.path.join(
        DATA_SPRINGBOARD, CUBIC_ODS_FACT_DATA, "EC_STAGE.METRIC_HISTORY", "year_013.parquet"
    )
    log = ProcessLog("odin_migration", migration="alpha_0010", parquet_path=parquet_path)

    if not object_exists(parquet_path):
        log.complete(object_found=False)
        return
    else:
        log.add_metadata(object_found=True)

    delete_failures = delete_objects([parquet_path])
    if delete_failures:
        exception = AssertionError(f"Failed to delete parquet file: {delete_failures}")
        log.add_metadata(delete_failures=str(delete_failures))
        log.failed(exception)
        raise exception

    log.complete()
