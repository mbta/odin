import os

from odin.utils.aws.s3 import copy_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.aws.s3 import get_client
from odin.utils.aws.s3 import split_object
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import CUBIC_QLIK_DATA
from odin.utils.locations import CUBIC_ODS_FACT_DATA
from odin.utils.locations import CUBIC_QLIK_PROCESSED
from odin.utils.locations import IN_QLIK_PREFIX
from odin.utils.logger import ProcessLog


def _list_paths(prefix: str) -> list[str]:
    """List all objects under prefix. Unlike list_objects(), a failed listing raises."""
    bucket, key = split_object(prefix)
    pages = get_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key)
    return [f"s3://{bucket}/{obj['Key']}" for page in pages for obj in page.get("Contents", [])]


def migration() -> None:
    """
    Rebuild PAL confirmation history and facts after removing the precision workaround.

    For each table:
    1. COPY (not move) LOAD/CDC files from the processed archive back to cubic/ods_qlik for
       re-ingestion. Processed files stay in place because dmap-import reads from there.
    2. Delete all archive parquet (cubic_qlik) and fact parquet (cubic_ods) for the table.

    Every listing and copy must succeed before anything is deleted, so a failed run can be
    retried safely. Note: ingestion skips LOAD files modified within the last 6 hours, so
    restored tables will not begin re-ingesting until ~6 hours after this runs.
    """
    tables = ["EDW.SALE_TRANSACTION", "EDW.PAL_CONFIRMATION", "EDW.UNSETTLED_CRDB_SYS_CONF"]
    log = ProcessLog("odin_migration", migration="alpha_0002", target_tables=", ".join(tables))
    try:
        processed_prefix = os.path.join(DATA_ARCHIVE, CUBIC_QLIK_PROCESSED, IN_QLIK_PREFIX)
        ingest_prefix = os.path.join(DATA_ARCHIVE, IN_QLIK_PREFIX)
        copies: list[tuple[str, str]] = []
        deletes: list[str] = []
        for table in tables:
            load_paths = _list_paths(os.path.join(processed_prefix, table, ""))
            cdc_paths = _list_paths(os.path.join(processed_prefix, f"{table}__ct", ""))
            paths = load_paths + cdc_paths

            if not any(p.endswith("/LOAD00000001.csv.gz") for p in load_paths):
                raise ValueError(f"No processed LOAD00000001.csv.gz found for {table}")
            for path in paths:
                if not path.endswith((".csv.gz", ".dfm")):
                    raise ValueError(f"Unexpected file in processed archive: {path}")
                if path.endswith(".csv.gz") and path.replace(".csv.gz", ".dfm") not in paths:
                    raise ValueError(f"Missing .dfm for {path}")

            copies += [(p, p.replace(processed_prefix, ingest_prefix, 1)) for p in paths]
            deletes += _list_paths(os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DATA, table, ""))
            deletes += _list_paths(os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_FACT_DATA, table, ""))

        log.add_metadata(copy_count=len(copies), delete_count=len(deletes))

        failed_copies = copy_objects(copies)
        if failed_copies:
            log.add_metadata(failed_copies=", ".join(failed_copies))
            raise RuntimeError(f"Failed to copy {len(failed_copies)} processed Qlik file(s)")
        failed_deletes = delete_objects(deletes)
        if failed_deletes:
            log.add_metadata(failed_deletes=", ".join(failed_deletes))
            raise RuntimeError(f"Failed to delete {len(failed_deletes)} Cubic parquet file(s)")

        log.complete()
    except Exception as exception:
        log.failed(exception)
        raise
