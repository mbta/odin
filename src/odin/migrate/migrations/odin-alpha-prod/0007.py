import os

from datetime import datetime
from typing import List
from typing import Tuple

from odin.utils.logger import ProcessLog
from odin.utils.aws.s3 import S3Object
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import rename_objects
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import DATA_ERROR
from odin.utils.locations import IN_QLIK_PREFIX
from odin.utils.locations import CUBIC_QLIK_IGNORED
from odin.ingestion.qlik.dfm import dfm_from_s3
from odin.ingestion.qlik.dfm import dfm_escapes_quotes
from odin.ingestion.qlik.dfm import dfm_snapshot_dt
from odin.ingestion.qlik.dfm import QlikDFM
from odin.ingestion.qlik.utils import RE_CDC_TS
from odin.ingestion.qlik.utils import re_get_first

MIGRATION = "alpha_prod_0007"

# Tables whose Qlik endpoint was re-configured from escapeChar='' to escapeChar='"'.
TARGET_TABLES = ["EDW.ABP_REPROCESS_LOG"]


def _load_files(table: str) -> List[Tuple[S3Object, QlikDFM]]:
    """
    List every LOAD object for table, paired with its DFM.

    :param table: Qlik table name

    :return: [(S3Object, QlikDFM), ...]
    """
    found = []
    prefix = os.path.join(DATA_ARCHIVE, IN_QLIK_PREFIX, table)
    for obj in list_objects(f"{prefix}/", in_filter="LOAD"):
        if not obj.path.endswith(".csv.gz"):
            continue
        found.append((obj, dfm_from_s3(obj.path)))
    return found


def _cdc_objects(table: str) -> List[S3Object]:
    """List every cdc object (.csv.gz and .dfm) for `table`."""
    found: List[S3Object] = []
    prefix = os.path.join(DATA_ARCHIVE, IN_QLIK_PREFIX, table)
    found += list_objects(f"{prefix}__ct/", max_objects=100_000)
    return found


def migration() -> None:
    """
    Retire Qlik exports produced Cubic updated escape character to escapeChar='"'

    1. List every LOAD object and read accompanying DFM
    2. If no escaped-quote LOAD file exists, skip table
    3. Set cutoff as earliest snapshot datetime among escaped-quote LOAD files
    4. Mark every legacy-format LOAD object and accompanying DFM for retirement
    5. Mark every cdc object older than cutoff for retirement
    6. Move everything marked to the "IGNORED" prefix of the archive bucket
    """
    log = ProcessLog("odin_migration", migration=MIGRATION, target_tables=", ".join(TARGET_TABLES))
    try:
        for table in TARGET_TABLES:
            table_log = ProcessLog("odin_migration_table", migration=MIGRATION, table=table)
            load_files = _load_files(table)
            current = [(obj, dfm) for obj, dfm in load_files if dfm_escapes_quotes(dfm)]
            legacy = [obj for obj, dfm in load_files if not dfm_escapes_quotes(dfm)]

            if not current:
                table_log.complete(
                    skipped="no escaped-quote LOAD file found",
                    num_legacy=len(legacy),
                )
                continue

            cutoff = min(dfm_snapshot_dt(dfm) for _, dfm in current)

            move_paths = [obj.path for obj in legacy]
            move_paths += [p.replace(".csv.gz", ".dfm") for p in move_paths]

            num_cdc = 0
            for obj in _cdc_objects(table):
                try:
                    obj_dt = datetime.fromisoformat(re_get_first(obj.path, RE_CDC_TS))
                except LookupError:
                    continue
                if obj_dt < cutoff:
                    move_paths.append(obj.path)
                    num_cdc += 1

            table_log.add_metadata(
                cutoff=cutoff.isoformat(),
                num_current_load=len(current),
                num_legacy_load=len(legacy),
                num_cdc_retired=num_cdc,
                num_objects=len(move_paths),
            )

            if not move_paths:
                table_log.complete(nothing_to_move=True)
                continue

            failures = rename_objects(move_paths, DATA_ARCHIVE, prepend_prefix=CUBIC_QLIK_IGNORED)
            if failures:
                exception = AssertionError(
                    f"Failed to retire {len(failures)} object(s) for {table}"
                )
                table_log.add_metadata(move_failures=", ".join(failures[:10]))
                table_log.failed(exception)
                raise exception

            table_log.complete()

        log.complete()

    except Exception as exception:
        log.failed(exception)
        raise exception
