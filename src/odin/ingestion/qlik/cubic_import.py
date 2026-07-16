import os
import sched

from typing import List
from typing import Optional
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor

from odin.job import OdinJob
from odin.job import job_proc_schedule
from odin.utils.logger import ProcessLog
from odin.utils.runtime import sigterm_check
from odin.utils.runtime import thread_cpus
from odin.utils.instance import get_odin_instance
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import list_partitions
from odin.utils.aws.s3 import object_exists
from odin.utils.aws.s3 import rename_object
from odin.ingestion.qlik.utils import SNAPSHOT_FMT
from odin.ingestion.qlik.tables import CUBIC_ODS_TABLES
from odin.utils.locations import DATA_INCOMING
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import DATA_ERROR
from odin.utils.locations import IN_QLIK_PREFIX

NEXT_RUN_SECS = 60 * 5  # 5 minutes

# Only this Odin instance may run CubicImport. The job consumes (deletes) objects from
# the Incoming bucket that is shared by all instances, so exactly one instance must own it.
CUBIC_IMPORT_INSTANCE = "alpha"

# Qlik snapshot marker object. The S3 LastModified of this object, per table, defines the
# `snapshot=` partition value used for all subsequent archive destination keys.
SNAPSHOT_CSV = "LOAD00000001.csv.gz"

# (from_csv, to_csv, dfm_in_incoming, require_dfm)
MovePair = Tuple[str, str, bool, bool]


def dfm_path(csv_path: str) -> str:
    """Companion .dfm path for a .csv.gz path."""
    return csv_path.replace(".csv.gz", ".dfm")


def move_pair(args: MovePair) -> Optional[str]:
    """
    Move a csv.gz object and its companion .dfm from the Incoming bucket to a destination.

    The .dfm is moved before the .csv.gz so an archived .csv.gz is never missing its .dfm,
    which downstream Qlik processing requires. If the .dfm is not in the Incoming bucket but
    already exists at the destination (a previous partially failed move), the .csv.gz move
    proceeds alone. If the .dfm is in neither location the pair is skipped, as the .dfm
    upload may still be in progress.

    :param args: (from_csv, to_csv, dfm_in_incoming, require_dfm)

    :return: 'from_csv' if pair was not fully moved (retried on next run), else None
    """
    from_csv, to_csv, dfm_in_incoming, require_dfm = args
    if dfm_in_incoming:
        if rename_object(dfm_path(from_csv), dfm_path(to_csv)) is not None:
            return from_csv
    elif require_dfm and not object_exists(dfm_path(to_csv)):
        ProcessLog("cubic_import_no_dfm", csv_path=from_csv).complete()
        return from_csv

    return rename_object(from_csv, to_csv)


class CubicImport(OdinJob):
    """
    Move Cubic Qlik files from the Incoming bucket to the Archive bucket.

    This job replaces the incoming->archive copy performed by the legacy data_platform
    application, preserving its destination key layout:
        LOAD/cdc files -> {DATA_ARCHIVE}/{IN_QLIK_PREFIX}/{table}[__ct]/snapshot=X/{file}
        error files    -> {DATA_ERROR}/{IN_QLIK_PREFIX}/{table}[__ct]/timestamp=X/{file}

    The `snapshot=` value is the S3 LastModified of the most recent LOAD00000001.csv.gz
    object seen in the Incoming bucket for the table. Between runs, this value is carried
    by the archive bucket itself as the max existing `snapshot=` partition of the table
    (data_platform tracked the same value in a database).
    """

    start_kwargs = {"import_tables": len(CUBIC_ODS_TABLES)}

    def current_snapshot(self, table: str) -> Optional[str]:
        """
        Get the current snapshot value for a table from the Archive bucket.

        :param table: Cubic ODS table name

        :return: max existing snapshot partition value, None if no snapshot exists
        """
        partitions = list_partitions(os.path.join(DATA_ARCHIVE, IN_QLIK_PREFIX, table))
        snapshots = [p for p in partitions if p.startswith("snapshot=")]
        if snapshots:
            return max(snapshots).removeprefix("snapshot=")
        return None

    def table_moves(self, table: str) -> List[MovePair]:
        """
        Build list of pending file moves for a table (and its __ct change table).

        Mirrors data_platform behavior:
            - LOAD00000001.csv.gz starts a new snapshot from its S3 LastModified, even if empty
            - 0-byte files are routed to the Error bucket
            - files arriving before any snapshot exists are routed to the Error bucket

        :param table: Cubic ODS table name

        :return: list of moves for `move_pair`
        """
        moves: List[MovePair] = []
        snapshot = self.current_snapshot(table)
        for suffix in ("", "__ct"):
            table_dir = f"{table}{suffix}"
            incoming = os.path.join(DATA_INCOMING, IN_QLIK_PREFIX, table_dir)
            objects = list_objects(f"{incoming}/", include_empty=True)
            dfm_objects = {o.path for o in objects if o.path.endswith(".dfm")}
            csv_objects = sorted(
                (o for o in objects if o.path.endswith(".csv.gz")), key=lambda o: o.path
            )
            for obj in csv_objects:
                basename = os.path.basename(obj.path)
                if suffix == "" and basename == SNAPSHOT_CSV:
                    snapshot = obj.last_modified.strftime(SNAPSHOT_FMT)
                dfm_in_incoming = dfm_path(obj.path) in dfm_objects
                if obj.size_bytes == 0 or snapshot is None:
                    to_csv = os.path.join(
                        DATA_ERROR,
                        IN_QLIK_PREFIX,
                        table_dir,
                        f"timestamp={obj.last_modified.strftime(SNAPSHOT_FMT)}",
                        basename,
                    )
                    moves.append((obj.path, to_csv, dfm_in_incoming, False))
                else:
                    to_csv = os.path.join(
                        DATA_ARCHIVE,
                        IN_QLIK_PREFIX,
                        table_dir,
                        f"snapshot={snapshot}",
                        basename,
                    )
                    moves.append((obj.path, to_csv, dfm_in_incoming, True))

        return moves

    def run(self) -> int:
        """
        Move Incoming bucket Qlik files to the Archive bucket for all Cubic ODS tables.

        A failure on one table is logged and does not stop processing of remaining tables.
        Files that fail to move remain in the Incoming bucket and are retried on the next run.
        """
        instance = get_odin_instance()
        if instance != CUBIC_IMPORT_INSTANCE:
            raise RuntimeError(
                f"CubicImport only runs on '{CUBIC_IMPORT_INSTANCE}' (ODIN_INSTANCE={instance})."
            )
        pool = ThreadPoolExecutor(max_workers=thread_cpus())
        try:
            for table in CUBIC_ODS_TABLES:
                sigterm_check()
                log = ProcessLog("cubic_import_table", table=table)
                try:
                    moves = self.table_moves(table)
                    not_moved = [r for r in pool.map(move_pair, moves) if r is not None]
                    log.complete(files_found=len(moves), files_not_moved=len(not_moved))
                except Exception as exception:
                    log.failed(exception)
        finally:
            pool.shutdown()

        return NEXT_RUN_SECS


def schedule_cubic_import(schedule: sched.scheduler) -> None:
    """
    Schedule CubicImport Job.

    No-op on any instance other than CUBIC_IMPORT_INSTANCE, as the job consumes objects
    from the shared Incoming bucket and must not run on multiple instances.

    :param schedule: application scheduler
    """
    if get_odin_instance() != CUBIC_IMPORT_INSTANCE:
        return
    job = CubicImport()
    schedule.enter(0, 1, job_proc_schedule, (job, schedule))
