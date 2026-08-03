"""
Delta-based Cubic ODS silver-table materialization.

This is a parallel alternative to ``generate/cubic/ods_fact.py``. It
reads the same snapshot-partitioned Qlik history parquet produced by
``ingestion/qlik/cubic_archive.py`` and materializes the current-state ("silver")
table as a Delta Lake table via MERGE, instead of the custom-parquet fact
pipeline.

The history parquet is queried with DuckDB (read side) and the silver table is
written with delta-rs (write side); no custom parquet utilities are involved.

Crucially it never moves, deletes, or prunes any source files. Its position on
the history input is recorded in the silver table's Delta commit metadata (the
snapshot generation and the max processed ``header__change_seq``), so it can run
concurrently with both ``cubic_archive.py`` (which owns moving raw files) and
``ods_fact.py`` (the existing fact pipeline). Tracking the watermark in commit
metadata rather than deriving it from the surviving rows is deliberate: a CDC
batch of only deletes removes the row holding the max ``header__change_seq``, so
a contents-derived watermark would regress and re-read that batch forever.

The source data is treated as untrusted: every run asserts the invariants it
depends on (required columns present, primary keys declared and present, CDC
records carry a change sequence, a load snapshot is non-empty, and — on
year/month-partitioned tables — load and insert images carry a non-null
``edw_inserted_dtm``, since a row without one would land in an odin_year=0
partition that the pruned merge scan never revisits). Violations raise rather
than silently producing a corrupt silver table.

Two Qlik Replicate behaviors are relied on without a runtime check:
  - ``header__change_seq`` is unique per change record, so paging with
    ``ORDER BY header__change_seq LIMIT n`` and a strictly-greater watermark
    can never split records sharing a sequence across batches (which would
    permanently skip the cut-off records).
  - "I" records are full row images (only "U" records may be sparse). Both
    "I" and "D" are therefore per-key *reset events*, and each key's latest
    one alone decides its action: D deletes the row (trailing orphan updates
    are dropped), I replaces/inserts it as the image plus trailing sparse
    updates (NULL means NULL), and a key with neither patches the target row
    sparsely. This resolution is batch-split invariant — see the reference
    interpreter in delta_ods_property_test.py.

Steps per run:
  1. Find the latest history ``snapshot=`` partition.
  2. If it differs from the silver table's ``odin_snapshot``, rebuild silver from
     that snapshot's "L" (load) records (full overwrite).
  3. MERGE any CDC records (I/U/D) with ``header__change_seq`` greater than the
     silver watermark into the silver table.
"""

import os
import re
import sched
import tempfile
import time
from typing import Iterator

import duckdb
from duckdb import OutOfMemoryException
import polars as pl
import psutil
import pyarrow as pa

from deltalake import CommitProperties
from deltalake import DeltaTable
from deltalake import WriterProperties
from deltalake import write_deltalake
from deltalake.exceptions import SchemaMismatchError

from odin.job import OdinJob
from odin.job import job_proc_schedule
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import list_partitions
from odin.utils.aws.s3 import s3_file
from odin.utils.aws.s3 import s3_folder
from odin.utils.delta import open_delta
from odin.utils.delta import row_count as delta_row_count
from odin.utils.locations import CUBIC_ODS_DELTA_DATA
from odin.utils.locations import CUBIC_ODS_DELTA_STATUS
from odin.utils.locations import CUBIC_QLIK_PROCESSED
from odin.utils.locations import CUBIC_QLIK_DATA
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.logger import MdValues
from odin.utils.logger import ProcessLog
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import ds_metadata_min_max
from odin.utils.runtime import sigterm_check
from odin.utils.status import lag_seconds
from odin.utils.status import progress_fields
from odin.utils.status import publish_status
from odin.utils.status import read_status
from odin.utils.status import utc_now
from odin.ingestion.qlik.dfm import QlikDFM
from odin.ingestion.qlik.dfm import dfm_from_s3
from odin.ingestion.qlik.utils import RE_SNAPSHOT_TS
from odin.ingestion.qlik.utils import seq_as_datetime
from odin.ingestion.qlik.tables import _ODIN_INSTANCE
from odin.ingestion.qlik.tables import CUBIC_ODS_DELTA_TABLES_INSTANCE

NEXT_RUN_DEFAULT = 60 * 60 * 4  # 4 hours
NEXT_RUN_BETA = 60 * 15  # 15 minutes
NEXT_RUN_IMMEDIATE = 30  # 30 seconds
NEXT_RUN_LONG = 60 * 60 * 12  # 12 hours

REBUILD_BATCH_SIZE = 10_000
MAX_MERGE_RECORDS = 1_000_000

CDC_OPERS = ("I", "U", "D")

DELTA_WRITER_PROPERTIES = WriterProperties(max_row_group_size=64 * 1024, compression="SNAPPY")

os.environ.setdefault("DELTARS_MAX_CONCURRENCY_TASKS", "4")

# Keys under which each Delta commit records the job's input position in its
# custom metadata (readable via DeltaTable.history()). This is the source of
# truth for "where the table is at", independent of the surviving row contents.
STATE_SNAPSHOT_KEY = "odin_snapshot"
STATE_WATERMARK_KEY = "odin_cdc_watermark"
INITIAL_WATERMARK = "0"  # header__change_seq is a zero-padded string; all seqs > "0"
HISTORY_SCAN_LIMIT = 50  # commits to scan back for the latest recorded position
# Deeper fallback for _read_state. One merged batch now commits once per partition it
# touches, so an interrupted run can bury the last recorded position well past the
# shallow limit; only reaching the end of the log proves there is no position to find.
HISTORY_SCAN_LIMIT_DEEP = 2_000

# DuckDB read expression over the snapshot-partitioned history parquet.
READ_HISTORY = "read_parquet('{glob}', hive_partitioning = true, union_by_name = true)"

# Hive partition values in a history object key, newest of which holds the newest seq.
RE_HISTORY_YEAR_MONTH = re.compile(r"header__year=(\d+)/header__month=(\d+)/")

# Columns required to be present in the history parquet for the job to run.
REQUIRED_HISTORY_COLUMNS = (
    "header__change_oper",
    "header__change_seq",
    "header__from_csv",
    "snapshot",
)

# History/CDC metadata columns dropped during materialization to silver.
# header__change_seq IS kept on silver — it is the CDC watermark.
META_DROP_COLUMNS = (
    "header__year",
    "header__month",
    "header__change_oper",
    "header__timestamp",
    "header__from_csv",
    "snapshot",
)


def _default_run_interval() -> int:
    """Return the normal rerun interval for the active instance."""
    return NEXT_RUN_BETA if _ODIN_INSTANCE == "beta" else NEXT_RUN_DEFAULT


def _long_run_interval() -> int:
    """Return the no-new-data rerun interval for the active instance."""
    return NEXT_RUN_BETA if _ODIN_INSTANCE == "beta" else NEXT_RUN_LONG


def _connect(path: str, spill_dir: str) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection, configuring S3 access for s3:// paths."""
    con = duckdb.connect()
    # An in-memory DuckDB has no temp_directory, so memory-heavy queries OOM
    # instead of spilling; point it at a job-scoped scratch folder.
    os.makedirs(spill_dir, exist_ok=True)
    con.execute(f"SET temp_directory = '{spill_dir}'")
    # DuckDB's default budget is 80% of RAM, but this process also holds the
    # polars CDC batch and runs the delta-rs merge afterwards; cap DuckDB at
    # half so those stages keep headroom (excess spills to disk instead).
    con.execute(f"SET memory_limit = '{psutil.virtual_memory().total // (2 * 1024**2)}MB'")
    # No query depends on row order (downstream sorts where it matters), so
    # let scans stream instead of buffering to preserve insertion order.
    con.execute("SET preserve_insertion_order = false")
    # Progress bar output would spam the logging system.
    con.execute("PRAGMA disable_progress_bar;")
    con.execute("PRAGMA disable_print_progress_bar;")
    # parquet_metadata_cache is deliberately NOT enabled: it is documented to
    # never invalidate, and cubic_archive rewrites history files in place on
    # S3, so a footer cached early in the run can go stale and decode garbage
    # byte ranges from the rewritten file (seen as absurd petabyte "OOM"
    # allocations). Footers are re-fetched per query instead. DuckDB's
    # external file cache stays on — it validates cached data against the
    # file's etag before reuse, so it is safe with mutable files.
    if path.startswith("s3://"):
        con.execute("CREATE OR REPLACE SECRET secret (TYPE s3, PROVIDER credential_chain);")
    return con


class NoQlikHistoryError(Exception):
    """No Qlik history snapshots are available to process."""


class CDCSchemaIncompatibleError(Exception):
    """A non-additive schema change was detected; the pipeline cannot proceed."""


class CubicODSDelta(OdinJob):
    """Materialize one Cubic ODS table as a Delta silver table from history parquet."""

    def __init__(self, table: str) -> None:
        """Create CubicODSDelta instance for `table`."""
        self.table = table
        self.s3_source = os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DATA, table)
        self.silver_uri = s3_file(os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_DELTA_DATA, table))
        self.start_kwargs = {"table": table}
        self.silver: DeltaTable | None = None
        self.history_root = s3_folder(self.s3_source)
        self.history_columns: list[str] = []
        self.history_snapshot = ""
        self.part_columns: list[str] = []
        self._con: duckdb.DuckDBPyConnection | None = None
        # Post-merge position, published by _write_status. _merge_cdc advances the
        # watermark; both stay at their defaults on the paths where it does no work.
        self.cdc_watermark = INITIAL_WATERMARK
        self.more_pending = False
        self._run_started: float | None = None

    def run(self) -> int:
        """Materialize the latest snapshot + CDC into silver; return seconds to next run."""
        self.start_kwargs = {"table": self.table}
        self._run_started = time.perf_counter()
        try:
            self.silver = open_delta(self.silver_uri)
            self._snapshot_check()

            silver_snapshot, cdc_watermark = self._read_state()
            if self.history_snapshot != silver_snapshot:
                self._rebuild_silver()
                cdc_watermark = INITIAL_WATERMARK

            next_run = self._merge_cdc(cdc_watermark)
            self._write_status(next_run)

            self.start_kwargs.update(
                {
                    "history_snapshot": self.history_snapshot,
                    "new_snapshot": str(self.history_snapshot != silver_snapshot),
                }
            )
            return next_run
        except OutOfMemoryException as e:
            # This has typically been due to transient invalid allocations by duckdb; notably
            # an actual non-duckdb OOM raises SystemError.
            self.run_delay_secs = NEXT_RUN_IMMEDIATE
            raise e
        finally:
            self._close_db()

    def _db(self) -> duckdb.DuckDBPyConnection:
        """Return the run-scoped DuckDB connection, creating it on first use."""
        if self._con is None:
            # OdinJob.start() provides tmpdir (cleaned after each run); direct
            # step calls (tests) fall back to the system temp folder.
            scratch = getattr(self, "tmpdir", None) or tempfile.gettempdir()
            self._con = _connect(self.history_root, os.path.join(scratch, "duckdb_spill"))
        return self._con

    def _close_db(self) -> None:
        """Close the run-scoped DuckDB connection, if open."""
        if self._con is not None:
            self._con.close()
            self._con = None

    @property
    def _read_history(self) -> str:
        """
        Return the DuckDB read_parquet expression for the current snapshot's history.

        The glob is restricted to the ``snapshot=`` partition being materialized
        (DuckDB's ``**`` matches zero or more directories, and hive partitioning
        still derives the ``snapshot`` column from the path). Reading the whole
        table root instead would union columns across every historical snapshot,
        letting columns dropped from the current snapshot leak into silver as
        phantom all-null columns.
        """
        glob = f"{self.history_root}snapshot={self.history_snapshot}/**/*.parquet"
        return READ_HISTORY.format(glob=glob)

    def _read_state(self) -> tuple[str, str]:
        """
        Return (snapshot, cdc_watermark) from the latest commit that recorded them.

        A silver table with no recorded position (never built, or built by an
        older version) reads as ("", INITIAL_WATERMARK), which forces a rebuild.

        The scan escalates rather than using one fixed depth. A completed run leaves
        the position on the newest commit, so the shallow pass almost always hits on
        the first entry — but a run interrupted between partition merges (sigterm_check
        fires inside that loop, so this is routine) leaves one position-less commit per
        merged partition on top of it. Giving up at a fixed depth there would report
        "no recorded position", and the caller reads that as a snapshot mismatch and
        rebuilds the entire table. Only a scan that reaches the end of the log without
        finding a position is entitled to that conclusion.
        """
        if self.silver is None:
            return "", INITIAL_WATERMARK
        for limit in (HISTORY_SCAN_LIMIT, HISTORY_SCAN_LIMIT_DEEP):
            commits = self.silver.history(limit)
            for commit in commits:
                if STATE_SNAPSHOT_KEY in commit:
                    return commit[STATE_SNAPSHOT_KEY], commit.get(
                        STATE_WATERMARK_KEY, INITIAL_WATERMARK
                    )
            if len(commits) < limit:
                break  # scanned the whole log; the position genuinely is not there
        ProcessLog(
            "delta_state_not_found",
            table=self.table,
            commits_scanned=len(commits),
        ).complete()
        return "", INITIAL_WATERMARK

    def _commit_state(self, watermark: str) -> CommitProperties:
        """Commit metadata recording the current snapshot and CDC watermark."""
        return CommitProperties(
            custom_metadata={
                STATE_SNAPSHOT_KEY: self.history_snapshot,
                STATE_WATERMARK_KEY: watermark,
            }
        )

    def _commit_watermark(self, watermark: str) -> None:
        """
        Record `watermark` as the job's position in a commit of its own.

        The position cannot ride along on the partition merges. delta-rs makes no
        commit at all for a MERGE that changes nothing ("Do not make a commit when
        there are zero updates to the state"), so a chunk that matches nothing —
        an ordinary occurrence, since orphan updates are dropped by design — would
        silently take the watermark with it and the batch would replay forever.

        A zero-row append is inert — no files added, no rows changed — but always
        commits.
        """
        assert self.silver is not None
        empty = pa.Table.from_pylist([], schema=pa.schema(self.silver.schema().to_arrow()))  # type: ignore
        write_deltalake(
            self.silver_uri,
            empty,
            mode="append",
            commit_properties=self._commit_state(watermark),
        )
        self.silver = DeltaTable(self.silver_uri)

    # ------------------------------------------------------------------
    # Snapshot discovery
    # ------------------------------------------------------------------

    def _snapshot_check(self) -> None:
        """Locate the latest history snapshot partition and validate its schema."""
        history_snapshots = list_partitions(self.s3_source)
        if not history_snapshots:
            raise NoQlikHistoryError(f"No history snapshots available for {self.table}.")
        self.history_snapshot = history_snapshots[-1].replace("snapshot=", "")
        assert RE_SNAPSHOT_TS.fullmatch(self.history_snapshot), (
            f"unexpected snapshot partition name for {self.table}: {self.history_snapshot!r}"
        )

        describe = self._db().execute(f"DESCRIBE SELECT * FROM {self._read_history}").pl()
        self.history_columns = describe.get_column("column_name").to_list()

        missing = set(REQUIRED_HISTORY_COLUMNS) - set(self.history_columns)
        assert not missing, (
            f"history for {self.table} is missing required columns: {sorted(missing)}"
        )
        self.part_columns = (
            ["odin_year", "odin_month"] if "edw_inserted_dtm" in self.history_columns else []
        )
        ProcessLog(
            "delta_snapshot_check",
            table=self.table,
            history_snapshot=self.history_snapshot,
            history_snapshots_available=len(history_snapshots),
            partition_columns=self.part_columns,
        ).complete()

    # ------------------------------------------------------------------
    # Snapshot rebuild (silver overwrite from "L" records)
    # ------------------------------------------------------------------

    def _rebuild_silver(self) -> None:
        """Overwrite silver with the "L" (load) records of the current snapshot."""
        log = ProcessLog("delta_rebuild_silver", table=self.table, snapshot=self.history_snapshot)
        data_columns = [f'"{c}"' for c in self.history_columns if c not in META_DROP_COLUMNS]
        partition_columns = (
            [
                "CAST(coalesce(strftime(edw_inserted_dtm, '%Y'), '0') AS INTEGER) AS odin_year",
                "CAST(coalesce(strftime(edw_inserted_dtm, '%m'), '0') AS INTEGER) AS odin_month",
            ]
            if self.part_columns
            else []
        )
        select_exprs = [*data_columns, "? AS odin_snapshot", *partition_columns]
        sql = (
            f"SELECT {', '.join(select_exprs)} FROM {self._read_history} "
            "WHERE snapshot = ? AND header__change_oper = 'L'"
        )

        # Validate the load records BEFORE the overwrite: once write_deltalake
        # commits, silver's contents and recorded snapshot have already advanced,
        # so a post-write failure would leave a wedged (empty or mispartitioned)
        # table that the next run no longer knows to rebuild.
        self._check_load_records()

        sigterm_check()
        reader = (
            self._db()
            .execute(sql, [self.history_snapshot, self.history_snapshot])
            .fetch_record_batch(REBUILD_BATCH_SIZE)
        )
        # A rebuild writes every year/month partition of the table's history in one
        # pass, so it holds the most concurrent partition writers of any step here;
        # the capped row group matters more on this path than on the merge.
        write_deltalake(
            self.silver_uri,
            reader,
            mode="overwrite",
            schema_mode="overwrite",
            partition_by=self.part_columns or None,
            commit_properties=self._commit_state(INITIAL_WATERMARK),
            writer_properties=DELTA_WRITER_PROPERTIES,
        )

        self.silver = DeltaTable(self.silver_uri)
        log.complete(rows_loaded=delta_row_count(self.silver))

    def _check_load_records(self) -> None:
        """Assert the snapshot's "L" records can produce a valid silver table."""
        checks = ["count(*)"]
        if self.part_columns:
            checks.append('count(*) FILTER (WHERE "edw_inserted_dtm" IS NULL)')
        row = (
            self._db()
            .execute(
                f"SELECT {', '.join(checks)} FROM {self._read_history} "
                "WHERE snapshot = ? AND header__change_oper = 'L'",
                [self.history_snapshot],
            )
            .fetchone()
        )
        assert row is not None and row[0] > 0, (
            f"snapshot {self.history_snapshot} for {self.table} has no L (load) records"
        )
        if self.part_columns:
            assert row[1] == 0, (
                f"snapshot {self.history_snapshot} for {self.table} has {row[1]} L (load) "
                "records with a null edw_inserted_dtm; rows would land in the odin_year=0 "
                "partition, which partition-pruned merges never revisit"
            )

    # ------------------------------------------------------------------
    # CDC MERGE (silver update from I/U/D records)
    # ------------------------------------------------------------------

    def _newest_history_files(self) -> list[str]:
        """
        Return the history files of the newest header__year/header__month partition.

        The partition values are read from the S3 keys themselves, so narrowing costs
        one LIST and no file reads.

        :return: paths in the newest partition, or every path if none are partitioned
        """
        objects = list_objects(
            f"{self.history_root}snapshot={self.history_snapshot}/", in_filter=".parquet"
        )
        newest_key: tuple[int, int] | None = None
        newest: list[str] = []
        for obj in objects:
            match = RE_HISTORY_YEAR_MONTH.search(obj.path)
            if match is None:
                continue
            key = (int(match.group(1)), int(match.group(2)))
            if newest_key is None or key > newest_key:
                newest_key, newest = key, [obj.path]
            elif key == newest_key:
                newest.append(obj.path)
        # Unpartitioned history (no header__year=/header__month= in the keys) still has
        # a correct answer, just a more expensive one: read every footer.
        return newest or [o.path for o in objects]

    def _history_max_seq(self) -> str | None:
        """
        Return the newest header__change_seq in the current snapshot's history.

        header__year/header__month derive from header__timestamp, the same clock that
        generates header__change_seq, so the newest partition holds the newest sequence
        by construction rather than by luck of file ordering.
        """
        files = self._newest_history_files()
        if not files:
            return None
        _, max_seq = ds_metadata_min_max(ds_from_path(files), "header__change_seq")
        return None if max_seq is None else str(max_seq)

    def _write_status(self, next_run_secs: int) -> None:
        """
        Publish this table's freshness status to S3 as JSON.

        :param next_run_secs: seconds until this table's next scheduled run
        """
        now = utc_now()
        try:
            history_max_seq = self._history_max_seq()
        except Exception as exception:
            # Degrade to the clock-only measure rather than fail the run: seq_lag is
            # the better signal, but it is not worth a completed merge.
            ProcessLog("delta_history_max_seq", table=self.table).failed(exception)
            history_max_seq = None
        seq_dt = seq_as_datetime(self.cdc_watermark)
        history_seq_dt = seq_as_datetime(history_max_seq)
        seq_lag = lag_seconds(history_seq_dt, seq_dt)
        row_count = None if self.silver is None else delta_row_count(self.silver)
        prev = read_status(CUBIC_ODS_DELTA_STATUS, self.table, self.scratch)
        status: dict[str, MdValues] = {
            "table": self.table,
            "last_run": now.isoformat(),
            "snapshot": self.history_snapshot,
            "row_count": row_count,
            "cdc_watermark": str(self.cdc_watermark),
            "watermark_datetime": None if seq_dt is None else seq_dt.isoformat(),
            "history_max_header__change_seq": (
                None if history_max_seq is None else str(history_max_seq)
            ),
            "history_max_change_seq_datetime": (
                None if history_seq_dt is None else history_seq_dt.isoformat()
            ),
            "seq_lag_seconds": seq_lag,
            "clock_lag_seconds": lag_seconds(now, seq_dt),
            "merge_budget_full": self.more_pending,
            "next_run_seconds": next_run_secs,
        }
        status.update(
            progress_fields(
                prev=prev,
                now=now,
                run_duration_secs=(
                    None if self._run_started is None else time.perf_counter() - self._run_started
                ),
                row_count=row_count,
                watermark=seq_dt,
                lag_secs=seq_lag,
                # A rebuild resets the watermark to INITIAL_WATERMARK and rewrites every
                # row, so differencing across it would report a fictional rate.
                comparable=prev is not None and prev.get("snapshot") == self.history_snapshot,
            )
        )
        publish_status(CUBIC_ODS_DELTA_STATUS, self.table, self.scratch, status)

    def _merge_cdc(self, after_seq: str) -> int:
        """Apply CDC records with seq > `after_seq` to silver; return the next-run interval."""
        self.cdc_watermark = after_seq
        if self.silver is None:
            return _long_run_interval()
        log = ProcessLog("delta_merge_cdc", table=self.table)

        cdc_df = self._read_cdc(after_seq, limit=MAX_MERGE_RECORDS)
        if cdc_df.height == 0:
            log.complete(cdc_records_found=0)
            return _long_run_interval()
        cdc_rows = cdc_df.height

        # The batch is fully materialized in polars now and nothing downstream of
        # here reads history, but DuckDB is still holding a buffer pool sized at
        # half of system memory — memory the merge is about to need. Hand it back
        # before the expensive stages; _db() reopens lazily if anything asks.
        self._close_db()

        assert cdc_df.get_column("header__change_seq").null_count() == 0, (
            f"CDC records for {self.table} contain a null header__change_seq"
        )
        max_seq = cdc_df.get_column("header__change_seq").max()
        assert max_seq, f"No valid header__change_seq (.max() => {max_seq})"
        max_seq_processed = str(max_seq)

        keys = self._discover_keys(cdc_df)
        self._check_partition_stability(cdc_df, keys)
        source = self._build_merge_source(cdc_df, keys)
        # The raw batch holds every CDC record; `source` holds one resolved row per
        # key. Drop the larger of the two before the merge rather than after it.
        del cdc_df
        try:
            metrics = self._merge_apply(source, keys, max_seq_processed)
        except SchemaMismatchError as exc:
            raise CDCSchemaIncompatibleError(
                f"silver MERGE failed for {self.table}: {exc}"
            ) from exc
        self._commit_watermark(max_seq_processed)

        self.silver = DeltaTable(self.silver_uri)
        more_pending = cdc_rows >= MAX_MERGE_RECORDS
        self.cdc_watermark = max_seq_processed
        self.more_pending = more_pending

        log.complete(
            cdc_records_processed=cdc_rows,
            merge_source_rows=source.height,
            final_row_count=delta_row_count(self.silver),
            cdc_watermark=max_seq_processed,
            more_pending=more_pending,
            key_cols=",".join(keys),
            **{f"merge_{k}": v for k, v in metrics.items()},
            **self._partition_metrics(source),
        )
        return NEXT_RUN_IMMEDIATE if more_pending else _default_run_interval()

    # How many per-partition row counts to spell out in the merge log line;
    # beyond this the list is truncated (partitions_touched stays exact).
    PARTITION_LOG_LIMIT = 24

    def _partition_metrics(self, source: pl.DataFrame) -> dict:
        """
        Log fields describing the partitions a merge touches (dated tables only).

        Reported from the merge source (the same values _partition_constraint
        prunes by): how many distinct odin_year/odin_month partitions the batch
        reaches and the row count landing in each, oldest first — the signal
        for pathological update patterns (e.g. frequent history-wide sweeps).
        Rows without edw_inserted_dtm have an unknown target partition; they
        are counted separately and disable pruning for the whole merge.
        """
        if "odin_year" not in source.columns:
            return {}
        parts = (
            source.filter(pl.col("edw_inserted_dtm").is_not_null())
            .group_by("odin_year", "odin_month")
            .len()
            .sort("odin_year", "odin_month")
        )
        labels = [f"{y:04d}-{m:02d}={n}" for y, m, n in parts.iter_rows()]
        if len(labels) > self.PARTITION_LOG_LIMIT:
            hidden = len(labels) - self.PARTITION_LOG_LIMIT
            labels = labels[: self.PARTITION_LOG_LIMIT] + [f"+{hidden} more"]
        metrics = {
            "partitions_touched": parts.height,
            "partition_rows": ",".join(labels),
            "partition_scan_pruned": bool(self._partition_constraint(source)),
        }
        unknown = source.get_column("edw_inserted_dtm").null_count()
        if unknown:
            metrics["partition_rows_unknown"] = unknown
        return metrics

    def _read_cdc(self, after_seq: str, limit: int) -> pl.DataFrame:
        """
        Read the next batch of CDC (I/U/D) records with seq > `after_seq`.

        Two steps so the wide read stays bounded no matter how well the parquet
        prunes: first find the ceiling seq of the next `limit` records reading only
        the seq column (a narrow top-k scan), then read full rows for
        `after_seq < seq <= ceiling`. Taking the whole `<= ceiling` range (no LIMIT)
        keeps records sharing the boundary seq in one batch, so the advancing
        watermark can never strand tied rows.

        The `IS NULL` arm surfaces any null-seq record (a data error) into
        the batch, where the null_count assertion in _merge_cdc rejects it
        instead of skipping it forever.
        """
        log = ProcessLog(process="_read_cdc", table=self.table)
        opers = ", ".join(f"'{o}'" for o in CDC_OPERS)
        snap = self.history_snapshot
        con = self._db()
        # Step 1 — narrow: ceiling seq of the next `limit` records, aggregated
        # server-side so only (count, max) crosses to the client instead of up
        # to `limit` seq strings. Doubles as the "anything past the watermark?"
        # probe; a non-empty window with a NULL ceiling means every pending
        # record has a null seq.
        window = con.execute(
            "SELECT count(*), max(seq) FROM ("
            f"SELECT header__change_seq AS seq FROM {self._read_history} "
            f"WHERE snapshot = ? AND header__change_oper IN ({opers}) "
            "AND (header__change_seq > ? OR header__change_seq IS NULL) "
            f"ORDER BY header__change_seq NULLS FIRST LIMIT {limit})",
            [snap, str(after_seq)],
        ).fetchone()
        assert window is not None
        window_rows, ceiling = window
        if window_rows == 0:
            log.complete(window_rows=0)
            return pl.DataFrame()

        # Step 2 — wide: full rows bounded to the window's seq range. No ORDER
        # BY: sorting the wide batch would buffer/spill the whole result inside
        # DuckDB, and _build_merge_source orders by seq itself where it matters.
        conditions = ["snapshot = ?", f"header__change_oper IN ({opers})"]
        params: list[str] = [snap]
        if ceiling is None:
            conditions.append("header__change_seq IS NULL")
        else:
            conditions.append(
                "((header__change_seq > ? AND header__change_seq <= ?) "
                "OR header__change_seq IS NULL)"
            )
            params += [str(after_seq), str(ceiling)]
        result = con.execute(
            f"SELECT * FROM {self._read_history} WHERE {' AND '.join(conditions)}",
            params,
        ).pl()
        log.complete(window_rows=window_rows, read_size=len(result))
        return result

    def _discover_keys(self, cdc_df: pl.DataFrame) -> list[str]:
        """Return the primary-key column names (lowercased) from the table DFM."""
        dfm = self._dfm_from_records(cdc_df)
        keys = [
            col["name"].lower() for col in dfm["dataInfo"]["columns"] if col["primaryKeyPos"] > 0
        ]
        assert keys, f"DFM for {self.table} declares no primary key columns"
        missing = set(keys) - set(cdc_df.columns)
        assert not missing, (
            f"primary key columns {sorted(missing)} absent from CDC data for {self.table}"
        )
        return keys

    def _check_partition_stability(self, cdc_df: pl.DataFrame, keys: list[str]) -> None:
        """
        Assert no key's CDC records disagree on edw_inserted_dtm.

        Merging one partition at a time means each MERGE scans only the partition
        its own source rows land in, so a key whose target row lives in a *different*
        partition is never seen: the row is inserted alongside the old one instead
        of replacing it, and the key silently duplicates. The whole scheme rests on
        edw_inserted_dtm being fixed for the life of a key, which makes a row's
        partition fixed too.

        This is the affordable half of that invariant — it compares a key's records
        against each other, in memory, and so catches a feed that starts mutating
        the column (including a delete/re-insert that reuses a primary key with a
        new timestamp, which a merge chunk cannot resolve). It cannot catch a
        mutation relative to a row written by an *earlier* batch; confirming that
        would mean reading every key's current partition out of silver on every
        run, which costs more than the merge it protects.
        """
        if "edw_inserted_dtm" not in cdc_df.columns:
            return
        disagreeing = (
            cdc_df.filter(pl.col("edw_inserted_dtm").is_not_null())
            .group_by(keys)
            .agg(pl.col("edw_inserted_dtm").n_unique().alias("distinct_edw"))
            .filter(pl.col("distinct_edw") > 1)
        )
        assert disagreeing.height == 0, (
            f"{disagreeing.height} key(s) in the {self.table} CDC batch carry more than one "
            "edw_inserted_dtm; a key's partition must not change or the partition-scoped "
            f"merge would duplicate it. Offending keys: {disagreeing.head(5).rows()}"
        )

    def _dfm_from_records(self, cdc_df: pl.DataFrame) -> QlikDFM:
        """Locate a DFM for the CDC source CSVs, trying processed then source paths."""
        for candidate in self._dfm_candidates(cdc_df):
            try:
                return dfm_from_s3(candidate)
            except Exception:
                continue
        raise RuntimeError(f"Could not locate DFM for any {self.table} CDC source path")

    def _dfm_candidates(self, cdc_df: pl.DataFrame) -> Iterator[str]:
        """
        Yield candidate DFM paths for each CDC source CSV, processed prefix first.

        cubic_archive.py moves source files to the processed prefix, so that
        location is tried first; the original path is the fallback for files that
        have not been moved yet.
        """
        for path in cdc_df.get_column("header__from_csv").unique().to_list():
            if not path:
                continue
            rel = path.replace("s3://", "").split("/", 1)[-1]
            yield s3_file(os.path.join(DATA_ARCHIVE, CUBIC_QLIK_PROCESSED, rel))
            yield s3_file(path)

    def _build_merge_source(self, cdc_df: pl.DataFrame, keys: list[str]) -> pl.DataFrame:
        """
        Resolve CDC records to one final row per key for the silver MERGE.

        Each key resolves to a single action, ``odin_resolved_oper``, decided by
        the key's latest I or D record. Both are *reset events* — a delete ends
        the row and an insert image wholly replaces it — so nothing recorded
        before the latest one can affect the final row:

          - "D" (latest reset is a delete): the row is deleted. Any trailing U
            records are orphans (updates to a row that no longer exists) and
            are dropped — the same outcome those events produce when a batch
            boundary separates them from the delete.
          - "I" (latest reset is an insert): the row becomes the insert image
            overlaid with the non-null values of the trailing U records. A NULL
            in that result means NULL (I images are full row images); on a
            matched target row it replaces, never coalesces.
          - "U" (no I or D in the batch): sparse update — per-column latest
            non-null value across the key's U records; the MERGE coalesces the
            remaining NULLs against the target row. Keys with no target row
            are dropped.

        This resolution is batch-split invariant: cutting the same event stream
        into batches at different points yields the same final table. Verified
        against the reference interpreter in delta_ods_property_test.py.
        """
        log = ProcessLog("_build_merge_source", table=self.table, cdc_size=len(cdc_df))
        data_cols = [
            c
            for c in cdc_df.columns
            if c not in keys and c not in META_DROP_COLUMNS and c != "header__change_seq"
        ]

        # Watermark lineage: each key's row carries its highest processed seq.
        winners = cdc_df.group_by(keys).agg(pl.col("header__change_seq").max())

        # The reset event (latest I or D) per key; keys with neither resolve "U".
        resets = (
            cdc_df.select(*keys, "header__change_seq", "header__change_oper")
            .filter(pl.col("header__change_oper").is_in(("I", "D")))
            .group_by(keys)
            .agg(
                pl.col("header__change_oper")
                .sort_by("header__change_seq")
                .last()
                .alias("odin_resolved_oper"),
                pl.col("header__change_seq").max().alias("_reset_seq"),
            )
        )

        # Data values: latest non-null per column across the key's records at or
        # after its reset (all records when there is none). By construction this
        # folds I + trailing Us for "I" keys and only Us for "U" keys; for "D"
        # keys it starts at the delete image itself, whose values are unused by
        # the delete except edw_inserted_dtm, which _partition_constraint needs
        # to keep the merge scan pruned (it names the deleted row's partition).
        folded = (
            cdc_df.join(resets.select(*keys, "_reset_seq"), on=keys, how="left", nulls_equal=True)
            .filter(
                pl.col("_reset_seq").is_null()
                | (pl.col("header__change_seq") >= pl.col("_reset_seq"))
            )
            .sort(by="header__change_seq", descending=True)
            .group_by(keys)
            .agg(pl.col(c).drop_nulls().first() for c in data_cols)
        )

        source = (
            winners.join(
                resets.select(*keys, "odin_resolved_oper"), on=keys, how="left", nulls_equal=True
            )
            .with_columns(pl.col("odin_resolved_oper").fill_null("U"))
            .join(folded, on=keys, how="left", nulls_equal=True)
            .with_columns(pl.lit(self.history_snapshot, dtype=pl.String).alias("odin_snapshot"))
        )
        if "edw_inserted_dtm" in source.columns:
            source = source.with_columns(
                pl.coalesce(pl.col("edw_inserted_dtm").dt.strftime("%Y"), pl.lit("0"))
                .cast(pl.Int32)
                .alias("odin_year"),
                pl.coalesce(pl.col("edw_inserted_dtm").dt.strftime("%m"), pl.lit("0"))
                .cast(pl.Int32)
                .alias("odin_month"),
            )

        log.complete(merge_size=len(source))
        return source

    def _merge_predicate(self, keys: list[str], source: pl.DataFrame) -> str:
        """
        Build the MERGE match predicate (keys + optional partition constraint).

        Each key uses plain equality, except the null-safe ``OR (both NULL)`` form
        on keys that actually carry a null in the batch, required so a null key
        matches its target row instead of duplicating it.

        The "only when a null is present" part is not cosmetic. Under
        ``streamed_exec=False`` delta-rs rewrites each ``target.k = source.k`` into
        ``target.k BETWEEN min(source.k) AND max(source.k)`` and pushes it into file
        skipping, but it only recognises simple equality — the ``OR (both NULL)``
        form generalizes to nothing and that key stops contributing any pruning.
        Emitting it unconditionally would quietly give up the key-range skipping.
        """
        key_pred = " AND ".join(
            f'target."{k}" = source."{k}"'
            if source.get_column(k).null_count() == 0
            else f'(target."{k}" = source."{k}" OR (target."{k}" IS NULL AND source."{k}" IS NULL))'
            for k in keys
        )
        return key_pred + self._partition_constraint(source)

    def _partition_constraint(self, source: pl.DataFrame) -> str:
        """
        Return a partition-pruning clause for the merge, or '' when unsafe.

        Emits ` AND ((odin_year = Y1 AND odin_month = M1) OR ...)` naming the exact
        (year, month) partitions the source touches, restricting the scan to just
        those.
        """
        if "odin_year" not in source.columns or "odin_month" not in source.columns:
            return ""
        if source.get_column("edw_inserted_dtm").null_count() > 0:
            return ""
        pairs = (
            source.select("odin_year", "odin_month")
            .unique()
            .sort(["odin_year", "odin_month"])
            .rows()
        )
        if not pairs:
            return ""
        # Literals are cast to INT (Int32) to match the partition columns' type
        # exactly: bare integer literals parse as Int64, and while DataFusion's
        # planner coerces that, delta-rs also evaluates this predicate in strict
        # non-coercing paths (kernel data skipping, concurrent-commit conflict
        # checks) where an Int32/Int64 comparison is a hard error
        # ("Invalid comparison operation: Int32 <= Int64").
        pair_sql = " OR ".join(
            f'(target."odin_year" = CAST({y} AS INT) AND target."odin_month" = CAST({m} AS INT))'
            for y, m in pairs
        )
        return f" AND ({pair_sql})"

    def _partition_chunks(self, source: pl.DataFrame) -> list[pl.DataFrame]:
        """
        Split the merge source into one frame per (odin_year, odin_month) partition.

        Each chunk is merged on its own so delta-rs rewrites only one partition's
        files per MERGE (see _merge_apply). Non-dated tables lack the partition
        columns and merge as a single chunk; null-edw rows share the (0, 0) chunk,
        which _partition_constraint leaves unpruned.
        """
        if "odin_year" not in source.columns or "odin_month" not in source.columns:
            return [source]
        return source.partition_by(["odin_year", "odin_month"])

    def _merge_apply(self, source: pl.DataFrame, keys: list[str], watermark: str) -> dict:
        """
        Execute the MERGE of `source` into silver, one action per resolved op.

        odin_resolved_oper maps directly onto the MERGE branches:
          - "D": delete the matched row (unmatched: nothing to delete).
          - "I": replace the matched row verbatim / insert when unmatched.
          - "U": coalesce onto the matched row; unmatched U keys are orphan
            updates (no live row to patch) and fall through untouched.

        The source is merged one (odin_year, odin_month) partition at a time to
        bound peak memory. None of these commits records the CDC watermark — see
        _commit_watermark, which records it once, afterwards.
        """
        log = ProcessLog(
            "_merge_apply", table=self.table, watermark=watermark, merge_size=len(source)
        )
        assert self.silver is not None
        target_cols = self.silver.schema().to_arrow().names
        missing = set(keys) - set(target_cols)
        assert not missing, (
            f"primary key columns {sorted(missing)} absent from silver table for {self.table}"
        )

        # On partitioned tables, every "I"-resolved row (replaced or inserted
        # with verbatim partition values) must carry edw_inserted_dtm: without
        # it the row would land in the odin_year=0 partition, which
        # partition-pruned merge scans never revisit. "U" rows are exempt —
        # they keep the target's partition (see update_set below).
        if "odin_year" in source.columns:
            inserts = source.filter(pl.col("odin_resolved_oper") == "I")
            null_edw = inserts.get_column("edw_inserted_dtm").null_count()
            assert null_edw == 0, (
                f"{null_edw} insert-resolved CDC records for {self.table} carry a null "
                "edw_inserted_dtm; inserted rows would land in the odin_year=0 "
                "partition, which partition-pruned merges never revisit"
            )

        # "U" rows (sparse update): watermark columns verbatim from the
        # resolved CDC row; data columns coalesce so untouched target values
        # survive. Partition columns follow edw_inserted_dtm: when no CDC
        # record in the batch carried it for a key, source odin_year/odin_month
        # degrade to 0 while the coalesce keeps the target's edw_inserted_dtm —
        # so the target's partition values must be kept too, or the row would
        # silently move to the 0/0 partition and out of partition-pruned
        # query results.
        passthrough = {"odin_snapshot", "header__change_seq"}
        partition_cols = {"odin_year", "odin_month"}
        source_cols = set(source.columns)
        update_set: dict[str, str] = {}
        for col in target_cols:
            if col in keys or col not in source_cols:
                continue
            if col in passthrough:
                update_set[col] = f'source."{col}"'
            elif col in partition_cols:
                update_set[col] = (
                    'CASE WHEN source."edw_inserted_dtm" IS NOT NULL '
                    f'THEN source."{col}" ELSE target."{col}" END'
                )
            else:
                update_set[col] = f'COALESCE(source."{col}", target."{col}")'
        # "I" rows replace a matched row wholesale (delete-then-reinsert within
        # one batch, or a duplicate/replayed insert — the spec's idempotent
        # upsert): the fold is anchored at the insert image, so a NULL in the
        # source means NULL — coalescing against the target would resurrect
        # pre-reset values. Partition columns are safe verbatim per the assert
        # above.
        replace_set = {
            col: f'source."{col}"' for col in target_cols if col in source_cols and col not in keys
        }
        insert_set = {col: f'source."{col}"' for col in target_cols if col in source_cols}

        # Merge one partition at a time so delta-rs only rewrites a single
        # partition's files per MERGE, bounding peak memory to the largest single
        # partition rather than every touched partition's rewrite at once.
        dt = self.silver
        chunks = self._partition_chunks(source)
        totals: dict = {}
        for chunk in chunks:
            predicate = self._merge_predicate(keys, chunk)
            sigterm_check()
            log.add_metadata(step=f"merging_chunk:{len(chunk)}")
            merger = dt.merge(
                source=chunk.to_arrow(),
                predicate=predicate,
                source_alias="source",
                target_alias="target",
                error_on_type_mismatch=False,
                merge_schema=False,
                streamed_exec=False,
                writer_properties=DELTA_WRITER_PROPERTIES,
            )
            stats = (
                merger.when_matched_delete(predicate="source.odin_resolved_oper = 'D'")
                .when_matched_update(
                    predicate="source.odin_resolved_oper = 'I'", updates=replace_set
                )
                .when_matched_update(
                    predicate="source.odin_resolved_oper = 'U'", updates=update_set
                )
                .when_not_matched_insert(
                    predicate="source.odin_resolved_oper = 'I'", updates=insert_set
                )
                .execute()
            )
            for k, v in stats.items():
                totals[k] = totals.get(k, 0) + v if isinstance(v, (int, float)) else v
            log.add_metadata(step="reloading_table")
            # reload so the next chunk merges against the freshly committed state
            dt = DeltaTable(self.silver_uri)
        self.silver = dt

        log.complete()
        return totals


def schedule_delta_ods(schedule: sched.scheduler) -> None:
    """Schedule one CubicODSDelta job per Delta-enabled Cubic ODS table for this instance."""
    for table in CUBIC_ODS_DELTA_TABLES_INSTANCE:
        job = CubicODSDelta(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
