"""
Delta-history-based Cubic ODS silver-table materialization ("d2").

This is a parallel successor to ``generate/cubic/delta_ods.py``. Where that
"midpoint" job reads the legacy snapshot-partitioned Qlik *parquet* history via
DuckDB, this job reads the immutable Delta **bronze** history produced by
``ingestion/qlik/delta_archive.py`` (``CUBIC_QLIK_DELTA_DATA``) and materializes
the same current-state ("silver") table as a Delta Lake table via MERGE.

The read side is delta-rs, not DuckDB: the bronze table's per-file
``header__change_seq`` min/max stats (guaranteed by the bronze table's
``dataSkippingStatsColumns`` config) are read from the Delta log via
``get_add_actions()`` to select only the files covering the next CDC window —
metadata-based pruning with no backlog scan (delta_ods.md §10, roadmap item 3).
The write side is unchanged from the midpoint job (delta-rs overwrite for the
rebuild, ``DeltaTable.merge`` for CDC), as is the CDC resolution/merge logic —
so the property/semantic guarantees carry over verbatim.

It writes to separate output/status paths (``CUBIC_ODS_DELTA_D2_DATA`` /
``CUBIC_ODS_DELTA_D2_STATUS``) so it runs concurrently with the in-production
midpoint job during incremental rollout. It never mutates the bronze side.

Position on the bronze CDC stream is recorded in the silver table's own Delta
commit metadata (snapshot generation + max processed ``header__change_seq``),
identical to the midpoint job — tracking it there rather than deriving it from
surviving rows keeps a delete-only CDC batch from regressing the watermark.

Snapshot selection differs from the midpoint job in one way: the bronze history
appends a snapshot's LOAD group incrementally and only marks
``odin_load_complete`` when the whole group is present, so a half-loaded newest
snapshot must not be materialized. This job reads bronze's own commit metadata
to pick the newest *load-complete* snapshot (falling back to the prior snapshot
while the newest is mid-load); by the bronze job's design only the tail snapshot
can ever be incomplete, so every earlier snapshot partition is complete.

The source data is treated as untrusted: every run asserts the invariants it
depends on (required columns present, primary keys declared and present, CDC
records carry a change sequence, a load snapshot is non-empty, and — on
year/month-partitioned tables — load and insert images carry a non-null
``edw_inserted_dtm``). Violations raise rather than producing a corrupt silver
table.

Two Qlik Replicate behaviors are relied on without a runtime check (identical to
the midpoint job):
  - ``header__change_seq`` is unique per change record, so paging by a
    strictly-greater watermark can never split records sharing a sequence.
  - "I" records are full row images (only "U" records may be sparse), so each
    key's latest reset event (I or D) alone decides its action.

Steps per run:
  1. Find the newest load-complete bronze ``snapshot`` partition.
  2. If it differs from the silver table's ``odin_snapshot``, rebuild silver from
     that snapshot's "load" records (full overwrite).
  3. MERGE any CDC records (I/U/D) with ``header__change_seq`` greater than the
     silver watermark into the silver table, selecting the bronze files to read
     from the Delta log's per-file seq stats.
"""

import os
import sched
import time
from typing import Iterator

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads

from deltalake import CommitProperties
from deltalake import DeltaTable
from deltalake import write_deltalake
from deltalake.exceptions import SchemaMismatchError

from odin.job import OdinJob
from odin.job import job_proc_schedule
from odin.utils.aws.s3 import s3_file
from odin.utils.delta import open_delta
from odin.utils.delta import row_count as delta_row_count
from odin.utils.locations import CUBIC_ODS_DELTA_D2_DATA
from odin.utils.locations import CUBIC_ODS_DELTA_D2_STATUS
from odin.utils.locations import CUBIC_QLIK_DELTA_DATA
from odin.utils.locations import CUBIC_QLIK_PROCESSED
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.logger import MdValues
from odin.utils.logger import ProcessLog
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
from odin.ingestion.qlik.tables import CUBIC_ODS_DELTA_D2_TABLES_INSTANCE
from odin.ingestion.qlik.delta_archive import CHANGE_CLASS_CDC
from odin.ingestion.qlik.delta_archive import CHANGE_CLASS_LOAD
from odin.ingestion.qlik.delta_archive import HISTORY_SCAN_LIMIT as BRONZE_HISTORY_SCAN_LIMIT
from odin.ingestion.qlik.delta_archive import STATE_LOAD_COMPLETE_KEY as BRONZE_LOAD_COMPLETE_KEY
from odin.ingestion.qlik.delta_archive import STATE_SNAPSHOT_KEY as BRONZE_SNAPSHOT_KEY

NEXT_RUN_DEFAULT = 60 * 60 * 4  # 4 hours
NEXT_RUN_BETA = 60 * 15  # 15 minutes
NEXT_RUN_IMMEDIATE = 30  # 30 seconds
NEXT_RUN_LONG = 60 * 60 * 12  # 12 hours

REBUILD_BATCH_SIZE = 10_000
MAX_MERGE_RECORDS = 200_000

CDC_OPERS = ("I", "U", "D")

# Keys under which each silver Delta commit records the job's input position in
# its custom metadata (readable via DeltaTable.history()). This is the source of
# truth for "where the table is at", independent of the surviving row contents.
STATE_SNAPSHOT_KEY = "odin_snapshot"
STATE_WATERMARK_KEY = "odin_cdc_watermark"
INITIAL_WATERMARK = "0"  # header__change_seq is a zero-padded string; all seqs > "0"
HISTORY_SCAN_LIMIT = 50  # commits to scan back for the latest recorded position

# get_add_actions(flatten=True) column names for the bronze table. The stats
# columns exist because bronze sets delta.dataSkippingStatsColumns; partition
# columns are always present. Note the literal dots — these are flat column
# names, so they are looked up by name (never pc.field, which reads dots as
# nested struct access).
ADD_PATH = "path"
ADD_NUM_RECORDS = "num_records"
STAT_MIN_SEQ = "min.header__change_seq"
STAT_MAX_SEQ = "max.header__change_seq"
STAT_NULL_SEQ = "null_count.header__change_seq"
PART_SNAPSHOT = "partition.snapshot"
PART_CHANGE_CLASS = "partition.odin_change_class"

# Columns required to be present in the bronze history for the job to run.
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
    "odin_change_class",
)


def _default_run_interval() -> int:
    """Return the normal rerun interval for the active instance."""
    return NEXT_RUN_BETA if _ODIN_INSTANCE == "beta" else NEXT_RUN_DEFAULT


def _long_run_interval() -> int:
    """Return the no-new-data rerun interval for the active instance."""
    return NEXT_RUN_BETA if _ODIN_INSTANCE == "beta" else NEXT_RUN_LONG


class NoQlikHistoryError(Exception):
    """No load-complete Qlik bronze snapshots are available to process."""


class CDCSchemaIncompatibleError(Exception):
    """A non-additive schema change was detected; the pipeline cannot proceed."""


class CubicODSDeltaD2(OdinJob):
    """Materialize one Cubic ODS table as a Delta silver table from the Delta bronze history."""

    def __init__(self, table: str) -> None:
        """Create CubicODSDeltaD2 instance for `table`."""
        self.table = table
        self.bronze_uri = s3_file(os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DELTA_DATA, table))
        self.silver_uri = s3_file(os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_DELTA_D2_DATA, table))
        self.start_kwargs = {"table": table}
        self.silver: DeltaTable | None = None
        self.bronze: DeltaTable | None = None
        self.history_columns: list[str] = []
        self.history_snapshot = ""
        self.part_columns: list[str] = []
        self._add_cache: pa.Table | None = None
        self._dataset_schema: pa.Schema | None = None
        # Post-merge position, published by _write_status. _merge_cdc advances the
        # watermark; both stay at their defaults on the paths where it does no work.
        self.cdc_watermark = INITIAL_WATERMARK
        self.more_pending = False
        self._run_started: float | None = None

    def run(self) -> int:
        """Materialize the latest snapshot + CDC into silver; return seconds to next run."""
        self.start_kwargs = {"table": self.table}
        self._run_started = time.perf_counter()
        self._add_cache = None

        self.silver = open_delta(self.silver_uri)
        self.bronze = open_delta(self.bronze_uri)
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

    # ------------------------------------------------------------------
    # Bronze metadata access
    # ------------------------------------------------------------------

    def _add_actions(self) -> pa.Table:
        """Return (and cache for the run) the bronze table's flattened add actions."""
        if self._add_cache is None:
            assert self.bronze is not None
            self._add_cache = pa.table(self.bronze.get_add_actions(flatten=True))
        return self._add_cache

    def _bronze_load_status(self) -> tuple[str, bool]:
        """
        Return (snapshot, load_complete) from bronze's latest recorded position.

        Reads the bronze table's own commit custom metadata (the same keys the
        bronze job writes). A bronze table with no recorded position reads as
        ("", False).
        """
        assert self.bronze is not None
        for commit in self.bronze.history(BRONZE_HISTORY_SCAN_LIMIT):
            if BRONZE_SNAPSHOT_KEY in commit:
                return commit[BRONZE_SNAPSHOT_KEY], commit.get(BRONZE_LOAD_COMPLETE_KEY) == "true"
        return "", False

    def _file_uri(self, rel_path: str) -> str:
        """Join a bronze add-action relative path onto the table root for reading."""
        assert self.bronze is not None
        root = self.bronze.table_uri
        # pyarrow's dataset reader wants a bare local path, but keeps s3:// URIs.
        if root.startswith("file://"):
            root = root[len("file://") :]
        return root.rstrip("/") + "/" + rel_path

    # ------------------------------------------------------------------
    # Silver position (own commit metadata)
    # ------------------------------------------------------------------

    def _read_state(self) -> tuple[str, str]:
        """
        Return (snapshot, cdc_watermark) from the latest commit that recorded them.

        A silver table with no recorded position (never built, or built by an
        older version) reads as ("", INITIAL_WATERMARK), which forces a rebuild.
        """
        if self.silver is None:
            return "", INITIAL_WATERMARK
        for commit in self.silver.history(HISTORY_SCAN_LIMIT):
            if STATE_SNAPSHOT_KEY in commit:
                return commit[STATE_SNAPSHOT_KEY], commit.get(
                    STATE_WATERMARK_KEY, INITIAL_WATERMARK
                )
        return "", INITIAL_WATERMARK

    def _commit_state(self, watermark: str) -> CommitProperties:
        """Commit metadata recording the current snapshot and CDC watermark."""
        return CommitProperties(
            custom_metadata={
                STATE_SNAPSHOT_KEY: self.history_snapshot,
                STATE_WATERMARK_KEY: watermark,
            }
        )

    # ------------------------------------------------------------------
    # Snapshot discovery
    # ------------------------------------------------------------------

    def _bronze_snapshots(self) -> list[str]:
        """Return the sorted distinct ``snapshot`` partition values present in bronze."""
        add = self._add_actions()
        if add.num_rows == 0 or PART_SNAPSHOT not in add.schema.names:
            return []
        values = pc.unique(pc.drop_null(add[PART_SNAPSHOT])).to_pylist()
        return sorted(values)

    def _snapshot_check(self) -> None:
        """Pick the newest load-complete bronze snapshot and validate its schema."""
        if self.bronze is None:
            raise NoQlikHistoryError(f"No Delta bronze history available for {self.table}.")

        snapshots = self._bronze_snapshots()
        if not snapshots:
            raise NoQlikHistoryError(f"No bronze snapshot partitions available for {self.table}.")

        # Bronze marks only its current (tail) snapshot as possibly-incomplete;
        # every earlier partition is fully loaded by construction. Use bronze's
        # authoritative signal to avoid rebuilding from a half-loaded snapshot.
        bronze_snapshot, load_complete = self._bronze_load_status()
        if bronze_snapshot and not load_complete:
            earlier = [s for s in snapshots if s < bronze_snapshot]
            if not earlier:
                raise NoQlikHistoryError(
                    f"newest bronze snapshot {bronze_snapshot} for {self.table} is mid-load "
                    "and no earlier complete snapshot exists yet"
                )
            self.history_snapshot = earlier[-1]
        else:
            self.history_snapshot = snapshots[-1]

        assert RE_SNAPSHOT_TS.fullmatch(self.history_snapshot), (
            f"unexpected snapshot partition name for {self.table}: {self.history_snapshot!r}"
        )

        self._dataset_schema = self.bronze.to_pyarrow_dataset().schema
        self.history_columns = list(self._dataset_schema.names)

        missing = set(REQUIRED_HISTORY_COLUMNS) - set(self.history_columns)
        assert not missing, (
            f"bronze history for {self.table} is missing required columns: {sorted(missing)}"
        )
        self.part_columns = (
            ["odin_year", "odin_month"] if "edw_inserted_dtm" in self.history_columns else []
        )
        ProcessLog(
            "delta_d2_snapshot_check",
            table=self.table,
            history_snapshot=self.history_snapshot,
            bronze_snapshot=bronze_snapshot,
            bronze_load_complete=load_complete,
            snapshots_available=len(snapshots),
            partition_columns=self.part_columns,
        ).complete()

    # ------------------------------------------------------------------
    # Snapshot rebuild (silver overwrite from "load" records)
    # ------------------------------------------------------------------

    def _load_dataset(self) -> pads.Dataset:
        """Return a pyarrow dataset over the current snapshot's bronze load partition."""
        assert self.bronze is not None
        return self.bronze.to_pyarrow_dataset(
            partitions=[
                ("snapshot", "=", self.history_snapshot),
                ("odin_change_class", "=", CHANGE_CLASS_LOAD),
            ]
        )

    def _rebuild_silver(self) -> None:
        """Overwrite silver with the "L" (load) records of the current snapshot."""
        log = ProcessLog(
            "delta_d2_rebuild_silver", table=self.table, snapshot=self.history_snapshot
        )
        ds = self._load_dataset()

        # Validate the load records BEFORE the overwrite: once write_deltalake
        # commits, silver's contents and recorded snapshot have already advanced,
        # so a post-write failure would leave a wedged (empty or mispartitioned)
        # table that the next run no longer knows to rebuild.
        self._check_load_records(ds)

        data_cols = [c for c in self.history_columns if c not in META_DROP_COLUMNS]
        out_schema = self._silver_schema(ds.schema, data_cols)

        sigterm_check()
        reader = pa.RecordBatchReader.from_batches(
            out_schema, self._rebuild_batches(ds, data_cols, out_schema)
        )
        write_deltalake(
            self.silver_uri,
            reader,
            mode="overwrite",
            schema_mode="overwrite",
            partition_by=self.part_columns or None,
            commit_properties=self._commit_state(INITIAL_WATERMARK),
        )

        self.silver = DeltaTable(self.silver_uri)
        log.complete(rows_loaded=delta_row_count(self.silver))

    def _check_load_records(self, ds: pads.Dataset) -> None:
        """Assert the snapshot's "L" records can produce a valid silver table."""
        is_load = pc.field("header__change_oper") == "L"
        total = ds.count_rows(filter=is_load)
        assert total > 0, (
            f"snapshot {self.history_snapshot} for {self.table} has no L (load) records"
        )
        if self.part_columns:
            null_edw = ds.count_rows(filter=is_load & pc.field("edw_inserted_dtm").is_null())
            assert null_edw == 0, (
                f"snapshot {self.history_snapshot} for {self.table} has {null_edw} L (load) "
                "records with a null edw_inserted_dtm; rows would land in the odin_year=0 "
                "partition, which partition-pruned merges never revisit"
            )

    def _silver_schema(self, ds_schema: pa.Schema, data_cols: list[str]) -> pa.Schema:
        """Derive the silver arrow schema by transforming an empty load frame."""
        proj = pa.schema([ds_schema.field(c) for c in data_cols])
        empty = pl.from_arrow(proj.empty_table())
        if isinstance(empty, pl.Series):
            raise TypeError("Always dataframe.")
        return self._materialize_load(empty).to_arrow().schema

    def _rebuild_batches(
        self, ds: pads.Dataset, data_cols: list[str], out_schema: pa.Schema
    ) -> Iterator[pa.RecordBatch]:
        """Stream the snapshot's L records through the silver transform, batch by batch."""
        scanner = ds.scanner(
            columns=list({*data_cols, "header__change_oper"}),
            filter=pc.field("header__change_oper") == "L",
            batch_size=REBUILD_BATCH_SIZE,
        )
        for batch in scanner.to_batches():
            if batch.num_rows == 0:
                continue
            frame = pl.from_arrow(pa.Table.from_batches([batch]))
            if isinstance(frame, pl.Series):
                raise TypeError("Always dataframe.")
            table = self._materialize_load(frame).to_arrow().cast(out_schema)
            yield from table.to_batches()

    def _materialize_load(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Project a bronze load frame to silver columns (drop metadata, add odin_* cols)."""
        data_cols = [c for c in self.history_columns if c not in META_DROP_COLUMNS]
        frame = frame.select(data_cols).with_columns(
            pl.lit(self.history_snapshot, dtype=pl.String).alias("odin_snapshot")
        )
        if self.part_columns:
            frame = frame.with_columns(
                pl.coalesce(pl.col("edw_inserted_dtm").dt.strftime("%Y"), pl.lit("0"))
                .cast(pl.Int32)
                .alias("odin_year"),
                pl.coalesce(pl.col("edw_inserted_dtm").dt.strftime("%m"), pl.lit("0"))
                .cast(pl.Int32)
                .alias("odin_month"),
            )
        return frame

    # ------------------------------------------------------------------
    # CDC MERGE (silver update from I/U/D records)
    # ------------------------------------------------------------------

    def _history_max_seq(self) -> str | None:
        """Return the newest header__change_seq in the snapshot's CDC files (metadata only)."""
        add = self._add_actions()
        if add.num_rows == 0 or STAT_MAX_SEQ not in add.schema.names:
            return None
        mask = pc.and_(
            pc.equal(add[PART_SNAPSHOT], self.history_snapshot),
            pc.equal(add[PART_CHANGE_CLASS], CHANGE_CLASS_CDC),
        )
        cdc = add.filter(mask)
        if cdc.num_rows == 0:
            return None
        maxes = pc.drop_null(cdc[STAT_MAX_SEQ])
        if len(maxes) == 0:
            return None
        return str(pc.max(maxes).as_py())

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
            ProcessLog("delta_d2_history_max_seq", table=self.table).failed(exception)
            history_max_seq = None
        seq_dt = seq_as_datetime(self.cdc_watermark)
        history_seq_dt = seq_as_datetime(history_max_seq)
        seq_lag = lag_seconds(history_seq_dt, seq_dt)
        row_count = None if self.silver is None else delta_row_count(self.silver)
        prev = read_status(CUBIC_ODS_DELTA_D2_STATUS, self.table, self.scratch)
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
        publish_status(CUBIC_ODS_DELTA_D2_STATUS, self.table, self.scratch, status)

    def _merge_cdc(self, after_seq: str) -> int:
        """Apply CDC records with seq > `after_seq` to silver; return the next-run interval."""
        self.cdc_watermark = after_seq
        if self.silver is None:
            return _long_run_interval()
        log = ProcessLog("delta_merge_cdc", table=self.table)

        cdc_df, more_pending = self._read_cdc(after_seq, limit=MAX_MERGE_RECORDS)
        if cdc_df.height == 0:
            log.complete(cdc_records_found=0)
            return _long_run_interval()

        assert cdc_df.get_column("header__change_seq").null_count() == 0, (
            f"CDC records for {self.table} contain a null header__change_seq"
        )
        max_seq = cdc_df.get_column("header__change_seq").max()
        assert max_seq, f"No valid header__change_seq (.max() => {max_seq})"
        max_seq_processed = str(max_seq)

        keys = self._discover_keys(cdc_df)
        source = self._build_merge_source(cdc_df, keys)
        try:
            metrics = self._merge_apply(source, keys, max_seq_processed)
        except SchemaMismatchError as exc:
            raise CDCSchemaIncompatibleError(
                f"silver MERGE failed for {self.table}: {exc}"
            ) from exc

        self.silver = DeltaTable(self.silver_uri)
        self.cdc_watermark = max_seq_processed
        self.more_pending = more_pending

        log.complete(
            cdc_records_processed=cdc_df.height,
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

    def _read_cdc(self, after_seq: str, limit: int) -> tuple[pl.DataFrame, bool]:
        """
        Read the next batch of CDC (I/U/D) records with seq > `after_seq`.

        File selection is metadata-only: the bronze Delta log's per-file
        ``header__change_seq`` min/max stats pick just the files that can hold
        records past the watermark (`_cdc_window_files`), so no backlog is
        scanned. The selected files are read in full and bounded in memory to the
        window's ceiling seq — reading the whole ``<= ceiling`` range (no LIMIT
        split) keeps records sharing the boundary seq in one batch, so the
        advancing watermark can never strand tied rows.

        The null-seq arm surfaces any null-seq record (a data error) into the
        batch, where the null_count assertion in _merge_cdc rejects it rather
        than skipping it forever.

        :return: (batch frame, whether more records remain past this batch)
        """
        log = ProcessLog(process="_read_cdc", table=self.table)
        selected, more_files = self._cdc_window_files(after_seq, limit)
        if not selected:
            log.complete(files=0)
            return pl.DataFrame(), False

        frame = self._read_files([f["path"] for f in selected])
        seq = pl.col("header__change_seq")
        frame = frame.filter(
            pl.col("header__change_oper").is_in(CDC_OPERS)
            & ((seq > pl.lit(after_seq)) | seq.is_null())
        )

        # Bound to exactly `limit` non-null seqs (plus their ties, but seqs are
        # unique) by cutting at the k-th smallest seq; null-seq rows always ride
        # along so the merge assertion can reject them.
        truncated = False
        non_null = frame.filter(seq.is_not_null())
        if non_null.height > limit:
            ceiling = non_null.get_column("header__change_seq").sort()[limit - 1]
            frame = frame.filter((seq <= pl.lit(ceiling)) | seq.is_null())
            truncated = True

        log.complete(
            files=len(selected),
            read_rows=frame.height,
            more_files=more_files,
            truncated=truncated,
        )
        return frame, (more_files or truncated)

    def _cdc_window_files(self, after_seq: str, limit: int) -> tuple[list[dict], bool]:
        """
        Select the bronze CDC files covering the next window from Delta log stats.

        Candidate = a CDC file of the current snapshot whose max seq is past the
        watermark (or whose seq stats are absent / it carries null-seq rows —
        "unknown means must read"). Candidates are ordered by min seq and a
        prefix is taken until its cumulative record count reaches `limit`; the
        prefix is then extended to include any candidate whose range overlaps the
        prefix's ceiling, so the ``<= ceiling`` window is read whole (tie-safe on
        overlapping file ranges). `more` is True when candidates remain unread.

        :return: (selected file stat dicts, whether unread candidates remain)
        """
        add = self._add_actions()
        names = set(add.schema.names)
        have_stats = STAT_MIN_SEQ in names and STAT_MAX_SEQ in names

        rows: list[dict] = []
        for i in range(add.num_rows):
            if add[PART_SNAPSHOT][i].as_py() != self.history_snapshot:
                continue
            if add[PART_CHANGE_CLASS][i].as_py() != CHANGE_CLASS_CDC:
                continue
            rows.append(
                {
                    "path": self._file_uri(add[ADD_PATH][i].as_py()),
                    "min": add[STAT_MIN_SEQ][i].as_py() if have_stats else None,
                    "max": add[STAT_MAX_SEQ][i].as_py() if have_stats else None,
                    "nulls": add[STAT_NULL_SEQ][i].as_py() if STAT_NULL_SEQ in names else None,
                    "num": add[ADD_NUM_RECORDS][i].as_py() or 0,
                }
            )

        def candidate(r: dict) -> bool:
            if not have_stats or r["max"] is None:
                return True  # unknown seq range → must read
            if r["max"] > after_seq:
                return True
            return bool(r["nulls"])  # holds null-seq rows to surface

        cands = [r for r in rows if candidate(r)]
        # min-seq order (nulls first), path as a deterministic tiebreaker.
        cands.sort(key=lambda r: (r["min"] is not None, r["min"] or "", r["path"]))

        selected: list[dict] = []
        cumulative = 0
        for r in cands:
            selected.append(r)
            cumulative += r["num"]
            if cumulative >= limit:
                break

        ceiling = max((r["max"] for r in selected if r["max"] is not None), default=None)
        if ceiling is not None:
            chosen = {r["path"] for r in selected}
            for r in cands:
                if r["path"] in chosen:
                    continue
                if r["min"] is not None and r["min"] <= ceiling:
                    selected.append(r)
                    chosen.add(r["path"])

        more_files = len(selected) < len(cands)
        return selected, more_files

    def _read_files(self, paths: list[str]) -> pl.DataFrame:
        """Read the given bronze parquet files into one frame under the bronze schema."""
        ds = pads.dataset(paths, format="parquet", partitioning="hive", schema=self._dataset_schema)
        frame = pl.from_arrow(ds.to_table())
        if isinstance(frame, pl.Series):
            raise TypeError("Always dataframe.")
        return frame

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

        Each key uses plain equality when the source column carries no nulls:
        with ``streamed_exec=False``, delta-rs derives an early-pruning predicate
        from the source key min/max stats and skips target files whose key range
        can't match — but only for simple equality conjunctions. The null-safe
        ``OR (both NULL)`` form defeats that analysis, so it is emitted per key
        and only when a null key value is actually present in the batch.
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

        ` AND target.odin_year IN (...) AND target.odin_month IN (...)` restricts the
        scan to the partitions the source touches. If any edw_inserted_dtm
        is missing  we fall back to an unpruned full scan.
        """
        if "odin_year" not in source.columns or "odin_month" not in source.columns:
            return ""
        if source.get_column("edw_inserted_dtm").null_count() > 0:
            return ""
        years = sorted(source.get_column("odin_year").unique().to_list())
        months = sorted(source.get_column("odin_month").unique().to_list())
        if not years or not months:
            return ""
        # Literals are cast to INT (Int32) to match the partition columns' type
        # exactly: bare integer literals parse as Int64, and while DataFusion's
        # planner coerces that, delta-rs also evaluates this predicate in strict
        # non-coercing paths (kernel data skipping, concurrent-commit conflict
        # checks) where an Int32/Int64 comparison is a hard error
        # ("Invalid comparison operation: Int32 <= Int64").
        years_sql = ", ".join(f"CAST({y} AS INT)" for y in years)
        months_sql = ", ".join(f"CAST({m} AS INT)" for m in months)
        return f' AND target."odin_year" IN ({years_sql}) AND target."odin_month" IN ({months_sql})'

    def _merge_apply(self, source: pl.DataFrame, keys: list[str], watermark: str) -> dict:
        """
        Execute the MERGE of `source` into silver, one action per resolved op.

        odin_resolved_oper maps directly onto the MERGE branches:
          - "D": delete the matched row (unmatched: nothing to delete).
          - "I": replace the matched row verbatim / insert when unmatched.
          - "U": coalesce onto the matched row; unmatched U keys are orphan
            updates (no live row to patch) and fall through untouched.
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

        predicate = self._merge_predicate(keys, source)

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

        sigterm_check()
        merger = self.silver.merge(
            source=source.to_arrow(),
            predicate=predicate,
            source_alias="source",
            target_alias="target",
            error_on_type_mismatch=False,
            merge_schema=False,
            commit_properties=self._commit_state(watermark),
            streamed_exec=False,
        )
        result_stats = (
            merger.when_matched_delete(predicate="source.odin_resolved_oper = 'D'")
            .when_matched_update(predicate="source.odin_resolved_oper = 'I'", updates=replace_set)
            .when_matched_update(predicate="source.odin_resolved_oper = 'U'", updates=update_set)
            .when_not_matched_insert(
                predicate="source.odin_resolved_oper = 'I'", updates=insert_set
            )
            .execute()
        )

        log.complete()
        return result_stats


def schedule_delta_ods_d2(schedule: sched.scheduler) -> None:
    """Schedule one CubicODSDeltaD2 job per d2-enabled Cubic ODS table for this instance."""
    for table in CUBIC_ODS_DELTA_D2_TABLES_INSTANCE:
        job = CubicODSDeltaD2(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
