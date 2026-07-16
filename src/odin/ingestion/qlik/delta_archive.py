"""
Delta-based Cubic Qlik bronze archive.

Parallel alternative to ``cubic_archive.py``: reads the same raw Qlik
Replicate CSV files and appends them to an immutable Delta Lake "bronze"
event-log table (L + I/U/D records, partitioned by ``snapshot`` and
``odin_change_class``), instead of the custom-parquet snapshot history.

It is strictly read-only on the source side: it never moves, deletes, or
rewrites any raw file, so it runs concurrently with ``cubic_archive.py``
(which owns moving consumed files to the ``processed/`` prefix) and with the
DMAP pipeline (a read-only consumer of ``processed/``). Because the legacy
mover's progress is uncorrelated with this job's, a file's presence in any
location carries no "already ingested" signal. Instead:

  - Files are identified by a *canonical key* (object key with the
    ``processed/`` prefix stripped — ``rename_objects`` preserves keys) and
    deduped across locations. Reads use the listed path only; a file the
    legacy job moves mid-run fails the read and the run, and the scheduler's
    retry finds it at its processed location. DFM files move together with
    their CSVs, so each is read from its CSV's location and a miss fails
    the run the same way.
  - Incoming locations are listed before ``processed/``. A legacy move is
    copy-then-delete, so with S3's strong consistency a file absent from the
    incoming listing was already copied to ``processed/`` before that listing
    began and must appear in the later ``processed/`` listing.
  - The job's position is a set of watermarks stored in Delta commit custom
    metadata, committed atomically with the rows they describe. A CDC file
    whose filename timestamp is at or below the watermark is never re-read;
    genuinely late-arriving files are deliberately ignored, matching the
    effective behavior of both existing consumers (see delta_archive.md §3).

See delta_archive.md for the full design and the accepted trade-offs.
"""

import gzip
import os
import sched
import shutil
import tempfile
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import NamedTuple

import polars as pl

from deltalake import CommitProperties
from deltalake import DeltaTable
from deltalake import write_deltalake

from odin.job import OdinJob
from odin.job import job_proc_schedule
from odin.utils.aws.s3 import S3Object
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import s3_file
from odin.utils.aws.s3 import s3_folder
from odin.utils.aws.s3 import stream_object
from odin.utils.delta import open_delta
from odin.utils.locations import CUBIC_QLIK_DELTA_DATA
from odin.utils.locations import CUBIC_QLIK_PROCESSED
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import DATA_ERROR
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import IN_QLIK_PREFIX
from odin.utils.logger import ProcessLog
from odin.utils.runtime import sigterm_check
from odin.utils.runtime import thread_cpus
from odin.ingestion.qlik.dfm import dfm_from_s3
from odin.ingestion.qlik.dfm import dfm_snapshot_dt
from odin.ingestion.qlik.dfm import dfm_to_polars_schema
from odin.ingestion.qlik.tables import _ODIN_INSTANCE
from odin.ingestion.qlik.tables import CUBIC_QLIK_BRONZE_TABLES_INSTANCE
from odin.ingestion.qlik.utils import RE_CDC_TS
from odin.ingestion.qlik.utils import RE_SNAPSHOT_TS
from odin.ingestion.qlik.utils import RecentSnapshotError
from odin.ingestion.qlik.utils import SNAPSHOT_FMT
from odin.ingestion.qlik.utils import re_get_first

NEXT_RUN_DEFAULT = 60 * 60 * 4  # 4 hours
NEXT_RUN_BETA = 60 * 15  # 15 minutes
NEXT_RUN_IMMEDIATE = 60 * 5  # 5 minutes
NEXT_RUN_LONG = 60 * 60 * 12  # 12 hours

# A snapshot whose LOAD files are still being written must not be ingested;
# mirror cubic_archive.py's guard window.
RECENT_SNAPSHOT_HOURS = 6

# Per-run and per-append-commit work bounds. Sizes are compressed (from the
# S3 listing) so batches are decided before any download; ~5x expansion makes
# these comparable to the legacy job's 1GB-uncompressed batch / 5GB-run caps.
MAX_CDC_FILES = 10_000
CDC_BATCH_BYTES = 256 * 1024 * 1024
RUN_CDC_BYTES = 5 * CDC_BATCH_BYTES
LOAD_BATCH_FILES = 20
LOAD_BATCH_BYTES = 256 * 1024 * 1024

# Bronze partition columns: the Qlik snapshot generation and a load-vs-cdc
# discriminator (silver's two read shapes: "all L records" / "CDC past seq").
PARTITION_COLUMNS = ["snapshot", "odin_change_class"]
CHANGE_CLASS_LOAD = "load"
CHANGE_CLASS_CDC = "cdc"

# Keys under which each Delta commit records the job's input position in its
# custom metadata (readable via DeltaTable.history()). Committed atomically
# with the rows they describe; the source of truth for "where the job is at".
STATE_SNAPSHOT_KEY = "odin_snapshot"
STATE_LOAD_COMPLETE_KEY = "odin_load_complete"
STATE_LOAD_WATERMARK_KEY = "odin_load_watermark"
STATE_CDC_WATERMARK_KEY = "odin_cdc_watermark"
HISTORY_SCAN_LIMIT = 50  # commits to scan back for the latest recorded position

# Guaranteed file-level stats for the CDC watermark column regardless of how
# wide the table is (delta-rs defaults to the first 32 columns only).
BRONZE_TABLE_CONFIG = {"delta.dataSkippingStatsColumns": "header__change_seq"}


class NoQlikLoadFilesError(Exception):
    """No Qlik LOAD files are available to bootstrap the bronze table from."""


class BronzeStateError(Exception):
    """The bronze table exists but carries no recorded position."""


class BronzeState(NamedTuple):
    """The job's recorded position on the raw file stream."""

    snapshot: str  # snapshot generation being ingested ("" = never bootstrapped)
    load_complete: bool  # snapshot's LOAD group fully appended
    load_watermark: str  # basename of last LOAD file appended (resume point)
    cdc_watermark: str  # max CDC filename timestamp (YYYYMMDD-HHMMSSXXX) ingested


class RawFile(NamedTuple):
    """A raw Qlik file found in one of the monitored S3 locations."""

    canonical: str  # object key with bucket and processed/ prefix stripped
    path: str  # full path the file was listed at (s3://bucket/key)
    size_bytes: int
    last_modified: datetime
    incoming: bool  # listed in an incoming location (mtime = Qlik write time)

    @property
    def basename(self) -> str:
        """Return the file's basename."""
        return os.path.basename(self.canonical)


def _default_run_interval() -> int:
    """Return the normal rerun interval for the active instance."""
    return NEXT_RUN_BETA if _ODIN_INSTANCE == "beta" else NEXT_RUN_DEFAULT


def _long_run_interval() -> int:
    """Return the no-new-data rerun interval for the active instance."""
    return NEXT_RUN_BETA if _ODIN_INSTANCE == "beta" else NEXT_RUN_LONG


def canonical_key(path: str) -> str:
    """
    Return the canonical object key for a raw file path.

    The bucket and (when present) the legacy mover's processed/ prefix are
    stripped, so the same file yields the same key wherever it is listed:
    cubic_archive.py's rename_objects prepends CUBIC_QLIK_PROCESSED to the
    original key, preserving it, for both archive- and error-bucket files.
    """
    key = path.replace("s3://", "").split("/", 1)[-1]
    processed_prefix = f"{CUBIC_QLIK_PROCESSED}/"
    if key.startswith(processed_prefix):
        key = key[len(processed_prefix) :]
    return key


def _cdc_ts(path: str) -> str:
    """Return the CDC filename timestamp (YYYYMMDD-HHMMSSXXX) of a file path."""
    return re_get_first(os.path.basename(path), RE_CDC_TS)


def _cdc_ts_as_snapshot(ts: str) -> str:
    """Convert a CDC filename timestamp to SNAPSHOT_FMT for snapshot comparison."""
    return f"{ts[:8]}T{ts[9:15]}Z"


class CubicQlikBronze(OdinJob):
    """Ingest one Cubic Qlik table's raw files into a Delta bronze table."""

    def __init__(self, table: str) -> None:
        """Create CubicQlikBronze instance for `table`."""
        self.table = table
        self.bronze_uri = s3_file(os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DELTA_DATA, table))
        self.start_kwargs = {"table": table}
        self.bronze: DeltaTable | None = None

    def run(self) -> int:
        """Ingest pending LOAD groups then CDC files; return seconds to next run."""
        self.start_kwargs = {"table": self.table}
        self.bronze = open_delta(self.bronze_uri)
        state = self._read_state()
        try:
            state = self._load_snapshots(state)
        except RecentSnapshotError as exception:
            # Not an error: a snapshot is mid-upload. Pause ALL ingestion (CDC
            # included) until it settles, exactly like the legacy job — this
            # also keeps new-snapshot CDC from being ingested before its LOAD.
            self.start_kwargs["skipped_recent_snapshot"] = "true"
            ProcessLog("CubicQlikBronze", table=self.table).failed(exception=exception)
            return _default_run_interval()
        if state.snapshot == "":
            raise NoQlikLoadFilesError(f"no Qlik LOAD files found anywhere for {self.table}")
        next_run = self._ingest_cdc(state)
        self.start_kwargs.update(
            {"snapshot": state.snapshot, "cdc_watermark": state.cdc_watermark}
        )
        return next_run

    # ------------------------------------------------------------------
    # Position tracking (Delta commit custom metadata)
    # ------------------------------------------------------------------

    def _read_state(self) -> BronzeState:
        """
        Return the recorded position from the latest commit that carries one.

        A missing table reads as the bootstrap state. A table without any
        recorded position was not built by this job; refusing to guess
        prevents appending a duplicate snapshot into it.
        """
        if self.bronze is None:
            return BronzeState("", False, "", "")
        for commit in self.bronze.history(HISTORY_SCAN_LIMIT):
            if STATE_SNAPSHOT_KEY in commit:
                return BronzeState(
                    snapshot=commit[STATE_SNAPSHOT_KEY],
                    load_complete=commit.get(STATE_LOAD_COMPLETE_KEY) == "true",
                    load_watermark=commit.get(STATE_LOAD_WATERMARK_KEY, ""),
                    cdc_watermark=commit.get(STATE_CDC_WATERMARK_KEY, ""),
                )
        raise BronzeStateError(
            f"bronze table for {self.table} exists but records no position; "
            "refusing to ingest into a table this job did not build"
        )

    def _append(self, frame: pl.DataFrame, state: BronzeState) -> None:
        """Append `frame` to bronze, committing `state` atomically with it."""
        commit_properties = CommitProperties(
            custom_metadata={
                STATE_SNAPSHOT_KEY: state.snapshot,
                STATE_LOAD_COMPLETE_KEY: "true" if state.load_complete else "false",
                STATE_LOAD_WATERMARK_KEY: state.load_watermark,
                STATE_CDC_WATERMARK_KEY: state.cdc_watermark,
            }
        )
        sigterm_check()
        write_deltalake(
            self.bronze_uri,
            frame.to_arrow(),
            mode="append",
            schema_mode="merge",
            partition_by=PARTITION_COLUMNS,
            commit_properties=commit_properties,
            configuration=BRONZE_TABLE_CONFIG if self.bronze is None else None,
        )
        self.bronze = DeltaTable(self.bronze_uri)

    # ------------------------------------------------------------------
    # LOAD (snapshot) ingestion
    # ------------------------------------------------------------------

    def _load_snapshots(self, state: BronzeState) -> BronzeState:
        """Discover and ingest the newest LOAD group when pending; return advanced state."""
        pending = self._pending_group(state, self._find_load_groups())
        if pending is not None:
            state = self._ingest_load_group(state, *pending)
        return state

    def _find_load_groups(self) -> dict[str, list[RawFile]]:
        """
        Find LOAD csv.gz files in every monitored location, grouped by snapshot.

        Grouping key is the ``snapshot=`` component of the file's canonical
        key (stable across moves). Files are deduped on canonical key with
        incoming listings taking precedence (their mtime is the Qlik write
        time, which the recency guard needs). Raises RecentSnapshotError when
        any incoming LOAD file is younger than RECENT_SNAPSHOT_HOURS: the
        snapshot may still be uploading. processed/ copies are exempt — their
        mtime is the move time, and legacy only moves fully-consumed files.
        """
        prefixes = (
            (s3_folder(os.path.join(DATA_ARCHIVE, IN_QLIK_PREFIX, self.table)), True),
            (s3_folder(os.path.join(DATA_ERROR, IN_QLIK_PREFIX, self.table)), True),
            (
                s3_folder(
                    os.path.join(DATA_ARCHIVE, CUBIC_QLIK_PROCESSED, IN_QLIK_PREFIX, self.table)
                ),
                False,
            ),
        )
        seen: dict[str, RawFile] = {}
        for prefix, incoming in prefixes:
            for obj in list_objects(prefix, in_filter="LOAD"):
                if not obj.path.endswith(".csv.gz"):
                    continue
                canonical = canonical_key(obj.path)
                if canonical in seen:
                    continue
                seen[canonical] = RawFile(
                    canonical, obj.path, obj.size_bytes, obj.last_modified, incoming
                )

        recent_floor = datetime.now(tz=UTC) - timedelta(hours=RECENT_SNAPSHOT_HOURS)
        groups: dict[str, list[RawFile]] = {}
        for file in seen.values():
            if file.incoming and file.last_modified > recent_floor:
                raise RecentSnapshotError(f"{file.path} modified within the last 6 hours.")
            groups.setdefault(re_get_first(file.canonical, RE_SNAPSHOT_TS), []).append(file)
        for files in groups.values():
            files.sort(key=lambda f: f.basename)
        return groups

    def _pending_group(
        self, state: BronzeState, groups: dict[str, list[RawFile]]
    ) -> tuple[str, list[RawFile]] | None:
        """
        Return the newest LOAD group as (snapshot, files) when it needs ingesting.

        Only the newest snapshot is ever ingested: its LOAD is the full table
        state as of its timestamp, superseding every older snapshot and that
        snapshot's CDC — which fact construction (latest-partition-only, both
        legacy and Delta) never reads. The partition value is the group's
        first-file DFM startWriteTimestamp (legacy-identical, so partitions
        line up with the parquet history for parity checks); the path
        timestamp only groups and orders.
        """
        if not groups:
            return None
        files = groups[max(groups)]
        snapshot = dfm_snapshot_dt(dfm_from_s3(files[0].path)).strftime(SNAPSHOT_FMT)
        if snapshot > state.snapshot:
            return snapshot, files
        if snapshot == state.snapshot and not state.load_complete:
            return snapshot, files
        return None

    def _ingest_load_group(
        self, state: BronzeState, snapshot: str, files: list[RawFile]
    ) -> BronzeState:
        """Append one snapshot's LOAD files to bronze in byte/count-bounded batches."""
        log = ProcessLog(
            "bronze_load_snapshot", table=self.table, snapshot=snapshot, group_files=len(files)
        )
        resuming = state.snapshot == snapshot and state.load_watermark != ""
        if not resuming:
            assert files[0].basename.endswith("0001.csv.gz"), (
                f"LOAD group {snapshot} for {self.table} does not start at "
                f"…0001.csv.gz (first file: {files[0].canonical})"
            )
            state = BronzeState(snapshot, False, "", state.cdc_watermark)
        pending = [f for f in files if f.basename > state.load_watermark]
        assert pending or state.load_complete, (
            f"LOAD group {snapshot} for {self.table} has no files past watermark "
            f"{state.load_watermark!r} but is not marked complete"
        )

        batches: list[list[RawFile]] = []
        batch: list[RawFile] = []
        batch_bytes = 0
        for file in pending:
            batch.append(file)
            batch_bytes += file.size_bytes
            if len(batch) >= LOAD_BATCH_FILES or batch_bytes >= LOAD_BATCH_BYTES:
                batches.append(batch)
                batch = []
                batch_bytes = 0
        if batch:
            batches.append(batch)

        for i, batch in enumerate(batches):
            frame = pl.concat(
                [self._load_file_frame(f) for f in batch], how="diagonal_relaxed"
            ).with_columns(
                pl.lit(snapshot).alias("snapshot"),
                pl.lit(CHANGE_CLASS_LOAD).alias("odin_change_class"),
            )
            # The final batch's commit must itself record load_complete: a
            # deferred "complete" only reaches the log via a later CDC commit,
            # and a quiet table (no CDC) or a crash before it would leave the
            # group permanently half-open.
            complete = i == len(batches) - 1
            state = BronzeState(snapshot, complete, batch[-1].basename, state.cdc_watermark)
            self._append(frame, state)

        log.complete(files_appended=sum(map(len, batches)), resumed=resuming)
        return state

    def _load_file_frame(self, file: RawFile) -> pl.DataFrame:
        """
        Read one LOAD csv.gz into a frame with the synthesized header columns.

        Header column values match the legacy job's snapshot_to_parquet: the
        file's own DFM startWriteTimestamp drives header__timestamp and
        header__year/month; header__change_seq is NULL and the oper is 'L'.
        """
        dfm = dfm_from_s3(file.path)
        dfm_dt = dfm_snapshot_dt(dfm)
        schema = dfm_to_polars_schema(dfm)
        with tempfile.TemporaryDirectory() as tmpdir:
            raw = os.path.join(tmpdir, "load.raw")
            local = os.path.join(tmpdir, "load.csv")
            with open(raw, "wb") as w_bytes:
                shutil.copyfileobj(stream_object(file.path), w_bytes)
            # LOAD objects are named .csv.gz but sniff the magic bytes rather
            # than trust the extension (the legacy job reads them uncompressed).
            with open(raw, "rb") as check:
                is_gzip = check.read(2) == b"\x1f\x8b"
            if is_gzip:
                with gzip.open(raw, "rb") as r_bytes, open(local, "wb") as w_bytes:
                    shutil.copyfileobj(r_bytes, w_bytes)
            else:
                local = raw
            return (
                pl.scan_csv(local, schema=schema, has_header=True)
                .with_columns(
                    pl.lit(dfm_dt.strftime("%Y")).cast(pl.Int32()).alias("header__year"),
                    pl.lit(dfm_dt.strftime("%m")).cast(pl.Int32()).alias("header__month"),
                    pl.lit(None, dtype=pl.String()).alias("header__change_seq"),
                    pl.lit("L").alias("header__change_oper"),
                    pl.lit(dfm_dt).alias("header__timestamp"),
                    pl.lit(file.path).alias("header__from_csv"),
                )
                .collect()
            )

    # ------------------------------------------------------------------
    # CDC ingestion
    # ------------------------------------------------------------------

    def _ingest_cdc(self, state: BronzeState) -> int:
        """Ingest CDC files past the watermark in batches; return next-run interval."""
        log = ProcessLog("bronze_ingest_cdc", table=self.table, cdc_watermark=state.cdc_watermark)
        files = self._find_cdc_files(state.cdc_watermark)
        if not files:
            log.complete(cdc_files_found=0)
            return _long_run_interval()

        # Only CDC newer than the current snapshot is ingested.
        work = [f for f in files if _cdc_ts_as_snapshot(_cdc_ts(f.path)) > state.snapshot]

        batches = []
        new_batch = []
        all_bytes = 0
        batch_bytes = 0
        size_capped = False
        for file in work:
            new_batch.append(file)
            batch_bytes += file.size_bytes
            all_bytes += file.size_bytes
            if all_bytes >= RUN_CDC_BYTES:
                size_capped = True
                break
            elif batch_bytes >= CDC_BATCH_BYTES:
                batches.append(new_batch)
                new_batch = []
                batch_bytes = 0
        if new_batch and not size_capped:
            batches.append(new_batch)

        pool = ThreadPoolExecutor(max_workers=thread_cpus())
        schema_cache: dict[bytes, pl.Schema] = {}
        try:
            for batch in batches:
                state = self._ingest_cdc_batch(batch, state, pool, schema_cache)
        finally:
            pool.shutdown()

        # A full listing (MAX_CDC_FILES) means more files exist behind the cap
        # even when the byte budget wasn't reached (a backlog of small files).
        more_pending = size_capped or len(files) >= MAX_CDC_FILES
        log.complete(
            cdc_files_found=len(files),
            cdc_files_pending=len(work),
            cdc_files_appended=sum(map(len, batches)),
            cdc_files_skipped_superseded=len(files) - len(work),
            cdc_watermark=state.cdc_watermark,
            more_pending=more_pending,
        )
        if more_pending:
            return NEXT_RUN_IMMEDIATE
        return _default_run_interval()

    def _find_cdc_files(self, watermark: str) -> list[RawFile]:
        """
        Find CDC csv.gz files with filename timestamp past `watermark`.

        One flat listing per location: list_objects is prefix-based, so the
        base ``__ct/`` folder covers the ``snapshot/``, ``snapshot=<TS>/``,
        and ``timestamp/`` sublayouts in a single paginated call (a
        per-subfolder loop here previously cost one round trip per date
        partition). Incoming locations (small; legacy keeps them drained) are
        listed before processed/ (see module docstring for why that order is
        race-free). The watermark filter is client-side, so the processed/
        listing pages through the full retained history; if that ever costs
        too many requests, the fix is a layout-aware server-side floor via
        list_objects' start_after. The result is deduped on canonical key and
        sorted by timestamp.
        """

        def past_watermark(obj: S3Object) -> bool:
            if not obj.path.endswith(".csv.gz"):
                return False
            match = RE_CDC_TS.search(os.path.basename(obj.path))
            if match is None:
                # Include so the collect loop's _cdc_ts raises loudly instead
                # of a silent skip (list_objects swallows in_func exceptions
                # by failing the whole listing to an empty result).
                return True
            return match.group(0) > watermark

        seen: dict[str, RawFile] = {}

        def collect(prefix: str, incoming: bool) -> None:
            found = list_objects(
                prefix,
                max_objects=MAX_CDC_FILES,
                in_filter=".csv.gz",
                in_func=past_watermark,
            )
            for obj in found:
                canonical = canonical_key(obj.path)
                if canonical in seen:
                    continue
                seen[canonical] = RawFile(
                    canonical, obj.path, obj.size_bytes, obj.last_modified, incoming
                )

        cdc_table = f"{self.table}__ct"
        collect(s3_folder(os.path.join(DATA_ARCHIVE, IN_QLIK_PREFIX, cdc_table)), True)
        collect(s3_folder(os.path.join(DATA_ERROR, IN_QLIK_PREFIX, cdc_table)), True)
        collect(
            s3_folder(
                os.path.join(DATA_ARCHIVE, CUBIC_QLIK_PROCESSED, IN_QLIK_PREFIX, cdc_table)
            ),
            False,
        )

        files = sorted(seen.values(), key=lambda f: (_cdc_ts(f.path), f.basename))
        return files[:MAX_CDC_FILES]

    def _ingest_cdc_batch(
        self,
        batch: list[RawFile],
        state: BronzeState,
        pool: ThreadPoolExecutor,
        schema_cache: dict[bytes, pl.Schema],
    ) -> BronzeState:
        """
        Download, convert, and append one CDC batch as a single commit.

        Each file is read straight into a typed frame — the legacy job staged
        merged CSVs on disk only to shape its output parquet files, a
        constraint Delta appends don't have. All frames are concatenated so
        the watermark advance and every row it covers share one commit: a
        crash either re-reads the whole batch (nothing appended) or resumes
        past all of it. Every row lands in the current snapshot's partition
        (older-snapshot CDC never reaches a batch).
        """
        log = ProcessLog("bronze_cdc_batch", table=self.table, batch_files=len(batch))
        reader = partial(self._cdc_file_frame, schema_cache=schema_cache)
        frames = [f for f in pool.map(reader, batch) if f is not None]

        state = state._replace(cdc_watermark=_cdc_ts(batch[-1].path))
        if not frames:
            log.complete(rows_appended=0, cdc_watermark=state.cdc_watermark)
            return state

        frame = (
            pl.concat(frames, how="diagonal_relaxed")
            .with_columns(
                pl.lit(state.snapshot).alias("snapshot"),
                pl.lit(CHANGE_CLASS_CDC).alias("odin_change_class"),
            )
            .sort("header__change_seq")
        )
        if frame.height > 0:
            self._append(frame, state)
        log.complete(rows_appended=frame.height, cdc_watermark=state.cdc_watermark)
        return state

    def _cdc_file_frame(
        self, file: RawFile, schema_cache: dict[bytes, pl.Schema]
    ) -> pl.DataFrame | None:
        """
        Read one CDC csv.gz directly into a typed frame.

        The parse schema comes from the file's DFM, cached on the CSV header
        line: files sharing a header share a schema (the legacy job's
        header-hash grouping, minus the disk staging), so one DFM read covers
        every file of a given shape. header__year/month derive from
        header__timestamp; the source path is added as a literal. Returns
        None for a fully empty file.
        """
        with gzip.open(stream_object(file.path), "rb") as r_bytes:
            header = r_bytes.readline()
            body = r_bytes.read()
        if not header:
            return None
        schema = schema_cache.get(header)
        if schema is None:
            schema = dfm_to_polars_schema(dfm_from_s3(file.path))
            schema_cache[header] = schema
        if body:
            frame = pl.read_csv(body, schema=schema, has_header=False)
        else:
            frame = pl.DataFrame(schema=schema)
        return frame.with_columns(
            pl.col("header__timestamp").dt.strftime("%Y").cast(pl.Int32()).alias("header__year"),
            pl.col("header__timestamp").dt.strftime("%m").cast(pl.Int32()).alias("header__month"),
            pl.lit(file.path).alias("header__from_csv"),
        )


def schedule_qlik_bronze(schedule: sched.scheduler) -> None:
    """Schedule one CubicQlikBronze job per bronze-enabled table for this instance."""
    for table in CUBIC_QLIK_BRONZE_TABLES_INSTANCE:
        job = CubicQlikBronze(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
