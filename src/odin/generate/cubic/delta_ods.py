"""
Delta-based Cubic ODS silver-table materialization.

This is a parallel, read-only alternative to ``generate/cubic/ods_fact.py``. It
reads the same snapshot-partitioned Qlik history parquet produced by
``ingestion/qlik/cubic_archive.py`` and materializes the current-state ("silver")
table as a Delta Lake table via MERGE, instead of the custom-parquet fact
pipeline.

The history parquet is queried with DuckDB (read side) and the silver table is
written with delta-rs (write side); no custom parquet utilities are involved.

Crucially it never moves, deletes, or prunes any source files. Its position on
the history input is tracked entirely by watermarks written into its own Delta
output (``odin_snapshot`` for the snapshot generation and ``header__change_seq``
for the CDC stream), so it can run concurrently with both ``cubic_archive.py``
(which owns moving raw files) and ``ods_fact.py`` (the existing fact pipeline).

The source data is treated as untrusted: every run asserts the invariants it
depends on (required columns present, primary keys declared and present, CDC
records carry a change sequence, a load snapshot is non-empty). Violations raise
rather than silently producing a corrupt silver table.

Steps per run:
  1. Find the latest history ``snapshot=`` partition.
  2. If it differs from the silver table's ``odin_snapshot``, rebuild silver from
     that snapshot's "L" (load) records (full overwrite).
  3. MERGE any CDC records (I/U/D) with ``header__change_seq`` greater than the
     silver watermark into the silver table.
"""

import os
import sched
from typing import Iterator

import duckdb
import polars as pl

from deltalake import DeltaTable
from deltalake import write_deltalake
from deltalake.exceptions import CommitFailedError
from deltalake.exceptions import SchemaMismatchError

from odin.job import OdinJob
from odin.job import job_proc_schedule
from odin.utils.aws.s3 import list_partitions
from odin.utils.aws.s3 import s3_file
from odin.utils.aws.s3 import s3_folder
from odin.utils.delta import column_max as delta_column_max
from odin.utils.delta import open_delta
from odin.utils.delta import row_count as delta_row_count
from odin.utils.locations import CUBIC_ODS_DELTA_DATA
from odin.utils.locations import CUBIC_QLIK_PROCESSED
from odin.utils.locations import CUBIC_QLIK_DATA
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.logger import ProcessLog
from odin.utils.runtime import sigterm_check
from odin.ingestion.qlik.dfm import QlikDFM
from odin.ingestion.qlik.dfm import dfm_from_s3
from odin.ingestion.qlik.utils import RE_SNAPSHOT_TS
from odin.ingestion.qlik.tables import _ODIN_INSTANCE
from odin.ingestion.qlik.tables import CUBIC_ODS_DELTA_TABLES

NEXT_RUN_DEFAULT = 60 * 60 * 4  # 4 hours
NEXT_RUN_BETA = 60 * 15  # 15 minutes
NEXT_RUN_IMMEDIATE = 60 * 5  # 5 minutes
NEXT_RUN_LONG = 60 * 60 * 12  # 12 hours

REBUILD_BATCH_SIZE = 10_000
MAX_MERGE_RECORDS = 100_000

CDC_OPERS = ("I", "U", "D")

# DuckDB read expression over the snapshot-partitioned history parquet.
READ_HISTORY = "read_parquet('{glob}', hive_partitioning = true, union_by_name = true)"

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


def _connect(glob: str) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection, configuring S3 access for s3:// globs."""
    con = duckdb.connect()
    if glob.startswith("s3://"):
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
        self.silver_uri = s3_file(
            os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_DELTA_DATA, table)
        )
        self.start_kwargs = {"table": table}
        self.silver: DeltaTable | None = None
        self.history_glob = f"{s3_folder(self.s3_source)}**/*.parquet"
        self.history_columns: list[str] = []
        self.history_snapshot = ""
        self.part_columns: list[str] = []

    def run(self) -> int:
        """Materialize the latest snapshot + CDC into silver; return seconds to next run."""
        log = ProcessLog("CubicODSDelta", table=self.table)
        self.start_kwargs = {"table": self.table}
        try:
            self.silver = open_delta(self.silver_uri)
            self._snapshot_check()

            silver_snapshot = ""
            if self.silver is not None:
                existing = delta_column_max(self.silver, "odin_snapshot")
                silver_snapshot = str(existing) if existing is not None else ""

            if self.history_snapshot != silver_snapshot:
                self._rebuild_silver()

            next_run = self._merge_cdc()
            log.complete(
                run_interval=next_run,
                history_snapshot=self.history_snapshot,
                new_snapshot=self.history_snapshot != silver_snapshot,
            )
            return next_run

        except NoQlikHistoryError as exc:
            self.start_kwargs["no_qlik_history_available"] = "True"
            log.failed(exception=exc)
            return _long_run_interval()
        except CommitFailedError as exc:
            self.start_kwargs["delta_concurrent_modification"] = "True"
            log.failed(exception=exc)
            return NEXT_RUN_IMMEDIATE
        except CDCSchemaIncompatibleError as exc:
            ProcessLog(
                "cdc_schema_incompatible", table=self.table, error=str(exc)
            ).failed(exception=exc)
            return NEXT_RUN_LONG

    @property
    def _read_history(self) -> str:
        """Return the DuckDB read_parquet expression for this table's history."""
        return READ_HISTORY.format(glob=self.history_glob)

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

        con = _connect(self.history_glob)
        try:
            describe = con.execute(f"DESCRIBE SELECT * FROM {self._read_history}").pl()
        finally:
            con.close()
        self.history_columns = describe.get_column("column_name").to_list()

        missing = set(REQUIRED_HISTORY_COLUMNS) - set(self.history_columns)
        assert not missing, (
            f"history for {self.table} is missing required columns: {sorted(missing)}"
        )
        self.part_columns = (
            ["odin_year", "odin_month"]
            if "edw_inserted_dtm" in self.history_columns
            else []
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
        log = ProcessLog(
            "delta_rebuild_silver", table=self.table, snapshot=self.history_snapshot
        )
        data_columns = [
            f'"{c}"' for c in self.history_columns if c not in META_DROP_COLUMNS
        ]
        partition_columns = (
            [
                "CAST(coalesce(strftime(edw_inserted_dtm, '%Y'), '0') AS INTEGER) AS odin_year",
                "CAST(coalesce(strftime(edw_inserted_dtm, '%m'), '0') AS INTEGER) AS odin_month",
            ]
            if self.part_columns
            else []
        )
        projection = ", ".join([*data_columns, "? AS odin_snapshot", *partition_columns])
        sql = (
            f"SELECT {projection} FROM {self._read_history} "
            "WHERE snapshot = ? AND header__change_oper = 'L'"
        )

        sigterm_check()
        con = _connect(self.history_glob)
        try:
            reader = con.execute(
                sql, [self.history_snapshot, self.history_snapshot]
            ).fetch_record_batch(REBUILD_BATCH_SIZE)
            write_deltalake(
                self.silver_uri,
                reader,
                mode="overwrite",
                schema_mode="overwrite",
                partition_by=self.part_columns or None,
            )
        finally:
            con.close()

        self.silver = DeltaTable(self.silver_uri)
        rows_loaded = delta_row_count(self.silver)
        assert rows_loaded > 0, (
            f"snapshot {self.history_snapshot} for {self.table} has no L (load) records"
        )
        log.complete(rows_loaded=rows_loaded)

    # ------------------------------------------------------------------
    # CDC MERGE (silver update from I/U/D records)
    # ------------------------------------------------------------------

    def _merge_cdc(self) -> int:
        """Apply pending CDC records to silver; return the next-run interval."""
        if self.silver is None:
            return _long_run_interval()
        log = ProcessLog("delta_merge_cdc", table=self.table)

        silver_max_seq = delta_column_max(self.silver, "header__change_seq")
        cdc_df = self._read_cdc(silver_max_seq, limit=MAX_MERGE_RECORDS)
        if cdc_df.height == 0:
            log.complete(cdc_records_found=0)
            return _long_run_interval()

        assert cdc_df.get_column("header__change_seq").null_count() == 0, (
            f"CDC records for {self.table} contain a null header__change_seq"
        )
        max_seq_processed = cdc_df.get_column("header__change_seq").max()

        keys = self._discover_keys(cdc_df)
        source = self._build_merge_source(cdc_df, keys)
        try:
            metrics = self._merge_apply(source, keys)
        except SchemaMismatchError as exc:
            raise CDCSchemaIncompatibleError(
                f"silver MERGE failed for {self.table}: {exc}"
            ) from exc

        self.silver = DeltaTable(self.silver_uri)
        more_pending = self._read_cdc(max_seq_processed, limit=1).height

        log.complete(
            cdc_records_processed=cdc_df.height,
            merge_source_rows=source.height,
            final_row_count=delta_row_count(self.silver),
            more_pending=more_pending,
            **{f"merge_{k}": v for k, v in metrics.items()},
        )
        return NEXT_RUN_IMMEDIATE if more_pending > 0 else _default_run_interval()

    def _read_cdc(self, after_seq, limit: int) -> pl.DataFrame:
        """Read up to `limit` CDC (I/U/D) records with seq > `after_seq`, seq-ascending."""
        opers = ", ".join(f"'{o}'" for o in CDC_OPERS)
        conditions = ["snapshot = ?", f"header__change_oper IN ({opers})"]
        params = [self.history_snapshot]
        if after_seq is not None:
            conditions.append("header__change_seq > ?")
            params.append(str(after_seq))
        sql = (
            f"SELECT * FROM {self._read_history} "
            f"WHERE {' AND '.join(conditions)} "
            f"ORDER BY header__change_seq LIMIT {limit}"
        )

        con = _connect(self.history_glob)
        try:
            return con.execute(sql, params).pl()
        finally:
            con.close()

    def _discover_keys(self, cdc_df: pl.DataFrame) -> list[str]:
        """Return the primary-key column names (lowercased) from the table DFM."""
        dfm = self._dfm_from_records(cdc_df)
        keys = [
            col["name"].lower()
            for col in dfm["dataInfo"]["columns"]
            if col["primaryKeyPos"] > 0
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

    def _build_merge_source(
        self, cdc_df: pl.DataFrame, keys: list[str]
    ) -> pl.DataFrame:
        """Resolve CDC records to one final row per key for the silver MERGE."""
        data_cols = [
            c
            for c in cdc_df.columns
            if c not in keys
            and c not in META_DROP_COLUMNS
            and c != "header__change_seq"
        ]
        sorted_desc = cdc_df.sort(by="header__change_seq", descending=True)
        latest = sorted_desc.unique(keys, keep="first").select(
            keys + ["header__change_seq", "header__change_oper"]
        )
        merged = sorted_desc.group_by(keys).agg(
            *[pl.col(c).drop_nulls().first() for c in data_cols],
            pl.col("header__change_oper").eq("I").any().alias("has_insert_base"),
        )
        source = latest.join(merged, on=keys, how="left", nulls_equal=True).with_columns(
            pl.lit(self.history_snapshot, dtype=pl.String).alias("odin_snapshot")
        )
        if "edw_inserted_dtm" in source.columns:
            source = source.with_columns(
                pl.coalesce(pl.col("edw_inserted_dtm").dt.strftime("%Y"), "0")
                .cast(pl.Int32)
                .alias("odin_year"),
                pl.coalesce(pl.col("edw_inserted_dtm").dt.strftime("%m"), "0")
                .cast(pl.Int32)
                .alias("odin_month"),
            )
        return source

    def _merge_apply(self, source: pl.DataFrame, keys: list[str]) -> dict:
        """Execute the delete/update/insert MERGE of `source` into silver."""
        assert self.silver is not None
        target_cols = [f.name for f in self.silver.schema().to_arrow()]
        missing = set(keys) - set(target_cols)
        assert not missing, (
            f"primary key columns {sorted(missing)} absent from silver table for {self.table}"
        )

        predicate = " AND ".join(
            f"(target.{k} = source.{k} OR (target.{k} IS NULL AND source.{k} IS NULL))"
            for k in keys
        )

        # Watermark/partition columns are taken verbatim from the resolved CDC row;
        # data columns coalesce so a sparse update preserves untouched values.
        passthrough = {
            "odin_snapshot",
            "header__change_seq",
            "odin_year",
            "odin_month",
        }
        source_cols = set(source.columns)
        update_set = {
            col: f"source.{col}"
            if col in passthrough
            else f"COALESCE(source.{col}, target.{col})"
            for col in target_cols
            if col not in keys and col in source_cols
        }
        insert_set = {col: f"source.{col}" for col in target_cols if col in source_cols}

        sigterm_check()
        merger = self.silver.merge(
            source=source.to_arrow(),
            predicate=predicate,
            source_alias="source",
            target_alias="target",
            error_on_type_mismatch=False,
            merge_schema=False,
        )
        return (
            merger.when_matched_delete(predicate="source.header__change_oper = 'D'")
            .when_matched_update(
                predicate="source.header__change_oper != 'D'", updates=update_set
            )
            .when_not_matched_insert(
                predicate=(
                    "source.header__change_oper != 'D' "
                    "AND source.has_insert_base = true"
                ),
                updates=insert_set,
            )
            .execute()
        )


def schedule_delta_ods(schedule: sched.scheduler) -> None:
    """Schedule one CubicODSDelta job per Delta-enabled Cubic ODS table."""
    for table in CUBIC_ODS_DELTA_TABLES:
        job = CubicODSDelta(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
