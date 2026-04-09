import os
import sched
from typing import List
from typing import Tuple

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyarrow.dataset as pd

from odin.job import OdinJob
from odin.job import job_proc_schedule
from odin.utils.logger import ProcessLog
from odin.utils.logger import free_disk_bytes
from odin.utils.runtime import sigterm_check
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import CUBIC_ODS_FACT_DATA
from odin.utils.locations import CUBIC_QLIK_DATA
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import CUBIC_QLIK_PROCESSED
from odin.utils.parquet import fast_last_mod_ds_max
from odin.utils.parquet import ds_metadata_min_max
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import ds_unique_values
from odin.utils.parquet import pq_dataset_writer
from odin.utils.parquet import ds_metadata_limit_k_sorted
from odin.utils.parquet import ds_batched_join
from odin.utils.parquet import polars_decimal_as_string
from odin.utils.aws.s3 import list_partitions
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.aws.s3 import download_object
from odin.utils.aws.s3 import upload_file
from odin.utils.aws.s3 import s3_folder
from odin.ingestion.qlik.dfm import dfm_from_s3
from odin.ingestion.qlik.dfm import QlikDFM
from odin.ingestion.qlik.tables import CUBIC_ODS_TABLES

NEXT_RUN_DEFAULT = 60 * 60 * 4  # 4 hours
NEXT_RUN_IMMEDIATE = 60 * 5  # 5 minutes
NEXT_RUN_LONG = 60 * 60 * 12  # 12 hours
MAX_LOAD_RECORDS = 10_000


class NoQlikHistoryError(Exception):
    """No Qlik history files available to process."""

    pass


def pl_pipe_update(left: pl.DataFrame, right: pl.DataFrame, keys: List[str]) -> pl.DataFrame:
    """
    DataFrame UPDATE operation that will join on NULL values.

    :param left: Dataframe to UPDATE
    :param right: Dataframe to UPDATE from
    :prarm keys: JOIN keys

    :return: left Dataframe with UPDATED values from right
    """
    non_keys = [c for c in right.columns if c not in keys and c in left.columns]
    suffix = "_update_NEW_"
    return (
        left.join(
            right.select(keys + non_keys),
            how="left",
            on=keys,
            suffix=suffix,
            coalesce=True,
            nulls_equal=True,
        )
        .with_columns(**{c: pl.coalesce([pl.col(c + suffix), pl.col(c)]) for c in non_keys})
        .drop([c + suffix for c in non_keys])
    )


def dfm_from_cdc_records(cdc_df: pl.DataFrame) -> QlikDFM:
    """
    Produce Qlik DFM record from CDC Dataframe.

    Will produce QlikDFM for last available csv file found in cdc_df. This QlikDFM information
    will be used to determine keys to be used for CDC-> Fact table operations.

    :param cdc_df: Dataframe of CDC records from qlik history dataset

    :return: dfm contents as TypedDict
    """
    dfm_path = str(cdc_df.get_column("header__from_csv").max())
    dfm_path = dfm_path.replace("s3://", "").split("/", 1)[-1]
    dfm_path = os.path.join(DATA_ARCHIVE, CUBIC_QLIK_PROCESSED, dfm_path)
    return dfm_from_s3(dfm_path)


class CubicODSFact(OdinJob):
    """Create/Update Cubic ODS Fact tables"""

    def __init__(self, table: str) -> None:
        """Create CubicODSFact instance."""
        self.table = table
        self.s3_source = os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DATA, table)
        self.s3_export = os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_FACT_DATA, table)
        self.start_kwargs = {"table": table}
        self.history_drop_columns = [
            "header__year",
            "header__month",
            "header__change_oper",
            "header__timestamp",
            "header__from_csv",
            "snapshot",
        ]

    def snapshot_check(self) -> None:
        """Check if new or ongoing snapshot"""
        history_snapshots = list_partitions(self.s3_source)
        if len(history_snapshots) == 0:
            raise NoQlikHistoryError("No history snapshots available.")
        self.history_snapshot = history_snapshots[-1].replace("snapshot=", "")
        self.snapshot_source = f"{self.s3_source}/snapshot={self.history_snapshot}/"
        self.history_ds = ds_from_path(f"s3://{self.snapshot_source}")

        self.part_columns = []
        if "edw_inserted_dtm" in self.history_ds.schema.names:
            self.part_columns.append("odin_year")

        history_ds_rows = self.history_ds.count_rows()
        history_ds_groups = 0
        frag: pd.ParquetFileFragment
        for frag in self.history_ds.get_fragments():
            history_ds_groups += frag.num_row_groups
        self.batch_size = max(5000, int(history_ds_rows / (4 * history_ds_groups)))

        try:
            self.fact_snapshot = str(fast_last_mod_ds_max(self.s3_export, "odin_snapshot"))
        except IndexError:
            self.fact_snapshot = ""

        # Log snapshot check results
        snapshot_match = self.history_snapshot == self.fact_snapshot
        snapshot_log = ProcessLog(
            "snapshot_check",
            table=self.table,
            history_snapshot=self.history_snapshot,
            fact_snapshot=self.fact_snapshot if self.fact_snapshot else "(empty)",
            snapshots_match=snapshot_match,
            new_snapshot_detected=not snapshot_match,
            history_snapshots_available=len(history_snapshots),
            all_history_snapshots=str(history_snapshots[-5:])
            if len(history_snapshots) > 5
            else str(history_snapshots),
            history_ds_rows=history_ds_rows,
            history_ds_groups=history_ds_groups,
            batch_size=self.batch_size,
        )
        snapshot_log.complete()

    def sync_tmp_paths(self, tmp_paths: list[str]) -> None:
        """Sync local parquet files with S3"""
        ProcessLog("sync_tmp_paths")
        sync_paths = []
        if self.part_columns:
            part_columns = self.part_columns
            search_paths = []
            for part in ds_unique_values(ds_from_path(tmp_paths), self.part_columns).to_pylist():
                part_prefix = "/".join([f"{k}={v}" for k, v in part.items()])
                search_paths.append(f"{os.path.join(self.s3_export, part_prefix)}/")
        else:
            part_columns = None
            search_paths = [f"{self.s3_export}/"]

        for search_path in search_paths:
            found_objs = list_objects(search_path, in_filter=".parquet")
            if found_objs:
                sync_file = found_objs[-1].path.replace("s3://", "")
                destination = os.path.join(self.tmpdir, sync_file.replace("/year_", "/temp_"))
                download_object(found_objs[-1].path, destination)
                sync_paths.append(destination)

        # Create new merged parquet file(s)
        new_paths = pq_dataset_writer(
            source=ds_from_path(sync_paths + tmp_paths),
            partition_columns=part_columns,
            export_folder=os.path.join(self.tmpdir, self.s3_export),
            export_file_prefix="year",
        )

        # Check for sigterm before upload (can't be un-done)
        sigterm_check()
        for new_path in new_paths:
            move_path = new_path.replace(f"{self.tmpdir}/", "")
            upload_file(new_path, move_path)

    def load_new_snapshot(self) -> None:
        """Load new snapshot from history tables"""
        # Log what will be deleted before the destructive operation
        existing_objects = list_objects(s3_folder(self.s3_export), in_filter=".parquet")
        existing_paths = [o.path for o in existing_objects]

        # Get row count of existing data before deletion
        existing_row_count = 0
        existing_seq_min = None
        existing_seq_max = None
        if existing_paths:
            try:
                existing_ds = ds_from_path(s3_folder(self.s3_export))
                existing_row_count = existing_ds.count_rows()
                existing_seq_min, existing_seq_max = ds_metadata_min_max(
                    existing_ds, "header__change_seq"
                )
            except Exception:
                existing_row_count = -1  # indicates error reading

        delete_log = ProcessLog(
            "load_new_snapshot_delete",
            table=self.table,
            history_snapshot=self.history_snapshot,
            fact_snapshot=self.fact_snapshot if self.fact_snapshot else "(empty)",
            files_to_delete=len(existing_paths),
            existing_row_count=existing_row_count,
            existing_seq_min=str(existing_seq_min) if existing_seq_min else None,
            existing_seq_max=str(existing_seq_max) if existing_seq_max else None,
            deleted_paths=str(existing_paths[:10])
            if len(existing_paths) > 10
            else str(existing_paths),
            total_deleted_paths=len(existing_paths),
        )
        delete_log.complete()

        delete_objects(existing_paths)
        write_schema = self.history_ds.schema
        for col in self.history_drop_columns:
            write_schema = write_schema.remove(write_schema.get_field_index(col))
        odin_columns: List[Tuple[str, pa.DataType]] = [
            ("odin_index", pa.int64()),
            ("odin_snapshot", pa.large_string()),
        ]
        if "odin_year" in self.part_columns:
            odin_columns.append(("odin_year", pa.int32()))
        for col, dtype in odin_columns:
            write_schema = write_schema.append(pa.field(col, dtype))
        ds_filter = pc.field("header__change_oper") == "L"
        odin_index = 0
        write_path = os.path.join(self.tmpdir, "t.parquet")
        writer = pq.ParquetWriter(
            write_path, schema=write_schema, compression="zstd", compression_level=3
        )
        for batch in self.history_ds.to_batches(
            filter=ds_filter, batch_readahead=0, fragment_readahead=0, batch_size=self.batch_size
        ):
            if batch.num_rows == 0:
                continue
            batch = batch.append_column(
                "odin_index",
                pa.array(list(range(odin_index, odin_index + batch.num_rows)), type=pa.int64()),
            )
            batch = batch.append_column(
                "odin_snapshot",
                pa.array([self.history_snapshot] * batch.num_rows, type=pa.large_string()),
            )
            if "edw_inserted_dtm" in write_schema.names:
                batch = batch.append_column(
                    "odin_year",
                    pc.coalesce(pc.strftime(batch.column("edw_inserted_dtm"), "%Y"), "0").cast(
                        pa.int32()
                    ),
                )
            batch = batch.drop_columns(self.history_drop_columns)
            writer.write_batch(batch)
            odin_index += batch.num_rows
            if os.path.getsize(write_path) * 3 > free_disk_bytes():
                # running low on disk space
                writer.close()
                self.sync_tmp_paths([write_path])
                self.reset_tmpdir()
                writer = pq.ParquetWriter(
                    write_path, schema=write_schema, compression="zstd", compression_level=3
                )

        writer.close()
        self.sync_tmp_paths([write_path])
        self.reset_tmpdir()

        # Log completion of new snapshot load
        verify_ds = ds_from_path(s3_folder(self.s3_export))
        verify_row_count = verify_ds.count_rows()
        verify_objects = list_objects(s3_folder(self.s3_export), in_filter=".parquet")
        verify_seq_min, verify_seq_max = ds_metadata_min_max(verify_ds, "header__change_seq")
        load_complete_log = ProcessLog(
            "load_new_snapshot_complete",
            table=self.table,
            history_snapshot=self.history_snapshot,
            loaded_row_count=verify_row_count,
            loaded_file_count=len(verify_objects),
            final_odin_index=odin_index,
            header__change_seq_min=str(verify_seq_min),
            header__change_seq_max=str(verify_seq_max),
        )
        load_complete_log.complete()

    def load_cdc_records(self) -> int:
        """
        Load CDC records and apply to fact table.

        Simplified CDC pipeline that resolves each key to a single final operation
        before touching the fact table. This avoids the interleaving issues of
        building separate insert/update/delete dataframes simultaneously.

        Order of operations:
          1. Read the current fact table and its max header__change_seq.
          2. Pull CDC records (I/U/D) from history with seq > max_fact_seq.
             Repeat in a loop to accumulate enough work.
          3. For every key touched by CDC, determine the FINAL operation:
             sort all CDC records by header__change_seq descending, deduplicate
             by key keeping the latest. The latest oper wins.
          4. Build a single "new_rows" dataframe:
             - For keys whose final op is I: use the I record directly.
             - For keys whose final op is U: fetch the existing fact row, apply
               the sparse column updates, produce an updated row.  If no fact
               row exists (I→U in same batch), use the I record as the base.
             - For keys whose final op is D: no new row (just drop the old one).
          5. Collect odin_index values of all fact rows being replaced or deleted
             (any key in the CDC batch that already exists in fact_ds).
          6. Write: filter old dataset to exclude touched rows, union with new_rows,
             upload to S3.
        """
        # --- Step 1: Load current fact table state ---
        logger = ProcessLog("load_cdc_records", table=self.table)
        fact_ds = ds_from_path(s3_folder(self.s3_export))
        initial_row_count = fact_ds.count_rows()
        _, max_fact_seq = ds_metadata_min_max(fact_ds, "header__change_seq")
        _, max_odin_index = ds_metadata_min_max(fact_ds, "odin_index")
        logger.add_metadata(
            initial_row_count=initial_row_count,
            max_fact_seq=str(max_fact_seq),
            max_odin_index=str(max_odin_index),
        )

        # --- Step 2: Accumulate CDC records ---
        cdc_filter = (
            (pc.field("header__change_oper") == "I")
            | (pc.field("header__change_oper") == "D")
            | (pc.field("header__change_oper") == "U")
        )
        all_cdc_frames: list[pl.DataFrame] = []
        current_min_seq = max_fact_seq
        max_load_records = MAX_LOAD_RECORDS
        for _ in range(11):
            batch_df = ds_metadata_limit_k_sorted(
                ds=self.history_ds,
                sort_column="header__change_seq",
                min_sort_value=current_min_seq,
                ds_filter=cdc_filter,
                ds_filter_columns=["header__change_oper"],
            )
            if batch_df.height == 0:
                break
            all_cdc_frames.append(batch_df)
            current_min_seq = batch_df.get_column("header__change_seq").max()
            max_load_records = max(max_load_records, batch_df.height)
            total_cdc = sum(f.height for f in all_cdc_frames)
            if total_cdc > max_load_records:
                break

        if not all_cdc_frames:
            logger.complete(cdc_records_found=0)
            return NEXT_RUN_LONG

        cdc_df = pl.concat(all_cdc_frames, how="diagonal")
        del all_cdc_frames

        # Get table keys from DFM
        dfm = dfm_from_cdc_records(cdc_df)
        keys = [
            col["name"].lower() for col in dfm["dataInfo"]["columns"] if col["primaryKeyPos"] > 0
        ]

        # --- Step 3: Resolve each key to its FINAL operation ---
        # Sort by seq descending, deduplicate by key keeping the latest record.
        # The header__change_oper of this record is the "winning" operation.
        mod_cast, orig_cast = polars_decimal_as_string(cdc_df.select(keys))
        resolved = (
            cdc_df.cast(mod_cast)
            .sort(by="header__change_seq", descending=True)
            .unique(keys, keep="first")
        )
        resolved_deletes = resolved.filter(pl.col("header__change_oper").eq("D"))
        resolved_inserts = resolved.filter(pl.col("header__change_oper").eq("I"))
        resolved_updates = resolved.filter(pl.col("header__change_oper").eq("U"))

        # --- Step 4: Build new_rows ---

        # 4a. For updates: build sparse column values from ALL U records for these keys,
        # then fetch existing fact rows and apply the updates.
        update_keys = resolved_updates.select(keys)
        if update_keys.height > 0:
            # Build per-column update values from all U records matching update keys
            u_records = cdc_df.cast(mod_cast).filter(pl.col("header__change_oper").eq("U"))
            update_values = update_keys.clone()
            for col in u_records.columns:
                if col in keys:
                    continue
                col_df = (
                    u_records.filter(pl.col(col).is_not_null())
                    .sort(by="header__change_seq", descending=True)
                    .unique(keys, keep="first")
                    .select(keys + [col])
                )
                if col_df.height == 0:
                    continue
                if col in update_values.columns:
                    update_values = update_values.pipe(pl_pipe_update, col_df, keys)
                else:
                    update_values = update_values.join(
                        col_df, on=keys, how="left", nulls_equal=True, coalesce=True
                    )

            # Fetch existing fact rows for these keys and apply updates
            existing_rows = ds_batched_join(
                fact_ds, update_values.cast(orig_cast), keys, self.batch_size
            )

            # I→U in same batch: key was inserted and then updated, so no
            # existing fact row exists. Fall back to the I record as the base.
            if existing_rows.height < update_keys.height:
                found_keys = existing_rows.select(keys).cast(mod_cast)
                missing_keys = update_keys.join(found_keys, on=keys, how="anti", nulls_equal=True)
                if missing_keys.height > 0:
                    insert_base = (
                        cdc_df.cast(mod_cast)
                        .filter(pl.col("header__change_oper").eq("I"))
                        .sort(by="header__change_seq", descending=True)
                        .unique(keys, keep="first")
                        .join(missing_keys, on=keys, how="inner", nulls_equal=True)
                    )
                    if insert_base.height > 0:
                        existing_rows = pl.concat(
                            [existing_rows.cast(mod_cast), insert_base],
                            how="diagonal",
                        ).cast(orig_cast)

            if existing_rows.height > 0:
                if "odin_year" in existing_rows.columns:
                    existing_rows = existing_rows.cast({"odin_year": pl.Int32()})
            new_update_rows = (
                existing_rows.cast(mod_cast)
                .pipe(
                    pl_pipe_update,
                    update_values.drop(self.history_drop_columns, strict=False),
                    keys,
                )
                .cast(orig_cast)
            )

        # 4b. For inserts: use the I records directly (drop CDC header columns)
        new_insert_rows = resolved_inserts.cast(orig_cast)

        # Combine new rows and assign odin_index / odin_snapshot
        new_rows_parts = [new_insert_rows]
        if update_keys.height > 0:
            new_rows_parts.append(new_update_rows)
        new_rows_parts = [df for df in new_rows_parts if df.height > 0]
        if new_rows_parts:
            new_rows = pl.concat(new_rows_parts, how="diagonal")
        else:
            new_rows = pl.DataFrame()

        if new_rows.height > 0:
            start_odin_index = max_odin_index + 1
            new_rows = new_rows.with_columns(
                pl.arange(
                    start_odin_index, start_odin_index + new_rows.height, dtype=pl.Int64()
                ).alias("odin_index"),
                pl.lit(self.history_snapshot, dtype=pl.String()).alias("odin_snapshot"),
            ).drop(self.history_drop_columns, strict=False)
            if "edw_inserted_dtm" in new_rows.columns:
                new_rows = new_rows.with_columns(
                    pl.coalesce(pl.col("edw_inserted_dtm").dt.strftime("%Y"), 0)
                    .cast(pl.Int32())
                    .alias("odin_year")
                )

        # --- Step 5: Find odin_index values to drop from fact_ds ---
        # Any key that appears in CDC (regardless of operation) and exists in the
        # fact table must have its old row removed. Updates get a replacement row
        # in new_rows; deletes do not.
        all_touched_keys = resolved.select(keys).cast(orig_cast)
        existing_touched = ds_batched_join(fact_ds, all_touched_keys, keys, self.batch_size)
        drop_indices = existing_touched.get_column("odin_index")

        # --- Step 6: Write merged result to S3 ---
        sync_filter = ~pc.field("odin_index").isin(drop_indices.to_arrow())
        part_columns = self.part_columns if self.part_columns else None

        # Write new_rows to temp file
        insert_path = os.path.join(self.tmpdir, "temp_insert.parquet")
        if new_rows.height > 0:
            new_rows.write_parquet(insert_path)

        # Write filtered fact_ds (old rows minus touched keys)
        sync_paths = pq_dataset_writer(
            fact_ds.filter(sync_filter),
            partition_columns=part_columns,
            export_folder=self.tmpdir,
            export_file_prefix="temp_sync",
        )
        if new_rows.height > 0:
            sync_paths.append(insert_path)

        # Merge into final parquet files
        new_paths = pq_dataset_writer(
            source=ds_from_path(sync_paths),
            partition_columns=part_columns,
            export_folder=os.path.join(self.tmpdir, self.s3_export),
            export_file_prefix="year",
        )

        # Upload
        sigterm_check()
        for new_path in new_paths:
            move_path = new_path.replace(f"{self.tmpdir}/", "")
            upload_file(new_path, move_path)

        # --- Verify and log ---
        verify_ds = ds_from_path(s3_folder(self.s3_export))
        verify_min, verify_max = ds_metadata_min_max(verify_ds, "header__change_seq")
        final_row_count = verify_ds.count_rows()

        rows_dropped = drop_indices.n_unique()
        rows_inserted = new_rows.height
        expected_row_count = initial_row_count - rows_dropped + rows_inserted
        row_count_mismatch = final_row_count != expected_row_count

        # Check if more CDC records are available
        ds_available_count = 0
        if cdc_df.height > 0:
            ds_available_count = ds_metadata_limit_k_sorted(
                ds=self.history_ds,
                sort_column="header__change_seq",
                min_sort_value=cdc_df.get_column("header__change_seq").max(),
                ds_filter=cdc_filter,
                ds_filter_columns=["header__change_oper"],
                max_rows=max_load_records,
            ).height

        logger.complete(
            cdc_records_processed=cdc_df.height,
            resolved_inserts=resolved_inserts.height,
            resolved_updates=resolved_updates.height,
            resolved_deletes=resolved_deletes.height,
            rows_dropped=rows_dropped,
            rows_inserted=rows_inserted,
            final_row_count=final_row_count,
            expected_row_count=expected_row_count,
            row_count_mismatch=row_count_mismatch,
            s3_verify_seq_min=str(verify_min),
            s3_verify_seq_max=str(verify_max),
            ds_available_count=ds_available_count,
        )

        if ds_available_count > int(0.9 * max_load_records):
            return NEXT_RUN_IMMEDIATE
        return NEXT_RUN_DEFAULT

    def run(self) -> int:
        """
        Create / Update ODS Fact tables from Qlik history dataset.

        next_run Duration:
            - default -> 60 mins
            - no new CDC records found -> 12 hours
            - excess of CDC records -> 5 mins

        Maintain consistency of FACT tables with Qlik CDC history dataset.

        Fact tables need to be updated with INSERT, UPDATE and DELETE operations, depending on CDC
        records sent by Cubic QLIK instances.

        fields to add to fact parquet:
            - odin_snapshot
            - odin_index (unique single-field index used for lookups)

        Steps:
         - Check if NEW snapshot or update existing snapshot.
            - If NEW snapshot, clean out old FACT files and re-load "L" records from NEW snapshot.
         - Get latest header__change_seq from FACT dataset to query against HISTORY dataset.
         - Pull CDC records from HISTORY dataset and convert to FACT format.
            - Iterate HISTORY pull if record count is reduced by FACT conversion.
         - Merge CDC FACT conversion with existing FACT S3 dataset.
         - Upload merged FACT files to S3.
        """
        self.start_kwargs = {"table": self.table}
        next_run_secs = NEXT_RUN_DEFAULT

        try:
            self.snapshot_check()
            if self.history_snapshot != self.fact_snapshot:
                # New snapshot detected
                snapshot_compare_to_history = ProcessLog(
                    "snapshot_compare_to_history",
                    table=self.table,
                    action="load_new_snapshot",
                    history_snapshot=self.history_snapshot,
                    fact_snapshot=self.fact_snapshot if self.fact_snapshot else "(empty)",
                    snapshots_match=False,
                )
                snapshot_compare_to_history.complete()
                self.load_new_snapshot()
            else:
                # No new snapshot, update existing fact table
                snapshot_compare_to_history = ProcessLog(
                    "snapshot_compare_to_history",
                    table=self.table,
                    action="cdc_update_only",
                    history_snapshot=self.history_snapshot,
                    fact_snapshot=self.fact_snapshot,
                    snapshots_match=True,
                )
                snapshot_compare_to_history.complete()
            next_run_secs = self.load_cdc_records()
        # For development, other ODIN running...
        except pa.ArrowInvalid:
            self.start_kwargs["other_odin_running"] = "True"
            return NEXT_RUN_IMMEDIATE
        except NoQlikHistoryError:
            self.start_kwargs["no_qlik_history_available"] = "True"

        return next_run_secs


def schedule_cubic_ods_fact_gen(schedule: sched.scheduler) -> None:
    """
    Schedule All Jobs for generate cubic ODS fact tables process.

    :param schedule: application scheduler
    """
    for table in CUBIC_ODS_TABLES:
        job = CubicODSFact(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
