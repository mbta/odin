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
from odin.ingestion.qlik.dfm import dfm_from_s3
from odin.ingestion.qlik.dfm import QlikDFM
from odin.ingestion.qlik.tables import CUBIC_ODS_TABLES

NEXT_RUN_DEFAULT = 60 * 60 * 4  # 4 hours
NEXT_RUN_IMMEDIATE = 60 * 5  # 5 minutes
NEXT_RUN_LONG = 60 * 60 * 12  # 12 hours


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


def cdc_to_fact(
    cdc_df: pl.DataFrame,
    insert_df: pl.DataFrame,
    update_df: pl.DataFrame,
    delete_df: pl.DataFrame,
    keys: List[str],
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Convert Qlik CDC records to fact dataframes.

    :param cdc_df: Dataframe of CDC records from qlik history dataset
    :param insert_df: Dataframe of CDC INSERT records
    :param update_df: Dataframe of CDC UPDATE records
    :param delete_df: Dataframe of CDC DELETE records
    :param keys: ODS Table Keys (for unique operations)

    :return: Tuple[new INSERT df, new UPDATE df, new DELETE df]
    """
    insert_df = pl.concat(
        [insert_df, cdc_df.filter(pl.col("header__change_oper").eq("I"))],
        how="diagonal",
    )
    delete_df = pl.concat(
        [delete_df, cdc_df.filter(pl.col("header__change_oper").eq("D"))],
        how="diagonal",
    )

    mod_cast, orig_cast = polars_decimal_as_string(cdc_df.select(keys))
    cdc_df = cdc_df.cast(mod_cast)

    # add keys from cdc to update, if not present
    if update_df.shape[0] == 0:
        update_df = cdc_df.select(keys).unique()
    else:
        update_df = update_df.cast(mod_cast).join(
            cdc_df.select(keys).unique(),
            on=keys,
            how="full",
            nulls_equal=True,
            coalesce=True,
        )

    # perform per-column cdc -> fact update
    for col in cdc_df.columns:
        if col in keys:
            continue
        _df = (
            cdc_df.filter(
                pl.col("header__change_oper").eq("U"),
                pl.col(col).is_not_null(),
            )
            .sort(by="header__change_seq", descending=True)
            .unique(keys, keep="first")
            .select(keys + [col])
        )
        if _df.shape[0] == 0:
            continue
        if col in update_df.columns:
            update_df = update_df.pipe(pl_pipe_update, _df, keys)
        else:
            update_df = update_df.join(_df, on=keys, how="left", nulls_equal=True, coalesce=True)

    return (insert_df, update_df.cast(orig_cast), delete_df)


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
        delete_objects([o.path for o in list_objects(f"{self.s3_export}/", in_filter=".parquet")])
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

    def load_cdc_records(self) -> int:
        """
        Load change records from history files.

        Qlik CDC Records consists of 5 potential header__change_seq values:
        - I (Insert records)
        - D (Delete records)
        - U (After Update records)
        - B (Before Update records)
        - Null (Initial snapshot load records (same as Insert but from LOAD.. files))

        "B" Records are ignored for this process as they do not contain any relevant information.
        """
        fact_ds = ds_from_path(f"s3://{self.s3_export}/")
        _, max_fact_seq = ds_metadata_min_max(fact_ds, "header__change_seq")
        cdc_filter = (
            (pc.field("header__change_oper") == "I")
            | (pc.field("header__change_oper") == "D")
            | (pc.field("header__change_oper") == "U")
        )
        cdc_df = ds_metadata_limit_k_sorted(
            ds=self.history_ds,
            sort_column="header__change_seq",
            min_sort_value=max_fact_seq,
            ds_filter=cdc_filter,
            ds_filter_columns=["header__change_oper"],
        )
        max_load_records = max(10_000, cdc_df.height)

        if cdc_df.height == 0:
            return NEXT_RUN_LONG

        dfm = dfm_from_cdc_records(cdc_df)
        keys = [
            col["name"].lower() for col in dfm["dataInfo"]["columns"] if col["primaryKeyPos"] > 0
        ]

        insert_df, update_df, delete_df = cdc_to_fact(
            cdc_df, pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), keys
        )

        # many CDC records often impact the same FACT row.
        # After cdc_to_fact colapses CDC records to FACT format, pull more CDC records until
        # expected number of FACT records are available for merging
        for _ in range(10):
            num_load_records = insert_df.height + delete_df.height + update_df.height
            if cdc_df.height == 0 or num_load_records > max_load_records:
                break
            max_fact_seq = cdc_df.get_column("header__change_seq").max()
            cdc_df = ds_metadata_limit_k_sorted(
                ds=self.history_ds,
                sort_column="header__change_seq",
                min_sort_value=max_fact_seq,
                ds_filter=cdc_filter,
                ds_filter_columns=["header__change_oper"],
            )
            insert_df, update_df, delete_df = cdc_to_fact(
                cdc_df, insert_df, update_df, delete_df, keys
            )
        # Determine if next run should be immediate
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

        if insert_df.height > 0:
            _, max_odin_index = ds_metadata_min_max(fact_ds, "odin_index")
            start_odin_index = max_odin_index + 1
            insert_df = insert_df.with_columns(
                pl.arange(
                    start_odin_index, start_odin_index + insert_df.height, dtype=pl.Int64()
                ).alias("odin_index"),
                pl.lit(self.history_snapshot, dtype=pl.String()).alias("odin_snapshot"),
            ).drop(self.history_drop_columns, strict=False)
            if "edw_inserted_dtm" in insert_df.columns:
                insert_df = insert_df.with_columns(
                    pl.coalesce(pl.col("edw_inserted_dtm").dt.strftime("%Y"), 0)
                    .cast(pl.Int32())
                    .alias("odin_year")
                )

        drop_indices = pl.Series("odin_index", [], pl.Int64())
        if update_df.height > 0:
            update_df = update_df.drop(self.history_drop_columns, strict=False)
            mod_cast, orig_cast = polars_decimal_as_string(update_df.select(keys))
            s3_update_df = ds_batched_join(fact_ds, update_df, keys, self.batch_size)
            if "odin_year" in s3_update_df.columns:
                s3_update_df = s3_update_df.cast({"odin_year": pl.Int32()})
            insert_df = pl.concat([s3_update_df, insert_df], how="diagonal")
            insert_df = (
                insert_df.cast(mod_cast)
                .pipe(pl_pipe_update, update_df.cast(mod_cast), keys)
                .cast(orig_cast)
            )
            del update_df
            del s3_update_df
            drop_indices = pl.concat([drop_indices, insert_df.get_column("odin_index")])

        if delete_df.height > 0:
            s3_delete_df = ds_batched_join(fact_ds, delete_df, keys, self.batch_size)
            drop_indices = pl.concat([drop_indices, s3_delete_df.get_column("odin_index")])
            del s3_delete_df

        sync_filter = ~pc.field("odin_index").isin(drop_indices.to_arrow())
        if self.part_columns:
            part_columns = self.part_columns
        else:
            part_columns = None
        insert_path = os.path.join(self.tmpdir, "temp_insert.parquet")
        insert_df.write_parquet(insert_path)
        sync_paths = pq_dataset_writer(
            fact_ds.filter(sync_filter),
            partition_columns=part_columns,
            export_folder=self.tmpdir,
            export_file_prefix="temp_sync",
        )
        sync_paths.append(insert_path)

        # Create new merged parquet file(s)
        new_paths = pq_dataset_writer(
            source=ds_from_path(sync_paths),
            partition_columns=part_columns,
            export_folder=os.path.join(self.tmpdir, self.s3_export),
            export_file_prefix="year",
        )

        # Check for sigterm before upload (can't be un-done)
        sigterm_check()
        for new_path in new_paths:
            move_path = new_path.replace(f"{self.tmpdir}/", "")
            upload_file(new_path, move_path)

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
                self.load_new_snapshot()
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
