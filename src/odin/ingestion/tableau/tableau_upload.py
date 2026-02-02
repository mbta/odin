import os
import boto3
import pyarrow
from pyarrow import parquet as pq
from pyarrow import fs
import pyarrow.dataset as pad
import polars as pl
import sched
import json
from typing import Any, Dict, Set, TypedDict, List
import tableauserverclient as TSC
from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    SqlType,
    TableDefinition,
    Telemetry,
    escape_string_literal,
)

from odin.job import OdinJob
from odin.job import job_proc_schedule
from odin.utils.logger import ProcessLog
from odin.utils.aws.s3 import list_objects, list_partitions
from odin.utils.parquet import file_column_stats
from odin.utils.locations import DATA_SPRINGBOARD

# Scheduling constants
NEXT_RUN_DEFAULT = 60 * 60 * 4  # 4 hours
NEXT_RUN_IMMEDIATE = 60 * 5  # 5 minutes
NEXT_RUN_LONG = 60 * 60 * 12  # 12 hours

# S3 Configuration
CHECKPOINT_FILE_KEY = "odin/state/tableau_checkpoints.json"
BATCH_SIZE = 500_000  # Number of rows per Hyper file batch

# Tables to sync
TABLES_TO_SYNC = [
    "EDW.ABP_TAP",
    "EDW.JOURNAL_ENTRY",
]


class TableConfig(TypedDict):
    """Rules for casting/dropping columns and the index high watermark column."""

    casts: Dict[str, type[pl.DataType]]
    drops: Set[str]
    index_column: str | None


# This dictionary defines and tracks column-specific rules for each table, including:
# - casts: Data type casts when convert_parquet_dtype() does not handle a column the way we want
# - drops: Columns to drop from hyper files, for data sensitivity, relevance, or size reasons
# - index_column: The column used for incremental updating. High watermark tracks largest value of
#       index_column successfully synced to Tableau
TABLE_CONFIG: Dict[str, TableConfig] = {
    "EDW.ABP_TAP": {
        "casts": {
            "token_id": pl.Int64,
        },
        "drops": set(),
        "index_column": "tap_id",
    },
    "EDW.JOURNAL_ENTRY": {
        "casts": {
            "line_item_nbr": pl.Int64,
        },
        "drops": set(["restricted_purse_id"]),
        "index_column": "journal_entry_key",
    },
}


def convert_parquet_dtype(dtype: pyarrow.DataType) -> SqlType:
    """
    Map Parquet data types to Tableau Hyper data types
    Modified from LAMP codebase

    :param dtype: PyArrow data type
    :return: Tableau Hyper SqlType
    """
    dtype_str = str(dtype)
    dtype_map = {
        "int8": SqlType.small_int(),
        "uint8": SqlType.small_int(),
        "int16": SqlType.small_int(),
        "uint16": SqlType.int(),
        "int32": SqlType.int(),
        "uint32": SqlType.big_int(),
        "int64": SqlType.big_int(),
        "bool": SqlType.bool(),
        "halffloat": SqlType.double(),
        "float": SqlType.double(),
        "double": SqlType.double(),
    }
    map_check = dtype_map.get(dtype_str)
    if map_check is not None:
        return map_check

    if dtype_str.startswith("date32"):
        return SqlType.date()

    if dtype_str.startswith("timestamp"):
        return SqlType.timestamp()

    return SqlType.text()


def parquet_schema_to_hyper_definition(parquet_path: str, table_name: str) -> TableDefinition:
    """
    Create Tableau Hyper table definition from parquet schema.

    :param parquet_path: Path to parquet file
    :param table_name: Name for the Hyper table
    :return: TableDefinition for Hyper
    """
    schema = pq.read_schema(parquet_path, filesystem=fs.LocalFileSystem())

    columns = [
        TableDefinition.Column(field.name, convert_parquet_dtype(field.type)) for field in schema
    ]
    return TableDefinition(table_name="public." + table_name.replace(" ", "_"), columns=columns)


def build_hyper_from_parquet(
    parquet_filepath: str, hyper_filepath: str, table_def: TableDefinition
) -> int:
    """
    Build Tableau Hyper extract from Parquet file.

    :param parquet_filepath: Path to source parquet file
    :param hyper_filepath: Path for output hyper file
    :param table_def: Tableau table definition
    :return: Number of rows in the extract
    """
    with HyperProcess(
        telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU, parameters={"log_config": ""}
    ) as hyper:
        with Connection(
            endpoint=hyper.endpoint,
            database=hyper_filepath,
            create_mode=CreateMode.CREATE_AND_REPLACE,
        ) as conn:
            conn.catalog.create_table(table_definition=table_def)
            copy_cmd = (
                f"COPY {table_def.table_name} "
                f"FROM {escape_string_literal(parquet_filepath)} "
                "WITH (FORMAT PARQUET)"
            )
            row_count = conn.execute_command(copy_cmd)

    return row_count


def resolve_project_id(server: TSC.Server, path: str) -> str:
    """
    Resolve a Tableau project path to its ID.

    :param server: Tableau server client
    :param path: Project path (e.g., "Technology Innovation/Odin")
    :return: Project ID
    """
    options = TSC.RequestOptions(pagesize=1000)
    projects, _ = server.projects.get(options)
    parent = None
    for name in path.split("/"):
        parent_id = parent.id if parent is not None else None
        parent = next(
            (proj for proj in projects if proj.name == name and proj.parent_id == parent_id),
            None,
        )
        if parent is None:
            raise RuntimeError(f"Project '{name}' not found in path '{path}'")

    if parent is None:
        raise RuntimeError(f"No projects found in path '{path}'")

    return parent.id


class TableauUpload(OdinJob):
    """
    Sync Parquet data to Tableau Server via Hyper extracts.

    This job:
    1. Discovers all parquet files across all hive partitions for a table
    2. Uses metadata to filter files that might contain new data (based on watermark)
    3. Reads only the relevant data directly from S3 without downloading full files
    4. Batches the data and publishes to Tableau
    """

    def __init__(self, table: str, overwrite_table: bool = False) -> None:
        """
        Create TableauUpload instance.

        :param table: Name of the table to sync (e.g., "EDW.ABP_TAP")
        :param overwrite_table: If True, overwrite existing data instead of incremental update
        """
        self.table = table
        self.overwrite_table = overwrite_table
        self.s3_prefix = f"s3://{DATA_SPRINGBOARD}/odin/data/cubic/ods/{table}/"
        self.rules = TABLE_CONFIG.get(table, {"casts": {}, "drops": set(), "index_column": None})
        self.start_kwargs = {"table": table}

    def discover_partitioned_files(self) -> List[str]:
        """
        Discover all parquet files across all hive partitions for a table.

        :return: List of S3 paths to all parquet files
        """
        log = ProcessLog("discover_partitioned_files", table=self.table, s3_prefix=self.s3_prefix)

        # List all partitions (e.g., odin_year=2023, odin_year=2024)
        partitions = list_partitions(self.s3_prefix)
        log.add_metadata(partition_count=len(partitions), partitions=str(partitions))

        all_files: List[str] = []
        for partition in partitions:
            partition_path = f"{self.s3_prefix}{partition}/"
            objects = list_objects(partition_path, in_filter=".parquet")
            all_files.extend([obj.path for obj in objects])

        log.complete(total_files_discovered=len(all_files))
        return all_files

    def filter_files_by_metadata(
        self, file_paths: List[str], index_column: str, watermark: Any
    ) -> List[str]:
        """
        Filter parquet files to only those that might contain data above the watermark.

        Uses parquet metadata (row group statistics) to efficiently determine which files
        may contain new data without reading the actual data.

        :param file_paths: List of S3 paths to parquet files
        :param index_column: Column name used for watermarking
        :param watermark: Current watermark value (only data > watermark is needed)
        :return: List of file paths that may contain data above the watermark
        """
        log = ProcessLog(
            "filter_files_by_metadata",
            table=self.table,
            index_column=index_column,
            watermark=str(watermark),
            input_file_count=len(file_paths),
        )

        if watermark is None:
            log.complete(result="no_watermark_set", filtered_file_count=len(file_paths))
            return file_paths

        filtered_files: List[str] = []
        skipped_missing_column = 0
        skipped_below_watermark = 0
        metadata_read_errors = 0

        s3_fs = fs.S3FileSystem()

        for file_path in file_paths:
            try:
                # Remove s3:// prefix for PyArrow S3FileSystem
                s3_path = file_path.replace("s3://", "")
                pq_file = pq.ParquetFile(s3_path, filesystem=s3_fs)
                metadata = pq_file.metadata

                # Check if index_column exists in this file's schema
                schema_names = pq_file.schema_arrow.names
                if index_column not in schema_names:
                    skipped_missing_column += 1
                    continue

                # Get column statistics across all row groups
                file_max = None
                col_stats = file_column_stats(metadata, index_column)
                for rg_stats in col_stats:
                    if rg_stats["has_min_max"] and rg_stats["max"] is not None:
                        if file_max is None or rg_stats["max"] > file_max:
                            file_max = rg_stats["max"]

                if file_max is not None and file_max > watermark:
                    filtered_files.append(file_path)
                else:
                    skipped_below_watermark += 1

                pq_file.close()

            except Exception as e:
                # If we can't read metadata, include the file to be safe
                log.add_metadata(metadata_read_error=f"{file_path}: {e}")
                metadata_read_errors += 1
                filtered_files.append(file_path)

        log.complete(
            filtered_file_count=len(filtered_files),
            skipped_missing_column=skipped_missing_column,
            skipped_below_watermark=skipped_below_watermark,
            metadata_read_errors=metadata_read_errors,
        )
        return filtered_files

    def load_filtered_data_from_s3(
        self,
        file_paths: List[str],
        watermark: Any,
    ) -> pl.LazyFrame:
        """
        Load data from S3 parquet files, applying table configuration rules and watermark filtering.

        Uses PyArrow dataset with hive partitioning to efficiently read from S3
        without downloading files to local disk.

        :param file_paths: List of S3 paths to parquet files to read
        :param watermark: Current watermark value for filtering
        :return: LazyFrame with data filtered according to watermark and table rules
        """
        log = ProcessLog(
            "load_filtered_data_from_s3",
            table=self.table,
            file_count=len(file_paths),
            watermark=str(watermark) if watermark is not None else "None",
        )

        if not file_paths:
            log.complete(result="empty_file_paths")
            return pl.LazyFrame()

        # Create S3 filesystem for PyArrow
        s3_fs = fs.S3FileSystem()

        # Strip s3:// prefix for PyArrow - it expects paths without the scheme
        s3_paths = [p.replace("s3://", "") for p in file_paths]

        # Create a PyArrow dataset from the S3 paths with hive partitioning
        ds = pad.dataset(
            s3_paths,
            format="parquet",
            partitioning="hive",
            filesystem=s3_fs,
        )

        # Convert to Polars LazyFrame for efficient processing
        lf = pl.scan_pyarrow_dataset(ds)

        # Apply table-specific configuration rules
        schema = lf.collect_schema()
        columns = schema.names()

        # Apply casts
        cast_exprs = [
            pl.col(col).cast(dtype, strict=False)
            for col, dtype in self.rules["casts"].items()
            if col in columns
        ]
        if cast_exprs:
            lf = lf.with_columns(*cast_exprs)
            log.add_metadata(casts_applied=len(cast_exprs))

        # Drop columns
        drop_cols = [col for col in self.rules["drops"] if col in columns]
        if drop_cols:
            lf = lf.drop(drop_cols)
            log.add_metadata(columns_dropped=len(drop_cols))

        # Apply watermark filter if applicable
        watermark_filter_applied = False
        if (
            watermark is not None
            and self.rules["index_column"]
            and self.rules["index_column"] in columns
        ):
            lf = lf.filter(pl.col(self.rules["index_column"]) > watermark)
            watermark_filter_applied = True

        log.complete(watermark_filter_applied=str(watermark_filter_applied))
        return lf

    def get_latest_watermark(self) -> Any | None:
        """
        Retrieve the max value of the index column from S3 state store.

        :return: Watermark value or None if not found
        """
        log = ProcessLog(
            "get_latest_watermark",
            table=self.table,
            s3_path=f"s3://{DATA_SPRINGBOARD}/{CHECKPOINT_FILE_KEY}",
        )
        s3 = boto3.client("s3")
        try:
            response = s3.get_object(Bucket=DATA_SPRINGBOARD, Key=CHECKPOINT_FILE_KEY)
            state = json.loads(response["Body"].read().decode("utf-8"))
            watermark = state.get(self.table)
            log.complete(watermark=str(watermark) if watermark is not None else "None")
            return watermark
        except s3.exceptions.NoSuchKey:
            log.complete(result="no_checkpoint_file")
            return None
        except Exception as e:
            log.failed(e)
            return None

    def update_watermark(self, new_value: Any) -> None:
        """
        Update the max value for a table in the S3 state store.

        :param new_value: New watermark value
        """
        log = ProcessLog(
            "update_watermark",
            table=self.table,
            new_value=str(new_value) if new_value is not None else "None",
        )

        if new_value is None:
            log.complete(result="skipped_null_value")
            return

        s3 = boto3.client("s3")
        try:
            # Read current state first to preserve other tables
            try:
                response = s3.get_object(Bucket=DATA_SPRINGBOARD, Key=CHECKPOINT_FILE_KEY)
                state = json.loads(response["Body"].read().decode("utf-8"))
            except s3.exceptions.NoSuchKey:
                state = {}

            state[self.table] = new_value

            s3.put_object(
                Bucket=DATA_SPRINGBOARD,
                Key=CHECKPOINT_FILE_KEY,
                Body=json.dumps(state, default=str),
            )
            log.complete(result="success")
        except Exception as e:
            log.failed(e)

    def publish_hyper_to_tableau(
        self,
        hyper_filepath: str,
        datasource_name: str,
        project_name: str,
        server_url: str,
        publish_mode: str = TSC.Server.PublishMode.Overwrite,
    ) -> None:
        """
        Publish Tableau Hyper extract to Tableau Server.

        :param hyper_filepath: Path to the hyper file
        :param datasource_name: Name for the datasource in Tableau
        :param project_name: Tableau project path
        :param server_url: Tableau server URL
        :param publish_mode: Publish mode (Overwrite or Append)
        """
        log = ProcessLog(
            "publish_hyper_to_tableau",
            datasource_name=datasource_name,
            project_name=project_name,
            server_url=server_url,
            publish_mode=str(publish_mode),
        )

        try:
            tableau_auth = TSC.PersonalAccessTokenAuth(
                os.environ["TABLEAU_PERSONAL_ACCESS_TOKEN_NAME"],
                os.environ["TABLEAU_PERSONAL_ACCESS_TOKEN_SECRET"],
            )

            server = TSC.Server(server_url, use_server_version=True, http_options={"verify": False})
            with server.auth.sign_in(tableau_auth):
                project_id = resolve_project_id(server, project_name)
                datasource_item = TSC.DatasourceItem(project_id=project_id, name=datasource_name)
                server.datasources.publish(datasource_item, hyper_filepath, mode=publish_mode)

            log.complete(result="success")
        except Exception as e:
            log.failed(e)
            raise

    def run(self) -> int:
        """
        Run the Tableau upload job.

        This method:
        1. Discovers parquet files for the table
        2. Filters files based on watermark metadata
        3. Loads and transforms the data
        4. Publishes to Tableau in batches

        :return: Seconds until next run
        """
        self.start_kwargs = {"table": self.table, "overwrite": str(self.overwrite_table)}
        log = ProcessLog(
            "run",
            table=self.table,
            overwrite=str(self.overwrite_table),
        )

        # Get watermark for incremental processing
        watermark = None
        if self.overwrite_table:
            log.add_metadata(watermark_status="skipped_overwrite_mode")
        elif self.rules["index_column"]:
            watermark = self.get_latest_watermark()
            log.add_metadata(current_watermark=str(watermark) if watermark is not None else "None")

        # Step 1: Discover all parquet files across all hive partitions
        all_files = self.discover_partitioned_files()

        if not all_files:
            log.complete(result="no_parquet_files_found")
            return NEXT_RUN_LONG

        # Step 2: Filter files based on metadata (only if we have a watermark and index column)
        if watermark is not None and self.rules["index_column"]:
            filtered_files = self.filter_files_by_metadata(
                all_files, self.rules["index_column"], watermark
            )
        else:
            filtered_files = all_files

        if not filtered_files:
            log.complete(result="no_files_above_watermark")
            return NEXT_RUN_LONG

        # Step 3: Load filtered data directly from S3 (no local download needed)
        lf = self.load_filtered_data_from_s3(filtered_files, watermark)

        # Calculate new max watermark from the filtered data (before batching)
        new_watermark = None
        if self.rules["index_column"]:
            try:
                new_watermark = lf.select(pl.col(self.rules["index_column"]).max()).collect().item()
            except Exception as e:
                log.add_metadata(watermark_calculation_error=str(e))

        # Calculate total rows to process
        total_rows = lf.select(pl.len()).collect().item()

        if total_rows == 0:
            log.complete(result="no_new_data")
            return NEXT_RUN_LONG

        log.add_metadata(total_rows=total_rows, batch_size=BATCH_SIZE)

        # Setup Tableau Environment
        server_url = os.environ.get("TABLEAU_SERVER_URL", "")
        project_name = os.environ.get("TABLEAU_WORKBOOK_PROJECT", "")

        # Process in batches
        total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
        for offset in range(0, total_rows, BATCH_SIZE):
            batch_num = offset // BATCH_SIZE + 1
            batch_log = ProcessLog(
                "process_batch",
                table=self.table,
                batch_num=batch_num,
                total_batches=total_batches,
                offset=offset,
                batch_end=min(offset + BATCH_SIZE, total_rows),
            )

            # Slice the LazyFrame and materialize the batch
            chunk_df = lf.slice(offset, BATCH_SIZE).collect()

            chunk_parquet_path = os.path.join(self.tmpdir, f"chunk_{offset}_{self.table}.parquet")
            chunk_hyper_path = os.path.join(self.tmpdir, f"chunk_{offset}_{self.table}.hyper")

            # Write temp parquet for this batch
            chunk_df.write_parquet(chunk_parquet_path, compression="snappy")

            # Create Tableau Hyper table definition from this chunk's schema
            table_def = parquet_schema_to_hyper_definition(chunk_parquet_path, self.table)

            # Build Hyper extract for this batch
            row_count = build_hyper_from_parquet(chunk_parquet_path, chunk_hyper_path, table_def)
            batch_log.add_metadata(hyper_row_count=row_count)

            # Determine Publish Mode
            # If watermark is None (Full Refresh):
            #   - First batch: Overwrite
            #   - Subsequent batches: Append
            # If watermark has value (Incremental):
            #   - All batches: Append
            if self.overwrite_table or (watermark is None and offset == 0):
                mode = TSC.Server.PublishMode.Overwrite
            else:
                mode = TSC.Server.PublishMode.Append

            batch_log.add_metadata(publish_mode=str(mode))
            self.publish_hyper_to_tableau(
                chunk_hyper_path,
                self.table,
                project_name,
                server_url,
                publish_mode=mode,
            )

            # Cleanup batch files
            if os.path.exists(chunk_parquet_path):
                os.remove(chunk_parquet_path)
            if os.path.exists(chunk_hyper_path):
                os.remove(chunk_hyper_path)

            batch_log.complete()

        # Update state if successful and we have a new max value
        if new_watermark is not None:
            self.update_watermark(new_watermark)

        log.complete(
            new_watermark=str(new_watermark) if new_watermark is not None else "None",
            batches_processed=total_batches,
        )
        return NEXT_RUN_DEFAULT


def schedule_tableau_upload(schedule: sched.scheduler) -> None:
    """
    Schedule All Jobs for Tableau upload process.

    :param schedule: application scheduler
    """
    for table in TABLES_TO_SYNC:
        job = TableauUpload(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
