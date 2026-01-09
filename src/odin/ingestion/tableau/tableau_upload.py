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
from odin.utils.logger import LOGGER
from odin.utils.aws.s3 import list_objects, list_partitions
from odin.utils.parquet import file_column_stats
from odin.utils.locations import DATA_SPRINGBOARD

# Scheduling constants
NEXT_RUN_DEFAULT = 60 * 60 * 4  # 4 hours
NEXT_RUN_IMMEDIATE = 60 * 5  # 5 minutes
NEXT_RUN_LONG = 60 * 60 * 12  # 12 hours

# S3 Configuration
STATE_FILE_KEY = "odin/state/tableau_watermarks.json"
BATCH_SIZE = 500_000  # Number of rows per Hyper file batch

# Tables to sync
TABLES_TO_SYNC = [
    "EDW.JOURNAL_ENTRY",
    # "EDW.ABP_TAP",
]


class TableConfig(TypedDict):
    """Rules for casting/dropping columns and the index watermark column."""

    casts: Dict[str, Any]
    drops: Set[str]
    index_column: str | None


# This dictionary defines and tracks column-specific rules for each table, including:
# - casts: Data type casts when convert_parquet_dtype() does not handle a column the way we want
# - drops: Columns to drop from hyper files, for data sensitivity, relevance, or size reasons
# - index_column: The column used for incremental updating. Watermark tracks largest value of index
#       column successfully synced to Tableau
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
        ProcessLog("discover_partitions", table=self.table)
        LOGGER.info(f"Discovering partitions under {self.s3_prefix}")

        # List all partitions (e.g., odin_year=2023, odin_year=2024)
        partitions = list_partitions(self.s3_prefix)
        LOGGER.info(f"Found {len(partitions)} partition(s): {partitions}")

        all_files: List[str] = []
        for partition in partitions:
            partition_path = f"{self.s3_prefix}{partition}/"
            objects = list_objects(partition_path, in_filter=".parquet")
            all_files.extend([obj.path for obj in objects])

        LOGGER.info(f"Total parquet files discovered: {len(all_files)}")
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
        if watermark is None:
            LOGGER.info("No watermark set, all files will be processed")
            return file_paths

        ProcessLog("filter_files_by_metadata", table=self.table, watermark=str(watermark))
        LOGGER.info(f"Filtering files using metadata where {index_column} max > {watermark}")
        filtered_files: List[str] = []

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
                    LOGGER.info(f"Skipping {file_path}: column '{index_column}' not in schema")
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
                    LOGGER.info(f"Include {file_path}: max={file_max} > watermark={watermark}")
                else:
                    LOGGER.info(f"Skip {file_path}: max={file_max} <= watermark={watermark}")

                pq_file.close()

            except Exception as e:
                # If we can't read metadata, include the file to be safe
                LOGGER.warning(f"Could not read metadata for {file_path}: {e}")
                filtered_files.append(file_path)

        LOGGER.info(f"Files after metadata filtering: {len(filtered_files)} of {len(file_paths)}")
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
        if not file_paths:
            return pl.LazyFrame()

        ProcessLog("load_filtered_data", table=self.table, file_count=len(file_paths))
        LOGGER.info(f"Loading data from {len(file_paths)} S3 files")

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

        # Drop columns
        drop_cols = [col for col in self.rules["drops"] if col in columns]
        if drop_cols:
            lf = lf.drop(drop_cols)

        # Apply watermark filter if applicable
        if (
            watermark is not None
            and self.rules["index_column"]
            and self.rules["index_column"] in columns
        ):
            LOGGER.info(f"Applying filter: {self.rules['index_column']} > {watermark}")
            lf = lf.filter(pl.col(self.rules["index_column"]) > watermark)

        return lf

    def get_latest_watermark(self) -> Any | None:
        """
        Retrieve the max value of the index column from S3 state store.

        :return: Watermark value or None if not found
        """
        s3 = boto3.client("s3")
        try:
            LOGGER.info(
                f"Fetching watermark for {self.table} from s3://{DATA_SPRINGBOARD}/{STATE_FILE_KEY}"
            )
            response = s3.get_object(Bucket=DATA_SPRINGBOARD, Key=STATE_FILE_KEY)
            state = json.loads(response["Body"].read().decode("utf-8"))
            return state.get(self.table)
        except s3.exceptions.NoSuchKey:
            return None
        except Exception as e:
            LOGGER.warning(f"Could not read watermark state: {e}")
            return None

    def update_watermark(self, new_value: Any) -> None:
        """
        Update the max value for a table in the S3 state store.

        :param new_value: New watermark value
        """
        if new_value is None:
            return

        s3 = boto3.client("s3")
        try:
            # Read current state first to preserve other tables
            try:
                response = s3.get_object(Bucket=DATA_SPRINGBOARD, Key=STATE_FILE_KEY)
                state = json.loads(response["Body"].read().decode("utf-8"))
            except s3.exceptions.NoSuchKey:
                state = {}

            state[self.table] = new_value

            s3.put_object(Bucket=DATA_SPRINGBOARD, Key=STATE_FILE_KEY, Body=json.dumps(state, default=str))
            LOGGER.info(f"Updated watermark for {self.table} to {new_value}")
        except Exception as e:
            LOGGER.error(f"Error updating watermark state: {e}")

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
        tableau_auth = TSC.PersonalAccessTokenAuth(
            os.environ["TABLEAU_PERSONAL_ACCESS_TOKEN_NAME"],
            os.environ["TABLEAU_PERSONAL_ACCESS_TOKEN_SECRET"],
            os.environ["TABLEAU_SITE_ID"],
        )

        server = TSC.Server(server_url, use_server_version=True, http_options={"verify": False})
        with server.auth.sign_in(tableau_auth):
            project_id = resolve_project_id(server, project_name)
            datasource_item = TSC.DatasourceItem(project_id=project_id, name=datasource_name)
            server.datasources.publish(datasource_item, hyper_filepath, mode=publish_mode)

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

        # Get watermark for incremental processing
        watermark = None
        if self.overwrite_table:
            LOGGER.info(
                f"Overwrite requested for {self.table}; skipping watermark fetch "
                "and publishing overwrite."
            )
        elif self.rules["index_column"]:
            watermark = self.get_latest_watermark()
            LOGGER.info(f"Current watermark: {watermark}")

        # Step 1: Discover all parquet files across all hive partitions
        all_files = self.discover_partitioned_files()

        if not all_files:
            LOGGER.info(f"No parquet files found for table {self.table}")
            return NEXT_RUN_LONG

        # Step 2: Filter files based on metadata (only if we have a watermark and index column)
        if watermark is not None and self.rules["index_column"]:
            filtered_files = self.filter_files_by_metadata(
                all_files, self.rules["index_column"], watermark
            )
        else:
            filtered_files = all_files

        if not filtered_files:
            LOGGER.info(f"No files contain data above watermark for {self.table}")
            return NEXT_RUN_LONG

        # Step 3: Load filtered data directly from S3 (no local download needed)
        lf = self.load_filtered_data_from_s3(filtered_files, watermark)

        # Calculate new max watermark from the filtered data (before batching)
        new_watermark = None
        if self.rules["index_column"]:
            try:
                new_watermark = lf.select(pl.col(self.rules["index_column"]).max()).collect().item()
            except Exception as e:
                LOGGER.warning(f"Could not calculate new watermark: {e}")

        # Calculate total rows to process
        total_rows = lf.select(pl.len()).collect().item()

        if total_rows == 0:
            LOGGER.info(f"No new data found for {self.table}")
            return NEXT_RUN_LONG

        ProcessLog("process_batches", table=self.table, total_rows=total_rows)
        LOGGER.info(f"Processing {total_rows} rows for {self.table} in batches of {BATCH_SIZE}")

        # Setup Tableau Environment
        server_url = os.environ.get("TABLEAU_SERVER_URL", "https://awdatatest.mbta.com/")
        project_name = os.environ.get(
            "TABLEAU_WORKBOOK_PROJECT", "Technology Innovation/Odin"
        )

        # Process in batches
        for offset in range(0, total_rows, BATCH_SIZE):
            batch_num = offset // BATCH_SIZE + 1
            ProcessLog(
                "process_batch",
                table=self.table,
                batch_num=batch_num,
                offset=offset,
                batch_end=min(offset + BATCH_SIZE, total_rows),
            )
            LOGGER.info(
                f"Preparing batch {batch_num} (rows {offset} to "
                f"{min(offset + BATCH_SIZE, total_rows)})"
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
            LOGGER.info(f"Hyper extract contains {row_count} rows")

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

            LOGGER.info(f"Publishing batch {batch_num} to Tableau (Mode: {mode})")
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

        # Update state if successful and we have a new max value
        if new_watermark is not None:
            self.update_watermark(new_watermark)

        return NEXT_RUN_DEFAULT


def schedule_tableau_upload(schedule: sched.scheduler) -> None:
    """
    Schedule All Jobs for Tableau upload process.

    :param schedule: application scheduler
    """
    for table in TABLES_TO_SYNC:
        job = TableauUpload(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
