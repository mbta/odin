import os
import boto3
import pyarrow
from pyarrow import parquet as pq
from pyarrow import fs
import polars as pl
import argparse
import json
from typing import Any, Dict, Set, TypedDict
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

S3_BUCKET = "mbta-ctd-dataplatform-local"  # TODO: using personal bucket for testing
BATCH_SIZE = 500_000  # Number of rows per Hyper file batch

TABLES_TO_SYNC = [
    # "EDW.JOURNAL_ENTRY",
    # "EDW.DEVICE_EVENT", # large table, only retest if necessary
    "EDW.TEST_TRAIN_DATA",  # test table for incremental uploads
]


class ScrubRule(TypedDict):
    """Rules for casting/dropping columns and the index watermark column."""

    casts: Dict[str, Any]
    drops: Set[str]
    index_column: str | None


# This dictionary defines and tracks column-specific rules for each table, including:
# - casts: Data type casts when convert_parquet_dtype() does not handle a column the way we want
# - drops: Columns to drop from hyper files, for data sensitivity, relevance, or size reasons
# - index_column: The column used for incremental updating. Watermark tracks largest value of index
#       column successfully synced to Tableau
SCRUB_RULES: Dict[str, ScrubRule] = {
    "EDW.JOURNAL_ENTRY": {
        "casts": {
            "line_item_nbr": pl.Int64,
        },
        "drops": set(["restricted_purse_id"]),
        "index_column": "job_id",
    },
    "EDW.DEVICE_EVENT": {
        "casts": {
            "component_serial_nbr": pl.Int64,
            "device_transaction_id": pl.Int64,
        },
        "drops": set(),
        "index_column": "job_id",
    },
    "EDW.TEST_TRAIN_DATA": {
        "casts": {},
        "drops": set(),
        "index_column": "job_id",
    },
}

STATE_FILE_KEY = "odin/state/tableau_watermarks.json"


def download_parquet(local_path: str, s3_bucket: str, s3_object_key: str) -> None:
    print(f"Downloading Parquet file from s3://{s3_bucket}/{s3_object_key} to {local_path}...")
    s3_client = boto3.client("s3")
    s3_client.download_file(Bucket=s3_bucket, Key=s3_object_key, Filename=local_path)


def get_latest_watermark(table_name: str, index_column: str) -> Any | None:
    """Retrieve the max value of the index column from S3 state store"""
    s3 = boto3.client("s3")
    try:
        print(f"Fetching watermark for {table_name} from s3://{S3_BUCKET}/{STATE_FILE_KEY}")
        response = s3.get_object(Bucket=S3_BUCKET, Key=STATE_FILE_KEY)
        state = json.loads(response["Body"].read().decode("utf-8"))
        return state.get(table_name)
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        print(f"Warning: Could not read watermark state: {e}")
        return None


def update_watermark(table_name: str, new_value: Any) -> None:
    """Update the max value for a table in the S3 state store"""
    if new_value is None:
        return

    s3 = boto3.client("s3")
    try:
        # Read current state first to preserve other tables
        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=STATE_FILE_KEY)
            state = json.loads(response["Body"].read().decode("utf-8"))
        except s3.exceptions.NoSuchKey:
            state = {}

        state[table_name] = new_value

        s3.put_object(Bucket=S3_BUCKET, Key=STATE_FILE_KEY, Body=json.dumps(state, default=str))
        print(f"Updated watermark for {table_name} to {new_value}")
    except Exception as e:
        print(f"Error updating watermark state: {e}")


def convert_parquet_dtype(dtype: pyarrow.DataType) -> SqlType:
    """
    Map Parquet data types to Tableau Hyper data types
    Modified from LAMP codebase
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
    """Create Tableau Hyper table definition from parquet schema"""
    schema = pq.read_schema(parquet_path, filesystem=fs.LocalFileSystem())

    columns = [
        TableDefinition.Column(field.name, convert_parquet_dtype(field.type)) for field in schema
    ]
    return TableDefinition(table_name="public." + table_name.replace(" ", "_"), columns=columns)


def build_hyper_from_parquet(
    parquet_filepath: str, hyper_filepath: str, table_def: TableDefinition
) -> None:
    """Build Tableau Hyper extract from Parquet file"""
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

    print(f"Hyper extract contains {row_count} rows")


def resolve_project_id(server, path):
    """Resolve a Tableau project path to its ID."""
    options = TSC.RequestOptions(pagesize=1000)
    projects, _ = server.projects.get(options)
    parent = None
    for name in path.split("/"):
        parent = next(
            (
                proj
                for proj in projects
                if proj.name == name and proj.parent_id == (parent.id if parent else None)
            ),
            None,
        )
        if parent is None:
            raise RuntimeError(f"Project '{name}' not found in path '{path}'")

    return parent.id


def publish_hyper_to_tableau(
    hyper_filepath: str,
    datasource_name: str,
    project_name: str,
    server_url: str,
    publish_mode: str = TSC.Server.PublishMode.Overwrite,
) -> None:
    """Publish Tableau Hyper extract to Tableau Server"""
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


def get_scrubbed_lazyframe(parquet_path: str, rules: ScrubRule) -> pl.LazyFrame:
    """Create a LazyFrame with casts/drops applied according to SCRUB_RULES."""
    lf = pl.scan_parquet(parquet_path)

    schema = lf.collect_schema()
    columns = schema.names()

    cast_exprs = [
        pl.col(col).cast(dtype, strict=False)
        for col, dtype in rules["casts"].items()
        if col in columns
    ]
    if cast_exprs:
        lf = lf.with_columns(*cast_exprs)

    drop_cols = [col for col in rules["drops"] if col in columns]
    if drop_cols:
        lf = lf.drop(drop_cols)

    return lf


def upload_tableau_table(table_name: str, overwrite_table: bool = False) -> None:
    """Upload a single table to Tableau, optionally forcing an overwrite."""
    s3_object_key = f"talexander3/test/{table_name}_BC.parquet"

    parquet_filepath = f"{table_name}.parquet"

    # Download Parquet file from S3
    download_parquet(parquet_filepath, S3_BUCKET, s3_object_key)

    # Get rules and watermark
    rules = SCRUB_RULES.get(table_name, {"casts": {}, "drops": set(), "index_column": None})
    watermark = None
    if overwrite_table:
        print(
            f"Overwrite requested for {table_name}; skipping watermark fetch "
            "and publishing overwrite."
        )
    elif rules["index_column"]:
        watermark = get_latest_watermark(table_name, rules["index_column"])
        print(f"Current watermark: {watermark}")

    # Prepare LazyFrame with scrubbing rules
    lf = get_scrubbed_lazyframe(parquet_filepath, rules)

    # Filter for new data if watermark exists
    if watermark is not None and rules["index_column"]:
        print(f"Filtering for data where {rules['index_column']} > {watermark}")
        lf = lf.filter(pl.col(rules["index_column"]) > watermark)

    # Calculate new max watermark from the filtered data (before batching)
    new_watermark = None
    if rules["index_column"]:
        try:
            # Calculate max of the new data we are about to process
            new_watermark = lf.select(pl.col(rules["index_column"]).max()).collect().item()
        except Exception as e:
            print(f"Could not calculate new watermark: {e}")

    # Calculate total rows to process
    total_rows = lf.select(pl.len()).collect().item()

    if total_rows == 0:
        print(f"No new data found for {table_name}")
        if os.path.exists(parquet_filepath):
            os.remove(parquet_filepath)
        return

    print(f"Processing {total_rows} rows for {table_name} in batches of {BATCH_SIZE}...")

    # Setup Tableau Environment
    os.environ["TABLEAU_SERVER_URL"] = "https://awdatatest.mbta.com/"  # hardcoding test server
    os.environ["TABLEAU_SITE_ID"] = ""  # default site
    os.environ["TABLEAU_WORKBOOK_PROJECT"] = "Technology Innovation/odin_rest_api_test"

    # Process in batches
    for offset in range(0, total_rows, BATCH_SIZE):
        batch_num = offset // BATCH_SIZE + 1
        print(
            f"Preparing batch {batch_num} (rows {offset} to "
            f"{min(offset + BATCH_SIZE, total_rows)})..."
        )

        # Slice the LazyFrame and materialize the batch
        # slice(offset, length)
        chunk_df = lf.slice(offset, BATCH_SIZE).collect()

        chunk_parquet_path = f"chunk_{offset}_{parquet_filepath}"
        chunk_hyper_path = f"chunk_{offset}_{table_name}.hyper"

        # Write temp parquet for this batch
        chunk_df.write_parquet(chunk_parquet_path, compression="snappy")

        # Create Tableau Hyper table definition from this chunk's schema
        table_def = parquet_schema_to_hyper_definition(chunk_parquet_path, table_name)

        # Build Hyper extract for this batch
        build_hyper_from_parquet(chunk_parquet_path, chunk_hyper_path, table_def)

        # Determine Publish Mode
        # If watermark is None (Full Refresh):
        #   - First batch: Overwrite
        #   - Subsequent batches: Append
        # If watermark has value (Incremental):
        #   - All batches: Append
        if overwrite_table or (watermark is None and offset == 0):
            mode = TSC.Server.PublishMode.Overwrite
        else:
            mode = TSC.Server.PublishMode.Append

        print(f"Publishing batch {batch_num} to Tableau (Mode: {mode})...")
        publish_hyper_to_tableau(
            chunk_hyper_path,
            table_name,
            os.environ["TABLEAU_WORKBOOK_PROJECT"],
            os.environ["TABLEAU_SERVER_URL"],
            publish_mode=mode,
        )

        # Cleanup batch files
        if os.path.exists(chunk_parquet_path):
            os.remove(chunk_parquet_path)
        if os.path.exists(chunk_hyper_path):
            os.remove(chunk_hyper_path)

    # Update state if successful and we have a new max value
    if new_watermark is not None:
        update_watermark(table_name, new_watermark)

    # Cleanup source file
    if os.path.exists(parquet_filepath):
        os.remove(parquet_filepath)


def run_tableau_uploads(overwrite_table: bool = False) -> None:
    """Run uploads for all configured tables."""
    for table_name in TABLES_TO_SYNC:
        print(f"Processing table: {table_name}")
        upload_tableau_table(table_name, overwrite_table=overwrite_table)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish Parquet data to Tableau extracts")
    parser.add_argument(
        "--overwrite-table",
        action="store_true",
        help="Overwrite the destination table instead of appending incrementally",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_tableau_uploads(overwrite_table=args.overwrite_table)
