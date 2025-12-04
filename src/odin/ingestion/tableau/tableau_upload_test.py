import os
import boto3
import pyarrow
from pyarrow import parquet as pq
from pyarrow import fs
import polars as pl
from datetime import datetime
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

S3_BUCKET = "mbta-ctd-dataplatform-springboard"  # TODO: move to env var

TABLES_TO_SYNC = [
    "EDW.JOURNAL_ENTRY",
    "EDW.DEVICE_EVENT",
]

# This dictionary defines and tracks column-specific rules for each table, including:
# - Data type casts when convert_parquet_dtype() does not handle a column the way we want
# - Columns to drop from hyper files, for data sensitivity, relevance, or size reasons
SCRUB_RULES = {
    "EDW.JOURNAL_ENTRY": {
        "casts": {
            "line_item_nbr": pl.Int64,
        },
        "drops": {"restricted_purse_id"},
    },
    "EDW.DEVICE_EVENT": {
        "casts": {
            "component_serial_nbr": pl.Int64,
            "device_transaction_id": pl.Int64,
        },
        "drops": {},
    },
}


def download_parquet(local_path: str, s3_bucket: str, s3_object_key: str) -> None:
    print(f"Downloading Parquet file from s3://{s3_bucket}/{s3_object_key} to {local_path}...")
    s3_client = boto3.client("s3")
    s3_client.download_file(Bucket=s3_bucket, Key=s3_object_key, Filename=local_path)


def convert_parquet_dtype(dtype: pyarrow.DataType) -> SqlType:
    """
    Map Parquet data types to Tableau Hyper data types
    Modified from LAMP codebase
    """
    dtype = str(dtype)
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
    map_check = dtype_map.get(dtype)
    if map_check is not None:
        return map_check

    if dtype.startswith("date32"):
        return SqlType.date()

    if dtype.startswith("timestamp"):
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
    """
    Given a path, resolve Tableau project and return its ID.
    Necessary for nested projects, as project names are not unique in Tableau.
    """
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
        server.datasources.publish(
            datasource_item, hyper_filepath, mode=TSC.Server.PublishMode.Overwrite
        )


def scrub_parquet(parquet_path: str, datasource_name: str) -> str:
    """
    Cast/drop columns in Parquet file according to SCRUB_RULES
    Processes parquet files in memory with Polars
    Larger files/updates should use a streaming approach instead.
    """
    rules = SCRUB_RULES.get(datasource_name, {"casts": {}, "drops": set()})
    df = pl.read_parquet(parquet_path)

    cast_exprs = [
        pl.col(col).cast(dtype, strict=False)
        for col, dtype in rules["casts"].items()
        if col in df.columns
    ]
    if cast_exprs:
        df = df.with_columns(*cast_exprs)

    drop_cols = [col for col in rules["drops"] if col in df.columns]
    if drop_cols:
        df = df.drop(drop_cols)

    output_path = f"casted_{parquet_path}"
    df.write_parquet(output_path, compression="snappy")
    return output_path


def upload_tableau_table(table_name: str) -> None:
    s3_object_key = f"odin/data/cubic/ods/{table_name}/odin_year=2023/year_001.parquet"  # using older/smaller subset for testing

    parquet_filepath = f"{table_name}.parquet"
    hyper_filepath = f"{table_name}.hyper"

    # Download Parquet file from S3
    download_parquet(parquet_filepath, S3_BUCKET, s3_object_key)

    # Cast/scrub columns for Hyper ingestion
    scrubbed_filepath = scrub_parquet(parquet_filepath, table_name)

    # Create incremental update logic (TODO)
    # For testing, taking small chunk of data and overwriting existing datasources
    if table_name == "EDW.DEVICE_EVENT":
        df = pl.read_parquet(scrubbed_filepath)
        df.filter(pl.col("staging_inserted_dtm") > datetime(2023, 10, 6, 0, 0, 0)).write_parquet(
            scrubbed_filepath, compression="snappy"
        )

    # Create Tableau Hyper table definition from Parquet schema
    table_def = parquet_schema_to_hyper_definition(scrubbed_filepath, table_name)

    # Build Hyper extract
    build_hyper_from_parquet(scrubbed_filepath, hyper_filepath, table_def)

    # Publish to Tableau
    os.environ["TABLEAU_SERVER_URL"] = "https://awdatatest.mbta.com/"  # hardcoding test server
    os.environ["TABLEAU_SITE_ID"] = ""  # default site
    os.environ["TABLEAU_WORKBOOK_PROJECT"] = "Technology Innovation/odin_rest_api_test"

    publish_hyper_to_tableau(
        hyper_filepath,
        table_name,
        os.environ["TABLEAU_WORKBOOK_PROJECT"],
        os.environ["TABLEAU_SERVER_URL"],
    )


def run_tableau_uploads():
    for table_name in TABLES_TO_SYNC:
        print(f"Processing table: {table_name}")
        upload_tableau_table(table_name)


if __name__ == "__main__":
    run_tableau_uploads()
