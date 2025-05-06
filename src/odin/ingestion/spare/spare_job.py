"""
Process data from the Spare API into parquet files.

Spare API JSON responses are polled and written into an incoming bucket by Delta.
This job processes those JSON responses into tabular data, and writes them into a parquet file.
That output file is located at {target_location}/{endpoint_name}.parquet

There is one job instance per Spare API endpoint.
So far, only one endpoint (vehicles) is supported. More will be added later.
Each time the job runs, it processes all files that Delta has created for the endpoint in a batch.

Note: Vehicle data changes rarely, and this file is optimized for that one case.
As other endpoints with other needs are added, different logic will be added.

Input files must have a timestamp in them.

The output parquet file is logically append-only.
Changes and deletions are appended to the end of the parquet file.
Unchanged data is ignored, so that the parquet file does not grow unnecessarily large.

Changes are based on the `id` field that Spare includes for every item in its API.
The job adds metadata columns for `ROW_TIMESTAMP` and `ROW_DELETED` to the parquet file to track
these changes and deletions.
To read these parquet files, look at only the latest row for each `id`.
(Because the file is append-only, the most recent row for each id is also the last in the file).
If the `ROW_DELETED` boolean is true, then that item doesn't exist anymore.
If `ROW_DELETED` is false, then the row contains the latest data for the id.
Older rows for each id contain edit history, but don't describe the final state of an item.

After running, any input files are moved to the archival bucket.
If this move fails, that will not cause problems, as processing is idempotent.
The next time the job runs, the inputs will be reprocessed (which will be a noop), then archived.

## Configuration:

The three locations specified in `config.toml` are passed as configuration to every job.
These can be s3 prefixes, like `s3://bucket/prefix`,
or (for local development) they can be paths on the local filesystem.

`source`, the job input, the incoming bucket where Delta puts the JSON response.
Files can be gzipped or raw json.

`target`, the location of the output parquet files, the springboard bucket.
This job also reads from the output file so that it can calculate changes and append to the file.

`archive`, where to move the input files to, once they have been processed.
Optional. If absent, input files are left where they are.

Each API endpoint is configured as a dictionary hardcoded in this file.
When the Odin application runs, it will create one instance per endpoint in this file.
"""

from dateutil.parser import isoparse
import json
import polars as pl
import os
import sched
from typing import Any, Dict, List, NotRequired, Tuple, TypeAlias, TypedDict

from odin.utils.logger import ProcessLog
import odin.ingestion.spare.spare_s3 as spare_s3
from odin.job import OdinJob
from odin.job import job_proc_schedule
import odin.utils.aws.s3 as s3


class Config(TypedDict):
    """Job config from config.toml, with paths"""

    source: str
    target: str
    archive: NotRequired[str]


def validate_config(config: Config) -> None:
    """Raise if config is invalid"""
    if "source" not in config or "target" not in config:
        raise RuntimeError(
            f'Spare job config requires "source" and "target". Got: {list(config.keys())}'
        )
    if "archive" in config:
        if not (config["source"].startswith("s3://") == config["archive"].startswith("s3://")):
            raise RuntimeError(
                f"Spare source and archive must both be in s3 or both local. Got source={
                    config['source']
                } archive={config['archive']}"
            )


Columns: TypeAlias = Dict[str, pl.DataType]


class ApiEndpoint(TypedDict):
    """Description of a Spare API endpoint."""

    # used to make the output parquet filename
    name: str
    # As written into the s3 key by Delta
    api_path: str
    # How to represent the json data from the API in a polars dataframe
    # Requires an `id` column
    # If there's a `metadata` column, it should be a string.
    columns: Columns


SPARE_API_ENDPOINTS: List[ApiEndpoint] = [
    {
        "name": "vehicles",
        "api_path": "vehicles_limit_20000",
        "columns": {
            "id": pl.String(),
            "identifier": pl.String(),
            "ownerUserId": pl.String(),
            "ownerType": pl.String(),
            "make": pl.String(),
            "model": pl.String(),
            "color": pl.String(),
            "licensePlate": pl.String(),
            "passengerSeats": pl.Int32(),
            "accessibilityFeatures": pl.List(
                pl.Struct(
                    {
                        "type": pl.String(),
                        "count": pl.Int32(),
                        "seatCost": pl.Int32(),
                        "requireFirstInLastOut": pl.Boolean(),
                    }
                )
            ),
            "status": pl.String(),
            "metadata": pl.String(),
            "emissionsRate": pl.Int64(),
            "vehicleTypeId": pl.String(),
            "capacityType": pl.String(),
            "createdAt": pl.Int64(),
            "updatedAt": pl.Int64(),
            "photoUrl": pl.String(),
        },
    }
]

METADATA_COLUMNS: Columns = {
    # ISO 8601, e.g. 2025-04-01T21:08:16Z
    # Comes from filename delta uses
    "ROW_TIMESTAMP": pl.Datetime("ms"),
    "ROW_DELETED": pl.Boolean(),
}


def load_json(f, columns: Columns) -> pl.DataFrame:
    """
    Turn a json file into a Polars DataFrame

    Takes a file-like object (with a .read() method), that has uncompressed json.
    Assumes the data is nested in a {"data": field
    """
    j = json.load(f)["data"]
    # metadata field is arbitrary dict, so reencode back to json string
    if "metadata" in columns:
        for row in j:
            row["metadata"] = json.dumps(row["metadata"])
    df = pl.DataFrame(j, columns)
    return df


def load_parquet(parquet_path: str, columns: Columns) -> pl.DataFrame:
    """
    Get a parquet file and return it as a DataFrame.

    Path may start with s3:// or be a local filesystem file.
    If the file doesn't exist, return an empty DataFrame with the columns from the schema.
    """
    columns = METADATA_COLUMNS | columns
    # TODO assert that loaded file has same columns in same order
    # TODO save the file timestamp to catch conflicts
    if parquet_path.startswith("s3://"):
        if s3.object_exists(parquet_path):
            s3_stream = s3.stream_object(parquet_path)
            return pl.read_parquet(s3_stream)
        else:
            return pl.DataFrame([], columns)
    else:
        try:
            with open(parquet_path, "r") as f:
                return pl.read_parquet(f)
        except FileNotFoundError:
            return pl.DataFrame([], columns)


def write_parquet(df: pl.DataFrame, parquet_path: str, tmpdir: str) -> None:
    """
    Write a dataframe to a parquet file.

    Path may start with s3:// or be a local filesystem file.

    Logically, we only append to the parquet file,
    but appending row groups is not well supported and is overkill for this use,
    so we write the whole file.

    TODO Prevent overwriting data if there's a conflict:
    It's possible that the output parquet file was modified (perhaps by another instance of Odin)
    after reading the file, while crunching the data. If so, we should not write the output file,
    as that would overwrite those modifications
    Instead, abort the job, leave the input files unarchived, and re-run the job immediately.
    This way, the input files can be reprocessed and changes calculated based on the newest version
    of the parquet file.
    Implementation notes: For AWS, Save the ETag on read and pass it to If-Match on write.
    For local files, use os.path.getmtime.
    """
    if parquet_path.startswith("s3://"):
        # df.write_parquet() can't write to s3 directly, so write to local file, then upload it
        tmp_path = os.path.join(tmpdir, parquet_path[5:])
        os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
        df.write_parquet(tmp_path)
        s3.upload_file(tmp_path, parquet_path)
    else:
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
        df.write_parquet(parquet_path)


def datetime_from_delta_path_ms(path: str) -> int:
    """
    Extract a datetime from the filenames generated by Delta

    Returns number of ms since the epoch.
    Delta makes filenames like
    2025/04/01/2025-04-01T18:12:33Z_https_example.com_path.gz
    This extracts extracts and parses the ISO8601 timestamp
    It ignores the yyyy/mm/dd/ prefix so it works if the file has been copied elsewhere.
    """
    iso8601 = path.split("/")[-1].split("_")[0]
    if not iso8601.endswith("Z"):
        raise Exception(
            f'Expected iso8601 timestamp with a Z. Got "{iso8601}". From path "{path}".'
        )
    return int(isoparse(iso8601).timestamp() * 1000)


def merge_new_data(
    prev_df: pl.DataFrame, new_df: pl.DataFrame, columns: Columns, datetime_ms: int
) -> pl.DataFrame:
    """
    Append any new or changed data to the file.

    Compare the new input to the existing df.
    Any new or changed rows get appended with a timestamp.
    Any deleted rows (not in the input df) get added with a deleted marker.

    Returns the new dataframe with extra rows, and a boolean for whether any changes were made
    """
    # Take the latest known data for each id.
    # (Older data has been replaced and isn't relevant for calculating new changes.)
    # We always keep the parquet files ordered by time, so the latest is the last.
    compacted_prev = prev_df.unique(subset=["id"], keep="last", maintain_order=True)
    # Then filter out deleted rows

    joined = compacted_prev.join(new_df, on="id", how="full", validate="1:1", suffix="_new")
    deleted_ids = joined.filter(
        # exists in old data but not new data
        (pl.col("ROW_DELETED").not_() & pl.col("id_new").is_null())
        &
        # update is newer than existing data
        # (if files are processed out of order, an old file shouldn't
        # uncreate a row that was just made in a previously-prcoessed new file.)
        (pl.col("ROW_TIMESTAMP") < datetime_ms)
    ).get_column("id")
    new_and_changed_ids = joined.filter(
        # if any column is updated.
        # (this also includes wholly new rows, which have an old id of None,
        # and undeleted rows, which have old ROW_DELETED of true
        pl.any_horizontal(pl.col(col).ne_missing(pl.col(col + "_new")) for col in columns)
        &
        # if it's newer than existing data for this row.
        # (if files are processed out of order, and existing data is newer, don't overwrite it.)
        (pl.col("ROW_TIMESTAMP").is_null() | (pl.col("ROW_TIMESTAMP") < datetime_ms))
    ).get_column("id_new")

    # new rows with the delete flag set and the current timestamp
    # we need the id, but the other columns don't matter.
    # this sets other cols to the last value before it was deleted.
    soft_deletes = compacted_prev.filter(pl.col("id").is_in(deleted_ids)).with_columns(
        pl.lit(datetime_ms, pl.Datetime("ms")).alias("ROW_TIMESTAMP"),
        pl.lit(True, pl.Boolean()).alias("ROW_DELETED"),
    )
    new_rows = new_df.filter(pl.col("id").is_in(new_and_changed_ids))

    if soft_deletes.is_empty() and new_rows.is_empty():
        return prev_df
    else:
        return pl.concat([prev_df, soft_deletes, new_rows], how="vertical")


def add_to_left(df: pl.DataFrame, cols: List[tuple[str, Any, pl.DataType]]) -> pl.DataFrame:
    """
    Add constant columns to the left of a dataframe

    Each column is a tuple: (name, value, type)
    """
    new_cols = [pl.repeat(value, len(df), dtype=dtype).alias(name) for (name, value, dtype) in cols]
    return pl.concat([pl.select(*new_cols), df], how="horizontal")


def run(
    config: Config,
    api_endpoint: ApiEndpoint,
    tmpdir: str,
) -> None:
    """
    Run the job once.

    In a function separate from the job class so it can be tested.
    """
    validate_config(config)
    source = config["source"]
    target = config["target"]
    columns = api_endpoint["columns"]
    parquet_path = os.path.join(target, f"{api_endpoint['name']}.parquet")

    # get new input files
    paths = spare_s3.list_objects(source)
    paths = filter(lambda path: api_endpoint["api_path"] in path, paths)
    # process new files in chronological order (which is also alphabetical order)
    paths = sorted(paths)
    ProcessLog(
        "SpareJob get_inputs",
        api_name=api_endpoint["name"],
        source=source,
        api_path=api_endpoint["api_path"],
        num_paths=len(paths),
    )
    if paths == []:
        # no new files to process
        return

    df = load_parquet(parquet_path, columns)
    rows_before = len(df)
    for path in paths:
        with spare_s3.open_file(os.path.join(source, path)) as f:
            input_df = load_json(f, columns)
        # add metadata
        datetime_ms = datetime_from_delta_path_ms(path)
        rows_input = len(input_df)
        input_df = add_to_left(
            input_df,
            [
                ("ROW_TIMESTAMP", datetime_ms, pl.Datetime("ms")),
                ("ROW_DELETED", False, pl.Boolean()),
            ],
        )
        rows_before_merge = len(df)
        df = merge_new_data(df, input_df, columns, datetime_ms)
        rows_after_merge = len(df)
        # log: input name, num rows, num rows in prev, num rows after. also api_name
        ProcessLog(
            "SpareJob process_input",
            api_name=api_endpoint["name"],
            obj_path=path,
            rows_input=rows_input,
            rows_before_merge=rows_before_merge,
            rows_after_merge=rows_after_merge,
        )

    rows_after = len(df)
    should_write_parquet = rows_after > rows_before
    ProcessLog(
        "SpareJob write_parquet",
        api_name=api_endpoint["name"],
        write_parquet=should_write_parquet,
        rows_before=rows_before,
        rows_after=rows_after,
        parquet_path=parquet_path,
    )
    if should_write_parquet:
        write_parquet(df, parquet_path, tmpdir)

    ProcessLog(
        "SpareJob archive",
        api_name=api_endpoint["name"],
        archive=config.get("archive"),
        num_paths=len(paths),
    )
    if "archive" in config:
        for path in paths:
            spare_s3.rename_object(
                os.path.join(source, path), os.path.join(config["archive"], path)
            )


class SpareJob(OdinJob):
    """Process JSON from Spare (Paratransit) API calls into Parquet files"""

    def __init__(self, config: Config, spare_api_endpoint: ApiEndpoint) -> None:
        """Create SpareJob instance."""
        self.config = config
        self.api_endpoint = spare_api_endpoint
        self.start_kwargs = {
            "name": spare_api_endpoint["name"],
            "api_path": spare_api_endpoint["api_path"],
        }

    def run(self) -> int:
        """Entrypoint to run the job."""
        run(self.config, self.api_endpoint, self.tmpdir)
        next_run_secs = 1 * 60  # 1min
        return next_run_secs


def schedule_spare_jobs(schedule: sched.scheduler, config: Config) -> None:
    """
    Schedule All Jobs for processing Spare paratransit data.

    :param schedule: application scheduler
    :param config: dict from the [spare] section of config.toml
    """
    validate_config(config)
    for endpoint in SPARE_API_ENDPOINTS:
        job = SpareJob(config, endpoint)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
