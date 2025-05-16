"""
Process data from the Spare API into parquet files.

Spare API JSON responses are polled and written into an incoming bucket by Delta.
This job processes those JSON responses into tabular data, and writes them into a parquet file.
That output file is located at {target_location}/{endpoint_name}.parquet

There is one job instance per Spare API endpoint.
Each time the job runs, it processes all files that Delta has created for the endpoint in a batch.

After running, any input files are moved to the archival bucket.
Processing is idempotent so that if this move fails, that will not cause problems.
The next time the job runs, the inputs will be reprocessed (which will be a noop), then archived.

## Api Endpoints and logic

So far, only one endpoint (vehicles) is supported. More will be added later.

Vehicles are handled in a very basic way: The output file is replaced with the data from the latest
input. Any inputs besides the latest are ignored. This means that old data and edit history is not
kept in the output file, which is okay for this use case.

There are few enough vehicles that this file fits in memory,
and the output file will not get bigger over time.

Future endpoints will have different needs will have different logic added for them later.

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

import json
import polars as pl
import os
import sched
from typing import Dict, List, NotRequired, TypeAlias, TypedDict

from odin.utils.logger import ProcessLog
import odin.ingestion.spare.spare_s3 as spare_s3
from odin.job import OdinJob
from odin.job import job_proc_schedule


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


def write_parquet(df: pl.DataFrame, parquet_path: str) -> None:
    """
    Write a dataframe to a parquet file.

    Path may start with s3:// or be a local filesystem file.
    """
    if parquet_path.startswith("s3://"):
        df.write_parquet(parquet_path)
    else:
        os.makedirs(os.path.dirname(parquet_path), exist_ok=True)
        df.write_parquet(parquet_path)


def run(
    config: Config,
    api_endpoint: ApiEndpoint,
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
    # sort new files in chronological order (which is also alphabetical order)
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

    # anything but the newest file is ignored
    path = paths[-1]
    with spare_s3.open_file(os.path.join(source, path)) as f:
        df = load_json(f, columns)

    ProcessLog(
        "SpareJob write_parquet",
        api_name=api_endpoint["name"],
        input_path=path,
        parquet_path=parquet_path,
        rows=len(df),
    )
    write_parquet(df, parquet_path)

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
        run(self.config, self.api_endpoint)
        next_run_secs = 6 * 60 * 60  # 6hr
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
