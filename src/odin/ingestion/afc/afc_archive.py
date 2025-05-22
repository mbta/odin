import os
import gzip
import sched
import shutil
import urllib3
from collections import ChainMap
from operator import itemgetter
from pathlib import Path
from typing import Literal
from typing import TypedDict


from odin.job import OdinJob
from odin.job import job_proc_schedule
from odin.utils.runtime import sigterm_check
from odin.utils.runtime import disk_free_pct
from odin.utils.logger import ProcessLog
from odin.utils.locations import AFC_DATA
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import download_object
from odin.utils.aws.s3 import upload_file
from odin.utils.aws.s3 import delete_objects
from odin.utils.parquet import ds_metadata_min_max
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import pq_dataset_writer
from odin.utils.parquet import ds_unique_values

import polars as pl

NEXT_RUN_DEFAULT = 60 * 60 * 6  # 6 hours

API_ROOT = "https://dwhexperianceapi-production.ir-e1.cloudhub.io/api/v1/datawarehouse"

# Table returned by `tableinfos` endpoint as of April 30, 2025
API_TABLES = [
    "v_card",
    "v_deviceclass",
    "v_eventgroup",
    "v_eventhistory",
    "v_mainshift",
    "v_media",
    "v_medium_types",
    "v_person",
    "v_product_templates",
    "v_routes",
    "v_sales_txns",
    "v_shiftevent",
    "v_stop_points",
    "v_trips",
    "v_tvmstation",
    "v_tvmtable",
    "v_validation_taps",
]

APICounts = list[dict[Literal["jobId", "dataCount"], int]]

APITableSchema = list[dict[Literal["column_name", "data_type"], str]]


class APITableInfo(TypedDict):
    """Schema of dictionary returned from API /tableinfos endpoint for single table."""

    type: Literal["static", "transactional"]
    frequency: str
    remarks: str
    table_infos: APITableSchema


def make_pl_schema(schema: APITableInfo) -> pl.Schema:
    """
    Create polars schema from APITableInfo dict.

    :param schema: dictionary of table information from API /tableinfos endpoint.

    :return: schema_list -> polars schema
    """
    converter = {
        "bigint": pl.Int64(),
        "integer": pl.Int32(),
        # "timestamp with time zone": pl.Datetime(), # Polars can't automatically parse ts from api
    }
    r_schema = {}
    for col in schema["table_infos"]:
        r_schema[col["column_name"]] = converter.get(col["data_type"], pl.String())

    return pl.Schema(r_schema)


def verify_downloads(csv_path: str, csv_schema: pl.Schema, download_jobs: APICounts) -> None:
    """
    Run checks on downloaded csv files to ensure they are in alignment with `count` endpoint.

    API Endpoints are not working as expected, this verification step will confirm that `count`
    and `stagetable` endpoints are in alignment with what they should be returning.

    :param csv_path: path to downloaded csv file
    :param csv_schema: schema of csv file
    :param download_jobs: job list from `count` endpoint
    """
    # create comparison dataframe with following columns:
    #   jobId -> sid values to compare
    #   csvCount -> record count found in csv file
    #   dataCount -> record cound from `count` endpoint
    log = ProcessLog("verify_downloads", csv_path=csv_path)
    compare_df = (
        pl.scan_csv(
            source=csv_path,
            schema=csv_schema,
            has_header=True,
        )
        .group_by("sid")
        .len(name="csvCount")
        .rename({"sid": "jobId"})
        .join(pl.LazyFrame(download_jobs), on="jobId", coalesce=True, how="full")
        .sort("jobId")
        .collect()
    )

    missing_api = compare_df.filter(pl.col("dataCount").is_null())
    missing_api_str = missing_api.get_column("jobId").str.join(",")[0]
    assert missing_api.height == 0, (
        f"SID(s) from `stagetable` not in `count` endpoint:({missing_api_str})"
    )

    missing_csv = compare_df.filter(pl.col("csvCount").is_null())
    missing_csv_str = missing_csv.get_column("jobId").str.join(", ")[0]
    assert missing_csv.height == 0, (
        f"SID(s) from `count` not in `stagetable` endpoint:({missing_csv_str})"
    )

    count_diff = compare_df.filter(
        pl.col("csvCount").is_not_null(),
        pl.col("dataCount").is_not_null(),
        pl.col("csvCount") != pl.col("dataCount"),
    )
    count_diff_str = count_diff.select(
        pl.format("sid {}: {}!={}", "jobId", "dataCount", "csvCount").str.join(", ")
    ).item(0, 0)
    assert count_diff.height == 0, (
        f"record counts from `count` and `stagetable` not equal:({count_diff_str})"
    )
    log.complete()


class ArchiveAFCAPI(OdinJob):
    """Combine AFC API files into single parquet file."""

    def __init__(self, table: str) -> None:
        """Create Job instance."""
        self.table = table
        self.start_kwargs = {"table": table}
        self.export_folder = os.path.join(DATA_SPRINGBOARD, AFC_DATA, table)
        self.headers = {
            "client_id": os.getenv("AFC_API_CLIENT_ID", ""),
            "client_secret": os.getenv("AFC_API_CLIENT_SECRET", ""),
        }

    def make_request(
        self,
        url: str,
        method: str = "GET",
        fields: dict[str, str] | None = None,
        preload_content: bool = True,
    ) -> urllib3.BaseHTTPResponse:
        """
        Make AFC API Request.

        Will raise if 200 status is not returned.

        :param url: full url to be used in Request.
        :param method: (Optional) HTTP request method (such as GET, POST, PUT, etc.)
        :param fields: (Optional) Data to encode and send in request body.
        :param preload_content: (Optional) If True (default) response body preloaded into memory.
                                If False used, call `release_conn()` when done with response

        :return: API Response
        """
        r = self.req_pool.request(method, url=url, fields=fields, preload_content=preload_content)
        if r.status != 200:
            raise urllib3.exceptions.HTTPError(
                f"API ERROR: {url=} status={r.status} response_data={r.data.decode()}"
            )
        return r

    def setup_job(self) -> None:
        """
        Gather data required to run ArchiveAFCAPI.

        1. Pull Table schema and type from `tableinfos` endpoint.
        2. Capture largest sid already processed from parquet dataset.
        3. Pull list of available sid's from `count` endpoint.
            `count` endpoint should always return some amount of sid's, even when called with only
            "table_name" parameter.
        """
        log = ProcessLog("setup_job", table=self.table)
        # set self.schema
        tableinfos_url = f"{API_ROOT}/tableinfos"
        r = self.make_request(tableinfos_url)
        schemas_list: list[dict[str, APITableInfo]] = r.json()
        schemas_dict: dict[str, APITableInfo] = dict(ChainMap(*schemas_list))
        self.schema = make_pl_schema(schemas_dict[self.table])

        # set self.table_type
        # this determines of process performs incremental load or full refresh
        self.table_type = schemas_dict[self.table]["type"]

        # set self.pq_sid from parquet dataset
        self.pq_sid: int | None = None
        self.max_sid: int | None = None
        pq_objects = list_objects(self.export_folder, in_filter=".parquet")
        if pq_objects:
            _, self.pq_sid = ds_metadata_min_max(ds_from_path(f"s3://{self.export_folder}"), "sid")
        log.add_metadata(pq_sid=self.pq_sid)

        # pull list of sid's available to process from API
        count_url = f"{API_ROOT}/count"
        count_fields = {"table_name": self.table}
        if self.pq_sid is not None:
            count_fields["sidFrom"] = str(self.pq_sid)
        r = self.make_request(count_url, fields=count_fields)
        self.job_ids: APICounts = r.json()
        assert isinstance(self.job_ids, list)

        log.complete(num_job_ids=len(self.job_ids))

    def download_csv(self, download_jobs: APICounts) -> None:
        """
        Download csv.gz table file for AFC API.

        Only download csv.gz table with known sid_from and sid_to range.

        :param download_jobs: list of API jobId's (sid's) to download (sorted by jobId - ascending)
        """
        sid_from = download_jobs[0]["jobId"]
        sid_to = download_jobs[-1]["jobId"]
        num_rows = sum(j["dataCount"] for j in download_jobs)
        log = ProcessLog(
            "afc_api_download_csv",
            table=self.table,
            sid_from=sid_from,
            sid_to=sid_to,
            num_rows=num_rows,
        )

        url = f"{API_ROOT}/stagetable"
        fields = {
            "table_name": self.table,
            "responseType": "application/csv",
            "compression": "gzip",
            "sidFrom": str(sid_from),
            "sidTo": str(sid_to),
        }
        r = self.make_request(url, fields=fields, preload_content=False)

        csv_path = os.path.join(self.tmpdir, f"{sid_from}.csv")
        gz_path = f"{csv_path}.gz"

        # download csv.gz file (as stream)
        with open(gz_path, mode="wb") as gz_write:
            shutil.copyfileobj(r, gz_write)
        r.release_conn()

        # convert to csv.gz to csv, so polars scan_csv can be used
        # csv file should also produce more consistent disk and memory usage profiles
        with gzip.open(gz_path, mode="rb") as gz_read, open(csv_path, mode="wb") as csv_write:
            shutil.copyfileobj(gz_read, csv_write)
        os.remove(gz_path)

        log.complete()
        verify_downloads(csv_path, self.schema, download_jobs)

    def load_sids(self) -> None:
        """
        Load API data based on sid values.

        SID pagination will be done with data returned from `count` endpoint.

        API data will be downloaded in batches to limit API reponse time.
        """
        # assume 12 bytes per column to ballbark 20mb per csv request
        target_rows = 20 * 1024 * 1024 / (12 * self.schema.len())
        download_jobs: APICounts = []
        download_rows = 0
        for api_job in sorted(self.job_ids, key=itemgetter("jobId")):
            if self.pq_sid is not None and int(api_job["jobId"]) <= int(self.pq_sid):
                continue
            download_jobs.append(api_job)
            download_rows += int(api_job["dataCount"])
            self.max_sid = int(api_job["jobId"])
            if download_rows > target_rows:
                self.download_csv(download_jobs)
                download_jobs = []
                download_rows = 0
            # stop if disk is getting too full
            if disk_free_pct() < 60:
                break
        if len(download_jobs) > 0:
            self.download_csv(download_jobs)

    def sync_parquet(self) -> None:
        """Convert csv to parquet and sync with S3 files."""
        log = ProcessLog("afc_api_sync_parquet")
        sync_paths = []
        for csv_file in sorted(Path(self.tmpdir).iterdir(), key=os.path.getmtime):
            if csv_file.suffix != ".csv":
                continue
            pq_path = str(csv_file.absolute()).replace(".csv", ".parquet")
            lf = pl.scan_csv(
                csv_file.absolute(),
                schema=self.schema,
                has_header=True,
            )
            lf.sink_parquet(
                pq_path,
                compression="zstd",
                compression_level=3,
                row_group_size=int(1024 * 1024 / (8 * self.schema.len())),
            )
            csv_file.unlink()
            pq_row_count = ds_from_path(pq_path).count_rows()
            if pq_row_count > 0:
                sync_paths.append(pq_path)
                log.add_metadata(pq_path=pq_path, pq_row_count=pq_row_count)

        if len(sync_paths) == 0:
            return

        found_objs = list_objects(f"s3://{self.export_folder}", in_filter=".parquet")
        del_objs = []
        if self.table_type == "transactional":
            # `transactional` table type is supposed to be incremental (append-only) data model
            if found_objs:
                sync_file = found_objs[-1].path.replace("s3://", "")
                s3_pq_file = os.path.join(self.tmpdir, sync_file.replace("/table_", "/temp_"))
                download_object(found_objs[-1].path, s3_pq_file)
                sync_paths.insert(0, s3_pq_file)
        elif self.table_type == "static":
            # `static` table type should only ever be comprised of 1 sid/jobId value and
            # operates as truncate -> reload operation
            del_objs = [obj.path for obj in found_objs]
            assert ds_unique_values(ds_from_path(sync_paths), ["sid"]).num_rows == 1
        else:
            raise NotImplementedError(f"S&B API table 'type' of ({self.table_type}) not supported.")

        # Create new merged parquet file(s)
        new_paths = pq_dataset_writer(
            source=ds_from_path(sync_paths),
            export_folder=os.path.join(self.tmpdir, self.export_folder),
            export_file_prefix="table",
        )

        # Check for sigterm before upload (can't be un-done)
        sigterm_check()
        delete_objects(del_objs)
        for new_path in new_paths:
            move_path = new_path.replace(f"{self.tmpdir}/", "")
            upload_file(new_path, move_path)
        log.complete()

    def re_run_check(self) -> int:
        """
        Determine when job should be ru-run.

        If `count` returning more than 1 job, based on self.max_sid, indicates more jobs available.
        """
        log = ProcessLog("re_run_check", table=self.table, max_sid=self.max_sid)
        return_duration = NEXT_RUN_DEFAULT
        if self.max_sid is not None:
            r = self.make_request(
                url=f"{API_ROOT}/count",
                fields={"table_name": self.table, "sidFrom": str(self.max_sid)},
            )
            if len(r.json()) > 1:
                return_duration = 60 * 5
        log.complete(return_duration=return_duration)
        return return_duration

    def run(self) -> int:
        """
        Archive S&B AFC Data from API.

        Process:
            1. setup job by:
                - pulling table schema from API Endpoint
                - grab the largest SID already processed, from parquet files
                - grab SID's available from /count endpoint
            2. Loop through all available SID's returned from /count endpoint.
                - Download SID ranges in batches as csv.gz files.
            3. Convert csv file(s) to parquet and merge with S3 parquet files.
            4. Determine next_runs_secs duration.

        May 12, 2025 - API has been updated to always return results from "count" endpoint.

        TODO: S&B indicated API has two table "types" one type has incrementing SID's for new data,
        which this process is currently desined for, the other does full table replacements for
        each new SID (not implemented). Waiting on S&B to provide information on which table
        is which.
        """
        self.start_kwargs = {"table": self.table}
        # Current timetout set to 10 mins, for full PROD deployment should be no more than 2 mins.
        self.req_pool = urllib3.PoolManager(
            headers=self.headers,
            timeout=urllib3.Timeout(total=60 * 10),  # 10 minute total timeout
            retries=False,  # No retires, if retry-able failures are documented, can be updated
        )
        self.setup_job()
        self.load_sids()
        self.sync_parquet()
        return self.re_run_check()


def schedule_afc_archive(schedule: sched.scheduler) -> None:
    """
    Schedule All Jobs for AFC API Archive process.

    :param schedule: application scheduler
    """
    for table in API_TABLES:
        job = ArchiveAFCAPI(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
