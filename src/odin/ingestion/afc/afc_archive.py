import os
import gzip
import json
import sched
import shutil
import urllib3
from collections import ChainMap
from operator import itemgetter
from pathlib import Path
from typing import Literal
from typing import TypedDict
from typing import Generator


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


class APIEmptyResponseError(Exception):
    """API Returned 204."""

    pass


NEXT_RUN_DEFAULT = 60 * 60 * 6  # 6 hours

API_ROOT = os.getenv("AFC_ROOT", "")

# Table returned by `tableinfos` endpoint as of October 20, 2025
API_TABLES = [
    "v_business_entities",
    "v_ca_legal_relations",
    "v_card",
    "v_deviceclass",
    "v_eventgroup",
    "v_eventhistory",
    "v_eventtext",
    "v_legal_persons",
    "v_mainshift",
    "v_medium_types",
    "v_person",
    "v_product_templates",
    "v_routes",
    "v_sales_txns",
    "v_shiftevent",
    "v_stop_points",
    "v_ta_ca_relations",
    "v_ta_legal_relations",
    "v_trips",
    "v_tvmstation",
    "v_tvmtable",
    "v_validation_taps",
    "v_cashless_payments",
    "v_inspections",
    "v_tsmstatus",
    "v_salesdetail",
    "v_salestransaction",
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
    }
    r_schema = {}
    for col in schema["table_infos"]:
        r_schema[col["column_name"]] = converter.get(col["data_type"], pl.String())

    return pl.Schema(r_schema)


def verify_downloads(path: str, schema: pl.Schema, download_jobs: APICounts) -> None:
    """
    Run checks on downloaded files to ensure they are in alignment with `count` endpoint.

    These checks are based on previous API Endpoints issues that we should confirm have not been
    reintroduced to the API.
     - confirm that `count` and `stagetable` endpoints are in alignment
     - confirm that `stagetable` and tableinfos` (schema) endpoints are in alignment

    :param path: path to downloaded json file
    :param schema: schema of file
    :param download_jobs: job list from `count` endpoint
    """
    log = ProcessLog("verify_downloads", path=path)
    schema_columns = set(schema.names())
    with open(path, "r") as r:
        file_columns = set(json.loads(r.readline()).keys())

    not_in_schema = file_columns - schema_columns
    assert len(not_in_schema) == 0, (
        f"Columns in API download not found in API schema: ({', '.join(not_in_schema)})"
    )

    not_in_file = schema_columns - file_columns
    assert len(not_in_file) == 0, (
        f"Columns in API schema not found in API download: ({', '.join(not_in_file)})"
    )

    # create comparison dataframe with following columns:
    #   jobId -> job_id values to compare
    #   jsonCount -> record count found in json file
    #   dataCount -> record cound from `count` endpoint
    compare_df = (
        pl.scan_ndjson(
            source=path,
            schema=schema,
        )
        .group_by("job_id")
        .len(name="jsonCount")
        .rename({"job_id": "jobId"})
        .join(pl.LazyFrame(download_jobs), on="jobId", coalesce=True, how="full")
        .sort("jobId")
        .collect()
    )

    missing_api = compare_df.filter(pl.col("dataCount").is_null())
    missing_api_str = missing_api.get_column("jobId").str.join(",")[0]
    assert missing_api.height == 0, (
        f"job_id(s) from `stagetable` not in `count` endpoint:({missing_api_str})"
    )

    missing_json = compare_df.filter(pl.col("jsonCount").is_null())
    missing_json_str = missing_json.get_column("jobId").str.join(", ")[0]
    assert missing_json.height == 0, (
        f"job_id(s) from `count` not in `stagetable` endpoint:({missing_json_str})"
    )

    count_diff = compare_df.filter(
        pl.col("jsonCount").is_not_null(),
        pl.col("dataCount").is_not_null(),
        pl.col("jsonCount") != pl.col("dataCount"),
    )
    count_diff_str = count_diff.select(
        pl.format("job_id {}: {}!={}", "jobId", "dataCount", "jsonCount").str.join(", ")
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
        if r.status == 204:
            raise APIEmptyResponseError("API 204 Response, NO DATA.")
        elif r.status != 200:
            raise urllib3.exceptions.HTTPError(
                f"API ERROR: {url=} status={r.status} response_data={r.data.decode()}"
            )
        return r

    def setup_job(self) -> None:
        """
        Gather data required to run ArchiveAFCAPI.

        1. Pull Table schema and type from `tableinfos` endpoint.
        2. Capture largest job_id already processed from parquet dataset.
        3. Pull list of available job_id's from `count` endpoint.
            `count` endpoint should always return some amount of job_id's, even when called
            with only "table_name" parameter.
        """
        log = ProcessLog("setup_job", table=self.table)
        # set self.schema
        tableinfos_url = f"{API_ROOT}/tableinfos"
        r = self.make_request(tableinfos_url)
        schemas_list: list[dict[str, APITableInfo]] = r.json()
        schemas_dict: dict[str, APITableInfo] = dict(ChainMap(*schemas_list))
        self.schema = make_pl_schema(schemas_dict[self.table])

        # columns to be converted to datetime types
        # default polars datetime parser can not understand S&B timestamp string format
        self.ts_cols = [
            ti["column_name"]
            for ti in schemas_dict[self.table]["table_infos"]
            if "timestamp" in ti["data_type"]
        ]

        # set self.table_type
        # this determines of process performs incremental load or full refresh
        self.table_type = schemas_dict[self.table]["type"]

        # set self.pq_job_id from parquet dataset
        self.pq_job_id = 0
        self.max_job_id = 0
        if list_objects(self.export_folder, in_filter=".parquet"):
            _, self.pq_job_id = ds_metadata_min_max(
                ds_from_path(f"s3://{self.export_folder}"), "job_id"
            )
        log.complete(pq_job_id=self.pq_job_id)

    def api_job_ids(self, job_id_from: int) -> Generator[APICounts]:
        """
        Generate job_id's from /count endpoint.

        Will pull job_ids from /count endpoint in batches until no more job_ids are available.

        :param job_id_from: job_id to start pulling from (exclusive)

        :return: sorted list of APICounts
        """
        count_url = f"{API_ROOT}/count"
        req_fields = {"table_name": self.table, "limit": str(10_000)}
        while True:
            req_fields["jobIdFrom"] = str(job_id_from)
            try:
                r = self.make_request(count_url, fields=req_fields)
            except APIEmptyResponseError:
                break
            api_counts: APICounts = r.json()
            # /count endpoint currently returns results inclusive if `jobIdFrom` parameter
            # however this is not documented so this process is designed to work with the endpoint
            # being either inclusive or exclusive of the `jobIdFrom` parameter
            api_counts = sorted(
                [j for j in api_counts if j["jobId"] > job_id_from], key=itemgetter("jobId")
            )
            if len(api_counts) > 0:
                yield api_counts
            else:
                break
            job_id_from = api_counts[-1]["jobId"]

    def download_json(self, download_jobs: APICounts) -> None:
        """
        Download json.gz table file for AFC API.

        Only download json.gz table with known jobIdFrom and jobIdTo range.

        :param download_jobs: list of API jobId's to download (sorted by jobId - ascending)
        """
        job_id_from = download_jobs[0]["jobId"]
        job_id_to = download_jobs[-1]["jobId"]
        num_rows = sum(j["dataCount"] for j in download_jobs)
        log = ProcessLog(
            "afc_api_download_json",
            table=self.table,
            job_id_from=job_id_from,
            job_id_to=job_id_to,
            num_rows=num_rows,
        )

        url = f"{API_ROOT}/stagetable"
        fields = {
            "table_name": self.table,
            "responseType": "application/json",
            "compression": "gzip",
            "jobIdFrom": str(job_id_from),
            "jobIdTo": str(job_id_to),
        }
        r = self.make_request(url, fields=fields, preload_content=False)

        json_path = os.path.join(self.tmpdir, f"{job_id_from}.json")
        gz_path = f"{json_path}.gz"

        # download json.gz file (as stream)
        with open(gz_path, mode="wb") as gz_write:
            shutil.copyfileobj(r, gz_write)
        r.release_conn()

        # convert to json.gz to json, so polars can be used
        # json file should also produce more consistent disk and memory usage profiles
        with gzip.open(gz_path, mode="rb") as gz_read, open(json_path, mode="wb") as json_w:
            shutil.copyfileobj(gz_read, json_w)
        os.remove(gz_path)

        log.complete()
        verify_downloads(json_path, self.schema, download_jobs)

    def load_job_ids(self) -> None:
        """
        Load API data based on job_id values.

        JobId pagination will be done with data returned from `count` endpoint.

        API data will be downloaded in batches to limit API reponse time.
        """
        # assume 12 bytes per column to ballbark 50mb per request
        min_disk_free_pct = 60
        target_rows = int(50 * 1024 * 1024 / (12 * self.schema.len()))
        download_jobs: APICounts = []
        download_rows = 0
        for job_ids in self.api_job_ids(self.pq_job_id):
            for api_job in job_ids:
                # extra check to make certain jobId should be processed
                if int(api_job["jobId"]) <= int(self.pq_job_id):
                    continue
                download_jobs.append(api_job)
                download_rows += int(api_job["dataCount"])
                self.max_job_id = int(api_job["jobId"])
                if download_rows > target_rows:
                    self.download_json(download_jobs)
                    download_jobs = []
                    download_rows = 0
                # stop if disk is getting too full
                if disk_free_pct() < min_disk_free_pct:
                    break
            if len(download_jobs) > 0:
                self.download_json(download_jobs)

            # This is ugly, but should work for now...
            if disk_free_pct() < min_disk_free_pct:
                break

    def sync_parquet(self) -> None:
        """Convert json to parquet and sync with S3 files."""
        log = ProcessLog("afc_api_sync_parquet")
        sync_paths = []
        for json_file in sorted(Path(self.tmpdir).iterdir(), key=os.path.getmtime):
            if json_file.suffix != ".json":
                continue
            pq_path = str(json_file.absolute()).replace(".json", ".parquet")
            lf = pl.scan_ndjson(
                json_file.absolute(),
                schema=self.schema,
            ).with_columns(pl.col(self.ts_cols).str.head(19).str.to_datetime("%Y-%m-%d %H:%M:%S"))
            lf.sink_parquet(
                pq_path,
                compression="zstd",
                compression_level=3,
                row_group_size=int(1024 * 1024 / (8 * self.schema.len())),
            )
            json_file.unlink()
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
            # `static` table type should only ever be comprised of 1 jobId value and
            # operates as truncate -> reload operation
            del_objs = [obj.path for obj in found_objs]
            assert ds_unique_values(ds_from_path(sync_paths), ["job_id"]).num_rows == 1
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

        If `count` returning more than 1 job, based on self.max_job_id, more jobs available.
        """
        log = ProcessLog("re_run_check", table=self.table, max_job_id=self.max_job_id)
        return_duration = NEXT_RUN_DEFAULT
        if self.max_job_id > 0:
            r = self.make_request(
                url=f"{API_ROOT}/count",
                fields={"table_name": self.table, "jobIdFrom": str(self.max_job_id)},
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
                - grab the largest job_id already processed, from parquet files
                - grab job_id's available from /count endpoint
            2. Loop through all available job_id's returned from /count endpoint.
                - Download job_id ranges in batches as json.gz files.
            3. Convert json file(s) to parquet and merge with S3 parquet files.
            4. Determine next_runs_secs duration.

        May 12, 2025 - API has been updated to always return results from "count" endpoint.
        June 2, 2025 - Updated to handle "static" and "transactional" table types.
        """
        self.start_kwargs = {"table": self.table}
        # Current timetout set to 10 mins, for full PROD deployment should be no more than 2 mins.
        self.req_pool = urllib3.PoolManager(
            headers=self.headers,
            timeout=urllib3.Timeout(total=60 * 10),  # 10 minute total timeout
            retries=False,  # No retries, if retry-able failures are documented, can be updated
        )
        self.setup_job()
        self.load_job_ids()
        next_run_duration = self.re_run_check()
        self.sync_parquet()
        return next_run_duration


def schedule_afc_archive(schedule: sched.scheduler) -> None:
    """
    Schedule All Jobs for AFC API Archive process.

    :param schedule: application scheduler
    """
    for table in API_TABLES:
        job = ArchiveAFCAPI(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
