import os
import gzip
import sched
import urllib3
from io import BytesIO
from operator import itemgetter
from pathlib import Path
from typing import Literal


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
from odin.utils.parquet import ds_metadata_min_max
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import pq_dataset_writer

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

ApiSchema = list[dict[Literal["column_name", "data_type"], str]]

ApiCounts = list[dict[Literal["jobId", "dataCount"], int]]


def make_pl_schema(schema_list: ApiSchema) -> pl.Schema:
    """
    Create polars schema from json API schema list.

    :param schema_list: list of dictionaires from API /tableinfos endpoint.

    :return: schema_list -> polars schema
    """
    converter = {
        "bigint": pl.Int64(),
        "integer": pl.Int32(),
        # "timestamp with time zone": pl.Datetime(), # Polars can't automatically parse ts from api
    }
    r_schema = {}
    for schema_d in schema_list:
        r_schema[schema_d["column_name"]] = converter.get(schema_d["data_type"], pl.String())

    return pl.Schema(r_schema)


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

        1. Pull Table schema from `tableinfos` endpoint.
        2. Capture largest sid already processed from parquet dataset.
        3. Pull list of available sid's from `count` endpoint.
            `count` endpoint should always return some amount of sid's, even when called with only
            "table_name" parameter.
        """
        log = ProcessLog("setup_job", table=self.table)
        # set self.schema
        tableinfos_url = f"{API_ROOT}/tableinfos"
        r = self.make_request(tableinfos_url)
        schema_list: list[dict[str, ApiSchema]] = r.json()
        for schema in schema_list:
            if self.table in schema:
                self.schema = make_pl_schema(schema[self.table])
                break
        else:
            raise IndexError(f"{self.table} not found in 'tableinfos' API endpoint.")

        # set self.pq_sid from parquet dataset
        self.pq_sid: int | None = None
        self.max_sid: int | None = None
        pq_objects = list_objects(self.export_folder, in_filter=".parquet")
        if pq_objects:
            _, self.pq_sid = ds_metadata_min_max(ds_from_path(f"s3://{self.export_folder}"), "sid")
        log.add_metadata(pq_sid=self.pq_sid)

        # pull list of sid's available to process API
        count_url = f"{API_ROOT}/count"
        count_fields = {"table_name": self.table}
        if self.pq_sid is not None:
            count_fields["sidFrom"] = str(self.pq_sid)
        r = self.make_request(count_url, fields=count_fields)
        self.job_ids: ApiCounts = r.json()
        assert isinstance(self.job_ids, list)

        log.complete(num_job_ids=len(self.job_ids))

    def download_csv(self, sid_from: int, sid_to: int) -> None:
        """
        Download csv.gz table file for AFC API.

        Only download csv.gz table with known sid_from and sid_to range.

        :param sid_from: used as sidFrom request parameter
        :param sid_to: used as sidTo request parameter
        """
        log = ProcessLog("afc_api_download_csv", table=self.table, sid_from=sid_from, sid_to=sid_to)

        csv_path = os.path.join(self.tmpdir, f"{sid_from}.csv")

        url = f"{API_ROOT}/stagetable"
        fields = {
            "table_name": self.table,
            "responseType": "application/csv",
            "compression": "gzip",
            "sidFrom": str(sid_from),
            "sidTo": str(sid_to),
        }
        r = self.make_request(url, fields=fields, preload_content=False)
        with gzip.open(BytesIO(r.data)) as gdata:
            with open(csv_path, mode="wb") as writer:
                writer.write(gdata.read())
        log.complete()

    def load_sids(self) -> None:
        """
        Load API data based on sid values.

        SID pagination will be done with data returned from `count` endpoint.

        API data will be saved in batches.
        """
        batch_count = 0
        # assume 12 bytes per column to ballbark 20mb per csv request
        target_rows = 20 * 1024 * 1024 / (12 * self.schema.len())
        start_sid = -1
        end_sid = -1
        for api_job in sorted(self.job_ids, key=itemgetter("jobId")):
            job_sid = int(api_job["jobId"])
            if self.pq_sid is not None and job_sid <= int(self.pq_sid):
                continue
            if start_sid < 0:
                start_sid = job_sid
            end_sid = job_sid
            self.max_sid = job_sid
            batch_count += int(api_job["dataCount"])
            if batch_count > target_rows:
                self.download_csv(start_sid, end_sid)
                batch_count = 0
                start_sid = -1
            # stop if disk is getting too full
            if disk_free_pct() < 60:
                break
        if batch_count > 0:
            self.download_csv(start_sid, end_sid)

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
        if found_objs:
            sync_file = found_objs[-1].path.replace("s3://", "")
            s3_pq_file = os.path.join(self.tmpdir, sync_file.replace("/table_", "/temp_"))
            download_object(found_objs[-1].path, s3_pq_file)
            sync_paths.insert(0, s3_pq_file)

        # Create new merged parquet file(s)
        new_paths = pq_dataset_writer(
            source=ds_from_path(sync_paths),
            export_folder=os.path.join(self.tmpdir, self.export_folder),
            export_file_prefix="table",
        )

        # Check for sigterm before upload (can't be un-done)
        sigterm_check()
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
