import os
import gzip
import sched
import urllib3
from io import BytesIO
from operator import itemgetter
from typing import List
from typing import Dict
from typing import Literal
from typing import Optional


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
# This API_ROOT bypasses a load balancer, allowing for long running requests.
# This is required because the API currently does not have a complete pagination solution.
# This api endpoint is also using a self-signed cert, so SSL verifcation is not posssible.
API_ROOT = (
    "https://mule-worker-dwhexperianceapi-production.ir-e1.cloudhub.io:8082/api/v1/datawarehouse"
)

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

ApiSchema = List[Dict[Literal["column_name", "data_type"], str]]


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
    """
    Combine AFC API files into single parquet file.

    This process should not be deployed to PROD. It has no pagination capability so resource
    usage of this process is completely un-knowable.
    """

    def __init__(self, table: str) -> None:
        """Create Job instance."""
        self.table = table
        self.start_kwargs = {"table": self.table}
        self.export_folder = os.path.join(DATA_SPRINGBOARD, AFC_DATA, self.table)
        self.headers = {
            "client_id": os.getenv("AFC_API_CLIENT_ID", ""),
            "client_secret": os.getenv("AFC_API_CLIENT_SECRET", ""),
        }

    def make_request(
        self, url: str, fields: Optional[Dict[str, str]] = None
    ) -> urllib3.BaseHTTPResponse:
        """
        Make AFC API Request.

        Will raise if 200 status is not returned.

        :param url: full url to be used in GET Request.
        :param fields: (Optional) params/fields to be included in GET request.

        :return: API Response
        """
        r = self.req_pool.request("GET", url=url, fields=fields)
        if r.status != 200:
            raise urllib3.exceptions.HTTPError(f"API ERROR: {url=} {r.status=} {r.data.decode()}")
        return r

    def setup_job(self) -> None:
        """
        Grab API table schema and largest SID already processed.

        Use of SID is used as a "pagination" method of API. One SID worth of records is the minimum
        number of records that can be pulled at one time from the API, however there is no control
        over how many records can/will be in one SID.
        """
        # set self.schema
        tableinfos_url = f"{API_ROOT}/tableinfos"
        r = self.make_request(tableinfos_url)
        schema_list: List[Dict[str, ApiSchema]] = r.json()
        for schema in schema_list:
            if self.table in schema:
                self.schema = make_pl_schema(schema[self.table])
                break
        else:
            raise IndexError(f"{self.table} not found in 'tableinfos' API endpoint.")

        # set self.last_sid
        self.last_sid = None
        pq_objects = list_objects(self.export_folder, in_filter=".parquet")
        if pq_objects:
            _, self.last_sid = ds_metadata_min_max(
                ds_from_path(f"s3://{self.export_folder}"), "sid"
            )

        # set self.job_ids (if possible)
        count_url = f"{API_ROOT}/count"
        try:
            r = self.make_request(count_url, fields={"table_name": self.table})
            self.job_ids = r.json()
            assert isinstance(self.job_ids, list)
        except Exception:
            self.job_ids = None

    def download_csv(self, sid_from: int | str | None, sid_to: int | str | None = None) -> None:
        """
        Download csv.gz table file for AFC API.

        No pagination is available at this endpoint. This is not a great design. There's supposed to
        be an API endpoint to query SID's available, but it is not functional.

        If sid_from and/or sid_to are available, pagination will be attempted.

        Using sidFrom and/or sidTo results in a response that is inclusive of both parameters.

        :param sid_from: used as sidFrom request parameter, if available
        :param sid_to: used as sidTo request parameter, if available
        """
        log = ProcessLog("afc_api_download_csv", sid_from=sid_from, sid_to=sid_to)
        csv_path = os.path.join(self.tmpdir, f"{self.table}.csv")
        if sid_from is not None:
            csv_path = os.path.join(self.tmpdir, f"{sid_from}.csv")
        url = f"{API_ROOT}/stagetable"
        fields = {
            "table_name": self.table,
            "responseType": "application/csv",
            "compression": "gzip",
        }
        if sid_from is not None:
            fields["sidFrom"] = str(sid_from)
        if sid_to is not None:
            fields["sidTo"] = str(sid_to)

        r = self.make_request(url, fields)
        with gzip.open(BytesIO(r.data)) as gdata:
            with open(csv_path, mode="wb") as writer:
                writer.write(gdata.read())
        log.complete()

    def load_sids(self) -> None:
        """
        Load API data based on sid values.

        Sid pagination will be attempted if range of sid values is available. These values need to
        to be pulled from `count` endpoint, however as of May 1, 2025 `count` endpoint is not
        functional with the use of `sidFrom` parameter.

        If pagination is possible, API data will be saved in batches, targeting file size of
        approximately 10mb.
        """
        if self.job_ids is None:
            self.download_csv(self.last_sid)
        else:
            batch_count = 0
            target_rows = 20 * 1024 * 1024 / (12 * self.schema.len())
            start_sid = ""
            end_sid = ""
            for job in sorted(self.job_ids, key=itemgetter("jobId")):
                job_sid = str(job["jobId"])
                if self.last_sid is not None and job_sid <= str(self.last_sid):
                    continue
                if start_sid == "":
                    start_sid = job_sid
                end_sid = job_sid
                batch_count += int(job["dataCount"])
                if batch_count > target_rows:
                    self.download_csv(start_sid, end_sid)
                    batch_count = 0
                    start_sid = ""
                # stop if disk is getting too full
                if disk_free_pct() < 60:
                    break
            if batch_count > 0:
                self.download_csv(start_sid, end_sid)

    def sync_parquet(self) -> None:
        """Convert csv to parquet and sync with S3 files."""
        log = ProcessLog("afc_api_sync_parquet")
        sync_paths = []
        for temp_file in os.listdir(self.tmpdir):
            if not temp_file.endswith(".csv"):
                continue
            csv_path = os.path.join(self.tmpdir, temp_file)
            pq_path = csv_path.replace(".csv", ".parquet")
            lf = pl.scan_csv(
                csv_path,
                schema=self.schema,
                has_header=True,
            )
            if self.last_sid is not None:
                lf = lf.filter(pl.col("sid") != self.last_sid)
            lf.sink_parquet(
                pq_path,
                compression="zstd",
                compression_level=3,
                row_group_size=int(1024 * 1024 / (8 * self.schema.len())),
            )
            os.remove(csv_path)
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

    def run(self) -> int:
        """
        Archive S&B AFC Data from API.

        Process:
            1. setup Job by:
                - pulling table schema from API Endpoint
                - grab the largest SID already loaded from parquet files
                - attempt to grab SID's available from /count endpoint
            2a. Download all available SID's as csv.gz glob.
                - No true pagination available for this process.
                - Use of "sidFrom" param is inclusive of SID submitted.
            2b. Loop through all available SID's returned from /count endpoint.
                - Download each SID as a csv.gz file.
            3. Convert csv file(s) to parquet and merge with S3 parquet files.
        """
        # TODO: Removed cert_reqs parameter when not using self-signed endpoint.
        self.req_pool = urllib3.PoolManager(headers=self.headers, cert_reqs="CERT_NONE")
        self.setup_job()
        self.load_sids()
        self.sync_parquet()
        return NEXT_RUN_DEFAULT


def schedule_afc_archive(schedule: sched.scheduler) -> None:
    """
    Schedule All Jobs for AFC API Archive process.

    :param schedule: application scheduler
    """
    for table in API_TABLES:
        job = ArchiveAFCAPI(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
