"""Masabi Data API ingestion pipeline."""

from __future__ import annotations

import json
import os
import pathlib
import sched
import time
from typing import Any
from typing import Generator

import polars as pl
import urllib3
import yaml

from odin.job import NEXT_RUN_DEFAULT
from odin.job import OdinJob
from odin.job import job_proc_schedule
from odin.utils.aws.s3 import download_object
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import upload_file
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_DATA
from odin.utils.logger import ProcessLog
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import ds_metadata_min_max
from odin.utils.parquet import pq_dataset_writer
from odin.utils.runtime import sigterm_check

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# API base URL — set per environment in devops config, e.g.:
#   dev:  https://uat.justride.systems/data-store/query/v2/MBTA
#   prod: https://prod.justride.systems/data-store/query/v2/MBTA  (verify exact URL)
API_ROOT = os.getenv("MASABI_API_ROOT", "")

# Basic-auth credentials (validated at startup by run.py)
_API_USERNAME = os.getenv("MASABI_DATA_API_USERNAME", "")
_API_PASSWORD = os.getenv("MASABI_DATA_API_PASSWORD", "")

# Page size: Masabi's maximum is 1000. Override with MASABI_API_PAGE_SIZE=100 for
# dev environments where the dataset is small.
API_PAGE_SIZE = int(os.getenv("MASABI_API_PAGE_SIZE", "1000"))

# Exclusive lower bound for the initial historical backfill: 2025-01-01 00:00:00 UTC (ms).
MASABI_START_TIMESTAMP_MS: int = 1_735_689_600_000

# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

TABLES = [
    "retail.account_actions",
    "retail.activations",
    "retail.ticket_purchases",
    "retail.tickets",
    "retail.rider_entitlement_events",
]

# Per-table columns to drop before ingestion (PII removal).
# Populate once stakeholders have identified PII columns; leave empty for now.
TABLE_EXCLUDE_COLUMNS: dict[str, list[str]] = {
    "retail.account_actions": [],
    "retail.activations": [],
    "retail.ticket_purchases": [],
    "retail.tickets": [],
    "retail.rider_entitlement_events": [],
}

# ---------------------------------------------------------------------------
# Schema loader
#
# TABLE_SCHEMAS and TABLE_JSON_COLS are derived at import time from
# masabi_schema.yaml (an OpenAPI 3.0.1 file co-located in this package).
#
# YAML type  →  Polars type
#   string   →  pl.String()
#   boolean  →  pl.Boolean()
#   number   →  pl.Float64()
#   array    →  pl.String()  (JSON-serialised)
#   object   →  pl.String()  (JSON-serialised)
#
# Any unknown YAML type falls back to pl.String() so that future schema
# additions are safe by default.
# ---------------------------------------------------------------------------

_SCHEMA_PATH = pathlib.Path(__file__).parent / "masabi_schema.yaml"

_YAML_TYPE_MAP: dict[str, pl.DataType] = {
    "string": pl.String(),
    "boolean": pl.Boolean(),
    "number": pl.Float64(),
    "array": pl.String(),
    "object": pl.String(),
}
_JSON_YAML_TYPES = frozenset({"array", "object"})


def _load_schemas(
    tables: list[str],
) -> tuple[dict[str, pl.Schema], dict[str, frozenset[str]]]:
    """
    Load per-table column schemas and JSON-column sets from masabi_schema.yaml.

    :param tables: table names to load (must all exist in the YAML)
    :return: (TABLE_SCHEMAS, TABLE_JSON_COLS)
    :raises KeyError: if a table in *tables* is absent from the YAML
    """
    with _SCHEMA_PATH.open() as fh:
        spec = yaml.safe_load(fh)

    all_schemas = spec["components"]["schemas"]
    schemas: dict[str, pl.Schema] = {}
    json_cols: dict[str, frozenset[str]] = {}

    for table in tables:
        if table not in all_schemas:
            raise KeyError(
                f"Table {table!r} not found in {_SCHEMA_PATH}; "
                "update masabi_schema.yaml or remove it from TABLES"
            )
        properties: dict[str, dict[str, Any]] = all_schemas[table]["properties"]
        column_types: dict[str, pl.DataType] = {}
        json_set: set[str] = set()
        for col, col_def in properties.items():
            yaml_type: str = col_def.get("type", "string")
            column_types[col] = _YAML_TYPE_MAP.get(yaml_type, pl.String())
            if yaml_type in _JSON_YAML_TYPES:
                json_set.add(col)
        schemas[table] = pl.Schema(column_types)
        json_cols[table] = frozenset(json_set)

    return schemas, json_cols


TABLE_SCHEMAS, TABLE_JSON_COLS = _load_schemas(TABLES)


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------


class ArchiveMasabi(OdinJob):
    """Archive a single Masabi Data API table into springboard parquet files."""

    def __init__(self, table: str) -> None:
        """Create job instance for one Masabi table."""
        self.table = table
        self.start_kwargs = {"table": table}
        self.export_folder = os.path.join(DATA_SPRINGBOARD, MASABI_DATA, table)
        self.json_cols: frozenset[str] = TABLE_JSON_COLS[table]
        self.exclude_cols: frozenset[str] = frozenset(TABLE_EXCLUDE_COLUMNS.get(table, []))
        # Schema restricted to non-excluded columns; computed once here for reuse.
        self.active_schema: pl.Schema = pl.Schema(
            {
                col: dtype
                for col, dtype in TABLE_SCHEMAS[table].items()
                if col not in self.exclude_cols
            }
        )

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _make_request_pool(self) -> urllib3.PoolManager:
        """Build a urllib3 connection pool with Masabi basic-auth headers."""
        # TODO: REMOVE 'cert_reqs="CERT_NONE"' BEFORE PRODUCTION RELEASE.
        # SSL certificate verification is disabled here solely to support local
        # development and testing against UAT endpoints. Leaving this in place
        # for a production deployment is a serious security risk — it makes the
        # connection vulnerable to man-in-the-middle attacks. Remove this flag
        # (and the disable_warnings call below) once proper certs are in place.
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        return urllib3.PoolManager(
            headers=urllib3.make_headers(basic_auth=f"{_API_USERNAME}:{_API_PASSWORD}"),
            timeout=urllib3.Timeout(total=60 * 10),
            retries=False,
            cert_reqs="CERT_NONE",
        )

    def _make_request(
        self,
        pool: urllib3.PoolManager,
        url: str,
        fields: dict[str, str],
    ) -> urllib3.BaseHTTPResponse:
        """
        Issue a GET request to the Masabi API.

        :param pool: urllib3 connection pool manager
        :param url: full endpoint URL
        :param fields: query-string parameters
        :return: raw urllib3 response
        :raises urllib3.exceptions.HTTPError: on any non-200 response
        """
        r = pool.request("GET", url, fields=fields)
        if r.status != 200:
            raise urllib3.exceptions.HTTPError(
                f"Masabi API error: url={url!r} status={r.status} "
                f"response={r.data.decode()!r}"
            )
        return r

    # ------------------------------------------------------------------
    # API pagination
    # ------------------------------------------------------------------

    def api_pages(
        self,
        pool: urllib3.PoolManager,
        from_ts: int,
        to_ts: int,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """
        Yield pages of API hits for the given serverTimestamp range.

        Results are requested in ascending serverTimestamp order. Pagination is
        handled via the `nextPageId` cursor returned in each response.

        :param pool: urllib3 connection pool manager
        :param from_ts: exclusive lower bound (ms since UTC epoch)
        :param to_ts: inclusive upper bound (ms since UTC epoch)
        """
        url = f"{API_ROOT}/{self.table}/"
        fields: dict[str, str] = {
            "filter": f"and(gt(serverTimestamp:{from_ts}),lte(serverTimestamp:{to_ts}))",
            "orderBy": "serverTimestamp:asc",
            "size": str(API_PAGE_SIZE),
        }
        log = ProcessLog(
            "masabi_api_pages", table=self.table, from_ts=from_ts, to_ts=to_ts
        )
        page_count = 0
        while True:
            r = self._make_request(pool, url, fields)
            data: dict[str, Any] = r.json()
            hits: list[dict[str, Any]] = data.get("hits", [])
            page_count += 1
            yield hits
            # Paginate only while the API signals more data and the page was non-empty.
            # Note: the reference example had a bug (`if nextPageId in resp_data` where
            # nextPageId was the *value*, not the key). The correct check is below.
            if "nextPageId" not in data or not hits:
                break
            fields["nextPageId"] = data["nextPageId"]
        log.complete(page_count=page_count)

    # ------------------------------------------------------------------
    # Record preprocessing
    # ------------------------------------------------------------------

    def preprocess_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Prepare a single API hit for NDJSON output.

        - Retains only columns defined in the table schema.
        - Drops any columns on the exclusion list (PII removal).
        - Serialises array/object values to JSON strings.
        - Columns absent from the API record are represented as null.

        :param record: raw JSON hit from the Masabi API
        :return: cleaned record ready to be written as a NDJSON line
        """
        result: dict[str, Any] = {}
        for col in self.active_schema.names():
            val = record.get(col)
            if col in self.json_cols:
                result[col] = json.dumps(val) if val is not None else None
            else:
                result[col] = val
        return result

    # ------------------------------------------------------------------
    # Core pipeline steps
    # ------------------------------------------------------------------

    def setup_job(self) -> int:
        """
        Determine the starting serverTimestamp for this run.

        Reads the max serverTimestamp from existing parquet metadata, defaulting
        to MASABI_START_TIMESTAMP_MS (2025-01-01 00:00:00 UTC) for the first run.

        :return: from_ts — exclusive lower bound for the API query (ms since epoch)
        """
        log = ProcessLog("masabi_setup_job", table=self.table)
        from_ts = MASABI_START_TIMESTAMP_MS
        existing = list_objects(f"s3://{self.export_folder}", in_filter=".parquet")
        if existing:
            _, max_ts = ds_metadata_min_max(
                ds_from_path(f"s3://{self.export_folder}"), "serverTimestamp"
            )
            if max_ts is not None:
                from_ts = int(max_ts)
        log.complete(from_ts=from_ts)
        return from_ts

    def fetch_and_write(
        self,
        pool: urllib3.PoolManager,
        from_ts: int,
        to_ts: int,
    ) -> str | None:
        """
        Fetch all records in (from_ts, to_ts] from the API and write as NDJSON.

        Records are written one page at a time for memory efficiency. The
        NDJSON file is written in ascending serverTimestamp order (enforced
        by the API `orderBy` parameter) so that partial writes are safe.

        :param pool: urllib3 connection pool manager
        :param from_ts: exclusive lower bound (ms since epoch)
        :param to_ts: inclusive upper bound (ms since epoch)
        :return: local path to the NDJSON file, or None if no records were returned
        """
        log = ProcessLog(
            "masabi_fetch_and_write",
            table=self.table,
            from_ts=from_ts,
            to_ts=to_ts,
        )
        ndjson_path = os.path.join(
            self.tmpdir, f"{self.table.replace('.', '_')}.ndjson"
        )
        total_rows = 0
        with open(ndjson_path, "w") as f:
            for page_hits in self.api_pages(pool, from_ts, to_ts):
                for hit in page_hits:
                    f.write(json.dumps(self.preprocess_record(hit)) + "\n")
                total_rows += len(page_hits)
        log.complete(total_rows=total_rows)
        return ndjson_path if total_rows > 0 else None

    def sync_parquet(self, ndjson_path: str) -> None:
        """
        Convert the NDJSON file to parquet and sync with S3.

        Downloads the most-recent existing S3 parquet file (if any), merges it
        with the newly-fetched data, and uploads the result. This keeps file
        count low while preserving the append-only invariant.

        :param ndjson_path: local path to the NDJSON file from fetch_and_write
        """
        log = ProcessLog("masabi_sync_parquet", table=self.table)

        # Convert NDJSON → local parquet using the pre-computed active schema.
        pq_path = ndjson_path.replace(".ndjson", ".parquet")
        pl.scan_ndjson(ndjson_path, schema=self.active_schema).sink_parquet(
            pq_path,
            compression="zstd",
            compression_level=3,
        )

        # Download the last existing S3 file so we can merge into it.
        found_objs = list_objects(f"s3://{self.export_folder}", in_filter=".parquet")
        sync_paths: list[str] = []
        if found_objs:
            last_s3 = found_objs[-1].path.replace("s3://", "")
            local_last = os.path.join(
                self.tmpdir, last_s3.replace("/table_", "/temp_")
            )
            download_object(found_objs[-1].path, local_last)
            sync_paths.append(local_last)
        sync_paths.append(pq_path)

        new_row_count = ds_from_path(pq_path).count_rows()
        log.add_metadata(new_rows=new_row_count)

        new_paths = pq_dataset_writer(
            source=ds_from_path(sync_paths),
            export_folder=os.path.join(self.tmpdir, self.export_folder),
            export_file_prefix="table",
        )

        # Perform S3 upload after sigterm check — uploads cannot be rolled back.
        sigterm_check()
        for new_path in new_paths:
            upload_path = new_path.replace(f"{self.tmpdir}/", "")
            upload_file(new_path, upload_path)
        log.complete()

    # ------------------------------------------------------------------
    # OdinJob entry point
    # ------------------------------------------------------------------

    def run(self) -> int:
        """
        Run one ingestion cycle for this Masabi table.

        Process:
            1. Determine the starting serverTimestamp from existing parquet metadata.
            2. Query the Masabi API for all records since that timestamp.
            3. Write records as NDJSON, convert to parquet, and sync to S3.

        :return: seconds until the next scheduled run
        """
        pool = self._make_request_pool()
        from_ts = self.setup_job()
        to_ts = int(time.time() * 1000)

        ndjson_path = self.fetch_and_write(pool, from_ts, to_ts)
        if ndjson_path is not None:
            self.sync_parquet(ndjson_path)

        return NEXT_RUN_DEFAULT


def schedule_masabi_archive(schedule: sched.scheduler) -> None:
    """
    Schedule ingestion jobs for all Masabi tables.

    :param schedule: application scheduler
    """
    for table in TABLES:
        job = ArchiveMasabi(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
