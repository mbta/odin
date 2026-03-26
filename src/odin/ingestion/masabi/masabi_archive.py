from __future__ import annotations

import os
import sched
import time
from typing import Any, Generator
import urllib3
import json
import yaml
import polars as pl
import pyarrow.parquet as pq

from odin.utils.logger import ProcessLog
from odin.job import NEXT_RUN_DEFAULT, OdinJob, job_proc_schedule
from odin.utils.locations import DATA_SPRINGBOARD, MASABI_DATA
from odin.utils.aws.s3 import s3_folder
from odin.utils.aws.s3 import download_object
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import upload_file
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import ds_metadata_min_max
from odin.utils.parquet import pq_dataset_writer
from odin.utils.runtime import sigterm_check

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# API base URL
API_ROOT = os.getenv("MASABI_DATA_API_URL", "")

# Basic-auth credentials (validated at startup by run.py)
_API_USERNAME = os.getenv("MASABI_DATA_API_USERNAME", "")
_API_PASSWORD = os.getenv("MASABI_DATA_API_PASSWORD", "")

# Page size: Masabi's maximum is 1000. Override with MASABI_API_PAGE_SIZE=100 for
# dev environments where the dataset is small.
API_PAGE_SIZE = int(os.getenv("MASABI_API_PAGE_SIZE", "1000"))

_SCHEMA_URL = os.getenv("MASABI_DATA_SCHEMA_URL", "")

# Maximum update size: Adjust to match the maximum size that can be safely handled
# by the ECS environment's RAM and disk resources.
MAXIMUM_ROWS_PER_RUN = 100

# Retry config for individual API page requests.
# On a non-200 response or network error, the request is retried up to
# API_MAX_RETRIES times, waiting API_RETRY_DELAY_S seconds between each attempt.
API_MAX_RETRIES: int = 3
API_RETRY_DELAY_S: float = 5.0

# Minimum interval between consecutive API requests (seconds).
API_MIN_REQUEST_INTERVAL_S: float = 1.0

# Exclusive lower bound for the initial historical backfill: 2025-01-01 00:00:00 UTC (ms).
MASABI_START_TIMESTAMP_MS: int = 1_735_689_600_000

TABLES = [
    "retail.account_actions",
    "retail.activations",
    "retail.ticket_purchases",
    "retail.tickets",
    "retail.rider_entitlement_events",
]

_YAML_TYPE_MAP: dict[str, pl.DataType] = {
    "string": pl.String(),
    "boolean": pl.Boolean(),
    "number": pl.Float64(),
    "array": pl.String(),
    "object": pl.String(),
}
_JSON_YAML_TYPES = frozenset({"array", "object"})

# Per-table overrides: columns whose API values arrive as JSON strings
# but whose schema type is numeric.  Values are coerced to float before
# NDJSON serialization; un-coercible values are null-filled and logged.
TABLE_NUMERIC_OVERRIDES: dict[str, frozenset[str]] = {
    "retail.account_actions": frozenset({"hourOfDay"}),
    "retail.activations": frozenset({"hourOfDay"}),
    "retail.ticket_purchases": frozenset({"hourOfDay"}),
    "retail.tickets": frozenset({"hourOfDay"}),
    "retail.rider_entitlement_events": frozenset({"hourOfDay"}),
}

# ---------------------------------------------------------------------------
# Schema Retrieval
# ---------------------------------------------------------------------------

TABLE_SCHEMAS = None
TABLE_JSON_COLS = None


def _fetch_schema_spec(connection_pool: urllib3.PoolManager) -> dict[str, Any]:
    """
    Fetch the Masabi OpenAPI schema spec.

    :return: parsed OpenAPI spec as a dict
    :raises RuntimeError: if the remote fetch returns a non-200 status
    """
    schema_remote_path = os.path.join(_SCHEMA_URL, "ds-query-schema.yaml")

    r = connection_pool.request("GET", schema_remote_path)
    if r.status != 200:
        raise RuntimeError(
            f"Failed to fetch Masabi schema from {schema_remote_path!r}: status={r.status}"
        )
    return yaml.safe_load(r.data)


def _load_schemas(
    tables: list[str], connection_pool: urllib3.PoolManager
) -> tuple[dict[str, pl.Schema], dict[str, frozenset[str]]]:
    """
    Load per-table column schemas and JSON-column sets from the schema spec.

    :param tables: table names to load (must all exist in the spec)
    :return: (TABLE_SCHEMAS, TABLE_JSON_COLS)
    :raises KeyError: if a table in *tables* is absent from the spec
    """
    log = ProcessLog(process="masabi_schema_download")
    spec = _fetch_schema_spec(connection_pool)
    all_schemas = spec["components"]["schemas"]
    schemas: dict[str, pl.Schema] = {}
    json_cols: dict[str, frozenset[str]] = {}

    for table in tables:
        if table not in all_schemas:
            log.add_metadata(missing_table=table)
            log.failed(exception=KeyError())
            raise KeyError(
                f"Table {table!r} not found in Masabi schema spec; "
                "update the schema source or remove it from TABLES"
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

    log.complete(schema_count=len(schemas), json_col_count=len(json_cols))
    return schemas, json_cols


_POLARS_TO_ARROW_TYPE_NAME: dict[type[pl.DataType], str] = {
    pl.String: "string",
    pl.Boolean: "bool",
    pl.Float64: "double",
    pl.Int64: "int64",
    pl.Int32: "int32",
}


def _arrow_type_name(pl_dtype: pl.DataType) -> str:
    """Return a comparable Arrow type-name string for a polars DataType."""
    return _POLARS_TO_ARROW_TYPE_NAME.get(type(pl_dtype), "string")


def _check_json_page(
    hits: list[dict[str, Any]],
    schema: pl.Schema,
    json_cols: frozenset[str],
    warned_columns: set[str],
) -> None:
    """
    Validate a page of JSON hits against the expected schema.

    Checks the first hit in the page for:
      - Extra columns (in JSON but not in schema) -> warning
      - Missing columns (in schema but not in JSON) -> tolerated (null-fill)
      - JSON-typed columns whose values are not list/dict -> warning
      - Number-typed columns whose values are not numeric -> warning

    Warnings are deduplicated across pages via warned_columns.

    :param hits: list of JSON row dicts from a single API page
    :param schema: expected polars schema (column name -> DataType)
    :param json_cols: set of column names expected to carry array/object values
    :param warned_columns: mutable set tracking which column warnings have been emitted
    """
    if not hits:
        return

    log = ProcessLog("masabi_check_json_page")
    sample = hits[0]
    schema_names = set(schema.names())
    hit_keys = set(sample.keys())

    extra = hit_keys - schema_names
    if extra and "extra_columns" not in warned_columns:
        log.add_metadata(extra_columns_in_json=sorted(extra))
        warned_columns.add("extra_columns")

    # Type spot-checks on the sample row
    for col in hit_keys & schema_names:
        val = sample[col]
        if val is None:
            continue
        if col in json_cols:
            if not isinstance(val, (list, dict)) and col not in warned_columns:
                log.add_metadata(
                    unexpected_type_column=col,
                    unexpected_type_actual=type(val).__name__,
                    unexpected_type_expected="list/dict",
                )
                warned_columns.add(col)
        elif isinstance(schema[col], pl.Float64):
            if not isinstance(val, (int, float)) and col not in warned_columns:
                log.add_metadata(
                    unexpected_type_column=col,
                    unexpected_type_actual=type(val).__name__,
                    unexpected_type_expected="number",
                )
                warned_columns.add(col)

    log.complete()


def _serialize_json_cols(hit: dict[str, Any], json_cols: frozenset[str]) -> dict[str, Any]:
    """
    Serialize array/object columns to JSON strings in-place.

    The YAML schema maps array and object types to pl.String().
    Raw list/dict values must be serialized before writing NDJSON so that
    pl.scan_ndjson() can coerce them without error.

    :param hit: single JSON row dict (mutated in-place)
    :param json_cols: set of column names whose values should be serialized
    :return: the same dict, with list/dict values replaced by JSON strings
    """
    for col in json_cols:
        val = hit.get(col)
        if isinstance(val, (list, dict)):
            hit[col] = json.dumps(val)
    return hit


def _coerce_hit_numerics(
    hit: dict[str, Any],
    numeric_cols: frozenset[str],
    warned_columns: set[str],
) -> dict[str, Any]:
    """
    Coerce string-encoded numeric columns to float in-place.

    The Masabi pipeline uses strict schema validation, and when
    there are type mismatches between the data and provided
    schema, we should address these issues as minimally and
    specifcally as possible.

    This function targets columns defined in the schema as numeric whose
    values are arriving as JSON strings (e.g. "8" instead of 8).
    Columns listed in the per-table TABLE_NUMERIC_OVERRIDES set
    are coerced here so that the downstream strict schema checking
    can still catch any unexpected behavior.

    Values that cannot be converted are replaced with None (which
    polars reads as a typed null) and a warning is logged once per
    column per run.

    :param hit: single JSON row dict (mutated in-place)
    :param numeric_cols: column names to attempt float coercion on
    :param warned_columns: mutable set for deduplicating warnings
    :return: the same dict, with coerced or null-filled values
    """
    for col in numeric_cols:
        val = hit.get(col)
        if val is None or isinstance(val, (int, float)):
            continue
        try:
            hit[col] = float(val)
        except (ValueError, TypeError):
            hit[col] = None
            warn_key = f"coerce_{col}"
            if warn_key not in warned_columns:
                log = ProcessLog("masabi_coerce_hit_numerics")
                log.add_metadata(
                    null_filled_column=col,
                    null_filled_value=repr(val),
                    null_filled_reason="not coercible to float",
                )
                log.complete()
                warned_columns.add(warn_key)
    return hit


def _check_parquet_schema(parquet_path: str, expected_schema: pl.Schema) -> None:
    """
    Compare an existing parquet file's schema against the expected polars schema.

    - Extra columns in the parquet (not in expected schema): logged as warning.
    - Missing columns in the parquet (in expected schema but absent): logged as warning.
    - Type mismatch on a shared column: raises SchemaError.

    :param parquet_path: local path to the downloaded parquet file
    :param expected_schema: the polars schema derived from the YAML spec
    :raises pl.exceptions.SchemaError: on column type mismatch
    """
    log = ProcessLog("masabi_check_parquet_schema", path=parquet_path)
    pq_schema = pq.read_schema(parquet_path)
    pq_col_types: dict[str, str] = {field.name: str(field.type) for field in pq_schema}
    expected_names = set(expected_schema.names())
    pq_names = set(pq_col_types.keys())

    extra_in_pq = pq_names - expected_names
    if extra_in_pq:
        log.add_metadata(extra_columns_in_parquet=sorted(extra_in_pq))

    missing_in_pq = expected_names - pq_names
    if missing_in_pq:
        log.add_metadata(missing_columns_in_parquet=sorted(missing_in_pq))

    mismatches: list[str] = []
    for col in pq_names & expected_names:
        expected_arrow = _arrow_type_name(expected_schema[col])
        actual_arrow = pq_col_types[col]
        if expected_arrow == "string" and actual_arrow in ("string", "large_string", "utf8"):
            continue
        if expected_arrow != actual_arrow:
            mismatches.append(f"{col}: parquet={actual_arrow}, expected={expected_arrow}")

    if mismatches:
        log.failed(exception=pl.exceptions.SchemaError(", ".join(mismatches)))
        raise pl.exceptions.SchemaError(
            f"Parquet schema type mismatch for {parquet_path}: {'; '.join(mismatches)}. "
            "A schema migration may be required."
        )

    log.complete()


class ArchiveMasabi(OdinJob):
    """Basic Odin job stub for Masabi ingestion."""

    def __init__(self, table: str) -> None:
        """Create Job instance."""
        self.table = table
        self.start_kwargs = {"table": table}
        self.export_folder = s3_folder(os.path.join(DATA_SPRINGBOARD, MASABI_DATA, table))
        self._last_request_time: float = 0.0

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
        Issue a GET request to the Masabi API, retrying on failure.

        Retries up to API_MAX_RETRIES times (with API_RETRY_DELAY_S seconds between
        each attempt) on non-200 responses or network-level errors. Raises on the
        final attempt if all retries are exhausted.

        :param pool: urllib3 connection pool manager
        :param url: full endpoint URL
        :param fields: query-string parameters
        :return: raw urllib3 response
        :raises urllib3.exceptions.HTTPError: if all attempts return a non-200 response
        :raises urllib3.exceptions.RequestError: if all attempts fail with a network error
        """
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < API_MIN_REQUEST_INTERVAL_S:
            time.sleep(API_MIN_REQUEST_INTERVAL_S - elapsed)
        self._last_request_time = time.monotonic()

        last_exc: Exception | None = None
        log = None  # Only log from this if something fails
        for attempt in range(API_MAX_RETRIES + 1):
            try:
                r = pool.request("GET", url, fields=fields)
                if r.status != 200:
                    raise urllib3.exceptions.HTTPError(
                        f"Masabi API error: url={url!r} status={r.status} "
                        f"response={r.data.decode()!r}"
                    )
                if log is not None:
                    log.complete()
                return r
            except (urllib3.exceptions.HTTPError, urllib3.exceptions.RequestError) as exc:
                if log is None:
                    log = ProcessLog(
                        "masabi_make_request",
                        url=url,
                        attempt_number=attempt,
                        retry_on_exception=str(exc),
                    )
                else:
                    log.add_metadata(attempt=attempt, retry_on_exception=str(exc))
                last_exc = exc
                if attempt < API_MAX_RETRIES:
                    time.sleep(API_RETRY_DELAY_S)
                    continue

        if log is not None:
            log.failed(exception=last_exc)  # type: ignore[misc]
        raise last_exc  # type: ignore[misc]

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
        url = "/".join([API_ROOT.rstrip("/"), "data-store/query/v2/MBTA", self.table]) + "/"
        fields: dict[str, str] = {
            "filter": f"and(gt(serverTimestamp:{from_ts}),lte(serverTimestamp:{to_ts}))",
            "orderBy": "serverTimestamp:asc",
            "size": str(API_PAGE_SIZE),
        }
        log = ProcessLog("masabi_api_pages", table=self.table, from_ts=from_ts, to_ts=to_ts)
        page_count = 0
        min_hits_per_page = float("inf")
        max_hits_per_page = -1
        while True:
            r = self._make_request(pool, url, fields)
            data: dict[str, Any] = r.json()
            # Each hit is {"type": "<table>", "doc": { ...fields... }}.
            # Unwrap "doc" here so the rest of the pipeline sees flat records.
            hits: list[dict[str, Any]] = [h["doc"] for h in data.get("hits", [])]
            page_count += 1
            min_hits_per_page = min(min_hits_per_page, len(hits))
            max_hits_per_page = max(max_hits_per_page, len(hits))
            yield hits
            # Paginate only while the API signals more data and the page was non-empty.
            # Note: the reference example had a bug (`if nextPageId in resp_data` where
            # nextPageId was the *value*, not the key). The correct check is below.
            if "nextPageId" not in data or not hits:
                break
            fields["nextPageId"] = data["nextPageId"]
        log.complete(
            page_count=page_count,
            min_hits_per_page=min_hits_per_page,
            max_hits_per_page=max_hits_per_page,
        )

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

        Each page is validated against self.schema (column presence and
        value types) via _check_json_page(). Columns whose YAML type is
        array or object are serialized to JSON strings before writing
        so that the downstream pl.scan_ndjson(..., schema=self.schema) call
        can coerce them without error.

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
        ndjson_path = os.path.join(self.tmpdir, f"{self.table.replace('.', '_')}.ndjson")
        total_rows = 0
        maximum_rows = False
        min_obs_ts = float("inf")
        max_obs_ts = -1
        warned_columns: set[str] = set()
        with open(ndjson_path, "w") as f:
            for page_hits in self.api_pages(pool, from_ts, to_ts):
                min_page_ts = min([x["serverTimestamp"] for x in page_hits])
                max_page_ts = max([x["serverTimestamp"] for x in page_hits])
                if min_page_ts < max_obs_ts:
                    log.add_metadata(
                        warning=(
                            f"Page timestamp {min_page_ts} "
                            f"prior to previous maximum timestamp, {max_obs_ts}"
                        )
                    )
                min_obs_ts = min(min_obs_ts, min_page_ts)
                max_obs_ts = max(max_obs_ts, max_page_ts)

                _check_json_page(page_hits, self.schema, self.json_cols, warned_columns)
                for hit in page_hits:
                    _coerce_hit_numerics(hit, self.numeric_overrides, warned_columns)
                    _serialize_json_cols(hit, self.json_cols)
                    f.write(json.dumps(hit) + "\n")
                    total_rows += 1
                    if total_rows >= MAXIMUM_ROWS_PER_RUN:
                        maximum_rows = True
                        break
                if maximum_rows:
                    break

        log.complete(
            total_rows=total_rows,
            encountered_max_rows=maximum_rows,
            min_obs_ts=min_obs_ts,
            max_obs_ts=max_obs_ts,
        )
        return ndjson_path if total_rows > 0 else None

    def sync_parquet(self, ndjson_path: str) -> None:
        """
        Convert the NDJSON file to parquet and sync with S3.

        Downloads the most-recent existing S3 parquet file (if any), merges it
        with the newly-fetched data, and uploads the result. This keeps file
        count low while preserving the append-only invariant.

        Rows at the maximum serverTimestamp are dropped before writing.
        Rationale: serverTimestamp values are not strictly unique — Masabi may
        write additional rows with the same timestamp after our run completes.
        The next run uses an *exclusive* lower bound
        (`gt(serverTimestamp:{from_ts})`), so any rows whose timestamp equals
        `from_ts` would be silently skipped, causing data loss. By dropping
        the boundary rows now, we guarantee that the next run's lower bound
        sits below those rows and re-fetches them in full (along with any
        late-arriving rows at that same timestamp).

        :param ndjson_path: local path to the NDJSON file from fetch_and_write
        """
        log = ProcessLog("masabi_sync_parquet", table=self.table)
        pq_path = ndjson_path.replace(".ndjson", ".parquet")

        lf = pl.scan_ndjson(ndjson_path, schema=self.schema)

        max_ts = lf.select(pl.col("serverTimestamp").max()).collect().item()
        ts_filtered_lf = lf.filter(pl.col("serverTimestamp") < max_ts)
        ts_filtered_lf.sink_parquet(
            pq_path,
            compression="zstd",
            compression_level=3,
        )
        log.add_metadata(boundary_ts_dropped=max_ts)

        # Download the last existing S3 file so we can merge into it.
        found_objs = list_objects(self.export_folder, in_filter=".parquet")
        sync_paths: list[str] = []
        if found_objs:
            last_s3 = found_objs[-1].path.replace("s3://", "")
            local_last = os.path.join(self.tmpdir, last_s3.replace("/table_", "/temp_"))
            download_object(found_objs[-1].path, local_last)
            _check_parquet_schema(local_last, self.schema)
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
        log.complete(uploaded_files=",".join(new_paths))

    def setup_job(self):
        """Read the pre-existing parquet files to get the start time for data."""
        log = ProcessLog("masabi_setup_job", table=self.table)

        from_ts = MASABI_START_TIMESTAMP_MS
        existing_ds = ds_from_path(self.export_folder)
        existing_ds_rows = existing_ds.count_rows()
        if existing_ds_rows:
            _, max_ts = ds_metadata_min_max(existing_ds, "serverTimestamp")
            if max_ts is not None:
                from_ts = int(max_ts)

        log.complete(from_ts=from_ts, existing_ds_size=existing_ds_rows)
        return from_ts

    def run(self) -> int:
        """Execute the Masabi archive run loop."""
        global TABLE_SCHEMAS
        global TABLE_JSON_COLS

        log = ProcessLog(process="masabi_run")

        pool = self._make_request_pool()

        if TABLE_SCHEMAS is None or TABLE_JSON_COLS is None:
            TABLE_SCHEMAS, TABLE_JSON_COLS = _load_schemas(TABLES, pool)

        from_ts = self.setup_job()
        to_ts = int(time.time() * 1000)

        self.schema = TABLE_SCHEMAS.get(self.table, None)
        assert self.schema is not None
        self.json_cols = TABLE_JSON_COLS.get(self.table, frozenset())
        self.numeric_overrides = TABLE_NUMERIC_OVERRIDES.get(self.table, frozenset())
        log.add_metadata(schema_size=len(self.schema))

        ndjson_path = self.fetch_and_write(pool, from_ts, to_ts)
        if ndjson_path is not None:
            self.sync_parquet(ndjson_path)

        log.complete()
        return NEXT_RUN_DEFAULT  # 6 hours


def schedule_masabi_archive(schedule: sched.scheduler) -> None:
    """
    Schedule the Masabi archive job on the provided scheduler.

    :param schedule: application scheduler
    """
    for table in TABLES:
        job = ArchiveMasabi(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
