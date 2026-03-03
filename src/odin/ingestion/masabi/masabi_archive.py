"""Masabi archive job placeholder."""

from __future__ import annotations

import os
import sched
import time
from typing import Any, Generator

import urllib3
import json

from odin.utils.logger import ProcessLog
from odin.job import NEXT_RUN_DEFAULT, OdinJob, job_proc_schedule
from odin.utils.locations import DATA_SPRINGBOARD, MASABI_DATA


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# API base URL
API_ROOT = os.getenv("MASABI_API_ROOT", "")

# Basic-auth credentials (validated at startup by run.py)
_API_USERNAME = os.getenv("MASABI_DATA_API_USERNAME", "")
_API_PASSWORD = os.getenv("MASABI_DATA_API_PASSWORD", "")

# Page size: Masabi's maximum is 1000. Override with MASABI_API_PAGE_SIZE=100 for
# dev environments where the dataset is small.
API_PAGE_SIZE = int(os.getenv("MASABI_API_PAGE_SIZE", "1000"))

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

# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------

TABLES = [
    "retail.account_actions",
]


class ArchiveMasabi(OdinJob):
    """Basic Odin job stub for Masabi ingestion."""

    def __init__(self, table: str) -> None:
        """Create Job instance."""
        self.table = table
        self.start_kwargs = {"table": table}
        self.export_folder = os.path.join(DATA_SPRINGBOARD, MASABI_DATA, table)
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
        url = f"{API_ROOT}/{self.table}/"
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
        with open(ndjson_path, "w") as f:
            for page_hits in self.api_pages(pool, from_ts, to_ts):
                # TODO Check schema
                for hit in page_hits:
                    min_obs_ts = min(min_obs_ts, hit["serverTimestamp"])
                    max_obs_ts = max(max_obs_ts, hit["serverTimestamp"])
                    f.write(json.dumps(hit) + "\n")  # TODO Check JSON elements match schema
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

    def setup_job(self):
        """
        TODO: This will read previous parquet file to get the actual starting time.

        For now returns the hardcoded total database start time.
        """
        return MASABI_START_TIMESTAMP_MS

    def run(self) -> int:
        """Execute the Masabi archive run loop."""
        print(f"Ingest: {self.table}")

        pool = self._make_request_pool()
        from_ts = self.setup_job()
        to_ts = int(time.time() * 1000)

        ndjson_path = self.fetch_and_write(pool, from_ts, to_ts)
        print(ndjson_path)

        return NEXT_RUN_DEFAULT  # 6 hours


def schedule_masabi_archive(schedule: sched.scheduler) -> None:
    """
    Schedule the Masabi archive job on the provided scheduler.

    :param schedule: application scheduler
    """
    for table in TABLES:
        job = ArchiveMasabi(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
