"""
Publish per-job status JSON to S3 for external monitoring.

Odin jobs answer "am I keeping up?" with data they already hold mid-run. Each job
writes that answer to one small JSON object per table under a status prefix, so
anyone with read access to the bucket can check freshness without scraping logs or
re-deriving it from dataset metadata.

Payload shape is deliberately per-job rather than one shared schema: a CDC job's
backlog is a span of time, an API job's is a count of pending work, and forcing
those into common field names would misrepresent both. What is shared is the
plumbing here -- serialize, upload, never fail the caller.
"""

import json
import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone

from odin.utils.aws.s3 import download_object
from odin.utils.aws.s3 import upload_file
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.logger import MdValues
from odin.utils.logger import ProcessLog

SECONDS_PER_DAY = 60 * 60 * 24
SECONDS_PER_HOUR = 60 * 60


def utc_now() -> datetime:
    """Return the current time as an aware UTC datetime."""
    return datetime.now(timezone.utc)


def ms_as_datetime(epoch_ms: int | float | None) -> datetime | None:
    """
    Convert milliseconds since the UTC epoch to an aware UTC datetime.

    :param epoch_ms: ms since epoch (may be None or out of range)

    :return: UTC datetime, or None if `epoch_ms` is missing or not convertible
    """
    if epoch_ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(epoch_ms) / 1000, tz=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def lag_seconds(newer: datetime | None, older: datetime | None) -> int | None:
    """
    Return whole seconds between two datetimes, or None if either is unknown.

    Not clamped: a negative result means `older` is ahead of `newer`, which is worth
    surfacing rather than hiding behind a floor of zero.

    :param newer: the later reference point
    :param older: the earlier reference point

    :return: int seconds, or None
    """
    if newer is None or older is None:
        return None
    return int((newer - older).total_seconds())


def iso_as_datetime(value: MdValues) -> datetime | None:
    """
    Parse an ISO-8601 string from a previous status payload back to a datetime.

    :param value: value read from JSON (may be missing, None, or malformed)

    :return: aware UTC datetime, or None if not parseable
    """
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def read_status(status_prefix: str, key: str, tmpdir: str) -> dict[str, MdValues] | None:
    """
    Read the status object a previous run published, if there is one.

    This is what makes run-over-run rates possible: each object carries the fields the
    next run differences against, so no separate history store is needed. One small GET
    per run.

    Best-effort by contract, and a miss is normal rather than exceptional -- the first
    run of a table has no predecessor. Any failure returns None, which callers render
    as absent rates rather than a failed run.

    :param status_prefix: bucket-relative status folder, e.g. CUBIC_ODS_FACT_STATUS
    :param key: object stem, typically the table name (no .json suffix)
    :param tmpdir: job scratch directory to stage the download in

    :return: the previous payload, or None if absent/unreadable
    """
    log = ProcessLog("read_status", status_prefix=status_prefix, key=key)
    try:
        local_path = os.path.join(tmpdir, "previous_status.json")
        downloaded = download_object(
            os.path.join(DATA_SPRINGBOARD, status_prefix, f"{key}.json"), local_path
        )
        if not downloaded:
            log.complete(found=False)
            return None
        with open(local_path) as status_file:
            payload = json.load(status_file)
        if not isinstance(payload, dict):
            log.complete(found=False)
            return None
        log.complete(found=True)
        return payload

    except Exception as exception:
        log.failed(exception)
        return None


def _rate_fields(
    row_count: int | None,
    prev_row_count: MdValues,
    run_duration_secs: float | None,
) -> dict[str, MdValues]:
    """Row-count delta and rows/sec over this run's processing time."""
    fields: dict[str, MdValues] = {}
    if not isinstance(prev_row_count, int) or row_count is None:
        return fields
    rows_added = row_count - prev_row_count
    fields["rows_added"] = rows_added
    if run_duration_secs and run_duration_secs > 0:
        fields["rows_per_second"] = round(rows_added / run_duration_secs, 2)
    return fields


def progress_fields(
    prev: dict[str, MdValues] | None,
    now: datetime,
    run_duration_secs: float | None,
    row_count: int | None = None,
    watermark: datetime | None = None,
    lag_secs: int | None = None,
    comparable: bool = True,
) -> dict[str, MdValues]:
    """
    Compare this run against the previous one: what moved, how fast, and time to catch up.

    Only selected scalars from `prev` are echoed, never the whole payload -- nesting the
    previous object would make each run's file contain every run before it.

    Two throughput measures, because they answer different questions:

      * ``data_days_per_processing_hour`` -- how much event time the job digests per hour
        it spends working. A pure throughput figure, independent of how often it is
        scheduled, and the one to compare across tables.
      * ``catchup_ratio`` -- watermark advance divided by *wall* time since the last run.
        Above 1.0 means gaining on the source, below means losing. This is the honest
        keep-up test, because the upstream frontier keeps moving while the job sleeps
        between runs.

    Both catch-up estimates are therefore reported:

      * ``catchup_processing_seconds`` -- lag / throughput. Answers "how much compute is
        left", holding the frontier still. Never None while the job is making progress,
        and always the smaller (more optimistic) of the two.
      * ``catchup_wall_seconds`` -- lag / (catchup_ratio - 1), accounting for the frontier
        advancing one second per second. None when ``catchup_ratio <= 1``, which is not a
        gap in the data but the finding itself: the table will never catch up on the
        current schedule, no matter how fast each individual run is.

    Rates are one-cycle deltas, not smoothed averages, so a single unusually large or
    small batch swings them; they describe the last cycle, not a trend.

    :param prev: previous status payload, or None on a first run
    :param now: this run's ``last_run`` timestamp
    :param run_duration_secs: processing seconds this run spent
    :param row_count: current row count
    :param watermark: current data-time position (None for jobs without an event time)
    :param lag_secs: remaining backlog in data-seconds (seq lag, or clock lag)
    :param comparable: False when the previous run measured something else (e.g. a
        different snapshot), which makes every delta against it meaningless

    :return: progress fields, empty-ish when there is nothing valid to compare against
    """
    fields: dict[str, MdValues] = {
        "run_duration_seconds": (
            None if run_duration_secs is None else round(run_duration_secs, 2)
        ),
    }
    if prev is None or not comparable:
        return fields

    prev_run = iso_as_datetime(prev.get("last_run"))
    fields["previous_last_run"] = None if prev_run is None else prev_run.isoformat()
    fields["previous_row_count"] = prev.get("row_count")
    wall_secs = lag_seconds(now, prev_run)
    fields["seconds_since_last_run"] = wall_secs

    fields.update(_rate_fields(row_count, prev.get("row_count"), run_duration_secs))

    prev_watermark = iso_as_datetime(
        prev.get("watermark_datetime")
        or prev.get("max_change_seq_datetime")
        or prev.get("max_timestamp_datetime")
    )
    advance = lag_seconds(watermark, prev_watermark)
    if advance is None:
        return fields
    fields["watermark_advance_seconds"] = advance

    if run_duration_secs and run_duration_secs > 0:
        fields["data_days_per_processing_hour"] = round(
            (advance / SECONDS_PER_DAY) / (run_duration_secs / SECONDS_PER_HOUR), 3
        )
    if wall_secs and wall_secs > 0:
        fields["catchup_ratio"] = round(advance / wall_secs, 3)

    if lag_secs is None:
        return fields
    if lag_secs <= 0:
        fields["catchup_processing_seconds"] = 0
        fields["catchup_wall_seconds"] = 0
        fields["projected_caught_up"] = None
        return fields
    if advance > 0 and run_duration_secs and run_duration_secs > 0:
        fields["catchup_processing_seconds"] = int(lag_secs / (advance / run_duration_secs))
    # Gaining only if the watermark outruns the frontier's one-second-per-second advance.
    if wall_secs and wall_secs > 0 and advance > wall_secs:
        wall_remaining = int(lag_secs / ((advance - wall_secs) / wall_secs))
        fields["catchup_wall_seconds"] = wall_remaining
        fields["projected_caught_up"] = (now + timedelta(seconds=wall_remaining)).isoformat()
    else:
        fields["catchup_wall_seconds"] = None
        fields["projected_caught_up"] = None
    return fields


def publish_status(
    status_prefix: str,
    key: str,
    tmpdir: str,
    payload: dict[str, MdValues],
) -> None:
    """
    Publish `payload` to ``s3://<DATA_SPRINGBOARD>/<status_prefix>/<key>.json``.

    Overwrites on every call, so the object always reflects the latest completed run.
    A single PUT per table keeps writes atomic; jobs run as separate processes, so an
    aggregate file would need a read-modify-write that could drop tables.

    Best-effort by contract: any failure is logged and swallowed. Status is telemetry
    and must never fail an otherwise successful job. The consequence is that a stale
    object means "this job is not finishing", which is itself the signal worth having.

    :param status_prefix: bucket-relative status folder, e.g. CUBIC_ODS_FACT_STATUS
    :param key: object stem, typically the table name (no .json suffix)
    :param tmpdir: job scratch directory to stage the file in
    :param payload: JSON-serializable status fields
    """
    log = ProcessLog("publish_status", status_prefix=status_prefix, key=key)
    try:
        status_path = os.path.join(tmpdir, "status.json")
        with open(status_path, "w") as status_file:
            json.dump(payload, status_file, indent=2)
        upload_file(
            status_path,
            os.path.join(DATA_SPRINGBOARD, status_prefix, f"{key}.json"),
            extra_args={"ContentType": "application/json"},
        )
        log.complete(**payload)

    except Exception as exception:
        log.failed(exception)
