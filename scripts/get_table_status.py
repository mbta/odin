#!/usr/bin/env python3
"""
Read the per-table freshness JSON that the Odin jobs publish to S3 and summarize it.

Each job writes one small JSON object per table under odin/logs/<group>/<table>.json
every time it runs (see odin/utils/status.py). This script reads those objects so a
user with S3 access can answer "is this table keeping up?" without scraping logs.

Four groups, one per publishing job:

  * ODS        Cubic ODS fact tables  (odin/logs/ods/)
  * delta_ODS  Cubic ODS Delta tables (odin/logs/ods_delta/)
  * AFC        S&B AFC API ingestion  (odin/logs/afc/)
  * masabi     Masabi API ingestion   (odin/logs/masabi/)

Each table gets three independent checks, and a table can fail any number of them at
once. A table that fails none is OK.

  * STALE: no object, or no run within --stale-seconds (default 24h). Jobs are
    serialized rather than run in parallel, so one flat threshold is more meaningful than
    each job's own cadence. A stale object means the job is not finishing -- which is
    itself the signal worth having.
  * BEHIND_SOURCE: more recent data was available but not ingested when the job was
    last run, the job's own keep-up signal says so (cdc_records_pending /
    merge_budget_full / caught_up false / jobs_lag > 0), or the backlog
    (seq_lag_seconds) exceeds --source-lag-seconds (default 4h). A STALE table
    can also be BEHIND_SOURCE as of its last run.
  * DATA_OUT_OF_TIME_RANGE: the newest value in a column defined here is further behind
    (or ahead of) the current time than specified in TIME_RANGES. Opt-in.

Files are read once from S3, and two reports are emitted:

  * engineering: every finding on every table
  * stakeholder: tables violating DATA_OUT_OF_TIME_RANGE rules

--only-if-problems skips whichever report has nothing to report.

Views are hand-maintained in a constant, and include all required tables for each view,
which can span across groups. A view is up to date if none of its tables have findings.

Usage:
    python scripts/get_table_status.py                      # engineering summary (hides OK tables)
    python scripts/get_table_status.py --detailed           # include info for OK tables
    python scripts/get_table_status.py --audience stakeholder  # the stakeholder report
    python scripts/get_table_status.py --table ODS:EDW.SALE_TRANSACTION  # one table's details
    python scripts/get_table_status.py --slack              # post both reports to Slack
    python scripts/get_table_status.py --slack-test         # preview both messages, post nothing
"""

import argparse
import json
import logging
import os
import sys
import tempfile
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, NamedTuple, Optional

from odin.utils.aws.s3 import download_object
from odin.utils.aws.s3 import list_objects
from odin.utils.locations import AFC_STATUS
from odin.utils.locations import CUBIC_ODS_DELTA_STATUS
from odin.utils.locations import CUBIC_ODS_FACT_STATUS
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_STATUS
from odin.utils.logger import LOGGER_NAME


# This script is standardized on seconds. Use these constants for readability
MINUTE = 60
HOUR = MINUTE * 60
DAY = HOUR * 24

class Group(NamedTuple):
    prefix: str  # location of logs in bucket
    module: str  # location of table lists in code
    attr: str  # name of table list
    manifest_index: Optional[str] = None  # used for delta_ODS; see expected_tables()


GROUPS: dict[str, Group] = {
    "ODS": Group(CUBIC_ODS_FACT_STATUS, "odin.ingestion.qlik.tables", "CUBIC_ODS_TABLES"),
    # CUBIC_ODS_DELTA_TABLES only lists the tables belonging to whichever instance this
    # process resolves to, but the status prefix holds every instance's objects. Read the
    # manifest directly so the expected list covers all of them.
    "delta_ODS": Group(
        CUBIC_ODS_DELTA_STATUS,
        "odin.ingestion.qlik.tables",
        "TABLE_MANIFEST",
        manifest_index="DELTA_FACT_JOB_IND",
    ),
    "AFC": Group(AFC_STATUS, "odin.ingestion.afc.afc_tables", "API_TABLES_INSTANCE"),
    "masabi": Group(MASABI_STATUS, "odin.ingestion.masabi.masabi_tables", "TABLES_INSTANCE"),
}

OK, STALE, BEHIND_SOURCE, DATA_OUT_OF_TIME_RANGE = (
    "OK",
    "STALE",
    "BEHIND_SOURCE",
    "DATA_OUT_OF_TIME_RANGE",
)

# The order that findings are checked/listed in
CHECKS = (STALE, BEHIND_SOURCE, DATA_OUT_OF_TIME_RANGE)

# The most recent job run for a table must be at least this recent, else: STALE
DEFAULT_STALE_SECONDS = 24 * HOUR

# How far behind the newest upstream data a table may be after its last run. One run
# cadence of the slowest job (4h on alpha), so a table between runs is not reported.
DEFAULT_SOURCE_LAG_SECONDS = 4 * HOUR

# Per-table BEHIND_SOURCE thresholds, in seconds.
# Note: a job's own keep-up flags (cdc_budget_nearly_full, merge_budget_full, caught_up,
# jobs_lag) raise BEHIND_SOURCE regardless of the threshold set here.
SOURCE_LAG_SECONDS_OVERRIDES: dict[str, float] = {}


class TimeRange(NamedTuple):
    """How far the newest value in specified column can be behind 'now'"""

    column: str
    max_behind_seconds: float
    # Most columns should never be ahead of 'now' (except e.g. start_date/expiration_date)
    max_ahead_seconds: float = 0


# Opt-in per-table DATA_OUT_OF_TIME_RANGE expectations
TIME_RANGES: dict[str, TimeRange] = {
    "ODS:EDW.FRM_CRDB_RECON_SYSCONF_ACQCONF": TimeRange(
        "capture_date", max_behind_seconds=2 * DAY, max_ahead_seconds=0
    ),
    "AFC:v_validation_taps": TimeRange(
        "transactiontimestamp", max_behind_seconds=4 * HOUR, max_ahead_seconds=0
    ),
    "AFC:v_sales_txns": TimeRange(
        "transactiontimestamp", max_behind_seconds=4 * HOUR, max_ahead_seconds=0
    ),
    "AFC:v_products": TimeRange(
        "txn_timestamp", max_behind_seconds=4 * HOUR, max_ahead_seconds=0
    ),
    "AFC:v_svw_balance_changes": TimeRange(
        "timestamp", max_behind_seconds=4 * HOUR, max_ahead_seconds=0
    ),
    "AFC:v_eventhistory": TimeRange(
        "eventtime", max_behind_seconds=4 * HOUR, max_ahead_seconds=0
    ),
    "AFC:v_mainshift": TimeRange(
        "startcreadate", max_behind_seconds=4 * HOUR, max_ahead_seconds=0
    ),
    "AFC:v_trips": TimeRange(
        "transactiontimestamp", max_behind_seconds=4 * HOUR, max_ahead_seconds=0
    ),
    "masabi:retail.activations": TimeRange(
        "purchaseTimestamp", max_behind_seconds=4 * HOUR, max_ahead_seconds=0
    ),
}

# Per-table STALE thresholds, in seconds.
# High-frequency tables should be marked as STALE after a much shorter delay that the default
STALE_SECONDS_OVERRIDES: dict[str, float] = {
    "ODS:EDW.PATRONAGE_SUMMARY": 1 * HOUR,
    "AFC:v_validation_taps": 1 * HOUR,
    "AFC:v_sales_txns": 1 * HOUR,
    "AFC:v_products": 1 * HOUR,
    "AFC:v_svw_balance_changes": 1 * HOUR,
    "AFC:v_eventhistory": 1 * HOUR,
    "AFC:v_mainshift": 1 * HOUR,
    "AFC:v_trips": 1 * HOUR,
    "masabi:retail.ticket_purchases": 1 * HOUR,
    "masabi:retail.activations": 1 * HOUR,
}

# Views are up to date when no member table has a finding; otherwise each member's
# findings are listed under the view.
VIEWS: dict[str, tuple[str, ...]] = {
    "comp_b_addendum_farerev_payg_trip_txn_a": (
        "ODS:EDW.FARE_REVENUE_REPORT_SCHEDULE",
        "delta_ODS:EDW.SALE_TRANSACTION",
        "delta_ODS:EDW.USE_TRANSACTION",
        "ODS:EDW.CARD_DIMENSION",
        "ODS:EDW.MEDIA_TYPE_DIMENSION",
        "ODS:EDW.OPERATOR_DIMENSION",
        "ODS:EDW.PATRON_TRIP",
        "ODS:EDW.PAYMENT_TYPE_DIMENSION",
        "ODS:EDW.READ_TRANSACTION",
        "ODS:EDW.SALE_TXN_PAYMENT",
        "delta_ODS:EDW.TRIP_PAYMENT",
        "ODS:EDW.TXN_CHANNEL_MAP",
    ),
    "comp_b_farerev_payg_trip_txn_c": (
        "ODS:EDW.FARE_REVENUE_REPORT_SCHEDULE",
        "delta_ODS:EDW.SALE_TRANSACTION",
        "delta_ODS:EDW.USE_TRANSACTION",
        "ODS:EDW.CARD_DIMENSION",
        "ODS:EDW.MEDIA_TYPE_DIMENSION",
        "ODS:EDW.OPERATOR_DIMENSION",
        "ODS:EDW.PATRON_TRIP",
        "ODS:EDW.PAYMENT_TYPE_DIMENSION",
        "ODS:EDW.READ_TRANSACTION",
        "ODS:EDW.SALE_TXN_PAYMENT",
        "delta_ODS:EDW.TRIP_PAYMENT",
        "ODS:EDW.TXN_CHANNEL_MAP",
    ),
    "comp_a_addendum_farerev_prod_sales_txn_a": (
        "delta_ODS:EDW.SALE_TRANSACTION",
        "ODS:EDW.CUSTOMER_DIMENSION",
        "ODS:EDW.DATE_DIMENSION",
        "ODS:EDW.DEVICE_DIMENSION",
        "ODS:EDW.FARE_PRODUCT_DIMENSION",
        "ODS:EDW.FARE_REVENUE_REPORT_SCHEDULE",
        "ODS:EDW.PATRON_ORDER",
        "ODS:EDW.PATRON_ORDER_LINE_ITEM",
        "ODS:EDW.PATRON_ORDER_PAYMENT",
        "ODS:EDW.PAYMENT_TYPE_DIMENSION",
        "ODS:EDW.REASON_DIMENSION",
        "ODS:EDW.SALE_TXN_PAYMENT",
        "ODS:EDW.TXN_CHANNEL_MAP",
    ),
    "comp_a_farerev_prod_sales_txn_c": (
        "delta_ODS:EDW.SALE_TRANSACTION",
        "ODS:EDW.CUSTOMER_DIMENSION",
        "ODS:EDW.DATE_DIMENSION",
        "ODS:EDW.DEVICE_DIMENSION",
        "ODS:EDW.FARE_PRODUCT_DIMENSION",
        "ODS:EDW.FARE_REVENUE_REPORT_SCHEDULE",
        "ODS:EDW.PATRON_ORDER",
        "ODS:EDW.PATRON_ORDER_LINE_ITEM",
        "ODS:EDW.PATRON_ORDER_PAYMENT",
        "ODS:EDW.PAYMENT_TYPE_DIMENSION",
        "ODS:EDW.REASON_DIMENSION",
        "ODS:EDW.SALE_TXN_PAYMENT",
        "ODS:EDW.TXN_CHANNEL_MAP",
    ),
    "patron_order_details_wo110": (
        "ODS:EDW.BUSINESS_ENTITY_DIMENSION",
        "ODS:EDW.CARD_DIMENSION",
        "ODS:EDW.CONTACT_DIMENSION",
        "ODS:EDW.EMPLOYEE_DIMENSION",
        "ODS:EDW.FARE_PROD_USERS_LIST_DIMENSION",
        "ODS:EDW.FARE_PRODUCT_DIMENSION",
        "ODS:EDW.FEE_TYPE_DIMENSION",
        "ODS:EDW.OPERATOR_DIMENSION",
        "ODS:EDW.PATRON_ORDER",
        "ODS:EDW.PATRON_ORDER_LINE_ITEM",
        "ODS:EDW.PATRON_ORDER_PAYMENT",
        "ODS:EDW.PATRON_ORDER_STATUS_DIMENSION",
        "ODS:EDW.PATRON_ORDER_TYPE_DIMENSION",
        "ODS:EDW.PAYMENT_TYPE_DIMENSION",
        "ODS:EDW.PURSE_TYPE_DIMENSION",
        "ODS:EDW.REASON_DIMENSION",
        "ODS:EDW.RIDER_CLASS_DIMENSION",
        "ODS:EDW.TRANSIT_ACCOUNT_DIMENSION",
    ),
}

ENGINEERING_TITLE = "Fares table status"
STAKEHOLDER_TITLE = "Fares data freshness"


class Finding(NamedTuple):
    """
    A finding is a failed check on one table, with its display text decided by the check itself.

    `seconds` is defined as 'time since run' for STALE, 'backlog' for BEHIND_SOURCE,
    'age of most recent data' for DATA_OUT_OF_TIME_RANGE (negative when the data is ahead of now)
    """

    check: str
    detail: str  # the displayed explanatory text, e.g. "26h; hit batch limit"
    seconds: Optional[float] = None


# {group: {table: [Finding, ...]}}; an empty list means the table is OK.
Findings = dict[str, dict[str, list[Finding]]]


# Cap on concurrent status downloads; the shared boto3 client is thread-safe and its
# connection pool is sized well above this.
MAX_FETCH_WORKERS = 16

# Webhook URL == credential for posting to Slack
# Ensure it never is passed in plain text, or makes it to error messages
SLACK_FULL_REPORT_WEBHOOK = os.environ.get("SLACK_FULL_REPORT_WEBHOOK", "").strip()
SLACK_STAKEHOLDER_WEBHOOK = os.environ.get("SLACK_STAKEHOLDER_WEBHOOK", "").strip()

# Slack `text` field accepts max 40,000 characters, truncate to fit if
# necessary.
# Not expecting to see this limit hit: --detailed view as of 2026-08-12 is
# displaying about 20k characters, and most status reports will be ~1k
SLACK_TEXT_LIMIT = 40000
TRUNCATION_NOTICE = "[truncated: report exceeded Slack's 40,000 character limit]"

# Literal emoji rather than Slack ":emoji:" strings, so that it formats the
# same in the console output
CHECK_EMOJI = {OK: "🟢", STALE: "🟠", BEHIND_SOURCE: "🟡", DATA_OUT_OF_TIME_RANGE: "🔵"}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(value: Any) -> Optional[datetime]:
    """Parse an ISO-8601 timestamp from a payload, tolerating None/malformed."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _fmt_duration(seconds: Optional[float]) -> str:
    """Human-readable duration, e.g. 3m / 7.1h / 26.6d / -32.0h, or '?' when unknown."""
    if seconds is None:
        return "?"
    sign, seconds = ("-", -seconds) if seconds < 0 else ("", seconds)
    if seconds < 90:
        return f"{sign}{seconds:.0f}s"
    if seconds < 90 * 60:
        return f"{sign}{seconds / 60:.0f}m"
    if seconds < 48 * 3600:
        return f"{sign}{seconds / 3600:.1f}h"
    return f"{sign}{seconds / 86400:.1f}d"


def _fmt_count(value: Any) -> str:
    """Thousands-separated int, or the value as-is when not an int."""
    return f"{value:,}" if isinstance(value, int) else str(value)


class Line(NamedTuple):
    """
    One row of the report, held as data so it can be rendered for Slack or console.

    If a Line doesn't have text or note, it renders as a blank separator.
    """

    text: str
    note: str = ""
    depth: int = 0  # nesting depth
    bold: bool = False  # 0-depth rows are always bolded


def render(lines: list[Line]) -> str:
    """Render report rows, identically for the terminal and for Slack"""
    out = []
    for line in lines:
        if not line.text and not line.note:
            out.append("")
            continue
        bold = (line.depth == 0 or line.bold) and line.text
        label = f"*{line.text}*" if bold else line.text
        note = f": {line.note}" if line.note else ""
        out.append(f"{'    ' * line.depth}{label}{note}")
    return "\n".join(out)


def check_label(check: str) -> str:
    """Add emoji to check"""
    return f"{CHECK_EMOJI[check]} `{check}`"


def member_key(group: str, table: str) -> str:
    """Build the "GROUP:table" key VIEWS members and per-table settings are written in."""
    return f"{group}:{table}"


def split_member(member: str) -> tuple[str, str]:
    """Split "GROUP:table" back apart; table names may themselves contain no colon."""
    group, _, table = member.partition(":")
    return group, table


def has_check(findings: list[Finding] | tuple[Finding, ...], check: str) -> bool:
    """Whether `findings` include a failure of `check`."""
    return any(finding.check == check for finding in findings)


def fetch_group(prefix: str, tmpdir: str, only: Optional[set[str]] = None) -> dict[str, dict]:
    """Download status objects under `prefix`; return {table: payload}"""
    objects = list_objects(f"{DATA_SPRINGBOARD}/{prefix}/", in_filter=".json")
    wanted = [(os.path.basename(obj.path)[: -len(".json")], obj.path) for obj in objects]
    if only is not None:
        wanted = [(table, path) for table, path in wanted if table in only]
    if not wanted:
        return {}

    def fetch_one(item: tuple[str, str]) -> tuple[str, dict]:
        table, remote_path = item
        local_path = os.path.join(tmpdir, f"{prefix.replace('/', '_')}__{table}.json")
        try:
            if not download_object(remote_path, local_path):
                raise RuntimeError("download failed")
            with open(local_path) as status_file:
                payload = json.load(status_file)
            if not isinstance(payload, dict):
                raise ValueError("status object is not a JSON object")
            return table, payload
        except Exception as exception:  # noqa: BLE001 - report, do not abort the whole run
            return table, {"_error": str(exception)}

    with ThreadPoolExecutor(max_workers=min(MAX_FETCH_WORKERS, len(wanted))) as pool:
        return dict(pool.map(fetch_one, wanted))


def expected_tables(group: str) -> list[str]:
    """Best-effort import of a group's configured table list; [] if unavailable."""
    config = GROUPS[group]
    try:
        module = __import__(config.module, fromlist=[config.attr])
        configured = getattr(module, config.attr)
        if config.manifest_index is None:
            return list(configured)
        # TABLE_MANIFEST maps table -> per-job instance assignment, None where that job
        # does not run for the table. Taking every non-None entry covers all instances.
        index = getattr(module, config.manifest_index)
        return [table for table, jobs in configured.items() if jobs[index] is not None]
    except Exception:  # noqa: BLE001 - the list is a nicety, not a requirement
        return []


def config_errors() -> list[str]:
    """Report errors if VIEWS constant configured incorrectly (bad group or no table)"""
    errors = []
    for view, members in VIEWS.items():
        for member in members:
            group, table = split_member(member)
            if group not in GROUPS:
                errors.append(f"view {view!r}: {member!r} names unknown group {group!r}")
            elif not table:
                errors.append(f"view {view!r}: {member!r} format must be 'group:table'")
    return errors


def column_age_seconds(payload: dict, column: str, now: datetime) -> Optional[float]:
    """
    How far the newest value in `column` is behind now, or None if not published.

    Negative when the value is ahead of now. Values with no time zone are read as UTC.
    """
    column_maxes = payload.get("max_column_timestamps")
    if not isinstance(column_maxes, dict):
        return None
    newest = _parse_iso(column_maxes.get(column))
    return None if newest is None else (now - newest).total_seconds()


def summarize_view(name: str, findings_by_group: Findings) -> dict[str, list[Finding]]:
    """
    Collect each member's findings for one view, as {"GROUP:table": findings}, most first.

    Members missing from `findings_by_group` never published status objects, so are STALE
    """
    members = {
        member: findings_by_group.get(group, {}).get(
            table, [Finding(STALE, "no valid run recorded")]
        )
        for member in VIEWS[name]
        for group, table in [split_member(member)]
    }
    return dict(sorted(members.items(), key=lambda item: (-len(item[1]), item[0])))


def source_reason(payload: dict, lag_seconds: float) -> Optional[str]:
    """
    Add reasons why table is behind source data, if any.

    Uses whichever signals the group publishes -- never clock_lag, which is large for a
    quiet-but-caught-up table.
    """
    pending = payload.get("cdc_records_pending")
    if isinstance(pending, int) and pending == 0:
        return None
    seq_lag = payload.get("seq_lag_seconds")
    seq_lag = float(seq_lag) if isinstance(seq_lag, (int, float)) else None
    reasons = []
    jobs_lag = payload.get("jobs_lag")  # AFC
    if isinstance(jobs_lag, int) and jobs_lag > 0:
        floor = "+" if payload.get("lag_truncated") is True else ""
        reasons.append(f"{jobs_lag}{floor} jobs pending")
    if (
        payload.get("cdc_budget_nearly_full") is True  # ODS fact, records remain
        or payload.get("caught_up") is False  # masabi
        # delta silver: the merge hit its row limit, and seq_lag shows more remains. Its
        # watermark is committed per merge, so trailing deletes do advance it.
        or (payload.get("merge_budget_full") is True and (seq_lag is None or seq_lag > 0))
    ):
        reasons.append("hit batch limit")
    # Secondary: a true backlog (seq lag, not clock lag) beyond the threshold.
    if seq_lag is not None and seq_lag > lag_seconds:
        reasons.append(f"backlog over {_fmt_duration(lag_seconds)}")
    return "; ".join(reasons) or None


def backlog_seconds(payload: dict, now: datetime) -> Optional[float]:
    """
    Return the best duration each group publishes for its backlog, or None.

    This is `seq_lag` for ODS and delta_ODS, how long it has been behind for AFC,
    and clock lag for masabi.
    """
    seq_lag = payload.get("seq_lag_seconds")
    if isinstance(seq_lag, (int, float)):
        return float(seq_lag)
    pending_since = _parse_iso(payload.get("pending_since"))
    if pending_since is not None:
        return (now - pending_since).total_seconds()
    clock_lag = payload.get("clock_lag_seconds")
    return float(clock_lag) if isinstance(clock_lag, (int, float)) else None


def check_stale(payload: dict, now: datetime, stale_seconds: float, note: str) -> Optional[Finding]:
    """STALE when the job has not run within `stale_seconds`, or we cannot tell when it did."""
    last_run = None if "_error" in payload else _parse_iso(payload.get("last_run"))
    if last_run is None:
        # No object, an unreadable one, or no last_run in it; --table says which.
        return Finding(STALE, "no valid run recorded")
    age = (now - last_run).total_seconds()
    return Finding(STALE, f"{_fmt_duration(age)}{note}", age) if age > stale_seconds else None


def check_source(
    payload: dict, now: datetime, lag_seconds: float, stale_age: Optional[float]
) -> Optional[Finding]:
    """
    BEHIND_SOURCE when the last payload says more recent data was available upstream.

    `stale_age` is set when the table is also STALE, since the backlog is then only known
    as of that old run, not now.
    """
    reason = source_reason(payload, lag_seconds)
    if reason is None:
        return None
    backlog = backlog_seconds(payload, now)
    detail = _fmt_duration(backlog)
    if stale_age is not None:
        detail += f" as of last run {_fmt_duration(stale_age)} ago"
    return Finding(BEHIND_SOURCE, f"{detail}; {reason}", backlog)


def check_time_range(group: str, table: str, payload: dict, now: datetime) -> Optional[Finding]:
    """DATA_OUT_OF_TIME_RANGE when a table's specified column is outside its TimeRange."""
    time_range = TIME_RANGES.get(member_key(group, table))
    if time_range is None:
        return None
    age = column_age_seconds(payload, time_range.column, now)
    if age is None or age > time_range.max_behind_seconds or -age > time_range.max_ahead_seconds:
        shown = "unknown" if age is None else _fmt_duration(age)
        return Finding(DATA_OUT_OF_TIME_RANGE, f"{shown}, based on `{time_range.column}`", age)
    return None


def check_table(
    group: str,
    table: str,
    payload: dict,
    now: datetime,
    default_stale_seconds: float,
    default_source_lag_seconds: float,
) -> list[Finding]:
    """
    Run every check on one table's payload; an empty list means OK.

    A table's own thresholds (STALE_SECONDS_OVERRIDES, SOURCE_LAG_SECONDS_OVERRIDES)
    replace the defaults, if available.
    """
    key = member_key(group, table)
    stale_override = STALE_SECONDS_OVERRIDES.get(key)
    stale_seconds = default_stale_seconds if stale_override is None else stale_override
    stale_note = "" if stale_override is None else f", threshold {_fmt_duration(stale_override)}"
    lag_seconds = SOURCE_LAG_SECONDS_OVERRIDES.get(key, default_source_lag_seconds)

    stale = check_stale(payload, now, stale_seconds, stale_note)
    findings = [
        stale,
        check_source(payload, now, lag_seconds, None if stale is None else stale.seconds),
        check_time_range(group, table, payload, now),
    ]
    return [finding for finding in findings if finding is not None]


def finding_text(finding: Finding, mbta_issue: bool = False) -> str:
    """
    Render one finding, e.g. "🟡 `BEHIND_SOURCE` (28h; hit batch limit)".

    `mbta_issue` marks a DATA_OUT_OF_TIME_RANGE finding on a table that is also STALE or
    BEHIND_SOURCE, for readers who will not see those findings themselves.
    """
    suffix = "; MBTA issue" if mbta_issue else ""
    return f"{check_label(finding.check)} ({finding.detail}{suffix})"


def _ok_note(payload: dict, now: datetime) -> str:
    """One-line key-info summary for a healthy (OK) table, shown in --detailed mode."""
    parts = []
    last_run = _parse_iso(payload.get("last_run"))
    if last_run is not None:
        parts.append(f"ran {_fmt_duration((now - last_run).total_seconds())} ago")
    if isinstance(payload.get("seq_lag_seconds"), (int, float)):
        parts.append(f"backlog {_fmt_duration(payload['seq_lag_seconds'])}")
    if isinstance(payload.get("jobs_lag"), int):
        parts.append(f"backlog {payload['jobs_lag']} jobs")
    if "row_count" in payload:
        parts.append(f"{_fmt_count(payload.get('row_count'))} rows")
    if "data_days_per_processing_hour" in payload:
        parts.append(f"{payload['data_days_per_processing_hour']} data-days/proc-hr")
    elif "rows_per_second" in payload:
        parts.append(f"{payload['rows_per_second']} rows/s")
    return f"{CHECK_EMOJI[OK]} OK: {', '.join(parts)}"


def table_note(payload: dict, findings: list[Finding], now: datetime) -> str:
    """Engineering line for one table: every finding, or the OK summary when none."""
    if not findings:
        return _ok_note(payload, now)
    return ", ".join(finding_text(finding) for finding in findings)


def print_table_detail(
    group: str,
    table: str,
    payload: dict,
    now: datetime,
    findings: list[Finding],
) -> None:
    """Print the full single-table summary."""
    key = member_key(group, table)
    lag_override_seconds = SOURCE_LAG_SECONDS_OVERRIDES.get(key)
    stale_override_seconds = STALE_SECONDS_OVERRIDES.get(key)
    time_range = TIME_RANGES.get(key)
    print(f"{table}  ({group})")
    if findings:
        print("  findings:")
        for finding in findings:
            print(f"    {finding_text(finding)}")
    else:
        print(f"  findings:     {CHECK_EMOJI[OK]} OK")
    if lag_override_seconds is not None:
        bar = _fmt_duration(lag_override_seconds)
        print(f"  threshold: backlog over {bar} counts as behind source (per-table)")
    if stale_override_seconds is not None:
        bar = _fmt_duration(stale_override_seconds)
        print(f"  threshold: no run in {bar} counts as stale (per-table)")
    if time_range is not None:
        behind = _fmt_duration(time_range.max_behind_seconds)
        ahead = _fmt_duration(time_range.max_ahead_seconds)
        print(
            f"  time range: `{time_range.column}` within {behind} behind / {ahead} ahead "
            "of now (stakeholder report)"
        )
    views = [name for name, members in VIEWS.items() if key in members]
    if views:
        print(f"  views:        {', '.join(views)}")
    if "_error" in payload:
        print(f"  problem:      {payload['_error']}")
        return
    if not payload:
        print("  problem:      no status object published")
        return

    last_run = _parse_iso(payload.get("last_run"))
    if last_run is not None:
        ago = _fmt_duration((now - last_run).total_seconds())
        print(f"  last run:     {payload['last_run']}  ({ago} ago)")
    if "next_run_seconds" in payload:
        print(f"  next run in:  {_fmt_duration(payload.get('next_run_seconds'))}")
    if "row_count" in payload:
        print(f"  rows:         {_fmt_count(payload.get('row_count'))}")

    # Two lags, kept distinct: clock lag is data age (theirs), seq lag is backlog (ours).
    if isinstance(payload.get("clock_lag_seconds"), (int, float)):
        print(f"  data age:     {_fmt_duration(payload['clock_lag_seconds'])} (clock_lag)")
    if "api_latest_job_first_seen" in payload:
        print(f"  newest job:   {payload.get('api_latest_job_id')}")
        print(f"    first seen: {payload['api_latest_job_first_seen']}")
    if payload.get("pending_since"):
        since = _parse_iso(payload["pending_since"])
        behind_for = "" if since is None else f"  ({_fmt_duration((now - since).total_seconds())})"
        print(f"  behind since: {payload['pending_since']}{behind_for}")
    if payload.get("table_frequency"):
        print(f"  vendor says:  updates {payload['table_frequency']}")
    if isinstance(payload.get("seq_lag_seconds"), (int, float)):
        print(
            f"  backlog:      {_fmt_duration(payload['seq_lag_seconds'])} behind source (seq_lag)"
        )
    if isinstance(payload.get("jobs_lag"), int):
        trunc = "  (truncated)" if payload.get("lag_truncated") else ""
        rows = _fmt_count(payload.get("rows_lag"))
        print(f"  backlog:      {payload['jobs_lag']} jobs / {rows} rows{trunc}")

    # Throughput and catch-up, when the run had a predecessor to difference against.
    if "data_days_per_processing_hour" in payload:
        ratio = payload.get("catchup_ratio")
        gain = f";  gaining {ratio}x on source" if isinstance(ratio, (int, float)) else ""
        dph = payload["data_days_per_processing_hour"]
        print(f"  throughput:   {dph} data-days / processing-hour{gain}")
    if "rows_per_second" in payload:
        print(f"  row rate:     {payload['rows_per_second']} rows/s")
    catchup_proc = payload.get("catchup_processing_seconds")
    if isinstance(catchup_proc, (int, float)):
        if catchup_proc < 1:
            print("  catch up in:  caught up (no backlog)")
        else:
            wall = payload.get("catchup_wall_seconds")
            wall_txt = (
                f" / ~{_fmt_duration(wall)} wall"
                if isinstance(wall, (int, float))
                else " / never on this schedule (losing ground)"
            )
            proj = payload.get("projected_caught_up")
            proj_txt = f";  projected {proj}" if proj else ""
            proc = _fmt_duration(catchup_proc)
            print(f"  catch up in:  ~{proc} processing{wall_txt}{proj_txt}")

    # Group-specific keep-up flags, spelled out.
    for flag in ("cdc_budget_nearly_full", "cdc_records_pending", "merge_budget_full", "caught_up"):
        if flag in payload:
            print(f"  {flag}: {payload[flag]}")
    if payload.get("lag_truncated") is True:
        print("  note:         backlog is a floor; the API's count limit was hit")
    if isinstance(payload.get("regression_count"), int) and payload["regression_count"] > 0:
        print(f"  note:         {payload['regression_count']} parquet regression(s) last run")

    # Every timestamp column's newest value, used to determine DATA_OUT_OF_TIME_RANGE
    column_maxes = payload.get("max_column_timestamps")
    if isinstance(column_maxes, dict) and column_maxes:
        print("  max_column_timestamps:")
        for column in sorted(column_maxes):
            print(f"    {column}: {column_maxes[column]}")


def _check_counts(counts: dict[str, int]) -> list[str]:
    """'2 STALE', '1 BEHIND_SOURCE', etc. for every check with a nonzero count."""
    return [f"{counts[check]} {check}" for check in CHECKS if counts.get(check)]


def view_lines(views: dict[str, dict[str, list[Finding]]], detailed: bool = False) -> list[Line]:
    """
    Build the view block, comprising a row per view with problem members nested

    `views` is {view name: summarize_view(...)}. Member tables that are OK are only
    displayed if `detailed` is True.
    """
    lines = [Line("Views")]
    for name, members in views.items():
        total = len(members)
        tables = "table" if total == 1 else "tables"
        if not any(members.values()):
            note = f"{CHECK_EMOJI[OK]} all {total} {tables} up to date"
        else:
            counts = {
                check: sum(1 for findings in members.values() if has_check(findings, check))
                for check in CHECKS
            }
            note = f"{', '.join(_check_counts(counts))} of {total} {tables}"
        lines.append(Line(name, note=note, depth=1))
        for member, findings in members.items():
            if not findings and not detailed:
                continue
            labels = [check_label(finding.check) for finding in findings]
            note = ", ".join(labels) if labels else f"{CHECK_EMOJI[OK]} OK"
            lines.append(Line(member, note=note, depth=2))
    return lines


def fetch_all(groups: list[str], tmpdir: str) -> dict[str, dict[str, dict]]:
    """
    Download every group's status objects up front; return {group: {table: payload}}.

    Views span groups, so their rollup cannot be printed until every group has been read.
    """
    payloads_by_group = {}
    for group in groups:
        payloads = fetch_group(GROUPS[group].prefix, tmpdir)
        # Flag expected and stakeholder-monitored tables that have never published an
        # object, so that absent tables are still reported.
        monitored = [table for g, table in map(split_member, TIME_RANGES) if g == group]
        for table in [*expected_tables(group), *monitored]:
            payloads.setdefault(table, {})
        payloads_by_group[group] = payloads
    return payloads_by_group


def evaluate(
    groups: list[str],
    now: datetime,
    default_stale_seconds: float,
    default_source_lag_seconds: float,
) -> tuple[dict[str, dict[str, dict]], Findings]:
    """
    Read every group once and run every check on every table.

    Returns ({group: {table: payload}}, {group: {table: findings}}), shared by both reports.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        payloads_by_group = fetch_all(groups, tmpdir)
    findings_by_group = {
        group: {
            table: check_table(
                group, table, payload, now, default_stale_seconds, default_source_lag_seconds
            )
            for table, payload in payloads.items()
        }
        for group, payloads in payloads_by_group.items()
    }
    return payloads_by_group, findings_by_group


def engineering_report(
    payloads_by_group: dict[str, dict[str, dict]],
    findings_by_group: Findings,
    now: datetime,
    default_stale_seconds: float,
    default_source_lag_seconds: float,
    detailed: bool = False,
) -> tuple[str, list[Line], int]:
    """
    Build the engineering report; return (state for the lead line, lines, problem count).

    The problem count is every table with any finding.

    When `detailed`, every table gets a per-table line (OK tables included, with a
    key-info summary); otherwise OK tables are not listed.
    """
    key = {
        STALE: (
            f"no successful update within {_fmt_duration(default_stale_seconds)} (or the "
            "threshold named on the line); measured from the last run"
        ),
        BEHIND_SOURCE: (
            "more recent data was available upstream than the last run consumed (backlog "
            f"over {_fmt_duration(default_source_lag_seconds)}, or the job's own keep-up "
            "signal)"
        ),
        DATA_OUT_OF_TIME_RANGE: (
            "the specified column's newest value is further behind (or, when negative, ahead "
            "of) now, based on column's threshold"
        ),
    }
    lines = [Line("Key:")]
    lines += [Line(check_label(check), note=note, depth=1) for check, note in key.items()]
    lines.append(Line(""))

    totals = dict.fromkeys(CHECKS, 0)
    tables_with_findings = 0
    for group, findings in findings_by_group.items():
        payloads = payloads_by_group[group]
        group_counts = {
            check: sum(1 for found in findings.values() if has_check(found, check))
            for check in CHECKS
        }
        n_problem_tables = sum(1 for found in findings.values() if found)
        tables_with_findings += n_problem_tables
        for check, n in group_counts.items():
            totals[check] += n

        counts = ", ".join(
            [f"{len(findings)} tables: {len(findings) - n_problem_tables} ok"]
            + _check_counts(group_counts)
        )

        lines.append(Line(group, note=counts))

        # Most findings first; no check outranks another.
        for table in sorted(findings, key=lambda t: (-len(findings[t]), t)):
            if not findings[table] and not detailed:
                continue
            note = table_note(payloads[table], findings[table], now)
            lines.append(Line(table, note=note, depth=1, bold=True))
        lines.append(Line(""))

    lines.append(
        Line(
            f"Summary: {totals[STALE]} stale, {totals[BEHIND_SOURCE]} behind source, "
            f"{totals[DATA_OUT_OF_TIME_RANGE]} out of time range across "
            f"{sum(len(findings) for findings in findings_by_group.values())} tables."
        )
    )
    lines.append(Line(""))

    if VIEWS:
        views = {name: summarize_view(name, findings_by_group) for name in VIEWS}
        lines.extend(view_lines(views, detailed))

    if tables_with_findings:
        state = f"⚠️ {tables_with_findings} problem(s) needing action"
    else:
        state = "✅ all tables OK"
    return state, lines, tables_with_findings


def stakeholder_report(
    payloads_by_group: dict[str, dict[str, dict]], findings_by_group: Findings, now: datetime
) -> tuple[str, list[Line], int]:
    """
    Build the stakeholder report.

    Only tables in TIME_RANGES are included, and only based on their DATA_OUT_OF_TIME_RANGE
    finding. If there are other findings (e.g. STALE), "MBTA issue" is appended.
    """
    lines = [
        Line(
            "Key",
            note=(
                f"{check_label(DATA_OUT_OF_TIME_RANGE)}: the newest value in the named "
                "column is further behind (or, when negative, ahead of) now than agreed. "
                '"MBTA issue" means our own pipeline is the cause.'
            ),
        ),
        Line(""),
    ]
    out_of_range = 0
    members_by_group: dict[str, list[str]] = {}
    for member in sorted(TIME_RANGES):
        group, table = split_member(member)
        members_by_group.setdefault(group, []).append(table)
    for group, tables in members_by_group.items():
        lines.append(Line(group))
        for table in tables:
            payload = payloads_by_group.get(group, {}).get(table, {})
            findings = findings_by_group.get(group, {}).get(table, [])
            out = [finding for finding in findings if finding.check == DATA_OUT_OF_TIME_RANGE]
            if out:
                out_of_range += 1
                mbta_issue = has_check(findings, STALE) or has_check(findings, BEHIND_SOURCE)
                note = finding_text(out[0], mbta_issue=mbta_issue)
            else:
                column = TIME_RANGES[member_key(group, table)].column
                age = _fmt_duration(column_age_seconds(payload, column, now))
                note = f"{CHECK_EMOJI[OK]} in range ({age}, based on `{column}`)"
            lines.append(Line(table, note=note, depth=1, bold=True))
        lines.append(Line(""))

    monitored = len(TIME_RANGES)
    if monitored == 0:
        state = "no tables are monitored yet"
    elif out_of_range:
        state = f"⚠️ {out_of_range} of {monitored} monitored table(s) out of time range"
    else:
        state = f"✅ all {monitored} monitored table(s) in time range"
    return state, lines, out_of_range


def _fit_to_slack_limit(lines: list[Line], overhead: int) -> str:
    """
    Render `lines`, dropping whole rows off the end until the result fits Slack's cap.

    Dropping rows rather than chopping the string mid-character keeps the tail readable;
    the notice says what happened. Not expected to trigger, see SLACK_TEXT_LIMIT.
    """
    budget = SLACK_TEXT_LIMIT - overhead
    body = render(lines)
    if len(body) <= budget:
        return body

    notice = render([Line(TRUNCATION_NOTICE)])
    kept: list[str] = []
    used = len(notice)
    for row in body.split("\n"):
        if used + len(row) + 1 > budget:
            break
        kept.append(row)
        used += len(row) + 1
    return "\n".join([*kept, notice])


def _slack_post(webhook: str, payload: dict) -> None:
    """
    POST one message to `webhook`; every failure becomes a RuntimeError with no URL in it

    Anything urllib raises (rejection, unreachable host, read timeout, malformed URL)
    is reported the same way, so the caller has one exception type to catch and no failure
    mode can escape as a traceback out of a scheduled job.
    """
    try:
        # Built inside the try: a webhook missing its https:// scheme raises here, not
        # at urlopen.
        request = urllib.request.Request(
            webhook,
            json.dumps(payload).encode("utf-8"),
            {"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8", "replace").strip()
    except urllib.error.HTTPError as error:
        # Slack names the reason in the body: invalid_payload/no_service/no_text
        detail = f"HTTP {error.code} {error.read().decode('utf-8', 'replace').strip()}"
    except Exception as error:  # noqa: BLE001 - report every failure the same way
        detail = f"{type(error).__name__}: {error}"
    else:
        if body == "ok":
            return
        detail = f"unexpected response body {body!r}"

    # Scrub before raising: a webhook stored without its https:// scheme comes back
    # inside ValueError's message, and this repo's CI logs are public.
    raise RuntimeError(f"Could not post to Slack: {detail}".replace(webhook, "<webhook>"))


def build_slack_message(title: str, state: str, lines: list[Line], now: datetime) -> str:
    """
    Put a lead-in on a report and render the rest as Slack markdown.

    The lead doubles as the report's title, so `lines` carries no heading of its own. No
    code fence: rows identify themselves by their circles rather than by column alignment,
    which is what lets emoji, bolding and indentation render throughout.
    """
    lead = f"*{title}* {now.strftime('%Y-%m-%dT%H:%MZ')}: {state}"
    return f"{lead}\n\n{_fit_to_slack_limit(lines, len(lead) + 2)}"


# Seeing no tables at all is most likely a failure to read: list_objects swallows
# AccessDenied/NoSuchBucket and returns an empty list.
NO_DATA_STATE = "🚨 NO DATA. Could not read any status objects"


class Report(NamedTuple):
    """One audience's report, and where it is posted."""

    label: str  # also the --audience choice that prints it
    title: str
    state: str  # the lead line's verdict
    lines: list[Line]
    problems: int  # what --only-if-problems tests
    webhook: str
    env_var: str  # names the webhook in errors, never the URL itself


def main() -> int:
    """Parse args and dispatch to the single-table or overall view."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else None)
    parser.add_argument(
        "--table",
        metavar="GROUP:TABLE",
        help='one table, qualified by its group (e.g. "ODS:EDW.SALE_TRANSACTION")',
    )
    parser.add_argument(
        "--source-lag-seconds",
        type=float,
        default=DEFAULT_SOURCE_LAG_SECONDS,
        help=(
            f"backlog over this many seconds counts as BEHIND_SOURCE (default "
            f"{DEFAULT_SOURCE_LAG_SECONDS}, i.e. {DEFAULT_SOURCE_LAG_SECONDS / 60}m); "
            "tables listed in SOURCE_LAG_SECONDS_OVERRIDES keep their own threshold"
        ),
    )
    parser.add_argument(
        "--stale-seconds",
        type=float,
        default=DEFAULT_STALE_SECONDS,
        help=(
            f"no run within this many seconds counts as stale (default "
            f"{DEFAULT_STALE_SECONDS}, i.e. {DEFAULT_STALE_SECONDS / 3600}h); "
            "tables listed in STALE_SECONDS_OVERRIDES keep their own threshold"
        ),
    )
    parser.add_argument("--json", action="store_true", help="print raw payload(s), unformatted")
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="in the engineering report, print a key-info line for every table, OK ones included",
    )
    parser.add_argument(
        "--audience",
        choices=("engineering", "stakeholder"),
        default="engineering",
        help="which report to print to the console (--slack always sends both)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="keep Odin's per-S3-call INFO logging (suppressed by default)",
    )
    parser.add_argument(
        "--slack",
        action="store_true",
        help=(
            "post both reports to Slack rather than printing them: full report to "
            "$SLACK_FULL_REPORT_WEBHOOK, stakeholder to $SLACK_STAKEHOLDER_WEBHOOK. Exit status"
            "here reports whether the reports were delivered, not whether the tables are healthy"
        ),
    )
    parser.add_argument(
        "--slack-test",
        action="store_true",
        help=(
            "render both Slack messages and print them here instead of posting; doesn't "
            "use webhooks or contact Slack"
        ),
    )
    parser.add_argument(
        "--only-if-problems",
        action="store_true",
        help="skip any report with nothing to report (a failed read is always reported)",
    )
    args = parser.parse_args()

    slack_mode = args.slack or args.slack_test

    if slack_mode and (args.json or args.table):
        parser.error("--slack applies to the overall report, not to --json or --table")

    # Report errors and exit if VIEWS or TIME_RANGES are configured incorrectly
    errors = config_errors()
    if errors:
        for error in errors:
            print(f"config error: {error}", file=sys.stderr)
        return 2

    # The S3 helpers log an INFO line per list/download via Odin's shared logger, which
    # would bury this summary. Quiet that logger unless --verbose is asked for; leave the
    # root logger alone so genuine warnings/errors still surface.
    if not args.verbose:
        logging.getLogger(LOGGER_NAME).setLevel(logging.WARNING)

    now = _utc_now()
    default_source_lag_seconds = args.source_lag_seconds
    default_stale_seconds = args.stale_seconds

    # Single table: fetch just that table's object, not the whole group.
    if args.table:
        group, table = split_member(args.table)
        if group not in GROUPS:
            parser.error(f"--table must be qualified GROUP:TABLE, with GROUP one of {list(GROUPS)}")
        with tempfile.TemporaryDirectory() as tmpdir:
            payloads = fetch_group(GROUPS[group].prefix, tmpdir, only={table})
        payload = payloads.get(table, {})
        if args.json:
            print(json.dumps(payload, indent=2))
            return 0
        findings = check_table(
            group, table, payload, now, default_stale_seconds, default_source_lag_seconds
        )
        print_table_detail(group, table, payload, now, findings)
        return 1 if findings else 0

    groups = list(GROUPS)

    if args.json:
        out: dict[str, dict] = {}
        with tempfile.TemporaryDirectory() as tmpdir:
            for group in groups:
                out[group] = fetch_group(GROUPS[group].prefix, tmpdir)
        print(json.dumps(out, indent=2))
        return 0

    payloads_by_group, findings_by_group = evaluate(
        groups, now, default_stale_seconds, default_source_lag_seconds
    )
    total_tables = sum(len(payloads) for payloads in payloads_by_group.values())
    reports = [
        Report(
            "engineering",
            ENGINEERING_TITLE,
            *engineering_report(
                payloads_by_group,
                findings_by_group,
                now,
                default_stale_seconds,
                default_source_lag_seconds,
                args.detailed,
            ),
            SLACK_FULL_REPORT_WEBHOOK,
            "SLACK_FULL_REPORT_WEBHOOK",
        ),
        Report(
            "stakeholder",
            STAKEHOLDER_TITLE,
            *stakeholder_report(payloads_by_group, findings_by_group, now),
            SLACK_STAKEHOLDER_WEBHOOK,
            "SLACK_STAKEHOLDER_WEBHOOK",
        ),
    ]
    if total_tables == 0:
        reports = [report._replace(state=NO_DATA_STATE) for report in reports]
    # A read that turned up nothing is always worth reporting, even under
    # --only-if-problems: silence there would look identical to a healthy day.
    if args.only_if_problems and total_tables > 0:
        reports = [report for report in reports if report.problems]

    if not slack_mode:
        reports = [report for report in reports if report.label == args.audience]
        for report in reports:
            stamp = now.strftime("%Y-%m-%dT%H:%MZ")
            header = [Line(f"{report.title} {stamp}: {report.state}"), Line("")]
            print(render(header + report.lines))
        return 1 if total_tables == 0 or any(report.problems for report in reports) else 0

    # Nothing above prints in Slack mode, so on this public repo's world-readable CI log
    # the summaries reach Slack without ever passing through stdout.
    if args.slack_test:
        for report in reports:
            message = build_slack_message(report.title, report.state, report.lines, now)
            # The message goes to stdout so it can be piped or diffed; the size note goes
            # to stderr so it never pollutes that output. Flush first, or the note (on
            # unbuffered stderr) jumps ahead of the message when stdout is piped.
            print(message, flush=True)
            truncated = TRUNCATION_NOTICE.strip() in message
            note = f"{len(message):,} of {SLACK_TEXT_LIMIT:,} characters"
            print(
                f"\n[--slack-test: {report.label} not posted; {note}"
                f"{'; TRUNCATED' if truncated else ''}]\n",
                file=sys.stderr,
                flush=True,
            )
        return 0

    # Post engineering and stakeholder reports independently
    failed = False
    for report in reports:
        if not report.webhook:
            print(
                f"{report.env_var} is unset; cannot post the {report.label} report.",
                file=sys.stderr,
            )
            failed = True
            continue
        try:
            message = build_slack_message(report.title, report.state, report.lines, now)
            _slack_post(report.webhook, {"text": message})
        except RuntimeError as error:
            print(f"{report.label}: {error}", file=sys.stderr)
            failed = True
    # No tables indicates a problem regardless
    return 1 if failed or total_tables == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
