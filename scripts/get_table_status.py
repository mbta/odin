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

Each table is classified into one of three states:

  * STALE: no object, or no run within --stale-seconds (default 24h). Jobs are
    serialized rather than run in parallel, so one flat threshold is more meaningful than
    each job's own cadence. A stale object means the job is not finishing -- which is
    itself the signal worth having.
  * BEHIND: running recently, but not caught up: the job's own keep-up flag says so
    (cdc_budget_nearly_full / merge_budget_full / caught_up false /
    jobs_lag > 0), or the true backlog (seq_lag_seconds) exceeds --lag-seconds.
  * OK: ran recently and caught up.

Rarely-updated tables (like the DIMENSION tables) have a large clock_lag_seconds
(its newest data is old), but a seq_lag_seconds of ~0 (it has consumed everything
upstream). Classification uses the backlog signals, never the clock lag, so these tables
read as OK as long as they are caught up to history.

Views are hand-maintained in a constant, and include all required tables for each view,
which can span across groups. A view is up to date if ALL of its tables are up to date,
and otherwise matches its worst member's status.

Usage:
    python scripts/get_table_status.py                      # overall summary (hides OK tables)
    python scripts/get_table_status.py --detailed           # include info for OK tables
    python scripts/get_table_status.py --table ODS:EDW.SALE_TRANSACTION  # one table's details
    python scripts/get_table_status.py --slack              # post to Slack using $SLACK_WEBHOOK
    python scripts/get_table_status.py --slack-test         # preview Slack message, post nothing
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

OK, BEHIND, STALE = "OK", "BEHIND", "STALE"

# STALE is considered 'worse' than BEHIND when a single status is needed
RANK = {STALE: 0, BEHIND: 1, OK: 2}

DEFAULT_STALE_SECONDS = 24 * 3600
DEFAULT_LAG_SECONDS = 4 * 3600

# Per-table BEHIND thresholds, in seconds.
# Note: jobs that are fully caught up with source (as judged by
# cdc_budget_nearly_full, merge_budget_full, caught_up, or jobs_lag)
# do not trigger BEHIND regardless of threshold set here.
LAG_SECONDS_OVERRIDES: dict[str, float] = {}

# Per-table STALE thresholds, in seconds.
# High-frequency tables should be marked as STALE after a much shorter delay that the default
STALE_SECONDS_OVERRIDES: dict[str, float] = {
    "ODS:EDW.PATRONAGE_SUMMARY": 1 * 3600,
    "AFC:v_validation_taps": 1 * 3600,
    "AFC:v_sales_txns": 1 * 3600,
    "AFC:v_products": 1 * 3600,
    "AFC:v_svw_balance_changes": 1 * 3600,
    "AFC:v_eventhistory": 1 * 3600,
    "AFC:v_mainshift": 1 * 3600,
    "AFC:v_trips": 1 * 3600,
    "masabi:retail.ticket_purchases": 1 * 3600,
    "masabi:retail.activations": 1 * 3600,
}

# Views take the status of the worst member, according to the RANK constant
# E.g., a view with one BEHIND table is BEHIND, unless it has a STALE table
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
        "ODS:EDW.TRIP_PAYMENT",
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
        "ODS:EDW.TRIP_PAYMENT",
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


class ViewSummary(NamedTuple):
    """One view's members, ranked worst-first; the view's own state is the worst of them."""

    name: str
    members: tuple[tuple[str, str], ...]  # (state, "GROUP:table"), worst first

    @property
    def state(self) -> str:
        """The rolled-up state: the worst member's, since members are sorted worst-first."""
        return self.members[0][0] if self.members else OK

    @property
    def total(self) -> int:
        """How many tables the view is built over."""
        return len(self.members)

    def count(self, state: str) -> int:
        """How many of the view's members are in `state`."""
        return sum(1 for member_state, _ in self.members if member_state == state)


# Cap on concurrent status downloads; the shared boto3 client is thread-safe and its
# connection pool is sized well above this.
MAX_FETCH_WORKERS = 16

# Webhook URL == credential for posting to Slack
# Ensure it never is passed in plain text, or makes it to error messages
SLACK_WEBHOOK = os.environ.get("SLACK_WEBHOOK", "").strip()

# Slack `text` field accepts max 40,000 characters, truncate to fit if
# necessary.
# Not expecting to see this limit hit: --detailed view as of 2026-08-12 is
# displaying about 20k characters, and most status reports will be ~1k
SLACK_TEXT_LIMIT = 40000
TRUNCATION_NOTICE = "[truncated: report exceeded Slack's 40,000 character limit]"

# Literal emoji rather than Slack ":emoji:" strings, so that it formats the
# same in the console output
STATE_EMOJI = {OK: "🟢", BEHIND: "🟡", STALE: "🟠"}


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
    """Human-readable duration, e.g. 3m / 7.1h / 26.6d, or '?' when unknown."""
    if seconds is None:
        return "?"
    seconds = abs(seconds)
    if seconds < 90:
        return f"{seconds:.0f}s"
    if seconds < 90 * 60:
        return f"{seconds / 60:.0f}m"
    if seconds < 48 * 3600:
        return f"{seconds / 3600:.1f}h"
    return f"{seconds / 86400:.1f}d"


def _fmt_count(value: Any) -> str:
    """Thousands-separated int, or the value as-is when not an int."""
    return f"{value:,}" if isinstance(value, int) else str(value)


class Line(NamedTuple):
    """
    One row of the report, held as data so it can be rendered for Slack or console.

    If a Line doesn't have text or note, it renders as a blank separator.
    """

    text: str
    state: Optional[str] = None
    note: str = ""
    depth: int = 0  # nesting depth; 0-depth rows are bolded


def render(lines: list[Line]) -> str:
    """Render report rows, identically for the terminal and for Slack"""
    out = []
    for line in lines:
        if not line.text and not line.note:
            out.append("")
            continue
        marker = f"{STATE_EMOJI[line.state]} " if line.state else ""
        label = f"*{line.text}*" if line.depth == 0 and line.text else line.text
        note = f": {line.note}" if line.note else ""
        out.append(f"{'    ' * line.depth}{marker}{label}{note}")
    return "\n".join(out)


def state_note(state: str, note: str = "") -> str:
    """
    Name the state in the note unless it is OK, so the circle is never the only signal.

    Yellow and orange read alike at a glance; the word is what separates BEHIND from STALE.
    """
    if state == OK:
        return note
    return f"{state}: {note}" if note else state


def member_key(group: str, table: str) -> str:
    """Build the "GROUP:table" key VIEWS members are written in."""
    return f"{group}:{table}"


def split_member(member: str) -> tuple[str, str]:
    """Split "GROUP:table" back apart; table names may themselves contain no colon."""
    group, _, table = member.partition(":")
    return group, table


def threshold_seconds(
    overrides: dict[str, float], group: str, table: str, default_seconds: float
) -> float:
    """Get hardcoded threshold from `overrides`, else return `default_seconds`"""
    return overrides.get(member_key(group, table), default_seconds)


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


def summarize_view(name: str, states_by_group: dict[str, dict[str, str]]) -> ViewSummary:
    """
    Give each view the status of the worst member, according to the RANK constant

    Members missing from `states_by_group` never published status objects, so are STALE
    """
    rows = []
    for member in VIEWS[name]:
        group, table = split_member(member)
        rows.append((states_by_group.get(group, {}).get(table, STALE), member))
    rows.sort(key=lambda row: (RANK[row[0]], row[1]))
    return ViewSummary(name=name, members=tuple(rows))


def is_behind(payload: dict, lag_seconds: float) -> bool:
    """
    Report whether the table's own keep-up signals say it has not caught up.

    Uses whichever backlog signals the group publishes -- never clock_lag, which is
    large for a quiet-but-caught-up table.
    """
    # Job-specific "cannot keep up" flags, the authoritative signal.
    if payload.get("cdc_budget_nearly_full") is True:  # ODS fact
        return True
    if payload.get("merge_budget_full") is True:  # delta silver
        return True
    if payload.get("caught_up") is False:  # masabi
        return True
    jobs_lag = payload.get("jobs_lag")  # AFC
    if isinstance(jobs_lag, int) and jobs_lag > 0:
        return True
    pending = payload.get("cdc_records_pending")
    if isinstance(pending, int) and pending == 0 and payload.get("cdc_budget_nearly_full") is False:
        return False
    # Secondary: a true backlog (seq lag, not clock lag) beyond the threshold.
    seq_lag = payload.get("seq_lag_seconds")
    if isinstance(seq_lag, (int, float)) and seq_lag > lag_seconds:
        return True
    return False


def classify(payload: dict, now: datetime, stale_seconds: float, lag_seconds: float) -> str:
    """Return OK / BEHIND / STALE for one table's payload."""
    if "_error" in payload or not payload:
        return STALE
    last_run = _parse_iso(payload.get("last_run"))
    if last_run is None:
        return STALE
    if (now - last_run).total_seconds() > stale_seconds:
        return STALE
    return BEHIND if is_behind(payload, lag_seconds) else OK


def _behind_note(payload: dict, override_seconds: Optional[float] = None) -> str:
    """
    One-line reason a BEHIND table is behind, using whatever the group publishes.

    Uses each table's individual threshold when one exists.
    """
    parts = []
    seq_lag = payload.get("seq_lag_seconds")
    if isinstance(seq_lag, (int, float)):
        parts.append(f"backlog {_fmt_duration(seq_lag)}")
    jobs_lag, rows_lag = payload.get("jobs_lag"), payload.get("rows_lag")
    if isinstance(jobs_lag, int) and jobs_lag > 0:
        parts.append(f"{jobs_lag} jobs / {_fmt_count(rows_lag)} rows pending")
    if payload.get("caught_up") is False:
        parts.append("hit row limit")
    if payload.get("cdc_budget_nearly_full") is True or payload.get("merge_budget_full") is True:
        parts.append("running at full capacity")
    catchup = payload.get("catchup_processing_seconds")
    if isinstance(catchup, (int, float)) and catchup > 0:
        wall = payload.get("catchup_wall_seconds")
        eta = _fmt_duration(wall) if isinstance(wall, (int, float)) else "never (losing ground)"
        parts.append(f"catch up in ~{_fmt_duration(catchup)} proc / {eta} wall")
    reason = ", ".join(parts) if parts else "not caught up"
    if override_seconds is not None:
        reason += f" (threshold {override_seconds / 3600}h)"
    return reason


def _stale_note(payload: dict, now: datetime, override_seconds: Optional[float] = None) -> str:
    """One-line reason a STALE table is stale."""
    if not payload:
        return "no status object published"
    if "_error" in payload:
        return f"unreadable status object: {payload['_error']}"
    last_run = _parse_iso(payload.get("last_run"))
    if last_run is None:
        return "status object has no valid last_run"

    seq_lag = payload.get("seq_lag_seconds")
    if isinstance(seq_lag, (int, float)):
        lag_statement = f", backlog {_fmt_duration(seq_lag)}"
    else:
        lag_statement = ""

    ago = _fmt_duration((now - last_run).total_seconds())
    cadence = _fmt_duration(payload.get("next_run_seconds"))
    threshold = "" if override_seconds is None else f", threshold {override_seconds / 3600}h"
    return f"last run {ago} ago (cadence {cadence}){lag_statement}{threshold}"


def _ok_note(payload: dict, now: datetime) -> str:
    """One-line key-info summary for a healthy (OK) table, shown in --detailed mode."""
    if not payload:
        return "no status object published"
    parts = []
    last_run = _parse_iso(payload.get("last_run"))
    if last_run is not None:
        parts.append(f"ran {_fmt_duration((now - last_run).total_seconds())} ago")
    if isinstance(payload.get("clock_lag_seconds"), (int, float)):
        parts.append(f"age {_fmt_duration(payload['clock_lag_seconds'])}")
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
    return ", ".join(parts) if parts else "caught up"


def note_for(
    state: str,
    payload: dict,
    now: datetime,
    lag_override_seconds: Optional[float] = None,
    stale_override_seconds: Optional[float] = None,
) -> str:
    """Pick the right one-line note for a table in `state` and name the state in it."""
    if state == STALE:
        note = _stale_note(payload, now, stale_override_seconds)
    elif state == BEHIND:
        note = _behind_note(payload, lag_override_seconds)
    else:
        note = _ok_note(payload, now)
    return state_note(state, note)


def print_table_detail(
    group: str,
    table: str,
    payload: dict,
    now: datetime,
    default_stale_seconds: float,
    default_lag_seconds: float,
) -> None:
    """Print the full single-table summary."""
    lag_override_seconds = LAG_SECONDS_OVERRIDES.get(member_key(group, table))
    stale_override_seconds = STALE_SECONDS_OVERRIDES.get(member_key(group, table))
    state = classify(
        payload,
        now,
        threshold_seconds(STALE_SECONDS_OVERRIDES, group, table, default_stale_seconds),
        threshold_seconds(LAG_SECONDS_OVERRIDES, group, table, default_lag_seconds),
    )
    print(f"{table}  ({group})")
    print(f"  state:        {STATE_EMOJI[state]} {state}")
    if lag_override_seconds is not None:
        bar = f"{lag_override_seconds / 3600}h"
        print(f"  threshold: backlog over {bar} counts as behind (per-table)")
    if stale_override_seconds is not None:
        bar = f"{stale_override_seconds / 3600}h"
        print(f"  threshold: no run in {bar} counts as stale (per-table)")
    views = [name for name, members in VIEWS.items() if member_key(group, table) in members]
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

    # Two lags, kept distinct: clock lag is data age, seq lag is the true backlog.
    if isinstance(payload.get("clock_lag_seconds"), (int, float)):
        print(f"  data age:     {_fmt_duration(payload['clock_lag_seconds'])} behind clock")
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
    for flag in ("cdc_budget_nearly_full", "merge_budget_full", "caught_up"):
        if flag in payload:
            print(f"  {flag}: {payload[flag]}")


def _view_detail(summary: ViewSummary) -> str:
    """Build the counts phrase on a view's own line, e.g. 'all 4 tables up to date'."""
    tables = "table" if summary.total == 1 else "tables"
    if summary.state == OK:
        return f"all {summary.total} {tables} up to date"
    parts = []
    if summary.count(BEHIND):
        parts.append(f"{summary.count(BEHIND)} behind")
    if summary.count(STALE):
        parts.append(f"{summary.count(STALE)} STALE")
    return f"{', '.join(parts)} of {summary.total} {tables}"


def view_lines(summaries: list[ViewSummary], detailed: bool = False) -> list[Line]:
    """
    Build the view block, comprising a row per view with problem members nested

    Member tables that are OK are only displayed if `detailed` is True
    """
    lines = [Line("Views")]
    for summary in summaries:
        lines.append(Line(summary.name, state=summary.state, note=_view_detail(summary), depth=1))
        for state, member in summary.members:
            if state == OK and not detailed:
                continue
            lines.append(Line(member, state=state, note=state_note(state), depth=2))
    return lines


def fetch_all(groups: list[str], tmpdir: str) -> dict[str, dict[str, dict]]:
    """
    Download every group's status objects up front; return {group: {table: payload}}.

    Views span groups, so their rollup cannot be printed until every group has been read.
    """
    payloads_by_group = {}
    for group in groups:
        payloads = fetch_group(GROUPS[group].prefix, tmpdir)
        # Flag expected tables that have never published an object.
        for table in expected_tables(group):
            payloads.setdefault(table, {})
        payloads_by_group[group] = payloads
    return payloads_by_group


def build_report(
    groups: list[str],
    now: datetime,
    default_stale_seconds: float,
    default_lag_seconds: float,
    detailed: bool = False,
) -> tuple[list[Line], int, int]:
    """
    Build the whole report as Lines; return (lines, not-OK count, tables seen).

    When `detailed`, every table gets a per-table line (OK tables included, with a
    key-info summary); otherwise OK tables are not listed.
    """
    behind_key = (
        f"latest timestamp older than {default_lag_seconds / 3600} hours, and uningested "
        "data remains from source; tables with their own threshold name it on their line"
    )
    stale_key = (
        f"no successful update within {default_stale_seconds / 3600} hours; "
        "tables with their own threshold name it on their line"
    )
    key = {
        OK: "not stale or behind",
        BEHIND: behind_key,
        STALE: stale_key,
    }
    lines = [Line("Key:")]
    lines += [Line(state, state=state, note=note, depth=1) for state, note in key.items()]
    lines.append(Line(""))

    with tempfile.TemporaryDirectory() as tmpdir:
        payloads_by_group = fetch_all(groups, tmpdir)

    states_by_group = {
        group: {
            table: classify(
                payload,
                now,
                threshold_seconds(STALE_SECONDS_OVERRIDES, group, table, default_stale_seconds),
                threshold_seconds(LAG_SECONDS_OVERRIDES, group, table, default_lag_seconds),
            )
            for table, payload in payloads.items()
        }
        for group, payloads in payloads_by_group.items()
    }

    total_behind = total_stale = total_tables = 0
    for group in groups:
        payloads, states = payloads_by_group[group], states_by_group[group]
        n_behind = sum(1 for state in states.values() if state == BEHIND)
        n_stale = sum(1 for state in states.values() if state == STALE)
        total_tables += len(states)
        total_behind += n_behind
        total_stale += n_stale

        counts = f"{len(states)} tables: {len(states) - n_behind - n_stale} ok"
        if n_behind:
            counts += f", {n_behind} behind"
        if n_stale:
            counts += f", {n_stale} STALE"
        lines.append(Line(group, note=counts))

        for table in sorted(states, key=lambda t: (RANK[states[t]], t)):
            state = states[table]
            if state == OK and not detailed:
                continue
            note = note_for(
                state,
                payloads[table],
                now,
                LAG_SECONDS_OVERRIDES.get(member_key(group, table)),
                STALE_SECONDS_OVERRIDES.get(member_key(group, table)),
            )
            lines.append(Line(table, state=state, note=note, depth=1))
        lines.append(Line(""))

    lines.append(
        Line(f"Summary: {total_behind} behind, {total_stale} stale across {total_tables} tables.")
    )

    summaries = [summarize_view(name, states_by_group) for name in VIEWS]
    if summaries:
        lines.append(Line(""))
        lines.extend(view_lines(summaries, detailed))

    return lines, total_behind + total_stale, total_tables


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


def build_slack_message(lines: list[Line], problems: int, total_tables: int, now: datetime) -> str:
    """
    Put a lead-in on the report and render the rest as Slack markdown.

    The lead doubles as the report's title, so `lines` carries no heading of its own. No
    code fence: rows identify themselves by their circle rather than by column alignment,
    which is what lets emoji, bolding and indentation render throughout.
    """
    stamp = now.strftime("%Y-%m-%dT%H:%MZ")
    if total_tables == 0:
        # Seeing no tables at all is most likely a failure to read:
        # list_objects swallows AccessDenied/NoSuchBucket and returns an empty list.
        state = "🚨 NO DATA. Could not read any status objects"
    elif problems:
        state = f"⚠️ {problems} table(s) behind or stale"
    else:
        state = "✅ all tables OK"
    lead = f"*Fares table status* {stamp}: {state}"

    return f"{lead}\n\n{_fit_to_slack_limit(lines, len(lead) + 2)}"


def main() -> int:
    """Parse args and dispatch to the single-table or overall view."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else None)
    parser.add_argument(
        "--table",
        metavar="GROUP:TABLE",
        help='one table, qualified by its group (e.g. "ODS:EDW.SALE_TRANSACTION")',
    )
    parser.add_argument(
        "--lag-seconds",
        type=float,
        default=DEFAULT_LAG_SECONDS,
        help=(
            f"seq_lag over this many seconds counts as behind (default "
            f"{DEFAULT_LAG_SECONDS}, i.e. {DEFAULT_LAG_SECONDS / 3600}h); "
            "tables listed in LAG_SECONDS_OVERRIDES keep their own threshold"
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
        help="in the overall report, print a key-info line for every table, OK ones included",
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
            "post the summary to Slack rather than printing it; the webhook URL is read "
            "from $SLACK_WEBHOOK. Exit status then reports whether the report was "
            "delivered, not whether the tables are healthy"
        ),
    )
    parser.add_argument(
        "--slack-test",
        action="store_true",
        help=(
            "render the Slack message and print it here instead of posting; needs no "
            "$SLACK_WEBHOOK and contacts Slack not at all"
        ),
    )
    parser.add_argument(
        "--only-if-problems",
        action="store_true",
        help="with --slack, post nothing when every table is OK",
    )
    args = parser.parse_args()

    slack_mode = args.slack or args.slack_test

    if slack_mode and (args.json or args.table):
        parser.error("--slack applies to the overall report, not to --json or --table")

    if args.only_if_problems and not slack_mode:
        parser.error("--only-if-problems requires --slack or --slack-test")

    # Report errors and exit if VIEWS constant configured incorrectly
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
    default_lag_seconds = args.lag_seconds
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
        print_table_detail(group, table, payload, now, default_stale_seconds, default_lag_seconds)
        table_lag_seconds = threshold_seconds(
            LAG_SECONDS_OVERRIDES, group, table, default_lag_seconds
        )
        table_stale_seconds = threshold_seconds(
            STALE_SECONDS_OVERRIDES, group, table, default_stale_seconds
        )
        return 0 if classify(payload, now, table_stale_seconds, table_lag_seconds) == OK else 1

    groups = list(GROUPS)

    if args.json:
        out: dict[str, dict] = {}
        with tempfile.TemporaryDirectory() as tmpdir:
            for group in groups:
                out[group] = fetch_group(GROUPS[group].prefix, tmpdir)
        print(json.dumps(out, indent=2))
        return 0

    if slack_mode:
        # --slack-test never contacts Slack, so it must not require the credential.
        if not SLACK_WEBHOOK and not args.slack_test:
            print("SLACK_WEBHOOK is unset; cannot post to Slack.", file=sys.stderr)
            return 1

        # build_report prints nothing, so on this public repo's world-readable CI log the
        # summary reaches Slack without ever passing through stdout.
        lines, problems, total_tables = build_report(
            groups, now, default_stale_seconds, default_lag_seconds, args.detailed
        )

        # A read that turned up nothing is always worth reporting, even under
        # --only-if-problems: silence there would look identical to a healthy day.
        if problems == 0 and total_tables > 0 and args.only_if_problems:
            if args.slack_test:
                print("Every table is OK; --only-if-problems would post nothing.", file=sys.stderr)
            return 0

        message = build_slack_message(lines, problems, total_tables, now)

        if args.slack_test:
            # The message goes to stdout so it can be piped or diffed; the size note goes
            # to stderr so it never pollutes that output. Flush first, or the note (on
            # unbuffered stderr) jumps ahead of the message when stdout is piped.
            print(message, flush=True)
            truncated = TRUNCATION_NOTICE.strip() in message
            note = f"{len(message):,} of {SLACK_TEXT_LIMIT:,} characters"
            print(
                f"\n[--slack-test: not posted; {note}{'; TRUNCATED' if truncated else ''}]",
                file=sys.stderr,
            )
            return 0

        try:
            _slack_post(SLACK_WEBHOOK, {"text": message})
        except RuntimeError as error:
            print(str(error), file=sys.stderr)
            return 1
        # No tables indicates a problem regardless
        return 1 if total_tables == 0 else 0

    lines, problems, total_tables = build_report(
        groups, now, default_stale_seconds, default_lag_seconds, args.detailed
    )
    header = [Line(f"Fares table status: {now.strftime('%Y-%m-%dT%H:%MZ')}"), Line("")]
    print(render(header + lines))
    return 1 if problems or total_tables == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
