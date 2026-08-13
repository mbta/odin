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

  * STALE: no object, or no run within --stale-hours (default 24h). Jobs are
    serialized rather than run in parallel, so one flat threshold is more meaningful than
    each job's own cadence. A stale object means the job is not finishing -- which is
    itself the signal worth having.
  * BEHIND: running recently, but not caught up: the job's own keep-up flag says so
    (cdc_budget_nearly_full / merge_budget_full / caught_up false /
    jobs_lag > 0), or the true backlog (seq_lag_seconds) exceeds --lag-hours.
  * OK: ran recently and caught up.

Rarely-updated tables (like the DIMENSION tables) have a large clock_lag_seconds
(its newest data is old), but a seq_lag_seconds of ~0 (it has consumed everything
upstream). Classification uses the backlog signals, never the clock lag, so these tables
read as OK as long as they are caught up to history.

Usage:
    python scripts/get_table_status.py                      # overall summary (hides OK tables)
    python scripts/get_table_status.py --detailed           # include info for OK tables
    python scripts/get_table_status.py --group ODS --table EDW.SALE_TRANSACTION # table details
    python scripts/get_table_status.py --slack              # post to Slack using $SLACK_WEBHOOK
    python scripts/get_table_status.py --slack-test         # preview Slack message, post nothing
"""

import argparse
import contextlib
import io
import json
import logging
import os
import sys
import tempfile
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Optional

from odin.utils.aws.s3 import download_object
from odin.utils.aws.s3 import list_objects
from odin.utils.locations import AFC_STATUS
from odin.utils.locations import CUBIC_ODS_DELTA_STATUS
from odin.utils.locations import CUBIC_ODS_FACT_STATUS
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_STATUS
from odin.utils.logger import LOGGER_NAME


GROUPS: dict[str, dict[str, Any]] = {
    "ODS": {
        "prefix": CUBIC_ODS_FACT_STATUS,
        "tables": ("odin.ingestion.qlik.tables", "CUBIC_ODS_TABLES_INSTANCE"),
    },
    "delta_ODS": {
        "prefix": CUBIC_ODS_DELTA_STATUS,
        "tables": ("odin.ingestion.qlik.tables", "CUBIC_ODS_DELTA_TABLES_INSTANCE"),
    },
    "AFC": {
        "prefix": AFC_STATUS,
        "tables": ("odin.ingestion.afc.afc_tables", "API_TABLES_INSTANCE"),
    },
    "masabi": {
        "prefix": MASABI_STATUS,
        "tables": ("odin.ingestion.masabi.masabi_tables", "TABLES_INSTANCE"),
    },
}

OK, BEHIND, STALE = "OK", "BEHIND", "STALE"

DEFAULT_STALE_HOURS = 24.0
DEFAULT_LAG_HOURS = 4.0

# Cap on concurrent status downloads; the shared boto3 client is thread-safe and its
# connection pool is sized well above this.
MAX_FETCH_WORKERS = 16

# Webhook URL == credential for posting to Slack
# Ensure it never is passed in plain text, or makes it to error messages
SLACK_WEBHOOK_ENV = "SLACK_WEBHOOK"

# Slack `text` field accepts max 40,000 characters, truncate to fit if
# necessary.
# Not expecting to see this limit hit: --detailed view as of 2026-08-12 is
# displaying about 20k characters, and most status reports will be ~1k
SLACK_TEXT_LIMIT = 40000
TRUNCATION_NOTICE = "\n[truncated: report exceeded Slack's 40,000 character limit]"


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
    module_path, attr = GROUPS[group]["tables"]
    try:
        module = __import__(module_path, fromlist=[attr])
        return list(getattr(module, attr))
    except Exception:  # noqa: BLE001 - the list is a nicety, not a requirement
        return []


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


def _behind_note(payload: dict) -> str:
    """One-line reason a BEHIND table is behind, using whatever the group publishes."""
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
    return ", ".join(parts) if parts else "not caught up"


def _stale_note(table: str, payload: dict, now: datetime) -> str:
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
    return f"last run {ago} ago (cadence {cadence}){lag_statement}"


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


def print_table_detail(
    group: str, table: str, payload: dict, now: datetime, stale_seconds: float, lag_seconds: float
) -> None:
    """Print the full single-table summary."""
    state = classify(payload, now, stale_seconds, lag_seconds)
    print(f"{table}  ({group})")
    print(f"  state:        {state}")
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


def print_overall(
    groups: list[str],
    now: datetime,
    stale_seconds: float,
    lag_seconds: float,
    detailed: bool = False,
) -> int:
    """
    Print the summary for `groups`; return the count of not-OK tables.

    When `detailed`, every table gets a per-table line (OK tables included, with a
    key-info summary); otherwise only not-OK tables are listed.
    """
    print(f"Fares table status: {now.strftime('%Y-%m-%dT%H:%MZ')}")

    print("\nKey:")
    print(
        f"\tBEHIND = Latest timestamp older than {lag_seconds / 3600:g} hours, and/or "
        "uningested data remains from source"
    )
    print(f"\tSTALE = No successful update within {stale_seconds / 3600:g} hours.")
    print("\tOK = Not stale or behind\n")

    total_behind = total_stale = total_tables = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        for group in groups:
            payloads = fetch_group(GROUPS[group]["prefix"], tmpdir)
            # Flag expected tables that have never published an object.
            for table in expected_tables(group):
                payloads.setdefault(table, {})

            states = {t: classify(p, now, stale_seconds, lag_seconds) for t, p in payloads.items()}
            n_ok = sum(1 for s in states.values() if s == OK)
            n_behind = sum(1 for s in states.values() if s == BEHIND)
            n_stale = sum(1 for s in states.values() if s == STALE)
            total_tables += len(states)
            total_behind += n_behind
            total_stale += n_stale

            counts = f"{n_ok} ok"
            if n_behind:
                counts += f"   {n_behind} behind"
            if n_stale:
                counts += f"   {n_stale} STALE"
            print(f"{group:<11} {len(states):>3} tables:  {counts}")

            # Detail lines: not-OK always; OK too under --detailed. Worst first.
            rank = {STALE: 0, BEHIND: 1, OK: 2}
            for table in sorted(states, key=lambda t: (rank[states[t]], t)):
                state = states[table]
                if state == OK and not detailed:
                    continue
                if state == STALE:
                    note = _stale_note(table, payloads[table], now)
                elif state == BEHIND:
                    note = _behind_note(payloads[table])
                else:
                    note = _ok_note(payloads[table], now)
                print(f"  {state:<7} {table:<32} {note}")
            print()

    problems = total_behind + total_stale
    print(f"Summary: {total_behind} behind, {total_stale} stale across {total_tables} tables.")
    return problems


def _fit_to_slack_limit(report: str, overhead: int) -> str:
    budget = SLACK_TEXT_LIMIT - overhead
    if len(report) <= budget:
        return report
    keep = max(budget - len(TRUNCATION_NOTICE), 0)
    return report[:keep].rstrip() + TRUNCATION_NOTICE


def _slack_post(webhook: str, payload: dict) -> None:
    """
    POST one message to `webhook`, raising RuntimeError with no URL in the message.

    urllib puts the request URL into HTTPError's string form, so failures are re-raised
    with only the status and Slack's error string ("invalid_payload", "no_service", ...).
    Letting the original propagate would print the webhook into a public CI log.
    """
    request = urllib.request.Request(
        webhook,
        json.dumps(payload).encode("utf-8"),
        {"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8", "replace").strip()
        if body != "ok":
            raise RuntimeError(f"Slack returned an unexpected body: {body!r}")
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", "replace").strip()
        raise RuntimeError(f"Slack rejected the message: HTTP {error.code} {detail}") from None
    except urllib.error.URLError as error:
        raise RuntimeError(f"Could not reach Slack: {error.reason}") from None


def build_slack_message(report: str, problems: int) -> str:
    """Add header to status body, which can optionally contain emojis (including mbta ones)"""
    if problems:
        lead = f":mbta_bus_intensifies: *Fares table status*: {problems} table(s) behind or stale"
    else:
        lead = ":white_check_mark: *Fares table status*: all tables OK"

    # The fence and the lead-in count against Slack's cap along with the report.
    prefix = f"{lead}\n```\n"
    suffix = "\n```"
    body = _fit_to_slack_limit(report, len(prefix) + len(suffix))
    return f"{prefix}{body}{suffix}"


def post_report_to_slack(message: str, webhook: str) -> None:
    """Post an already-rendered message to Slack as text"""
    _slack_post(webhook, {"text": message})


def main() -> int:
    """Parse args and dispatch to the single-table or overall view."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else None)
    parser.add_argument("--group", choices=list(GROUPS), help="restrict to one group")
    parser.add_argument("--table", help="a single table within --group")
    parser.add_argument(
        "--lag-hours",
        type=float,
        default=DEFAULT_LAG_HOURS,
        help=f"seq_lag over this many hours counts as behind (default {DEFAULT_LAG_HOURS})",
    )
    parser.add_argument(
        "--stale-hours",
        type=float,
        default=DEFAULT_STALE_HOURS,
        help=f"no run within this many hours counts as stale (default {DEFAULT_STALE_HOURS:g})",
    )
    parser.add_argument("--json", action="store_true", help="print raw payload(s), unformatted")
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="in the overall/group view, print a key-info line for every table, OK ones included",
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
            f"from ${SLACK_WEBHOOK_ENV}. Exit status then reports whether the report was "
            "delivered, not whether the tables are healthy"
        ),
    )
    parser.add_argument(
        "--slack-test",
        action="store_true",
        help=(
            "render the Slack message and print it here instead of posting; needs no "
            f"${SLACK_WEBHOOK_ENV} and contacts Slack not at all"
        ),
    )
    parser.add_argument(
        "--only-if-problems",
        action="store_true",
        help="with --slack, post nothing when every table is OK",
    )
    args = parser.parse_args()

    slack_mode = args.slack or args.slack_test

    if args.table and not args.group:
        parser.error("--table requires --group")

    if slack_mode and (args.json or args.table):
        parser.error("--slack applies to the overall/group view, not to --json or --table")

    if args.only_if_problems and not slack_mode:
        parser.error("--only-if-problems requires --slack or --slack-test")

    # The S3 helpers log an INFO line per list/download via Odin's shared logger, which
    # would bury this summary. Quiet that logger unless --verbose is asked for; leave the
    # root logger alone so genuine warnings/errors still surface.
    if not args.verbose:
        logging.getLogger(LOGGER_NAME).setLevel(logging.WARNING)

    now = _utc_now()
    lag_seconds = args.lag_hours * 3600
    stale_seconds = args.stale_hours * 3600

    # Single table: fetch just that table's object, not the whole group.
    if args.table:
        with tempfile.TemporaryDirectory() as tmpdir:
            payloads = fetch_group(GROUPS[args.group]["prefix"], tmpdir, only={args.table})
        payload = payloads.get(args.table, {})
        if args.json:
            print(json.dumps(payload, indent=2))
            return 0
        print_table_detail(args.group, args.table, payload, now, stale_seconds, lag_seconds)
        return 0 if classify(payload, now, stale_seconds, lag_seconds) == OK else 1

    # A single group, or all of them.
    groups = [args.group] if args.group else list(GROUPS)

    if args.json:
        out: dict[str, dict] = {}
        with tempfile.TemporaryDirectory() as tmpdir:
            for group in groups:
                out[group] = fetch_group(GROUPS[group]["prefix"], tmpdir)
        print(json.dumps(out, indent=2))
        return 0

    if slack_mode:
        # --slack-test never contacts Slack, so it must not require the credential.
        webhook = os.environ.get(SLACK_WEBHOOK_ENV, "").strip()
        if not webhook and not args.slack_test:
            print(f"{SLACK_WEBHOOK_ENV} is unset; cannot post to Slack.", file=sys.stderr)
            return 1

        # Capture the report instead of printing it. On a public repo the CI job log is
        # world-readable, so the summary should reach Slack without passing through stdout.
        buffer = io.StringIO()
        with contextlib.redirect_stdout(buffer):
            problems = print_overall(groups, now, stale_seconds, lag_seconds, args.detailed)

        if problems == 0 and args.only_if_problems:
            if args.slack_test:
                print("Every table is OK; --only-if-problems would post nothing.", file=sys.stderr)
            return 0

        message = build_slack_message(buffer.getvalue().strip(), problems)

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
            post_report_to_slack(message, webhook)
        except RuntimeError as error:
            print(str(error), file=sys.stderr)
            return 1
        return 0

    return 1 if print_overall(groups, now, stale_seconds, lag_seconds, args.detailed) else 0


if __name__ == "__main__":
    sys.exit(main())
