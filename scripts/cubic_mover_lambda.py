"""
Move selected Qlik CSV/DFM pairs from incoming to archive, without a database.

This file is deployed alone using Lambda's boto3. Ambiguous or partial moves fail
for manual reconciliation; see docs/cubic_mover_lambda.md for the operating contract.
"""

import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import unquote_plus

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

# Names only, e.g. "EDW.WHATEVER". Each entry also includes its __ct directory.
# Empty means no tables are moved.
TABLE_PREFIXES: list[str] = []

QLIK_PREFIX = "cubic/ods_qlik/"
SNAPSHOT_FORMAT = "%Y%m%dT%H%M%SZ"
FIRST_LOAD_NAMES = ("LOAD00000001.csv.gz", "LOAD1.csv.gz")
# Odin moves archived files to these locations, retaining the original key suffix.
ARCHIVE_LOCATIONS = (
    "",
    "odin/archive/cubic_qlik/processed/",
    "odin/archive/cubic_qlik/ignored/",
    "odin/error/cubic_qlik/",
)
TRANSFER_CONFIG = TransferConfig(
    multipart_threshold=64 * 1024 * 1024,
    multipart_chunksize=64 * 1024 * 1024,
    max_concurrency=4,
)
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
_s3: Any = None


@dataclass(frozen=True)
class Settings:
    """Explicit source and archive locations."""

    incoming_bucket: str
    archive_bucket: str
    incoming_prefix: str
    archive_prefix: str

    @classmethod
    def from_environment(cls) -> "Settings":
        """Validate locations and table names before accessing S3."""
        # FOR TESTING ONLY put in proper env vars before prod deploy
        incoming_bucket = os.environ.get("S3_BUCKET_INCOMING", "mbta-ctd-dataplatform-dev-incoming")
        archive_bucket = os.environ.get("S3_BUCKET_ARCHIVE", "mbta-ctd-dataplatform-dev-archive")
        incoming_prefix = os.environ.get("S3_BUCKET_PREFIX_INCOMING", "cubic/ods_qlik")
        archive_prefix = os.environ.get("S3_BUCKET_PREFIX_ARCHIVE", "mover_test/incoming")

        for bucket in (incoming_bucket, archive_bucket):
            if not bucket or "/" in bucket or bucket != bucket.strip():
                raise ValueError("Bucket settings must be bucket names, not S3 URLs")

        for prefix in (incoming_prefix, archive_prefix):
            if prefix and (prefix.startswith("/") or not prefix.endswith("/")):
                raise ValueError("Nonempty bucket prefixes must end with / and not start with /")

        if incoming_bucket == archive_bucket and (
            incoming_prefix.startswith(archive_prefix) or archive_prefix.startswith(incoming_prefix)
        ):
            raise ValueError("Incoming and archive prefixes must not overlap in the same bucket")

        for table in TABLE_PREFIXES:
            if (
                not isinstance(table, str)
                or not table
                or "/" in table
                or table != table.strip()
                or table in (".", "..")
                or table.endswith("__ct")
            ):
                raise ValueError(f"Expected a base table name such as EDW.WHATEVER: {table!r}")

        return cls(incoming_bucket, archive_bucket, incoming_prefix, archive_prefix)


def s3_client() -> Any:
    """Reuse the client while letting boto3 retry transient request failures."""
    global _s3
    if _s3 is None:
        _s3 = boto3.client(
            "s3",
            config=Config(
                retries={"mode": "standard", "total_max_attempts": 5},
                connect_timeout=10,
                read_timeout=60,
            ),
        )

    return _s3


def head_object(client: Any, bucket: str, key: str) -> dict[str, Any] | None:
    """Treat only not-found errors as absence; permission and service errors must fail."""
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def table_snapshots(client: Any, settings: Settings, table: str) -> set[datetime]:
    """
    Read snapshot state once per table scan, before moving any first-LOAD files.

    Read incoming anchors before archive directories so a moving anchor cannot
    disappear between those reads. __ct files share the base table's snapshot.
    """
    base_directory = QLIK_PREFIX + table
    snapshot_times = set()

    for first_load in FIRST_LOAD_NAMES:
        anchor_key = f"{settings.incoming_prefix}{base_directory}/{first_load}"
        anchor = head_object(client, settings.incoming_bucket, anchor_key)
        if anchor is not None:
            snapshot_times.add(anchor["LastModified"].astimezone(UTC).replace(microsecond=0))

    for location in ARCHIVE_LOCATIONS:
        prefix = f"{location}{settings.archive_prefix}{base_directory}/"
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket=settings.archive_bucket, Prefix=prefix, Delimiter="/"
        )
        for page in pages:
            for entry in page.get("CommonPrefixes", []):
                partition = entry["Prefix"][len(prefix) :].rstrip("/")
                if not partition.startswith("snapshot="):
                    continue

                timestamp = partition.removeprefix("snapshot=")
                parsed = datetime.strptime(timestamp, SNAPSHOT_FORMAT).replace(tzinfo=UTC)
                if parsed.strftime(SNAPSHOT_FORMAT) != timestamp:
                    raise ValueError(f"Invalid snapshot partition: {entry['Prefix']}")
                snapshot_times.add(parsed)

    return snapshot_times


def snapshot_for_source(
    relative_key: str, source: dict[str, Any], snapshot_times: set[datetime]
) -> str:
    """Choose the newest snapshot not after this CSV, preserving the legacy layout."""
    directory, filename = relative_key.rsplit("/", 1)
    source_time = source["LastModified"].astimezone(UTC).replace(microsecond=0)
    if filename in FIRST_LOAD_NAMES and not directory.endswith("__ct"):
        return source_time.strftime(SNAPSHOT_FORMAT)

    eligible_times = [timestamp for timestamp in snapshot_times if timestamp <= source_time]
    if not eligible_times:
        raise RuntimeError(f"No snapshot at or before source modification time: {relative_key}")

    return max(eligible_times).strftime(SNAPSHOT_FORMAT)


def copy_verified(
    client: Any,
    settings: Settings,
    source_key: str,
    source: dict[str, Any],
    destination_key: str,
) -> None:
    """Copy server-side without retaining metadata, then check destination size."""
    copy_source = {"Bucket": settings.incoming_bucket, "Key": source_key}
    if "VersionId" in source:
        copy_source["VersionId"] = source["VersionId"]

    # REPLACE intentionally discards source metadata, including any old mover receipts.
    # Managed copy supports files above the single-request 5 GiB limit.
    client.copy(
        copy_source,
        settings.archive_bucket,
        destination_key,
        ExtraArgs={
            "CopySourceIfMatch": source["ETag"],
            "MetadataDirective": "REPLACE",
            "Metadata": {},
        },
        Config=TRANSFER_CONFIG,
    )

    destination = head_object(client, settings.archive_bucket, destination_key)
    if destination is None or destination["ContentLength"] != source["ContentLength"]:
        raise RuntimeError(f"Copy verification failed: {destination_key}")


def notification_table(settings: Settings, record: dict[str, Any]) -> str | None:
    """Treat a CSV or DFM creation as a table wake-up, not an object-processing command."""
    if record["eventSource"] != "aws:s3":
        raise ValueError("Expected a direct S3 notification")

    if not record["eventName"].startswith("ObjectCreated:"):
        LOGGER.info("Ignoring non-creation notification: %s", record["eventName"])
        return None

    source_bucket = record["s3"]["bucket"]["name"]
    source_key = unquote_plus(record["s3"]["object"]["key"])
    if source_bucket != settings.incoming_bucket or not source_key.startswith(
        settings.incoming_prefix
    ):
        raise ValueError(
            f"Notification outside incoming location: s3://{source_bucket}/{source_key}"
        )

    relative_key = source_key.removeprefix(settings.incoming_prefix)
    key_parts = relative_key.split("/")
    if (
        not relative_key.startswith(QLIK_PREFIX)
        or len(key_parts) != 4
        or not relative_key.endswith((".csv.gz", ".dfm"))
    ):
        LOGGER.info("Ignoring non-Qlik notification: %s", relative_key)
        return None

    table = key_parts[2].removesuffix("__ct")
    if table not in TABLE_PREFIXES:
        LOGGER.warning("Ignoring table outside allowlist: %s", relative_key)
        return None

    return table


def check_source_consistency(source: dict[str, Any] | None, listed: dict[str, Any]) -> dict[str, Any]:
    """Refuse files that disappeared or changed since this scan's listing."""
    if (
        source is None
        or source["ContentLength"] <= 0
        or source["ETag"] != listed["ETag"]
        or source["ContentLength"] != listed["Size"]
        or source["LastModified"] != listed["LastModified"]
    ):
        raise RuntimeError(f"Source missing, empty or replaced during scan: {listed['Key']}")
    return source

def move_pair(
    client: Any,
    settings: Settings,
    csv_listed: dict[str, Any],
    dfm_listed: dict[str, Any],
    snapshot_times: set[datetime],
) -> None:
    """Move one scanned pair; never recover or overwrite an ambiguous destination."""
    source_bucket = settings.incoming_bucket
    source_key = csv_listed["Key"]
    dfm_source_key = dfm_listed["Key"]
    source = head_object(client, source_bucket, source_key)
    dfm_source = head_object(client, source_bucket, dfm_source_key)

    source = check_source_consistency(source, csv_listed)
    dfm_source = check_source_consistency(dfm_source, dfm_listed)

    relative_key = source_key.removeprefix(settings.incoming_prefix)
    snapshot = snapshot_for_source(relative_key, source, snapshot_times)
    directory, filename = relative_key.rsplit("/", 1)
    destination_key = f"{settings.archive_prefix}{directory}/snapshot={snapshot}/{filename}"
    dfm_destination_key = destination_key.removesuffix(".csv.gz") + ".dfm"
    object_pairs = [
        (source_key, destination_key, source),
        (dfm_source_key, dfm_destination_key, dfm_source),
    ]

    # Preflight both destinations, including files already consumed by Odin.
    # Existence alone cannot prove that a previous copy completed safely.
    for _, archive_key, _ in object_pairs:
        for location in ARCHIVE_LOCATIONS:
            candidate_key = location + archive_key
            if head_object(client, settings.archive_bucket, candidate_key) is not None:
                raise RuntimeError(f"Destination already exists: {candidate_key}")

    for incoming_key, archive_key, incoming in object_pairs:
        copy_verified(client, settings, incoming_key, incoming, archive_key)

    # Neither source is deleted until BOTH copies succeed and their sizes are checked.
    # A failure between deletes is deliberately left for manual reconciliation.
    for incoming_key, _, incoming in object_pairs:
        delete_args = {"Bucket": source_bucket, "Key": incoming_key, "IfMatch": incoming["ETag"]}
        if incoming.get("VersionId") not in (None, "null"):
            delete_args["VersionId"] = incoming["VersionId"]
        client.delete_object(**delete_args)

    LOGGER.info(
        "Archived s3://%s/%s to s3://%s/%s",
        source_bucket,
        source_key,
        settings.archive_bucket,
        destination_key,
    )


def scan_table(client: Any, settings: Settings, table: str) -> int:
    """
    Scan both incoming directories and move pairs in CSV LastModified/key order.

    Finish listing before deleting: mutations must not affect pagination. Require
    complete pairs in the listing so a lone DFM left by a failed delete cannot be
    silently abandoned. A temporarily incomplete upload can succeed on a later scan.
    """
    objects: dict[str, dict[str, Any]] = {}
    for directory in (table, table + "__ct"):
        prefix = f"{settings.incoming_prefix}{QLIK_PREFIX}{directory}/"
        pages = client.get_paginator("list_objects_v2").paginate(
            Bucket=settings.incoming_bucket, Prefix=prefix, Delimiter="/"
        )
        for page in pages:
            for listed in page.get("Contents", []):
                if listed["Key"].endswith((".csv.gz", ".dfm")):
                    objects[listed["Key"]] = listed

    for key in objects:
        companion_key = (
            key.removesuffix(".csv.gz") + ".dfm"
            if key.endswith(".csv.gz")
            else key.removesuffix(".dfm") + ".csv.gz"
        )
        if companion_key not in objects:
            raise RuntimeError(f"Incomplete incoming pair: {key}; missing {companion_key}")

    csv_objects = sorted(
        (listed for key, listed in objects.items() if key.endswith(".csv.gz")),
        key=lambda listed: (listed["LastModified"], listed["Key"]),
    )
    if not csv_objects:
        LOGGER.info("No incoming pairs for table=%s", table)
        return 0

    snapshot_times = table_snapshots(client, settings, table)
    for listed in csv_objects:
        dfm_key = listed["Key"].removesuffix(".csv.gz") + ".dfm"
        move_pair(client, settings, listed, objects[dfm_key], snapshot_times)

    LOGGER.info("Scanned table=%s archived_pairs=%s", table, len(csv_objects))
    return len(csv_objects)


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    """Scan notified tables once each, or all configured tables for an explicit sweep."""
    if event.get("Event") == "s3:TestEvent":
        LOGGER.info("Received S3 notification configuration test")
        return {"test_event": 1}

    settings = Settings.from_environment()
    # A scheduler can use this same explicit payload; no synthetic S3 events are needed.
    if event == {"scan": True}:
        tables = set(TABLE_PREFIXES)
    else:
        records = event.get("Records")
        if not isinstance(records, list) or not records:
            raise ValueError('Expected a nonempty Records list or {"scan": true}')
        tables = set() 
        for record in records:
            table = notification_table(settings, record)
            if table is not None and table not in tables:
                tables.add(table)

    archived_pairs = 0
    failures = []

    for table in tables:
        try:
            archived_pairs += scan_table(s3_client(), settings, table)
        except (BotoCoreError, ClientError, KeyError, TypeError, ValueError, RuntimeError) as error:
            LOGGER.exception("Table scan failed; manual reconciliation required: %s", table)
            failures.append(error)

    if failures:
        raise ExceptionGroup("Cubic moves failed; manual reconciliation required", failures)

    return {"tables_scanned": len(tables), "archived": archived_pairs}
