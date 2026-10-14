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
        incoming_bucket = os.environ["S3_BUCKET_INCOMING"]
        archive_bucket = os.environ["S3_BUCKET_ARCHIVE"]
        incoming_prefix = os.environ.get("S3_BUCKET_PREFIX_INCOMING", "")
        archive_prefix = os.environ.get("S3_BUCKET_PREFIX_ARCHIVE", "")

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


def snapshot_for_source(
    client: Any, settings: Settings, relative_key: str, source: dict[str, Any]
) -> str:
    """
    Select the base table's first-LOAD S3 modification time, as data_platform did.

    Read incoming anchors before archive directories so a moving anchor cannot
    disappear between those reads. __ct files share the base table's snapshot.
    """
    directory, filename = relative_key.rsplit("/", 1)
    source_time = source["LastModified"].astimezone(UTC).replace(microsecond=0)
    if filename in FIRST_LOAD_NAMES and not directory.endswith("__ct"):
        return source_time.strftime(SNAPSHOT_FORMAT)

    base_directory = directory.removesuffix("__ct")
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


def process_record(client: Any, settings: Settings, record: dict[str, Any]) -> str:
    """Move one selected CSV/DFM pair only when both sources and destinations are unambiguous."""
    if record["eventSource"] != "aws:s3":
        raise ValueError("Expected a direct S3 notification")

    if not record["eventName"].startswith("ObjectCreated:"):
        return "ignored_event"

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
        or not relative_key.endswith(".csv.gz")
    ):
        return "ignored_key"

    if key_parts[2].removesuffix("__ct") not in TABLE_PREFIXES:
        LOGGER.warning("Ignoring table outside allowlist: %s", relative_key)
        return "ignored_table"

    source = head_object(client, source_bucket, source_key)
    event_object = record["s3"]["object"]
    event_time = datetime.fromisoformat(record["eventTime"])
    if event_time.tzinfo is None:
        raise ValueError("S3 eventTime must include a timezone")

    if (
        source is None
        or source["ContentLength"] <= 0
        or source["ETag"].strip('"') != event_object["eTag"].strip('"')
        or source["ContentLength"] != event_object["size"]
        or source.get("VersionId") != event_object.get("versionId")
        or source["LastModified"] > event_time
    ):
        raise RuntimeError(f"CSV missing, empty or replaced: {source_key}")

    dfm_source_key = source_key.removesuffix(".csv.gz") + ".dfm"
    dfm_source = head_object(client, source_bucket, dfm_source_key)
    if dfm_source is None or dfm_source["ContentLength"] <= 0:
        raise RuntimeError(f"DFM missing or empty: {dfm_source_key}")

    snapshot = snapshot_for_source(client, settings, relative_key, source)
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
    return "archived"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    """Process all records; log failures requiring reconciliation and fail the invocation."""
    if event.get("Event") == "s3:TestEvent":
        LOGGER.info("Received S3 notification configuration test")
        return {"test_event": 1}

    settings = Settings.from_environment()
    records = event.get("Records")
    if not isinstance(records, list) or not records:
        raise ValueError("Expected a nonempty Records list")

    outcomes: dict[str, int] = {}
    failures = []

    for record in records:
        try:
            outcome = process_record(s3_client(), settings, record)
            outcomes[outcome] = outcomes.get(outcome, 0) + 1
            LOGGER.info("Cubic mover outcome=%s", outcome)
        except (BotoCoreError, ClientError, KeyError, TypeError, ValueError, RuntimeError) as error:
            LOGGER.exception("Cubic move failed; manual reconciliation required: %s", record)
            failures.append(error)

    if failures:
        raise ExceptionGroup("Cubic moves failed; manual reconciliation required", failures)

    return outcomes
