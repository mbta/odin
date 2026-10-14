"""
Archive Cubic S3 arrivals without the legacy database or Glue ingestion.

This file is deployed on its own; use only the standard library and Lambda's boto3.
See docs/cubic_mover_lambda.md for the event contract and cutover requirements.
"""

import hashlib
import json
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

# Populate with base-table prefixes, e.g. "cubic/ods_qlik/EDW.SAMPLE/" or "cubic/dmap/sample/".
# ODS entries also include their __ct directory. An empty list copies nothing.
TARGET_TABLE_PREFIXES: list[str] = []

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
SNAPSHOT_FORMAT = "%Y%m%dT%H%M%SZ"
FIRST_LOAD_NAMES = ("LOAD00000001.csv.gz", "LOAD1.csv.gz")
# Odin relocates archived Qlik files, preserving their original path and metadata.
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
_s3: Any = None


@dataclass(frozen=True)
class Settings:
    """Explicit source, destination and active-table allowlist."""

    incoming_bucket: str
    archive_bucket: str
    incoming_prefix: str
    archive_prefix: str
    table_prefixes: frozenset[str]

    @classmethod
    def from_environment(cls) -> "Settings":
        """Validate environment-supplied locations and the in-script table allowlist."""
        incoming_bucket = os.environ["S3_BUCKET_INCOMING"]
        archive_bucket = os.environ["S3_BUCKET_ARCHIVE"]
        incoming_prefix = os.environ.get("S3_BUCKET_PREFIX_INCOMING", "")
        archive_prefix = os.environ.get("S3_BUCKET_PREFIX_ARCHIVE", "")

        for bucket in (incoming_bucket, archive_bucket):
            if not bucket or "/" in bucket or bucket != bucket.strip():
                raise ValueError("Bucket settings must be nonempty bucket names, not S3 URLs")

        for prefix in (incoming_prefix, archive_prefix):
            if prefix and (prefix.startswith("/") or not prefix.endswith("/")):
                raise ValueError("Nonempty bucket prefixes must end with / and not start with /")

        if incoming_bucket == archive_bucket:
            if incoming_prefix.startswith(archive_prefix) or archive_prefix.startswith(
                incoming_prefix
            ):
                raise ValueError(
                    "Incoming and archive prefixes must not overlap in the same bucket"
                )

        for prefix in TARGET_TABLE_PREFIXES:
            if not isinstance(prefix, str):
                raise ValueError("Table prefixes must be strings")

            parts = prefix.split("/")
            if (
                len(parts) != 4
                or parts[0] != "cubic"
                or parts[1] not in ("ods_qlik", "dmap")
                or not parts[2]
                or parts[2] in (".", "..")
                or parts[2].endswith("__ct")
                or parts[3] != ""
            ):
                raise ValueError(f"Invalid base-table prefix: {prefix!r}")

        return cls(
            incoming_bucket,
            archive_bucket,
            incoming_prefix,
            archive_prefix,
            frozenset(TARGET_TABLE_PREFIXES),
        )


def s3_client() -> Any:
    """Reuse connections on warm invocations without making AWS calls during import."""
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
    """Only a genuine not-found response means absence; access errors must fail."""
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def fingerprint(value: Any) -> str:
    """Create a compact, deterministic metadata value without embedding S3 key text."""
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode("utf-8")).hexdigest()


def source_fingerprint(bucket: str, key: str, source: dict[str, Any]) -> str:
    """Identify a particular source revision, including unversioned object timestamps."""
    return fingerprint(
        [
            bucket,
            key,
            source.get("VersionId"),
            source["ETag"],
            source["ContentLength"],
            source["LastModified"].isoformat(),
        ]
    )


def snapshot_partitions(client: Any, bucket: str, prefix: str) -> list[str]:
    """List all snapshot directories, including pages beyond S3's first 1,000 entries."""
    partitions = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix, Delimiter="/"
    ):
        for common_prefix in page.get("CommonPrefixes", []):
            partition = common_prefix["Prefix"][len(prefix) :].rstrip("/")
            if not partition.startswith("snapshot="):
                continue

            # A malformed snapshot is an operational error, not a reason to guess an older one.
            timestamp = partition.removeprefix("snapshot=")
            parsed = datetime.strptime(timestamp, SNAPSHOT_FORMAT)
            if parsed.strftime(SNAPSHOT_FORMAT) != timestamp:
                raise ValueError(f"Invalid snapshot partition: {common_prefix['Prefix']}")
            partitions.append(partition)

    return partitions


def destination_candidates(client: Any, settings: Settings, relative_key: str) -> list[str]:
    """Locate possible copies, including Qlik files already consumed by Odin."""
    if relative_key.startswith("cubic/dmap/"):
        return [settings.archive_prefix + relative_key]

    directory, filename = relative_key.rsplit("/", 1)
    candidates = []

    for location in ARCHIVE_LOCATIONS:
        prefix = f"{location}{settings.archive_prefix}{directory}/"
        for partition in snapshot_partitions(client, settings.archive_bucket, prefix):
            candidates.append(f"{prefix}{partition}/{filename}")

    return candidates


def snapshot_for_source(
    client: Any, settings: Settings, relative_key: str, source: dict[str, Any]
) -> str:
    """
    Use the first LOAD's S3 modification time, just as data_platform did.

    Full-load chunks and __ct files share the base table's snapshot. Read incoming
    anchors BEFORE archive directories so an anchor moving between buckets cannot
    disappear between the two reads. Never use invocation time or DFM write time.
    """
    directory, filename = relative_key.rsplit("/", 1)
    source_time = source["LastModified"].astimezone(UTC).replace(microsecond=0)
    if filename in FIRST_LOAD_NAMES and not directory.endswith("__ct"):
        return source_time.strftime(SNAPSHOT_FORMAT)

    base_directory = directory.removesuffix("__ct")
    snapshot_times = set()

    for first_load in FIRST_LOAD_NAMES:
        incoming_key = f"{settings.incoming_prefix}{base_directory}/{first_load}"
        anchor = head_object(client, settings.incoming_bucket, incoming_key)
        if anchor is not None:
            snapshot_times.add(anchor["LastModified"].astimezone(UTC).replace(microsecond=0))

    for location in ARCHIVE_LOCATIONS:
        prefix = f"{location}{settings.archive_prefix}{base_directory}/"
        for partition in snapshot_partitions(client, settings.archive_bucket, prefix):
            snapshot_times.add(
                datetime.strptime(partition.removeprefix("snapshot="), SNAPSHOT_FORMAT).replace(
                    tzinfo=UTC
                )
            )

    eligible_times = [timestamp for timestamp in snapshot_times if timestamp <= source_time]
    if not eligible_times:
        raise RuntimeError(f"No snapshot at or before source modification time for {relative_key}")

    return max(eligible_times).strftime(SNAPSHOT_FORMAT)


def find_copy(
    client: Any, settings: Settings, destination_key: str
) -> tuple[str, dict[str, Any]] | None:
    """Find a destination even if Odin has moved it since the preceding invocation."""
    relative_destination = destination_key
    for location in ARCHIVE_LOCATIONS[1:]:
        if relative_destination.startswith(location):
            relative_destination = relative_destination.removeprefix(location)
            break

    locations = ARCHIVE_LOCATIONS if "cubic/ods_qlik/" in relative_destination else ("",)

    for location in locations:
        candidate_key = location + relative_destination
        destination = head_object(client, settings.archive_bucket, candidate_key)
        if destination is not None:
            return candidate_key, destination

    return None


def copy_verified(
    client: Any,
    settings: Settings,
    source_key: str,
    source: dict[str, Any],
    destination_key: str,
    event_id: str,
) -> None:
    """Copy server-side, preserving metadata, and refuse conflicting archive objects."""
    identity = source_fingerprint(settings.incoming_bucket, source_key, source)
    existing = find_copy(client, settings, destination_key)

    if existing is None:
        copy_source = {"Bucket": settings.incoming_bucket, "Key": source_key}
        if "VersionId" in source:
            copy_source["VersionId"] = source["VersionId"]

        # The legacy copy lets S3 preserve metadata by default. Adding our retry receipts
        # requires REPLACE, so explicitly retain the original user metadata and headers too.
        extra_args = {
            "CopySourceIfMatch": source["ETag"],
            "MetadataDirective": "REPLACE",
            "Metadata": {
                **source.get("Metadata", {}),
                "cubic-mover-source": identity,
                "cubic-mover-event": event_id,
                "cubic-mover-size": str(source["ContentLength"]),
            },
        }

        for header in (
            "ContentType",
            "ContentEncoding",
            "ContentDisposition",
            "ContentLanguage",
            "CacheControl",
            "Expires",
            "WebsiteRedirectLocation",
        ):
            if header in source:
                extra_args[header] = source[header]

        # Managed copy uses multipart copy for large objects; no CSV bytes enter Lambda.
        client.copy(
            copy_source,
            settings.archive_bucket,
            destination_key,
            ExtraArgs=extra_args,
            Config=TRANSFER_CONFIG,
        )
        existing = find_copy(client, settings, destination_key)

    if (
        existing is None
        or existing[1].get("Metadata", {}).get("cubic-mover-source") != identity
        or existing[1].get("Metadata", {}).get("cubic-mover-event") != event_id
        or existing[1]["ContentLength"] != source["ContentLength"]
    ):
        raise RuntimeError(f"Archive identity/size conflict at {destination_key}; source retained")


def process_record(client: Any, settings: Settings, record: dict[str, Any]) -> str:
    """Move one DMAP CSV or one ODS CSV/DFM pair; return a logged outcome."""
    if record["eventSource"] != "aws:s3":
        raise ValueError("Expected a direct S3 notification, not an SQS/SNS/EventBridge envelope")

    if not record["eventName"].startswith("ObjectCreated:"):
        return "ignored_event"

    source_bucket = record["s3"]["bucket"]["name"]
    source_key = unquote_plus(record["s3"]["object"]["key"])
    if source_bucket != settings.incoming_bucket or not source_key.startswith(
        settings.incoming_prefix
    ):
        raise ValueError(
            f"Notification outside configured incoming location: s3://{source_bucket}/{source_key}"
        )

    relative_key = source_key.removeprefix(settings.incoming_prefix)
    key_parts = relative_key.split("/")
    if len(key_parts) != 4 or not relative_key.endswith(".csv.gz"):
        return "ignored_key"

    table_prefix = f"{key_parts[0]}/{key_parts[1]}/{key_parts[2].removesuffix('__ct')}/"
    if table_prefix not in settings.table_prefixes:
        LOGGER.warning("Ignoring table outside active allowlist: %s", relative_key)
        return "ignored_table"

    is_ods = relative_key.startswith("cubic/ods_qlik/")
    event_object = record["s3"]["object"]

    # Sequencer distinguishes same-key uploads with identical bytes. It is stable on redelivery.
    event_id = fingerprint(
        [source_bucket, source_key, event_object.get("versionId"), event_object["sequencer"]]
    )
    event_etag = event_object["eTag"].strip('"')
    event_size = event_object["size"]
    event_time = datetime.fromisoformat(record["eventTime"])

    if event_time.tzinfo is None:
        raise ValueError("S3 eventTime must include a timezone")

    if not event_etag or not isinstance(event_size, int) or event_size <= 0:
        raise ValueError(f"Empty or invalid CSV notification: {source_key}")

    source = head_object(client, source_bucket, source_key)
    event_matches_source = source is not None and (
        source["ETag"].strip('"') == event_etag
        and source["ContentLength"] == event_size
        and source.get("VersionId") == event_object.get("versionId")
        and source["LastModified"] <= event_time
    )

    # Check durable copy receipts before deriving a snapshot. This handles retries after
    # deletion, a crash between the two deletes, and notifications delayed past a reload.
    destination_key = None
    for candidate_key in destination_candidates(client, settings, relative_key):
        candidate = head_object(client, settings.archive_bucket, candidate_key)
        if (
            candidate is not None
            and candidate.get("Metadata", {}).get("cubic-mover-event") == event_id
        ):
            if candidate["ContentLength"] != event_size:
                raise RuntimeError(
                    f"Archived CSV size does not match notification: {candidate_key}"
                )

            destination_key = candidate_key
            break

    if destination_key is None:
        if not event_matches_source or source is None:
            raise RuntimeError(
                f"Source missing or replaced with no matching archive receipt: {source_key}"
            )

        if is_ods:
            snapshot = snapshot_for_source(client, settings, relative_key, source)
            directory, filename = relative_key.rsplit("/", 1)
            destination_key = f"{settings.archive_prefix}{directory}/snapshot={snapshot}/{filename}"
        else:
            destination_key = settings.archive_prefix + relative_key

    object_pairs = [(source_key, destination_key, source)]

    if is_ods:
        dfm_source_key = source_key.removesuffix(".csv.gz") + ".dfm"
        dfm_destination_key = destination_key.removesuffix(".csv.gz") + ".dfm"
        object_pairs.append(
            (
                dfm_source_key,
                dfm_destination_key,
                head_object(client, source_bucket, dfm_source_key),
            )
        )

    # Preflight the whole pair before copying anything. A CSV notification can precede
    # its DFM upload; raising leaves the CSV in place for Lambda retry or DLQ redrive.
    sources_to_delete = []
    copies_to_complete = []

    for incoming_key, archive_key, incoming in object_pairs:
        archived = find_copy(client, settings, archive_key)

        if archived is not None:
            archive_metadata = archived[1].get("Metadata", {})
            if archive_metadata.get("cubic-mover-event") != event_id or not archive_metadata.get(
                "cubic-mover-source"
            ):
                raise RuntimeError(f"Conflicting archive object: {archive_key}; source retained")

            if archive_metadata.get("cubic-mover-size") != str(archived[1]["ContentLength"]):
                raise RuntimeError(f"Archive size does not match receipt: {archive_key}")

            if incoming is not None and archive_metadata[
                "cubic-mover-source"
            ] == source_fingerprint(source_bucket, incoming_key, incoming):
                if archived[1]["ContentLength"] != incoming["ContentLength"]:
                    raise RuntimeError(f"Archive size mismatch: {archive_key}")

                sources_to_delete.append((incoming_key, incoming))
        else:
            if not event_matches_source or incoming is None or incoming["ContentLength"] <= 0:
                raise RuntimeError(f"Missing source or incomplete archived pair: {incoming_key}")

            copies_to_complete.append((incoming_key, archive_key, incoming))
            sources_to_delete.append((incoming_key, incoming))

    for incoming_key, archive_key, incoming in copies_to_complete:
        copy_verified(client, settings, incoming_key, incoming, archive_key, event_id)

    # Neither source is deleted until BOTH copies have succeeded and been verified.
    for incoming_key, incoming in sources_to_delete:
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

    return "archived" if sources_to_delete else "already_archived"


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    """Process every S3 record and raise failures so asynchronous delivery retries."""
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
            LOGGER.exception(
                "Cubic archive record failed: %s",
                record.get("s3") if isinstance(record, dict) else record,
            )
            failures.append(error)

    if failures:
        raise ExceptionGroup("Cubic archive records failed; redelivery is required", failures)

    return outcomes
