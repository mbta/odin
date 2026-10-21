"""Observe notified S3 objects without changing them or reading their contents."""

import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from urllib.parse import unquote_plus
from uuid import uuid4

import boto3
from botocore.exceptions import BotoCoreError, ClientError

ARCHIVE_RELOCATIONS = (
    "",
    "odin/archive/cubic_qlik/processed/",
    "odin/archive/cubic_qlik/ignored/",
    "odin/error/cubic_qlik/",
)
QLIK_PATH = re.compile(r"(cubic/ods_qlik/[^/]+)/([^/]+\.(?:csv\.gz|dfm))")
SNAPSHOT_DIRECTORY = re.compile(r"snapshot=\d{8}T\d{6}Z/")

JsonObject = dict[str, Any]
PartitionCache = dict[str, tuple[list[str], list[JsonObject]]]


def _log_formatter() -> logging.Formatter:
    log_format = "{asctime} {levelname:>8s} {message}"
    if os.getenv("AWS_DEFAULT_REGION"):
        log_format = "{levelname:>8s} {message}"
    return logging.Formatter(fmt=log_format, datefmt="%Y-%m-%dT%H:%M:%S%z", style="{")


LOGGER = logging.getLogger("odin_app.cubic_file_observation")
if not LOGGER.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(_log_formatter())
    LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)
# Lambda's root handler would otherwise duplicate and reformat each observation.
LOGGER.propagate = False


def _log_value(value: Any) -> str:
    if isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_./:+%-]+", value):
        return value
    if isinstance(value, (list, dict)):
        value = json.dumps(value, separators=(",", ":"))
    return json.dumps(value)


def _log_observation(observation: JsonObject, started_at: float) -> None:
    """Emit one ProcessLog-style completion line, without Odin runtime dependencies."""
    fields: JsonObject = {
        "uuid": str(uuid4()),
        "parent": "odin",
        "process": "cubic_file_observation",
        "odin_instance": os.getenv("ODIN_INSTANCE", "local"),
        "ecs_task_group": os.getenv("ECS_TASK_GROUP", "no_task_group"),
        "process_id": os.getpid(),
        "status": "failed" if observation["errors"] else "complete",
        "duration": f"{time.monotonic() - started_at:.2f}",
    }
    if observation["errors"]:
        fields["error_type"] = observation["errors"][0]["type"]
    for name, value in observation.items():
        if name in ("source", "archive"):
            fields.update({f"{name}_{key}": item for key, item in value.items()})
        else:
            fields[name] = value
    message = ", ".join(f"{name}={_log_value(value)}" for name, value in fields.items())
    LOGGER.log(logging.ERROR if observation["errors"] else logging.INFO, message)


@dataclass(frozen=True)
class Configuration:
    """Bucket names and literal, slash-terminated S3 key prefixes."""

    incoming_bucket: str
    archive_bucket: str
    incoming_prefix: str
    archive_prefix: str

    @classmethod
    def from_environment(cls) -> "Configuration":
        """Read configuration without silently accepting missing bucket names."""
        incoming_bucket = os.environ["S3_BUCKET_INCOMING"]
        archive_bucket = os.environ["S3_BUCKET_ARCHIVE"]
        if not incoming_bucket or not archive_bucket:
            raise ValueError("S3_BUCKET_INCOMING and S3_BUCKET_ARCHIVE must be nonempty")

        incoming_prefix = os.getenv("S3_BUCKET_PREFIX_INCOMING", "")
        archive_prefix = os.getenv("S3_BUCKET_PREFIX_ARCHIVE", "")
        for name, prefix in (
            ("S3_BUCKET_PREFIX_INCOMING", incoming_prefix),
            ("S3_BUCKET_PREFIX_ARCHIVE", archive_prefix),
        ):
            if prefix and not prefix.endswith("/"):
                raise ValueError(f"{name} must be empty or end with '/'")
        return cls(incoming_bucket, archive_bucket, incoming_prefix, archive_prefix)


class ObservationError(RuntimeError):
    """At least one event record could not be fully observed."""


def _error_details(exception: Exception, operation: str, **location: str) -> JsonObject:
    details: JsonObject = {
        "operation": operation,
        **location,
        "type": type(exception).__name__,
        "message": str(exception),
    }
    if isinstance(exception, ClientError):
        details["code"] = exception.response.get("Error", {}).get("Code")
        metadata = exception.response.get("ResponseMetadata", {})
        details["http_status"] = metadata.get("HTTPStatusCode")
        details["aws_request_id"] = metadata.get("RequestId")
    return details


def _head_object(
    client: Any, bucket: str, key: str, operation: str, errors: list[JsonObject]
) -> JsonObject:
    try:
        response = client.head_object(Bucket=bucket, Key=key)
        return {
            "status": "present",
            "size_bytes": response["ContentLength"],
            "last_modified": response["LastModified"].isoformat(),
            "etag": response.get("ETag"),
            "version_id": response.get("VersionId"),
        }
    except ClientError as exception:
        if exception.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return {"status": "missing"}
        errors.append(_error_details(exception, operation, bucket=bucket, key=key))
    except BotoCoreError as exception:
        errors.append(_error_details(exception, operation, bucket=bucket, key=key))
    return {"status": "error"}


def _snapshot_directories(
    client: Any, bucket: str, prefix: str, cache: PartitionCache
) -> tuple[list[str], list[JsonObject]]:
    if prefix not in cache:
        directories: list[str] = []
        errors: list[JsonObject] = []
        try:
            pages = client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix, Delimiter="/"
            )
            for page in pages:
                for entry in page.get("CommonPrefixes", []):
                    directory = entry["Prefix"]
                    if directory.startswith(prefix) and SNAPSHOT_DIRECTORY.fullmatch(
                        directory[len(prefix) :]
                    ):
                        directories.append(directory)
        except (ClientError, BotoCoreError) as exception:
            errors.append(_error_details(exception, "archive_list", bucket=bucket, prefix=prefix))
        cache[prefix] = (sorted(set(directories)), errors)
    return cache[prefix]


def _observe_archive(
    client: Any,
    configuration: Configuration,
    directory: str,
    filename: str,
    cache: PartitionCache,
    observation: JsonObject,
) -> None:
    archive = observation["archive"]
    archive.update(
        status="error",
        bucket=configuration.archive_bucket,
        match_basis="directory_and_basename_only",
        searched_prefixes=[],
    )
    errors = observation["errors"]
    for relocation in ARCHIVE_RELOCATIONS:
        prefix = f"{relocation}{configuration.archive_prefix}{directory}/"
        archive["searched_prefixes"].append(prefix)
        directories, discovery_errors = _snapshot_directories(
            client, configuration.archive_bucket, prefix, cache
        )
        errors.extend(discovery_errors)
        for snapshot_directory in directories:
            key = snapshot_directory + filename
            metadata = _head_object(
                client, configuration.archive_bucket, key, "archive_head", errors
            )
            if metadata["status"] == "present":
                archive["candidates"].append(
                    {"bucket": configuration.archive_bucket, "key": key, **metadata}
                )
    if errors:
        archive["status"] = "error"
    elif archive["candidates"]:
        archive["status"] = "candidates_found"
    else:
        archive["status"] = "no_candidates"


def _record_identity(record: JsonObject) -> JsonObject:
    s3_record = record["s3"]
    bucket = s3_record["bucket"]["name"]
    encoded_key = s3_record["object"]["key"]
    if not isinstance(bucket, str) or not bucket:
        raise ValueError("Record must contain a nonempty S3 bucket name")
    if not isinstance(encoded_key, str) or not encoded_key:
        raise ValueError("Record must contain a nonempty S3 object key")
    return {
        "bucket": bucket,
        "key": unquote_plus(encoded_key),
        "event_time": record.get("eventTime"),
        "event_name": record.get("eventName"),
        "event_source": record.get("eventSource"),
        "event_version": record.get("eventVersion"),
        "aws_region": record.get("awsRegion"),
        "event_request_id": record.get("responseElements", {}).get("x-amz-request-id"),
        "event_size_bytes": s3_record["object"].get("size"),
        "event_etag": s3_record["object"].get("eTag"),
        "event_version_id": s3_record["object"].get("versionId"),
        "event_sequencer": s3_record["object"].get("sequencer"),
    }


def _observe_record(
    client: Any,
    configuration: Configuration,
    cache: PartitionCache,
    observation: JsonObject,
) -> None:
    bucket, key = observation["bucket"], observation["key"]
    if bucket != configuration.incoming_bucket:
        observation["path_status"] = "unexpected_bucket"
        observation["archive"]["status"] = "unsupported_path"
        return

    match = None
    if key.startswith(configuration.incoming_prefix):
        match = QLIK_PATH.fullmatch(key[len(configuration.incoming_prefix) :])
    observation["path_status"] = "supported" if match else "unsupported_path"
    observation["source"] = _head_object(client, bucket, key, "source_head", observation["errors"])
    source_status = observation["source"]["status"]
    if source_status == "present":
        observation["archive"]["status"] = "not_needed"
    elif source_status == "error":
        observation["archive"]["status"] = "source_error"
    elif match is None:
        observation["archive"]["status"] = "unsupported_path"
    else:
        _observe_archive(client, configuration, match[1], match[2], cache, observation)


def lambda_handler(event: JsonObject, context: Any) -> JsonObject:
    """
    Emit one ProcessLog-style line per S3 event record; fail after logging all errors.

    HEAD describes the current object, not necessarily the revision that emitted the event.
    S3 LastModified is not an original creation timestamp.
    """
    records = event["Records"]
    if not isinstance(records, list):
        raise ValueError("Expected a direct S3 event with a Records list")
    request_id = getattr(context, "aws_request_id", None) or f"local-{uuid4()}"
    if not records:
        return {"request_id": request_id, "observed_records": 0}

    initialization_error = None
    configuration = None
    client = None
    try:
        configuration = Configuration.from_environment()
        client = boto3.client("s3")
    except Exception as exception:
        initialization_error = _error_details(exception, "initialize")

    cache: PartitionCache = {}
    failed_records = 0
    for index, record in enumerate(records):
        started_at = time.monotonic()
        observation: JsonObject = {
            "type": "cubic_file_observation",
            "request_id": request_id,
            "record_index": index,
            "record_count": len(records),
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "source": {"status": "not_checked"},
            "archive": {"status": "not_checked", "candidates": []},
            "errors": [],
        }
        try:
            observation.update(_record_identity(record))
            if initialization_error is not None:
                observation["errors"].append(initialization_error)
            else:
                assert configuration is not None
                _observe_record(client, configuration, cache, observation)
        except Exception as exception:
            # Keep unexpected failures on this record's single line, then fail the batch below.
            observation["errors"].append(_error_details(exception, "observe_record"))
        if observation["errors"]:
            failed_records += 1
        _log_observation(observation, started_at)

    if failed_records:
        raise ObservationError(
            f"{failed_records}/{len(records)} records failed; see request_id={request_id}"
        )
    return {"request_id": request_id, "observed_records": len(records)}
