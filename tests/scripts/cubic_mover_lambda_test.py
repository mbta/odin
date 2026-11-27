"""Test the standalone mover without AWS credentials or network requests."""

import copy
import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock
from urllib.parse import quote_plus

import boto3
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "cubic_mover_lambda.py"
SPEC = importlib.util.spec_from_file_location("cubic_mover_lambda", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
mover = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mover
SPEC.loader.exec_module(mover)

START = datetime(2026, 10, 1, 12, tzinfo=UTC)
ODS = "cubic/ods_qlik/EDW.SAMPLE/"
CDC = "cubic/ods_qlik/EDW.SAMPLE__ct/"
FIRST = ODS + "LOAD00000001.csv.gz"
PARTITION = "snapshot=20261001T120000Z/"
DESTINATION = ODS + PARTITION + "LOAD00000001.csv.gz"


def s3_error(code):
    """Use the real SDK exception shape for simulated failures."""
    return ClientError({"Error": {"Code": code, "Message": code}}, "S3Operation")


class MemoryS3:
    """Model source identity and mutations separately from real-SDK request tests."""

    def __init__(self):
        """Start with no stored objects or operations."""
        self.objects = {}
        self.operations = []
        self.fail_copy = None
        self.fail_delete = None

    def put(self, bucket, key, modified=START, **kwargs):
        """Store a synthetic HEAD response."""
        self.objects[bucket, key] = {
            "LastModified": modified,
            "ETag": '"original"',
            "ContentLength": 123,
            "Metadata": {"vendor": "cubic", "cubic-mover-event": "old-receipt"},
            "ContentEncoding": "gzip",
            **kwargs,
        }

    def head_object(self, *, Bucket, Key):  # noqa: N803
        """Distinguish a genuine 404 from other API failures."""
        if (Bucket, Key) not in self.objects:
            raise s3_error("404")
        return copy.deepcopy(self.objects[Bucket, Key])

    def get_paginator(self, name):
        """Return a one-prefix-per-page paginator to exercise pagination."""
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket, Prefix, Delimiter):  # noqa: N803
        """List immediate directories only, matching S3 delimiter semantics."""
        assert Delimiter == "/"
        prefixes = set()
        for bucket, key in self.objects:
            if bucket == Bucket and key.startswith(Prefix):
                remainder = key.removeprefix(Prefix)
                if "/" in remainder:
                    prefixes.add(Prefix + remainder.split("/", 1)[0] + "/")
        for prefix in sorted(prefixes):
            yield {"CommonPrefixes": [{"Prefix": prefix}]}

    def copy(self, source, bucket, key, *, ExtraArgs, Config):  # noqa: N803
        """Enforce copy conditions and deliberately drop source metadata and headers."""
        if key == self.fail_copy:
            raise s3_error("InternalError")
        original = self.objects[source["Bucket"], source["Key"]]
        if ExtraArgs["CopySourceIfMatch"] != original["ETag"]:
            raise s3_error("PreconditionFailed")
        assert source.get("VersionId") == original.get("VersionId")
        assert ExtraArgs == {
            "CopySourceIfMatch": original["ETag"],
            "MetadataDirective": "REPLACE",
            "Metadata": {},
        }
        assert Config.multipart_threshold == 64 * 1024 * 1024
        self.objects[bucket, key] = {
            "LastModified": START + timedelta(hours=1),
            "ContentLength": original["ContentLength"],
            "ETag": '"copied-2"',
            "Metadata": {},
        }
        self.operations.append(("copy", key))

    def delete_object(self, *, Bucket, Key, IfMatch, **kwargs):  # noqa: N803
        """Require the inspected version and ETag before deletion."""
        if Key == self.fail_delete:
            raise s3_error("AccessDenied")
        source = self.objects[Bucket, Key]
        if IfMatch != source["ETag"]:
            raise s3_error("PreconditionFailed")
        assert kwargs.get("VersionId") == source.get("VersionId")
        del self.objects[Bucket, Key]
        self.operations.append(("delete", Key))


@pytest.fixture
def storage(monkeypatch):
    """Enable exactly one table and route all S3 calls to isolated memory."""
    client = MemoryS3()
    monkeypatch.setattr(mover, "_s3", client)
    monkeypatch.setattr(mover, "TABLE_PREFIXES", ["EDW.SAMPLE"])
    monkeypatch.setenv("S3_BUCKET_INCOMING", "incoming")
    monkeypatch.setenv("S3_BUCKET_ARCHIVE", "archive")
    monkeypatch.setenv("S3_BUCKET_PREFIX_INCOMING", "")
    monkeypatch.setenv("S3_BUCKET_PREFIX_ARCHIVE", "")
    return client


def put_pair(storage, key=FIRST, modified=START, **kwargs):
    """Stage a CSV and its companion DFM."""
    storage.put("incoming", key, modified, **kwargs)
    storage.put("incoming", key.removesuffix(".csv.gz") + ".dfm", modified, **kwargs)


def notification(key=FIRST, **object_fields):
    """Construct a real-shaped S3 event, without any receipt/sequencer dependency."""
    return {
        "eventSource": "aws:s3",
        "eventName": "ObjectCreated:Put",
        "eventTime": (START + timedelta(minutes=10)).isoformat(),
        "s3": {
            "bucket": {"name": "incoming"},
            "object": {
                "key": quote_plus(key),
                "eTag": "original",
                "size": 123,
                **object_fields,
            },
        },
    }


def invoke(*records):
    """Exercise the public Lambda entry point."""
    return mover.lambda_handler({"Records": list(records)}, None)


def test_move_pair_discards_metadata_and_deletes_only_after_both_copies(storage):
    """Copy bytes to the legacy layout without carrying vendor metadata or old receipts."""
    put_pair(storage)
    assert invoke(notification()) == {"archived": 1}
    assert storage.operations == [
        ("copy", DESTINATION),
        ("copy", DESTINATION.removesuffix(".csv.gz") + ".dfm"),
        ("delete", FIRST),
        ("delete", FIRST.removesuffix(".csv.gz") + ".dfm"),
    ]
    for object_head in storage.objects.values():
        assert object_head["Metadata"] == {}
        assert "ContentEncoding" not in object_head


@pytest.mark.parametrize("first_name", mover.FIRST_LOAD_NAMES)
def test_snapshot_uses_first_load_s3_time(storage, first_name):
    """Use UTC LastModified truncated to seconds, not notification time."""
    key = ODS + first_name
    put_pair(storage, key, START + timedelta(microseconds=987654))
    invoke(notification(key))
    assert ("archive", ODS + PARTITION + first_name) in storage.objects


@pytest.mark.parametrize("directory", [ODS, CDC])
@pytest.mark.parametrize("location", ["incoming", *mover.ARCHIVE_LOCATIONS])
def test_other_chunks_and_cdc_share_base_snapshot(storage, directory, location):
    """Support out-of-order notifications and every downstream relocation path."""
    if location == "incoming":
        storage.put("incoming", FIRST)
    else:
        storage.put("archive", location + DESTINATION)
    key = directory + "20261001-120100000.csv.gz"
    put_pair(storage, key, START + timedelta(minutes=1))
    invoke(notification(key))
    assert ("archive", directory + PARTITION + key.rsplit("/", 1)[1]) in storage.objects


def test_paginated_snapshots_select_latest_not_after_source(storage):
    """Do not route delayed events into a future snapshot."""
    for partition in ("snapshot=20260901T120000Z/", PARTITION, "snapshot=20261101T120000Z/"):
        storage.put("archive", ODS + partition + "LOAD00000001.csv.gz")
    key = CDC + "20261001-120100000.csv.gz"
    put_pair(storage, key, START + timedelta(minutes=1))
    invoke(notification(key))
    assert ("archive", CDC + PARTITION + key.rsplit("/", 1)[1]) in storage.objects


@pytest.mark.parametrize("partition", [None, "snapshot=20261101T120000Z/", "snapshot=invalid/"])
def test_missing_future_or_invalid_snapshot_fails_without_mutation(storage, partition):
    """Missing or malformed snapshot state must not be guessed."""
    if partition:
        storage.put("archive", ODS + partition + "LOAD00000001.csv.gz")
    key = CDC + "20261001-120100000.csv.gz"
    put_pair(storage, key)
    with pytest.raises(ExceptionGroup, match="manual reconciliation"):
        invoke(notification(key))
    assert storage.operations == []


@pytest.mark.parametrize("suffix", [".csv.gz", ".dfm"])
@pytest.mark.parametrize("location", mover.ARCHIVE_LOCATIONS)
def test_any_existing_destination_requires_manual_reconciliation(storage, suffix, location):
    """Even identical bytes, or a single relocated companion, block the whole move."""
    put_pair(storage)
    storage.put("archive", location + DESTINATION.removesuffix(".csv.gz") + suffix)
    before = copy.deepcopy(storage.objects)
    with pytest.raises(ExceptionGroup, match="manual reconciliation"):
        invoke(notification())
    assert storage.objects == before
    assert storage.operations == []


@pytest.mark.parametrize("failed_step", ["csv_copy", "dfm_copy", "csv_delete", "dfm_delete"])
def test_failure_never_triggers_automatic_partial_move_recovery(storage, failed_step):
    """Retries refuse partial state instead of overwriting or completing cleanup."""
    put_pair(storage)
    if failed_step.endswith("copy"):
        storage.fail_copy = (
            DESTINATION if failed_step == "csv_copy" else DESTINATION.replace(".csv.gz", ".dfm")
        )
    else:
        storage.fail_delete = (
            FIRST if failed_step == "csv_delete" else FIRST.replace(".csv.gz", ".dfm")
        )
    with pytest.raises(ExceptionGroup):
        invoke(notification())

    if failed_step.endswith("copy"):
        assert not any(operation == "delete" for operation, _ in storage.operations)

    # A failure before any mutation can safely be retried after the request issue is fixed.
    storage.fail_copy = storage.fail_delete = None
    if failed_step == "csv_copy":
        assert invoke(notification()) == {"archived": 1}
    else:
        before = copy.deepcopy(storage.objects)
        operations = list(storage.operations)
        with pytest.raises(ExceptionGroup):
            invoke(notification())
        assert storage.objects == before
        assert storage.operations == operations


def test_duplicate_after_success_requires_manual_reconciliation(storage, caplog):
    """Without receipts, a missing source cannot be acknowledged as already archived."""
    put_pair(storage)
    invoke(notification())
    before = copy.deepcopy(storage.objects)
    with pytest.raises(ExceptionGroup):
        invoke(notification())
    assert storage.objects == before
    assert "manual reconciliation required" in caplog.text


@pytest.mark.parametrize("missing_key", [FIRST, FIRST.replace(".csv.gz", ".dfm")])
def test_missing_companion_preserves_all_existing_objects(storage, missing_key):
    """Preflight both sources before initiating any copy."""
    put_pair(storage)
    del storage.objects["incoming", missing_key]
    with pytest.raises(ExceptionGroup):
        invoke(notification())
    assert storage.operations == []


@pytest.mark.parametrize(
    "change",
    [
        {"ETag": '"new"'},
        {"ContentLength": 0},
        {"VersionId": "new"},
        {"LastModified": START + timedelta(days=1)},
    ],
)
def test_stale_or_empty_source_is_not_moved(storage, change):
    """Do not apply an old notification to a replacement source."""
    put_pair(storage)
    storage.objects["incoming", FIRST].update(change)
    with pytest.raises(ExceptionGroup):
        invoke(notification())
    assert storage.operations == []


def test_versioned_pair_pins_copy_and_delete_versions(storage):
    """Retain version and conditional-delete protection independently of receipts."""
    put_pair(storage, VersionId="version-1")
    assert invoke(notification(versionId="version-1")) == {"archived": 1}


def test_changed_source_during_copy_cannot_be_deleted(storage, monkeypatch):
    """A changed source ETag fails conditional deletion rather than losing new data."""
    put_pair(storage)
    original_copy = storage.copy

    def replace_after_copy(*args, **kwargs):
        original_copy(*args, **kwargs)
        storage.objects["incoming", FIRST]["ETag"] = '"replacement"'

    monkeypatch.setattr(storage, "copy", replace_after_copy)
    with pytest.raises(ExceptionGroup):
        invoke(notification())
    assert storage.objects["incoming", FIRST]["ETag"] == '"replacement"'


def test_verification_failure_preserves_sources(storage, monkeypatch):
    """A successful API call is not sufficient if the destination size is wrong."""
    put_pair(storage)
    original_copy = storage.copy

    def corrupt_size(source, bucket, key, **kwargs):
        original_copy(source, bucket, key, **kwargs)
        storage.objects[bucket, key]["ContentLength"] = 999

    monkeypatch.setattr(storage, "copy", corrupt_size)
    with pytest.raises(ExceptionGroup):
        invoke(notification())
    assert not any(operation == "delete" for operation, _ in storage.operations)


def test_table_list_ships_empty():
    """No tables are enabled by default."""
    assert mover.TABLE_PREFIXES == []


def test_empty_list_never_accesses_s3(storage, monkeypatch):
    """An empty list is valid, not a wildcard."""
    monkeypatch.setattr(mover, "TABLE_PREFIXES", [])
    client = Mock(spec=MemoryS3)
    monkeypatch.setattr(mover, "_s3", client)
    assert invoke(notification()) == {"ignored_table": 1}
    assert client.mock_calls == []


@pytest.mark.parametrize(
    "key",
    [
        "cubic/dmap/EDW.SAMPLE/daily.csv.gz",
        "cubic/ods_qlik/EDW.SAMPLE_OTHER/LOAD00000001.csv.gz",
        FIRST.replace(".csv.gz", ".dfm"),
        ODS,
    ],
)
def test_only_exact_qlik_table_names_are_eligible(storage, key):
    """Neither a similarly named table, DMAP path nor DFM notification starts a move."""
    storage.put("incoming", key)
    before = copy.deepcopy(storage.objects)
    invoke(notification(key))
    assert storage.operations == []
    assert storage.objects == before


@pytest.mark.parametrize("table", ["", ".", "..", " EDW.SAMPLE", "EDW.SAMPLE__ct", ODS, 123])
def test_invalid_table_names_fail_closed(storage, monkeypatch, table):
    """Require names only, with the __ct suffix inferred automatically."""
    monkeypatch.setattr(mover, "TABLE_PREFIXES", [table])
    with pytest.raises(ValueError):
        invoke(notification())
    assert storage.operations == []


def test_prefixes_and_url_decoding(storage, monkeypatch):
    """Strip only the configured prefix and decode keys once."""
    monkeypatch.setenv("S3_BUCKET_PREFIX_INCOMING", "test/incoming/")
    monkeypatch.setenv("S3_BUCKET_PREFIX_ARCHIVE", "test/archive/")
    key = "test/incoming/" + ODS + "a + %2F.csv.gz"
    storage.put("incoming", "test/incoming/" + FIRST)
    put_pair(storage, key)
    invoke(notification(key))
    assert ("archive", "test/archive/" + ODS + PARTITION + "a + %2F.csv.gz") in storage.objects


def test_failed_record_does_not_skip_later_records(storage):
    """Attempt all records, but fail the invocation when any record needs attention."""
    put_pair(storage)
    with pytest.raises(ExceptionGroup):
        invoke(notification(ODS + "missing.csv.gz"), notification())
    assert ("archive", DESTINATION) in storage.objects


@pytest.mark.parametrize("bucket", ["incoming", "archive"])
def test_permission_errors_are_not_absence(storage, monkeypatch, bucket):
    """Do not treat a 403 as permission to copy or delete."""
    put_pair(storage)
    original_head = storage.head_object

    def deny(**kwargs):
        if kwargs["Bucket"] == bucket:
            raise s3_error("403")
        return original_head(**kwargs)

    monkeypatch.setattr(storage, "head_object", deny)
    with pytest.raises(ExceptionGroup):
        invoke(notification())
    assert storage.operations == []


@pytest.mark.parametrize(
    "setting,value",
    [
        ("S3_BUCKET_INCOMING", ""),
        ("S3_BUCKET_ARCHIVE", "s3://archive"),
        ("S3_BUCKET_PREFIX_INCOMING", "missing-slash"),
        ("S3_BUCKET_PREFIX_ARCHIVE", "/leading/slash/"),
        ("S3_BUCKET_ARCHIVE", "incoming"),
    ],
)
def test_unsafe_configuration_fails(storage, monkeypatch, setting, value):
    """Reject malformed locations and overlapping same-bucket paths."""
    monkeypatch.setenv(setting, value)
    with pytest.raises(ValueError):
        invoke(notification())
    assert storage.operations == []


def test_event_boundaries(storage):
    """Reject wrong buckets/envelopes while acknowledging configuration and removal events."""
    record = notification()
    record["s3"]["bucket"]["name"] = "unexpected"
    with pytest.raises(ExceptionGroup):
        invoke(record)
    with pytest.raises(ValueError):
        mover.lambda_handler({"detail": record}, None)
    record["eventName"] = "ObjectRemoved:Delete"
    assert invoke(record) == {"ignored_event": 1}
    assert mover.lambda_handler({"Event": "s3:TestEvent"}, None) == {"test_event": 1}


@pytest.mark.parametrize("large", [False, True])
def test_real_sdk_copy_without_metadata(large):
    """Validate actual boto3 requests for single-copy and a simulated 6 GiB multipart copy."""
    client = boto3.client(
        "s3", region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test"
    )
    copy_source = {"Bucket": "incoming", "Key": FIRST, "VersionId": "version-1"}
    part_size = 3 * 1024**3
    size = 2 * part_size if large else 123
    config = mover.TransferConfig(
        multipart_threshold=part_size, multipart_chunksize=part_size, use_threads=False
    )
    extra_args = {"CopySourceIfMatch": '"source"', "MetadataDirective": "REPLACE", "Metadata": {}}
    target = {"Bucket": "archive", "Key": DESTINATION}
    with Stubber(client) as stubber:
        stubber.add_response(
            "head_object",
            {"ContentLength": size},
            {
                "Bucket": "incoming",
                "Key": FIRST,
                "VersionId": "version-1",
                "IfMatch": '"source"',
            },
        )
        if large:
            stubber.add_response(
                "create_multipart_upload", {"UploadId": "upload"}, {**target, "Metadata": {}}
            )
            for number in (1, 2):
                byte_range = f"bytes={(number - 1) * part_size}-{number * part_size - 1}"
                stubber.add_response(
                    "upload_part_copy",
                    {"CopyPartResult": {"ETag": f'"{number}"'}},
                    {
                        **target,
                        "UploadId": "upload",
                        "PartNumber": number,
                        "CopySource": copy_source,
                        "CopySourceIfMatch": '"source"',
                        "CopySourceRange": byte_range,
                    },
                )
            stubber.add_response(
                "complete_multipart_upload",
                {},
                {
                    **target,
                    "UploadId": "upload",
                    "MultipartUpload": {
                        "Parts": [
                            {"PartNumber": 1, "ETag": '"1"'},
                            {"PartNumber": 2, "ETag": '"2"'},
                        ]
                    },
                },
            )
        else:
            stubber.add_response(
                "copy_object",
                {"CopyObjectResult": {"ETag": '"copied"'}},
                {**target, "CopySource": copy_source, **extra_args},
            )
        client.copy(copy_source, "archive", DESTINATION, ExtraArgs=extra_args, Config=config)
        stubber.assert_no_pending_responses()
