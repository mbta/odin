import boto3
from botocore.response import StreamingBody
from botocore.stub import Stubber
import datetime
import gzip
import io
import os
import pytest
from unittest.mock import patch

import odin.ingestion.spare.spare_s3 as spare_s3


@pytest.fixture
def s3_stub():
    """Mock S3 client for mocking aws calls. Patches s3.get_client()."""
    s3_client = boto3.client("s3")
    with Stubber(s3_client) as stubber:
        with patch("odin.utils.aws.s3.get_client", return_value=s3_client):
            yield stubber
        stubber.assert_no_pending_responses()


# open_file


def test_open_file_s3_gz(s3_stub):
    """open_file() for a gzip file in s3"""
    body = b"body"
    gz_bytes = gzip.compress(body)
    s3_stub.add_response(
        "get_object",
        expected_params={
            "Bucket": "my-bucket",
            "Key": "foo.gz",
        },
        service_response={"Body": StreamingBody(io.BytesIO(gz_bytes), len(gz_bytes))},
    )
    with s3_stub:
        with spare_s3.open_file("s3://my-bucket/foo.gz") as f:
            result = f.read()
    assert result == body


def test_open_file_s3_uncompressed(s3_stub):
    """open_file() for an uncompressed file in s3"""
    body = b"body"
    s3_stub.add_response(
        "get_object",
        expected_params={
            "Bucket": "my-bucket",
            "Key": "foo.txt",
        },
        service_response={"Body": StreamingBody(io.BytesIO(body), len(body))},
    )
    with s3_stub:
        with spare_s3.open_file("s3://my-bucket/foo.txt") as f:
            result = f.read()
    assert result == body


def test_open_file_local_gz(tmpdir):
    """open_file() for an gzipped local file"""
    body = b"body"
    gz_bytes = gzip.compress(body)
    tmpdir.join("foo.gz").write_binary(gz_bytes)
    with spare_s3.open_file(tmpdir.join("foo.gz").strpath) as f:
        result = f.read()
    assert result == body


def test_open_file_local_uncompressed(tmpdir):
    """open_file() for an uncompressed local file"""
    tmpdir.join("file.txt").write("body")
    with spare_s3.open_file(tmpdir.join("file.txt").strpath) as f:
        result = f.read()
    assert result == "body"


# list_objects


def test_list_objects_s3(s3_stub):
    """list_objects() for s3"""
    s3_stub.add_response(
        "list_objects_v2",
        expected_params={
            "Bucket": "my-bucket",
            "Prefix": "prefix1/",
        },
        service_response={
            "IsTruncated": False,
            "KeyCount": 2,
            "Contents": [
                {
                    "Key": "prefix1/prefix2/file1",
                    "LastModified": datetime.datetime(2016, 1, 20, 22, 9),
                    "Size": 1231,
                },
                {
                    "Key": "prefix1/prefix2/file2",
                    "LastModified": datetime.datetime(2016, 1, 20, 22, 9),
                    "Size": 43212,
                },
            ],
        },
    )
    with s3_stub:
        result = spare_s3.list_objects("s3://my-bucket/prefix1/")
    assert result == ["prefix2/file1", "prefix2/file2"]


def test_list_objects_local(tmpdir):
    """list_objects() for a local directory"""
    tmpdir.mkdir("empty_sub")
    tmpdir.join("foo.json").write("[]")
    tmpdir.mkdir("sub").join("bar.json").write("[]")
    result = spare_s3.list_objects(tmpdir.strpath)
    assert result == ["foo.json", "sub/bar.json"]


# rename_object


def test_rename_object_s3(s3_stub):
    """rename_object() in s3 (copy and delete)"""
    s3_stub.add_response(
        "copy_object",
        expected_params={
            "Bucket": "bucket2",
            "CopySource": "bucket1/path1/file1",
            "Key": "path2/file2",
        },
        service_response={},
    )
    s3_stub.add_response(
        "delete_object",
        expected_params={
            "Bucket": "bucket1",
            "Key": "path1/file1",
        },
        service_response={},
    )
    spare_s3.rename_object("s3://bucket1/path1/file1", "s3://bucket2/path2/file2")


def test_rename_object_local(tmpdir):
    """rename_object() for a local file (os mv)"""
    # use nested path so it tests creating the directory
    tmpdir.mkdir("from")
    tmpdir.join("from/file1").write("contents")
    spare_s3.rename_object(
        os.path.join(tmpdir, "from/file1"),
        os.path.join(tmpdir, "to/file2"),
    )
    assert not os.path.exists(os.path.join(tmpdir, "from/file1"))
    assert os.path.exists(os.path.join(tmpdir, "to/file2"))


def test_rename_object_mixed():
    """rename_object() raises if given bad input"""
    with pytest.raises(Exception):
        spare_s3.rename_object("s3://bucket/path", "local/path")
