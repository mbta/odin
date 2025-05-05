import boto3
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber
import datetime
import gzip
import io
import json
import os
import polars as pl
from polars.testing import assert_frame_equal
import pytest
from unittest.mock import patch

import odin.ingestion.spare.spare_job as spare_job


@pytest.fixture
def s3_stub():
    s3_client = boto3.client("s3")
    with Stubber(s3_client) as stubber:
        with patch("odin.utils.aws.s3.get_client", return_value=s3_client):
            yield stubber
        stubber.assert_no_pending_responses()


api_endpoint: spare_job.ApiEndpoint = {
    "name": "testname",
    "api_path": "testpath",
    "columns": {
        "id": pl.String(),
        "value": pl.Int32(),
    },
}


def write_gzipped_json(data, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(gzip.compress(json.dumps(data).encode()))


# load_parquet

empty_df = pl.DataFrame(
    {},
    spare_job.METADATA_COLUMNS | api_endpoint["columns"],
)
one_row_df = pl.DataFrame(
    {
        "ROW_TIMESTAMP": [1745582400000],
        "ROW_DELETED": [False],
        "id": ["id"],
        "value": [10],
    },
    spare_job.METADATA_COLUMNS | api_endpoint["columns"],
)


def test_load_parquet_s3_exists(s3_stub, tmpdir):
    localpath = os.path.join(tmpdir, "test.parquet")
    one_row_df.write_parquet(localpath)
    with open(localpath, "rb") as f:
        bytes = f.read()
    s3_stub.add_response(
        "head_object",
        expected_params={
            "Bucket": "bucket",
            "Key": "test.parquet",
        },
        service_response={},
    )
    s3_stub.add_response(
        "get_object",
        expected_params={"Bucket": "bucket", "Key": "test.parquet"},
        service_response={"Body": StreamingBody(io.BytesIO(bytes), len(bytes))},
    )
    df = spare_job.load_parquet("s3://bucket/test.parquet", api_endpoint["columns"])
    assert_frame_equal(df, one_row_df)


def test_load_parquet_s3_not_exists(s3_stub):
    s3_stub.add_client_error(
        "head_object",
        expected_params={
            "Bucket": "target",
            "Key": "testname.parquet",
        },
        service_error_code="404",
    )
    df = spare_job.load_parquet("s3://target/testname.parquet", api_endpoint["columns"])
    assert_frame_equal(df, empty_df)


def test_load_parquet_local_exists(tmpdir):
    path = os.path.join(tmpdir, "test.parquet")
    one_row_df.write_parquet(path)
    df = spare_job.load_parquet(path, api_endpoint["columns"])
    assert_frame_equal(df, one_row_df)


def test_load_parquet_local_not_exists(tmpdir):
    path = os.path.join(tmpdir, "test.parquet")
    df = spare_job.load_parquet(path, api_endpoint["columns"])
    assert_frame_equal(df, empty_df)


# datetime_from_delta_path_ms


def test_datetime_from_delta_path_ms():
    assert (
        spare_job.datetime_from_delta_path_ms(
            "2025/04/24/2025-04-24T18:55:49Z_https_example.com_path.gz"
        )
        == 1745520949000
    )


def test_datetime_from_delta_path_ms_raises():
    """Raises if given bad input"""
    with pytest.raises(Exception):
        spare_job.datetime_from_delta_path_ms("doesn't have timestamp")


# run


def test_run_s3_makes_new_output(s3_stub, tmpdir):
    """
    Processes a file, and writes a new parquet file for it.

    Hooked up to a mock s3, with gzipped files.
    """
    data = {"data": [{"id": "id", "value": 10}]}
    compressed = gzip.compress(json.dumps(data).encode())
    # list inputs
    s3_stub.add_response(
        "list_objects_v2",
        expected_params={
            "Bucket": "source",
            "Prefix": "",
        },
        service_response={
            "IsTruncated": False,
            "KeyCount": 1,
            "Contents": [
                {
                    "Key": "2025/04/25/2025-04-25T12:00:00Z_testpath.gz",
                    "LastModified": datetime.datetime(2025, 4, 25, 12, 0, 0),
                    "Size": len(compressed),
                },
            ],
        },
    )
    # checks if output exists (it doesn't, so no get_object call for it)
    s3_stub.add_client_error(
        "head_object",
        expected_params={
            "Bucket": "target",
            "Key": "testname.parquet",
        },
        service_error_code="404",
    )
    # get inputs
    s3_stub.add_response(
        "get_object",
        expected_params={
            "Bucket": "source",
            "Key": "2025/04/25/2025-04-25T12:00:00Z_testpath.gz",
        },
        service_response={"Body": StreamingBody(io.BytesIO(compressed), len(compressed))},
    )
    # write output
    s3_stub.add_response(
        "put_object",
        expected_params={
            "Bucket": "target",
            "Key": "testname.parquet",
            "Body": ANY,
            "ChecksumAlgorithm": ANY,
        },
        service_response={},
    )
    # archive input (step 1, copy)
    s3_stub.add_response(
        "copy_object",
        expected_params={
            "Bucket": "archive",
            "CopySource": "source/2025/04/25/2025-04-25T12:00:00Z_testpath.gz",
            "Key": "2025/04/25/2025-04-25T12:00:00Z_testpath.gz",
        },
        service_response={},
    )
    # archive input (step 2, delete)
    s3_stub.add_response(
        "delete_object",
        expected_params={
            "Bucket": "source",
            "Key": "2025/04/25/2025-04-25T12:00:00Z_testpath.gz",
        },
        service_response={},
    )

    spare_job.run(
        {
            "source": "s3://source/",
            "target": "s3://target/",
            "archive": "s3://archive/",
        },
        api_endpoint,
        tmpdir,
    )

    assert_frame_equal(
        spare_job.load_parquet(
            os.path.join(tmpdir, "target/testname.parquet"), api_endpoint["columns"]
        ),
        pl.DataFrame(
            {
                "ROW_TIMESTAMP": [1745582400000],
                "ROW_DELETED": [False],
                "id": ["id"],
                "value": [10],
            },
            spare_job.METADATA_COLUMNS | api_endpoint["columns"],
        ),
    )


def test_run_appends(tmpdir):
    """If a parquet file already exists at the destination, append data to it."""
    # Make existing parquet file
    pl.DataFrame(
        {
            "ROW_TIMESTAMP": [1745582400000],
            "ROW_DELETED": [False],
            "id": "id1",
            "value": [10],
        },
        spare_job.METADATA_COLUMNS | api_endpoint["columns"],
    ).write_parquet(os.path.join(tmpdir, "testname.parquet"))
    # Make new input data
    write_gzipped_json(
        {"data": [{"id": "id2", "value": 20}]},
        os.path.join(tmpdir, "2025/04/25/2025-04-25T12:00:01Z_testpath.gz"),
    )
    time0 = 1745582400000
    time1 = 1745582401000

    spare_job.run(
        {
            "source": tmpdir.strpath,
            "target": tmpdir.strpath,
        },
        api_endpoint,
        tmpdir,
    )

    assert_frame_equal(
        spare_job.load_parquet(os.path.join(tmpdir, "testname.parquet"), api_endpoint["columns"]),
        pl.DataFrame(
            [
                {"ROW_TIMESTAMP": time0, "ROW_DELETED": False, "id": "id1", "value": 10},
                # deletion because new data doesn't include the id from the existing data
                {"ROW_TIMESTAMP": time1, "ROW_DELETED": True, "id": "id1", "value": 10},
                {"ROW_TIMESTAMP": time1, "ROW_DELETED": False, "id": "id2", "value": 20},
            ],
            spare_job.METADATA_COLUMNS | api_endpoint["columns"],
        ),
    )

    # archive is not in config, so input file isn't archived
    assert os.path.exists(os.path.join(tmpdir, "2025/04/25/2025-04-25T12:00:01Z_testpath.gz"))


def test_run_merges(tmpdir):
    """
    Handles additions, deletions, and edits correctly

    Also tests that it can process multiple files at once
    And tests idempotency from reprocessing files
    """
    write_gzipped_json(
        {
            "data": [
                {"id": "changed", "value": 10},
                {"id": "unchanged", "value": 10},
                {"id": "deleted", "value": 10},
            ]
        },
        os.path.join(tmpdir, "2025/04/25/2025-04-25T12:00:00Z_testpath.gz"),
    )
    write_gzipped_json(
        {
            "data": [
                {"id": "changed", "value": 20},
                {"id": "unchanged", "value": 10},
                {"id": "added", "value": 10},
            ]
        },
        os.path.join(tmpdir, "2025/04/25/2025-04-25T12:00:01Z_testpath.gz"),
    )
    time0 = 1745582400000
    time1 = 1745582401000

    spare_job.run(
        {
            "source": tmpdir.strpath,
            "target": tmpdir.strpath,
        },
        api_endpoint,
        tmpdir,
    )

    expected_df = pl.DataFrame(
        [
            {"ROW_TIMESTAMP": time0, "ROW_DELETED": False, "id": "changed", "value": 10},
            {"ROW_TIMESTAMP": time0, "ROW_DELETED": False, "id": "unchanged", "value": 10},
            {"ROW_TIMESTAMP": time0, "ROW_DELETED": False, "id": "deleted", "value": 10},
            # note this row is a deletion
            {"ROW_TIMESTAMP": time1, "ROW_DELETED": True, "id": "deleted", "value": 10},
            {"ROW_TIMESTAMP": time1, "ROW_DELETED": False, "id": "changed", "value": 20},
            {"ROW_TIMESTAMP": time1, "ROW_DELETED": False, "id": "added", "value": 10},
        ],
        spare_job.METADATA_COLUMNS | api_endpoint["columns"],
    )
    assert_frame_equal(
        spare_job.load_parquet(os.path.join(tmpdir, "testname.parquet"), api_endpoint["columns"]),
        expected_df,
    )

    # test idempotency by running again, asserting output is the same
    # previous run didn't archive, so input files still exist
    spare_job.run(
        {
            "source": tmpdir.strpath,
            "target": tmpdir.strpath,
        },
        api_endpoint,
        tmpdir,
    )
    assert_frame_equal(
        spare_job.load_parquet(os.path.join(tmpdir, "testname.parquet"), api_endpoint["columns"]),
        expected_df,
    )


def test_run_no_changes(tmpdir):
    """Doesn't write parquet file if there are no changes to it"""
    # write the parquet file by running the job
    data = {"data": [{"id": "id", "value": 20}]}
    write_gzipped_json(
        data,
        os.path.join(tmpdir, "2025/04/25/2025-04-25T12:00:01Z_testpath.gz"),
    )
    spare_job.run(
        {
            "source": tmpdir.strpath,
            "target": tmpdir.strpath,
        },
        api_endpoint,
        tmpdir,
    )

    path = os.path.join(tmpdir, "testname.parquet")
    before_file_timestamp = os.path.getmtime(path)

    # run the job again with the same input at a newer timestamp
    write_gzipped_json(
        data,
        os.path.join(tmpdir, "2025/04/25/2025-04-25T12:00:02Z_testpath.gz"),
    )
    spare_job.run(
        {
            "source": tmpdir.strpath,
            "target": tmpdir.strpath,
            "archive": os.path.join(tmpdir, "archive"),
        },
        api_endpoint,
        tmpdir,
    )

    # check that the file wasn't rewritten
    after_file_timestamp = os.path.getmtime(path)
    assert after_file_timestamp == before_file_timestamp
    # but the input was still archived
    assert not os.path.exists(os.path.join(tmpdir, "2025/04/25/2025-04-25T12:00:02Z_testpath.gz"))
    assert os.path.exists(
        os.path.join(tmpdir, "archive/2025/04/25/2025-04-25T12:00:02Z_testpath.gz")
    )
