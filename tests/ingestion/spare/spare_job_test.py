import boto3
from botocore.response import StreamingBody
from botocore.stub import Stubber
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
    """Mock S3 client for mocking aws calls. Patches s3.get_client()."""
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
    """Make a gzip file with test data."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(gzip.compress(json.dumps(data).encode()))


# run


def test_run_s3(s3_stub, tmpdir):
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
            "KeyCount": 2,
            "Contents": [
                {
                    "Key": "2025/04/25/2025-04-25T11:59:00Z_testpath.gz",
                    "LastModified": datetime.datetime(2025, 4, 25, 11, 59, 0),
                    "Size": len(compressed),
                },
                {
                    "Key": "2025/04/25/2025-04-25T12:00:00Z_testpath.gz",
                    "LastModified": datetime.datetime(2025, 4, 25, 12, 0, 0),
                    "Size": len(compressed),
                },
            ],
        },
    )
    # get input (only for later file, not earlier file)
    s3_stub.add_response(
        "get_object",
        expected_params={
            "Bucket": "source",
            "Key": "2025/04/25/2025-04-25T12:00:00Z_testpath.gz",
        },
        service_response={"Body": StreamingBody(io.BytesIO(compressed), len(compressed))},
    )
    # no s3 mock for writing output because that's covered by the write_parquet mock below
    # archive inputs (copy, then delete, for both files)
    s3_stub.add_response(
        "copy_object",
        expected_params={
            "Bucket": "archive",
            "CopySource": "source/2025/04/25/2025-04-25T11:59:00Z_testpath.gz",
            "Key": "2025/04/25/2025-04-25T11:59:00Z_testpath.gz",
        },
        service_response={},
    )
    s3_stub.add_response(
        "delete_object",
        expected_params={
            "Bucket": "source",
            "Key": "2025/04/25/2025-04-25T11:59:00Z_testpath.gz",
        },
        service_response={},
    )
    s3_stub.add_response(
        "copy_object",
        expected_params={
            "Bucket": "archive",
            "CopySource": "source/2025/04/25/2025-04-25T12:00:00Z_testpath.gz",
            "Key": "2025/04/25/2025-04-25T12:00:00Z_testpath.gz",
        },
        service_response={},
    )
    s3_stub.add_response(
        "delete_object",
        expected_params={
            "Bucket": "source",
            "Key": "2025/04/25/2025-04-25T12:00:00Z_testpath.gz",
        },
        service_response={},
    )

    # mock polars.write_parquet so it doesn't actually write to s3
    with patch.object(pl.DataFrame, "write_parquet") as polars_write_parquet:
        # that mock can't get the dataframe written by df.write_parquet()
        # (because the mock only allows seeing the method args, not the instance/self)
        # but we need it to assert we write the right output data at the end
        # so spy on spare_job.write_parquet to get the df from the arg to that function call
        with patch(
            "odin.ingestion.spare.spare_job.write_parquet", wraps=spare_job.write_parquet
        ) as spare_job_write_parquet:
            spare_job.run(
                {
                    "source": "s3://source/",
                    "target": "s3://target/",
                    "archive": "s3://archive/",
                },
                api_endpoint,
            )
            polars_write_parquet.assert_called_once_with("s3://target/testname.parquet")
            written_df = spare_job_write_parquet.call_args[0][0]

    expected_df = pl.DataFrame(
        {
            "id": ["id"],
            "value": [10],
        },
        api_endpoint["columns"],
    )
    assert_frame_equal(written_df, expected_df)


def test_run_s3_no_inputs(s3_stub):
    """If there are no new input files, does nothing."""
    s3_stub.add_response(
        "list_objects_v2",
        expected_params={
            "Bucket": "source",
            "Prefix": "",
        },
        service_response={
            "IsTruncated": False,
            "KeyCount": 0,
            "Contents": [],
        },
    )
    # list_objects is the only s3 call, the job doesn't call any other reads or writes.
    spare_job.run(
        {
            "source": "s3://source/",
            "target": "s3://target/",
        },
        api_endpoint,
    )
