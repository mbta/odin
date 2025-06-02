import os
import re
import shutil
from datetime import datetime
from typing import Generator
from unittest.mock import MagicMock
from unittest.mock import patch
from unittest.mock import call

import polars as pl
import pytest

from odin.ingestion.afc.afc_archive import ArchiveAFCAPI
from odin.ingestion.afc.afc_archive import make_pl_schema
from odin.ingestion.afc.afc_archive import verify_downloads
from odin.utils.aws.s3 import S3Object


@patch.object(ArchiveAFCAPI, "download_json")
@patch("odin.ingestion.afc.afc_archive.disk_free_pct")
def test_load_job_ids(disk_free: MagicMock, dl_csv: MagicMock):
    """Test load_job_ids method of ArchiveAFCAPI"""
    disk_free.return_value = 99
    job = ArchiveAFCAPI("test_table")
    test_schema = {
        "type": "static",
        "table_infos": [
            {"column_name": "col1", "data_type": "bigint"},
            {"column_name": "col2", "data_type": "integer"},
            {"column_name": "col3", "data_type": "varchar"},
            {"column_name": "col4", "data_type": "timestamp with time zone"},
            {"column_name": "col5", "data_type": "varchar"},
        ],
    }
    expected_schema = pl.Schema(
        {
            "col1": pl.Int64(),
            "col2": pl.Int32(),
            "col3": pl.String(),
            "col4": pl.String(),
            "col5": pl.String(),
        }
    )
    job.schema = make_pl_schema(test_schema)
    assert job.schema == expected_schema

    # Test jobId skipping
    job.pq_job_id = 1000
    job.job_ids = [
        {"jobId": 1, "dataCount": 1},
        {"jobId": 2, "dataCount": 1},
        {"jobId": 3, "dataCount": 1},
        {"jobId": 4, "dataCount": 1},
        {"jobId": 5, "dataCount": 1},
        {"jobId": 6, "dataCount": 1},
        {"jobId": 7, "dataCount": 1},
        {"jobId": 8, "dataCount": 1},
        {"jobId": 9, "dataCount": 1},
        {"jobId": 1000, "dataCount": 1},
        {"jobId": 1001, "dataCount": 1},
    ]
    job.load_job_ids()
    dl_csv.assert_called_once_with([{"jobId": 1001, "dataCount": 1}])
    dl_csv.reset_mock()

    # Test target_rows hit
    job.job_ids = [
        {"jobId": 1, "dataCount": 1},
        {"jobId": 1000, "dataCount": 1},
        {"jobId": 1001, "dataCount": 1_000_000},
        {"jobId": 1002, "dataCount": 500},
    ]
    job.load_job_ids()
    dl_csv.assert_has_calls(
        [
            call([{"jobId": 1001, "dataCount": 1_000_000}]),
            call([{"jobId": 1002, "dataCount": 500}]),
        ]
    )
    dl_csv.reset_mock()

    # Test combine jobIds
    job.job_ids = [
        {"jobId": 1, "dataCount": 1},
        {"jobId": 1000, "dataCount": 1},
        {"jobId": 1001, "dataCount": 500},
        {"jobId": 1002, "dataCount": 500},
    ]
    job.load_job_ids()
    dl_csv.assert_called_once_with(
        [
            {"jobId": 1001, "dataCount": 500},
            {"jobId": 1002, "dataCount": 500},
        ]
    )
    dl_csv.reset_mock()

    # Confirm job sorting
    job.job_ids = [
        {"jobId": 1002, "dataCount": 200},
        {"jobId": 1001, "dataCount": 500},
        {"jobId": 1004, "dataCount": 500},
        {"jobId": 1003, "dataCount": 300},
    ]
    job.load_job_ids()
    dl_csv.assert_called_once_with(
        [
            {"jobId": 1001, "dataCount": 500},
            {"jobId": 1002, "dataCount": 200},
            {"jobId": 1003, "dataCount": 300},
            {"jobId": 1004, "dataCount": 500},
        ]
    )
    dl_csv.reset_mock()

    # Test disk_free_pct hit
    disk_free.return_value = 50
    job.load_job_ids()
    dl_csv.assert_called_once_with([{"jobId": 1001, "dataCount": 500}])
    dl_csv.reset_mock()


@pytest.fixture(scope="module")
def csv_file(tmp_path_factory) -> Generator[str]:
    """Create temporary csv file for testing."""
    tmp_path = tmp_path_factory.mktemp("csv_verify_downloads", numbered=False)
    path = os.path.join(tmp_path, "1.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = [
        {"job_id": 1, "value": "sid_1"},
        {"job_id": 2, "value": "sid_2"},
        {"job_id": 2, "value": "sid_2"},
        {"job_id": 3, "value": "sid_3"},
        {"job_id": 3, "value": "sid_3"},
        {"job_id": 3, "value": "sid_3"},
    ]
    (pl.DataFrame(data).write_ndjson(path))

    yield str(tmp_path)
    shutil.rmtree(tmp_path)


def test_verify_downloads(csv_file):
    """Test verify_downloads function of AFC archive process."""
    csv_schema = pl.Schema({"job_id": pl.Int64(), "value": pl.String()})

    download_jobs = [
        {"jobId": 1, "dataCount": 1},
        {"jobId": 2, "dataCount": 2},
        {"jobId": 3, "dataCount": 3},
    ]
    verify_downloads(csv_file, csv_schema, download_jobs)

    download_jobs = [
        {"jobId": 1, "dataCount": 1},
        {"jobId": 2, "dataCount": 2},
    ]
    assert_re = re.escape("job_id(s) from `stagetable` not in `count` endpoint:(3)")
    with pytest.raises(AssertionError, match=assert_re):
        verify_downloads(csv_file, csv_schema, download_jobs)

    download_jobs = [
        {"jobId": 1, "dataCount": 1},
        {"jobId": 2, "dataCount": 2},
        {"jobId": 3, "dataCount": 3},
        {"jobId": 4, "dataCount": 4},
    ]
    assert_re = re.escape("job_id(s) from `count` not in `stagetable` endpoint:(4)")
    with pytest.raises(AssertionError, match=assert_re):
        verify_downloads(csv_file, csv_schema, download_jobs)

    download_jobs = [
        {"jobId": 1, "dataCount": 10},
        {"jobId": 2, "dataCount": 2},
        {"jobId": 3, "dataCount": 3},
    ]
    assert_re = re.escape("record counts from `count` and `stagetable` not equal:(job_id 1: 10!=1)")
    with pytest.raises(AssertionError, match=assert_re):
        verify_downloads(csv_file, csv_schema, download_jobs)


@patch("odin.ingestion.afc.afc_archive.list_objects")
@patch("odin.ingestion.afc.afc_archive.delete_objects")
@patch("odin.ingestion.afc.afc_archive.upload_file")
@patch("odin.ingestion.afc.afc_archive.download_object")
def test_sync_parquet_bad_type(
    dl_obj: MagicMock, mock_upload: MagicMock, del_obj: MagicMock, ls_obj: MagicMock, tmpdir
):
    """Test sync_parquet method of ArchiveAFCAPI for bad table type"""
    data = [
        {"job_id": 1, "value": "sid_1"},
        {"job_id": 1, "value": "sid_1"},
        {"job_id": 1, "value": "sid_1"},
    ]
    pl.DataFrame(data).write_ndjson(os.path.join(tmpdir, "1.json"))

    job = ArchiveAFCAPI("test_table")
    job.tmpdir = tmpdir
    job.schema = pl.Schema({"job_id": pl.Int64(), "value": pl.String()})
    job.table_type = "not_static"
    job.ts_cols = []
    job.export_folder = ""
    ls_obj.return_value = []

    with pytest.raises(NotImplementedError):
        job.sync_parquet()

    dl_obj.assert_not_called()
    mock_upload.assert_not_called()
    del_obj.assert_not_called()


@patch("odin.ingestion.afc.afc_archive.list_objects")
@patch("odin.ingestion.afc.afc_archive.delete_objects")
@patch("odin.ingestion.afc.afc_archive.upload_file")
@patch("odin.ingestion.afc.afc_archive.download_object")
def test_sync_parquet_static(
    dl_obj: MagicMock, mock_upload: MagicMock, del_obj: MagicMock, ls_obj: MagicMock, tmpdir
):
    """Test sync_parquet method of ArchiveAFCAPI for static table type"""
    export = "bucket"
    data = [
        {"job_id": 1, "value": "sid_1"},
        {"job_id": 1, "value": "sid_1"},
        {"job_id": 1, "value": "sid_1"},
    ]
    pl.DataFrame(data).write_ndjson(os.path.join(tmpdir, "1.json"))

    job = ArchiveAFCAPI("test_table")
    job.tmpdir = tmpdir
    job.schema = pl.Schema({"job_id": pl.Int64(), "value": pl.String()})
    job.table_type = "static"
    job.ts_cols = []
    job.export_folder = export
    ls_obj.return_value = [S3Object(path="delete_me", size_bytes=0, last_modified=datetime.now())]
    export_file = os.path.join(tmpdir, export, "table_001.parquet")
    job.sync_parquet()

    mock_upload.assert_called_once_with(export_file, f"{export}/table_001.parquet")
    del_obj.assert_called_once_with(["delete_me"])
    dl_obj.assert_not_called()
    os.unlink(export_file)

    data = [
        {"job_id": 1, "value": "sid_1"},
        {"job_id": 1, "value": "sid_1"},
        {"job_id": 1, "value": "sid_1"},
    ]
    pl.DataFrame(data).write_ndjson(os.path.join(tmpdir, "1.json"))
    data = [
        {"job_id": 2, "value": "sid_2"},
        {"job_id": 2, "value": "sid_2"},
    ]
    pl.DataFrame(data).write_ndjson(os.path.join(tmpdir, "2.json"))
    with pytest.raises(AssertionError):
        job.sync_parquet()


@patch("odin.ingestion.afc.afc_archive.list_objects")
@patch("odin.ingestion.afc.afc_archive.delete_objects")
@patch("odin.ingestion.afc.afc_archive.upload_file")
@patch("odin.ingestion.afc.afc_archive.download_object")
def test_sync_parquet_transactional(
    dl_obj: MagicMock, mock_upload: MagicMock, del_obj: MagicMock, ls_obj: MagicMock, tmpdir
):
    """Test sync_parquet method of ArchiveAFCAPI for transactional table type"""
    export = "bucket"
    data = [
        {"job_id": 1, "value": "sid_1"},
        {"job_id": 1, "value": "sid_1"},
        {"job_id": 1, "value": "sid_1"},
    ]
    pl.DataFrame(data).write_ndjson(os.path.join(tmpdir, "1.json"))

    data = [
        {"job_id": 0, "value": "sid_0"},
    ]
    pl.DataFrame(data).write_parquet(os.path.join(tmpdir, "temp_.parquet"))

    job = ArchiveAFCAPI("test_table")
    job.tmpdir = tmpdir
    job.schema = pl.Schema({"job_id": pl.Int64(), "value": pl.String()})
    job.table_type = "transactional"
    job.ts_cols = []
    job.export_folder = export
    ls_obj.return_value = [
        S3Object(path="temp_.parquet", size_bytes=0, last_modified=datetime.now())
    ]
    export_file = os.path.join(tmpdir, export, "table_001.parquet")
    job.sync_parquet()

    dl_obj.assert_called_once_with("temp_.parquet", f"{tmpdir}/temp_.parquet")
    mock_upload.assert_called_once_with(export_file, f"{export}/table_001.parquet")
    del_obj.assert_called_once_with([])
