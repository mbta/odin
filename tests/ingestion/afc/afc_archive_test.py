import json
import os
import re
import shutil
import time
from datetime import datetime
from datetime import timedelta
from typing import Generator
from unittest.mock import MagicMock
from unittest.mock import patch
from unittest.mock import call

import polars as pl
import pytest

from odin.ingestion.afc.afc_archive import API_COUNT_LIMIT
from odin.ingestion.afc.afc_archive import APICounts
from odin.ingestion.afc.afc_archive import ArchiveAFCAPI
from odin.ingestion.afc.afc_archive import make_pl_schema
from odin.ingestion.afc.afc_archive import verify_downloads
from odin.utils.aws.s3 import S3Object
from odin.utils.parquet import ds_from_path
from odin.utils.status import utc_now


@patch.object(ArchiveAFCAPI, "make_request")
@patch("shutil.copyfileobj")
@patch("gzip.open")
@patch("odin.ingestion.afc.afc_archive.verify_downloads")
def test_download_json(
    verify_downloads: MagicMock,
    gzip_open: MagicMock,
    copyfileobj: MagicMock,
    make_request: MagicMock,
):
    """Test download_json method of ArchiveAFCAPI"""
    job = ArchiveAFCAPI("test_table")
    job.reset_tmpdir()
    job.schema = None
    mock_response = MagicMock()
    mock_response.release_conn = lambda: None

    # Require a single retry to succeed in the test.
    make_request.side_effect = [IOError("Something happened!!"), mock_response]

    test_download_jobs: APICounts = [{"jobId": 0, "dataCount": -1}, {"jobId": 10, "dataCount": -1}]

    job.download_json(test_download_jobs)

    gzip_open.assert_called()
    copyfileobj.assert_called()
    verify_downloads.assert_called()


@patch.object(ArchiveAFCAPI, "api_job_ids")
@patch.object(ArchiveAFCAPI, "download_json")
@patch("odin.ingestion.afc.afc_archive.disk_free_pct")
def test_load_job_ids(disk_free: MagicMock, dl_csv: MagicMock, api_jobs: MagicMock):
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
    api_jobs.return_value = iter(
        [
            [
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
        ]
    )
    job.load_job_ids()
    dl_csv.assert_called_once_with([{"jobId": 1001, "dataCount": 1}])
    dl_csv.reset_mock()

    # Test target_rows hit
    api_jobs.return_value = iter(
        [
            [
                {"jobId": 1, "dataCount": 1},
                {"jobId": 1000, "dataCount": 1},
                {"jobId": 1001, "dataCount": 1_000_000},
                {"jobId": 1002, "dataCount": 500},
            ]
        ]
    )
    job.load_job_ids()
    dl_csv.assert_has_calls(
        [
            call([{"jobId": 1001, "dataCount": 1_000_000}]),
            call([{"jobId": 1002, "dataCount": 500}]),
        ]
    )
    dl_csv.reset_mock()

    # Test combine jobIds
    api_jobs.return_value = iter(
        [
            [
                {"jobId": 1, "dataCount": 1},
                {"jobId": 1000, "dataCount": 1},
                {"jobId": 1001, "dataCount": 500},
                {"jobId": 1002, "dataCount": 500},
            ]
        ]
    )
    job.load_job_ids()
    dl_csv.assert_called_once_with(
        [
            {"jobId": 1001, "dataCount": 500},
            {"jobId": 1002, "dataCount": 500},
        ]
    )
    dl_csv.reset_mock()

    # Test disk_free_pct hit
    api_jobs.return_value = iter(
        [
            [
                {"jobId": 1, "dataCount": 1},
                {"jobId": 1000, "dataCount": 1},
                {"jobId": 1001, "dataCount": 500},
                {"jobId": 1002, "dataCount": 500},
            ]
        ]
    )
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

    yield str(path)
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

    download_jobs = [
        {"jobId": 1, "dataCount": 1},
        {"jobId": 2, "dataCount": 2},
        {"jobId": 3, "dataCount": 3},
    ]
    csv_schema = pl.Schema({"job_id": pl.Int64()})
    assert_re = re.escape("Columns in API download not found in API schema: (value)")
    with pytest.raises(AssertionError, match=assert_re):
        verify_downloads(csv_file, csv_schema, download_jobs)

    csv_schema = pl.Schema({"job_id": pl.Int64(), "value": pl.String(), "extra_col": pl.String()})
    assert_re = re.escape("Columns in API schema not found in API download: (extra_col)")
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
@patch.dict(
    "odin.ingestion.afc.afc_archive.API_TABLE_PII_DROP_COLUMNS", {"test_table": ["pii_value"]}
)
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


@patch("odin.ingestion.afc.afc_archive.list_objects")
@patch("odin.ingestion.afc.afc_archive.delete_objects")
@patch("odin.ingestion.afc.afc_archive.upload_file")
@patch("odin.ingestion.afc.afc_archive.download_object")
def test_sync_parquet_lists_folder_scoped_prefix(
    dl_obj: MagicMock, mock_upload: MagicMock, del_obj: MagicMock, ls_obj: MagicMock, tmpdir
):
    """
    sync_parquet must scope its S3 listing to the table's own folder.

    A bare prefix (no trailing "/") matches sibling folders whose name starts with
    this table's name (e.g. "v_entitlements" also matches "v_entitlements_full/..."),
    which makes found_objs[-1] resolve to the wrong table's file and corrupts the
    re-merge/part-offset, silently overwriting data on S3. Pin the folder boundary.
    """
    export = "bucket/odin/data/sb/api/v_entitlements"
    data = [{"job_id": 1, "value": "sid_1"}]
    pl.DataFrame(data).write_ndjson(os.path.join(tmpdir, "1.json"))

    job = ArchiveAFCAPI("test_table")
    job.tmpdir = tmpdir
    job.schema = pl.Schema({"job_id": pl.Int64(), "value": pl.String()})
    job.table_type = "transactional"
    job.ts_cols = []
    job.export_folder = export
    ls_obj.return_value = []

    job.sync_parquet()

    # Every S3 listing for this folder must end with "/" so a raw prefix match cannot
    # spill into a sibling folder like ".../v_entitlements_full/".
    assert ls_obj.call_count >= 1
    for c in ls_obj.call_args_list:
        listed_prefix = c.args[0]
        assert listed_prefix.endswith("/"), f"listing not folder-scoped: {listed_prefix!r}"
        assert listed_prefix.rstrip("/").endswith("/v_entitlements")


@patch("odin.ingestion.afc.afc_archive.list_objects")
@patch("odin.ingestion.afc.afc_archive.delete_objects")
@patch("odin.ingestion.afc.afc_archive.upload_file")
@patch("odin.ingestion.afc.afc_archive.download_object")
@patch.dict(
    "odin.ingestion.afc.afc_archive.API_TABLE_PII_DROP_COLUMNS", {"test_table": ["pii_value"]}
)
def test_sync_parquet_drops_pii_columns(
    dl_obj: MagicMock, mock_upload: MagicMock, del_obj: MagicMock, ls_obj: MagicMock, tmpdir
):
    """Configured table columns should be removed before parquet upload."""
    export = "bucket"
    data = [
        {"job_id": 1, "value": "sid_1", "pii_value": "secret"},
        {"job_id": 1, "value": "sid_1", "pii_value": "secret"},
    ]
    pl.DataFrame(data).write_ndjson(os.path.join(tmpdir, "1.json"))

    job = ArchiveAFCAPI("test_table")
    job.tmpdir = tmpdir
    job.schema = pl.Schema({"job_id": pl.Int64(), "value": pl.String(), "pii_value": pl.String()})
    job.pii_drop_columns = ["pii_value"]
    job.table_type = "static"
    job.ts_cols = []
    job.export_folder = export
    ls_obj.return_value = []

    job.sync_parquet()

    export_file = os.path.join(tmpdir, export, "table_001.parquet")
    out_cols = pl.scan_parquet(export_file).collect_schema().names()
    assert "pii_value" not in out_cols
    assert set(out_cols) == {"job_id", "value"}

    mock_upload.assert_called_once_with(export_file, f"{export}/table_001.parquet")
    del_obj.assert_called_once_with([])
    dl_obj.assert_not_called()


_FakeRequestResponse = MagicMock()
_FakeRequestResponse.json.return_value = [
    {"test_table": {"table_infos": [{"data_type": "test_table_type"}], "type": "test_type"}}
]
_FakeSchema = MagicMock()
_FakeSchema.len.return_value = 10000


@patch.dict("odin.ingestion.afc.afc_archive.API_TABLE_START_JOBID", {"test_table": 123})
@patch("odin.ingestion.afc.afc_archive.list_objects")
@patch("odin.ingestion.afc.afc_archive.ds_metadata_min_max")
@patch.object(ArchiveAFCAPI, "make_request", return_value=_FakeRequestResponse)
@patch.object(ArchiveAFCAPI, "api_job_ids", return_value=[])
@patch("odin.ingestion.afc.afc_archive.make_pl_schema", return_value=_FakeSchema)
def test_set_starting_jobid(
    make_pl_schema: MagicMock,
    api_job_ids: MagicMock,
    make_request: MagicMock,
    ds_metadata_min_max: MagicMock,
    list_objects: MagicMock,
):
    """Test API_TABLE_START_JOBID to set starting jobid for table ingestion."""
    import odin.ingestion.afc.afc_archive as afc_archive

    # If there are prior parquet files, return greatest of either previous job_id or starting job_id
    for max_job_id, jobid_start_id in [(100, 1000), (2000, 100)]:
        afc_archive.API_TABLE_START_JOBID = {"test_table": jobid_start_id}
        list_objects.return_value = [True]
        ds_metadata_min_max.return_value = (0, max_job_id)

        job = ArchiveAFCAPI("test_table")
        job.setup_job()
        job.load_job_ids()

        expected_start_jobid = max(max_job_id, jobid_start_id)
        api_job_ids.assert_called_with(expected_start_jobid)

    # If there are no prior parquet files found, should always go from starting job_id
    for jobid_start_id in [0, 100, 1000]:
        afc_archive.API_TABLE_START_JOBID = {"test_table": jobid_start_id}
        list_objects.return_value = 0
        ds_metadata_min_max.return_value = None

        job = ArchiveAFCAPI("test_table")
        job.setup_job()
        job.load_job_ids()

        api_job_ids.assert_called_with(jobid_start_id)


# ===========================================================================
# Backlog (jobs_lag / rows_lag) and status publishing
# ===========================================================================


def _count_response(jobs: list[dict]) -> MagicMock:
    """Fake /count response returning `jobs` as JSON."""
    response = MagicMock()
    response.json.return_value = jobs
    return response


@patch.object(ArchiveAFCAPI, "make_request")
def test_re_run_check_backlog_excludes_inclusive_boundary_job(make_request: MagicMock):
    """
    /count is inclusive of jobIdFrom, so the already-ingested job must not be counted.

    Counting it would overstate rows_lag by that job's entire dataCount.
    """
    job = ArchiveAFCAPI("v_test")
    job.max_job_id = 100
    make_request.return_value = _count_response(
        [
            {"jobId": 100, "dataCount": 5_000},  # already ingested - must be excluded
            {"jobId": 250, "dataCount": 300},
            {"jobId": 900, "dataCount": 700},
        ]
    )

    job.re_run_check()

    assert job.jobs_lag == 2
    assert job.rows_lag == 1_000
    assert job.api_latest_job_id == 900
    assert job.lag_truncated is False


@patch.object(ArchiveAFCAPI, "make_request")
def test_re_run_check_backlog_when_caught_up(make_request: MagicMock):
    """A response holding only the boundary job means no outstanding work."""
    job = ArchiveAFCAPI("v_test")
    job.max_job_id = 100
    make_request.return_value = _count_response([{"jobId": 100, "dataCount": 5_000}])

    job.re_run_check()

    assert job.jobs_lag == 0
    assert job.rows_lag == 0


@patch.object(ArchiveAFCAPI, "make_request")
def test_re_run_check_requests_explicit_count_limit(make_request: MagicMock):
    """
    The /count request must send an explicit limit.

    Without one the endpoint applies its own default, silently truncating the totals.
    """
    job = ArchiveAFCAPI("v_test")
    job.max_job_id = 100
    make_request.return_value = _count_response([{"jobId": 100, "dataCount": 1}])

    job.re_run_check()

    assert make_request.call_args.kwargs["fields"]["limit"] == str(API_COUNT_LIMIT)


def test_backlog_defaults_to_caught_up_when_nothing_downloaded():
    """
    max_job_id == 0 means load_job_ids found nothing past the parquet watermark.

    re_run_check skips its API call entirely on that path, so the defaults must
    already read as caught up rather than as unknown.
    """
    job = ArchiveAFCAPI("v_test")
    job.max_job_id = 0

    with patch.object(ArchiveAFCAPI, "make_request") as make_request:
        job.re_run_check()

    make_request.assert_not_called()
    assert job.jobs_lag == 0
    assert job.rows_lag == 0


def test_write_status_publishes_backlog(tmp_path):
    """_write_status publishes the backlog totals and the post-sync parquet state."""
    job = ArchiveAFCAPI("v_test")
    job.jobs_lag = 2
    job.rows_lag = 1_000
    job.api_latest_job_id = 900
    job.table_type = "transactional"
    job.post_snapshot = {
        "object_count": 3,
        "total_size_bytes": 1_234,
        "total_rows": 500_000,
        "min_job_id": 1,
        "max_job_id": 100,
    }

    captured: dict = {}

    def fake_upload(src, dst, **kwargs):
        captured["dst"] = dst
        captured["payload"] = json.loads(open(src).read())

    with (
        patch("odin.utils.status.upload_file", fake_upload),
        # No previous status object, so this run publishes no rates.
        patch("odin.utils.status.download_object", return_value=False),
    ):
        job._write_status(next_run_secs=300)

    assert captured["dst"].endswith("odin/logs/afc/v_test.json")
    payload = captured["payload"]
    assert payload["table"] == "v_test"
    assert payload["table_type"] == "transactional"
    assert payload["max_job_id"] == 100
    assert payload["api_latest_job_id"] == 900
    assert payload["jobs_lag"] == 2
    assert payload["rows_lag"] == 1_000
    assert payload["row_count"] == 500_000
    assert payload["next_run_seconds"] == 300


def test_write_status_estimates_catchup_from_rows():
    """With no event time, the catch-up estimate comes from rows_lag over rows/sec."""
    now = utc_now()
    job = ArchiveAFCAPI("v_test")
    job.jobs_lag = 2
    job.rows_lag = 1_000
    job.api_latest_job_id = 900
    job.table_type = "transactional"
    job._run_started = time.perf_counter() - 100.0  # a 100s run
    job.post_snapshot = {
        "object_count": 3,
        "total_size_bytes": 1_234,
        "total_rows": 500_000,
        "min_job_id": 1,
        "max_job_id": 100,
    }
    prev = {
        "last_run": (now - timedelta(hours=1)).isoformat(),
        "row_count": 490_000,
    }

    captured: dict = {}

    def fake_upload(src, dst, **kwargs):
        captured["payload"] = json.loads(open(src).read())

    def fake_download(obj, local_path):
        with open(local_path, "w") as f:
            json.dump(prev, f)
        return True

    with (
        patch("odin.utils.status.upload_file", fake_upload),
        patch("odin.utils.status.download_object", fake_download),
    ):
        job._write_status(next_run_secs=300)

    payload = captured["payload"]
    assert payload["previous_row_count"] == 490_000
    assert payload["rows_added"] == 10_000
    # 10,000 rows in 100s.
    assert payload["rows_per_second"] == pytest.approx(100.0, abs=1.0)
    # 1,000 rows of backlog at 100 rows/sec == 10s of processing left.
    assert payload["catchup_processing_seconds"] == pytest.approx(10, abs=1)
    # No event time means no frontier, so no wall-clock estimate is invented.
    assert "catchup_wall_seconds" not in payload
    assert "watermark_advance_seconds" not in payload


def test_write_status_caught_up_reports_zero_catchup():
    """A drained backlog reports zero remaining, not an absent estimate."""
    now = utc_now()
    job = ArchiveAFCAPI("v_test")
    job.jobs_lag = 0
    job.rows_lag = 0
    job.table_type = "transactional"
    job._run_started = time.perf_counter() - 10.0
    job.post_snapshot = {
        "object_count": 3,
        "total_size_bytes": 1_234,
        "total_rows": 500_000,
        "min_job_id": 1,
        "max_job_id": 100,
    }
    # A run that added nothing: rows/sec is 0, so the rows_lag == 0 arm must answer.
    prev = {"last_run": (now - timedelta(hours=1)).isoformat(), "row_count": 500_000}

    captured: dict = {}

    def fake_upload(src, dst, **kwargs):
        captured["payload"] = json.loads(open(src).read())

    def fake_download(obj, local_path):
        with open(local_path, "w") as f:
            json.dump(prev, f)
        return True

    with (
        patch("odin.utils.status.upload_file", fake_upload),
        patch("odin.utils.status.download_object", fake_download),
    ):
        job._write_status(next_run_secs=300)

    assert captured["payload"]["rows_added"] == 0
    assert captured["payload"]["catchup_processing_seconds"] == 0


def test_re_run_check_quiet_run_records_parquet_watermark_as_frontier():
    """
    A run with nothing to download leaves the API frontier at our own watermark.

    Publishing None there instead would look like a changed value on every quiet run and
    reset api_latest_job_first_seen, erasing the only clock AFC has.
    """
    job = ArchiveAFCAPI("v_test")
    job.max_job_id = 0
    job.pq_job_id = 100

    with patch.object(ArchiveAFCAPI, "make_request") as make_request:
        job.re_run_check()

    make_request.assert_not_called()
    assert job.api_latest_job_id == 100


def _capture_status(job: ArchiveAFCAPI, prev: dict | None, next_run_secs: int = 300) -> dict:
    """Run _write_status against `prev` and return the payload it would publish."""
    captured: dict = {}

    def fake_upload(src, dst, **kwargs):
        captured["payload"] = json.loads(open(src).read())

    def fake_download(obj, local_path):
        if prev is None:
            return False
        with open(local_path, "w") as f:
            json.dump(prev, f)
        return True

    with (
        patch("odin.utils.status.upload_file", fake_upload),
        patch("odin.utils.status.download_object", fake_download),
        patch.object(ArchiveAFCAPI, "_max_timestamps", return_value={}),
    ):
        job._write_status(next_run_secs=next_run_secs)

    return captured["payload"]


def _status_job(jobs_lag: int, api_latest_job_id: int) -> ArchiveAFCAPI:
    """Build a job posed mid-run, with just the state _write_status reads."""
    job = ArchiveAFCAPI("v_test")
    job.jobs_lag = jobs_lag
    job.rows_lag = jobs_lag * 10
    job.api_latest_job_id = api_latest_job_id
    job.table_type = "transactional"
    job.table_frequency = "hourly"
    job.post_snapshot = {
        "object_count": 3,
        "total_size_bytes": 1_234,
        "total_rows": 500_000,
        "min_job_id": 1,
        "max_job_id": 100,
    }
    return job


def test_write_status_carries_first_seen_while_frontier_holds():
    """While S&B publishes no new job, the first-seen timestamp keeps its clock running."""
    now = utc_now()
    first_seen = (now - timedelta(hours=9)).isoformat()
    prev = {
        "last_run": (now - timedelta(hours=6)).isoformat(),
        "api_latest_job_id": 900,
        "api_latest_job_first_seen": first_seen,
    }

    payload = _capture_status(_status_job(jobs_lag=0, api_latest_job_id=900), prev)

    assert payload["api_latest_job_first_seen"] == first_seen
    assert payload["table_frequency"] == "hourly"


def test_write_status_resets_first_seen_on_new_frontier_job():
    """A new job id upstream means new data exists, so the clock restarts."""
    now = utc_now()
    prev = {
        "last_run": (now - timedelta(hours=6)).isoformat(),
        "api_latest_job_id": 900,
        "api_latest_job_first_seen": (now - timedelta(hours=9)).isoformat(),
    }

    payload = _capture_status(_status_job(jobs_lag=0, api_latest_job_id=901), prev)

    assert payload["api_latest_job_first_seen"] != prev["api_latest_job_first_seen"]


@pytest.mark.parametrize(
    "prev_first_seen",
    [pytest.param(None, id="missing"), pytest.param("not a timestamp", id="unparseable")],
)
def test_write_status_restarts_first_seen_without_usable_carried_time(prev_first_seen):
    """
    An unchanged job id with no usable stored timestamp starts the clock at this run.

    Status objects published before the field existed take this path, so it must
    produce a usable answer rather than propagate the missing value.
    """
    now = utc_now()
    prev = {"last_run": (now - timedelta(hours=6)).isoformat(), "api_latest_job_id": 900}
    if prev_first_seen is not None:
        prev["api_latest_job_first_seen"] = prev_first_seen

    payload = _capture_status(_status_job(jobs_lag=0, api_latest_job_id=900), prev)

    assert payload["api_latest_job_first_seen"] == payload["last_run"]


def test_write_status_first_seen_on_first_run():
    """With no previous status there is nothing to carry, so the job id is first seen now."""
    payload = _capture_status(_status_job(jobs_lag=0, api_latest_job_id=900), prev=None)

    assert payload["api_latest_job_first_seen"] == payload["last_run"]


def test_write_status_pending_since_holds_through_partial_progress():
    """
    pending_since dates the shortfall, not the last advance.

    A run that ingests some of the backlog still leaves us behind, so the timestamp must
    survive it; max_job_id moved, and nothing else would record when we fell behind.
    """
    now = utc_now()
    pending_since = (now - timedelta(hours=7)).isoformat()
    prev = {
        "last_run": (now - timedelta(hours=6)).isoformat(),
        "max_job_id": 50,
        "jobs_lag": 9,
        "pending_since": pending_since,
    }

    payload = _capture_status(_status_job(jobs_lag=3, api_latest_job_id=903), prev)

    assert payload["pending_since"] == pending_since


def test_write_status_pending_since_starts_and_clears():
    """The backlog's first run sets the timestamp; a drained backlog clears it."""
    now = utc_now()
    prev = {"last_run": (now - timedelta(hours=6)).isoformat(), "jobs_lag": 0}

    fell_behind = _capture_status(_status_job(jobs_lag=3, api_latest_job_id=903), prev)
    assert fell_behind["pending_since"] == fell_behind["last_run"]

    caught_up = _capture_status(_status_job(jobs_lag=0, api_latest_job_id=903), dict(fell_behind))
    assert caught_up["pending_since"] is None


def test_max_timestamps_reads_footer_stats(tmp_path):
    """
    Every timestamp column's max is published, read from parquet footer statistics.

    The API gives job ids but no event time, so these are the only values that can say
    how old the data itself is.
    """
    pq_path = tmp_path / "table_001.parquet"
    pl.DataFrame(
        {
            "job_id": [1, 2],
            "created_at": [datetime(2026, 9, 1, 8, 0), datetime(2026, 9, 2, 9, 30)],
            "updated_at": [datetime(2026, 9, 1, 8, 5), datetime(2026, 9, 2, 9, 35)],
        }
    ).write_parquet(pq_path)

    job = ArchiveAFCAPI("v_test")
    job.ts_cols = ["created_at", "updated_at"]

    with patch(
        "odin.ingestion.afc.afc_archive.ds_from_path",
        return_value=ds_from_path(str(tmp_path) + "/"),
    ):
        maxes = job._max_timestamps()

    assert maxes == {
        "created_at": "2026-09-02T09:30:00",
        "updated_at": "2026-09-02T09:35:00",
    }


def test_max_timestamps_without_timestamp_columns():
    """A table with no timestamp columns publishes nothing and reads no footers."""
    job = ArchiveAFCAPI("v_test")
    job.ts_cols = []

    with patch("odin.ingestion.afc.afc_archive.ds_from_path") as ds_from:
        assert job._max_timestamps() == {}

    ds_from.assert_not_called()


def test_max_timestamps_survives_unreadable_dataset():
    """Status is best-effort: an unreadable dataset costs this field, not the run."""
    job = ArchiveAFCAPI("v_test")
    job.ts_cols = ["created_at"]

    with patch(
        "odin.ingestion.afc.afc_archive.ds_from_path", side_effect=IOError("no such dataset")
    ):
        assert job._max_timestamps() == {}


def test_write_status_nests_column_timestamps():
    """Column maxima land under one key, indexable by column name."""
    job = _status_job(jobs_lag=0, api_latest_job_id=900)
    maxes = {"created_at": "2026-09-02T09:30:00", "updated_at": None}
    captured: dict = {}

    def fake_upload(src, dst, **kwargs):
        captured["payload"] = json.loads(open(src).read())

    with (
        patch("odin.utils.status.upload_file", fake_upload),
        patch("odin.utils.status.download_object", return_value=False),
        patch.object(ArchiveAFCAPI, "_max_timestamps", return_value=maxes),
    ):
        job._write_status(next_run_secs=300)

    assert captured["payload"]["max_column_timestamps"] == maxes
