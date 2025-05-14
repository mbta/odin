from unittest.mock import MagicMock
from unittest.mock import patch
from unittest.mock import call

import polars as pl

from odin.ingestion.afc.afc_archive import ArchiveAFCAPI
from odin.ingestion.afc.afc_archive import make_pl_schema


@patch.object(ArchiveAFCAPI, "download_csv")
@patch("odin.ingestion.afc.afc_archive.disk_free_pct")
def test_load_sids(disk_free: MagicMock, dl_csv: MagicMock):
    """Test load_sids method of ArchiveAFCAPI"""
    disk_free.return_value = 99
    job = ArchiveAFCAPI("test_table")
    test_schema = [
        {"column_name": "col1", "data_type": "bigint"},
        {"column_name": "col2", "data_type": "integer"},
        {"column_name": "col3", "data_type": "varchar"},
        {"column_name": "col4", "data_type": "timestamp with time zone"},
        {"column_name": "col5", "data_type": "varchar"},
    ]
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
    job.pq_sid = 1000
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
    job.load_sids()
    dl_csv.assert_called_once_with([{"jobId": 1001, "dataCount": 1}])
    dl_csv.reset_mock()

    # Test target_rows hit
    job.job_ids = [
        {"jobId": 1, "dataCount": 1},
        {"jobId": 1000, "dataCount": 1},
        {"jobId": 1001, "dataCount": 1_000_000},
        {"jobId": 1002, "dataCount": 500},
    ]
    job.load_sids()
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
    job.load_sids()
    dl_csv.assert_called_once_with(
        [
            {"jobId": 1001, "dataCount": 500},
            {"jobId": 1002, "dataCount": 500},
        ]
    )
    dl_csv.reset_mock()

    # Test disk_free_pct hit
    disk_free.return_value = 50
    job.load_sids()
    dl_csv.assert_called_once_with([{"jobId": 1001, "dataCount": 500}])
    dl_csv.reset_mock()
