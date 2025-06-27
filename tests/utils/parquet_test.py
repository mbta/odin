import os
import shutil
import pytest
from typing import Generator
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.dataset as pd
import pyarrow.compute as pc
import pyarrow.parquet as pq

from odin.utils.parquet import pq_rows_and_bytes
from odin.utils.parquet import pq_rows_per_mb
from odin.utils.parquet import ds_files
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import ds_column_min_max
from odin.utils.parquet import ds_metadata_min_max
from odin.utils.parquet import ds_unique_values
from odin.utils.parquet import _pq_find_part_offset
from odin.utils.parquet import ds_metadata_limit_k_sorted
from odin.utils.parquet import pq_path_partitions
from odin.utils.parquet import row_group_column_stats


PQ_NUM_ROWS = 500_000
PQ_MAX_INT = 50_000


@pytest.fixture(scope="module")
def pq_files(tmp_path_factory) -> Generator[list[str]]:
    """Create temporary parquet files for testing."""
    tmp_path = tmp_path_factory.mktemp("pq_files", numbered=False)
    pq_files = []
    pq_files.append(os.path.join(tmp_path, "file=t1", "year=2025", "t1.parquet"))
    os.makedirs(os.path.dirname(pq_files[-1]), exist_ok=True)
    (
        pl.DataFrame()
        .with_columns(
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("col1"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("col2"),
        )
        .write_parquet(pq_files[-1], row_group_size=int(PQ_NUM_ROWS / 10))
    )

    pq_files.append(os.path.join(tmp_path, "file=t2", "year=2024", "t2.parquet"))
    os.makedirs(os.path.dirname(pq_files[-1]), exist_ok=True)
    (
        pl.DataFrame()
        .with_columns(
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("col1"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("col2"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("col3"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("col4"),
        )
        .write_parquet(pq_files[-1], row_group_size=int(PQ_NUM_ROWS / 10))
    )

    pq_files.append(os.path.join(tmp_path, "file=t3", "year=2023", "t3.parquet"))
    os.makedirs(os.path.dirname(pq_files[-1]), exist_ok=True)
    (
        pl.DataFrame()
        .with_columns(
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("col1"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("col2"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("col3"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("col4"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("col5"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("col6"),
            pl.lit("single_value").alias("col7"),
        )
        .write_parquet(pq_files[-1], row_group_size=int(PQ_NUM_ROWS / 10))
    )

    yield pq_files
    shutil.rmtree(tmp_path)


def test_pq_rows_and_bytes(pq_files) -> None:
    """Test pq_row_and_bytes parquet utility."""
    tmp_pq_bytes = os.path.getsize(pq_files[0])

    assert pq_rows_and_bytes(pq_files[0]) == (PQ_NUM_ROWS, tmp_pq_bytes)
    assert pq_rows_and_bytes(pq_files[0], 1000) == (1000, tmp_pq_bytes)


def test_pq_rows_per_mb(pq_files) -> None:
    """Test pq_rows_per_mb parquet utility."""
    pq_files_mb = [os.path.getsize(p) / (1024 * 1024) for p in pq_files]

    assert pq_rows_per_mb(pq_files[0]) == int(PQ_NUM_ROWS / pq_files_mb[0])
    assert pq_rows_per_mb(pq_files) == int(PQ_NUM_ROWS * len(pq_files) / sum(pq_files_mb))


def test_ds_files(pq_files) -> None:
    """Test ds_files parquet utility."""
    # TODO: Figure out way to test S3 type dataset functionality.
    ds = pd.dataset([pd.dataset(p) for p in pq_files])
    assert ds_files(ds) == pq_files
    assert ds_files(ds_from_path(pq_files)) == pq_files


def test_ds_from_path(pq_files) -> None:
    """Test ds_from_path parquet utility."""
    ds = ds_from_path(pq_files[0])
    assert isinstance(ds, pd.UnionDataset)
    expected_scehma = pa.schema(
        (
            ("col1", pa.int64()),
            ("col2", pa.int64()),
            ("file", pa.string()),
            ("year", pa.int32()),
        )
    )
    assert ds.schema.equals(expected_scehma)

    ds = ds_from_path(pq_files)
    assert isinstance(ds, pd.UnionDataset)
    assert ds.count_rows() == PQ_NUM_ROWS * len(pq_files)
    assert len(ds.schema.names) == 9
    expected_scehma = pa.schema(
        (
            ("col1", pa.int64()),
            ("col2", pa.int64()),
            ("file", pa.string()),
            ("year", pa.int32()),
            ("col3", pa.int64()),
            ("col4", pa.int64()),
            ("col5", pa.int64()),
            ("col6", pa.int64()),
            ("col7", pa.large_string()),
        )
    )
    assert ds.schema.equals(expected_scehma)


def test_ds_column_min_max(pq_files) -> None:
    """Test ds_column_min_max parquet utility."""
    # Keep an eye on this test, the source data is technically random so this could fail...
    ds = ds_from_path(pq_files)
    assert ds_column_min_max(ds, "col1") == (0, PQ_MAX_INT - 1)

    # test partition column
    assert ds_column_min_max(ds, "file") == ("t1", "t3")

    ds_filter = pc.field("col1") < 50
    assert ds_column_min_max(ds, "col1", ds_filter) == (0, 49)


def test_ds_metadata_min_max(pq_files) -> None:
    """Test ds_column_min_max parquet utility."""
    # Keep an eye on this test, the source data is technically random so this could fail...
    ds = ds_from_path(pq_files)
    assert ds_metadata_min_max(ds, "col1") == (0, PQ_MAX_INT - 1)

    # test partition column
    assert ds_metadata_min_max(ds, "file") == ("t1", "t3")

    assert ds_metadata_min_max(ds, "col7") == ("single_value", "single_value")


def test_ds_unique_values(tmp_path) -> None:
    """Test ds_unique_values parquet utility."""
    data = [
        {"col1": "A", "col2": 1},
        {"col1": "A", "col2": 1},
        {"col1": "B", "col2": 1},
        {"col1": "B", "col2": 2},
        {"col1": "B", "col2": 2},
    ]
    tmp_file = os.path.join(tmp_path, "t.parquet")
    (pl.DataFrame(data).write_parquet(tmp_file))
    ds = ds_from_path(tmp_file)
    assert ds_unique_values(ds, ["col1"]).to_pydict() == {"col1": ["A", "B"]}
    assert ds_unique_values(ds, ["col1", "col2"]).to_pylist() == [
        {"col1": "A", "col2": 1},
        {"col1": "B", "col2": 1},
        {"col1": "B", "col2": 2},
    ]


def test__pq_find_part_offset(tmp_path) -> None:
    """Test _pq_find_part_offset parquet utility."""
    tmp_fldr = os.path.join(tmp_path, "offset")
    os.makedirs(tmp_fldr, exist_ok=True)

    assert _pq_find_part_offset(tmp_fldr) == 1
    file1 = os.path.join(tmp_fldr, "f_002.parquet")
    Path(file1).touch()
    file2 = os.path.join(tmp_fldr, "f_003.parquet")
    Path(file2).touch()

    assert _pq_find_part_offset(tmp_fldr) == 3

    os.remove(file2)
    assert _pq_find_part_offset(tmp_fldr) == 2


def test_pq_path_partitions(pq_files) -> None:
    """Test pq_path_partitions parquet utility."""
    pq_path = "/test/path/no/partition.parquet"
    assert pq_path_partitions(pq_path) == []

    pq_path = "/test/path/col1=val1/partition.parquet"
    assert pq_path_partitions(pq_path) == [("col1", "val1")]

    pq_path = "/test/path/col1=val1/col2=99/partition.parquet"
    assert pq_path_partitions(pq_path) == [("col1", "val1"), ("col2", "99")]

    assert pq_path_partitions(pq_files[0]) == [("file", "t1"), ("year", "2025")]


def test_row_group_column_stats(pq_files) -> None:
    """Test row_group_column_stats parquet utility."""
    pq_file = pq.ParquetFile(pq_files[0])
    pq_metadata = pq_file.metadata
    for rg_index in range(pq_file.num_row_groups):
        rg_meta = pq_metadata.row_group(rg_index)
        with pytest.raises(IndexError):
            row_group_column_stats(rg_meta, "col7")

        stats = row_group_column_stats(rg_meta, "col1")
        assert stats["max"] <= PQ_MAX_INT - 1
        assert stats["min"] >= 0

        stats = row_group_column_stats(rg_meta, "col2")
        assert stats["max"] <= PQ_MAX_INT - 1
        assert stats["min"] >= 0


def test_ds_metadata_limit_k_sorted(pq_files) -> None:
    """
    Test ds_metadata_limit_k_sorted parquet utility.

    The scope of this test is pretty limited and could be expanded...
    """
    ds = ds_from_path(pq_files)

    # test column not in all files
    df = ds_metadata_limit_k_sorted(ds, "col7")
    assert df.shape == (PQ_NUM_ROWS, 9)

    df = ds_metadata_limit_k_sorted(ds, "col7", max_rows=200)
    assert df.shape == (200, 9)

    df = ds_metadata_limit_k_sorted(ds, "col1")
    assert df.shape == (PQ_NUM_ROWS * 3, 9)
    assert df.get_column("col1")[0] == 0
    assert df.get_column("col1")[-1] == PQ_MAX_INT - 1

    df = ds_metadata_limit_k_sorted(ds, "col1", min_sort_value=500)
    assert df.width == 9
    assert df.height < PQ_NUM_ROWS * 3
    assert df.get_column("col1")[0] == 501
    assert df.get_column("col1")[-1] == PQ_MAX_INT - 1

    df = ds_metadata_limit_k_sorted(
        ds, "col1", ds_filter=(pc.field("col7") == "single_value"), ds_filter_columns=["col7"]
    )
    assert df.shape == (PQ_NUM_ROWS, 9)
    assert df.get_column("col1")[0] == 0
    assert df.get_column("col1")[-1] == PQ_MAX_INT - 1
