import os
import pytest
from typing import List
from pathlib import Path

import polars as pl
import pyarrow.dataset as pd
import pyarrow.compute as pc

from odin.utils.parquet import pq_rows_and_bytes
from odin.utils.parquet import pq_rows_per_mb
from odin.utils.parquet import ds_files
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import ds_column_min_max
from odin.utils.parquet import ds_metadata_min_max
from odin.utils.parquet import ds_unique_values
from odin.utils.parquet import _pq_find_part_offset

PQ_NUM_ROWS = 500_000
PQ_MAX_INT = 50_000


@pytest.fixture
def pq_files(tmp_path) -> List[str]:
    """Create temporary parquet files for testing."""
    pq_files = []
    pq_files.append(os.path.join(tmp_path, "t1.parquet"))
    (
        pl.DataFrame()
        .with_columns(
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("row1"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("row2"),
        )
        .write_parquet(pq_files[-1], row_group_size=int(PQ_NUM_ROWS / 10))
    )

    pq_files.append(os.path.join(tmp_path, "t2.parquet"))
    (
        pl.DataFrame()
        .with_columns(
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("row1"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("row2"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("row3"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("row4"),
        )
        .write_parquet(pq_files[-1], row_group_size=int(PQ_NUM_ROWS / 10))
    )

    pq_files.append(os.path.join(tmp_path, "t3.parquet"))
    (
        pl.DataFrame()
        .with_columns(
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("row1"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("row2"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("row3"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("row4"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("row5"),
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("row6"),
            pl.lit("single_value").alias("row7"),
        )
        .write_parquet(pq_files[-1], row_group_size=int(PQ_NUM_ROWS / 10))
    )

    return pq_files


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

    ds = ds_from_path(pq_files)
    assert isinstance(ds, pd.UnionDataset)
    assert ds.count_rows() == PQ_NUM_ROWS * len(pq_files)
    assert len(ds.schema.names) == 7


def test_ds_column_min_max(pq_files) -> None:
    """Test ds_column_min_max parquet utility."""
    # Keep an eye on this test, the source data is technically random so this could fail...
    ds = ds_from_path(pq_files)
    assert ds_column_min_max(ds, "row1") == (0, PQ_MAX_INT - 1)

    ds_filter = pc.field("row1") < 50
    assert ds_column_min_max(ds, "row1", ds_filter) == (0, 49)


def test_ds_metadata_min_max(pq_files) -> None:
    """Test ds_column_min_max parquet utility."""
    # Keep an eye on this test, the source data is technically random so this could fail...
    ds = ds_from_path(pq_files)
    assert ds_metadata_min_max(ds, "row1") == (0, PQ_MAX_INT - 1)

    assert ds_metadata_min_max(ds, "row7") == ("single_value", "single_value")


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
