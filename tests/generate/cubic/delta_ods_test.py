"""
Tests for CubicODSDelta (Delta-based ODS silver materialization).

Correctness lives in two steps: rebuilding silver from a snapshot's "L" records
and the CDC MERGE. These tests write a real on-disk parquet "history" dataset
(hive-partitioned by ``snapshot=``, as cubic_archive.py produces) and a real
local Delta silver table, then call the steps directly and assert on silver's
contents. DuckDB reads the parquet; delta-rs writes the silver table.
"""

from unittest.mock import patch

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from deltalake import DeltaTable, write_deltalake

from odin.generate.cubic.delta_ods import CubicODSDelta, READ_HISTORY
from odin.utils.delta import column_max


KEYS = ["txn_id"]
TEST_SNAPSHOT = "20250101T000000Z"
FROM_CSV = "s3://archive/cubic/ods_qlik/EDW.TEST_TABLE__ct/test.csv.gz"

# History file schema (the hive ``snapshot`` column is derived from the path).
HISTORY_SCHEMA = pa.schema(
    [
        pa.field("txn_id", pa.int64()),
        pa.field("amount", pa.int64()),
        pa.field("status", pa.large_string()),
        pa.field("header__change_seq", pa.large_string()),
        pa.field("header__change_oper", pa.large_string()),
        pa.field("header__year", pa.int32()),
        pa.field("header__month", pa.int32()),
        pa.field("header__timestamp", pa.timestamp("us")),
        pa.field("header__from_csv", pa.large_string()),
    ]
)

SILVER_SCHEMA = pa.schema(
    [
        pa.field("txn_id", pa.int64()),
        pa.field("amount", pa.int64()),
        pa.field("status", pa.large_string()),
        pa.field("header__change_seq", pa.large_string()),
        pa.field("odin_snapshot", pa.large_string()),
    ]
)

# History schema variant carrying edw_inserted_dtm, which drives year/month
# partitioning of the silver table.
HISTORY_SCHEMA_DATED = pa.schema(
    list(HISTORY_SCHEMA) + [pa.field("edw_inserted_dtm", pa.timestamp("us"))]
)

SILVER_VISIBLE = ["txn_id", "amount", "status", "header__change_seq", "odin_snapshot"]


def history_rows(rows: list[dict]) -> pa.Table:
    """Build a history arrow table, filling defaults for unspecified columns."""
    defaults = {
        "txn_id": None,
        "amount": None,
        "status": None,
        "header__change_seq": None,
        "header__change_oper": None,
        "header__year": 2025,
        "header__month": 1,
        "header__timestamp": None,
        "header__from_csv": FROM_CSV,
    }
    return pa.Table.from_pylist([{**defaults, **r} for r in rows], schema=HISTORY_SCHEMA)


def dated_history_rows(rows: list[dict]) -> pa.Table:
    """Build a history arrow table including edw_inserted_dtm (drives partitioning)."""
    defaults = {
        "txn_id": None,
        "amount": None,
        "status": None,
        "header__change_seq": None,
        "header__change_oper": None,
        "header__year": 2025,
        "header__month": 1,
        "header__timestamp": None,
        "header__from_csv": FROM_CSV,
        "edw_inserted_dtm": None,
    }
    return pa.Table.from_pylist([{**defaults, **r} for r in rows], schema=HISTORY_SCHEMA_DATED)


def silver_rows(rows: list[dict]) -> pa.Table:
    """Build a silver arrow table in post-rebuild shape."""
    defaults = {
        "txn_id": None,
        "amount": None,
        "status": None,
        "header__change_seq": None,
        "odin_snapshot": TEST_SNAPSHOT,
    }
    return pa.Table.from_pylist([{**defaults, **r} for r in rows], schema=SILVER_SCHEMA)


def mock_dfm_for_keys(keys: list[str]) -> dict:
    """Minimal DFM dict marking `keys` as primary keys."""
    data_cols = ["txn_id", "amount", "status"]
    return {
        "dataInfo": {
            "columns": [
                {
                    "name": col,
                    "primaryKeyPos": (keys.index(col) + 1) if col in keys else 0,
                }
                for col in data_cols
            ]
        }
    }


@pytest.fixture
def job(tmp_path):
    """Yield (job, write_history, write_silver, read_silver) wired to local paths."""
    snapshot_dir = tmp_path / "history" / f"snapshot={TEST_SNAPSHOT}"
    snapshot_dir.mkdir(parents=True)
    history_file = snapshot_dir / "part.parquet"
    glob = str(tmp_path / "history" / "**" / "*.parquet")
    silver_dir = tmp_path / "silver"

    pipeline = CubicODSDelta("EDW.TEST_TABLE")
    pipeline.silver_uri = str(silver_dir)
    pipeline.history_snapshot = TEST_SNAPSHOT
    pipeline.history_glob = glob
    pipeline.part_columns = []

    def write_history(table: pa.Table) -> None:
        pq.write_table(table, str(history_file))
        # Populate history_columns the way _snapshot_check would (incl. hive snapshot).
        con = duckdb.connect()
        describe = con.execute(f"DESCRIBE SELECT * FROM {READ_HISTORY.format(glob=glob)}").pl()
        con.close()
        pipeline.history_columns = describe.get_column("column_name").to_list()
        # Mirror _snapshot_check: edw_inserted_dtm drives year/month partitioning.
        pipeline.part_columns = (
            ["odin_year", "odin_month"] if "edw_inserted_dtm" in pipeline.history_columns else []
        )

    def write_silver(table: pa.Table) -> None:
        write_deltalake(str(silver_dir), table, mode="overwrite", schema_mode="overwrite")
        pipeline.silver = DeltaTable(str(silver_dir))

    def read_silver() -> pl.DataFrame:
        result = pl.from_arrow(DeltaTable(str(silver_dir)).to_pyarrow_table())
        assert isinstance(result, pl.DataFrame)
        return result.select(SILVER_VISIBLE).sort("txn_id")

    with (
        patch("odin.generate.cubic.delta_ods.sigterm_check"),
        patch(
            "odin.generate.cubic.delta_ods.dfm_from_s3",
            return_value=mock_dfm_for_keys(KEYS),
        ),
    ):
        yield pipeline, write_history, write_silver, read_silver


def test_rebuild_silver_loads_only_load_records(job):
    """Rebuild keeps "L" rows, drops CDC metadata, and assigns odin index/snapshot."""
    pipeline, write_history, _, read_silver = job
    write_history(
        history_rows(
            [
                {"txn_id": 1, "amount": 10, "status": "a", "header__change_oper": "L"},
                {"txn_id": 2, "amount": 20, "status": "b", "header__change_oper": "L"},
                # A CDC row in the same snapshot must be ignored by the rebuild.
                {
                    "txn_id": 3,
                    "amount": 30,
                    "status": "c",
                    "header__change_oper": "I",
                    "header__change_seq": "0001",
                },
            ]
        )
    )
    pipeline._rebuild_silver()

    out = read_silver()
    assert out.get_column("txn_id").to_list() == [1, 2]
    assert out.get_column("odin_snapshot").to_list() == [TEST_SNAPSHOT, TEST_SNAPSHOT]
    # CDC metadata columns are dropped; silver carries only data + watermarks.
    silver_cols = DeltaTable(pipeline.silver_uri).schema().to_arrow().names
    assert "header__change_oper" not in silver_cols
    assert "odin_index" not in silver_cols


def test_rebuild_silver_partitions_by_year_and_month(job):
    """With edw_inserted_dtm present, silver is partitioned by odin_year + odin_month."""
    from datetime import datetime

    pipeline, write_history, _, _ = job
    write_history(
        dated_history_rows(
            [
                {
                    "txn_id": 1,
                    "amount": 10,
                    "header__change_oper": "L",
                    "edw_inserted_dtm": datetime(2025, 1, 15),
                },
                {
                    "txn_id": 2,
                    "amount": 20,
                    "header__change_oper": "L",
                    "edw_inserted_dtm": datetime(2025, 3, 20),
                },
            ]
        )
    )
    pipeline._rebuild_silver()

    dt = DeltaTable(pipeline.silver_uri)
    assert dt.metadata().partition_columns == ["odin_year", "odin_month"]
    out = pl.from_arrow(dt.to_pyarrow_table()).sort("txn_id")
    assert out.get_column("odin_year").to_list() == [2025, 2025]
    assert out.get_column("odin_month").to_list() == [1, 3]


def test_merge_cdc_dated_table_derives_year_and_month(job):
    """CDC merge on a partitioned table derives odin_year/odin_month from edw_inserted_dtm."""
    from datetime import datetime

    pipeline, write_history, _, _ = job
    write_history(
        dated_history_rows(
            [
                {
                    "txn_id": 1,
                    "amount": 10,
                    "header__change_oper": "L",
                    "edw_inserted_dtm": datetime(2025, 1, 15),
                },
                {
                    "txn_id": 2,
                    "amount": 20,
                    "header__change_oper": "I",
                    "header__change_seq": "0001",
                    "edw_inserted_dtm": datetime(2025, 3, 20),
                },
                {
                    "txn_id": 1,
                    "amount": 99,
                    "header__change_oper": "U",
                    "header__change_seq": "0002",
                    "edw_inserted_dtm": datetime(2025, 1, 15),
                },
            ]
        )
    )
    pipeline._rebuild_silver()  # loads txn 1
    pipeline._merge_cdc()  # inserts txn 2, updates txn 1

    out = pl.from_arrow(DeltaTable(pipeline.silver_uri).to_pyarrow_table()).sort("txn_id")
    assert out.get_column("txn_id").to_list() == [1, 2]
    assert out.get_column("amount").to_list() == [99, 20]
    assert out.get_column("odin_year").to_list() == [2025, 2025]
    assert out.get_column("odin_month").to_list() == [1, 3]


def test_snapshot_watermark_readable_on_wide_table(job):
    """
    odin_snapshot stays readable from stats even as a trailing column on a wide table.

    delta-rs only indexes the first 32 columns by default; without sizing the
    indexed-column count to the full width, column_max(odin_snapshot) returns 0
    and run() rebuilds every time.
    """
    pipeline, write_history, _, _ = job
    ncol = 40
    schema = pa.schema(
        [
            pa.field("txn_id", pa.int64()),
            pa.field("header__change_seq", pa.large_string()),
            pa.field("header__change_oper", pa.large_string()),
            *[pa.field(f"c{i}", pa.int64()) for i in range(ncol)],
        ]
    )
    rows = [
        {
            "txn_id": k,
            "header__change_seq": None,
            "header__change_oper": "L",
            **{f"c{i}": 0 for i in range(ncol)},
        }
        for k in (1, 2)
    ]
    write_history(pa.Table.from_pylist(rows, schema=schema))
    pipeline._rebuild_silver()

    assert column_max(pipeline.silver, "odin_snapshot") == TEST_SNAPSHOT


def test_rebuild_silver_without_load_records_raises(job):
    """A snapshot containing no L records is treated as an error."""
    pipeline, write_history, _, _ = job
    write_history(
        history_rows(
            [
                {
                    "txn_id": 1,
                    "amount": 10,
                    "header__change_oper": "I",
                    "header__change_seq": "0001",
                }
            ]
        )
    )
    with pytest.raises(AssertionError, match="no L"):
        pipeline._rebuild_silver()


def test_merge_cdc_insert_adds_new_key(job):
    """An I record for a new key is inserted into silver."""
    pipeline, write_history, write_silver, read_silver = job
    write_silver(silver_rows([{"txn_id": 1, "amount": 10, "status": "a"}]))
    write_history(
        history_rows(
            [
                {"txn_id": 1, "amount": 10, "status": "a", "header__change_oper": "L"},
                {
                    "txn_id": 2,
                    "amount": 20,
                    "status": "b",
                    "header__change_oper": "I",
                    "header__change_seq": "0001",
                },
            ]
        )
    )
    pipeline._merge_cdc()

    out = read_silver()
    assert out.get_column("txn_id").to_list() == [1, 2]
    assert out.filter(pl.col("txn_id") == 2).get_column("amount").to_list() == [20]


def test_merge_cdc_update_coalesces_sparse_columns(job):
    """A U record updates supplied columns and retains existing values for nulls."""
    pipeline, write_history, write_silver, read_silver = job
    write_silver(silver_rows([{"txn_id": 1, "amount": 10, "status": "a"}]))
    write_history(
        history_rows(
            [
                {"txn_id": 1, "amount": 10, "status": "a", "header__change_oper": "L"},
                {
                    "txn_id": 1,
                    "amount": 99,
                    "status": None,  # sparse: status should remain "a"
                    "header__change_oper": "U",
                    "header__change_seq": "0001",
                },
            ]
        )
    )
    pipeline._merge_cdc()

    row = read_silver().filter(pl.col("txn_id") == 1)
    assert row.get_column("amount").to_list() == [99]
    assert row.get_column("status").to_list() == ["a"]


def test_merge_cdc_delete_removes_key(job):
    """A D record deletes the matching silver row."""
    pipeline, write_history, write_silver, read_silver = job
    write_silver(
        silver_rows(
            [
                {"txn_id": 1, "amount": 10, "status": "a"},
                {"txn_id": 2, "amount": 20, "status": "b"},
            ]
        )
    )
    write_history(
        history_rows(
            [
                {"txn_id": 1, "amount": 10, "status": "a", "header__change_oper": "L"},
                {"txn_id": 2, "amount": 20, "status": "b", "header__change_oper": "L"},
                {
                    "txn_id": 2,
                    "header__change_oper": "D",
                    "header__change_seq": "0001",
                },
            ]
        )
    )
    pipeline._merge_cdc()

    assert read_silver().get_column("txn_id").to_list() == [1]


def test_merge_cdc_no_pending_leaves_silver_untouched(job):
    """When all CDC is below the silver watermark, silver is unchanged."""
    pipeline, write_history, write_silver, read_silver = job
    write_silver(
        silver_rows(
            [
                {
                    "txn_id": 1,
                    "amount": 10,
                    "status": "a",
                    "header__change_seq": "0005",
                }
            ]
        )
    )
    write_history(
        history_rows(
            [
                {"txn_id": 1, "amount": 10, "status": "a", "header__change_oper": "L"},
                {
                    "txn_id": 1,
                    "amount": 99,
                    "header__change_oper": "U",
                    "header__change_seq": "0001",  # below watermark 0005
                },
            ]
        )
    )
    interval = pipeline._merge_cdc()

    assert read_silver().get_column("amount").to_list() == [10]
    assert interval > 0
