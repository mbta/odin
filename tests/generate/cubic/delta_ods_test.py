"""
Tests for CubicODSDelta (Delta-based ODS silver materialization).

Correctness lives in two steps: rebuilding silver from a snapshot's "L" records
and the CDC MERGE. These tests write a real on-disk parquet "history" dataset
(hive-partitioned by ``snapshot=``, as cubic_archive.py produces) and a real
local Delta silver table, then call the steps directly and assert on silver's
contents. DuckDB reads the parquet; delta-rs writes the silver table.
"""

from typing import Any
from unittest.mock import patch

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from deltalake import DeltaTable, write_deltalake

from odin.generate.cubic.delta_ods import CubicODSDelta


KEYS = ["txn_id"]
TEST_SNAPSHOT = "20250101T000000Z"
FROM_CSV = "s3://archive/cubic/ods_qlik/EDW.TEST_TABLE__ct/test.csv.gz"

# History file schema (the hive ``snapshot`` column is derived from the path).
# The lists are annotated so mypy keeps them as list[pa.Field[Any]] rather than
# widening the heterogeneous ``pa.field(...)`` element types to list[object].
_HISTORY_FIELDS: "list[pa.Field[Any]]" = [
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
HISTORY_SCHEMA = pa.schema(_HISTORY_FIELDS)

_SILVER_FIELDS: "list[pa.Field[Any]]" = [
    pa.field("txn_id", pa.int64()),
    pa.field("amount", pa.int64()),
    pa.field("status", pa.large_string()),
    pa.field("header__change_seq", pa.large_string()),
    pa.field("odin_snapshot", pa.large_string()),
]
SILVER_SCHEMA = pa.schema(_SILVER_FIELDS)

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
    silver_dir = tmp_path / "silver"

    pipeline = CubicODSDelta("EDW.TEST_TABLE")
    pipeline.silver_uri = str(silver_dir)
    pipeline.history_snapshot = TEST_SNAPSHOT
    pipeline.history_root = f"{tmp_path}/history/"
    pipeline.part_columns = []

    def write_history(table: pa.Table) -> None:
        pq.write_table(table, str(history_file))
        # Populate history_columns the way _snapshot_check would (incl. hive snapshot).
        con = duckdb.connect()
        describe = con.execute(f"DESCRIBE SELECT * FROM {pipeline._read_history}").pl()
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
    pipeline._merge_cdc("0")  # inserts txn 2, updates txn 1

    out = pl.from_arrow(DeltaTable(pipeline.silver_uri).to_pyarrow_table()).sort("txn_id")
    assert out.get_column("txn_id").to_list() == [1, 2]
    assert out.get_column("amount").to_list() == [99, 20]
    assert out.get_column("odin_year").to_list() == [2025, 2025]
    assert out.get_column("odin_month").to_list() == [1, 3]


# Two partitions, each holding two keys whose ranges overlap the source key, so
# only a partition constraint (not key-stat pruning) can skip a file.
def _two_partition_history() -> "pa.Table":
    from datetime import datetime

    return dated_history_rows(
        [
            {
                "txn_id": 1,
                "amount": 10,
                "header__change_oper": "L",
                "edw_inserted_dtm": datetime(2024, 1, 5),
            },
            {
                "txn_id": 3,
                "amount": 30,
                "header__change_oper": "L",
                "edw_inserted_dtm": datetime(2024, 1, 6),
            },
            {
                "txn_id": 2,
                "amount": 20,
                "header__change_oper": "L",
                "edw_inserted_dtm": datetime(2025, 3, 5),
            },
            {
                "txn_id": 4,
                "amount": 40,
                "header__change_oper": "L",
                "edw_inserted_dtm": datetime(2025, 3, 6),
            },
        ]
    )


def test_merge_prunes_untouched_partitions(job):
    """A CDC update carrying edw_inserted_dtm skips files outside its partition."""
    from datetime import datetime

    pipeline, write_history, _, _ = job
    base = _two_partition_history()
    update = dated_history_rows(
        [
            {
                "txn_id": 2,
                "amount": 99,
                "header__change_oper": "U",
                "header__change_seq": "0001",
                "edw_inserted_dtm": datetime(2025, 3, 5),
            }
        ]
    )
    write_history(pa.concat_tables([base, update]))
    pipeline._rebuild_silver()

    cdc_df = pipeline._read_cdc("0", limit=100)
    source = pipeline._build_merge_source(cdc_df, KEYS)
    metrics = pipeline._merge_apply(source, KEYS, "0001")

    # The (2024, 1) file is skipped by the partition constraint, not key stats.
    assert metrics["num_target_files_skipped_during_scan"] >= 1
    out = pl.from_arrow(DeltaTable(pipeline.silver_uri).to_pyarrow_table()).sort("txn_id")
    assert out.get_column("txn_id").to_list() == [1, 2, 3, 4]
    assert out.filter(pl.col("txn_id") == 2).get_column("amount").to_list() == [99]


def test_read_cdc_keeps_tied_boundary_seqs_together(job):
    """The <= ceiling read returns all rows sharing the boundary seq, not a truncated k."""
    pipeline, write_history, _, _ = job
    write_history(
        history_rows(
            [
                {"txn_id": 1, "header__change_oper": "I", "header__change_seq": "0001"},
                {"txn_id": 2, "header__change_oper": "I", "header__change_seq": "0002"},
                {"txn_id": 3, "header__change_oper": "I", "header__change_seq": "0002"},
            ]
        )
    )
    # limit=2 lands the ceiling on the tied 0002; the whole <= 0002 range comes back.
    batch = pipeline._read_cdc("0", limit=2)
    assert batch.height == 3
    assert sorted(batch.get_column("header__change_seq").to_list()) == ["0001", "0002", "0002"]


def test_read_cdc_empty_when_caught_up(job):
    """No records past the watermark yields an empty batch (single narrow probe)."""
    pipeline, write_history, _, _ = job
    write_history(
        history_rows([{"txn_id": 1, "header__change_oper": "I", "header__change_seq": "0001"}])
    )
    assert pipeline._read_cdc("0005", limit=100).height == 0


def test_merge_no_prune_when_edw_missing(job):
    """A CDC update missing edw_inserted_dtm falls back to an unpruned scan, still correct."""
    pipeline, write_history, _, _ = job
    base = _two_partition_history()
    update = dated_history_rows(
        [{"txn_id": 2, "amount": 99, "header__change_oper": "U", "header__change_seq": "0001"}]
    )
    write_history(pa.concat_tables([base, update]))
    pipeline._rebuild_silver()

    cdc_df = pipeline._read_cdc("0", limit=100)
    source = pipeline._build_merge_source(cdc_df, KEYS)
    metrics = pipeline._merge_apply(source, KEYS, "0001")

    # No partition constraint, and key ranges overlap, so no file can be skipped.
    assert metrics["num_target_files_skipped_during_scan"] == 0
    out = pl.from_arrow(DeltaTable(pipeline.silver_uri).to_pyarrow_table()).sort("txn_id")
    row2 = out.filter(pl.col("txn_id") == 2)
    assert row2.get_column("amount").to_list() == [99]
    # Partition preserved from the retained edw_inserted_dtm (not clobbered to 0).
    assert row2.get_column("odin_year").to_list() == [2025]
    assert row2.get_column("odin_month").to_list() == [3]


def test_rebuild_records_snapshot_and_initial_watermark(job):
    """Rebuild records the snapshot and a reset watermark in commit metadata."""
    pipeline, write_history, _, _ = job
    write_history(history_rows([{"txn_id": 1, "amount": 10, "header__change_oper": "L"}]))
    pipeline._rebuild_silver()

    assert pipeline._read_state() == (TEST_SNAPSHOT, "0")


def test_state_readable_on_wide_table(job):
    """
    Recorded position is read from commit metadata, so table width is irrelevant.

    (Contrast with reading a trailing column's stats, which delta-rs only collects
    for the first 32 columns by default.)
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

    assert pipeline._read_state()[0] == TEST_SNAPSHOT


def test_delete_only_batch_advances_watermark(job):
    """
    A CDC batch of only deletes still advances the recorded watermark.

    The deleted row holds the max header__change_seq, so a contents-derived
    watermark would regress and re-read the batch forever; the recorded position
    must move past it.
    """
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
                {"txn_id": 2, "header__change_oper": "D", "header__change_seq": "0005"},
            ]
        )
    )
    pipeline._merge_cdc("0")

    assert read_silver().get_column("txn_id").to_list() == [1]
    # Watermark advanced past the delete even though its row is gone from silver.
    assert pipeline._read_state() == (TEST_SNAPSHOT, "0005")
    # Re-running from the recorded watermark is a no-op (batch not re-read).
    pipeline._merge_cdc("0005")
    assert read_silver().get_column("txn_id").to_list() == [1]


def test_rebuild_silver_without_load_records_raises(job):
    """
    A snapshot containing no L records raises BEFORE silver is overwritten.

    The check must precede the overwrite: a post-write failure would leave an
    empty silver whose recorded snapshot has already advanced, so the next run
    would not know to rebuild it.
    """
    pipeline, write_history, write_silver, read_silver = job
    write_silver(silver_rows([{"txn_id": 7, "amount": 70, "status": "z"}]))
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
    # The existing silver table was not touched.
    assert read_silver().get_column("txn_id").to_list() == [7]


def test_rebuild_silver_null_edw_on_partitioned_table_raises(job):
    """On a partitioned table, an L record with a null edw_inserted_dtm raises pre-write."""
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
                # Null edw_inserted_dtm would land this row in the unreachable
                # odin_year=0 partition.
                {"txn_id": 2, "amount": 20, "header__change_oper": "L"},
            ]
        )
    )
    with pytest.raises(AssertionError, match="edw_inserted_dtm"):
        pipeline._rebuild_silver()


def test_merge_cdc_insert_with_null_edw_raises(job):
    """On a partitioned table, an insertable CDC record with null edw_inserted_dtm raises."""
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
                # Full-image I record missing edw_inserted_dtm: inserting it would
                # put the row in the unreachable odin_year=0 partition.
                {
                    "txn_id": 2,
                    "amount": 20,
                    "header__change_oper": "I",
                    "header__change_seq": "0001",
                },
            ]
        )
    )
    pipeline._rebuild_silver()
    with pytest.raises(AssertionError, match="edw_inserted_dtm"):
        pipeline._merge_cdc("0")


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
    pipeline._merge_cdc("0")

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
    pipeline._merge_cdc("0")

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
    pipeline._merge_cdc("0")

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
    interval = pipeline._merge_cdc("0005")

    assert read_silver().get_column("amount").to_list() == [10]
    assert interval > 0


def test_merge_cdc_sparse_update_keeps_partition_values(job):
    """
    A sparse U without edw_inserted_dtm must not move the row to partition 0/0.

    The merge source derives odin_year/odin_month from the batch's coalesced
    edw_inserted_dtm, which is null here (degrading to 0), while the data-column
    coalesce keeps the target's real edw_inserted_dtm. The update must keep the
    target's partition values so partition and data stay consistent.
    """
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
                    "txn_id": 1,
                    "amount": 99,
                    "header__change_oper": "U",
                    "header__change_seq": "0001",
                    # edw_inserted_dtm omitted: sparse update
                },
            ]
        )
    )
    pipeline._rebuild_silver()
    pipeline._merge_cdc("0")

    out = pl.from_arrow(DeltaTable(pipeline.silver_uri).to_pyarrow_table())
    assert out.get_column("amount").to_list() == [99]
    assert out.get_column("edw_inserted_dtm").to_list() == [datetime(2025, 1, 15)]
    assert out.get_column("odin_year").to_list() == [2025]
    assert out.get_column("odin_month").to_list() == [1]


def test_merge_cdc_null_change_seq_raises(job):
    """
    A CDC record with a null header__change_seq raises instead of being skipped.

    The watermark comparison (seq > ?) would silently exclude NULL sequences in
    SQL; the read must surface them so the invariant assertion can reject the
    batch rather than dropping the record forever.
    """
    pipeline, write_history, write_silver, _ = job
    write_silver(silver_rows([{"txn_id": 1, "amount": 10, "status": "a"}]))
    write_history(
        history_rows(
            [
                {"txn_id": 1, "amount": 10, "status": "a", "header__change_oper": "L"},
                {
                    "txn_id": 2,
                    "amount": 20,
                    "header__change_oper": "I",
                    "header__change_seq": None,
                },
            ]
        )
    )
    with pytest.raises(AssertionError, match="null header__change_seq"):
        pipeline._merge_cdc("0")


def test_build_merge_source_one_row_per_key_with_correct_resolution(job):
    """
    The merge source carries exactly one row per key across all op mixes.

    Guards the reset-event resolution: each key's latest I or D decides its
    odin_resolved_oper (U when the batch has neither), with folded values.
    """
    pipeline, write_history, _, _ = job
    write_history(
        history_rows(
            [
                # key 1: I then sparse U -> resolves I (insert image + overlay)
                {
                    "txn_id": 1,
                    "amount": 10,
                    "status": "a",
                    "header__change_oper": "I",
                    "header__change_seq": "0001",
                },
                {
                    "txn_id": 1,
                    "amount": 99,
                    "header__change_oper": "U",
                    "header__change_seq": "0002",
                },
                # key 2: U only -> resolves U (sparse patch)
                {
                    "txn_id": 2,
                    "amount": 20,
                    "header__change_oper": "U",
                    "header__change_seq": "0003",
                },
                # key 3: I only -> resolves I
                {
                    "txn_id": 3,
                    "amount": 30,
                    "status": "c",
                    "header__change_oper": "I",
                    "header__change_seq": "0004",
                },
                # key 4: I then D -> resolves D
                {
                    "txn_id": 4,
                    "amount": 40,
                    "header__change_oper": "I",
                    "header__change_seq": "0005",
                },
                {"txn_id": 4, "header__change_oper": "D", "header__change_seq": "0006"},
                # key 5: D then orphan U -> resolves D (trailing U dropped)
                {"txn_id": 5, "header__change_oper": "D", "header__change_seq": "0007"},
                {
                    "txn_id": 5,
                    "amount": 50,
                    "header__change_oper": "U",
                    "header__change_seq": "0008",
                },
            ]
        )
    )
    cdc_df = pipeline._read_cdc("0", limit=100)
    source = pipeline._build_merge_source(cdc_df, KEYS).sort("txn_id")

    assert source.get_column("txn_id").to_list() == [1, 2, 3, 4, 5]
    assert source.get_column("odin_resolved_oper").to_list() == ["I", "U", "I", "D", "D"]
    # Watermark lineage: each key carries its highest seq, even for D keys.
    assert source.get_column("header__change_seq").to_list() == [
        "0002",
        "0003",
        "0004",
        "0006",
        "0008",
    ]
    # Key 1's fold: latest non-null amount wins, status from the I base image.
    row1 = source.filter(pl.col("txn_id") == 1)
    assert row1.get_column("amount").to_list() == [99]
    assert row1.get_column("status").to_list() == ["a"]


def test_merge_cdc_reinsert_applies_insert_image_verbatim(job):
    """
    D→I for an existing key in one batch: the row becomes exactly the I image.

    I records are full row images, so a NULL in the reinserted image means NULL.
    The matched-update must not coalesce against the target (which would
    resurrect the pre-delete status), and the batch fold must not backfill from
    the D record's pre-delete image.
    """
    pipeline, write_history, write_silver, read_silver = job
    write_silver(silver_rows([{"txn_id": 1, "amount": 10, "status": "a"}]))
    write_history(
        history_rows(
            [
                {"txn_id": 1, "amount": 10, "status": "a", "header__change_oper": "L"},
                # D carries the full pre-delete image, as Qlik deletes often do.
                {
                    "txn_id": 1,
                    "amount": 10,
                    "status": "a",
                    "header__change_oper": "D",
                    "header__change_seq": "0001",
                },
                # Reinsert with status legitimately NULL.
                {
                    "txn_id": 1,
                    "amount": 50,
                    "status": None,
                    "header__change_oper": "I",
                    "header__change_seq": "0002",
                },
            ]
        )
    )
    pipeline._merge_cdc("0")

    row = read_silver()
    assert row.get_column("txn_id").to_list() == [1]
    assert row.get_column("amount").to_list() == [50]
    assert row.get_column("status").to_list() == [None]


def test_merge_cdc_reinsert_new_key_not_backfilled_from_older_records(job):
    """
    I→D→I for a new key in one batch: the inserted row is the final I image only.

    NULLs in the winning insert image must not be backfilled from the key's
    earlier records in the batch (the first insert's values).
    """
    pipeline, write_history, write_silver, read_silver = job
    write_silver(silver_rows([{"txn_id": 9, "amount": 90, "status": "z"}]))
    write_history(
        history_rows(
            [
                {"txn_id": 9, "amount": 90, "status": "z", "header__change_oper": "L"},
                {
                    "txn_id": 1,
                    "amount": 10,
                    "status": "a",
                    "header__change_oper": "I",
                    "header__change_seq": "0001",
                },
                {"txn_id": 1, "header__change_oper": "D", "header__change_seq": "0002"},
                {
                    "txn_id": 1,
                    "amount": 50,
                    "status": None,
                    "header__change_oper": "I",
                    "header__change_seq": "0003",
                },
            ]
        )
    )
    pipeline._merge_cdc("0")

    row = read_silver().filter(pl.col("txn_id") == 1)
    assert row.get_column("amount").to_list() == [50]
    assert row.get_column("status").to_list() == [None]


def test_merge_cdc_fold_does_not_reach_behind_insert_reset(job):
    """
    U→D→I→U in one batch: values older than the I must not leak forward.

    The I is a full-image reset. A column that is NULL in the I and untouched
    by the later sparse U is genuinely NULL — it must not be backfilled from
    the pre-reset U record, nor from the matched target row.
    """
    pipeline, write_history, write_silver, read_silver = job
    write_silver(silver_rows([{"txn_id": 1, "amount": 10, "status": "a"}]))
    write_history(
        history_rows(
            [
                {"txn_id": 1, "amount": 10, "status": "a", "header__change_oper": "L"},
                # Pre-reset update sets a status that must NOT survive the reset.
                {
                    "txn_id": 1,
                    "status": "zzz",
                    "header__change_oper": "U",
                    "header__change_seq": "0001",
                },
                {"txn_id": 1, "header__change_oper": "D", "header__change_seq": "0002"},
                # Reinsert with status legitimately NULL.
                {
                    "txn_id": 1,
                    "amount": 50,
                    "status": None,
                    "header__change_oper": "I",
                    "header__change_seq": "0003",
                },
                # Post-reset sparse update touching only amount.
                {
                    "txn_id": 1,
                    "amount": 60,
                    "header__change_oper": "U",
                    "header__change_seq": "0004",
                },
            ]
        )
    )
    pipeline._merge_cdc("0")

    row = read_silver()
    assert row.get_column("txn_id").to_list() == [1]
    assert row.get_column("amount").to_list() == [60]
    # NULL from the reinserted image: not "zzz" (pre-reset U), not "a" (target).
    assert row.get_column("status").to_list() == [None]


def test_merge_cdc_insert_reset_new_key_uses_final_image(job):
    """
    I→D→I→U for a new key: the inserted row is the second image plus the U.

    The first insert's values must not backfill NULLs in the reinserted image.
    """
    pipeline, write_history, write_silver, read_silver = job
    write_silver(silver_rows([{"txn_id": 9, "amount": 90, "status": "z"}]))
    write_history(
        history_rows(
            [
                {"txn_id": 9, "amount": 90, "status": "z", "header__change_oper": "L"},
                {
                    "txn_id": 1,
                    "amount": 10,
                    "status": "a",
                    "header__change_oper": "I",
                    "header__change_seq": "0001",
                },
                {"txn_id": 1, "header__change_oper": "D", "header__change_seq": "0002"},
                {
                    "txn_id": 1,
                    "amount": 50,
                    "status": None,
                    "header__change_oper": "I",
                    "header__change_seq": "0003",
                },
                {
                    "txn_id": 1,
                    "amount": 60,
                    "header__change_oper": "U",
                    "header__change_seq": "0004",
                },
            ]
        )
    )
    pipeline._merge_cdc("0")

    row = read_silver().filter(pl.col("txn_id") == 1)
    assert row.get_column("amount").to_list() == [60]
    assert row.get_column("status").to_list() == [None]


def test_merge_cdc_orphan_update_after_delete_drops_row(job):
    """
    D→U (no I) for an existing key: the delete wins; the orphan U is dropped.

    The latest reset event (the D) decides the key's action. A U with no
    subsequent insert image has no live row to patch — the same events split
    across two batches would delete the row and drop the U, and resolution
    must be batch-split invariant. (Legacy let the U resurrect the row when
    both fell in one batch — a batch-boundary-dependent outcome.)
    """
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
                    "txn_id": 1,
                    "amount": 10,
                    "status": "zzz",
                    "header__change_oper": "D",
                    "header__change_seq": "0001",
                },
                {
                    "txn_id": 1,
                    "amount": 99,
                    "header__change_oper": "U",
                    "header__change_seq": "0002",
                },
            ]
        )
    )
    pipeline._merge_cdc("0")

    # Row 1 deleted (orphan U dropped); row 2 untouched.
    assert read_silver().get_column("txn_id").to_list() == [2]
    # Watermark still advanced past the orphan U.
    assert pipeline._read_state() == (TEST_SNAPSHOT, "0002")


def test_db_connection_spills_to_disk_without_progress_bar(job):
    """The run-scoped connection can spill to disk and never prints progress."""
    pipeline, _, _, _ = job
    con = pipeline._db()

    def setting(name: str) -> object:
        return con.execute(f"SELECT current_setting('{name}')").fetchone()[0]

    assert str(setting("temp_directory")).endswith("duckdb_spill")
    assert setting("enable_progress_bar") is False
    # Run-scoped: repeated calls reuse the same connection.
    assert pipeline._db() is con


def test_partition_metrics_reports_touched_partitions(job):
    """Merge logging reports distinct partitions with row counts, oldest first."""
    from datetime import datetime

    pipeline, _, _, _ = job
    source = pl.DataFrame(
        {
            "edw_inserted_dtm": [
                datetime(2025, 3, 1),
                datetime(2024, 1, 5),
                datetime(2024, 1, 6),
            ],
            "odin_year": [2025, 2024, 2024],
            "odin_month": [3, 1, 1],
        }
    )
    metrics = pipeline._partition_metrics(source)
    assert metrics == {
        "partitions_touched": 2,
        "partition_rows": "2024-01=2,2025-03=1",
        "partition_scan_pruned": True,
    }


def test_partition_metrics_counts_unknown_partition_rows(job):
    """Rows without edw_inserted_dtm are counted as unknown and disable pruning."""
    from datetime import datetime

    pipeline, _, _, _ = job
    source = pl.DataFrame(
        {
            "edw_inserted_dtm": [datetime(2025, 3, 1), None],
            "odin_year": [2025, 0],
            "odin_month": [3, 0],
        }
    )
    metrics = pipeline._partition_metrics(source)
    assert metrics["partitions_touched"] == 1
    assert metrics["partition_rows"] == "2025-03=1"
    assert metrics["partition_scan_pruned"] is False
    assert metrics["partition_rows_unknown"] == 1


def test_partition_metrics_truncates_long_lists_and_skips_undated(job):
    """The per-partition list truncates beyond the limit; undated tables log nothing."""
    from datetime import datetime

    pipeline, _, _, _ = job
    n = pipeline.PARTITION_LOG_LIMIT + 2
    months = [datetime(2000 + i, 1 + i % 12, 1) for i in range(n)]
    source = pl.DataFrame(
        {
            "edw_inserted_dtm": months,
            "odin_year": [d.year for d in months],
            "odin_month": [d.month for d in months],
        }
    )
    metrics = pipeline._partition_metrics(source)
    assert metrics["partitions_touched"] == n
    assert metrics["partition_rows"].endswith(",+2 more")
    assert metrics["partition_rows"].count(",") == pipeline.PARTITION_LOG_LIMIT

    assert pipeline._partition_metrics(pl.DataFrame({"txn_id": [1]})) == {}


def test_merge_predicate_plain_equality_for_null_free_keys(job):
    """
    Null-free source keys produce a plain-equality predicate.

    delta-rs only derives an early-pruning predicate from source key stats for
    simple equality conjunctions; the null-safe OR form must appear only when
    the batch actually contains a null key value (and only on that key).
    """
    pipeline, _, _, _ = job
    clean = pl.DataFrame({"txn_id": [1, 2], "other_id": [5, 6]})
    assert pipeline._merge_predicate(["txn_id"], clean) == 'target."txn_id" = source."txn_id"'
    assert pipeline._merge_predicate(["txn_id", "other_id"], clean) == (
        'target."txn_id" = source."txn_id" AND target."other_id" = source."other_id"'
    )

    with_null = pl.DataFrame({"txn_id": [1, None], "other_id": [5, 6]})
    assert pipeline._merge_predicate(["txn_id", "other_id"], with_null) == (
        '(target."txn_id" = source."txn_id" '
        'OR (target."txn_id" IS NULL AND source."txn_id" IS NULL)) '
        'AND target."other_id" = source."other_id"'
    )


def test_merge_cdc_null_key_still_matches(job):
    """A null primary key falls back to null-safe matching (update, not duplicate)."""
    pipeline, write_history, write_silver, _ = job
    write_silver(silver_rows([{"txn_id": None, "amount": 10, "status": "a"}]))
    write_history(
        history_rows(
            [
                {"txn_id": None, "amount": 10, "status": "a", "header__change_oper": "L"},
                {
                    "txn_id": None,
                    "amount": 99,
                    "header__change_oper": "U",
                    "header__change_seq": "0001",
                },
            ]
        )
    )
    pipeline._merge_cdc("0")

    out = pl.from_arrow(DeltaTable(pipeline.silver_uri).to_pyarrow_table())
    assert out.height == 1
    assert out.get_column("amount").to_list() == [99]


def test_merge_cdc_quotes_reserved_word_columns(job):
    """MERGE expressions must quote identifiers so reserved-word columns work."""
    pipeline, write_history, _, _ = job
    schema = pa.schema(
        [
            pa.field("txn_id", pa.int64()),
            pa.field("order", pa.large_string()),
            pa.field("header__change_seq", pa.large_string()),
            pa.field("header__change_oper", pa.large_string()),
            pa.field("header__from_csv", pa.large_string()),
        ]
    )
    write_history(
        pa.Table.from_pylist(
            [
                {
                    "txn_id": 1,
                    "order": "a",
                    "header__change_oper": "L",
                    "header__from_csv": FROM_CSV,
                },
                {
                    "txn_id": 1,
                    "order": "b",
                    "header__change_oper": "U",
                    "header__change_seq": "0001",
                    "header__from_csv": FROM_CSV,
                },
                {
                    "txn_id": 2,
                    "order": "c",
                    "header__change_oper": "I",
                    "header__change_seq": "0002",
                    "header__from_csv": FROM_CSV,
                },
            ],
            schema=schema,
        )
    )
    pipeline._rebuild_silver()
    pipeline._merge_cdc("0")

    out = pl.from_arrow(DeltaTable(pipeline.silver_uri).to_pyarrow_table()).sort("txn_id")
    assert out.get_column("txn_id").to_list() == [1, 2]
    assert out.get_column("order").to_list() == ["b", "c"]
