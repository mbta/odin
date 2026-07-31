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

from odin.generate.cubic import delta_ods
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


def test_partition_constraint_names_exact_pairs_not_cross_product(job):
    """
    The constraint names the (year, month) pairs the batch touches, not year x month.

    Independent ``odin_year IN (...) AND odin_month IN (...)`` lists admit every
    combination of the two, so a batch touching 2024-01 and 2025-03 would also drag
    2024-03 and 2025-01 into the scan. Since the scan predicate is what bounds merge
    memory, that cross product is the difference between scanning two partitions and
    scanning dozens.
    """
    from datetime import datetime

    pipeline, write_history, _, _ = job
    base = _two_partition_history()
    updates = dated_history_rows(
        [
            {
                "txn_id": 1,
                "amount": 11,
                "header__change_oper": "U",
                "header__change_seq": "0001",
                "edw_inserted_dtm": datetime(2024, 1, 5),
            },
            {
                "txn_id": 2,
                "amount": 99,
                "header__change_oper": "U",
                "header__change_seq": "0002",
                "edw_inserted_dtm": datetime(2025, 3, 5),
            },
        ]
    )
    write_history(pa.concat_tables([base, updates]))
    pipeline._rebuild_silver()

    cdc_df = pipeline._read_cdc("0", limit=100)
    source = pipeline._build_merge_source(cdc_df, KEYS)
    constraint = pipeline._partition_constraint(source)

    assert "2024" in constraint and "2025" in constraint
    # Both real pairs are named...
    assert '"odin_year" = CAST(2024 AS INT) AND target."odin_month" = CAST(1 AS INT)' in constraint
    assert '"odin_year" = CAST(2025 AS INT) AND target."odin_month" = CAST(3 AS INT)' in constraint
    # ...and only those two, so the phantom 2024-03 / 2025-01 combinations are absent.
    assert constraint.count("odin_year") == 2


def test_merge_writes_stay_compressed(job):
    """
    Silver files keep SNAPPY compression despite passing explicit WriterProperties.

    deltalake builds WriterProperties from a bare parquet builder whose default is
    UNCOMPRESSED, and only falls back to its own SNAPPY default when no properties
    are passed at all. Setting any property therefore silently drops compression
    unless it is restated -- guard that, since nothing else would notice.
    """
    pipeline, write_history, _, _ = job
    write_history(
        history_rows(
            [
                {"txn_id": 1, "amount": 10, "status": "a", "header__change_oper": "L"},
                {"txn_id": 2, "amount": 20, "status": "b", "header__change_oper": "L"},
            ]
        )
    )
    pipeline._rebuild_silver()

    for uri in DeltaTable(pipeline.silver_uri).file_uris():
        metadata = pq.ParquetFile(uri).metadata
        for group in range(metadata.num_row_groups):
            assert metadata.row_group(group).column(0).compression == "SNAPPY"


def test_merge_writes_cap_row_group_size(job):
    """
    Written files split into bounded row groups rather than one 1M-row group.

    Each open parquet writer buffers the row group it is encoding in memory, and a
    write spans one writer per partition, so the row group cap is what keeps the
    write side's resident memory flat as partition count grows.
    """
    from odin.generate.cubic.delta_ods import DELTA_WRITER_PROPERTIES

    cap = DELTA_WRITER_PROPERTIES.max_row_group_size
    assert cap is not None

    pipeline, write_history, _, _ = job
    rows = cap * 2 + 1000
    write_history(
        pa.Table.from_pydict(
            {
                "txn_id": list(range(rows)),
                "amount": [1] * rows,
                "status": ["a"] * rows,
                "header__change_seq": [None] * rows,
                "header__change_oper": ["L"] * rows,
                "header__year": [2025] * rows,
                "header__month": [1] * rows,
                "header__timestamp": [None] * rows,
                "header__from_csv": [FROM_CSV] * rows,
            },
            schema=HISTORY_SCHEMA,
        )
    )
    pipeline._rebuild_silver()

    for uri in DeltaTable(pipeline.silver_uri).file_uris():
        metadata = pq.ParquetFile(uri).metadata
        for group in range(metadata.num_row_groups):
            assert metadata.row_group(group).num_rows <= cap


def test_merge_cdc_releases_duckdb_before_merging(job):
    """
    The DuckDB connection is handed back before the merge runs.

    DuckDB is configured with a buffer pool of half of system memory. Holding it
    open through the merge leaves delta-rs competing with memory that nothing is
    using any more -- the CDC batch is fully materialized in polars by then.
    """
    pipeline, write_history, _, _ = job
    write_history(
        history_rows(
            [
                {"txn_id": 1, "amount": 10, "status": "a", "header__change_oper": "L"},
                {
                    "txn_id": 1,
                    "amount": 99,
                    "header__change_oper": "U",
                    "header__change_seq": "0001",
                },
            ]
        )
    )
    pipeline._rebuild_silver()

    seen = {}
    real_merge_apply = pipeline._merge_apply

    def spy(*args, **kwargs):
        seen["con_open_during_merge"] = pipeline._con is not None
        return real_merge_apply(*args, **kwargs)

    with patch.object(pipeline, "_merge_apply", side_effect=spy):
        pipeline._merge_cdc("0")

    assert seen["con_open_during_merge"] is False


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


def test_watermark_recorded_when_a_chunk_matches_nothing(job):
    """
    The watermark survives a partition chunk that produces no Delta commit.

    delta-rs makes no commit at all for a MERGE with zero actions, so a watermark
    riding on a partition merge vanishes whenever that chunk matches nothing --
    ordinary here, since orphan updates are dropped by design. The batch would
    then replay forever while the status file reports an advancing position.
    """
    from datetime import datetime

    pipeline, write_history, _, _ = job
    base = dated_history_rows(
        [
            {
                "txn_id": 1,
                "amount": 10,
                "header__change_oper": "L",
                "edw_inserted_dtm": datetime(2025, 3, 5),
            }
        ]
    )
    cdc = dated_history_rows(
        [
            # real update, lands in the (2025, 3) chunk
            {
                "txn_id": 1,
                "amount": 99,
                "header__change_oper": "U",
                "header__change_seq": "0001",
                "edw_inserted_dtm": datetime(2025, 3, 5),
            },
            # orphan update for a key with no target row, alone in a (2026, 7)
            # chunk: matches nothing, so that merge commits nothing
            {
                "txn_id": 777,
                "amount": 5,
                "header__change_oper": "U",
                "header__change_seq": "0002",
                "edw_inserted_dtm": datetime(2026, 7, 1),
            },
        ]
    )
    write_history(pa.concat_tables([base, cdc]))
    pipeline._rebuild_silver()
    pipeline._merge_cdc("0")

    assert pipeline._read_state() == (TEST_SNAPSHOT, "0002")
    # And the real update still landed.
    out = pl.from_arrow(DeltaTable(pipeline.silver_uri).to_pyarrow_table())
    assert out.filter(pl.col("txn_id") == 1).get_column("amount").to_list() == [99]


def test_read_state_survives_an_interrupted_run(job):
    """
    A run interrupted between partition merges does not force a full rebuild.

    Each partition merge commits on its own without recording a position, so an
    interruption buries the last recorded position under them. Reporting "no
    position" there reads as a snapshot mismatch upstream and rebuilds the whole
    table, so the scan has to reach the end of the log before concluding that.
    """
    pipeline, write_history, write_silver, _ = job
    write_history(history_rows([{"txn_id": 1, "amount": 10, "header__change_oper": "L"}]))
    pipeline._rebuild_silver()
    assert pipeline._read_state() == (TEST_SNAPSHOT, "0")

    # Simulate the debris of an interrupted chunked merge: position-less commits.
    from odin.generate.cubic.delta_ods import HISTORY_SCAN_LIMIT

    empty = pa.Table.from_pylist(
        [], schema=pa.schema(DeltaTable(pipeline.silver_uri).schema().to_arrow())
    )
    for _ in range(HISTORY_SCAN_LIMIT + 5):
        write_deltalake(pipeline.silver_uri, empty, mode="append")

    # run() re-opens the table before reading state; _read_state on a handle pinned
    # to an older version would not see the commits piled on top at all.
    pipeline.silver = DeltaTable(pipeline.silver_uri)
    assert pipeline.silver.version() > HISTORY_SCAN_LIMIT
    assert pipeline._read_state() == (TEST_SNAPSHOT, "0")


def test_partition_changing_key_is_rejected_not_duplicated(job):
    """
    A key whose edw_inserted_dtm changes is caught rather than silently duplicated.

    Each partition merge scans only its own partition, so a key whose target row
    lives elsewhere is not seen and gets inserted alongside the old row. The batch
    must be rejected instead.
    """
    from datetime import datetime

    pipeline, write_history, _, _ = job
    base = dated_history_rows(
        [
            {
                "txn_id": 1,
                "amount": 10,
                "header__change_oper": "L",
                "edw_inserted_dtm": datetime(2024, 1, 5),
            },
        ]
    )
    cdc = dated_history_rows(
        [
            {
                "txn_id": 1,
                "amount": 11,
                "header__change_oper": "U",
                "header__change_seq": "0001",
                "edw_inserted_dtm": datetime(2024, 1, 5),
            },
            # same key, different edw_inserted_dtm -> would move partitions
            {
                "txn_id": 1,
                "amount": 12,
                "header__change_oper": "I",
                "header__change_seq": "0002",
                "edw_inserted_dtm": datetime(2025, 3, 5),
            },
        ]
    )
    write_history(pa.concat_tables([base, cdc]))
    pipeline._rebuild_silver()

    with pytest.raises(AssertionError, match="more than one edw_inserted_dtm"):
        pipeline._merge_cdc("0")


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


# --- File layout: target file size, key order, reclustering -------------------

# Silver shape used by the reclustering tests: a key column plus the partition
# columns, which is all the layout logic looks at.
_RECLUSTER_FIELDS: "list[pa.Field[Any]]" = [
    pa.field("txn_id", pa.int64()),
    pa.field("amount", pa.int64()),
    pa.field("odin_year", pa.int32()),
    pa.field("odin_month", pa.int32()),
]
RECLUSTER_SCHEMA = pa.schema(_RECLUSTER_FIELDS)


# Eight files, every one spanning nearly the whole key range, so seven overlap a
# file ordering before them — past the threshold for a unit of this size.
WIDE_RANGE_FILES = [[(i, 1), (101 - i, 1)] for i in range(1, 9)]
WIDE_RANGE_KEYS = sorted(k for batch in WIDE_RANGE_FILES for k, _ in batch)


def write_silver_files(
    uri: str, batches: list[list[tuple[int, int]]], partitioned: bool = True
) -> None:
    """
    Write one silver data file per batch of (txn_id, month) pairs.

    Each append commits its own file, which is how a unit ends up holding several
    files with overlapping key ranges.
    """
    for index, batch in enumerate(batches):
        table = pa.Table.from_pylist(
            [
                {"txn_id": txn_id, "amount": txn_id, "odin_year": 2025, "odin_month": month}
                for txn_id, month in batch
            ],
            schema=RECLUSTER_SCHEMA,
        )
        write_deltalake(
            uri,
            table,
            mode="overwrite" if index == 0 else "append",
            partition_by=["odin_year", "odin_month"] if partitioned else None,
        )


def recluster_source(
    partitions: list[tuple[int, int]] | None = None, oper: str = "I"
) -> pl.DataFrame:
    """Minimal merge source, naming the partitions its resolved rows land in."""
    if partitions is None:  # unpartitioned table: no partition columns to carry
        return pl.DataFrame(
            {"odin_resolved_oper": [oper]}, schema={"odin_resolved_oper": pl.String}
        )
    return pl.DataFrame(
        {
            "odin_resolved_oper": [oper] * len(partitions),
            "odin_year": [y for y, _ in partitions],
            "odin_month": [m for _, m in partitions],
        },
        schema={"odin_resolved_oper": pl.String, "odin_year": pl.Int32, "odin_month": pl.Int32},
    )


def test_ensure_target_file_size_sets_then_leaves_property(job):
    """The property is committed once and the call is a no-op afterwards."""
    pipeline, write_history, _, _ = job
    write_history(history_rows([{"txn_id": 1, "amount": 10, "header__change_oper": "L"}]))
    pipeline._rebuild_silver()

    pipeline.silver = DeltaTable(pipeline.silver_uri)
    pipeline._ensure_target_file_size()
    config = DeltaTable(pipeline.silver_uri).metadata().configuration
    assert config["delta.targetFileSize"] == str(delta_ods.TARGET_FILE_SIZE_BYTES)

    version = DeltaTable(pipeline.silver_uri).version()
    pipeline._ensure_target_file_size()
    assert DeltaTable(pipeline.silver_uri).version() == version  # no second commit


def test_discover_keys_ordered_by_primary_key_position():
    """keys[0] is the DFM's leading key, not the first key column in the schema."""
    pipeline = CubicODSDelta("EDW.TEST_TABLE")
    cdc_df = pl.DataFrame({"txn_id": [1], "status": ["a"], "header__from_csv": [FROM_CSV]})
    # "status" is primaryKeyPos 1 but appears after "txn_id" among the columns.
    with patch(
        "odin.generate.cubic.delta_ods.dfm_from_s3",
        return_value=mock_dfm_for_keys(["status", "txn_id"]),
    ):
        assert pipeline._discover_keys(cdc_df) == ["status", "txn_id"]


def test_overlapping_files_counts_out_of_order_files():
    """Disjoint ranges score 0; each file overlapping an earlier one adds 1."""
    ordered = pl.DataFrame({"min_key": [1, 11, 21], "max_key": [10, 20, 30]})
    assert CubicODSDelta._overlapping_files(ordered) == 0

    overlapping = pl.DataFrame({"min_key": [1, 1, 1, 1], "max_key": [100, 100, 100, 100]})
    assert CubicODSDelta._overlapping_files(overlapping) == 3

    # A file with no statistics makes the partition unmeasurable, not "ordered".
    unknown = pl.DataFrame({"min_key": [1, None], "max_key": [10, None]})
    assert CubicODSDelta._overlapping_files(unknown) is None


def test_recluster_reorders_disordered_partition(job):
    """
    A partition whose files overlap is z-ordered, and comes back key-ordered.

    The row-order assertion is the point: reclustering relies on a single-column
    z-order being equivalent to a sort by that column (nothing to interleave), and
    that is a delta-rs behaviour rather than a documented guarantee. If it ever
    changes, merges silently lose file skipping and get slower — this test is how
    that surfaces. Overlap alone would not catch it, since z-order also bin-packs
    small files and a single output file trivially overlaps nothing.
    """
    pipeline, _, _, _ = job
    pipeline.part_columns = ["odin_year", "odin_month"]
    write_silver_files(pipeline.silver_uri, WIDE_RANGE_FILES)
    pipeline.silver = DeltaTable(pipeline.silver_uri)
    before = pipeline._overlapping_files(
        pipeline._add_action_key_ranges("txn_id").select("min_key", "max_key")
    )
    assert before >= delta_ods._recluster_threshold(len(WIDE_RANGE_FILES))

    metrics = pipeline._recluster_inserted_partitions(recluster_source([(2025, 1)]), ["txn_id"])

    assert metrics["recluster_units"] == 1
    after = pipeline._overlapping_files(
        pipeline._add_action_key_ranges("txn_id").select("min_key", "max_key")
    )
    assert after == 0
    # Layout only: the same rows, now in key order.
    out = DeltaTable(pipeline.silver_uri).to_pyarrow_table().column("txn_id").to_pylist()
    assert out == WIDE_RANGE_KEYS


def test_recluster_leaves_ordered_partition_alone(job):
    """Below the overlap threshold nothing is rewritten."""
    pipeline, _, _, _ = job
    pipeline.part_columns = ["odin_year", "odin_month"]
    write_silver_files(
        pipeline.silver_uri, [[(1, 1), (10, 1)], [(11, 1), (20, 1)], [(21, 1), (30, 1)]]
    )
    pipeline.silver = DeltaTable(pipeline.silver_uri)
    version = DeltaTable(pipeline.silver_uri).version()

    metrics = pipeline._recluster_inserted_partitions(recluster_source([(2025, 1)]), ["txn_id"])

    assert metrics["recluster_units"] == 0
    assert DeltaTable(pipeline.silver_uri).version() == version  # no rewrite committed


def test_recluster_skips_partitions_without_inserts(job):
    """Updates and deletes preserve a file's key range, so they trigger nothing."""
    pipeline, _, _, _ = job
    pipeline.part_columns = ["odin_year", "odin_month"]
    write_silver_files(
        pipeline.silver_uri,
        [
            [(1, 1), (100, 1)],
            [(2, 1), (99, 1)],
            [(3, 1), (98, 1)],
            [(4, 1), (97, 1)],
            [(5, 1), (96, 1)],
        ],
    )
    pipeline.silver = DeltaTable(pipeline.silver_uri)
    version = DeltaTable(pipeline.silver_uri).version()

    metrics = pipeline._recluster_inserted_partitions(
        recluster_source([(2025, 1)], oper="U"), ["txn_id"]
    )

    assert metrics == {}
    assert DeltaTable(pipeline.silver_uri).version() == version


def test_recluster_sorts_unpartitioned_table_as_one_unit(job):
    """
    With no partition columns the whole table is the unit, and it still reclusters.

    An unpartitioned table needs this more than a partitioned one: the merge has no
    partition constraint, so the key range is the only thing that can skip a file.
    """
    pipeline, _, _, _ = job
    pipeline.part_columns = []
    write_silver_files(pipeline.silver_uri, WIDE_RANGE_FILES, partitioned=False)
    pipeline.silver = DeltaTable(pipeline.silver_uri)

    metrics = pipeline._recluster_inserted_partitions(recluster_source(), ["txn_id"])

    assert metrics["recluster_units"] == 1
    assert metrics["recluster_before"].startswith("table=")
    assert pipeline._overlapping_files(pipeline._add_action_key_ranges("txn_id")) == 0
    out = DeltaTable(pipeline.silver_uri).to_pyarrow_table().column("txn_id").to_pylist()
    assert out == WIDE_RANGE_KEYS


def test_recluster_threshold_scales_with_file_count():
    """Bigger units need proportionally more damage before a rewrite repays itself."""
    # Small units keep the floor, and can never reach it: overlap tops out at N-1.
    assert delta_ods._recluster_threshold(1) == 4
    assert delta_ods._recluster_threshold(4) == 4
    # Past that the 2*sqrt(N) break-even governs, so large units recluster rarely
    # rather than never.
    assert delta_ods._recluster_threshold(100) == 20
    assert delta_ods._recluster_threshold(2500) == 100


def test_recluster_reports_missing_key_statistics(job):
    """A key with no per-file stats cannot be made prunable, so no rewrite happens."""
    pipeline, _, _, _ = job
    pipeline.part_columns = ["odin_year", "odin_month"]
    write_silver_files(pipeline.silver_uri, [[(1, 1)], [(2, 1)]])
    pipeline.silver = DeltaTable(pipeline.silver_uri)
    version = DeltaTable(pipeline.silver_uri).version()

    metrics = pipeline._recluster_inserted_partitions(
        recluster_source([(2025, 1)]), ["not_a_column"]
    )

    assert metrics == {"recluster_no_key_stats": True}
    assert DeltaTable(pipeline.silver_uri).version() == version
