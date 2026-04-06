"""
Tests for CubicODSFact CDC handling logic.

Integration tests for load_cdc_records():
  Local parquet on disk; S3 boundary functions patched to local equivalents.
  Validates the full apply-CDC-to-fact pipeline end-to-end.

Coverage includes:
  - Single-operation batches (pure I, U, D)
  - Mixed-operation batches (different keys)
  - Same-key conflict resolution (I→U, D→I, D→U, multi-U sparse merge)
  - B-record filtering
  - Null-means-no-change sparse update semantics

Assumptions documented by the test suite (per Qlik CDC spec §2.4.2):
  - A NULL value in a "U" record means "no change to this column", NOT
    "set this column to NULL".  This assumption is implicit in the code's
    null-skipping filter and is pinned by test_load_cdc_null_update_not_applied.
"""

import datetime
import os
import shutil
from unittest.mock import patch

import polars as pl
import pytest

from odin.generate.cubic.ods_fact import (
    NEXT_RUN_DEFAULT,
    NEXT_RUN_LONG,
    CubicODSFact,
)
from odin.utils.aws.s3 import S3Object
from odin.utils.parquet import ds_from_path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

KEYS = ["txn_id"]
TEST_SNAPSHOT = "20250101T000000Z"

# Relative path (no leading slash) so f"s3://{s3_export}/" is valid.
S3_EXPORT_PREFIX = "test-bucket/odin/data/cubic/ods/EDW.TEST_TABLE"


# ---------------------------------------------------------------------------
# Schema / DataFrame helpers
# ---------------------------------------------------------------------------

CDC_SCHEMA = {
    "txn_id": pl.Int64,
    "amount": pl.Int64,
    "status": pl.Utf8,
    "header__change_seq": pl.Utf8,
    "header__change_oper": pl.Utf8,
    "header__from_csv": pl.Utf8,
    "header__year": pl.Int32,
    "header__month": pl.Int32,
    "header__timestamp": pl.Utf8,
    "snapshot": pl.Utf8,
}

# Columns retained in the fact table after snapshot load / CDC apply.
# Mirrors history_drop_columns removal from the full CDC schema.
FACT_SCHEMA = {
    "txn_id": pl.Int64,
    "amount": pl.Int64,
    "status": pl.Utf8,
    "header__change_seq": pl.Utf8,
    "odin_index": pl.Int64,
    "odin_snapshot": pl.Utf8,
}


def make_cdc_df(rows: list[dict]) -> pl.DataFrame:
    """Build a CDC DataFrame with the full history parquet schema."""
    defaults: dict = {
        "txn_id": None,
        "amount": None,
        "status": None,
        "header__change_seq": None,
        "header__change_oper": None,
        "header__from_csv": "s3://archive/processed/test.csv",
        "header__year": 2025,
        "header__month": 1,
        "header__timestamp": None,
        "snapshot": TEST_SNAPSHOT,
    }
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=CDC_SCHEMA)


def make_fact_df(rows: list[dict]) -> pl.DataFrame:
    """Build a fact DataFrame with the post-snapshot-load schema."""
    defaults: dict = {
        "txn_id": None,
        "amount": None,
        "status": None,
        "header__change_seq": None,
        "odin_index": None,
        "odin_snapshot": TEST_SNAPSHOT,
    }
    return pl.DataFrame([{**defaults, **r} for r in rows], schema=FACT_SCHEMA)


def mock_dfm_for_keys(keys: list[str]) -> dict:
    """Minimal DFM dict identifying the given columns as primary keys."""
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


# ---------------------------------------------------------------------------
# S3 mock helpers (Tier 2)
# ---------------------------------------------------------------------------


def make_s3_mocks(tmp_path):
    """
    Return (mock_list_objects, mock_ds_from_path, mock_upload_file) that
    redirect s3:// paths to the local tmp_path directory tree.
    """

    def mock_list_objects(partition, in_filter=None, **kwargs):
        local = partition.replace("s3://", str(tmp_path) + "/")
        result = []
        if os.path.exists(local):
            for root, _, files in os.walk(local):
                for fname in files:
                    if in_filter is None or fname.endswith(in_filter.lstrip(".")):
                        result.append(
                            S3Object(
                                path=os.path.join(root, fname),
                                last_modified=datetime.datetime(2025, 1, 1),
                                size_bytes=0,
                            )
                        )
        return result

    def mock_ds_from_path(source):
        if isinstance(source, str) and source.startswith("s3://"):
            local = source.replace("s3://", str(tmp_path) + "/")
            return ds_from_path(local)
        return ds_from_path(source)

    def mock_upload_file(src, dst, **kwargs):
        full_dst = str(tmp_path / dst)
        os.makedirs(os.path.dirname(full_dst), exist_ok=True)
        shutil.copy2(src, full_dst)

    return mock_list_objects, mock_ds_from_path, mock_upload_file


# ---------------------------------------------------------------------------
# Tier 2 fixture and runner
# ---------------------------------------------------------------------------


@pytest.fixture
def cdc_integration(tmp_path):
    """
    Yield (job, write_fact, write_history, read_fact).

      write_fact(df)     writes df as the live fact parquet.
      write_history(df)  writes df as history and wires job.history_ds.
      read_fact()        reads the current live fact parquet as polars DataFrame.
    """
    fact_dir = tmp_path / S3_EXPORT_PREFIX
    fact_dir.mkdir(parents=True)

    history_dir = tmp_path / "history"
    history_dir.mkdir(parents=True)

    job = CubicODSFact("EDW.TEST_TABLE")
    job.reset_tmpdir()
    job.s3_export = S3_EXPORT_PREFIX
    job.history_snapshot = TEST_SNAPSHOT
    job.part_columns = []
    job.batch_size = 1000

    def write_fact(df: pl.DataFrame) -> None:
        df.write_parquet(str(fact_dir / "year_001.parquet"))

    def write_history(df: pl.DataFrame) -> None:
        # Write flat (no snapshot= partition directory) to avoid the polars
        # large_string vs pyarrow hive-partition string type conflict.
        df.write_parquet(str(history_dir / "history.parquet"))
        job.history_ds = ds_from_path(str(history_dir / "history.parquet"))

    def read_fact() -> pl.DataFrame:
        files = list(fact_dir.glob("*.parquet"))
        assert files, "No fact parquet files found after load_cdc_records"
        return pl.read_parquet(files).select(list(FACT_SCHEMA.keys()))

    yield job, write_fact, write_history, read_fact


def run_load_cdc(job, tmp_path) -> int:
    """Run load_cdc_records with S3 functions redirected to tmp_path."""
    mock_list, mock_ds, mock_upload = make_s3_mocks(tmp_path)
    with (
        patch("odin.generate.cubic.ods_fact.list_objects", mock_list),
        patch("odin.generate.cubic.ods_fact.ds_from_path", mock_ds),
        patch("odin.generate.cubic.ods_fact.upload_file", mock_upload),
        patch("odin.generate.cubic.ods_fact.sigterm_check"),
        patch(
            "odin.generate.cubic.ods_fact.dfm_from_cdc_records",
            return_value=mock_dfm_for_keys(KEYS),
        ),
    ):
        return job.load_cdc_records()


# ===========================================================================
# load_cdc_records() integration tests
# ===========================================================================


def test_load_cdc_no_new_records(cdc_integration, tmp_path):
    """
    When no CDC records exist beyond the current watermark, returns NEXT_RUN_LONG
    and the fact table is unchanged.
    """
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {"txn_id": 1, "amount": 100, "header__change_seq": "00010", "odin_index": 0},
            ]
        )
    )
    # History contains only records at or below the current watermark.
    write_history(
        make_cdc_df(
            [
                {
                    "txn_id": 99,
                    "amount": 0,
                    "header__change_seq": "00010",
                    "header__change_oper": "I",
                },
            ]
        )
    )

    result = run_load_cdc(job, tmp_path)

    assert result == NEXT_RUN_LONG
    fact = read_fact()
    assert fact.height == 1
    assert fact["txn_id"][0] == 1


def test_load_cdc_pure_inserts(cdc_integration, tmp_path):
    """I records increase fact row count; new rows get sequential odin_index."""
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {"txn_id": 1, "amount": 100, "header__change_seq": "00001", "odin_index": 0},
                {"txn_id": 2, "amount": 200, "header__change_seq": "00002", "odin_index": 1},
            ]
        )
    )
    write_history(
        make_cdc_df(
            [
                {
                    "txn_id": 3,
                    "amount": 300,
                    "header__change_seq": "00010",
                    "header__change_oper": "I",
                },
                {
                    "txn_id": 4,
                    "amount": 400,
                    "header__change_seq": "00011",
                    "header__change_oper": "I",
                },
            ]
        )
    )

    result = run_load_cdc(job, tmp_path)

    assert result == NEXT_RUN_DEFAULT
    fact = read_fact()
    assert fact.height == 4
    assert set(fact["txn_id"].to_list()) == {1, 2, 3, 4}
    new_rows = fact.filter(pl.col("txn_id").is_in([3, 4]))
    assert (new_rows["odin_index"] >= 2).all()


def test_load_cdc_pure_deletes(cdc_integration, tmp_path):
    """
    D records reduce the fact row count; deleted keys must not reappear.

    This is the end-to-end regression test for Bug 1.  Before the fix,
    load_cdc_records no-op'd pure-delete batches because update_df was
    seeded with delete keys, causing the deleted rows to be fetched and
    re-inserted immediately after being dropped.
    """
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {
                    "txn_id": i,
                    "amount": i * 100,
                    "header__change_seq": f"0000{i}",
                    "odin_index": i - 1,
                }
                for i in range(1, 6)  # txn_id 1–5
            ]
        )
    )
    # Delete txn_id 1, 2, 3.
    write_history(
        make_cdc_df(
            [
                {"txn_id": 1, "header__change_seq": "00010", "header__change_oper": "D"},
                {"txn_id": 2, "header__change_seq": "00011", "header__change_oper": "D"},
                {"txn_id": 3, "header__change_seq": "00012", "header__change_oper": "D"},
            ]
        )
    )

    run_load_cdc(job, tmp_path)

    fact = read_fact()
    assert fact.height == 2, (
        f"Expected 2 rows after 3 deletes from 5, got {fact.height}. "
        "If 5 rows remain, the Bug 1 fix is not applied."
    )
    remaining = set(fact["txn_id"].to_list())
    assert remaining == {4, 5}
    assert not remaining.intersection({1, 2, 3}), "Deleted rows reappeared in fact table."


def test_load_cdc_pure_updates(cdc_integration, tmp_path):
    """U records change column values; row count and odin_index are unchanged."""
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {
                    "txn_id": 1,
                    "amount": 100,
                    "status": "old",
                    "header__change_seq": "00001",
                    "odin_index": 0,
                },
                {
                    "txn_id": 2,
                    "amount": 200,
                    "status": "old",
                    "header__change_seq": "00002",
                    "odin_index": 1,
                },
                {
                    "txn_id": 3,
                    "amount": 300,
                    "status": "old",
                    "header__change_seq": "00003",
                    "odin_index": 2,
                },
            ]
        )
    )
    write_history(
        make_cdc_df(
            [
                {
                    "txn_id": 1,
                    "amount": 999,
                    "status": "new",
                    "header__change_seq": "00010",
                    "header__change_oper": "U",
                },
                {
                    "txn_id": 2,
                    "amount": 888,
                    "status": "new",
                    "header__change_seq": "00011",
                    "header__change_oper": "U",
                },
            ]
        )
    )

    result = run_load_cdc(job, tmp_path)

    assert result == NEXT_RUN_DEFAULT
    fact = read_fact()
    assert fact.height == 3

    row1 = fact.filter(pl.col("txn_id") == 1)
    row2 = fact.filter(pl.col("txn_id") == 2)
    row3 = fact.filter(pl.col("txn_id") == 3)

    assert row1["amount"][0] == 999
    assert row1["status"][0] == "new"
    assert row2["amount"][0] == 888
    assert row3["amount"][0] == 300  # unchanged
    assert row3["status"][0] == "old"  # unchanged


def test_load_cdc_mixed_operations(cdc_integration, tmp_path):
    """
    A mixed I/U/D batch produces the correct net row count and values.
    """
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {"txn_id": 1, "amount": 100, "header__change_seq": "00001", "odin_index": 0},
                {"txn_id": 2, "amount": 200, "header__change_seq": "00002", "odin_index": 1},
                {"txn_id": 3, "amount": 300, "header__change_seq": "00003", "odin_index": 2},
            ]
        )
    )
    write_history(
        make_cdc_df(
            [
                {
                    "txn_id": 2,
                    "amount": 999,
                    "header__change_seq": "00010",
                    "header__change_oper": "U",
                },
                {"txn_id": 3, "header__change_seq": "00011", "header__change_oper": "D"},
                {
                    "txn_id": 4,
                    "amount": 400,
                    "header__change_seq": "00012",
                    "header__change_oper": "I",
                },
            ]
        )
    )

    run_load_cdc(job, tmp_path)

    fact = read_fact()
    # Started with 3; +1 insert, -1 delete, 0 net from update → 3 rows
    assert fact.height == 3
    assert set(fact["txn_id"].to_list()) == {1, 2, 4}
    assert fact.filter(pl.col("txn_id") == 2)["amount"][0] == 999
    assert fact.filter(pl.col("txn_id") == 1)["amount"][0] == 100  # unchanged


# ===========================================================================
# Same-key conflict resolution tests
# ===========================================================================


def test_load_cdc_insert_then_update_same_key(cdc_integration, tmp_path):
    """
    I→U for the same key in one batch: the row must survive with the I record
    as the base and the U columns applied on top.

    This is the I→U bug regression test.  Before the fix, the update path
    looked for an existing fact row (which doesn't exist — the key was just
    inserted), found nothing, and silently dropped the row.
    """
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {"txn_id": 1, "amount": 100, "header__change_seq": "00001", "odin_index": 0},
            ]
        )
    )
    write_history(
        make_cdc_df(
            [
                {
                    "txn_id": 5,
                    "amount": 500,
                    "status": "initial",
                    "header__change_seq": "00010",
                    "header__change_oper": "I",
                },
                {
                    "txn_id": 5,
                    "amount": 555,
                    "header__change_seq": "00011",
                    "header__change_oper": "U",
                },
            ]
        )
    )

    run_load_cdc(job, tmp_path)

    fact = read_fact()
    assert fact.height == 2, (
        f"Expected 2 rows (original + I→U), got {fact.height}. "
        "If 1, the I→U row was silently dropped."
    )
    assert set(fact["txn_id"].to_list()) == {1, 5}
    row5 = fact.filter(pl.col("txn_id") == 5)
    assert row5["amount"][0] == 555, "U column should overwrite I base value"
    assert row5["status"][0] == "initial", "Non-updated columns should retain I base value"


def test_load_cdc_insert_then_multiple_updates_same_key(cdc_integration, tmp_path):
    """
    I→U→U for the same key: both sparse U records contribute column values
    on top of the I base row.
    """
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {"txn_id": 1, "amount": 100, "header__change_seq": "00001", "odin_index": 0},
            ]
        )
    )
    write_history(
        make_cdc_df(
            [
                {
                    "txn_id": 5,
                    "amount": 500,
                    "status": "initial",
                    "header__change_seq": "00010",
                    "header__change_oper": "I",
                },
                {
                    "txn_id": 5,
                    "amount": 555,
                    "header__change_seq": "00011",
                    "header__change_oper": "U",
                },
                {
                    "txn_id": 5,
                    "status": "final",
                    "header__change_seq": "00012",
                    "header__change_oper": "U",
                },
            ]
        )
    )

    run_load_cdc(job, tmp_path)

    fact = read_fact()
    assert fact.height == 2
    row5 = fact.filter(pl.col("txn_id") == 5)
    assert row5["amount"][0] == 555, "Latest U for amount should win"
    assert row5["status"][0] == "final", "Latest U for status should win"


def test_load_cdc_delete_then_insert_same_key(cdc_integration, tmp_path):
    """
    D→I for the same key: the old row is removed and the new I row is inserted.
    Final op is I, so the insert record is used directly.
    """
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {
                    "txn_id": 1,
                    "amount": 100,
                    "status": "old",
                    "header__change_seq": "00001",
                    "odin_index": 0,
                },
                {"txn_id": 2, "amount": 200, "header__change_seq": "00002", "odin_index": 1},
            ]
        )
    )
    write_history(
        make_cdc_df(
            [
                {"txn_id": 1, "header__change_seq": "00010", "header__change_oper": "D"},
                {
                    "txn_id": 1,
                    "amount": 999,
                    "status": "reinserted",
                    "header__change_seq": "00011",
                    "header__change_oper": "I",
                },
            ]
        )
    )

    run_load_cdc(job, tmp_path)

    fact = read_fact()
    assert fact.height == 2
    assert set(fact["txn_id"].to_list()) == {1, 2}
    row1 = fact.filter(pl.col("txn_id") == 1)
    assert row1["amount"][0] == 999
    assert row1["status"][0] == "reinserted"


def test_load_cdc_delete_then_update_same_key(cdc_integration, tmp_path):
    """
    D→U for the same key (key exists in fact): final op is U, so the existing
    fact row is fetched and the sparse U columns applied.  The intermediate D
    is effectively overridden.
    """
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {
                    "txn_id": 1,
                    "amount": 100,
                    "status": "old",
                    "header__change_seq": "00001",
                    "odin_index": 0,
                },
                {"txn_id": 2, "amount": 200, "header__change_seq": "00002", "odin_index": 1},
            ]
        )
    )
    write_history(
        make_cdc_df(
            [
                {"txn_id": 1, "header__change_seq": "00010", "header__change_oper": "D"},
                {
                    "txn_id": 1,
                    "status": "updated",
                    "header__change_seq": "00011",
                    "header__change_oper": "U",
                },
            ]
        )
    )

    run_load_cdc(job, tmp_path)

    fact = read_fact()
    assert fact.height == 2
    assert set(fact["txn_id"].to_list()) == {1, 2}
    row1 = fact.filter(pl.col("txn_id") == 1)
    assert row1["amount"][0] == 100, "Non-updated columns retain original value"
    assert row1["status"][0] == "updated"


def test_load_cdc_multiple_updates_sparse_columns(cdc_integration, tmp_path):
    """
    Two U records for the same key updating different columns: both column
    values are applied (latest non-null per column wins).
    """
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {
                    "txn_id": 1,
                    "amount": 100,
                    "status": "old",
                    "header__change_seq": "00001",
                    "odin_index": 0,
                },
            ]
        )
    )
    write_history(
        make_cdc_df(
            [
                {
                    "txn_id": 1,
                    "amount": 999,
                    "header__change_seq": "00010",
                    "header__change_oper": "U",
                },
                {
                    "txn_id": 1,
                    "status": "new",
                    "header__change_seq": "00011",
                    "header__change_oper": "U",
                },
            ]
        )
    )

    run_load_cdc(job, tmp_path)

    fact = read_fact()
    assert fact.height == 1
    row1 = fact.filter(pl.col("txn_id") == 1)
    assert row1["amount"][0] == 999, "amount from first U should be applied"
    assert row1["status"][0] == "new", "status from second U should be applied"


def test_load_cdc_null_update_not_applied(cdc_integration, tmp_path):
    """
    A NULL value in a U record means 'no change', not 'set to NULL'.

    Pins the implicit assumption (Qlik CDC spec §2.4.2): the pipeline lacks a
    change_mask and treats NULL in U records as 'column not touched'.
    """
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {
                    "txn_id": 1,
                    "amount": 100,
                    "status": "original",
                    "header__change_seq": "00001",
                    "odin_index": 0,
                },
            ]
        )
    )
    write_history(
        make_cdc_df(
            [
                {
                    "txn_id": 1,
                    "amount": None,
                    "status": "changed",
                    "header__change_seq": "00010",
                    "header__change_oper": "U",
                },
            ]
        )
    )

    run_load_cdc(job, tmp_path)

    fact = read_fact()
    assert fact.height == 1
    row1 = fact.filter(pl.col("txn_id") == 1)
    assert row1["amount"][0] == 100, "NULL in U record should not overwrite existing value"
    assert row1["status"][0] == "changed"


def test_load_cdc_before_images_filtered(cdc_integration, tmp_path):
    """
    B (before-image) records in the history are filtered out by the CDC filter
    and do not affect the fact table.
    """
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {"txn_id": 1, "amount": 100, "header__change_seq": "00001", "odin_index": 0},
            ]
        )
    )
    write_history(
        make_cdc_df(
            [
                {
                    "txn_id": 2,
                    "amount": 200,
                    "header__change_seq": "00010",
                    "header__change_oper": "B",
                },
                {
                    "txn_id": 3,
                    "amount": 300,
                    "header__change_seq": "00011",
                    "header__change_oper": "I",
                },
            ]
        )
    )

    run_load_cdc(job, tmp_path)

    fact = read_fact()
    assert fact.height == 2
    assert set(fact["txn_id"].to_list()) == {1, 3}, "B record (txn_id=2) must not appear"


def test_load_cdc_update_latest_seq_wins(cdc_integration, tmp_path):
    """
    When two U records target the same key and same column, the one with the
    higher header__change_seq wins (spec §2.4.3).
    """
    job, write_fact, write_history, read_fact = cdc_integration

    write_fact(
        make_fact_df(
            [
                {
                    "txn_id": 1,
                    "amount": 100,
                    "status": "old",
                    "header__change_seq": "00001",
                    "odin_index": 0,
                },
            ]
        )
    )
    write_history(
        make_cdc_df(
            [
                {
                    "txn_id": 1,
                    "amount": 111,
                    "status": "first",
                    "header__change_seq": "00010",
                    "header__change_oper": "U",
                },
                {
                    "txn_id": 1,
                    "amount": 999,
                    "status": "second",
                    "header__change_seq": "00011",
                    "header__change_oper": "U",
                },
            ]
        )
    )

    run_load_cdc(job, tmp_path)

    fact = read_fact()
    assert fact.height == 1
    row1 = fact.filter(pl.col("txn_id") == 1)
    assert row1["amount"][0] == 999
    assert row1["status"][0] == "second"
