"""
Tests for CubicODSFact CDC handling logic.

Two-tier structure:

  Tier 1 — cdc_to_fact() unit tests
    Pure DataFrame logic; no I/O, no mocking.  Fast and precise.
    These are the primary spec for CDC operation semantics and directly
    pin the Bug 1 regression (pure-delete batches being no-op'd).

  Tier 2 — load_cdc_records() integration tests
    Local parquet on disk; S3 boundary functions patched to local equivalents.
    Validates the full apply-CDC-to-fact pipeline end-to-end.

Assumptions documented by the test suite (per Qlik CDC spec §2.4.2):
  - A NULL value in a "U" record means "no change to this column", NOT
    "set this column to NULL".  This assumption is implicit in the code's
    null-skipping filter and is pinned by test_cdc_to_fact_null_update_not_applied.
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
    cdc_to_fact,
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
# TIER 1 — cdc_to_fact() unit tests
# ===========================================================================


def test_cdc_to_fact_pure_inserts():
    """I records land in insert_df; update_df and delete_df are empty."""
    cdc = make_cdc_df(
        [
            {"txn_id": 1, "amount": 100, "header__change_seq": "00001", "header__change_oper": "I"},
            {"txn_id": 2, "amount": 200, "header__change_seq": "00002", "header__change_oper": "I"},
        ]
    )
    ins, upd, dlt = cdc_to_fact(cdc, pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), KEYS)

    assert ins.height == 2
    assert set(ins["txn_id"].to_list()) == {1, 2}
    assert upd.height == 0
    assert dlt.height == 0


def test_cdc_to_fact_pure_deletes():
    """
    D records land in delete_df; update_df must be EMPTY.

    This is the direct regression test for Bug 1: before the fix, update_df
    was seeded with all CDC keys regardless of operation, causing delete-only
    batches to be silently no-op'd when applied to the fact table.
    """
    cdc = make_cdc_df(
        [
            {"txn_id": 1, "header__change_seq": "00001", "header__change_oper": "D"},
            {"txn_id": 2, "header__change_seq": "00002", "header__change_oper": "D"},
            {"txn_id": 3, "header__change_seq": "00003", "header__change_oper": "D"},
        ]
    )
    ins, upd, dlt = cdc_to_fact(cdc, pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), KEYS)

    assert ins.height == 0
    assert upd.height == 0, (
        f"update_df should be empty for pure-delete batch but has {upd.height} rows. "
        "This indicates the Bug 1 fix is not applied: delete keys must not seed update_df."
    )
    assert dlt.height == 3
    assert set(dlt["txn_id"].to_list()) == {1, 2, 3}


def test_cdc_to_fact_pure_updates():
    """U records build update_df with correct key + column values; others empty."""
    cdc = make_cdc_df(
        [
            {
                "txn_id": 1,
                "amount": 999,
                "status": "active",
                "header__change_seq": "00001",
                "header__change_oper": "U",
            },
        ]
    )
    ins, upd, dlt = cdc_to_fact(cdc, pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), KEYS)

    assert ins.height == 0
    assert dlt.height == 0
    assert upd.height == 1
    assert upd["txn_id"][0] == 1
    assert upd["amount"][0] == 999
    assert upd["status"][0] == "active"


def test_cdc_to_fact_before_images_ignored():
    """B records are fully ignored; all output DataFrames are empty."""
    cdc = make_cdc_df(
        [
            {"txn_id": 1, "amount": 100, "header__change_seq": "00001", "header__change_oper": "B"},
            {"txn_id": 2, "amount": 200, "header__change_seq": "00002", "header__change_oper": "B"},
        ]
    )
    ins, upd, dlt = cdc_to_fact(cdc, pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), KEYS)

    assert ins.height == 0
    assert upd.height == 0
    assert dlt.height == 0


def test_cdc_to_fact_update_latest_seq_wins():
    """
    When two U records target the same key, the one with the higher
    header__change_seq wins per column (spec §2.4.3).
    """
    cdc = make_cdc_df(
        [
            {
                "txn_id": 1,
                "amount": 111,
                "status": "old",
                "header__change_seq": "00001",
                "header__change_oper": "U",
            },
            {
                "txn_id": 1,
                "amount": 999,
                "status": "new",
                "header__change_seq": "00002",
                "header__change_oper": "U",
            },
        ]
    )
    _, upd, _ = cdc_to_fact(cdc, pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), KEYS)

    row = upd.filter(pl.col("txn_id") == 1)
    assert row.height == 1
    assert row["amount"][0] == 999
    assert row["status"][0] == "new"


def test_cdc_to_fact_null_update_not_applied():
    """
    A NULL value in a U record is treated as 'no change', not 'set to NULL'.

    This pins the implicit assumption in the code (spec §2.4.2 note): the
    pipeline lacks a change_mask and assumes the source never uses NULL to
    mean 'set-to-NULL'.  If that assumption breaks, this test will need
    revisiting alongside the apply logic.
    """
    cdc = make_cdc_df(
        [
            # amount is null — should not overwrite; status has a new value.
            {
                "txn_id": 1,
                "amount": None,
                "status": "changed",
                "header__change_seq": "00001",
                "header__change_oper": "U",
            },
        ]
    )
    _, upd, _ = cdc_to_fact(cdc, pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), KEYS)

    assert upd.height == 1
    assert upd["txn_id"][0] == 1
    # amount column should not appear in update_df (null value was skipped)
    assert "amount" not in upd.columns or upd["amount"][0] is None
    assert upd["status"][0] == "changed"


def test_cdc_to_fact_mixed_operations():
    """I / U / D / B records in one batch each route to the correct DataFrame."""
    cdc = make_cdc_df(
        [
            {
                "txn_id": 10,
                "amount": 100,
                "header__change_seq": "00001",
                "header__change_oper": "I",
            },
            {
                "txn_id": 20,
                "amount": 200,
                "header__change_seq": "00002",
                "header__change_oper": "U",
            },
            {"txn_id": 30, "header__change_seq": "00003", "header__change_oper": "D"},
            {
                "txn_id": 40,
                "amount": 400,
                "header__change_seq": "00004",
                "header__change_oper": "B",
            },
        ]
    )
    ins, upd, dlt = cdc_to_fact(cdc, pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), KEYS)

    assert set(ins["txn_id"].to_list()) == {10}
    assert set(upd["txn_id"].to_list()) == {20}
    assert set(dlt["txn_id"].to_list()) == {30}
    # B record (txn_id=40) must not appear anywhere
    for df in (ins, upd, dlt):
        if df.height > 0:
            assert 40 not in df["txn_id"].to_list()


def test_cdc_to_fact_delete_then_insert_same_key():
    """
    A D followed by an I for the same key lands in delete_df AND insert_df.
    update_df must not contain the key (no U records).
    """
    cdc = make_cdc_df(
        [
            {"txn_id": 1, "header__change_seq": "00001", "header__change_oper": "D"},
            {"txn_id": 1, "amount": 999, "header__change_seq": "00002", "header__change_oper": "I"},
        ]
    )
    ins, upd, dlt = cdc_to_fact(cdc, pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), KEYS)

    assert 1 in ins["txn_id"].to_list()
    assert 1 in dlt["txn_id"].to_list()
    assert upd.height == 0


def test_cdc_to_fact_accumulation_across_batches():
    """
    Calling cdc_to_fact multiple times (simulating the fetch loop) accumulates
    results correctly without double-counting.
    """
    batch1 = make_cdc_df(
        [
            {"txn_id": 1, "amount": 100, "header__change_seq": "00001", "header__change_oper": "I"},
            {"txn_id": 2, "amount": 200, "header__change_seq": "00002", "header__change_oper": "U"},
        ]
    )
    batch2 = make_cdc_df(
        [
            {"txn_id": 3, "header__change_seq": "00003", "header__change_oper": "D"},
            {"txn_id": 4, "amount": 400, "header__change_seq": "00004", "header__change_oper": "I"},
        ]
    )

    ins, upd, dlt = cdc_to_fact(batch1, pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), KEYS)
    ins, upd, dlt = cdc_to_fact(batch2, ins, upd, dlt, KEYS)

    assert set(ins["txn_id"].to_list()) == {1, 4}
    # update_df accumulates keys from U records only
    assert set(upd["txn_id"].to_list()) == {2}
    assert set(dlt["txn_id"].to_list()) == {3}


def test_cdc_to_fact_accumulation_deletes_do_not_pollute_updates():
    """
    Across multiple batches, D records in a subsequent batch must not add
    keys to update_df (the specific multi-batch form of Bug 1).
    """
    batch1 = make_cdc_df(
        [
            {"txn_id": 1, "amount": 100, "header__change_seq": "00001", "header__change_oper": "U"},
        ]
    )
    batch2 = make_cdc_df(
        [
            {"txn_id": 2, "header__change_seq": "00002", "header__change_oper": "D"},
            {"txn_id": 3, "header__change_seq": "00003", "header__change_oper": "D"},
        ]
    )

    ins, upd, dlt = cdc_to_fact(batch1, pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), KEYS)
    ins, upd, dlt = cdc_to_fact(batch2, ins, upd, dlt, KEYS)

    # update_df should only contain txn_id=1 (from the U in batch 1)
    assert set(upd["txn_id"].to_list()) == {1}, (
        f"update_df contains unexpected keys {upd['txn_id'].to_list()}. "
        "Delete keys from batch2 must not be merged into update_df."
    )
    assert set(dlt["txn_id"].to_list()) == {2, 3}


# ===========================================================================
# TIER 2 — load_cdc_records() integration tests
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
