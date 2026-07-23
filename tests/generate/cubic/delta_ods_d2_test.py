"""
Tests for CubicODSDeltaD2 (Delta-bronze-based ODS silver materialization).

Unlike delta_ods_test.py (which reads a parquet history via DuckDB), these tests
build a real local Delta **bronze** table — snapshot/odin_change_class partitioned,
with per-file header__change_seq stats, and bronze-style commit metadata — then
drive the d2 job's steps directly and assert on the real Delta silver table it
writes. This exercises the new delta-rs read path: snapshot selection from
bronze commit metadata, the partition-filtered load rebuild, and the
metadata-pruned CDC window selection.

The CDC resolution/merge semantics themselves are shared verbatim with
delta_ods.py and covered by its property harness; here we confirm the reader
feeds them correctly.
"""

from datetime import datetime
from typing import Any
from unittest.mock import patch

import polars as pl
import pytest

from deltalake import CommitProperties, DeltaTable, write_deltalake

from odin.generate.cubic import delta_ods_d2 as mod
from odin.generate.cubic.delta_ods_d2 import CubicODSDeltaD2

KEYS = ["txn_id"]
SNAP1 = "20250101T000000Z"
SNAP2 = "20250201T000000Z"
FROM_CSV = "s3://archive/cubic/ods_qlik/EDW.TEST_TABLE__ct/test.csv.gz"

BRONZE_PARTITIONS = ["snapshot", "odin_change_class"]
BRONZE_CONFIG = {"delta.dataSkippingStatsColumns": "header__change_seq"}


def _bronze_frame(snapshot: str, change_class: str, rows: list[dict], dated: bool) -> pl.DataFrame:
    """Build one bronze partition frame with all header columns filled in."""
    defaults: dict[str, Any] = {
        "txn_id": None,
        "amount": None,
        "status": None,
        "header__change_seq": None,
        "header__change_oper": None,
        "header__year": 2025,
        "header__month": 1,
        "header__timestamp": datetime(2025, 1, 1),
        "header__from_csv": FROM_CSV,
    }
    if dated:
        defaults["edw_inserted_dtm"] = None
    filled = [{**defaults, **r} for r in rows]
    schema: dict[str, Any] = {
        "txn_id": pl.Int64,
        "amount": pl.Int64,
        "status": pl.String,
        "header__change_seq": pl.String,
        "header__change_oper": pl.String,
        "header__year": pl.Int32,
        "header__month": pl.Int32,
        "header__timestamp": pl.Datetime("us"),
        "header__from_csv": pl.String,
    }
    if dated:
        schema["edw_inserted_dtm"] = pl.Datetime("us")
    frame = pl.DataFrame(filled, schema=schema)
    return frame.with_columns(
        pl.lit(snapshot).alias("snapshot"),
        pl.lit(change_class).alias("odin_change_class"),
    )


def mock_dfm_for_keys(keys: list[str]) -> dict:
    """Minimal DFM dict marking `keys` as primary keys."""
    data_cols = ["txn_id", "amount", "status"]
    return {
        "dataInfo": {
            "columns": [
                {"name": col, "primaryKeyPos": (keys.index(col) + 1) if col in keys else 0}
                for col in data_cols
            ]
        }
    }


class BronzeWriter:
    """Helper that appends bronze partitions with bronze-style commit metadata."""

    def __init__(self, uri: str) -> None:
        """Create a BronzeWriter targeting the local Delta table at `uri`."""
        self.uri = uri
        self.created = False

    def append(
        self,
        snapshot: str,
        change_class: str,
        rows: list[dict],
        *,
        dated: bool = False,
        load_complete: bool = True,
    ) -> None:
        """Append one partition's rows, recording bronze position in commit metadata."""
        frame = _bronze_frame(snapshot, change_class, rows, dated)
        commit = CommitProperties(
            custom_metadata={
                "odin_snapshot": snapshot,
                "odin_load_complete": "true" if load_complete else "false",
                "odin_load_watermark": "",
                "odin_cdc_watermark": "",
            }
        )
        write_deltalake(
            self.uri,
            frame.to_arrow(),
            mode="overwrite" if not self.created else "append",
            schema_mode="overwrite" if not self.created else "merge",
            partition_by=BRONZE_PARTITIONS,
            commit_properties=commit,
            configuration=BRONZE_CONFIG if not self.created else None,
        )
        self.created = True


@pytest.fixture
def job(tmp_path):
    """Yield (job, bronze_writer, refresh, read_silver) wired to local Delta paths."""
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

    pipeline = CubicODSDeltaD2("EDW.TEST_TABLE")
    pipeline.bronze_uri = str(bronze_dir)
    pipeline.silver_uri = str(silver_dir)
    writer = BronzeWriter(str(bronze_dir))

    def refresh() -> None:
        """Re-open bronze/silver and re-run snapshot discovery, as run() would."""
        pipeline._add_cache = None
        pipeline.bronze = DeltaTable(str(bronze_dir))
        pipeline.silver = DeltaTable(str(silver_dir)) if silver_dir.exists() else None
        pipeline._snapshot_check()

    def read_silver() -> pl.DataFrame:
        result = pl.from_arrow(DeltaTable(str(silver_dir)).to_pyarrow_table())
        assert isinstance(result, pl.DataFrame)
        cols = [c for c in ["txn_id", "amount", "status", "odin_snapshot"] if c in result.columns]
        return result.select(cols).sort("txn_id")

    with (
        patch("odin.generate.cubic.delta_ods_d2.sigterm_check"),
        patch(
            "odin.generate.cubic.delta_ods_d2.dfm_from_s3",
            return_value=mock_dfm_for_keys(KEYS),
        ),
    ):
        yield pipeline, writer, refresh, read_silver


def test_snapshot_check_picks_newest_complete(job):
    """_snapshot_check selects the newest load-complete snapshot."""
    pipeline, writer, refresh, _ = job
    writer.append(SNAP1, "load", [{"txn_id": 1, "header__change_oper": "L"}])
    writer.append(SNAP2, "load", [{"txn_id": 2, "header__change_oper": "L"}])
    refresh()
    assert pipeline.history_snapshot == SNAP2


def test_snapshot_check_falls_back_when_newest_mid_load(job):
    """A half-loaded newest snapshot is skipped for the prior complete one."""
    pipeline, writer, refresh, _ = job
    writer.append(SNAP1, "load", [{"txn_id": 1, "header__change_oper": "L"}], load_complete=True)
    # SNAP2 has load files present but bronze marks it not-yet-complete.
    writer.append(SNAP2, "load", [{"txn_id": 2, "header__change_oper": "L"}], load_complete=False)
    refresh()
    assert pipeline.history_snapshot == SNAP1


def test_rebuild_silver_loads_only_load_records(job):
    """Rebuild keeps L rows, drops CDC metadata, assigns odin_snapshot."""
    pipeline, writer, refresh, read_silver = job
    writer.append(
        SNAP1,
        "load",
        [
            {"txn_id": 1, "amount": 10, "status": "a", "header__change_oper": "L"},
            {"txn_id": 2, "amount": 20, "status": "b", "header__change_oper": "L"},
        ],
    )
    # A CDC row in the same snapshot must not be pulled into the rebuild.
    writer.append(
        SNAP1,
        "cdc",
        [{"txn_id": 3, "amount": 30, "header__change_oper": "I", "header__change_seq": "1" * 17}],
    )
    refresh()
    pipeline._rebuild_silver()

    out = read_silver()
    assert out.get_column("txn_id").to_list() == [1, 2]
    assert out.get_column("odin_snapshot").to_list() == [SNAP1, SNAP1]
    silver_cols = DeltaTable(pipeline.silver_uri).schema().to_arrow().names
    assert "header__change_oper" not in silver_cols
    assert "odin_change_class" not in silver_cols
    assert "snapshot" not in silver_cols


def test_rebuild_silver_partitions_by_year_month(job):
    """With edw_inserted_dtm present, silver is partitioned by odin_year + odin_month."""
    pipeline, writer, refresh, _ = job
    writer.append(
        SNAP1,
        "load",
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
        ],
        dated=True,
    )
    refresh()
    assert pipeline.part_columns == ["odin_year", "odin_month"]
    pipeline._rebuild_silver()

    dt = DeltaTable(pipeline.silver_uri)
    assert dt.metadata().partition_columns == ["odin_year", "odin_month"]
    out = pl.from_arrow(dt.to_pyarrow_table()).sort("txn_id")
    assert out.get_column("odin_year").to_list() == [2025, 2025]
    assert out.get_column("odin_month").to_list() == [1, 3]


def test_rebuild_raises_on_empty_load(job):
    """A snapshot with no L records raises before any overwrite."""
    pipeline, writer, refresh, _ = job
    writer.append(
        SNAP1,
        "cdc",
        [{"txn_id": 1, "header__change_oper": "I", "header__change_seq": "1" * 17}],
    )
    # Force the (cdc-only) snapshot as target; there are no load records.
    pipeline._add_cache = None
    pipeline.bronze = DeltaTable(pipeline.bronze_uri)
    pipeline.silver = None
    pipeline.history_snapshot = SNAP1
    pipeline._dataset_schema = pipeline.bronze.to_pyarrow_dataset().schema
    pipeline.history_columns = list(pipeline._dataset_schema.names)
    pipeline.part_columns = []
    with pytest.raises(AssertionError, match="no L"):
        pipeline._rebuild_silver()


def test_cdc_window_files_prunes_to_tail(job):
    """Only bronze CDC files whose max seq is past the watermark are selected."""
    pipeline, writer, refresh, _ = job
    writer.append(SNAP1, "load", [{"txn_id": 1, "header__change_oper": "L"}])
    # three separate CDC files (each its own append → its own seq stats)
    writer.append(
        SNAP1,
        "cdc",
        [{"txn_id": 1, "header__change_oper": "U", "header__change_seq": "20250102" + "0" * 9}],
    )
    writer.append(
        SNAP1,
        "cdc",
        [{"txn_id": 1, "header__change_oper": "U", "header__change_seq": "20250103" + "0" * 9}],
    )
    writer.append(
        SNAP1,
        "cdc",
        [{"txn_id": 1, "header__change_oper": "U", "header__change_seq": "20250104" + "0" * 9}],
    )
    refresh()
    watermark = "20250102" + "9" * 9  # past file 1, before files 2 and 3
    selected, more = pipeline._cdc_window_files(watermark, limit=mod.MAX_MERGE_RECORDS)
    maxes = sorted(r["max"] for r in selected)
    assert maxes == ["20250103" + "0" * 9, "20250104" + "0" * 9]
    assert more is False


def test_merge_cdc_insert_update_delete(job):
    """End-to-end CDC merge from bronze: insert, sparse update, delete."""
    pipeline, writer, refresh, read_silver = job
    writer.append(
        SNAP1,
        "load",
        [
            {"txn_id": 1, "amount": 10, "status": "a", "header__change_oper": "L"},
            {"txn_id": 2, "amount": 20, "status": "b", "header__change_oper": "L"},
        ],
    )
    writer.append(
        SNAP1,
        "cdc",
        [
            # insert new key 3
            {
                "txn_id": 3,
                "amount": 30,
                "status": "c",
                "header__change_oper": "I",
                "header__change_seq": "20250102" + "0" * 9,
            },
            # sparse update key 1 (amount only; status must survive)
            {
                "txn_id": 1,
                "amount": 11,
                "header__change_oper": "U",
                "header__change_seq": "20250103" + "0" * 9,
            },
            # delete key 2
            {"txn_id": 2, "header__change_oper": "D", "header__change_seq": "20250104" + "0" * 9},
        ],
    )
    refresh()
    pipeline._rebuild_silver()
    next_run = pipeline._merge_cdc(mod.INITIAL_WATERMARK)

    out = read_silver()
    assert out.get_column("txn_id").to_list() == [1, 3]
    assert dict(zip(out.get_column("txn_id"), out.get_column("amount"))) == {1: 11, 3: 30}
    # sparse update preserved key 1's status
    assert out.filter(pl.col("txn_id") == 1).get_column("status").item() == "a"
    # watermark advanced to the max processed seq, recorded in commit metadata
    assert pipeline.cdc_watermark == "20250104" + "0" * 9
    snap, wm = pipeline._read_state()
    assert (snap, wm) == (SNAP1, "20250104" + "0" * 9)
    assert next_run == mod._default_run_interval()


def test_merge_cdc_no_pending_when_caught_up(job):
    """With no CDC past the watermark, the merge is a no-op and reschedules long."""
    pipeline, writer, refresh, read_silver = job
    writer.append(SNAP1, "load", [{"txn_id": 1, "amount": 10, "header__change_oper": "L"}])
    refresh()
    pipeline._rebuild_silver()
    next_run = pipeline._merge_cdc(mod.INITIAL_WATERMARK)
    assert next_run == mod._long_run_interval()
    assert read_silver().get_column("txn_id").to_list() == [1]
