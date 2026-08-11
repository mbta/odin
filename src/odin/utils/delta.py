"""Shared helpers for delta-rs-backed jobs."""

import os
from datetime import datetime
from typing import Any
from typing import Iterator

import pyarrow as pa
import pyarrow.compute as pc

from deltalake import CommitProperties
from deltalake import DeltaTable
from deltalake import WriterProperties
from deltalake import write_deltalake
from deltalake.exceptions import TableNotFoundError

from odin.utils.aws.s3 import s3_file
from odin.utils.aws.s3 import s3_folder
from odin.utils.locations import CUBIC_ODS_DELTA_DATA
from odin.utils.locations import CUBIC_ODS_FACT_DATA
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.logger import ProcessLog
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import ds_metadata_min_max

DELTA_WRITER_PROPERTIES = WriterProperties(max_row_group_size=64 * 1024, compression="SNAPPY")

TARGET_FILE_SIZE_PROPERTY = "delta.targetFileSize"
TARGET_FILE_SIZE_BYTES = 32 * 1024 * 1024

# Keys under which each Delta commit records a job's input position in its custom
# metadata (readable via DeltaTable.history()). This is the source of truth for
# "where the table is at", independent of the surviving row contents.
STATE_SNAPSHOT_KEY = "odin_snapshot"
STATE_WATERMARK_KEY = "odin_cdc_watermark"
INITIAL_WATERMARK = "0"  # header__change_seq is a zero-padded string; all seqs > "0"

# Snapshot partition names produced by the Qlik ingestion (e.g. 20250101T000000Z).
SNAPSHOT_FMT = "%Y%m%dT%H%M%SZ"


def open_delta(uri: str) -> DeltaTable | None:
    """Open a Delta table at uri, returning None if it does not yet exist."""
    try:
        return DeltaTable(uri)
    except TableNotFoundError:
        return None


def column_max(dt: DeltaTable, column: str) -> int:
    """Read the max value of a column from delta stats (file or partition value)."""
    actions = pa.table(dt.get_add_actions(flatten=True))
    if actions.num_rows == 0:
        return 0
    for field in (f"max.{column}", f"partition.{column}"):
        if field not in actions.schema.names:
            continue
        valid = pc.drop_null(actions[field])
        if len(valid) > 0:
            return pc.max(valid).as_py()
    return 0


def row_count(dt: DeltaTable) -> int:
    """Sum num_records from delta file-level statistics."""
    actions = pa.table(dt.get_add_actions(flatten=True))
    if "num_records" not in actions.schema.names or actions.num_rows == 0:
        return 0
    total = pc.sum(actions["num_records"]).as_py()
    return int(total) if total is not None else 0


def state_commit_properties(snapshot: str, watermark: str) -> CommitProperties:
    """Commit metadata recording a table's snapshot generation and CDC watermark."""
    return CommitProperties(
        custom_metadata={
            STATE_SNAPSHOT_KEY: snapshot,
            STATE_WATERMARK_KEY: watermark,
        }
    )


def set_target_file_size(dt: DeltaTable, uri: str) -> DeltaTable:
    """
    Put delta.targetFileSize on `dt` when it is missing or stale.

    An explicit alter is required rather than just passing ``configuration`` to
    write_deltalake: that argument only takes effect when the table is *created*,
    so a table written with mode="overwrite" keeps whatever it had before.

    :return: the table, reloaded when a property commit was made
    """
    target = str(TARGET_FILE_SIZE_BYTES)
    if dt.metadata().configuration.get(TARGET_FILE_SIZE_PROPERTY) == target:
        return dt
    dt.alter.set_table_properties({TARGET_FILE_SIZE_PROPERTY: target})
    return DeltaTable(uri)


# ----------------------------------------------------------------------
# Legacy fact table -> Delta silver table migration
# ----------------------------------------------------------------------

# Legacy-fact-only columns, dropped so the migrated table matches the schema
# CubicODSDelta._rebuild_silver would produce. odin_index is the fact pipeline's
# row identity for its rewrite-in-place merges; the Delta MERGE keys on the
# vendor primary key instead and never populates it.
FACT_ONLY_COLUMNS = ("odin_index",)

# Derived on write from edw_inserted_dtm, so any stored copy is ignored: the fact
# table carries only odin_year, and it lives in the hive path rather than the file.
FACT_PARTITION_COLUMNS = ("odin_year", "odin_month")

# Columns the delta silver table cannot be built without.
REQUIRED_FACT_COLUMNS = ("odin_snapshot", "header__change_seq")

FACT_TO_DELTA_BATCH_SIZE = 10_000


def _fact_batches(
    fact_ds: Any, columns: list[str], partitioned: bool, batch_size: int
) -> Iterator[pa.RecordBatch]:
    """
    Stream fact rows as silver-shaped record batches.

    Reads only `columns` (dropping the fact-only ones), then appends the
    odin_year/odin_month partition values derived from edw_inserted_dtm — the
    same expression _rebuild_silver uses, so a migrated table partitions
    identically to a rebuilt one.
    """
    for batch in fact_ds.to_batches(
        columns=columns, batch_size=batch_size, batch_readahead=1, fragment_readahead=0
    ):
        if batch.num_rows == 0:
            continue
        if partitioned:
            inserted = batch.column("edw_inserted_dtm")
            batch = batch.append_column(
                "odin_year", pc.coalesce(pc.strftime(inserted, "%Y"), "0").cast(pa.int32())
            )
            batch = batch.append_column(
                "odin_month", pc.coalesce(pc.strftime(inserted, "%m"), "0").cast(pa.int32())
            )
        yield batch


def fact_to_delta(
    table: str,
    fact_root: str | None = None,
    delta_uri: str | None = None,
    z_order_by: list[str] | None = None,
    overwrite: bool = False,
    batch_size: int = FACT_TO_DELTA_BATCH_SIZE,
) -> dict[str, Any]:
    """
    Copy a legacy Cubic ODS fact table into its Delta silver table.

    This is the cheap migration path onto ``generate/cubic/delta_ods.py`` for a
    table whose Qlik history holds an unreasonable amount of CDC since its last
    snapshot: rather than have CubicODSDelta rebuild from the snapshot's "L"
    records and then chew through that whole backlog, the already-current fact
    table is translated in place into a Delta table and the job picks up from
    the fact table's position.

    The translation is purely a reshaping — no CDC is read or applied:

      * ``odin_index`` (fact-only) is dropped, so the schema matches what
        _rebuild_silver writes and a later snapshot rebuild is a clean overwrite.
      * ``odin_year``/``odin_month`` are re-derived from ``edw_inserted_dtm``;
        tables without that column are written unpartitioned, as in the job.
      * ``odin_snapshot`` and ``header__change_seq`` ride along unchanged — the
        latter is the CDC watermark and must survive the copy.

    The state CubicODSDelta reads back on its next run is recorded in the write's
    commit metadata: the snapshot generation the fact table was built from, and
    the max ``header__change_seq`` present in it. ``delta.targetFileSize`` is set
    on the table as well, so the first merge does not have to correct file sizing.

    Two things worth knowing about the recorded watermark. It is derived from the
    surviving rows, which is exactly the resume point ods_fact.py itself uses, but
    it can sit *behind* the last sequence actually applied — a CDC batch of deletes
    removes the row holding the max sequence. The delta job therefore replays from
    there, which is safe: replaying a contiguous suffix of the change stream over
    the same rows resolves to the same table (see _build_merge_source). And if the
    Qlik history has rolled to a newer snapshot since the fact table last ran, the
    delta job will find the mismatch and rebuild from history — the migration is
    wasted work in that case, not a corrupt table.

    :param table: Cubic ODS table name (e.g. "EDW.FARE_RULE")
    :param fact_root: source fact dataset; defaults to the springboard fact path
    :param delta_uri: destination Delta table; defaults to the springboard delta path
    :param z_order_by: optional columns (normally the vendor primary key) to
        z-order the written table by. Fact rows are laid out in odin_index order,
        which gives the merge's key-range file skipping nothing to work with;
        sorting by key up front makes the first merges prunable.
    :param overwrite: replace an existing Delta table at `delta_uri`. Off by
        default so a migration cannot silently discard a live silver table.
    :param batch_size: rows per record batch read from the fact dataset

    :return: metrics describing the migration
    """
    fact_root = fact_root or s3_folder(os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_FACT_DATA, table))
    delta_uri = delta_uri or s3_file(os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_DELTA_DATA, table))
    log = ProcessLog("fact_to_delta", table=table, fact_root=fact_root, delta_uri=delta_uri)

    existing = open_delta(delta_uri)
    assert existing is None or overwrite, (
        f"Delta table for {table} already exists at {delta_uri}; pass overwrite=True to replace it."
    )

    fact_ds = ds_from_path(fact_root)
    fact_rows = fact_ds.count_rows()
    assert fact_rows > 0, f"No fact rows found for {table} at {fact_root}"

    fact_columns = list(fact_ds.schema.names)
    missing = set(REQUIRED_FACT_COLUMNS) - set(fact_columns)
    assert not missing, f"fact table for {table} is missing required columns: {sorted(missing)}"

    # The snapshot generation the fact table was loaded from. load_new_snapshot
    # rewrites every row when the snapshot rolls, so a table straddling two of
    # them is a broken fact table and there is no single position to record.
    snapshot_min, snapshot_max = ds_metadata_min_max(fact_ds, "odin_snapshot")
    assert snapshot_min == snapshot_max and snapshot_max, (
        f"fact table for {table} does not carry a single odin_snapshot "
        f"(min={snapshot_min!r}, max={snapshot_max!r})"
    )
    snapshot = str(snapshot_max)
    # Fail here rather than write a table whose recorded position can never match
    # a history partition, which would make every future run rebuild from scratch.
    datetime.strptime(snapshot, SNAPSHOT_FMT)

    _, max_seq = ds_metadata_min_max(fact_ds, "header__change_seq")
    watermark = INITIAL_WATERMARK if max_seq is None else str(max_seq)

    read_columns = [
        c for c in fact_columns if c not in FACT_ONLY_COLUMNS and c not in FACT_PARTITION_COLUMNS
    ]
    partitioned = "edw_inserted_dtm" in read_columns
    if partitioned:
        # A row without edw_inserted_dtm lands in the odin_year=0 partition, which
        # the delta job's partition-pruned merges never revisit, so it could never
        # be updated or deleted again. Refuse rather than write it.
        null_dtm = fact_ds.count_rows(filter=pc.field("edw_inserted_dtm").is_null())
        assert null_dtm == 0, (
            f"fact table for {table} has {null_dtm} rows with a null edw_inserted_dtm; "
            "they would land in the odin_year=0 partition, which partition-pruned "
            "merges never revisit"
        )

    delta_schema = pa.schema([fact_ds.schema.field(c) for c in read_columns])
    if partitioned:
        delta_schema = delta_schema.append(pa.field("odin_year", pa.int32()))
        delta_schema = delta_schema.append(pa.field("odin_month", pa.int32()))
    # Checked before the write: z-ordering runs after it, and a bad column name
    # there would leave a written-but-unsorted table behind a raised error.
    unknown = set(z_order_by or []) - set(delta_schema.names)
    assert not unknown, f"z_order_by columns absent from {table}: {sorted(unknown)}"
    reader = pa.RecordBatchReader.from_batches(
        delta_schema, _fact_batches(fact_ds, read_columns, partitioned, batch_size)
    )

    log.add_metadata(
        fact_rows=fact_rows,
        snapshot=snapshot,
        watermark=watermark,
        partitioned=partitioned,
        replaced_existing=existing is not None,
    )
    write_deltalake(
        delta_uri,
        reader,
        mode="overwrite",
        schema_mode="overwrite",
        partition_by=list(FACT_PARTITION_COLUMNS) if partitioned else None,
        commit_properties=state_commit_properties(snapshot, watermark),
        writer_properties=DELTA_WRITER_PROPERTIES,
        configuration={TARGET_FILE_SIZE_PROPERTY: str(TARGET_FILE_SIZE_BYTES)},
        target_file_size=TARGET_FILE_SIZE_BYTES,
    )

    dt = set_target_file_size(DeltaTable(delta_uri), delta_uri)
    delta_rows = row_count(dt)
    assert delta_rows == fact_rows, (
        f"migrated Delta table for {table} holds {delta_rows} rows, "
        f"but the fact table held {fact_rows}"
    )

    if z_order_by:
        dt.optimize.z_order(
            z_order_by,
            target_size=TARGET_FILE_SIZE_BYTES,
            max_concurrent_tasks=1,
            writer_properties=DELTA_WRITER_PROPERTIES,
        )
        dt = DeltaTable(delta_uri)

    metrics: dict[str, Any] = {
        "table": table,
        "delta_uri": delta_uri,
        "rows": delta_rows,
        "snapshot": snapshot,
        "watermark": watermark,
        "partition_by": ",".join(FACT_PARTITION_COLUMNS) if partitioned else "",
        "columns_dropped": ",".join(c for c in fact_columns if c not in read_columns),
        "z_ordered_by": ",".join(z_order_by or []),
        "delta_version": dt.version(),
    }
    log.complete(**metrics)
    return metrics
