"""Shared helpers for delta-rs-backed jobs."""

from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError


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


def column_maxes(dt: DeltaTable, columns: list[str]) -> dict[str, Any]:
    """
    Read the max of each column from delta file statistics.

    Columns outside of delta.dataSkippingNumIndexedCols (the first 32 columns by
    default) will have a max of None.
    """
    actions = pa.table(dt.get_add_actions(flatten=True))
    maxes: dict[str, Any] = {}
    for column in columns:
        field = f"max.{column}"
        if actions.num_rows == 0 or field not in actions.schema.names:
            maxes[column] = None
            continue
        valid = pc.drop_null(actions[field])
        maxes[column] = pc.max(valid).as_py() if len(valid) > 0 else None
    return maxes


def row_count(dt: DeltaTable) -> int:
    """Sum num_records from delta file-level statistics."""
    actions = pa.table(dt.get_add_actions(flatten=True))
    if "num_records" not in actions.schema.names or actions.num_rows == 0:
        return 0
    total = pc.sum(actions["num_records"]).as_py()
    return int(total) if total is not None else 0
