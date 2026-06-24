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


def column_max(dt: DeltaTable, column: str) -> Any:
    """Read the max value of a column from delta stats (file or partition value)."""
    actions = pa.table(dt.get_add_actions(flatten=True))
    if actions.num_rows == 0:
        return None
    for field in (f"max.{column}", f"partition.{column}"):
        if field not in actions.schema.names:
            continue
        valid = pc.drop_null(actions[field])
        if len(valid) > 0:
            return pc.max(valid).as_py()
    return None


def row_count(dt: DeltaTable) -> int:
    """Sum num_records from delta file-level statistics."""
    actions = pa.table(dt.get_add_actions(flatten=True))
    if "num_records" not in actions.schema.names or actions.num_rows == 0:
        return 0
    total = pc.sum(actions["num_records"]).as_py()
    return int(total) if total is not None else 0
