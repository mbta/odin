"""
Property-based tests for CubicODSDelta's CDC apply semantics.

``apply_event`` below is the REFERENCE INTERPRETER: the executable spec of how
one CDC event transforms one row. It is deliberately trivial — every semantic
decision (I images are full row images where NULL means NULL; U records patch
only their non-null columns; a U with no live row is an orphan and is dropped;
D is delete-if-exists) is stated here once, in plain Python, and the real
pipeline (batch resolution in ``_build_merge_source`` + Delta MERGE in
``_merge_apply``) is treated as an optimized implementation of it.

The property verified: for a random initial table, a random event stream, and a
random split of that stream into batches, running the pipeline batch-by-batch
produces exactly the table obtained by folding the events one at a time with
the interpreter. This checks both value semantics and batch-split invariance
(the outcome must not depend on where batch boundaries fall).

Cases are generated from seeded ``random.Random`` instances, so failures are
reproducible: re-run with the case number reported in the assertion message.
"""

from typing import Any
from unittest.mock import patch

import polars as pl
import pyarrow as pa
import pytest

from deltalake import DeltaTable, write_deltalake

from odin.generate.cubic.delta_ods import CubicODSDelta

KEYS = [1, 2, 3]
DATA_COLS = ("amount", "status")
N_CASES = 60
TEST_SNAPSHOT = "20250101T000000Z"


_SILVER_SCHEMA_FIELDS: "list[pa.Field[Any]]" = [
    pa.field("txn_id", pa.int64()),
    pa.field("amount", pa.int64()),
    pa.field("status", pa.large_string()),
    pa.field("header__change_seq", pa.large_string()),
    pa.field("odin_snapshot", pa.large_string()),
]
SILVER_SCHEMA = pa.schema(_SILVER_SCHEMA_FIELDS)

BATCH_SCHEMA = {
    "txn_id": pl.Int64,
    "amount": pl.Int64,
    "status": pl.String,
    "header__change_seq": pl.String,
    "header__change_oper": pl.String,
}

Row = dict[str, Any]


# ----------------------------------------------------------------------
# Reference interpreter (the executable spec)
# ----------------------------------------------------------------------


def apply_event(row: Row | None, event: Row) -> Row | None:
    """Apply one CDC event to one row; the definition of the CDC semantics."""
    oper = event["header__change_oper"]
    if oper == "D":
        # Delete-if-exists.
        return None
    if oper == "I":
        # Full row image: the row becomes exactly the image; NULL means NULL.
        return {
            **{c: event[c] for c in DATA_COLS},
            "header__change_seq": event["header__change_seq"],
        }
    if oper == "U":
        if row is None:
            # Orphan update: no live row to patch; the event is dropped.
            return None
        # Sparse update: NULL means "column not present", so patch only the
        # non-null columns and keep the rest of the row.
        patched = dict(row)
        for c in DATA_COLS:
            if event[c] is not None:
                patched[c] = event[c]
        patched["header__change_seq"] = event["header__change_seq"]
        return patched
    raise AssertionError(f"unknown oper {oper!r}")


def reference_final(initial: dict[int, Row], events: list[Row]) -> dict[int, Row]:
    """Fold `events` (in seq order) over `initial` one at a time."""
    table = {k: dict(v) for k, v in initial.items()}
    for event in sorted(events, key=lambda e: str(e["header__change_seq"])):
        key = event["txn_id"]
        row = apply_event(table.get(key), event)
        if row is None:
            table.pop(key, None)
        else:
            table[key] = row
    return table


# ----------------------------------------------------------------------
# Case generation
# ----------------------------------------------------------------------


def generate_case(rng: Any) -> tuple[dict[int, Row], list[Row], list[list[Row]]]:
    """Return (initial rows, event stream, the stream split into batches)."""
    initial: dict[int, Row] = {}
    for key in KEYS:
        if rng.random() < 0.5:
            initial[key] = {
                "amount": rng.choice([None, *range(1, 6)]),
                "status": rng.choice([None, "a", "b"]),
                "header__change_seq": None,
            }

    n_events = rng.randint(1, 12)
    events = [
        {
            "txn_id": rng.choice(KEYS),
            "header__change_oper": rng.choices(["I", "U", "D"], weights=[3, 5, 2])[0],
            "header__change_seq": f"{i + 1:04d}",
            "amount": rng.choice([None, *range(10, 15)]),
            "status": rng.choice([None, "x", "y"]),
        }
        for i in range(n_events)
    ]

    n_cuts = rng.randint(0, min(2, n_events - 1))
    cuts = sorted(rng.sample(range(1, n_events), n_cuts)) if n_cuts else []
    batches = [events[a:b] for a, b in zip([0, *cuts], [*cuts, n_events])]
    return initial, events, batches


# ----------------------------------------------------------------------
# Pipeline execution
# ----------------------------------------------------------------------


def pipeline_final(silver_dir: str, initial: dict[int, Row], batches: list[list[Row]]) -> dict:
    """Run the real resolver + Delta MERGE per batch; return the final table."""
    rows = [{"txn_id": k, **v, "odin_snapshot": TEST_SNAPSHOT} for k, v in initial.items()]
    write_deltalake(
        silver_dir,
        pa.Table.from_pylist(rows, schema=SILVER_SCHEMA),
        mode="overwrite",
        schema_mode="overwrite",
    )

    job = CubicODSDelta("EDW.TEST_TABLE")
    job.silver_uri = silver_dir
    job.history_snapshot = TEST_SNAPSHOT
    for batch in batches:
        batch_df = pl.DataFrame(batch, schema=BATCH_SCHEMA)
        source = job._build_merge_source(batch_df, ["txn_id"])
        watermark = str(batch_df.get_column("header__change_seq").max())
        job.silver = DeltaTable(silver_dir)
        job._merge_apply(source, ["txn_id"], watermark)

    final = pl.from_arrow(DeltaTable(silver_dir).to_pyarrow_table())
    assert isinstance(final, pl.DataFrame)
    return {
        row["txn_id"]: {c: row[c] for c in (*DATA_COLS, "header__change_seq")}
        for row in final.to_dicts()
    }


# ----------------------------------------------------------------------
# The property
# ----------------------------------------------------------------------


@pytest.mark.parametrize("case", range(N_CASES))
def test_pipeline_matches_reference_interpreter(case, tmp_path):
    """Batched resolver+MERGE ≡ one-event-at-a-time reference fold."""
    import random

    rng = random.Random(case)
    initial, events, batches = generate_case(rng)

    with patch("odin.generate.cubic.delta_ods.sigterm_check"):
        actual = pipeline_final(str(tmp_path / "silver"), initial, batches)
    expected = reference_final(initial, events)

    assert actual == expected, (
        f"case {case}: pipeline diverges from reference interpreter\n"
        f"initial={initial}\nevents={events}\n"
        f"batch sizes={[len(b) for b in batches]}\n"
        f"actual={actual}\nexpected={expected}"
    )
