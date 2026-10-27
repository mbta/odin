from datetime import date
from datetime import datetime

import pyarrow as pa
from deltalake import DeltaTable
from deltalake import write_deltalake

from odin.utils.delta import column_maxes


def test_column_maxes_reads_stats_across_files(tmp_path):
    """Each column's max is taken over every active file's statistics."""
    for day in (1, 9, 4):
        write_deltalake(
            str(tmp_path),
            pa.table(
                {
                    "ts": pa.array([datetime(2026, 7, day, 12)], pa.timestamp("us")),
                    "day": pa.array([date(2026, 7, day)]),
                    "never_set": pa.array([None], pa.timestamp("us")),
                }
            ),
            mode="append",
        )

    maxes = column_maxes(DeltaTable(str(tmp_path)), ["ts", "day", "never_set"])

    assert maxes == {"ts": datetime(2026, 7, 9, 12), "day": date(2026, 7, 9), "never_set": None}


def test_column_maxes_without_stats_is_none(tmp_path):
    """
    A column outside delta.dataSkippingNumIndexedCols has no statistics, so its max is None.

    Wide silver tables only index their first 32 columns by default, so this is the case
    for any timestamp column past that point.
    """
    write_deltalake(
        str(tmp_path),
        pa.table(
            {
                "indexed": pa.array([datetime(2026, 7, 7)], pa.timestamp("us")),
                "unindexed": pa.array([datetime(2026, 7, 8)], pa.timestamp("us")),
            }
        ),
        configuration={"delta.dataSkippingNumIndexedCols": "1"},
    )

    maxes = column_maxes(DeltaTable(str(tmp_path)), ["indexed", "unindexed"])

    assert maxes == {"indexed": datetime(2026, 7, 7), "unindexed": None}


def test_column_maxes_on_empty_table(tmp_path):
    """A table with no active files has no maxima to report."""
    write_deltalake(
        str(tmp_path),
        pa.table({"ts": pa.array([], pa.timestamp("us"))}),
    )

    assert column_maxes(DeltaTable(str(tmp_path)), ["ts"]) == {"ts": None}
