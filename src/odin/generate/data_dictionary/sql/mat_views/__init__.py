"""
Gets name of each materialized view.

scripts/get_table_status.py uses this for expected_tables().
"""

from importlib.resources import files

MAT_VIEWS: list[str] = sorted(
    entry.name[: -len(".sql")] for entry in files(__name__).iterdir() if entry.name.endswith(".sql")
)
