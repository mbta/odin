import json
from unittest.mock import patch

from odin.ingestion.masabi.masabi_archive import ArchiveMasabi

_TABLE = "retail.account_actions"
_MAX_ROWS = 50


@patch("odin.ingestion.masabi.masabi_archive.TABLES", [_TABLE])
@patch("odin.ingestion.masabi.masabi_archive.MAXIMUM_ROWS_PER_RUN", _MAX_ROWS)
def test_download_account_actions(tmpdir):
    """
    Integration smoke test

    Download a small slice of retail.account_actions and
    verify basic structural invariants of the written NDJSON.

    Requires MASABI_API_ROOT, MASABI_DATA_API_USERNAME, and
    MASABI_DATA_API_PASSWORD to be set in the environment.
    """
    job = ArchiveMasabi(_TABLE)
    job.tmpdir = str(tmpdir)
    job.run()

    ndjson_path = tmpdir.join("retail_account_actions.ndjson")
    assert ndjson_path.exists(), "Expected NDJSON file was not written"

    rows = []
    for line in open(ndjson_path, "r"):
        rows.append(json.loads(line))

    assert len(rows) == _MAX_ROWS, f"Expected {_MAX_ROWS} rows, got {len(rows)}"

    # serverTimestamp must be non-decreasing (API returns orderBy serverTimestamp:asc).
    timestamps = [row["serverTimestamp"] for row in rows]
    for i in range(1, len(timestamps)):
        assert timestamps[i] >= timestamps[i - 1], (
            f"serverTimestamp decreased at row {i}: {timestamps[i - 1]} -> {timestamps[i]}"
        )
