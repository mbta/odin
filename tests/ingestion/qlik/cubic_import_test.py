from datetime import datetime
from datetime import UTC
from unittest.mock import patch

import pytest

from odin.ingestion.qlik.cubic_import import CubicImport
from odin.ingestion.qlik.cubic_import import move_pair
from odin.ingestion.qlik.cubic_import import dfm_path
from odin.utils.aws.s3 import S3Object

_TABLE = "EDW.TEST_TABLE"
_MOD = "odin.ingestion.qlik.cubic_import"

_SNAP_DT = datetime(2026, 7, 1, 4, 18, 23, 123456, tzinfo=UTC)
_SNAP = "20260701T041823Z"
_CDC_DT = datetime(2026, 7, 2, 10, 30, 5, tzinfo=UTC)


def _incoming(table_dir: str, basename: str) -> str:
    return f"s3://incoming/cubic/ods_qlik/{table_dir}/{basename}"


def _obj(table_dir: str, basename: str, last_modified: datetime, size: int = 100) -> S3Object:
    return S3Object(
        path=_incoming(table_dir, basename),
        last_modified=last_modified,
        size_bytes=size,
    )


def _with_dfms(objects: list[S3Object]) -> list[S3Object]:
    """Add a companion .dfm object for every .csv.gz object."""
    dfms = [
        S3Object(path=dfm_path(o.path), last_modified=o.last_modified, size_bytes=1)
        for o in objects
    ]
    return objects + dfms


def _run_table_moves(load_objects, ct_objects, archive_partitions):
    """Run CubicImport.table_moves with mocked S3 calls."""

    def mock_list_objects(prefix, **kwargs):
        if f"{_TABLE}__ct/" in prefix:
            return ct_objects
        return load_objects

    with (
        patch(f"{_MOD}.list_objects", side_effect=mock_list_objects),
        patch(f"{_MOD}.list_partitions", return_value=archive_partitions),
        patch(f"{_MOD}.DATA_INCOMING", "incoming"),
        patch(f"{_MOD}.DATA_ARCHIVE", "archive"),
        patch(f"{_MOD}.DATA_ERROR", "error"),
    ):
        return CubicImport().table_moves(_TABLE)


def test_new_snapshot_from_load_00001():
    """LOAD00000001.csv.gz LastModified defines the snapshot partition for the LOAD group."""
    loads = _with_dfms(
        [
            _obj(_TABLE, "LOAD00000001.csv.gz", _SNAP_DT),
            _obj(_TABLE, "LOAD00000002.csv.gz", _SNAP_DT),
        ]
    )
    moves = _run_table_moves(loads, [], archive_partitions=["snapshot=20260101T000000Z"])

    assert [(m[0], m[1]) for m in moves] == [
        (
            _incoming(_TABLE, "LOAD00000001.csv.gz"),
            f"archive/cubic/ods_qlik/{_TABLE}/snapshot={_SNAP}/LOAD00000001.csv.gz",
        ),
        (
            _incoming(_TABLE, "LOAD00000002.csv.gz"),
            f"archive/cubic/ods_qlik/{_TABLE}/snapshot={_SNAP}/LOAD00000002.csv.gz",
        ),
    ]
    # both are archive moves with dfm present in incoming and required
    assert all(m[2] is True and m[3] is True for m in moves)


def test_load_without_00001_uses_archive_partition():
    """A LOAD file landing after its 00001 was already moved joins the existing partition."""
    loads = _with_dfms([_obj(_TABLE, "LOAD00000002.csv.gz", _SNAP_DT)])
    moves = _run_table_moves(loads, [], archive_partitions=[f"snapshot={_SNAP}"])

    assert moves[0][1] == f"archive/cubic/ods_qlik/{_TABLE}/snapshot={_SNAP}/LOAD00000002.csv.gz"


def test_cdc_files_use_current_snapshot():
    """__ct files are archived under the current snapshot partition of the parent table."""
    ct = _with_dfms([_obj(f"{_TABLE}__ct", "20260702-103005123.csv.gz", _CDC_DT)])
    moves = _run_table_moves([], ct, archive_partitions=[f"snapshot={_SNAP}"])

    assert moves[0][1] == (
        f"archive/cubic/ods_qlik/{_TABLE}__ct/snapshot={_SNAP}/20260702-103005123.csv.gz"
    )


def test_cdc_files_use_max_snapshot_partition():
    """The most recent of multiple archive snapshot partitions is the current snapshot."""
    ct = _with_dfms([_obj(f"{_TABLE}__ct", "20260702-103005123.csv.gz", _CDC_DT)])
    moves = _run_table_moves(
        [], ct, archive_partitions=["snapshot=20250101T000000Z", f"snapshot={_SNAP}"]
    )

    assert f"snapshot={_SNAP}" in moves[0][1]


def test_snapshot_carries_from_load_to_cdc():
    """A new 00001 in the same run updates the snapshot used for __ct files."""
    loads = _with_dfms([_obj(_TABLE, "LOAD00000001.csv.gz", _SNAP_DT)])
    ct = _with_dfms([_obj(f"{_TABLE}__ct", "20260702-103005123.csv.gz", _CDC_DT)])
    moves = _run_table_moves(loads, ct, archive_partitions=["snapshot=20250101T000000Z"])

    assert all(f"snapshot={_SNAP}" in m[1] for m in moves)


def test_cdc_before_any_snapshot_goes_to_error():
    """__ct files arriving before any snapshot exists are routed to the Error bucket."""
    ct = _with_dfms([_obj(f"{_TABLE}__ct", "20260702-103005123.csv.gz", _CDC_DT)])
    moves = _run_table_moves([], ct, archive_partitions=[])

    assert moves[0][1] == (
        f"error/cubic/ods_qlik/{_TABLE}__ct/timestamp=20260702T103005Z/20260702-103005123.csv.gz"
    )
    # error moves do not require a dfm
    assert moves[0][3] is False


def test_zero_byte_file_goes_to_error():
    """0-byte files are routed to the Error bucket, even when a snapshot exists."""
    loads = _with_dfms([_obj(_TABLE, "LOAD00000002.csv.gz", _SNAP_DT, size=0)])
    moves = _run_table_moves(loads, [], archive_partitions=[f"snapshot={_SNAP}"])

    assert moves[0][1] == (f"error/cubic/ods_qlik/{_TABLE}/timestamp={_SNAP}/LOAD00000002.csv.gz")


def test_zero_byte_00001_still_starts_snapshot():
    """An empty 00001 is errored but still updates the snapshot (data_platform parity)."""
    loads = _with_dfms(
        [
            _obj(_TABLE, "LOAD00000001.csv.gz", _SNAP_DT, size=0),
            _obj(_TABLE, "LOAD00000002.csv.gz", _SNAP_DT),
        ]
    )
    moves = _run_table_moves(loads, [], archive_partitions=["snapshot=20250101T000000Z"])

    assert moves[0][1].startswith("error/")
    assert moves[1][1] == f"archive/cubic/ods_qlik/{_TABLE}/snapshot={_SNAP}/LOAD00000002.csv.gz"


def test_missing_dfm_flagged():
    """A csv.gz without its companion .dfm in incoming is flagged for move_pair."""
    loads = [_obj(_TABLE, "LOAD00000002.csv.gz", _SNAP_DT)]  # no dfms added
    moves = _run_table_moves(loads, [], archive_partitions=[f"snapshot={_SNAP}"])

    assert moves[0][2] is False


def test_move_pair_moves_dfm_first():
    """move_pair moves the .dfm before the .csv.gz."""
    calls = []
    with patch(f"{_MOD}.rename_object", side_effect=lambda f, t: calls.append((f, t))):
        result = move_pair(("s3://in/a.csv.gz", "s3://out/a.csv.gz", True, True))

    assert result is None
    assert calls == [
        ("s3://in/a.dfm", "s3://out/a.dfm"),
        ("s3://in/a.csv.gz", "s3://out/a.csv.gz"),
    ]


def test_move_pair_dfm_move_failure_skips_csv():
    """If the .dfm move fails, the .csv.gz is not moved and the pair is retried."""

    def mock_rename(from_obj, to_obj):
        if from_obj.endswith(".dfm"):
            return from_obj
        raise AssertionError("csv should not be moved when dfm move fails")

    with patch(f"{_MOD}.rename_object", side_effect=mock_rename):
        result = move_pair(("s3://in/a.csv.gz", "s3://out/a.csv.gz", True, True))

    assert result == "s3://in/a.csv.gz"


def test_move_pair_missing_dfm_skips_unless_at_destination():
    """A required missing .dfm skips the pair unless it already exists at the destination."""
    with (
        patch(f"{_MOD}.rename_object", side_effect=lambda f, t: None) as mock_rename,
        patch(f"{_MOD}.object_exists", return_value=False),
    ):
        assert move_pair(("s3://in/a.csv.gz", "s3://out/a.csv.gz", False, True)) == (
            "s3://in/a.csv.gz"
        )
        mock_rename.assert_not_called()

    with (
        patch(f"{_MOD}.rename_object", side_effect=lambda f, t: None) as mock_rename,
        patch(f"{_MOD}.object_exists", return_value=True),
    ):
        assert move_pair(("s3://in/a.csv.gz", "s3://out/a.csv.gz", False, True)) is None
        mock_rename.assert_called_once_with("s3://in/a.csv.gz", "s3://out/a.csv.gz")


def test_move_pair_error_route_without_dfm():
    """Error-bucket moves proceed without a .dfm (require_dfm=False)."""
    with patch(f"{_MOD}.rename_object", side_effect=lambda f, t: None) as mock_rename:
        assert move_pair(("s3://in/a.csv.gz", "s3://err/a.csv.gz", False, False)) is None
        mock_rename.assert_called_once_with("s3://in/a.csv.gz", "s3://err/a.csv.gz")


def test_run_raises_on_wrong_instance():
    """CubicImport.run raises immediately when not on the designated instance."""
    with patch(f"{_MOD}.get_odin_instance", return_value="beta"):
        with pytest.raises(RuntimeError, match="CubicImport only runs on"):
            CubicImport().run()
