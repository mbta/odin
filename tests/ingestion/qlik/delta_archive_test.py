"""
Tests for CubicQlikBronze (Delta-based Qlik bronze archive).

A FakeS3 serves an in-memory bucket tree through the module's S3 helpers
(list/stream/partitions patched in the delta_archive and dfm namespaces);
the bronze table is a real local Delta table. Tests drive `run()` end to end
and assert on bronze contents plus the position recorded in commit metadata.
"""

import gzip
import io
import json
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import patch

import polars as pl
import pytest

from deltalake import DeltaTable
from deltalake import write_deltalake

from odin.ingestion.qlik.delta_archive import BronzeStateError
from odin.ingestion.qlik.delta_archive import CubicQlikBronze
from odin.ingestion.qlik.delta_archive import NEXT_RUN_IMMEDIATE
from odin.ingestion.qlik.delta_archive import NoQlikLoadFilesError
from odin.ingestion.qlik.delta_archive import STATE_CDC_WATERMARK_KEY
from odin.ingestion.qlik.delta_archive import STATE_LOAD_COMPLETE_KEY
from odin.ingestion.qlik.delta_archive import STATE_SNAPSHOT_KEY
from odin.ingestion.qlik.delta_archive import canonical_key
from odin.utils.aws.s3 import S3Object

TABLE = "EDW.TEST_TABLE"
ARCHIVE = "archive"
ERROR = "error"
PROCESSED = "odin/archive/cubic_qlik/processed"

SNAP1 = "20250101T000000Z"
SNAP1_ISO = "2025-01-01T00:00:00"
SNAP2 = "20250301T000000Z"
SNAP2_ISO = "2025-03-01T00:00:00"

OLD_MTIME = datetime(2025, 1, 1, tzinfo=UTC)

def col(name: str, type_: str, pk: int = 0) -> dict:
    """Build one DFM dataInfo column entry."""
    return {
        "name": name, "type": type_, "length": 0, "precision": 0, "scale": 0, "primaryKeyPos": pk
    }


LOAD_COLUMNS = [col("ID", "INT4", pk=1), col("AMOUNT", "INT4"), col("NOTE", "STRING")]
CDC_HEADER_COLUMNS = [
    col("header__change_seq", "STRING"),
    col("header__change_oper", "STRING"),
    col("header__timestamp", "DATETIME"),
]


class FakeS3:
    """In-memory S3: {bucket: {key: (content, last_modified)}}."""

    def __init__(self) -> None:
        """Create empty FakeS3."""
        self.buckets: dict[str, dict[str, tuple[bytes, datetime]]] = {}

    def put(self, bucket: str, key: str, content: bytes, mtime: datetime = OLD_MTIME) -> None:
        """Store an object."""
        self.buckets.setdefault(bucket, {})[key] = (content, mtime)

    def delete(self, bucket: str, key: str) -> None:
        """Delete an object."""
        del self.buckets[bucket][key]

    def move_to_processed(self, bucket: str, key: str) -> None:
        """Mimic the legacy job's rename_objects (copy to processed, delete)."""
        content, _ = self.buckets[bucket][key]
        self.put(ARCHIVE, f"{PROCESSED}/{key}", content, datetime.now(tz=UTC))
        self.delete(bucket, key)

    @staticmethod
    def _split(partition: str) -> tuple[str, str]:
        bucket, prefix = partition.replace("s3://", "").split("/", 1)
        return bucket, prefix

    def list_objects(
        self,
        partition: str,
        max_objects: int = 1_000_000,
        in_filter: Any = None,
        in_func: Any = None,
        start_after: Any = None,
    ) -> list[S3Object]:
        """Mimic odin list_objects (sorted keys, size-0 skip, filters, StartAfter)."""
        bucket, prefix = self._split(partition)
        found = []
        for key in sorted(self.buckets.get(bucket, {})):
            if not key.startswith(prefix):
                continue
            if start_after is not None and key <= start_after:
                continue
            content, mtime = self.buckets[bucket][key]
            if len(content) == 0:
                continue
            if in_filter is not None and in_filter not in key:
                continue
            obj = S3Object(
                path=f"s3://{bucket}/{key}", last_modified=mtime, size_bytes=len(content)
            )
            if in_func is not None and in_func(obj) is False:
                continue
            found.append(obj)
            if len(found) >= max_objects:
                break
        return found

    def list_partitions(self, partition: str, max_objects: int = 10_000) -> list[str]:
        """Mimic odin list_partitions (one level of common prefixes)."""
        bucket, prefix = self._split(partition)
        if not prefix.endswith("/"):
            prefix = f"{prefix}/"
        parts = []
        for key in sorted(self.buckets.get(bucket, {})):
            if key.startswith(prefix) and "/" in key[len(prefix) :]:
                part = key[len(prefix) :].split("/", 1)[0]
                if part not in parts:
                    parts.append(part)
        return parts

    def stream_object(self, path: str) -> io.BytesIO:
        """Mimic odin stream_object; raises when the object is absent."""
        bucket, key = self._split(path)
        if key not in self.buckets.get(bucket, {}):
            raise FileNotFoundError(path)
        return io.BytesIO(self.buckets[bucket][key][0])


def gz(text: str) -> bytes:
    """Gzip-compress a text payload."""
    return gzip.compress(text.encode("utf8"))


def dfm_bytes(columns: list[dict], start_write: str) -> bytes:
    """Build a minimal .dfm payload."""
    return json.dumps(
        {
            "fileInfo": {"startWriteTimestamp": start_write},
            "dataInfo": {"columns": columns},
        }
    ).encode("utf8")


def put_load_file(
    s3: FakeS3,
    snap: str,
    snap_iso: str,
    seq: int,
    rows: list[str],
    bucket: str = ARCHIVE,
    mtime: datetime = OLD_MTIME,
) -> str:
    """Store one LOAD csv.gz + .dfm; return the object key."""
    key = f"cubic/ods_qlik/{TABLE}/snapshot={snap}/LOAD{seq:08d}.csv.gz"
    csv = "\n".join(["ID,AMOUNT,NOTE"] + rows) + "\n"
    s3.put(bucket, key, gz(csv), mtime)
    s3.put(bucket, key.replace(".csv.gz", ".dfm"), dfm_bytes(LOAD_COLUMNS, snap_iso), mtime)
    return key


def put_cdc_file(
    s3: FakeS3,
    ts: str,
    rows: list[str],
    bucket: str = ARCHIVE,
    columns: list[dict] | None = None,
    subfolder: str = "timestamp",
) -> str:
    """Store one CDC csv.gz + .dfm; return the object key."""
    key = f"cubic/ods_qlik/{TABLE}__ct/{subfolder}/{ts}.csv.gz"
    header = "header__change_seq,header__change_oper,header__timestamp,ID,AMOUNT,NOTE"
    csv = "\n".join([header] + rows) + "\n"
    s3.put(bucket, key, gz(csv), OLD_MTIME)
    dfm_cols = columns if columns is not None else CDC_HEADER_COLUMNS + LOAD_COLUMNS
    s3.put(bucket, key.replace(".csv.gz", ".dfm"), dfm_bytes(dfm_cols, "2025-01-01T01:00:00"))
    return key


def cdc_row(seq: str, oper: str, ts_iso: str, id_: int, amount: int, note: str) -> str:
    """Build one CDC csv data row."""
    return f"{seq},{oper},{ts_iso},{id_},{amount},{note}"


@pytest.fixture(name="s3")
def fixture_s3() -> Any:
    """Provide a FakeS3 patched into the delta_archive and dfm namespaces."""
    fake = FakeS3()
    patches: list[Any] = [
        patch("odin.ingestion.qlik.delta_archive.DATA_ARCHIVE", ARCHIVE),
        patch("odin.ingestion.qlik.delta_archive.DATA_ERROR", ERROR),
        patch("odin.ingestion.qlik.delta_archive.list_objects", fake.list_objects),
        patch("odin.ingestion.qlik.delta_archive.list_partitions", fake.list_partitions),
        patch("odin.ingestion.qlik.delta_archive.stream_object", fake.stream_object),
        patch("odin.ingestion.qlik.dfm.stream_object", fake.stream_object),
    ]
    for p in patches:
        p.start()
    yield fake
    for p in patches:
        p.stop()


def make_job(tmp_path: Any) -> CubicQlikBronze:
    """Create a job writing bronze to a local temp path."""
    job = CubicQlikBronze(TABLE)
    job.bronze_uri = str(tmp_path / "bronze")
    return job


def bronze_df(job: CubicQlikBronze) -> pl.DataFrame:
    """Read the job's bronze table into polars."""
    return pl.from_arrow(DeltaTable(job.bronze_uri).to_pyarrow_table())  # type: ignore[return-value]


def last_commit(job: CubicQlikBronze) -> dict:
    """Return the latest commit info dict of the bronze table."""
    return DeltaTable(job.bronze_uri).history(1)[0]


def seed_snap1(s3: FakeS3) -> None:
    """Seed a standard SNAP1 LOAD group and three CDC files."""
    put_load_file(s3, SNAP1, SNAP1_ISO, 1, ["1,10,a", "2,20,b"])
    put_load_file(s3, SNAP1, SNAP1_ISO, 2, ["3,30,c"])
    # pre-bootstrap CDC file (older than SNAP1): must be skipped, not read
    put_cdc_file(s3, "20241231-235959000", [cdc_row("X", "I", "2024-12-31T23:59:59", 9, 9, "x")])
    put_cdc_file(
        s3,
        "20250102-000000000",
        [cdc_row("20250102000000000000001", "I", "2025-01-02T00:00:00", 4, 40, "d")],
    )
    put_cdc_file(
        s3,
        "20250103-000000000",
        [
            cdc_row("20250103000000000000001", "U", "2025-01-03T00:00:00", 1, 11, "a2"),
            cdc_row("20250103000000000000002", "D", "2025-01-03T00:00:01", 2, 0, ""),
        ],
    )


def test_canonical_key() -> None:
    """Canonical keys are stable across the incoming/error/processed locations."""
    incoming = f"s3://{ARCHIVE}/cubic/ods_qlik/{TABLE}__ct/timestamp/20250102-000000000.csv.gz"
    error = f"s3://{ERROR}/cubic/ods_qlik/{TABLE}__ct/timestamp/20250102-000000000.csv.gz"
    processed = f"s3://{ARCHIVE}/{PROCESSED}/cubic/ods_qlik/{TABLE}__ct/timestamp/20250102-000000000.csv.gz"
    assert canonical_key(incoming) == canonical_key(error) == canonical_key(processed)
    assert canonical_key(incoming).startswith("cubic/ods_qlik/")


def test_bootstrap_load_and_cdc(s3: FakeS3, tmp_path: Any) -> None:
    """Bootstrap ingests the newest snapshot's LOAD group then CDC past it."""
    # an old, fully-processed snapshot group must be ignored at bootstrap
    put_load_file(s3, "20240601T000000Z", "2024-06-01T00:00:00", 1, ["8,80,old"])
    for key in list(s3.buckets[ARCHIVE]):
        if "20240601" in key:
            s3.move_to_processed(ARCHIVE, key)
    seed_snap1(s3)

    job = make_job(tmp_path)
    job.run()
    df = bronze_df(job)

    loads = df.filter(pl.col("odin_change_class") == "load")
    assert loads.height == 3
    assert set(loads.get_column("snapshot").to_list()) == {SNAP1}
    assert set(loads.get_column("header__change_oper").to_list()) == {"L"}
    assert loads.get_column("header__change_seq").null_count() == 3

    cdc = df.filter(pl.col("odin_change_class") == "cdc")
    assert cdc.height == 3  # pre-bootstrap file skipped
    assert set(cdc.get_column("snapshot").to_list()) == {SNAP1}
    assert set(cdc.get_column("header__change_oper").to_list()) == {"I", "U", "D"}
    assert cdc.get_column("header__year").to_list() == [2025, 2025, 2025]

    commit = last_commit(job)
    assert commit[STATE_SNAPSHOT_KEY] == SNAP1
    assert commit[STATE_LOAD_COMPLETE_KEY] == "true"
    assert commit[STATE_CDC_WATERMARK_KEY] == "20250103-000000000"


def test_load_with_no_cdc_completes(s3: FakeS3, tmp_path: Any) -> None:
    """A snapshot load followed by zero CDC files must still commit completion."""
    put_load_file(s3, SNAP1, SNAP1_ISO, 1, ["1,10,a"])
    job = make_job(tmp_path)
    job.run()
    assert last_commit(job)[STATE_LOAD_COMPLETE_KEY] == "true"

    # rerun must be a no-op, not a resume of a half-open group
    job2 = make_job(tmp_path)
    job2.run()
    assert bronze_df(job2).height == 1


def test_caught_up_run_is_noop(s3: FakeS3, tmp_path: Any) -> None:
    """A second run with no new files appends nothing and keeps its position."""
    seed_snap1(s3)
    job = make_job(tmp_path)
    job.run()
    version = DeltaTable(job.bronze_uri).version()

    job.run()
    assert DeltaTable(job.bronze_uri).version() == version


def test_late_arrival_is_ignored(s3: FakeS3, tmp_path: Any) -> None:
    """A CDC file older than the watermark is never ingested (by design)."""
    seed_snap1(s3)
    job = make_job(tmp_path)
    job.run()
    rows = bronze_df(job).height

    put_cdc_file(
        s3, "20250102-120000000", [cdc_row("LATE", "I", "2025-01-02T12:00:00", 7, 70, "late")]
    )
    job.run()
    assert bronze_df(job).height == rows


def test_files_moved_by_legacy_not_reingested(s3: FakeS3, tmp_path: Any) -> None:
    """Legacy moving consumed files to processed/ must not cause re-ingestion."""
    seed_snap1(s3)
    job = make_job(tmp_path)
    job.run()
    rows = bronze_df(job).height

    for key in list(s3.buckets[ARCHIVE]):
        if not key.startswith(PROCESSED):
            s3.move_to_processed(ARCHIVE, key)
    job.run()
    assert bronze_df(job).height == rows


def test_mid_run_move_fails_then_recovers(s3: FakeS3, tmp_path: Any) -> None:
    """A file moved between listing and read fails the run; the retry recovers."""
    seed_snap1(s3)

    real_stream = s3.stream_object
    key = f"cubic/ods_qlik/{TABLE}__ct/timestamp/20250102-000000000.csv.gz"

    def move_then_stream(path: str) -> Any:
        # "legacy" moves the file at the moment this job reaches for it: the
        # listing captured its incoming path, the read finds it gone
        if key in path and key in s3.buckets.get(ARCHIVE, {}):
            s3.move_to_processed(ARCHIVE, key)
            s3.move_to_processed(ARCHIVE, key.replace(".csv.gz", ".dfm"))
        return real_stream(path)

    with patch("odin.ingestion.qlik.delta_archive.stream_object", move_then_stream):
        job = make_job(tmp_path)
        with pytest.raises(FileNotFoundError):
            job.run()

    # the retry lists the file at its processed location and ingests everything
    job2 = make_job(tmp_path)
    job2.run()
    cdc = bronze_df(job2).filter(pl.col("odin_change_class") == "cdc")
    assert cdc.height == 3
    assert cdc.filter(pl.col("id") == 4).height == 1


def test_recency_guard_incoming_only(s3: FakeS3, tmp_path: Any) -> None:
    """A fresh incoming LOAD file pauses the run; fresh processed/ mtimes don't."""
    seed_snap1(s3)
    put_load_file(s3, SNAP2, SNAP2_ISO, 1, ["5,50,e"], mtime=datetime.now(tz=UTC))

    job = make_job(tmp_path)
    delay = job.run()
    assert job.start_kwargs.get("skipped_recent_snapshot") == "true"
    assert delay > 0
    assert job.bronze is None  # nothing was ingested

    # age the fresh file, run again: both snapshots ingest (bootstrap = newest,
    # so SNAP1 is skipped and SNAP2 becomes the bootstrapped snapshot)
    key = f"cubic/ods_qlik/{TABLE}/snapshot={SNAP2}/LOAD00000001.csv.gz"
    content, _ = s3.buckets[ARCHIVE][key]
    s3.put(ARCHIVE, key, content, OLD_MTIME)
    job.run()
    assert set(bronze_df(job).get_column("snapshot").to_list()) == {SNAP2}


def test_new_snapshot_supersedes_older_cdc(s3: FakeS3, tmp_path: Any) -> None:
    """A new snapshot is loaded; CDC older than it is skipped as superseded."""
    seed_snap1(s3)
    job = make_job(tmp_path)
    job.run()

    put_load_file(s3, SNAP2, SNAP2_ISO, 1, ["1,100,new"])
    # between SNAP1 and SNAP2 -> superseded by SNAP2's LOAD; after SNAP2 -> ingested
    put_cdc_file(
        s3,
        "20250201-000000000",
        [cdc_row("20250201000000000000001", "U", "2025-02-01T00:00:00", 3, 31, "c2")],
    )
    put_cdc_file(
        s3,
        "20250302-000000000",
        [cdc_row("20250302000000000000001", "I", "2025-03-02T00:00:00", 6, 60, "f")],
    )
    job.run()
    df = bronze_df(job)

    assert df.filter(
        (pl.col("odin_change_class") == "load") & (pl.col("snapshot") == SNAP2)
    ).height == 1
    assert df.filter(pl.col("id") == 3, pl.col("amount") == 31).height == 0  # superseded
    assert df.filter(pl.col("id") == 6).get_column("snapshot").to_list() == [SNAP2]
    commit = last_commit(job)
    assert commit[STATE_SNAPSHOT_KEY] == SNAP2
    # the superseded file's timestamp is absorbed by the advancing watermark
    assert commit[STATE_CDC_WATERMARK_KEY] == "20250302-000000000"


def test_crash_between_load_batches_resumes_without_duplicates(
    s3: FakeS3, tmp_path: Any
) -> None:
    """A crash after a mid-group commit resumes past the load watermark."""
    seed_snap1(s3)
    job = make_job(tmp_path)

    calls = {"n": 0}
    real_append = job._append

    def append_then_die(frame: pl.DataFrame, state: Any) -> None:
        real_append(frame, state)
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("injected crash")

    with patch("odin.ingestion.qlik.delta_archive.LOAD_BATCH_FILES", 1):
        with patch.object(job, "_append", append_then_die):
            with pytest.raises(RuntimeError, match="injected crash"):
                job.run()
        commit = last_commit(job)
        assert commit[STATE_LOAD_COMPLETE_KEY] == "false"

        job2 = make_job(tmp_path)
        job2.run()

    df = bronze_df(job2)
    loads = df.filter(pl.col("odin_change_class") == "load")
    assert loads.height == 3
    assert loads.get_column("id").sort().to_list() == [1, 2, 3]  # no duplicates
    assert last_commit(job2)[STATE_LOAD_COMPLETE_KEY] == "true"


def test_schema_merge_on_added_column(s3: FakeS3, tmp_path: Any) -> None:
    """An additive DFM column widens bronze; earlier rows read NULL."""
    seed_snap1(s3)
    job = make_job(tmp_path)
    job.run()

    wide_columns = CDC_HEADER_COLUMNS + LOAD_COLUMNS + [col("EXTRA", "STRING")]
    key = f"cubic/ods_qlik/{TABLE}__ct/timestamp/20250104-000000000.csv.gz"
    header = "header__change_seq,header__change_oper,header__timestamp,ID,AMOUNT,NOTE,EXTRA"
    row = "20250104000000000000001,I,2025-01-04T00:00:00,5,50,e,xt"
    s3.put(ARCHIVE, key, gz(f"{header}\n{row}\n"))
    s3.put(ARCHIVE, key.replace(".csv.gz", ".dfm"), dfm_bytes(wide_columns, "2025-01-01T01:00:00"))

    job.run()
    df = bronze_df(job)
    assert "extra" in df.columns
    assert df.filter(pl.col("id") == 5).get_column("extra").to_list() == ["xt"]
    assert df.filter(pl.col("id") == 4).get_column("extra").to_list() == [None]


def test_empty_cdc_file_absorbed(s3: FakeS3, tmp_path: Any) -> None:
    """A header-only CDC file appends nothing; a later file advances past it."""
    seed_snap1(s3)
    put_cdc_file(s3, "20250105-000000000", [])  # header-only
    put_cdc_file(
        s3,
        "20250106-000000000",
        [cdc_row("20250106000000000000001", "I", "2025-01-06T00:00:00", 6, 60, "f")],
    )
    job = make_job(tmp_path)
    job.run()
    assert last_commit(job)[STATE_CDC_WATERMARK_KEY] == "20250106-000000000"
    assert bronze_df(job).filter(pl.col("id") == 6).height == 1


def test_cdc_run_cap_returns_immediate(s3: FakeS3, tmp_path: Any) -> None:
    """Hitting the per-run file cap requests an immediate rerun and resumes."""
    seed_snap1(s3)
    with patch("odin.ingestion.qlik.delta_archive.MAX_CDC_FILES", 2):
        job = make_job(tmp_path)
        assert job.run() == NEXT_RUN_IMMEDIATE
    # uncapped follow-up run picks up the rest
    job2 = make_job(tmp_path)
    job2.run()
    assert bronze_df(job2).filter(pl.col("odin_change_class") == "cdc").height == 3


def test_error_bucket_and_processed_sources(s3: FakeS3, tmp_path: Any) -> None:
    """CDC files in the error bucket and in processed/ are both ingested once."""
    seed_snap1(s3)
    put_cdc_file(
        s3,
        "20250107-000000000",
        [cdc_row("20250107000000000000001", "I", "2025-01-07T00:00:00", 7, 70, "err")],
        bucket=ERROR,
    )
    key = put_cdc_file(
        s3,
        "20250108-000000000",
        [cdc_row("20250108000000000000001", "I", "2025-01-08T00:00:00", 8, 80, "proc")],
    )
    s3.move_to_processed(ARCHIVE, key)
    s3.move_to_processed(ARCHIVE, key.replace(".csv.gz", ".dfm"))

    job = make_job(tmp_path)
    job.run()
    df = bronze_df(job)
    assert df.filter(pl.col("id") == 7).height == 1
    assert df.filter(pl.col("id") == 8).height == 1


def test_no_load_files_raises(s3: FakeS3, tmp_path: Any) -> None:
    """A table with no LOAD files anywhere fails loudly."""
    job = make_job(tmp_path)
    with pytest.raises(NoQlikLoadFilesError):
        job.run()


def test_foreign_bronze_table_raises(s3: FakeS3, tmp_path: Any) -> None:
    """A bronze table without a recorded position is refused, not appended to."""
    seed_snap1(s3)
    job = make_job(tmp_path)
    write_deltalake(job.bronze_uri, pl.DataFrame({"a": [1]}).to_arrow())
    with pytest.raises(BronzeStateError):
        job.run()
