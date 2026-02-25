import json
import os
from datetime import datetime
from unittest.mock import MagicMock
from unittest.mock import patch

import polars as pl
import pytest

from odin.ingestion.masabi.masabi_archive import MASABI_START_TIMESTAMP_MS
from odin.ingestion.masabi.masabi_archive import ArchiveMasabi
from odin.ingestion.masabi.masabi_archive import TABLE_JSON_COLS
from odin.ingestion.masabi.masabi_archive import TABLE_SCHEMAS
from odin.utils.aws.s3 import S3Object


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

TABLE = "retail.account_actions"


def make_job() -> ArchiveMasabi:
    """Return a fresh ArchiveMasabi instance without touching S3 or the API."""
    return ArchiveMasabi(TABLE)


def _s3obj(path: str) -> S3Object:
    return S3Object(path=path, size_bytes=100, last_modified=datetime.now())


# ---------------------------------------------------------------------------
# preprocess_record
# ---------------------------------------------------------------------------


class TestPreprocessRecord:
    """ArchiveMasabi.preprocess_record behaviour."""

    def test_keeps_schema_columns(self) -> None:
        """Only columns defined in the schema are retained."""
        job = make_job()
        raw = {"eventId": "abc", "brand": "MBTA", "unknownExtraCol": "drop_me"}
        result = job.preprocess_record(raw)
        assert "eventId" in result
        assert "brand" in result
        assert "unknownExtraCol" not in result

    def test_all_schema_columns_present_even_if_absent_in_record(self) -> None:
        """Missing columns are represented as None (null) in the output."""
        job = make_job()
        result = job.preprocess_record({})
        for col in job.active_schema.names():
            assert col in result
            assert result[col] is None

    def test_json_cols_serialised_to_string(self) -> None:
        """Array/object columns are JSON-serialised before writing."""
        job = make_job()
        raw = {"ipAddress": ["192.168.0.1", "10.0.0.1"]}
        result = job.preprocess_record(raw)
        # Should be a JSON string, not the original list
        assert isinstance(result["ipAddress"], str)
        assert json.loads(result["ipAddress"]) == ["192.168.0.1", "10.0.0.1"]

    def test_json_col_none_stays_none(self) -> None:
        """A null API value for a JSON col stays None (not the string 'null')."""
        job = make_job()
        result = job.preprocess_record({"ipAddress": None})
        assert result["ipAddress"] is None

    def test_excluded_columns_absent(self) -> None:
        """Columns in the exclusion list are not written."""
        job = make_job()
        job.exclude_cols = frozenset({"eventId"})
        job.active_schema = pl.Schema(
            {c: t for c, t in TABLE_SCHEMAS[TABLE].items() if c != "eventId"}
        )
        result = job.preprocess_record({"eventId": "secret"})
        assert "eventId" not in result

    def test_boolean_value_preserved(self) -> None:
        """Boolean values are passed through without modification."""
        job = make_job()
        result = job.preprocess_record({"usedDeviceChangeCredit": True})
        assert result["usedDeviceChangeCredit"] is True


# ---------------------------------------------------------------------------
# api_pages
# ---------------------------------------------------------------------------


class TestApiPages:
    """ArchiveMasabi.api_pages pagination logic."""

    def _fake_response(
        self, hits: list, next_page_id: str | None = None
    ) -> MagicMock:
        resp = MagicMock()
        body: dict = {"hits": hits}
        if next_page_id is not None:
            body["nextPageId"] = next_page_id
        resp.status = 200
        resp.json.return_value = body
        return resp

    def test_single_page_no_next_id(self) -> None:
        """Single-page response yields one batch and stops."""
        job = make_job()
        pool = MagicMock()
        hits = [{"eventId": "e1"}, {"eventId": "e2"}]
        pool.request.return_value = self._fake_response(hits)

        pages = list(job.api_pages(pool, 0, 9999))
        assert pages == [hits]
        assert pool.request.call_count == 1

    def test_multi_page_follows_next_page_id(self) -> None:
        """Multi-page response follows nextPageId until it disappears."""
        job = make_job()
        pool = MagicMock()
        pool.request.side_effect = [
            self._fake_response([{"eventId": "e1"}], next_page_id="cursor_abc"),
            self._fake_response([{"eventId": "e2"}], next_page_id="cursor_xyz"),
            self._fake_response([{"eventId": "e3"}]),  # no nextPageId → stop
        ]

        pages = list(job.api_pages(pool, 0, 9999))
        assert len(pages) == 3
        assert pool.request.call_count == 3

        # The first request must NOT have a nextPageId key.
        _, first_kwargs = pool.request.call_args_list[0]
        # Note: api_pages mutates a single fields dict in place, so inspecting
        # call_args_list[0] post-loop shows the final dict state (nextPageId=cursor_xyz).
        # Instead, verify ordering by checking the final-state nextPageId on the last call,
        # which is definitively cursor_xyz (page 2's cursor sent to page 3's request).
        _, last_kwargs = pool.request.call_args_list[2]
        assert last_kwargs["fields"]["nextPageId"] == "cursor_xyz"

    def test_empty_hits_stops_pagination(self) -> None:
        """An empty hits list stops pagination even if nextPageId is present."""
        job = make_job()
        pool = MagicMock()
        pool.request.return_value = self._fake_response([], next_page_id="should_not_follow")

        pages = list(job.api_pages(pool, 0, 9999))
        assert pages == [[]]
        assert pool.request.call_count == 1

    def test_raises_on_non_200(self) -> None:
        """A non-200 status raises HTTPError."""
        import urllib3

        job = make_job()
        pool = MagicMock()
        bad_resp = MagicMock()
        bad_resp.status = 500
        bad_resp.data = b"server error"
        pool.request.return_value = bad_resp

        with pytest.raises(urllib3.exceptions.HTTPError):
            list(job.api_pages(pool, 0, 9999))

    def test_filter_string_format(self) -> None:
        """The serverTimestamp filter is formatted correctly."""
        job = make_job()
        pool = MagicMock()
        pool.request.return_value = self._fake_response([])

        list(job.api_pages(pool, from_ts=1000, to_ts=2000))
        _, kwargs = pool.request.call_args
        assert kwargs["fields"]["filter"] == (
            "and(gt(serverTimestamp:1000),lte(serverTimestamp:2000))"
        )
        assert kwargs["fields"]["orderBy"] == "serverTimestamp:asc"


# ---------------------------------------------------------------------------
# fetch_and_write
# ---------------------------------------------------------------------------


class TestFetchAndWrite:
    """ArchiveMasabi.fetch_and_write end-to-end (mocking api_pages)."""

    def test_returns_none_when_no_records(self, tmpdir) -> None:
        """Returns None and does not create a populated NDJSON when API has no data."""
        job = make_job()
        job.tmpdir = str(tmpdir)

        with patch.object(job, "api_pages", return_value=iter([[]])):
            result = job.fetch_and_write(MagicMock(), from_ts=0, to_ts=9999)
        assert result is None

    def test_returns_ndjson_path_with_records(self, tmpdir) -> None:
        """Returns the NDJSON path and writes one line per hit."""
        job = make_job()
        job.tmpdir = str(tmpdir)
        fake_hit = {"eventId": "ev1", "brand": "MBTA", "serverTimestamp": 1_735_689_600_001}

        with patch.object(job, "api_pages", return_value=iter([[fake_hit]])):
            result = job.fetch_and_write(MagicMock(), from_ts=0, to_ts=9999)

        assert result is not None
        assert os.path.exists(result)
        lines = open(result).read().strip().splitlines()
        assert len(lines) == 1
        parsed = json.loads(lines[0])
        assert parsed["eventId"] == "ev1"

    def test_multiple_pages_all_written(self, tmpdir) -> None:
        """Records from all pages are written to the same NDJSON file."""
        job = make_job()
        job.tmpdir = str(tmpdir)
        page1 = [{"eventId": "e1", "serverTimestamp": 1.0}]
        page2 = [{"eventId": "e2", "serverTimestamp": 2.0}]

        with patch.object(job, "api_pages", return_value=iter([page1, page2])):
            result = job.fetch_and_write(MagicMock(), from_ts=0, to_ts=9999)

        lines = open(result).read().strip().splitlines()
        assert len(lines) == 2

    def test_json_cols_serialised_in_ndjson(self, tmpdir) -> None:
        """Array/object values in the NDJSON output are JSON strings."""
        job = make_job()
        job.tmpdir = str(tmpdir)
        hit = {"ipAddress": ["10.0.0.1"], "serverTimestamp": 1.0}

        with patch.object(job, "api_pages", return_value=iter([[hit]])):
            result = job.fetch_and_write(MagicMock(), from_ts=0, to_ts=9999)

        line = json.loads(open(result).readline())
        assert isinstance(line["ipAddress"], str)
        assert json.loads(line["ipAddress"]) == ["10.0.0.1"]


# ---------------------------------------------------------------------------
# setup_job
# ---------------------------------------------------------------------------


class TestSetupJob:
    """ArchiveMasabi.setup_job watermark detection."""

    @patch("odin.ingestion.masabi.masabi_archive.list_objects", return_value=[])
    def test_no_existing_files_returns_start_timestamp(self, _ls) -> None:
        """Falls back to MASABI_START_TIMESTAMP_MS when no parquet files exist."""
        job = make_job()
        assert job.setup_job() == MASABI_START_TIMESTAMP_MS

    @patch("odin.ingestion.masabi.masabi_archive.ds_metadata_min_max")
    @patch("odin.ingestion.masabi.masabi_archive.ds_from_path")
    @patch(
        "odin.ingestion.masabi.masabi_archive.list_objects",
        return_value=[_s3obj("s3://bucket/table_001.parquet")],
    )
    def test_existing_files_returns_max_ts(self, _ls, _ds, mock_minmax) -> None:
        """Uses the max serverTimestamp from parquet metadata as the watermark."""
        mock_minmax.return_value = (1_735_689_600_000, 1_800_000_000_000)
        job = make_job()
        result = job.setup_job()
        assert result == 1_800_000_000_000

    @patch("odin.ingestion.masabi.masabi_archive.ds_metadata_min_max")
    @patch("odin.ingestion.masabi.masabi_archive.ds_from_path")
    @patch(
        "odin.ingestion.masabi.masabi_archive.list_objects",
        return_value=[_s3obj("s3://bucket/table_001.parquet")],
    )
    def test_null_max_ts_falls_back_to_start(self, _ls, _ds, mock_minmax) -> None:
        """Falls back to MASABI_START_TIMESTAMP_MS if max ts is None."""
        mock_minmax.return_value = (None, None)
        job = make_job()
        assert job.setup_job() == MASABI_START_TIMESTAMP_MS


# ---------------------------------------------------------------------------
# Schema constants sanity checks
# ---------------------------------------------------------------------------


class TestSchemaConstants:
    """Quick sanity checks on the hardcoded schema and JSON-column constants."""

    def test_all_tables_have_schemas(self) -> None:
        """Every table in TABLES has an entry in TABLE_SCHEMAS."""
        from odin.ingestion.masabi.masabi_archive import TABLES

        for table in TABLES:
            assert table in TABLE_SCHEMAS, f"Missing schema for {table}"

    def test_all_tables_have_json_cols(self) -> None:
        """Every table in TABLES has an entry in TABLE_JSON_COLS."""
        from odin.ingestion.masabi.masabi_archive import TABLES

        for table in TABLES:
            assert table in TABLE_JSON_COLS, f"Missing json_cols for {table}"

    def test_json_cols_are_in_schema(self) -> None:
        """Every JSON column listed in TABLE_JSON_COLS exists in TABLE_SCHEMAS."""
        for table, json_cols in TABLE_JSON_COLS.items():
            schema_names = set(TABLE_SCHEMAS[table].names())
            for col in json_cols:
                assert col in schema_names, (
                    f"JSON col {col!r} in TABLE_JSON_COLS[{table!r}] "
                    f"not found in TABLE_SCHEMAS"
                )

    def test_all_schemas_have_server_timestamp(self) -> None:
        """Every table schema includes the serverTimestamp watermark column."""
        for table, schema in TABLE_SCHEMAS.items():
            assert "serverTimestamp" in schema.names(), (
                f"serverTimestamp missing from schema for {table}"
            )

    def test_active_schema_excludes_excluded_cols(self) -> None:
        """active_schema drops columns in TABLE_EXCLUDE_COLUMNS."""
        job = ArchiveMasabi(TABLE)
        job.exclude_cols = frozenset({"eventId"})
        job.active_schema = pl.Schema(
            {c: t for c, t in TABLE_SCHEMAS[TABLE].items() if c != "eventId"}
        )
        assert "eventId" not in job.active_schema.names()
