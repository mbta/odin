import json
import os
from unittest.mock import MagicMock
from unittest.mock import patch

import polars as pl
import pytest
import urllib3

import odin.ingestion.masabi.masabi_archive as masabi_archive
from odin.ingestion.masabi.masabi_archive import ArchiveMasabi
from odin.ingestion.masabi.masabi_archive import SchemaCheck
from odin.ingestion.masabi.masabi_archive import _fetch_schema_spec
from odin.ingestion.masabi.masabi_archive import _load_schemas
from odin.ingestion.masabi.masabi_archive import MASABI_START_TIMESTAMP_MS
from odin.ingestion.masabi.masabi_archive import MASABI_BACKFILL_START_TIMESTAMP_MS

_TABLE = "retail.account_actions"


def _make_schema_check(
    json_cols=frozenset(),
    numeric_overrides=frozenset(),
    exclude_cols=None,
) -> SchemaCheck:
    """Build a SchemaCheck over a small representative schema for unit tests."""
    schema = pl.Schema(
        {
            "serverTimestamp": pl.Float64(),
            "hourOfDay": pl.Float64(),
            "name": pl.String(),
            "flag": pl.Boolean(),
            "tags": pl.String(),  # array in YAML -> String
            "meta": pl.String(),  # object in YAML -> String
        }
    )
    return SchemaCheck(
        schema=schema,
        json_cols=json_cols,
        numeric_overrides=numeric_overrides,
        exclude_cols=exclude_cols if exclude_cols is not None else [],
    )


# ---------------------------------------------------------------------------
# SchemaCheck._arrow_type_name / reset_warnings
# ---------------------------------------------------------------------------


def test_arrow_type_name_mapping():
    """Polars dtypes map to comparable Arrow type-name strings, defaulting to string."""
    assert SchemaCheck._arrow_type_name(pl.String()) == "string"
    assert SchemaCheck._arrow_type_name(pl.Boolean()) == "bool"
    assert SchemaCheck._arrow_type_name(pl.Float64()) == "double"
    assert SchemaCheck._arrow_type_name(pl.Int64()) == "int64"
    assert SchemaCheck._arrow_type_name(pl.Int32()) == "int32"
    # Unknown dtypes fall back to "string".
    assert SchemaCheck._arrow_type_name(pl.Datetime()) == "string"


def test_reset_warnings_clears_state():
    """reset_warnings clears per-run dedup state so warnings re-fire next run."""
    sc = _make_schema_check()
    sc.warned_columns.add("extra_columns")
    sc.reset_warnings()
    assert sc.warned_columns == set()


# ---------------------------------------------------------------------------
# SchemaCheck.check_json_page
# ---------------------------------------------------------------------------


def test_check_json_page_empty_is_noop():
    """An empty page produces no warnings."""
    sc = _make_schema_check()
    sc.check_json_page([])
    assert sc.warned_columns == set()


def test_check_json_page_flags_extra_columns_once():
    """Columns present in JSON but absent from the schema warn once (deduped)."""
    sc = _make_schema_check()
    sc.check_json_page([{"serverTimestamp": 1.0, "surprise": "x"}])
    assert "extra_columns" in sc.warned_columns
    # A second page with a different extra column does not re-warn.
    sc.check_json_page([{"serverTimestamp": 2.0, "another": "y"}])
    assert sc.warned_columns == {"extra_columns"}


def test_check_json_page_flags_bad_json_col_type():
    """A JSON-typed column whose value is not list/dict warns under its own name."""
    sc = _make_schema_check(json_cols=frozenset({"tags", "meta"}))
    sc.check_json_page([{"serverTimestamp": 1.0, "tags": "not-a-list"}])
    assert "tags" in sc.warned_columns


def test_check_json_page_flags_bad_number_type():
    """A number-typed column arriving as a non-numeric warns."""
    sc = _make_schema_check()
    sc.check_json_page([{"serverTimestamp": "not-a-number"}])
    assert "serverTimestamp" in sc.warned_columns


def test_check_json_page_numeric_override_not_flagged():
    """A stringy value in a numeric-override column is expected, so not warned here."""
    sc = _make_schema_check(numeric_overrides=frozenset({"hourOfDay"}))
    sc.check_json_page([{"hourOfDay": "8"}])
    assert "hourOfDay" not in sc.warned_columns


def test_check_json_page_null_values_tolerated():
    """Null values are valid for any column type and never warn."""
    sc = _make_schema_check(json_cols=frozenset({"tags"}))
    sc.check_json_page([{"serverTimestamp": None, "tags": None, "name": None}])
    assert sc.warned_columns == set()


# ---------------------------------------------------------------------------
# SchemaCheck.serialize_json_cols
# ---------------------------------------------------------------------------


def test_serialize_json_cols_serializes_list_and_dict():
    """List/dict values in JSON columns are serialized to JSON strings in-place."""
    sc = _make_schema_check(json_cols=frozenset({"tags", "meta"}))
    hit = {"tags": ["a", "b"], "meta": {"k": 1}}
    sc.serialize_json_cols(hit)
    assert hit["tags"] == json.dumps(["a", "b"])
    assert hit["meta"] == json.dumps({"k": 1})


def test_serialize_json_cols_leaves_none_and_warns_on_scalar():
    """None passes through untouched; an unexpected scalar is left as-is but warned once."""
    sc = _make_schema_check(json_cols=frozenset({"tags", "meta"}))
    hit = {"tags": None, "meta": "already-a-string"}
    sc.serialize_json_cols(hit)
    assert hit["tags"] is None
    assert hit["meta"] == "already-a-string"
    assert "json_type_meta" in sc.warned_columns


# ---------------------------------------------------------------------------
# SchemaCheck.coerce_hit_numerics
# ---------------------------------------------------------------------------


def test_coerce_hit_numerics_string_to_float():
    """String-encoded numerics are coerced to float in-place."""
    sc = _make_schema_check(numeric_overrides=frozenset({"hourOfDay"}))
    hit = {"hourOfDay": "8"}
    sc.coerce_hit_numerics(hit)
    assert hit["hourOfDay"] == 8.0
    assert isinstance(hit["hourOfDay"], float)


def test_coerce_hit_numerics_passthrough_none_and_numbers():
    """None and already-numeric values are left untouched."""
    sc = _make_schema_check(numeric_overrides=frozenset({"hourOfDay"}))
    for value in (None, 5, 3.14):
        hit = {"hourOfDay": value}
        sc.coerce_hit_numerics(hit)
        assert hit["hourOfDay"] == value


def test_coerce_hit_numerics_uncoercible_nullfilled_and_warns():
    """Un-coercible values become None and warn once per column per run."""
    sc = _make_schema_check(numeric_overrides=frozenset({"hourOfDay"}))
    hit = {"hourOfDay": "not-a-number"}
    sc.coerce_hit_numerics(hit)
    assert hit["hourOfDay"] is None
    assert "coerce_hourOfDay" in sc.warned_columns


def test_process_page_coerces_and_serializes():
    """process_page runs coercion and JSON serialization across every hit in a page."""
    sc = _make_schema_check(
        json_cols=frozenset({"tags"}),
        numeric_overrides=frozenset({"hourOfDay"}),
    )
    page = [
        {"serverTimestamp": 1.0, "hourOfDay": "8", "tags": ["x"]},
        {"serverTimestamp": 2.0, "hourOfDay": 9, "tags": {"k": "v"}},
    ]
    sc.process_page(page)
    assert page[0]["hourOfDay"] == 8.0
    assert page[0]["tags"] == json.dumps(["x"])
    assert page[1]["hourOfDay"] == 9
    assert page[1]["tags"] == json.dumps({"k": "v"})


# ---------------------------------------------------------------------------
# SchemaCheck.check_parquet_schema
# ---------------------------------------------------------------------------


def _write_parquet(tmpdir, name, df: pl.DataFrame) -> str:
    path = os.path.join(str(tmpdir), name)
    df.write_parquet(path)
    return path


def test_check_parquet_schema_matching_ok(tmpdir):
    """A parquet whose schema matches the expected schema passes without raising."""
    sc = _make_schema_check()
    path = _write_parquet(
        tmpdir,
        "match.parquet",
        pl.DataFrame(
            {
                "serverTimestamp": [1.0],
                "hourOfDay": [8.0],
                "name": ["a"],
                "flag": [True],
                "tags": ["[]"],
                "meta": ["{}"],
            }
        ),
    )
    sc.check_parquet_schema(path)  # no raise


def test_check_parquet_schema_extra_and_missing_columns_ok(tmpdir):
    """Extra/missing columns are logged as warnings, not raised."""
    sc = _make_schema_check()
    # Missing "meta"/"tags"/etc, plus an unexpected extra column.
    path = _write_parquet(
        tmpdir,
        "extramissing.parquet",
        pl.DataFrame({"serverTimestamp": [1.0], "unexpected": ["x"]}),
    )
    sc.check_parquet_schema(path)  # no raise


def test_check_parquet_schema_excluded_extra_not_error(tmpdir):
    """A parquet column that is an excluded (PII) column is not treated as unexpected."""
    sc = _make_schema_check(exclude_cols=["email"])
    path = _write_parquet(
        tmpdir,
        "excluded.parquet",
        pl.DataFrame({"serverTimestamp": [1.0], "email": ["a@b.c"]}),
    )
    sc.check_parquet_schema(path)  # no raise


def test_check_parquet_schema_type_mismatch_raises(tmpdir):
    """A shared column with an incompatible type raises SchemaError."""
    sc = _make_schema_check()
    # serverTimestamp is Float64 in the schema but written here as a string.
    path = _write_parquet(
        tmpdir,
        "mismatch.parquet",
        pl.DataFrame({"serverTimestamp": ["not-a-float"], "name": ["a"]}),
    )
    with pytest.raises(pl.exceptions.SchemaError):
        sc.check_parquet_schema(path)


def test_check_parquet_schema_string_variants_ok(tmpdir):
    """large_string in parquet is accepted for a schema String column."""
    sc = _make_schema_check()
    df = pl.DataFrame({"serverTimestamp": [1.0], "name": ["a"]})
    path = _write_parquet(tmpdir, "strvar.parquet", df)
    # polars writes String columns as arrow large_string; expected type is "string".
    sc.check_parquet_schema(path)  # no raise


# ---------------------------------------------------------------------------
# _fetch_schema_spec / _load_schemas
# ---------------------------------------------------------------------------


def test_fetch_schema_spec_parses_yaml_on_200():
    """A 200 response body is parsed as YAML and returned."""
    pool = MagicMock()
    pool.request.return_value = MagicMock(status=200, data=b"components:\n  schemas: {}\n")
    spec = _fetch_schema_spec(pool)
    assert spec == {"components": {"schemas": {}}}


def test_fetch_schema_spec_non_200_raises():
    """A non-200 response raises RuntimeError."""
    pool = MagicMock()
    pool.request.return_value = MagicMock(status=503, data=b"down")
    with pytest.raises(RuntimeError):
        _fetch_schema_spec(pool)


def test_load_schemas_builds_types_and_json_cols():
    """Schemas and JSON-column sets are derived from the spec's property types."""
    spec = {
        "components": {
            "schemas": {
                "retail.foo": {
                    "properties": {
                        "serverTimestamp": {"type": "number"},
                        "name": {"type": "string"},
                        "flag": {"type": "boolean"},
                        "tags": {"type": "array"},
                        "meta": {"type": "object"},
                        "untyped": {},  # defaults to string
                    }
                }
            }
        }
    }
    with patch.object(masabi_archive, "_fetch_schema_spec", return_value=spec):
        schemas, json_cols = _load_schemas(["retail.foo"], MagicMock())

    assert schemas["retail.foo"] == pl.Schema(
        {
            "serverTimestamp": pl.Float64(),
            "name": pl.String(),
            "flag": pl.Boolean(),
            "tags": pl.String(),
            "meta": pl.String(),
            "untyped": pl.String(),
        }
    )
    assert json_cols["retail.foo"] == frozenset({"tags", "meta"})


def test_load_schemas_missing_table_raises_keyerror():
    """A requested table absent from the spec raises KeyError."""
    spec = {"components": {"schemas": {}}}
    with patch.object(masabi_archive, "_fetch_schema_spec", return_value=spec):
        with pytest.raises(KeyError):
            _load_schemas(["retail.missing"], MagicMock())


# ---------------------------------------------------------------------------
# ArchiveMasabi.__init__ path/status-key routing
# ---------------------------------------------------------------------------


def test_init_default_paths():
    """A default (alpha) job targets MASABI_DATA and keys status by table name."""
    with patch.object(masabi_archive, "_ODIN_INSTANCE", "alpha"):
        job = ArchiveMasabi(_TABLE)
    assert job.status_key == _TABLE
    assert job.export_folder.startswith("s3://")
    assert masabi_archive.MASABI_DATA in job.export_folder
    assert job.export_folder.endswith(f"{_TABLE}/")
    assert job.restricted is False


def test_init_restricted_paths():
    """A restricted job targets MASABI_RESTRICTED with a distinct status key."""
    with patch.object(masabi_archive, "_ODIN_INSTANCE", "alpha"):
        job = ArchiveMasabi(_TABLE, restricted=True)
    assert job.status_key == f"{_TABLE}__restricted"
    assert masabi_archive.MASABI_RESTRICTED in job.export_folder
    assert job.restricted is True


def test_init_gamma_backfill_paths():
    """On the gamma instance a job targets the backfill prefix and backfill status key."""
    with patch.object(masabi_archive, "_ODIN_INSTANCE", "gamma"):
        job = ArchiveMasabi(_TABLE)
    assert job.status_key == f"{_TABLE}__backfill"
    assert masabi_archive.MASABI_BACKFILL in job.export_folder


# ---------------------------------------------------------------------------
# ArchiveMasabi._make_request retry / throttle
# ---------------------------------------------------------------------------


@patch("odin.ingestion.masabi.masabi_archive.time.sleep")
@patch("odin.ingestion.masabi.masabi_archive.time.monotonic", return_value=0.0)
def test_make_request_success_first_try(_mono, _sleep):
    """A 200 response is returned directly."""
    job = ArchiveMasabi(_TABLE)
    pool = MagicMock()
    ok = MagicMock(status=200)
    pool.request.return_value = ok
    assert job._make_request(pool, "http://x", []) is ok
    pool.request.assert_called_once()


@patch("odin.ingestion.masabi.masabi_archive.time.sleep")
@patch("odin.ingestion.masabi.masabi_archive.time.monotonic", return_value=0.0)
def test_make_request_retries_then_succeeds(_mono, sleep):
    """A non-200 response is retried and a subsequent 200 is returned."""
    job = ArchiveMasabi(_TABLE)
    pool = MagicMock()
    pool.request.side_effect = [
        MagicMock(status=500, data=b"boom"),
        MagicMock(status=200),
    ]
    r = job._make_request(pool, "http://x", [])
    assert r.status == 200
    assert pool.request.call_count == 2
    sleep.assert_any_call(masabi_archive.API_RETRY_DELAY_S)


@patch("odin.ingestion.masabi.masabi_archive.time.sleep")
@patch("odin.ingestion.masabi.masabi_archive.time.monotonic", return_value=0.0)
def test_make_request_raises_after_exhausting_retries(_mono, _sleep):
    """All attempts failing raises the last HTTPError."""
    job = ArchiveMasabi(_TABLE)
    pool = MagicMock()
    pool.request.return_value = MagicMock(status=500, data=b"boom")
    with pytest.raises(urllib3.exceptions.HTTPError):
        job._make_request(pool, "http://x", [])
    assert pool.request.call_count == masabi_archive.API_MAX_RETRIES + 1


# ---------------------------------------------------------------------------
# ArchiveMasabi.api_pages pagination
# ---------------------------------------------------------------------------


def test_api_pages_unwraps_doc_and_paginates():
    """api_pages unwraps the `doc` envelope and follows nextPageId until exhausted."""
    job = ArchiveMasabi(_TABLE)
    job.schema_check = _make_schema_check()

    page1 = MagicMock()
    page1.json.return_value = {
        "hits": [{"doc": {"serverTimestamp": 1.0}}, {"doc": {"serverTimestamp": 2.0}}],
        "nextPageId": "cursor-1",
    }
    page2 = MagicMock()
    page2.json.return_value = {"hits": [{"doc": {"serverTimestamp": 3.0}}]}

    with patch.object(job, "_make_request", side_effect=[page1, page2]) as req:
        pages = list(job.api_pages(MagicMock(), from_ts=0, to_ts=100, ts_key="serverTimestamp"))

    assert pages == [
        [{"serverTimestamp": 1.0}, {"serverTimestamp": 2.0}],
        [{"serverTimestamp": 3.0}],
    ]
    # Second request must carry the nextPageId cursor from the first response.
    second_fields = req.call_args_list[1].args[2]
    assert ("nextPageId", "cursor-1") in second_fields


def test_api_pages_stops_on_empty_page():
    """An empty hit list ends pagination even if nextPageId is present."""
    job = ArchiveMasabi(_TABLE)
    job.schema_check = _make_schema_check()

    empty = MagicMock()
    empty.json.return_value = {"hits": [], "nextPageId": "cursor-1"}

    with patch.object(job, "_make_request", return_value=empty) as req:
        pages = list(job.api_pages(MagicMock(), 0, 100, "serverTimestamp"))

    assert pages == [[]]
    req.assert_called_once()


def test_api_pages_includes_exclude_columns_in_request():
    """Excluded (PII) columns are passed to the API as `exclude` query params."""
    job = ArchiveMasabi(_TABLE)
    job.schema_check = _make_schema_check(exclude_cols=["email", "phoneNumber"])

    resp = MagicMock()
    resp.json.return_value = {"hits": []}

    with patch.object(job, "_make_request", return_value=resp) as req:
        list(job.api_pages(MagicMock(), 0, 100, "serverTimestamp"))

    fields = req.call_args_list[0].args[2]
    assert ("exclude", "email") in fields
    assert ("exclude", "phoneNumber") in fields


# ---------------------------------------------------------------------------
# ArchiveMasabi.fetch_and_write
# ---------------------------------------------------------------------------


def test_fetch_and_write_writes_ndjson(tmpdir):
    """All returned hits are written as NDJSON and the path is returned."""
    job = ArchiveMasabi(_TABLE)
    job.tmpdir = str(tmpdir)
    job.schema_check = _make_schema_check()

    pages = [
        [{"serverTimestamp": 1.0, "name": "a"}, {"serverTimestamp": 2.0, "name": "b"}],
        [{"serverTimestamp": 3.0, "name": "c"}],
    ]
    with patch.object(job, "api_pages", return_value=iter(pages)):
        path, hit_limit = job.fetch_and_write(MagicMock(), 0, 100, "serverTimestamp")

    assert hit_limit is False
    assert path is not None
    rows = [json.loads(line) for line in open(path)]
    assert [r["serverTimestamp"] for r in rows] == [1.0, 2.0, 3.0]


def test_fetch_and_write_no_rows_returns_none(tmpdir):
    """When the API returns nothing, no file path is returned."""
    job = ArchiveMasabi(_TABLE)
    job.tmpdir = str(tmpdir)
    job.schema_check = _make_schema_check()

    with patch.object(job, "api_pages", return_value=iter([[]])):
        path, hit_limit = job.fetch_and_write(MagicMock(), 0, 100, "serverTimestamp")

    assert path is None
    assert hit_limit is False


@patch("odin.ingestion.masabi.masabi_archive.MAXIMUM_ROWS_PER_RUN", 2)
def test_fetch_and_write_honors_row_limit(tmpdir):
    """Fetching stops at MAXIMUM_ROWS_PER_RUN and flags that the limit was hit."""
    job = ArchiveMasabi(_TABLE)
    job.tmpdir = str(tmpdir)
    job.schema_check = _make_schema_check()

    pages = [
        [
            {"serverTimestamp": 1.0},
            {"serverTimestamp": 2.0},
            {"serverTimestamp": 3.0},
        ]
    ]
    with patch.object(job, "api_pages", return_value=iter(pages)):
        path, hit_limit = job.fetch_and_write(MagicMock(), 0, 100, "serverTimestamp")

    assert hit_limit is True
    rows = [json.loads(line) for line in open(path)]
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# ArchiveMasabi.setup_job watermark resume
# ---------------------------------------------------------------------------


@patch("odin.ingestion.masabi.masabi_archive.ds_metadata_min_max")
@patch("odin.ingestion.masabi.masabi_archive.ds_from_path")
def test_setup_job_no_existing_uses_start_constant(ds_from_path, ds_min_max):
    """With no existing parquet, the run starts from the configured start constant."""
    with patch.object(masabi_archive, "_ODIN_INSTANCE", "alpha"):
        job = ArchiveMasabi(_TABLE)
        ds_from_path.return_value = MagicMock(count_rows=MagicMock(return_value=0))
        from_ts = job.setup_job("serverTimestamp")

    assert from_ts == MASABI_START_TIMESTAMP_MS
    assert job.existing_rows == 0
    ds_min_max.assert_not_called()


@patch("odin.ingestion.masabi.masabi_archive.ds_metadata_min_max")
@patch("odin.ingestion.masabi.masabi_archive.ds_from_path")
def test_setup_job_resumes_from_existing_max(ds_from_path, ds_min_max):
    """With existing parquet, the run resumes from the max observed timestamp."""
    with patch.object(masabi_archive, "_ODIN_INSTANCE", "alpha"):
        job = ArchiveMasabi(_TABLE)
        ds_from_path.return_value = MagicMock(count_rows=MagicMock(return_value=5))
        ds_min_max.return_value = (100, 5000)
        from_ts = job.setup_job("serverTimestamp")

    assert from_ts == 5000
    assert job.existing_rows == 5
    assert job.existing_max_ts == 5000


@patch("odin.ingestion.masabi.masabi_archive.ds_metadata_min_max")
@patch("odin.ingestion.masabi.masabi_archive.ds_from_path")
def test_setup_job_gamma_uses_backfill_start(ds_from_path, ds_min_max):
    """On gamma with no existing data, the run starts from the backfill constant."""
    with patch.object(masabi_archive, "_ODIN_INSTANCE", "gamma"):
        job = ArchiveMasabi(_TABLE)
        ds_from_path.return_value = MagicMock(count_rows=MagicMock(return_value=0))
        from_ts = job.setup_job("serverTimestamp")

    assert from_ts == MASABI_BACKFILL_START_TIMESTAMP_MS


# ---------------------------------------------------------------------------
# ArchiveMasabi.sync_parquet boundary-timestamp drop
# ---------------------------------------------------------------------------


@patch("odin.ingestion.masabi.masabi_archive.sigterm_check")
@patch("odin.ingestion.masabi.masabi_archive.upload_file")
@patch("odin.ingestion.masabi.masabi_archive.download_object")
@patch("odin.ingestion.masabi.masabi_archive.list_objects")
def test_sync_parquet_drops_boundary_timestamp(
    list_objects, download_object, upload_file, sigterm_check, tmpdir
):
    """
    Drop rows at the maximum timestamp before writing.

    The next run's exclusive lower bound then re-fetches them (and any late
    arrivals at that same timestamp) rather than silently skipping them.
    """
    list_objects.return_value = []  # no existing S3 files
    job = ArchiveMasabi(_TABLE)
    job.tmpdir = str(tmpdir)
    job.export_folder = os.path.join(str(tmpdir), "export") + "/"
    job.schema_check = _make_schema_check()

    # Rows at timestamps 1, 2, 3, 3 -> boundary ts 3 rows must be dropped.
    ndjson_path = os.path.join(str(tmpdir), "retail_account_actions.ndjson")
    with open(ndjson_path, "w") as f:
        for ts in (1.0, 2.0, 3.0, 3.0):
            f.write(json.dumps({"serverTimestamp": ts, "name": "x"}) + "\n")

    job.sync_parquet(ndjson_path, "serverTimestamp")

    # Inspect the locally-written parquet before it would have been uploaded.
    written = [
        os.path.join(root, name)
        for root, _dirs, files in os.walk(job.export_folder)
        for name in files
        if name.endswith(".parquet")
    ]
    assert written, "expected a parquet file to be written"
    df = pl.read_parquet(written)
    assert sorted(df["serverTimestamp"].to_list()) == [1.0, 2.0]
    upload_file.assert_called()
    sigterm_check.assert_called_once()


@patch("odin.ingestion.masabi.masabi_archive.sigterm_check")
@patch("odin.ingestion.masabi.masabi_archive.upload_file")
@patch("odin.ingestion.masabi.masabi_archive.download_object")
@patch("odin.ingestion.masabi.masabi_archive.list_objects")
def test_sync_parquet_validates_existing_schema(
    list_objects, download_object, upload_file, sigterm_check, tmpdir
):
    """An existing S3 file is downloaded and schema-checked before merging."""
    existing = MagicMock()
    existing.path = "s3://bucket/odin/data/masabi/api/retail.account_actions/table_001.parquet"
    list_objects.return_value = [existing]

    job = ArchiveMasabi(_TABLE)
    job.tmpdir = str(tmpdir)
    job.export_folder = os.path.join(str(tmpdir), "export") + "/"
    job.schema_check = _make_schema_check()

    # Make the "downloaded" existing file a real, schema-compatible parquet.
    def _fake_download(src, dst):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        pl.DataFrame({"serverTimestamp": [0.5], "name": ["old"]}).write_parquet(dst)
        return True

    download_object.side_effect = _fake_download

    ndjson_path = os.path.join(str(tmpdir), "retail_account_actions.ndjson")
    with open(ndjson_path, "w") as f:
        for ts in (1.0, 2.0, 3.0):
            f.write(json.dumps({"serverTimestamp": ts, "name": "x"}) + "\n")

    with patch.object(
        job.schema_check, "check_parquet_schema", wraps=job.schema_check.check_parquet_schema
    ) as check:
        job.sync_parquet(ndjson_path, "serverTimestamp")

    download_object.assert_called_once()
    check.assert_called_once()
    upload_file.assert_called()


# ---------------------------------------------------------------------------
# ArchiveMasabi.run orchestration
# ---------------------------------------------------------------------------


def _run_with_mocks(job, ndjson_path, hit_row_limit, instance="alpha"):
    """Drive ArchiveMasabi.run with all IO boundaries mocked out."""
    schema = pl.Schema(
        {
            "serverTimestamp": pl.Float64(),
            "email": pl.String(),
            "ipAddress": pl.String(),
            "proofId": pl.String(),
            "name": pl.String(),
        }
    )
    with (
        patch.object(masabi_archive, "_ODIN_INSTANCE", instance),
        patch.object(masabi_archive, "TABLE_SCHEMAS", {job.table: schema}),
        patch.object(masabi_archive, "TABLE_JSON_COLS", {job.table: frozenset()}),
        patch.object(job, "_make_request_pool", return_value=MagicMock()),
        patch.object(job, "setup_job", return_value=0),
        patch.object(job, "fetch_and_write", return_value=(ndjson_path, hit_row_limit)),
        patch.object(job, "sync_parquet") as sync,
        patch.object(job, "_write_status") as write_status,
    ):
        result = job.run()
    return result, sync, write_status


def test_run_immediate_reschedule_on_row_limit():
    """Hitting the row limit reschedules immediately and still syncs the fetched data."""
    job = ArchiveMasabi("retail.rider_entitlement_events")
    result, sync, write_status = _run_with_mocks(job, "/tmp/x.ndjson", hit_row_limit=True)
    assert result == masabi_archive.NEXT_RUN_IMMEDIATE
    sync.assert_called_once()
    write_status.assert_called_once()


def test_run_default_reschedule_when_caught_up():
    """A caught-up run on alpha reschedules on the default (long) interval."""
    job = ArchiveMasabi("retail.rider_entitlement_events")
    result, sync, _ = _run_with_mocks(job, "/tmp/x.ndjson", hit_row_limit=False)
    assert result == masabi_archive.NEXT_RUN_DEFAULT
    sync.assert_called_once()


def test_run_skips_sync_when_no_data():
    """When fetch_and_write returns no file, sync_parquet is not invoked."""
    job = ArchiveMasabi("retail.rider_entitlement_events")
    result, sync, write_status = _run_with_mocks(job, None, hit_row_limit=False)
    assert result == masabi_archive.NEXT_RUN_DEFAULT
    sync.assert_not_called()
    write_status.assert_called_once()
    # wrote_data must reflect that nothing was written.
    assert write_status.call_args.kwargs["wrote_data"] is False


def test_run_excludes_pii_columns_from_schema():
    """Run drops configured PII columns from the schema used for the run."""
    job = ArchiveMasabi("retail.rider_entitlement_events")
    _run_with_mocks(job, None, hit_row_limit=False)
    names = job.schema_check.schema.names()
    # rider_entitlement_events drops ipAddress and proofId (non-restricted).
    assert "ipAddress" not in names
    assert "proofId" not in names
    assert "serverTimestamp" in names


def test_run_restricted_keeps_allowed_pii_column():
    """A restricted run retains PII columns explicitly allowed for the restricted export."""
    job = ArchiveMasabi("retail.rider_entitlement_events", restricted=True)
    _run_with_mocks(job, None, hit_row_limit=False)
    names = job.schema_check.schema.names()
    # proofId is allowed in the restricted export; ipAddress is still dropped.
    assert "proofId" in names
    assert "ipAddress" not in names
