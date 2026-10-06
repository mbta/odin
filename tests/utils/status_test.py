import json
from types import MappingProxyType
from unittest.mock import patch

from odin.utils.status import publish_status


def test_publish_status_uploads_non_dict_mapping(tmp_path):
    """
    Any Mapping the signature accepts must upload, nested fields intact.

    json only serializes dict and its subclasses, and publish_status swallows its own
    failures, so an unconverted MappingProxyType would skip the upload silently.
    """
    payload = MappingProxyType(
        {
            "table": "v_test",
            "max_column_timestamps": {"creadate": "2026-07-07T17:25:33"},
        }
    )
    uploaded: dict = {}

    def fake_upload(src, dst, **kwargs):
        with open(src) as status_file:
            uploaded["payload"] = json.load(status_file)

    with patch("odin.utils.status.upload_file", fake_upload):
        publish_status("odin/logs/afc", "v_test", str(tmp_path), payload)

    assert uploaded["payload"] == {
        "table": "v_test",
        "max_column_timestamps": {"creadate": "2026-07-07T17:25:33"},
    }
