import os
import pytest
from unittest.mock import MagicMock
from unittest.mock import patch

from odin.utils.runtime import validate_env_vars
from odin.utils.runtime import thread_cpus
from odin.utils.runtime import handle_sigterm
from odin.utils.runtime import sigterm_check


def test_validate_env_vars(caplog, monkeypatch) -> None:
    """Test validate_env_vars util."""
    # Should throw because "SERVICE_NAME" is not set
    with pytest.raises(RuntimeError):
        validate_env_vars(required=[])
    caplog.clear()

    monkeypatch.setenv("SERVICE_NAME", "odin")
    monkeypatch.setenv("PRIVATE", "hide_me")
    monkeypatch.setenv("REQUIRED", "see_me")
    monkeypatch.setenv("IN_AWS", "true")

    validate_env_vars(required=["REQUIRED"], private=["PRIVATE"], aws=["IN_AWS"])
    assert len(caplog.messages) == 2
    assert "hide_me" not in caplog.messages[-1]
    assert "IN_AWS" not in caplog.messages[-1]
    assert "see_me" in caplog.messages[-1]
    caplog.clear()

    with pytest.raises(RuntimeError):
        validate_env_vars(required=["REQUIRED"], private=["NOT_FOUND"])
    caplog.clear()

    with patch("odin.utils.runtime.running_in_aws") as in_aws:
        in_aws.return_value = True
        validate_env_vars(required=[], aws=["IN_AWS"])
        assert len(caplog.messages) == 2
        assert "IN_AWS=true" in caplog.messages[-1]
        caplog.clear()

        validate_env_vars(required=[], private=["IN_AWS"], aws=["IN_AWS"])
        assert len(caplog.messages) == 2
        assert "IN_AWS=" in caplog.messages[-1]
        assert "IN_AWS=true" not in caplog.messages[-1]


@patch("odin.utils.runtime.os.cpu_count")
def test_thread_cpus(cpu_count: MagicMock) -> None:
    """Test thread_cpus util."""
    cpu_count.return_value = 50
    assert thread_cpus() == 100

    cpu_count.return_value = None
    assert thread_cpus() == 8


def test_handle_sigterm() -> None:
    """Test handle_sigterm util."""
    assert os.getenv("GOT_SIGTERM") is None
    handle_sigterm(0, 0)
    assert os.getenv("GOT_SIGTERM") == "TRUE"

    # reset env var
    os.environ.pop("GOT_SIGTERM", None)
    assert os.getenv("GOT_SIGTERM") is None


def test_sigterm_check(caplog, monkeypatch) -> None:
    """Test sigterm_check util."""
    sigterm_check()
    assert len(caplog.messages) == 0

    monkeypatch.setenv("GOT_SIGTERM", "TRUE")
    with pytest.raises(SystemExit):
        sigterm_check()
    assert len(caplog.messages) == 1
    assert "process=stopping_ecs" in caplog.messages[0]
