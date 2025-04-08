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
    # Passes if no requirements
    validate_env_vars()
    caplog.clear()

    monkeypatch.setenv("IN_AWS", "true")

    # doesn't validate env var if not in AWS
    validate_env_vars(aws=["IN_AWS"])
    assert len(caplog.messages) == 2
    assert "IN_AWS" not in caplog.messages[-1]
    caplog.clear()

    # does validate env var if in AWS
    with patch("odin.utils.runtime.running_in_aws") as in_aws:
        in_aws.return_value = True
        validate_env_vars(aws=["IN_AWS"])
        assert len(caplog.messages) == 2
        assert "IN_AWS=true" in caplog.messages[-1]
        caplog.clear()


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
