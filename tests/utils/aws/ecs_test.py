import logging
import pytest
from unittest.mock import MagicMock
from unittest.mock import patch

from odin.utils.aws.ecs import running_in_aws
from odin.utils.aws.ecs import check_for_parallel_tasks


def test_running_in_aws(monkeypatch) -> None:
    """Test running_in_aws ecs utility."""
    assert running_in_aws() is False

    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east1")
    assert running_in_aws() is True


@patch("odin.utils.aws.ecs.running_in_aws")
@patch("odin.utils.aws.ecs.boto3.client")
def test_check_for_parallel_tasks(
    client: MagicMock, in_aws: MagicMock, caplog, monkeypatch
) -> None:
    """Test check_for_parralell_tasks ecs utility."""
    # test not running in AWS
    in_aws.return_value = False
    check_for_parallel_tasks()
    assert len(caplog.messages) == 0

    # test one task currently running is OK
    in_aws.return_value = True
    client.return_value.list_tasks.return_value = {"taskArns": [1]}
    client.return_value.describe_tasks.return_value = {"tasks": [{"group": "odin"}]}
    monkeypatch.setenv("ECS_CLUSTER", "odin")
    monkeypatch.setenv("ECS_TASK_GROUP", "odin")
    check_for_parallel_tasks()
    assert len(caplog.messages) == 2
    assert "status=complete" in caplog.messages[-1]
    caplog.clear()

    # test two tasks running is BAD
    client.return_value.describe_tasks.return_value = {
        "tasks": [
            {"group": "odin"},
            {"group": "odin"},
        ]
    }
    with pytest.raises(SystemError):
        check_for_parallel_tasks()
    assert len(caplog.messages) == 3
    assert caplog.record_tuples[1][1] == logging.ERROR
    assert "status=failed" in caplog.messages[-1]
