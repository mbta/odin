import pytest
from datetime import datetime
from unittest.mock import MagicMock
from unittest.mock import patch

from odin.utils.aws.s3 import S3Object
from odin.migrate.process import start_migrations
from odin.migrate.process import get_last_run_migration


@patch("odin.migrate.process.list_objects")
def test_get_last_run_migration(ls: MagicMock):
    """Test get_last_migration helper function"""
    # no existing migration status
    status_path = "aws_s3_bucket/odin/migrations/_odin-test"
    ls.return_value = []
    assert get_last_run_migration(status_path) is None

    # more than one object in status_path (bad)
    return_value = [
        S3Object(path="obj1", last_modified=datetime.now(), size_bytes=0),
        S3Object(path="obj2", last_modified=datetime.now(), size_bytes=0),
    ]
    ls.return_value = return_value
    with pytest.raises(AssertionError):
        get_last_run_migration(status_path)

    # normal return
    return_value = [
        S3Object(path=f"{status_path}/0004", last_modified=datetime.now(), size_bytes=0),
    ]
    ls.return_value = return_value
    assert get_last_run_migration(status_path) == "0004"


def test_migrate_not_in_aws(caplog):
    """Test no action if not in AWS."""
    start_migrations()
    assert len(caplog.messages) == 0


@patch("odin.migrate.process.list_objects")
def test_no_migrations_available(ls: MagicMock, caplog, monkeypatch):
    """Test no migrations available for ECS_TASK_GROUP."""
    ls.return_value = []
    monkeypatch.setenv("ECS_TASK_GROUP", "family:_odin_no_migration")
    start_migrations()
    assert len(caplog.messages) == 2
    assert "no_migrations_found=True" in caplog.messages[-1]
    assert "status=complete" in caplog.messages[-1]
    assert "process=start_migrations" in caplog.messages[-1]


@patch("odin.migrate.process.infinite_wait")
@patch("odin.migrate.process.list_objects")
def test_migration_failure(ls: MagicMock, infinite: MagicMock, caplog, monkeypatch):
    """Test migration failed creates infinte loop."""
    ls.return_value = []
    monkeypatch.setenv("ECS_TASK_GROUP", "family:_odin-test")
    task_name = "_odin-test"
    start_migrations()
    infinite.assert_called_once_with(f"Migration failed for {task_name=}.")
    assert "status=failed" in caplog.messages[-1]
    assert "AssertionError" in caplog.messages[-1]


@patch("odin.migrate.process.upload_migration_file")
@patch("odin.migrate.process.get_last_run_migration")
def test_migration_process(last_run: MagicMock, upload: MagicMock, caplog, monkeypatch):
    """Test migration process."""
    monkeypatch.setenv("ECS_TASK_GROUP", "family:_odin-test")

    last_run.return_value = "0001"
    start_migrations()
    last_run.assert_called_once()
    upload.assert_called_once()
    assert len(caplog.messages) == 6
    assert "ran_migration_0002" in caplog.messages[-3]
    caplog.clear()

    # Test all migrations run
    last_run.return_value = "0002"
    start_migrations()
    assert len(caplog.messages) == 4
    assert "status=complete" in caplog.messages[-1]
    assert "process=start_migrations" in caplog.messages[-1]
