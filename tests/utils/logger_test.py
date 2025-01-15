import logging

from odin.utils.logger import LOGGER_NAME
from odin.utils.logger import ProcessLog


def test_logger_no_auto_start(caplog):
    """Check auto_start=False"""
    caplog.set_level(level=logging.INFO, logger=LOGGER_NAME)

    _ = ProcessLog("no_auto_start", auto_start=False)
    assert caplog.record_tuples == []


def test_logger_auto_start(caplog):
    """Check auto_start=True (defalt)"""
    caplog.set_level(level=logging.INFO, logger=LOGGER_NAME)
    logger = ProcessLog("auto_start")
    log_contains = (
        "process=auto_start",
        "status=started",
        "uuid=",
        "parent=",
        "process_id=",
        "disk_mb_free=",
        "mem_pct_free=",
    )
    assert len(caplog.record_tuples) > 0
    for message in log_contains:
        assert message in caplog.record_tuples[0][2]

    log_contains = (
        "process=auto_start",
        "status=complete",
        "uuid=",
        "parent=",
        "process_id=",
        "disk_mb_free=",
        "mem_pct_free=",
        "duration=",
    )
    logger.complete()
    for message in log_contains:
        assert message in caplog.record_tuples[-1][2]


def test_add_metadata_print_log(caplog):
    """Check logger.add_metadata"""
    caplog.set_level(level=logging.INFO, logger=LOGGER_NAME)
    logger = ProcessLog("add_metadata")
    caplog.clear()
    logger.add_metadata(no_print=True, print_log=False)
    assert caplog.record_tuples == []

    logger.add_metadata(no_print=True, print_log=True)
    assert len(caplog.record_tuples) > 0
    assert "status=add_metadata" in caplog.record_tuples[0][2]
    assert "no_print=True" in caplog.record_tuples[0][2]
    assert "print_log" not in caplog.record_tuples[0][2]


def test_add_metadata_protected_keys(caplog):
    """Check that logger.protected_keys produce warnings"""
    caplog.set_level(level=logging.WARNING, logger=LOGGER_NAME)
    logger = ProcessLog("add_metadata")
    for key in logger.protected_keys:
        caplog.clear()
        kwargs = {
            key: True,
            "print_log": False,
        }
        logger.add_metadata(**kwargs)
        assert len(caplog.record_tuples) > 0
        assert f"'{key}' conflicts with protected ProcessLog key." in caplog.record_tuples[0][2]


def test_failed_logs(caplog):
    """Check exceptions property logged"""
    caplog.set_level(level=logging.INFO, logger=LOGGER_NAME)
    log = ProcessLog("failed_process")
    exception = Exception("test_log_failed")
    log.failed(exception)

    assert caplog.record_tuples[-1][1] == logging.INFO
    assert "status=failed" in caplog.record_tuples[-1][2]

    assert caplog.record_tuples[-2][1] == logging.ERROR
    assert "Exception: test_log_failed" in caplog.record_tuples[-2][2]
