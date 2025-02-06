import pytest
import sched
import time

from odin.job import OdinJob


def test_odin_job(caplog, monkeypatch) -> None:
    """Test OdinJob ABC."""

    # test Standard Operation
    class TestJob(OdinJob):
        start_kwargs = {"test": True, "kwargs": "found"}

        def run(self) -> int:
            self.start_kwargs["added"] = "this"
            return 1000

    scheduler = sched.scheduler(time.monotonic, time.sleep)
    test_job = TestJob()
    test_job.start(scheduler)
    assert len(caplog.messages) == 2
    assert "TestJob" in caplog.messages[0]
    assert "test=True" in caplog.messages[0]
    assert "kwargs=found" in caplog.messages[0]
    assert "added=this" in caplog.messages[-1]
    assert scheduler.empty() is False

    scheduler.cancel(scheduler.queue[0])
    assert scheduler.empty() is True
    caplog.clear()

    # test Job that raises
    class TestJobError(OdinJob):
        start_kwargs = {"test": True, "kwargs": "found"}

        def run(self) -> int:
            self.start_kwargs["added"] = "this"
            raise Exception("don't run")

    test_job = TestJobError()
    t_start = time.monotonic()
    test_job.start(scheduler)
    assert len(caplog.messages) > 2
    assert "status=failed" in caplog.messages[-1]
    assert "added=this" in caplog.messages[-1]
    assert scheduler.empty() is False
    # Job re-scheduled for 6 hours after failure
    assert "run_delay_mins=360.00" in caplog.messages[-1]
    assert scheduler.queue[0][0] > t_start + (60 * 60 * 6)
    assert scheduler.queue[0][0] < time.monotonic() + (60 * 60 * 6)

    scheduler.cancel(scheduler.queue[0])
    assert scheduler.empty() is True
    caplog.clear()

    # test Job interrupt by SIGTERM
    monkeypatch.setenv("GOT_SIGTERM", "TRUE")
    test_job = TestJob()
    with pytest.raises(SystemExit):
        test_job.start(scheduler)
    assert len(caplog.messages) == 1
    assert "process=stopping_ecs" in caplog.messages[0]
