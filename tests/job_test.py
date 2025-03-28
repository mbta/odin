import pytest
import sched
import time

from odin.job import OdinJob


# caplog doesn't work in multiprocessing. this should be ok for now, but could be improved
# to make caplog work fully with the original function and multiprocessing
def job_proc_schedule(job: OdinJob, schedule: sched.scheduler) -> None:
    """
    Odin Job Runner as Process.

    This function runs each OdinJob as it's own process so resource usage can be fully cleared
    between job runs.

    :param job: Job to be run and re-scheduled
    :param schedule: main application scheduler
    """
    run_delay_secs = job.start()
    schedule.enter(run_delay_secs, 1, job_proc_schedule, (job, schedule))


def test_odin_job(caplog, monkeypatch) -> None:
    """Test OdinJob ABC."""
    # test Standard Operation
    scheduler = sched.scheduler(time.monotonic, time.sleep)

    class TestJob(OdinJob):
        start_kwargs = {"test": True, "kwargs": "found"}

        def run(self) -> int:
            self.start_kwargs["added"] = "this"
            return 1000

    test_job = TestJob()
    job_proc_schedule(test_job, scheduler)
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
    job_proc_schedule(test_job, scheduler)
    assert len(caplog.messages) > 2
    assert "status=failed" in caplog.messages[-1]
    assert "added=this" in caplog.messages[-1]
    assert scheduler.empty() is False
    # Job re-scheduled for 12 hours after failure
    assert "run_delay_mins=720.00" in caplog.messages[-1]
    assert scheduler.queue[0][0] > t_start + (60 * 60 * 12)
    assert scheduler.queue[0][0] < time.monotonic() + (60 * 60 * 12)

    scheduler.cancel(scheduler.queue[0])
    assert scheduler.empty() is True
    caplog.clear()

    # test Job interrupt by SIGTERM
    monkeypatch.setenv("GOT_SIGTERM", "TRUE")
    test_job = TestJob()
    with pytest.raises(SystemExit):
        job_proc_schedule(test_job, scheduler)
    assert len(caplog.messages) == 1
    assert "process=stopping_ecs" in caplog.messages[0]
