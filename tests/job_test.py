import os
import pytest
import sched
import time
from multiprocessing import get_context

from odin.job import OdinJob
from odin.job import job_proc_schedule as real_job_proc_schedule
from odin.job import _monitor_proc_peak_usage


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
    manager = get_context("spawn").Manager()
    return_val = manager.Value("i", 0)
    tmpdir_val = manager.Value("u", "")
    job.start(return_val, tmpdir_val)
    schedule.enter(return_val.value, 1, job_proc_schedule, (job, schedule))


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
    assert "run_delay_mins=1440.00" in caplog.messages[-1]
    assert scheduler.queue[0][0] > t_start + (60 * 60 * 24)
    assert scheduler.queue[0][0] < time.monotonic() + (60 * 60 * 24)

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


# Defined at module level so the spawn context can pickle them
def _mem_and_disk_hog(tmpdir_val, spill_dir: str) -> None:
    """Allocate ~50MB and spill ~20MB to ``spill_dir`` so the parent can sample both."""
    tmpdir_val.value = spill_dir
    blob = bytearray(50 * 1024 * 1024)  # noqa: F841 -- keep ref so it stays resident
    with open(os.path.join(spill_dir, "spill.bin"), "wb") as spill_file:
        spill_file.write(b"\0" * 20 * 1024 * 1024)
        spill_file.flush()
        time.sleep(0.5)


class PeakMemJob(OdinJob):
    """Trivial job used to exercise peak-memory logging in the real runner."""

    start_kwargs = {"table": "retail.tickets"}

    def run(self) -> int:
        """Return a fixed re-run delay."""
        return 1000


def test_monitor_proc_peak_usage(tmp_path) -> None:
    """A running subprocess' peak resident memory and disk spill are captured."""
    manager = get_context("spawn").Manager()
    tmpdir_val = manager.Value("u", "")
    proc = get_context("spawn").Process(
        target=_mem_and_disk_hog, args=(tmpdir_val, str(tmp_path))
    )
    proc.start()
    peak_mem_mb, peak_spill_mb = _monitor_proc_peak_usage(proc, tmpdir_val, 0.05)

    # Process is fully reaped and we observed a non-trivial memory and disk footprint
    assert proc.exitcode == 0
    assert peak_mem_mb > 10.0
    assert peak_spill_mb > 10.0


def test_job_proc_schedule_logs_peak_mem(caplog) -> None:
    """The real runner logs peak memory with job type and start_kwargs."""
    scheduler = sched.scheduler(time.monotonic, time.sleep)
    real_job_proc_schedule(PeakMemJob(), scheduler)

    peak_logs = [m for m in caplog.messages if "process=odin_job_peak_mem" in m]
    assert len(peak_logs) > 0
    assert any("job_type=PeakMemJob" in m for m in peak_logs)
    assert any("table=retail.tickets" in m for m in peak_logs)
    assert any("peak_mem_mb=" in m for m in peak_logs)
    assert any("peak_spill_mb=" in m for m in peak_logs)

    scheduler.cancel(scheduler.queue[0])
