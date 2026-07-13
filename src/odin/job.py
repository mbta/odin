from abc import ABC
from abc import abstractmethod
from multiprocessing import get_context
from multiprocessing.process import BaseProcess
from multiprocessing.sharedctypes import Synchronized
from typing import Dict
import glob
import os
import shutil
import tempfile
import sched

import psutil

from odin.utils.logger import ProcessLog
from odin.utils.logger import MdValues
from odin.utils.runtime import sigterm_check

NEXT_RUN_DEFAULT = 60 * 60 * 6  # 6 hours
NEXT_RUN_FAILED = 60 * 60 * 24  # 24 hours

# How often the parent samples a running job subprocess' memory, in seconds
MEM_POLL_INTERVAL_SECS = 1.0

# Prefix for Odin temp directories to avoid conflicts with other processes
ODINJOB_TMPDIR_PREFIX = "odinjob_tmp_"


def cleanup_orphaned_temp_dirs() -> None:
    """
    Clean up orphaned Odin temporary directories from previous failed runs

    When a subprocess is killed (e.g. through OOM), the finally block that normally
    cleans up TemporaryDirectory never runs. This function cleans up any leftover
    tmp directories matching the OdinJob prefix
    """
    tmp_base = tempfile.gettempdir()
    orphaned_dirs = glob.glob(os.path.join(tmp_base, f"{ODINJOB_TMPDIR_PREFIX}*"))
    orphaned_dirs = [p for p in orphaned_dirs if os.path.isdir(p)]

    if not orphaned_dirs:
        return

    log = ProcessLog(
        "cleanup_orphaned_temp_dirs",
        orphaned_count=len(orphaned_dirs),
        tmp_base=tmp_base,
    )
    cleaned_count = 0
    for tmp_path in orphaned_dirs:
        try:
            shutil.rmtree(tmp_path, ignore_errors=True)
            if not os.path.exists(tmp_path):
                cleaned_count += 1
        except Exception:
            pass

    log.complete(cleaned_count=cleaned_count)


class OdinJob(ABC):
    """Base Class for Odin Event Loop Jobs."""

    start_kwargs: Dict[str, MdValues] = {}

    def reset_tmpdir(self) -> None:
        """Reset TemporaryDirectory folder."""
        if hasattr(self, "_tdir"):
            self._tdir.cleanup()  # type: ignore
        self._tdir = tempfile.TemporaryDirectory(
            prefix=ODINJOB_TMPDIR_PREFIX,
            ignore_cleanup_errors=True,
        )
        self.tmpdir = self._tdir.name

    @abstractmethod
    def run(self) -> int:
        """
        Run job business logic.

        :return: seconds to delay until next run of job
        """

    def start(self, return_val: "Synchronized[int]") -> None:
        """Start Odin job with logging."""
        sigterm_check()
        self.reset_tmpdir()
        run_delay_secs = NEXT_RUN_DEFAULT
        log = ProcessLog(
            process=self.__class__.__name__,
            auto_start=True,
            **self.start_kwargs,
        )
        try:
            run_delay_secs = self.run()

            assert isinstance(run_delay_secs, int)

            log.complete(
                run_delay_mins=f"{run_delay_secs / 60:.2f}",
                **self.start_kwargs,
            )

        except Exception as exception:
            run_delay_secs = NEXT_RUN_FAILED
            log.add_metadata(
                print_log=False,
                run_delay_mins=f"{run_delay_secs / 60:.2f}",
                **self.start_kwargs,
            )
            log.failed(exception)

        finally:
            self.reset_tmpdir()
            return_val.value = run_delay_secs


def _proc_tree_rss_mb(proc: psutil.Process) -> float:
    """Resident set size of ``proc`` and all of its children, in MB."""
    rss = proc.memory_info().rss
    for child in proc.children(recursive=True):
        try:
            rss += child.memory_info().rss
        except psutil.NoSuchProcess:
            # A child proc died mid-sample
            continue
    return rss / (1024 * 1024)


def _monitor_proc_peak_rss_mb(proc: BaseProcess, poll_interval_secs: float) -> float:
    """
    Track the peak resident memory of a running job Process (and its children).

    Sampling is done from the parent so the final reading survives an OOM kill of the
    child. Blocks until ``proc`` exits, then returns the high-water mark in MB.

    :param proc: the started job subprocess to monitor
    :param poll_interval_secs: seconds between memory samples

    :return: peak resident memory observed, in MB
    """
    peak_mem_mb = 0.0
    try:
        mon = psutil.Process(proc.pid)
    except psutil.NoSuchProcess:
        # Job finished before we could attach; nothing to sample
        proc.join()
        return peak_mem_mb

    while True:
        try:
            peak_mem_mb = max(peak_mem_mb, _proc_tree_rss_mb(mon))
        except psutil.NoSuchProcess:
            break
        if not proc.is_alive():
            break
        proc.join(timeout=poll_interval_secs)

    proc.join()
    return peak_mem_mb


def job_proc_schedule(job: OdinJob, schedule: sched.scheduler | None) -> None:
    """
    Odin Job Runner as Process.

    This function runs each OdinJob as it's own process so resource usage can be fully cleared
    between job runs.

    Running the job in a `Process` also allows the scheduler to be unaffected by jobs that are
    killed by the machine kernel because of something like an OOM error. The exitcode of each
    process is checked and all non 0 codes are logged as job failures.

    Jobs can be run on an ad-hoc basis, for development purposes by calling this function with
    `schedule=None`.

    :param job: Job to be run and re-scheduled
    :param schedule: main application scheduler (or None to run Job once)
    """
    # Clean up any orphaned temp directories from previous killed processes
    cleanup_orphaned_temp_dirs()

    return_manager = get_context("spawn").Manager()
    proc_return_val = return_manager.Value("i", NEXT_RUN_FAILED)
    proc = get_context("spawn").Process(target=job.start, args=(proc_return_val,))
    proc.start()

    # Track the job subprocess' peak memory from the parent so OOM-prone jobs can be
    # identified by name and arguments (e.g. which table's update job is OOMing). This
    # blocks until the job finishes, replacing a plain proc.join().
    peak_mem_mb = _monitor_proc_peak_rss_mb(proc, MEM_POLL_INTERVAL_SECS)

    ProcessLog(
        "odin_job_peak_mem",
        auto_start=False,
        job_type=job.__class__.__name__,
        peak_mem_mb=f"{peak_mem_mb:.2f}",
        **job.start_kwargs,
    ).complete()

    if proc.exitcode != 0:
        fail_log = ProcessLog(
            "odin_job_died",
            job_type=job.__class__.__name__,
            job_exit_code=proc.exitcode,
            peak_mem_mb=f"{peak_mem_mb:.2f}",
            **job.start_kwargs,
        )
        fail_log.failed(SystemError("OdinJob killed by ECS."))
        proc_return_val.value = NEXT_RUN_FAILED

    if schedule is not None:
        schedule.enter(proc_return_val.value, 1, job_proc_schedule, (job, schedule))
