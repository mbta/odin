from abc import ABC
from abc import abstractmethod
from multiprocessing import get_context
from typing import Dict
import tempfile
import sched


from odin.utils.logger import ProcessLog
from odin.utils.logger import MdValues
from odin.utils.runtime import sigterm_check

NEXT_RUN_DEFAULT = 60 * 60 * 6  # 6 hours
NEXT_RUN_FAILED = 60 * 60 * 12  # 12 hours


class OdinJob(ABC):
    """Base Class for Odin Event Loop Jobs."""

    start_kwargs: Dict[str, MdValues] = {}

    def reset_tmpdir(self) -> None:
        """Reset TemporaryDirectory folder."""
        if hasattr(self, "_tdir"):
            self._tdir.cleanup()  # type: ignore
        self._tdir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.tmpdir = self._tdir.name

    @abstractmethod
    def run(self) -> int:
        """
        Run job business logic.

        :return: seconds to delay until next run of job
        """

    def start(self) -> int:
        """Start Odin job with logging."""
        sigterm_check()
        self.reset_tmpdir()
        run_delay_secs = NEXT_RUN_DEFAULT
        try:
            log = ProcessLog(
                process=self.__class__.__name__,
                **self.start_kwargs,
            )

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

        return run_delay_secs


def job_proc_schedule(job: OdinJob, schedule: sched.scheduler) -> None:
    """
    Odin Job Runner as Process.

    This function runs each OdinJob as it's own process so resource usage can be fully cleared
    between job runs.

    :param job: Job to be run and re-scheduled
    :param schedule: main application scheduler
    """
    with get_context("spawn").Pool(1) as pool:
        run_delay_secs = pool.apply(job.start)
    schedule.enter(run_delay_secs, 1, job_proc_schedule, (job, schedule))
