from abc import ABC
from abc import abstractmethod
from multiprocessing import get_context
from multiprocessing.sharedctypes import Synchronized
from typing import Dict
import tempfile
import sched


from odin.utils.logger import ProcessLog
from odin.utils.logger import MdValues
from odin.utils.runtime import sigterm_check
from odin.utils.runtime import delete_folder_contens
from odin.utils.aws.ecs import running_in_aws

NEXT_RUN_DEFAULT = 60 * 60 * 6  # 6 hours
NEXT_RUN_FAILED = 60 * 60 * 24  # 24 hours


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

    def start(self, return_val: "Synchronized[int]") -> None:
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
            return_val.value = run_delay_secs


def job_proc_schedule(job: OdinJob, schedule: sched.scheduler) -> None:
    """
    Odin Job Runner as Process.

    This function runs each OdinJob as it's own process so resource usage can be fully cleared
    between job runs.

    Running the job in a `Process` also allows the scheduler to be unaffected by jobs that are
    killed by the machine kernel because of something like an OOM error. The exitcode of each
    process is checked and all non 0 codes are logged as job failures.

    :param job: Job to be run and re-scheduled
    :param schedule: main application scheduler
    """
    return_manager = get_context("spawn").Manager()
    proc_return_val = return_manager.Value("i", NEXT_RUN_FAILED)
    proc = get_context("spawn").Process(target=job.start, args=(proc_return_val,))
    proc.start()
    proc.join()
    if proc.exitcode != 0:
        fail_log = ProcessLog(
            "odin_job_died",
            job_type=job.__class__.__name__,
            job_exit_code=proc.exitcode,
            **job.start_kwargs,
        )
        fail_log.failed(SystemError("OdinJob killed by ECS."))
        proc_return_val.value = NEXT_RUN_FAILED

    # always clear temp directory in AWS (in case of process getting killed)
    if running_in_aws():
        delete_folder_contens(tempfile.gettempdir())

    schedule.enter(proc_return_val.value, 1, job_proc_schedule, (job, schedule))
