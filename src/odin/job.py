from abc import ABC
from abc import abstractmethod
from typing import Dict
import tempfile
import sched

from odin.utils.logger import ProcessLog
from odin.utils.logger import MdValues
from odin.utils.runtime import sigterm_check


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

    def start(self, schedule: sched.scheduler) -> None:
        """Start Odin job with logging."""
        sigterm_check()
        self.reset_tmpdir()
        # default run delay of 6 hours
        run_delay_secs = 60 * 60 * 6
        process_name = self.__class__.__name__
        try:
            log = ProcessLog(
                process=process_name,
                **self.start_kwargs,
            )

            run_delay_secs = self.run()

            assert isinstance(run_delay_secs, int)

            log.complete(
                run_delay_mins=f"{run_delay_secs/60:.2f}",
                **self.start_kwargs,
            )

        except Exception as exception:
            # Don't run again for 12 hours on failure
            run_delay_secs = 60 * 60 * 12
            log.add_metadata(
                print_log=False,
                run_delay_mins=f"{run_delay_secs/60:.2f}",
                **self.start_kwargs,
            )
            log.failed(exception)

        finally:
            self.reset_tmpdir()
            schedule.enter(run_delay_secs, 1, self.start, (schedule,))
