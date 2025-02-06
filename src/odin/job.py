from abc import ABC
from abc import abstractmethod
from typing import Dict
from typing import Any
import sched

from odin.utils.logger import ProcessLog
from odin.utils.runtime import sigterm_check


class OdinJob(ABC):
    """Base Class for Odin Event Loop Jobs."""

    @property
    @abstractmethod
    def start_kwargs(self) -> Dict[str, Any]:
        """Return dictonary of kwargs for start method."""

    @abstractmethod
    def run(self) -> int:
        """
        Run job business logic.

        :return: seconds to delay until next run of job
        """

    def start(self, schedule: sched.scheduler) -> None:
        """Start Odin job with logging."""
        sigterm_check()
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
            log.add_metadata(
                print_log=False,
                run_delay_mins=f"{run_delay_secs/60:.2f}",
                **self.start_kwargs,
            )
            log.failed(exception)

        finally:
            schedule.enter(run_delay_secs, 1, self.start, (schedule,))
