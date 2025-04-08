import os
import sched
import sys
from typing import List
from typing import Optional
from typing import Any

from odin.utils.logger import ProcessLog
from odin.utils.aws.ecs import running_in_aws


def validate_env_vars(
    aws: Optional[List[str]] = None,
) -> None:
    """
    Check that exepected environment variables are set before application starts.

    :param aws: ENV vars only needed when running on AWS
    """
    logger = ProcessLog("validate_env_vars")

    required_set = set()

    if aws and running_in_aws():
        required_set = required_set | set(aws)

    missing = []
    for key in required_set:
        value = os.environ.get(key, None)
        if value is None:
            missing.append(key)
        else:
            logger.add_metadata(**{key: value}, print_log=False)

    if missing:
        exception = RuntimeError(
            f"Expected environment variable(s) are not set {{{','.join(missing)}}}."
        )
        logger.failed(exception)
        raise exception

    logger.complete()


def thread_cpus() -> int:
    """
    Get the number of work threads to utilize.

    :return: number of threads to use
    """
    os_cpu_count = os.cpu_count()
    if os_cpu_count is None:
        return 8
    else:
        return os_cpu_count * 2


def handle_sigterm(_: int, __: Any) -> None:
    """Set ENV var when SIGTERM recieved."""
    log = ProcessLog("sigterm_received")
    os.environ["GOT_SIGTERM"] = "TRUE"
    log.complete()


def sigterm_check() -> None:
    """Check if SIGTERM recieved and if so exit program."""
    if os.environ.get("GOT_SIGTERM") is not None:
        ProcessLog("stopping_ecs")
        sys.exit(0)


def schedule_sigterm_check(schedule: sched.scheduler) -> None:
    """
    Schedule sigterm check to always be running between idle jobs.

    :param schedule: application scheduler
    """
    sig_check_delay_secs = 30
    sigterm_check()
    schedule.enter(sig_check_delay_secs, 1, schedule_sigterm_check, (schedule,))
