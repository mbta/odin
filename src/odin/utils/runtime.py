import os
import sched
import shutil
import sys
import time
from typing import List
from typing import Optional
from typing import Any

from odin.utils.logger import ProcessLog
from odin.utils.aws.ecs import running_in_aws


def validate_env_vars(
    required: List[str],
    private: Optional[List[str]] = None,
    aws: Optional[List[str]] = None,
) -> None:
    """
    Check that exepected environment variables are set before application starts.

    :param required: ENV vars needed for application runtime
    :param private: required ENV vars that will not be logged
    :param aws: ENV vars only needed when running on AWS
    """
    logger = ProcessLog("validate_env_vars")

    if private is None:
        private = []

    required_set = set(required) | set(private)

    if aws and running_in_aws():
        required_set = required_set | set(aws)

    missing = []
    for key in required_set:
        value = os.environ.get(key, None)
        if value is None:
            missing.append(key)
        elif key in private:
            logger.add_metadata(**{key: "**********"}, print_log=False)
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


def disk_free_pct() -> float:
    """
    Get current % of free disk space for root partition.

    :return: % of free disk space as float 0.0 to 100.0
    """
    total_disk_bytes, _, free_disk_bytes = shutil.disk_usage("/")
    return free_disk_bytes / total_disk_bytes * 100


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


def infinite_wait(reason: str) -> None:
    """
    Put ECS into infinte wait if no processing should occur because of possible tainted state.

    When running on ECS, propagating an exception up the call stack and killing
    the processes will result in the process being restarted, to keep the task
    count at one. This method should be called instead when we want to pause
    the process for intervention before restarting.
    """
    # amount of time to sleep between logging statements
    sleep_time = 30
    count = 0

    while True:
        sigterm_check()

        # log every 5 minutes
        if count == 10:
            ProcessLog(infinite_wait, reason=reason)
            count = 0

        # sleep
        time.sleep(sleep_time)
        count += 1


def delete_folder_contens(folder: str) -> None:
    """
    Delete contents of entire folder.

    :param folder: folder that will have contents deleted, folder will remain (empty)
    """
    for entry in os.listdir(folder):
        path = os.path.join(folder, entry)
        try:
            if os.path.isfile(path) or os.path.islink(path):
                os.unlink(path)
            elif os.path.isdir(path):
                shutil.rmtree(path)
        except Exception as _:
            pass
