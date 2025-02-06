import os
import sys
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

    # every pipeline needs a service name for logging
    required.append("SERVICE_NAME")

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
        os_cpu_count = 4

    if running_in_aws():
        os_cpu_count *= 2

    return os_cpu_count


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
