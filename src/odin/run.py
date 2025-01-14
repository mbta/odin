import os

from odin.utils.logger import ProcessLog
from odin.utils.runtime import validate_env_vars


def start():
    """Application Entry"""
    os.environ["SERVICE_NAME"] = "odin"
    validate_env_vars(
        required=[],
    )
    log = ProcessLog("odin_event_loop")
    log.complete()


if __name__ == "main":
    start()
