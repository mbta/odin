import os
import sched
import signal
import time
import logging
import threading
import tomllib
from datetime import datetime


from odin.utils.runtime import validate_env_vars
from odin.utils.runtime import handle_sigterm
from odin.utils.logger import ProcessLog
from odin.utils.logger import LOGGER_NAME
from odin.utils.logger import LOG_FORMAT
from odin.utils.logger import DATE_FORMAT
from odin.utils.logger import log_max_mem_usage

# Job Schedule functions
from odin.utils.runtime import schedule_sigterm_check
from odin.ingestion.qlik.cubic_archive import schedule_cubic_archive_qlik
from odin.generate.cubic.ods_fact import schedule_cubic_ods_fact_gen


def start():
    """
    Application Entry.

    Odin runs "jobs" on a continuous event loop. All jobs are initiated by a sched.scheduler general
    purpose event scheduler.

    Currently, all jobs are added to the scheduler via the "delayed" scheduling type, which will
    re-exectue the job after the specified "delay" period has passed, since the last execution.

    All jobs must be a child of the OdinJob base class, specified in job.py. The OdinJob base class
    guarantees certain logging characteristics for every job and makes certain that the Job will not
    fail in a way that interrupts the execution of subsequently scheduled jobs.
    """
    signal.signal(signal.SIGTERM, handle_sigterm)
    config = load_config()
    ProcessLog("load_config", config=config)
    if "cubic_archive_qlik" in config or "cubic_ods_fact" in config:
        required_env_vars = [
            "DATA_ARCHIVE",
            "DATA_ERROR",
            "DATA_INCOMING",
            "DATA_SPRINGBOARD",
        ]
    else:
        required_env_vars = []

    validate_env_vars(
        required=required_env_vars,
        aws=[
            "ECS_CLUSTER",
            "ECS_TASK_GROUP",
        ],
    )

    schedule = sched.scheduler(time.monotonic, time.sleep)

    log = ProcessLog("odin_event_loop")

    # Schedule ODIN Jobs
    schedule_sigterm_check(schedule)
    if "cubic_archive_qlik" in config:
        schedule_cubic_archive_qlik(schedule)
    if "cubic_ods_fact" in config:
        schedule_cubic_ods_fact_gen(schedule)

    schedule.run()
    log.complete()


def start_log_file():
    """Application Entry for logging to local file instead of stderr/stdout."""
    logger = logging.getLogger(LOGGER_NAME)
    for handler in logger.handlers:
        logger.removeHandler(handler)

    now = datetime.now().replace(microsecond=0)
    log_file_name = f"./odin_log_{now.isoformat()}.log"
    handler = logging.FileHandler(log_file_name, mode="w")
    handler.setFormatter(logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT, style="{"))
    logger.addHandler(handler)

    stop_event = threading.Event()
    mem_thread = threading.Thread(target=log_max_mem_usage, args=(stop_event,))
    mem_thread.start()

    try:
        start()
    except Exception as exception:
        raise exception
    finally:
        stop_event.set()
        mem_thread.join()


def load_config():
    """Load from env var `ODIN_CONFIG`, fallback to file `config.toml`. Raise if neither exist."""
    config_string = os.getenv("ODIN_CONFIG")
    if config_string is not None:
        return tomllib.loads(config_string)
    else:
        try:
            with open("config.toml", "rb") as f:
                return tomllib.load(f)
        except FileNotFoundError as e:
            raise Exception("Missing config. Needs env var ODIN_CONFIG or file config.toml") from e
