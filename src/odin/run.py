import os
import sched
import signal
import time
import tomllib

from odin.utils.runtime import validate_env_vars
from odin.utils.runtime import handle_sigterm
from odin.utils.logger import ProcessLog
from odin.migrate.process import start_migrations

# Job Schedule functions
from odin.utils.runtime import schedule_sigterm_check
from odin.ingestion.qlik.cubic_archive import schedule_cubic_archive_qlik
from odin.ingestion.spare.spare_job import schedule_spare_jobs
from odin.generate.cubic.ods_fact import schedule_cubic_ods_fact_gen
from odin.ingestion.afc.afc_archive import schedule_afc_archive


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
        private=[
            "AFC_API_CLIENT_ID",
            "AFC_API_CLIENT_SECRET",
        ],
        aws=[
            "ECS_CLUSTER",
            "ECS_TASK_GROUP",
        ],
    )
    start_migrations()

    schedule = sched.scheduler(time.monotonic, time.sleep)

    ProcessLog("odin_event_loop")

    # Schedule ODIN Jobs
    schedule_sigterm_check(schedule)
    if "cubic_archive_qlik" in config:
        schedule_cubic_archive_qlik(schedule)
    if "cubic_ods_fact" in config:
        schedule_cubic_ods_fact_gen(schedule)
    if "afc_archive" in config:
        schedule_afc_archive(schedule)
    if "spare" in config:
        schedule_spare_jobs(schedule, config["spare"])

    schedule.run()


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
