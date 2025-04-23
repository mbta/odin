import sched
import signal
import time

from odin.utils.runtime import validate_env_vars
from odin.utils.runtime import handle_sigterm
from odin.utils.logger import ProcessLog
from odin.migrate.process import start_migrations

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
    validate_env_vars(
        required=[
            "DATA_ARCHIVE",
            "DATA_ERROR",
            "DATA_INCOMING",
            "DATA_SPRINGBOARD",
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
    schedule_cubic_archive_qlik(schedule)
    schedule_cubic_ods_fact_gen(schedule)

    schedule.run()
