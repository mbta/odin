import sched
import signal
import time

from odin.utils.runtime import validate_env_vars
from odin.utils.runtime import handle_sigterm
from odin.utils.runtime import load_config
from odin.utils.logger import ProcessLog
from odin.migrate.process import start_migrations
from odin.utils.aws.ecs import check_for_parallel_tasks

# Job Schedule functions
from odin.utils.runtime import schedule_sigterm_check
from odin.ingestion.qlik.cubic_archive import schedule_cubic_archive_qlik
from odin.generate.cubic.ods_fact import schedule_cubic_ods_fact_gen
from odin.ingestion.afc.afc_archive import schedule_afc_archive
from odin.ingestion.afc.afc_restricted import schedule_restricted_afc_archive
from odin.generate.data_dictionary.dictionary import schedule_dictionary
from odin.ingestion.tableau.tableau_upload_test import schedule_tableau_upload


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
        private=[
            "AFC_API_CLIENT_ID",
            "AFC_API_CLIENT_SECRET",
        ],
        aws=[
            "ECS_CLUSTER",
            "ECS_TASK_GROUP",
        ],
    )
    check_for_parallel_tasks()
    start_migrations()

    ProcessLog("odin_event_loop")
    schedule = sched.scheduler(time.monotonic, time.sleep)

    # Schedule ODIN Jobs
    config = load_config()
    schedule_sigterm_check(schedule)
    if "cubic_archive_qlik" in config:
        schedule_cubic_archive_qlik(schedule)
    if "cubic_ods_fact" in config:
        schedule_cubic_ods_fact_gen(schedule)
    if "afc_archive" in config:
        schedule_afc_archive(schedule)
        schedule_restricted_afc_archive(schedule)
    if "data_dictionary" in config:
        schedule_dictionary(schedule)
    if "tableau_upload" in config:
        schedule_tableau_upload(schedule)

    schedule.run()
