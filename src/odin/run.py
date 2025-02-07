import os
import sched
import signal
import time
import logging
import threading
from datetime import datetime


from odin.utils.runtime import validate_env_vars
from odin.utils.runtime import handle_sigterm
from odin.utils.logger import ProcessLog
from odin.utils.logger import LOGGER_NAME
from odin.utils.logger import LOG_FORMAT
from odin.utils.logger import DATE_FORMAT
from odin.utils.logger import log_max_mem_usage
from odin.ingestion.qlik.cubic_archive import ArchiveCubicQlikTable
from odin.ingestion.qlik.tables import CUBIC_ODS_TABLES
from odin.utils.aws.ecs import running_in_aws
from odin.ingestion.qlik.clean import clean_old_snapshots

def start():
    """Application Entry."""
    signal.signal(signal.SIGTERM, handle_sigterm)
    os.environ["SERVICE_NAME"] = "odin"
    validate_env_vars(required=[])

    scheduler = sched.scheduler(time.monotonic, time.sleep)

    log = ProcessLog("odin_event_loop")

    for table in CUBIC_ODS_TABLES:
        if running_in_aws():
            # This will be not be a permanent part of the pipeline and can be removed in the future
            # This will move any qlik files, not associated with the most recent snapshot, to the
            # "Ignore" odin partition
            clean_old_snapshots(table)
        job = ArchiveCubicQlikTable(table)
        scheduler.enter(0, 1, job.start, (scheduler,))

    scheduler.run()
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
