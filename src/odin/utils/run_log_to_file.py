import logging
import threading
from datetime import datetime


from odin.utils.logger import LOGGER_NAME
from odin.utils.logger import LOG_FORMAT
from odin.utils.logger import DATE_FORMAT
from odin.utils.logger import log_max_mem_usage
from odin.run import start


def start_log_file():
    """
    Application Entry for logging to local file instead of stderr/stdout.

    This is a utility function that is only used for local development.
    """
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
