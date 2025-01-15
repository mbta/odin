import logging
import os
import time
import uuid
import shutil
import traceback
from typing import Dict
from typing import Union
from typing import Optional

import psutil


MdValues = Optional[Union[str, int, float]]

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
# if running on AWS drop timestamp because Splunk handles that
if bool(os.getenv("AWS_DEFAULT_REGION")):
    LOG_FORMAT = "{levelname:>8s} {message}"
else:
    LOG_FORMAT = "{asctime} {levelname:>8s} {message}"

# Use/Create logger based on SERVICE_NAME, don't use root logger
LOGGER_NAME = "odin_app"
LOGGER = logging.getLogger(LOGGER_NAME)
if len(LOGGER.handlers) == 0:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT, style="{"))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(logging.INFO)


class ProcessLog:
    """Process Logger."""

    # default_data keys that can not be added as metadata
    protected_keys = [
        "parent",
        "process_name",
        "process_id",
        "uuid",
        "status",
        "duration",
        "error_type",
        "auto_start",
    ]

    def __init__(self, process: str, auto_start: bool = True, **metadata: MdValues) -> None:
        """
        Create a process logger with a name and optional metadata.

        :param process: name of process being logged
        :param auto_start: bool -> if True(default) automatically start log
        :param metadata: any key/value pair to log
        """
        self.default_data: Dict[str, MdValues] = {}
        self.metadata: Dict[str, MdValues] = {}

        self.default_data["parent"] = os.getenv("SERVICE_NAME", "unset")
        self.default_data["process"] = process

        self.start_time = 0.0
        self.uuid = ""

        self.add_metadata(**metadata)

        if auto_start:
            self.start()

    def _get_log_string(self) -> str:
        """Create logging string from all default_data and metadata."""
        _, _, free_disk_bytes = shutil.disk_usage("/")
        self.default_data["disk_mb_free"] = int(free_disk_bytes / (1000 * 1000))
        self.default_data["mem_pct_free"] = int(100 - psutil.virtual_memory().percent)
        logging_list = []
        # add default data to log output with uuid first
        logging_list.append(f"uuid={self.uuid}")
        for key, value in self.default_data.items():
            logging_list.append(f"{key}={value}")

        # add metadata to log output
        for key, value in self.metadata.items():
            logging_list.append(f"{key}={value}")

        return ", ".join(logging_list)

    def add_metadata(self, **metadata: MdValues) -> None:
        """
        Add metadata to a log.

        :param print_log: bool -> if True(default), print log after metadata is added
        :param metadata: any key/value pair to log
        """
        metadata.setdefault("print_log", True)
        print_log = bool(metadata.pop("print_log"))

        for key, value in metadata.items():
            if key in self.protected_keys:
                LOGGER.warning(
                    f"uuid={self.uuid}, '{key}' conflicts with protected ProcessLog key."
                )
                continue
            self.metadata[str(key)] = str(value)

        if self.default_data.get("status") is not None and print_log:
            self.default_data["status"] = "add_metadata"
            LOGGER.info(self._get_log_string())

    def start(self) -> None:
        """Log start of a proccess."""
        self.uuid = str(uuid.uuid4())
        self.default_data["process_id"] = os.getpid()
        self.default_data["status"] = "started"
        self.default_data.pop("duration", None)
        self.default_data.pop("error_type", None)

        self.start_time = time.monotonic()

        LOGGER.info(self._get_log_string())

    def complete(self, **metadata: MdValues) -> None:
        """
        Log completion of a proccess.

        :param metadata: any key/value pair to log
        """
        self.add_metadata(print_log=False, **metadata)

        duration = time.monotonic() - self.start_time
        self.default_data["status"] = "complete"
        self.default_data["duration"] = f"{duration:.2f}"

        LOGGER.info(self._get_log_string())

    def failed(self, exception: Exception) -> None:
        """
        Log failure of a process.

        :param exception: Any Exception to be logged
        """
        duration = time.monotonic() - self.start_time
        self.default_data["status"] = "failed"
        self.default_data["duration"] = f"{duration:.2f}"
        self.default_data["error_type"] = type(exception).__name__

        # Last 2 stacks are ProcessLog calls and should be dropped
        # This is for exceptions that are not 'raised'
        # 'raised' exceptions will also be logged to sys.stderr
        for stack_entry in traceback.format_stack()[:-2]:
            for line in stack_entry.strip("\n").split("\n"):
                LOGGER.error(f"uuid={self.uuid}, {line.strip('\n')}")

        # Log Exception
        for line in traceback.format_exception_only(exception):
            LOGGER.error(f"uuid={self.uuid}, {line.strip('\n')}")

        # Log Process Failure
        LOGGER.info(self._get_log_string())
