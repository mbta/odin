from os import environ
from sched import scheduler
from stat import filemode

from paramiko import AutoAddPolicy, SSHClient

from odin.job import OdinJob, job_proc_schedule
from odin.utils.logger import ProcessLog

NEXT_RUN_DEFAULT: int = 60 * 60  # hourly


def schedule_fiserv_archive(schedule: scheduler) -> None:
    """
    Schedule FiservArchive process.

    :param schedule: application scheduler
    """
    job = FiservArchive()
    schedule.enter(0, 1, job_proc_schedule, (job, schedule))


class FiservArchive(OdinJob):
    """Ingest new files from `fiserv/` directory of MBTA SFTP server."""

    def run(self) -> int:
        """Log access to SFTP directories."""
        logger = ProcessLog("FiservArchive")
        logger.start()

        client = SSHClient()
        client.set_missing_host_key_policy(AutoAddPolicy())

        client.connect(
            hostname=environ.get("FISERV_SFTP_URL", "unset"),
            port=int(environ.get("FISERV_SFTP_PORT", 0)),
            username=environ.get("FISERV_SFTP_USERNAME", "unset"),
            password=environ.get("FISERV_SFTP_PASSWORD", "unset"),
            look_for_keys=False,
        )

        sftp = client.open_sftp()

        for d in ["dev", "prod"]:
            attr = sftp.stat(d)
            if attr.st_mode:
                logger.add_metadata(**{d: filemode(attr.st_mode)})
            else:
                logger.add_metadata(access=False)

        sftp.close()
        client.close()
        logger.complete()

        return NEXT_RUN_DEFAULT
