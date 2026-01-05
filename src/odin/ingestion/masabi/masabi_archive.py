"""Masabi archive job placeholder."""

from __future__ import annotations

import os
import sched

from odin.job import NEXT_RUN_DEFAULT, OdinJob, job_proc_schedule
from odin.utils.locations import DATA_SPRINGBOARD, MASABI_DATA

TABLES = [
    "retail.account_actions",
]


class ArchiveMasabi(OdinJob):
    """Basic Odin job stub for Masabi ingestion."""

    def __init__(self, table: str) -> None:
        """Create Job instance."""
        self.table = table
        self.start_kwargs = {"table": table}
        self.export_folder = os.path.join(DATA_SPRINGBOARD, MASABI_DATA, table)
        self.headers = {
            "client_id": os.getenv("MASABI_API_CLIENT_ID", ""),
            "client_secret": os.getenv("MASABI_API_CLIENT_SECRET", ""),
        }

    def run(self) -> int:
        """Execute the Masabi archive run loop."""
        print(f"Ingest: {self.table}")

        return NEXT_RUN_DEFAULT  # 6 hours


def schedule_masabi_archive(schedule: sched.scheduler) -> None:
    """
    Schedule the Masabi archive job on the provided scheduler.

    :param schedule: application scheduler
    """
    for table in TABLES:
        job = ArchiveMasabi(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
