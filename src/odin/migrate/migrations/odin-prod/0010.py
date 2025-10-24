import os

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import ODIN_DATA


def migration() -> None:
    """
    ODIN PROD Migration 0010.

    August 28, 2025

    This migration is to delete files leftover from ODIN PROD Migration 0009, specifically
    s3://springboard/odin/data/cubic_reports.
    """
    sb_prefix = os.path.join(DATA_SPRINGBOARD, ODIN_DATA, "cubic_reports", "")
    attempted_deletions = delete_objects([obj.path for obj in list_objects(sb_prefix)])
    assert len(attempted_deletions) == 0, (
        f"Failed to delete ... {len(attempted_deletions)} objects."
    )
