import os

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import AFC_DATA


def migration() -> None:
    """
    ODIN DEV Migration 009.

    August 28, 2025

    This migration is to delete files leftover from ODIN DEV Migration 0008, specifically
    s3://springboard/odin/data/cubic_reports.
    """
    sb_prefix = os.path.join(DATA_SPRINGBOARD, ODIN_DATA, "cubic_reports", "")
    attempted_deletions = delete_objects([obj.path for obj in list_objects(sb_prefix)]) # if there is a failure, it doesn't raise. It returns a list of failed deletions, so we should capture these and assert that the length is 0
    assert len(attempted_deletions) == 0, f"Failed to delete ... {len(attempted_deletions)} objects."
