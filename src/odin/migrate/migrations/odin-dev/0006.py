import os

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import AFC_DATA


def migration() -> None:
    """
    ODIN DEV Migration 0006.

    July 2, 2025

    This migration is to reset S&B API files after changes have been deployed
    to fix type inconsistencies.
    After deletion, files will be re-created on next ArchiveAFCAPI job runs.

    1. Delete all parquet files from S&B API prefix.
    """
    sb_prefix = os.path.join(DATA_SPRINGBOARD, AFC_DATA)
    delete_objects([obj.path for obj in list_objects(sb_prefix)])
