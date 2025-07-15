import os

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import AFC_DATA


def migration() -> None:
    """
    ODIN PROD Migration 0008.

    July 15, 2025

    This migration is to reset S&B API files for the "v_meida" table to prepare for the
    removal of PPI from the dataset.
    """
    sb_prefix = os.path.join(DATA_SPRINGBOARD, AFC_DATA, "v_media", "")
    delete_objects([obj.path for obj in list_objects(sb_prefix)])
