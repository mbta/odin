import os

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import ODIN_DICTIONARY


def migration() -> None:
    """
    ODIN DEV Migration 0004.

    June 20, 2025

    This file deletes the existing data dictionary objects because a new file name format is
    being used that includes the application environment.
    """
    data_dict_prefix = os.path.join(DATA_SPRINGBOARD, ODIN_DICTIONARY)
    delete_objects([obj.path for obj in list_objects(data_dict_prefix)])
