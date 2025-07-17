import os

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import _rename_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import ODIN_DATA
from odin.utils.locations import CUBIC_QLIK_DATA
from odin.utils.locations import CUBIC_ODS_FACT_DATA
from odin.utils.locations import CUBIC_ODS_REPORTS
from odin.utils.locations import AFC_DATA
from odin.utils.locations import AFC_RESTRICTED


def migration() -> None:
    """
    ODIN PROD Migration 0009.

    July 17, 2025

    This migration changes the dataset naming convention of several ODIN result sets:

    - odin/data/cubic_qlik          -> odin/data/cubic/ods_history
    - odin/data/cubic_ods           -> odin/data/cubic/ods
    - odin/data/cubic_reports       -> odin/data/cubic/reports
    - odin/data/afc/api             -> odin/data/sb/api
    - odin/data/afc/sb_restricted   -> doin/data/sb/restricted

    """
    prefix_jobs = [
        (
            os.path.join(DATA_SPRINGBOARD, ODIN_DATA, "cubic_qlik", ""),
            os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DATA, ""),
        ),
        (
            os.path.join(DATA_SPRINGBOARD, ODIN_DATA, "cubic_ods", ""),
            os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_FACT_DATA, ""),
        ),
        (
            os.path.join(DATA_SPRINGBOARD, ODIN_DATA, "cubic_reports", ""),
            os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_REPORTS, ""),
        ),
        (
            os.path.join(DATA_SPRINGBOARD, ODIN_DATA, "afc/api", ""),
            os.path.join(DATA_SPRINGBOARD, AFC_DATA, ""),
        ),
        (
            os.path.join(DATA_SPRINGBOARD, ODIN_DATA, "afc/sb_restricted", ""),
            os.path.join(DATA_SPRINGBOARD, AFC_RESTRICTED, ""),
        ),
    ]
    for old, new in prefix_jobs:
        rename_map = [(o.path, o.path.replace(old, new)) for o in list_objects(old)]
        _rename_objects(rename_map)
