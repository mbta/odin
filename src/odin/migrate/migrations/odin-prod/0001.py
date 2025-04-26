import os

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import list_partitions
from odin.utils.aws.s3 import delete_objects
from odin.utils.aws.s3 import rename_objects
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import CUBIC_QLIK_DATA
from odin.utils.locations import CUBIC_ODS_FACT_DATA
from odin.utils.locations import CUBIC_QLIK_PROCESSED
from odin.utils.locations import IN_QLIK_PREFIX


def migration() -> None:
    """
    ODIN PROD Migration 0001.

    This migration is to reset cubic ODS archive a fact datasets. The migration will be executing
    the following for each ODS Table Type:

    1. Move CDC/LOAD files from ODIN archive location back to cubic/ods_qlik ingestion partition.
    2. Delete all Archive parquet files from odin/data/cubic_qlik partition on SPRINGBOARD bucket
    3. Delete all Fact parquet files from odin/data/cubic_ods partition on SPRINGBOARD bucket.
    """
    prefix = os.path.join(DATA_ARCHIVE, CUBIC_QLIK_PROCESSED, IN_QLIK_PREFIX)

    # Iterate through each ODS table partition (EDW.ABP_TAP, EDW.ABP_TAP__ct, ...)
    for table in list_partitions(prefix):
        table_prefix = os.path.join(prefix, f"{table}/")
        # Iterate through each "snapshot" partition to maintain naming on object rename
        # (snapshot=..., timestamp=....)
        snap_parts = list_partitions(table_prefix)
        while len(snap_parts) > 0:
            for snap_part in snap_parts:
                snap_prefix = os.path.join(table_prefix, snap_part)
                replace_prefix = os.path.join(IN_QLIK_PREFIX, table, snap_part)
                part_objects = [obj.path for obj in list_objects(snap_prefix)]
                while len(part_objects) > 0:
                    # Move objects back to Archive for ingestion
                    rename_objects(part_objects, DATA_ARCHIVE, replace_prefix=replace_prefix)
                    part_objects = [obj.path for obj in list_objects(snap_prefix)]

            snap_parts = list_partitions(table_prefix)

        if table.endswith("__ct"):
            continue

        # Delete Cubic Archive and Fact parquet files.
        fact_prefix = os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_FACT_DATA, table)
        archive_prefix = os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DATA, table)
        delete_objects([obj.path for obj in list_objects(fact_prefix)])
        delete_objects([obj.path for obj in list_objects(archive_prefix)])
