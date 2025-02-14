import os

from datetime import datetime
from datetime import timedelta
from datetime import UTC
from typing import List
from typing import Tuple

from odin.utils.logger import ProcessLog
from odin.utils.aws.s3 import S3Object
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import list_partitions
from odin.utils.aws.s3 import rename_objects
from odin.ingestion.qlik.utils import re_get_first
from odin.ingestion.qlik.utils import RE_CDC_TS
from odin.ingestion.qlik.utils import RE_SNAPSHOT_TS
from odin.ingestion.qlik.utils import SNAPSHOT_FMT
from odin.ingestion.qlik.utils import RecentSnapshotError
from odin.ingestion.qlik.dfm import dfm_snapshot_dt
from odin.ingestion.qlik.dfm import QlikDFM
from odin.ingestion.qlik.dfm import dfm_from_s3
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import DATA_ERROR
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import IN_QLIK_PREFIX
from odin.utils.locations import CUBIC_QLIK_ERROR
from odin.utils.locations import CUBIC_QLIK_IGNORED
from odin.utils.locations import CUBIC_QLIK_DATA


def clean_find_qlik_load_files(table: str) -> List[Tuple[str, QlikDFM]]:
    """
    Get sorted List of LOAD***.csv.gz from from bucket locations

    will be sorted by "startWriteTimestamp" of .dfm file associated with .csv.gz file.

    :param table: QLIK Table name

    :return: sorted list of LOAD**.csv.gz files
    """
    prefixes = (
        os.path.join(DATA_ARCHIVE, IN_QLIK_PREFIX, table),
        os.path.join(DATA_ERROR, IN_QLIK_PREFIX, table),
    )
    paths: List[Tuple[str, QlikDFM]] = []
    error_paths: List[str] = []
    log = ProcessLog("clean_find_qlik_load_files", table=table)
    try:
        for prefix in prefixes:
            for obj in list_objects(f"{prefix}/", in_filter="LOAD"):
                if not obj.path.endswith("csv.gz"):
                    continue
                try:
                    paths.append((obj.path, dfm_from_s3(obj.path)))
                except Exception as _:
                    error_paths.append(obj.path)
        log.complete(num_load_files=len(paths))
    except Exception as exception:
        log.failed(exception)
        raise exception
    finally:
        if error_paths:
            error_paths += [p.replace(".csv.gz", ".dfm") for p in error_paths]
            rename_objects(error_paths, DATA_ARCHIVE, prepend_prefix=CUBIC_QLIK_ERROR)

    return sorted(paths, key=lambda tup: tup[1]["fileInfo"]["startWriteTimestamp"])


def clean_old_snapshots(table: str) -> None:
    """
    Move old snapshot and change files to ignore in S3.

    This process moves snapshot and change from from the Archive and Error buckets
    to an "IGNORED" prefix of the archive bucket.

    If existing Odin snapshot partitions are found, the oldest partition is used as the
    `min_good_dt` for the process.

    If NO existing Odin snapshot partitions are found, the most recent snapshot is kept
    and any older snapshot moved to "IGNORED".

    :param table: Cubic ODS Table partition to process.
    """
    # check for existing snapshots
    log = ProcessLog("clean_old_snapshots", table=table)

    snaps = list_partitions(os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DATA, table))
    if snaps:
        min_good_dt = datetime.strptime(re_get_first(snaps[0], RE_SNAPSHOT_TS), SNAPSHOT_FMT)
    else:
        # Each group in groups will be a List[Tuple[str,datetime]]
        #       with str = path of snapshot file, datetime = datetime snapshot was generated
        # a group represents all of the individual snapshot files associated with a unique Qlik snapshot
        # e.g.: [
        #           (s3://mbta-ctd-dataplatform-archive/cubic/ods_qlik/EDW.ABP_TAP/snapshot=20250131T041823Z/LOAD00000001.csv.gz, datetime(2025,01,30,23,25,46)),
        #           (s3://mbta-ctd-dataplatform-archive/cubic/ods_qlik/EDW.ABP_TAP/snapshot=20250131T041823Z/LOAD00000002.csv.gz, datetime(2025,01,30,23,28,13)),
        #           ...
        #       ] 
        groups = []
        current_group: List[Tuple[str, datetime]] = []
        for path, dfm in clean_find_qlik_load_files(table):
            snapshot_dt = dfm_snapshot_dt(dfm)
            if path.endswith("0001.csv.gz") and current_group:
                groups.append(current_group)
                current_group = []
            current_group.append((path, snapshot_dt))
        if current_group:
            groups.append(current_group)
        # groups is sorted from oldest snapshot group to most recent
        # this is indexing the most recent (last in groups) snapshot group
        # then indexing the creation datetime of the first snapshot file of the group (LOAD00000001.csv.gz)
        min_good_dt = groups[-1][0][1]

        move_snap_paths = []
        for group in groups:
            for csv_path, csv_dt in group:
                if csv_dt < min_good_dt:
                    move_snap_paths.append(csv_path)
                    move_snap_paths.append(str(csv_path).replace(".csv.gz", ".dfm"))
        if move_snap_paths:
            rename_objects(move_snap_paths, DATA_ARCHIVE, prepend_prefix=CUBIC_QLIK_IGNORED)

    log.add_metadata(min_good_dt=min_good_dt)

    # Move change files
    prefixes = (
        os.path.join(DATA_ARCHIVE, IN_QLIK_PREFIX, table),
        os.path.join(DATA_ERROR, IN_QLIK_PREFIX, table),
    )
    objects_moved = 0
    while True:
        found_objects: List[S3Object] = []
        move_change_paths: List[str] = []
        for prefix in prefixes:
            found_objects += list_objects(f"{prefix}__ct/", max_objects=100_000)

        for obj in found_objects:
            if datetime.fromisoformat(re_get_first(obj.path, RE_CDC_TS)) < min_good_dt:
                move_change_paths.append(obj.path)

        if len(move_change_paths) == 0:
            break
        objects_moved += len(move_change_paths)
        rename_objects(move_change_paths, DATA_ARCHIVE, prepend_prefix=CUBIC_QLIK_IGNORED)

    log.complete(objects_moved=objects_moved)
