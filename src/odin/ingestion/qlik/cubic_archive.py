import os
import gzip
import shutil
import hashlib
import sched
import tempfile

from datetime import datetime
from typing import List
from typing import Tuple
from typing import Dict
from typing import Optional
from itertools import batched
from concurrent.futures import ThreadPoolExecutor

from odin.utils.logger import ProcessLog
from odin.utils.runtime import sigterm_check
from odin.job import OdinJob
from odin.job import job_proc_schedule
from odin.utils.runtime import thread_cpus
from odin.utils.aws.s3 import S3Object
from odin.utils.aws.s3 import upload_file
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import list_partitions
from odin.utils.aws.s3 import download_object
from odin.utils.aws.s3 import stream_object
from odin.utils.aws.s3 import rename_objects
from odin.utils.aws.ecs import running_in_aws
from odin.ingestion.qlik.utils import SNAPSHOT_FMT
from odin.ingestion.qlik.utils import RE_CDC_TS
from odin.ingestion.qlik.utils import RecentSnapshotError
from odin.ingestion.qlik.utils import find_qlik_load_files
from odin.ingestion.qlik.utils import find_qlik_cdc_files
from odin.ingestion.qlik.utils import cdc_csv_to_parquet
from odin.ingestion.qlik.utils import snapshot_to_parquet
from odin.ingestion.qlik.utils import re_get_first
from odin.ingestion.qlik.dfm import QlikDFM
from odin.ingestion.qlik.dfm import dfm_snapshot_dt
from odin.ingestion.qlik.tables import CUBIC_ODS_TABLES
from odin.ingestion.qlik.clean import clean_old_snapshots
from odin.utils.locations import CUBIC_QLIK_DATA
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import CUBIC_QLIK_PROCESSED
from odin.utils.locations import IN_QLIK_PREFIX
from odin.utils.parquet import pq_dataset_writer
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import ds_unique_values

NEXT_RUN_DEFAULT = 60 * 60 * 4  # 4 hours
NEXT_RUN_IMMEDIATE = 60 * 5  # 5 minutes
NEXT_RUN_LONG = 60 * 60 * 12  # 12 hours


def thread_save_csv(args: Tuple[str, str]) -> Tuple[int, Optional[str]]:
    """
    Save csv file from S3 to local disk.

    The S3 path of the csv file will be pre-pended to each csv row.

    :param args: (csv_s3_object, directory to save to)

    :return: (bytes_written, object_path, save_failed)
    """
    obj_path, save_dir = args
    bytes_written = 0
    save_file = obj_path.replace("s3://", "").replace("/", "|").replace(".gz", "")
    b_path = f'"{obj_path}",'.encode("utf8")
    try:
        with gzip.open(stream_object(obj_path), "rb") as r_bytes:
            save_folder = os.path.join(save_dir, hashlib.sha1(r_bytes.readline()).hexdigest())
            os.makedirs(save_folder, exist_ok=True)
            with open(os.path.join(save_folder, save_file), mode="wb") as w_bytes:
                for line in r_bytes.readlines():
                    w_bytes.write(b_path + line)
                bytes_written = w_bytes.tell()
        return_path = None

    except Exception as exception:
        log = ProcessLog("thread_save_csv", obj_path=obj_path)
        log.failed(exception)
        return_path = obj_path

    return (bytes_written, return_path)


class ArchiveCubicQlikTable(OdinJob):
    """Combine Qlik files into single parquet file."""

    def __init__(self, table: str) -> None:
        """Create QlikSingleTable instance."""
        self.table = table

        self.save_local = True
        if running_in_aws():
            self.save_local = False

        self.export_folder = os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DATA, self.table)
        self.start_kwargs = {"table": self.table, "save_local": self.save_local}

    def sync_tmp_paths(self, tmp_paths: List[str]) -> None:
        """
        Sync existing parquet files.

        For each table partition, if existing partition files exist, sync them to the local
        temporary directory to be joined with newly ingested records.
        """
        if len(tmp_paths) == 0:
            return
        ProcessLog("sync_tmp_paths")
        part_columns = ["snapshot", "header__year", "header__month"]
        sync_paths = []
        for part in ds_unique_values(ds_from_path(tmp_paths), part_columns).to_pylist():
            part_prefix = "/".join([f"{k}={v}" for k, v in part.items()])
            search_path = f"{os.path.join(self.export_folder, part_prefix)}/"
            # Sync existing files from local disk
            if self.save_local:
                search_path = f"./{search_path}"
                os.makedirs(search_path, exist_ok=True)
                found_files = sorted(os.listdir(search_path))
                if found_files:
                    sync_file = os.path.join(search_path, found_files[-1])
                    destination = os.path.join(
                        self.tmpdir, sync_file.lstrip("./").replace("/month_", "/temp_")
                    )
                    os.makedirs(os.path.dirname(destination), exist_ok=True)
                    shutil.copy(sync_file, destination)
                    sync_paths.append(destination)
            # Sync existing objects from S3
            else:
                found_objs = list_objects(search_path, in_filter=".parquet")
                if found_objs:
                    sync_file = found_objs[-1].path.replace("s3://", "")
                    destination = os.path.join(self.tmpdir, sync_file.replace("/month_", "/temp_"))
                    download_object(found_objs[-1].path, destination)
                    sync_paths.append(destination)

        # TODO: This process could be improved by not ALWAYS merging new data into existing parquet
        # files. Be default, new append-only data could be written straigh to the dataset as new
        # files and then once a threshold is hit (number of dataset files under max size limit)
        # the files would begin to be merged together. Basically an automatic sweeping function
        # that cleans-up/merges parquet files for better read performance at expected intervals.

        # Create new merged parquet file(s)
        new_paths = pq_dataset_writer(
            source=ds_from_path(sync_paths + tmp_paths),
            partition_columns=part_columns,
            export_folder=os.path.join(self.tmpdir, self.export_folder),
            export_file_prefix="month",
        )

        # Check for sigterm before upload (can't be un-done)
        sigterm_check()
        for new_path in new_paths:
            # Save merged parquet paths to local disk
            if self.save_local:
                move_path = new_path.replace(self.tmpdir, ".")
                os.makedirs(os.path.dirname(move_path), exist_ok=True)
                shutil.move(new_path, move_path)
            # Upload merged parquet paths to S3
            else:
                move_path = new_path.replace(f"{self.tmpdir}/", "")
                upload_file(new_path, move_path)

    def process_snapshot_group(self, group: List[Tuple[str, QlikDFM]]) -> None:
        """
        Convert group of snapshot files to parquet and sync to S3.

        All snapshot files MUST be processed. Any error encountered during shapshot loading will
        abort the process.

        Process:
            1. Download each snapshot CSV file
            2. Convert each CSV to parquet
            3. Sync all parquet files to S3
        """
        if len(group) == 0:
            return
        log = ProcessLog("load_qlik_snapshot_group", group_len=len(group))
        assert group[0][0].endswith("0001.csv.gz")
        snap_dt = dfm_snapshot_dt(group[0][1])
        snap_part = f"snapshot={snap_dt.strftime(SNAPSHOT_FMT)}"
        part_folder = os.path.join(self.tmpdir, snap_part)
        os.makedirs(part_folder, exist_ok=True)
        pq_snap_paths = []
        for path, dfm in group:
            pq_snap_paths.append(snapshot_to_parquet(path, dfm, part_folder))
            self.archive_objects.append(path)

            # If more than 20 snapshot files are being processed, they will be "synced"
            # in batches of 20. This limits to amount of local storage used during the
            # snapshot load process.
            if len(pq_snap_paths) > 19:
                self.sync_tmp_paths(pq_snap_paths)
                self.reset_tmpdir()
                part_folder = os.path.join(self.tmpdir, snap_part)
                os.makedirs(part_folder, exist_ok=True)
                pq_snap_paths = []

        self.sync_tmp_paths(pq_snap_paths)
        self.reset_tmpdir()
        log.complete()

    def load_snapshots(self) -> None:
        """
        Archive Qlik LOAD file(s) to parquet partitions.

        In the case that there are multiple Qlik snapshots are available for processsing, this
        function will group files belonging to the same snapshot together.

        All of the LOAD files, belonging to a snapshot group, are then processed individually and
        added to the same snapshot partition.
        """
        snapshot_group: List[Tuple[str, QlikDFM]] = []
        for path, dfm in find_qlik_load_files(self.table, self.save_local):
            if path.endswith("00001.csv.gz"):
                self.process_snapshot_group(snapshot_group)
                snapshot_group.clear()
            snapshot_group.append((path, dfm))
        self.process_snapshot_group(snapshot_group)

    def group_cdc(self, cdc_files: List[S3Object]) -> Dict[str, List[S3Object]]:
        """
        Group cdc files by applicable Qlik snapshot.

        :param cdc_files: files to group

        :return: {snap_part as (snapshot=YYYYmmddTHHMMSSZ): [S3Object(s) in snapshot]}
        """
        if self.save_local:
            snap_parts = os.listdir(self.export_folder)
        else:
            snap_parts = list_partitions(self.export_folder)

        snaps: Dict[str, List[S3Object]] = {d: [] for d in sorted(snap_parts, reverse=True)}
        for obj in cdc_files:
            file_dt = datetime.fromisoformat(re_get_first(obj.path, RE_CDC_TS))
            file_snap = f"snapshot={file_dt.strftime(SNAPSHOT_FMT)}"
            for snap in snaps.keys():
                if file_snap > snap:
                    snaps[snap].append(obj)
                    break

        for snap, objs in snaps.items():
            snaps[snap] = sorted(objs, key=lambda obj: obj.path[-25:])

        return snaps

    def load_cdc(self, cdc_files: List[S3Object]) -> None:
        """
        Archive Qlik cdc files to parquet partitions.

        Process:
            1. Group cdc files by snapshot.
             - group key will be snapshot partition (snapshot=YYYYmmddTHHMMSSZ)
            2. Download cdc files in batches of `batch_limit_bytes`
             - downloaded cdc files will be saved in folders based on sha hash of file header
            3. For each batch, merge all csv files into parquet files
            4. Sync newly created parquet files with existing files
        """
        batch_limit_bytes = 1_000 * 1024 * 1024
        groups = self.group_cdc(cdc_files)
        pool = ThreadPoolExecutor(max_workers=thread_cpus())
        log = ProcessLog("load_qlik_cdc_groups")
        try:
            for snap_part in sorted(groups.keys()):
                if len(groups[snap_part]) == 0:
                    continue
                log.add_metadata(snapshot=snap_part)
                os.makedirs(os.path.join(self.tmpdir, snap_part))
                cdc_batch_bytes = [0]
                cdc_paths = []
                failed_objects: List[str] = []
                # Create temporary directory for cdc file downloads
                with tempfile.TemporaryDirectory() as tmpdir:
                    work_objs = [(obj.path, tmpdir) for obj in groups[snap_part]]
                    # download .csv.gz cdc files in batches using ThreadPool download operation
                    # returns size of uncompressed csv file to determine when a bunch of downloaded
                    # csv files should be converted to parquet
                    for batch in batched(work_objs, thread_cpus()):
                        for size_bytes, obj_path in pool.map(thread_save_csv, batch):
                            cdc_batch_bytes[-1] += size_bytes
                            if isinstance(obj_path, str):
                                failed_objects.append(obj_path)
                        if failed_objects:
                            raise Exception(f"Failed to save csv(s): {','.join(failed_objects)}")
                        if cdc_batch_bytes[-1] > batch_limit_bytes:
                            written, archive = cdc_csv_to_parquet(
                                tmpdir, os.path.join(self.tmpdir, snap_part)
                            )
                            cdc_batch_bytes.append(0)
                            cdc_paths += written
                            self.archive_objects += archive
                        # process max of ~5GB raw csv files in one event loop.
                        if sum(cdc_batch_bytes) > 5_000 * 1024 * 1024:
                            break
                    if cdc_batch_bytes[-1] > 0:
                        written, archive = cdc_csv_to_parquet(
                            tmpdir, os.path.join(self.tmpdir, snap_part)
                        )
                        cdc_paths += written
                        self.archive_objects += archive

                self.sync_tmp_paths(cdc_paths)
                self.reset_tmpdir()

            log.complete()
        except Exception as exception:
            log.failed(exception)
            raise exception

        finally:
            pool.shutdown()

    def move_objects(self) -> None:
        """Move objects to Archive location"""
        dfm_archive = [s.replace(".csv.gz", ".dfm") for s in self.archive_objects]
        to_archive = self.archive_objects + dfm_archive
        if self.save_local:
            # if running locally, files will be moved to local disk
            for file_to_download in to_archive:
                assert file_to_download.startswith(f"{DATA_ARCHIVE}/{IN_QLIK_PREFIX}")
                download_location = file_to_download.replace(
                    f"{DATA_ARCHIVE}/{IN_QLIK_PREFIX}",
                    f"{DATA_ARCHIVE}/{CUBIC_QLIK_PROCESSED}/{IN_QLIK_PREFIX}"
                )
                download_object(file_to_download, download_location)
        else:
            # if running in AWS, rename
            rename_objects(to_archive, DATA_ARCHIVE, prepend_prefix=CUBIC_QLIK_PROCESSED)

    def run(self) -> int:
        """
        Archive QLIK snapshot/cdc files.

        Process:
            1. Generate list of cdc files before snapshot loading
                - avoid race condition of processing cdc files without associated
                  snapshot having been loaded
            2. Load any available snapshot files
                - if new snapshot within 6 hours process will be aborted
                - this is not reported as an Error, just logged with "skipped_recent_snapshot" field
            3. Load cdc files generated from step 1
            4. Move processed files on S3

        next_run Duration:
            - default -> 60 mins
            - if table updated in-frequently -> 6 hours
                * when 0 cdc files are available to process
            - if backlog of cdc files exists -> 5 mins
                * when cdc files > half of max_cdc_files requested
        """
        try:
            self.start_kwargs = {"table": self.table, "save_local": self.save_local}
            self.archive_objects: List[str] = []

            next_run_secs = NEXT_RUN_DEFAULT
            max_cdc_files = 10_000
            cdc_files = find_qlik_cdc_files(self.table, self.save_local, max_cdc_files)
            if len(cdc_files) == 0:
                next_run_secs = NEXT_RUN_LONG
            elif len(cdc_files) / max_cdc_files > 0.5:
                next_run_secs = NEXT_RUN_IMMEDIATE

            self.load_snapshots()
            self.load_cdc(cdc_files)
            self.move_objects()

        except RecentSnapshotError:
            self.start_kwargs["skipped_recent_snapshot"] = True
            next_run_secs = NEXT_RUN_DEFAULT

        return next_run_secs


def schedule_cubic_archive_qlik(schedule: sched.scheduler) -> None:
    """
    Schedule All Jobs for Cubic Qlik Archive process.

    :param schedule: application scheduler
    """
    for table in CUBIC_ODS_TABLES:
        # This clean process should remain as long as new Qlik tables are being added.
        # This will move any qlik files, not associated with the most recent snapshot,
        # to the "ignore" odin partition, if there is an existing processed shapshot, it is a no-op
        try:
            clean_old_snapshots(table)
        except Exception as _:
            # on Error don't schedule Archive Job
            continue
        job = ArchiveCubicQlikTable(table)
        schedule.enter(0, 1, job_proc_schedule, (job, schedule))
