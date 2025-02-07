import os
import gzip
import shutil
import hashlib
import tempfile

from datetime import datetime
from typing import List
from typing import Tuple
from typing import Dict
from typing import Optional
from typing import Any
from itertools import batched
from concurrent.futures import ThreadPoolExecutor

from odin.utils.logger import ProcessLog
from odin.job import OdinJob
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
from odin.ingestion.qlik.utils import find_qlik_change_files
from odin.ingestion.qlik.utils import change_csv_to_parquet
from odin.ingestion.qlik.utils import snapshot_to_parquet
from odin.ingestion.qlik.utils import re_get_first
from odin.ingestion.qlik.dfm import QlikDFM
from odin.ingestion.qlik.dfm import dfm_snapshot_dt
from odin.utils.locations import CUBIC_QLIK_DATA
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import CUBIC_QLIK_ERROR
from odin.utils.locations import CUBIC_QLIK_PROCESSED
from odin.utils.parquet import pq_dataset_writer
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import ds_unique_values


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

    start_kwargs: Dict[str, Any] = {}

    def __init__(self, table: str) -> None:
        """Create QlikSingleTable instance."""
        self.table = table

        self.save_local = True
        if running_in_aws():
            self.save_local = False

        self.start_kwargs = {"table": table, "save_local": self.save_local}
        self.archive_objects: List[str] = []
        self.error_objects: List[str] = []
        self.export_folder = os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DATA, self.table)
        self.reset_tmpdir(make_new=True)

    def reset_tmpdir(self, make_new: bool = True) -> None:
        """Reset TemporaryDirectory folder."""
        if hasattr(self, "_tdir"):
            self._tdir.cleanup() # type: ignore
        if make_new:
            self._tdir = tempfile.TemporaryDirectory()
            self.tmpdir = self._tdir.name

    def sync_tmp_paths(self, tmp_paths: List[str]) -> None:
        """
        Sync existing parquet files.

        For each table partition, if existing partition files exist, sync them to the local
        temporary directory to be joined with newly ingested records.
        """
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

        # Create new merged parquet file(s)
        new_paths = pq_dataset_writer(
            source=ds_from_path(sync_paths + tmp_paths),
            partition_columns=part_columns,
            export_folder=os.path.join(self.tmpdir, self.export_folder),
            export_file_prefix="month",
        )

        for new_path in new_paths:
            # Save merged parquet paths to local disk
            if self.save_local:
                move_path = str(new_path).replace(self.tmpdir, ".")
                os.makedirs(os.path.dirname(move_path), exist_ok=True)
                shutil.move(new_path, move_path)
            # Upload merged parquet paths to S3
            else:
                move_path = str(new_path).lstrip(self.tmpdir)
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
        assert group[0][0].endswith("0001.csv.gz")
        snap_dt = dfm_snapshot_dt(group[0][1])
        snap_part = f"snapshot={snap_dt.strftime(SNAPSHOT_FMT)}"
        part_folder = os.path.join(self.tmpdir, snap_part)
        os.makedirs(part_folder, exist_ok=True)
        pq_snap_paths = []
        for path, dfm in group:
            pq_snap_paths.append(snapshot_to_parquet(path, dfm, part_folder))
            self.archive_objects.append(path)

            if len(pq_snap_paths) > 19:
                self.sync_tmp_paths(pq_snap_paths)
                self.reset_tmpdir()
                part_folder = os.path.join(self.tmpdir, snap_part)
                os.makedirs(part_folder, exist_ok=True)
                pq_snap_paths = []

        self.sync_tmp_paths(pq_snap_paths)
        self.reset_tmpdir()

    def load_snapshots(self) -> None:
        """Archive Qlik LOAD file(s) to parquet partitions."""
        snapshot_group: List[Tuple[str, QlikDFM]] = []
        for path, dfm in find_qlik_load_files(self.table):
            if path.endswith("0001.csv.gz"):
                self.process_snapshot_group(snapshot_group)
                snapshot_group.clear()
            snapshot_group.append((path, dfm))
        self.process_snapshot_group(snapshot_group)

    def group_changes(self, change_files: List[S3Object]) -> Dict[str, List[S3Object]]:
        """
        Group change files by applicable Qlik snapshot.

        :param change_files: files to group

        :return: {snap_part as (snapshot=YYYYmmddTHHMMSSZ): [S3Object(s) in snapshot]}
        """
        if self.save_local:
            snap_parts = os.listdir(self.export_folder)
        else:
            snap_parts = list_partitions(self.export_folder)

        snaps: Dict[str, List[S3Object]] = {d: [] for d in sorted(snap_parts, reverse=True)}
        for obj in change_files:
            file_dt = datetime.fromisoformat(re_get_first(obj.path, RE_CDC_TS))
            file_snap = f"snapshot={file_dt.strftime(SNAPSHOT_FMT)}"
            for snap in snaps.keys():
                if file_snap > snap:
                    snaps[snap].append(obj)
                    break

        for snap, objs in snaps.items():
            snaps[snap] = sorted(objs, key=lambda obj: obj.path[-25:])

        return snaps

    def load_changes(self, change_files: List[S3Object]) -> None:
        """
        Archive Qlik change files to parquet partitions.

        Process:
            1. Group change files by snapshot.
             - group key will be snapshot partition (snapshot=YYYYmmddTHHMMSSZ)
            2. Download change files in batches of `batch_limit_bytes`
             - downloaded change files will be saved in folders based on sha hash of file header
            3. For each batch, merge all csv files into parquet files
            4. Sync newly created parquet files with existing files
        """
        batch_limit_bytes = 1_000 * 1024 * 1024
        groups = self.group_changes(change_files)
        pool = ThreadPoolExecutor(max_workers=thread_cpus())
        for snap_part in sorted(groups.keys()):
            if len(groups[snap_part]) == 0:
                continue
            log = ProcessLog("load_changes_group", snapshot=snap_part)
            os.makedirs(os.path.join(self.tmpdir, snap_part))
            changes_bytes = 0
            change_paths = []
            # Create temporary directory for change file downloads
            with tempfile.TemporaryDirectory() as tmpdir:
                work_objs = [(obj.path, tmpdir) for obj in groups[snap_part]]
                for batch in batched(work_objs, thread_cpus() * 2):
                    for size_bytes, obj_path in pool.map(thread_save_csv, batch):
                        changes_bytes += size_bytes
                        if isinstance(obj_path, str):
                            self.error_objects.append(obj_path)
                    if changes_bytes > batch_limit_bytes:
                        written, archive, error = change_csv_to_parquet(
                            tmpdir, os.path.join(self.tmpdir, snap_part)
                        )
                        changes_bytes = 0
                        change_paths += written
                        self.archive_objects += archive
                        self.error_objects += error
                    # Process max of 20 files.
                    if len(change_paths) > 19:
                        break
                if changes_bytes > 0:
                    written, archive, error = change_csv_to_parquet(
                        tmpdir, os.path.join(self.tmpdir, snap_part)
                    )
                    change_paths += written
                    self.archive_objects += archive
                    self.error_objects += error

            self.sync_tmp_paths(change_paths)
            self.reset_tmpdir()
            log.complete()

        pool.shutdown()

    def move_objects(self) -> None:
        """Move objects to Archive and error locations"""
        if self.save_local:
            return
        dfm_archive = [s.replace(".csv.gz", ".dfm") for s in self.archive_objects]
        to_archive = self.archive_objects + dfm_archive
        rename_objects(to_archive, DATA_ARCHIVE, prepend_prefix=CUBIC_QLIK_PROCESSED)

        dfm_error = [s.replace(".csv.gz", ".dfm") for s in self.error_objects]
        to_error = self.error_objects + dfm_error
        rename_objects(to_error, DATA_ARCHIVE, prepend_prefix=CUBIC_QLIK_ERROR)

    def run(self) -> int:
        """
        Archive QLIK snapshot/change files.

        Process:
            1. Generate list of change files before snapshot loading
                - avoid race condition of processing change files without associated
                  snapshot having been loaded
            2. Load any available snapshot files
                - if new snapshot within 6 hours process will be aborted
                - this is not reported as an Error, just logged with "skipped_recent_snapshot" field
            3. Load change files generated from step 1
            4. Move processed files on S3

        next_run Duration:
            - default -> 60 mins
            - if table updated in-frequently -> 6 hours
                * when 0 change files are available to process
            - if backlog of change files exists -> 5 mins
                * when change files > half of max_change_files requested
        """
        try:
            next_run_secs = 60 * 60
            max_change_files = 10_000
            change_files = find_qlik_change_files(self.table, self.save_local, max_change_files)
            if len(change_files) == 0:
                next_run_secs = 60 * 60 * 6
            elif len(change_files) / max_change_files > 0.5:
                next_run_secs = 60 * 5

            self.load_snapshots()
            self.load_changes(change_files)
            self.move_objects()

        except RecentSnapshotError:
            self.start_kwargs["skipped_recent_snapshot"] = True

        finally:
            self.reset_tmpdir(make_new=False)

        return next_run_secs
