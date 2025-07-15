import os
import sched
from pathlib import Path


from odin.job import job_proc_schedule
from odin.utils.runtime import sigterm_check
from odin.utils.logger import ProcessLog
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import AFC_RESTRICTED
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import download_object
from odin.utils.aws.s3 import upload_file
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import pq_dataset_writer

import polars as pl
from odin.ingestion.afc.afc_archive import ArchiveAFCAPI


class RestrictedAFC(ArchiveAFCAPI):
    """Custom class for AFC Restricted Table"""

    def sync_parquet(self) -> None:
        """Convert json to parquet and sync with S3 files."""
        log = ProcessLog("afc_api_sync_parquet")
        sync_paths = []
        rs_sync_paths = []
        for json_file in sorted(Path(self.tmpdir).iterdir(), key=os.path.getmtime):
            if json_file.suffix != ".json":
                continue
            lf = pl.scan_ndjson(
                json_file.absolute(),
                schema=self.schema,
            ).with_columns(pl.col(self.ts_cols).str.head(19).str.to_datetime("%Y-%m-%d %H:%M:%S"))

            # create public parquet file
            pq_path = str(json_file.absolute()).replace(".json", ".parquet")
            lf.drop("engraveid").sink_parquet(
                pq_path,
                compression="zstd",
                compression_level=3,
                row_group_size=int(1024 * 1024 / (8 * self.schema.len())),
            )

            # create restricted lookup file
            rs_pq_path = str(json_file.absolute()).replace(".json", "_restricted.parquet")
            lf.select(["mediumid", "engraveid"]).unique().sink_parquet(
                rs_pq_path,
                compression="zstd",
                compression_level=3,
                row_group_size=int(1024 * 1024 / (8 * self.schema.len())),
            )

            json_file.unlink()
            pq_row_count = ds_from_path(pq_path).count_rows()
            if pq_row_count > 0:
                sync_paths.append(pq_path)
                rs_sync_paths.append(rs_pq_path)
                log.add_metadata(pq_path=pq_path, pq_row_count=pq_row_count)

        if len(sync_paths) == 0:
            return

        export_jobs = (
            (self.export_folder, sync_paths),
            (os.path.join(DATA_SPRINGBOARD, AFC_RESTRICTED, self.table), rs_sync_paths),
        )
        new_paths: list[str] = []
        for exp_fldr, read_paths in export_jobs:
            found_objs = list_objects(f"s3://{exp_fldr}", in_filter=".parquet")
            if found_objs:
                sync_file = found_objs[-1].path.replace("s3://", "")
                s3_pq_file = os.path.join(self.tmpdir, sync_file.replace("/table_", "/temp_"))
                download_object(found_objs[-1].path, s3_pq_file)
                read_paths.insert(0, s3_pq_file)

            # Create new merged parquet file(s)
            new_paths += pq_dataset_writer(
                source=ds_from_path(read_paths),
                export_folder=os.path.join(self.tmpdir, exp_fldr),
                export_file_prefix="table",
            )

        # Check for sigterm before upload (can't be un-done)
        sigterm_check()
        for new_path in new_paths:
            move_path = new_path.replace(f"{self.tmpdir}/", "")
            upload_file(new_path, move_path)
        log.complete()


def schedule_restricted_afc_archive(schedule: sched.scheduler) -> None:
    """
    Schedule job for Restricted AFC API Archive process.

    :param schedule: application scheduler
    """
    job = RestrictedAFC("v_media")
    schedule.enter(0, 1, job_proc_schedule, (job, schedule))
