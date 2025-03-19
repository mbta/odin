import os
import re
import shutil
import tempfile
from typing import List
from typing import Tuple
from datetime import datetime
from datetime import timedelta
from datetime import UTC

import polars as pl
import pyarrow.compute as pc

from odin.utils.logger import ProcessLog
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import DATA_ERROR
from odin.utils.locations import IN_QLIK_PREFIX
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import CUBIC_QLIK_DATA
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import S3Object
from odin.utils.aws.s3 import download_object
from odin.ingestion.qlik.dfm import dfm_from_s3
from odin.ingestion.qlik.dfm import dfm_to_polars_schema
from odin.ingestion.qlik.dfm import QlikDFM
from odin.utils.parquet import ds_from_path
from odin.utils.parquet import ds_column_min_max

SNAPSHOT_FMT = "%Y%m%dT%H%M%SZ"
RE_SNAPSHOT_TS = re.compile(r"(\d{8}T\d{6}Z)")
RE_CDC_TS = re.compile(r"(\d{8}-\d{9})")


class RecentSnapshotError(Exception):
    """
    Exception to raise if new Qlik snapshot was created within the last 6 hours.

    This is to be certain that snapshot process is fullly complete.
    """

    pass


class ReadSnapshotError(Exception):
    """Probably could not load Snapshot DFM file."""

    pass


def re_get_first(string: str, pattern: re.Pattern) -> str:
    """
    Pull first regex match from string.

    this should not fail and will raise if no match is found

    :param string: string to perform search on
    :param pattern: compiled regex pattern

    :return: first found pattern match
    """
    match = pattern.search(string)
    if match is None:
        raise LookupError(f"regex pattern({pattern.pattern}) not found in {string}")

    return match.group(0)


def find_qlik_load_files(table: str, save_local=bool) -> List[Tuple[str, QlikDFM]]:
    """
    Get sorted List of LOAD***.csv.gz from from bucket locations

    will be sorted by "startWriteTimestamp" of .dfm file associated with .csv.gz file.

    :param table: QLIK Table name

    :return: list of load file tuples sorted from oldest to newest
    """
    prefixes = (
        os.path.join(DATA_ARCHIVE, IN_QLIK_PREFIX, table),
        os.path.join(DATA_ERROR, IN_QLIK_PREFIX, table),
    )
    paths: List[Tuple[str, QlikDFM]] = []
    log = ProcessLog("find_qlik_load_files", table=table)
    try:
        for prefix in prefixes:
            in_func = None
            # If running locally, find last file processed to use as filter for S3 objects
            if save_local:
                try:
                    ds = ds_from_path(os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DATA, table))
                    filter = pc.starts_with(
                        pc.field("header__from_csv"), f"s3://{prefix}"
                    ) & pc.match_substring(pc.field("header__from_csv"), "LOAD")
                    _, min_path = ds_column_min_max(ds, "header__from_csv", ds_filter=filter)
                    if min_path is not None:

                        def filter_func(obj: S3Object) -> bool:
                            return obj.path > min_path

                        in_func = filter_func
                except Exception as _:
                    pass

            for obj in list_objects(f"{prefix}/", in_filter="LOAD", in_func=in_func):
                if not obj.path.endswith("csv.gz"):
                    continue
                if obj.last_modified > (datetime.now(tz=UTC) - timedelta(hours=6)):
                    raise RecentSnapshotError(f"{obj.path} modified with the last 6 hours.")
                paths.append((obj.path, dfm_from_s3(obj.path)))
        log.complete(num_load_files=len(paths))
    except RecentSnapshotError as exception:
        log.complete(skipped_recent_snapshot=True)
        raise exception
    except Exception as exception:
        log.failed(exception)
        raise exception

    return sorted(paths, key=lambda tup: tup[1]["fileInfo"]["startWriteTimestamp"])


def find_qlik_cdc_files(table: str, save_local: bool, max_cdc_files: int) -> List[S3Object]:
    """
    Get cdc .csv.gz from from bucket locations with no sorting.

    :param table: QLIK Table name
    :param save_local: Running process locally (with no file moves) requiers filtering S3 results

    :return: list of CDC .csv.gz files
    """
    cdc_table = f"{table}__ct/"
    prefixes = (
        os.path.join(DATA_ARCHIVE, IN_QLIK_PREFIX, cdc_table),
        os.path.join(DATA_ERROR, IN_QLIK_PREFIX, cdc_table),
    )
    paths = []
    for prefix in prefixes:
        in_func = None
        if save_local:
            try:
                ds = ds_from_path(os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DATA, table))
                filter = pc.starts_with(pc.field("header__from_csv"), f"s3://{prefix}")
                _, min_path = ds_column_min_max(ds, "header__from_csv", ds_filter=filter)
            except Exception:
                pass
            else:
                if min_path is not None:

                    def filter_func(obj: S3Object) -> bool:
                        return obj.path > min_path

                    in_func = filter_func

        paths += list_objects(
            prefix, in_filter=".csv.gz", max_objects=max_cdc_files, in_func=in_func
        )

    return paths


def snapshot_to_parquet(obj_path: str, dfm: QlikDFM, write_folder: str) -> str:
    """
    Convert snapshot csv file to parquet.

    Raise any exception and abort snapshot processing, all snapshot must be processed.

    :param object: path to snapshot object on S3
    :param dfm: QlikDFM for snapshot
    :param write_folder: folder that parquet file will be written to

    :return: path to written parquet file
    """
    log = ProcessLog("snapshot_to_parquet", obj_path=obj_path, write_folder=write_folder)
    try:
        dfm_dt = datetime.fromisoformat(dfm["fileInfo"]["startWriteTimestamp"])
        change_seq = dfm_dt.strftime("%Y%m%d%H%M%S").ljust(35, "0")
        schema = dfm_to_polars_schema(dfm)
        write_file = os.path.join(write_folder, f"{len(os.listdir(write_folder))}.parquet")
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpfile = os.path.join(tmpdir, "temp.csv")
            download_object(obj_path, tmpfile)
            pl.scan_csv(
                tmpfile,
                schema=schema,
                has_header=True,
            ).with_columns(
                pl.lit(dfm_dt.strftime("%Y")).cast(pl.Int32()).alias("header__year"),
                pl.lit(dfm_dt.strftime("%m")).cast(pl.Int32()).alias("header__month"),
                pl.lit(change_seq, dtype=pl.Decimal(35, 0)).alias("header__change_seq"),
                pl.lit("L").alias("header__change_oper"),
                pl.lit(dfm_dt).alias("header__timestamp"),
                pl.lit(obj_path).alias("header__from_csv"),
            ).sink_parquet(
                write_file,
                compression="zstd",
                compression_level=3,
                row_group_size=int(1024 * 1024 / (8 * schema.len())),
            )
        log.complete()

    except Exception as exception:
        log.failed(exception)
        raise exception

    return write_file


def cdc_csv_to_parquet(
    read_folder: str, write_folder: str
) -> Tuple[List[str], List[str], List[str]]:
    """
    Merge all csv subfolder files and create parquet files in write_folder.

    'read_folder' must contain "header" sub-folders.

    :param read_folder: local folder containing 'header' folders
    :param write_folder: local destination folder for parquet files create from merged csv files

    :return: (parquet_files_created, archive_objects, error_objects)
    """
    pq_written = []
    archive_objects = []
    error_objects = []
    for header_folder in os.listdir(read_folder):
        try:
            log = ProcessLog("cdc_csv_to_parquet", header_folder=header_folder)
            header_folder = os.path.join(read_folder, header_folder)
            merge_file = os.path.join(header_folder, "merge.csv")
            csv_paths = [os.path.join(header_folder, f) for f in os.listdir(header_folder)]
            csv_gz_objects = [
                f.replace("|", "/").replace(".csv", ".csv.gz") for f in os.listdir(header_folder)
            ]
            with open(merge_file, "wb") as fout:
                for csv_path in csv_paths:
                    with open(csv_path, "rb") as f:
                        fout.write(f.read())

            schema = dfm_to_polars_schema(
                dfm_from_s3(csv_gz_objects[0]),
                prefix={"header__from_csv": pl.String()},
            )
            pq_written.append(
                os.path.join(write_folder, f"{len(os.listdir(write_folder))}.parquet")
            )
            pl.scan_csv(
                merge_file,
                schema=schema,
                has_header=False,
            ).with_columns(
                pl.col("header__timestamp")
                .dt.strftime("%Y")
                .cast(pl.Int32())
                .alias("header__year"),
                pl.col("header__timestamp")
                .dt.strftime("%m")
                .cast(pl.Int32())
                .alias("header__month"),
            ).sink_parquet(
                pq_written[-1],
                compression="zstd",
                compression_level=3,
                row_group_size=int(1024 * 1024 / (8 * schema.len())),
            )
            shutil.rmtree(header_folder, ignore_errors=True)
            archive_objects += csv_gz_objects
            log.complete(pq_written=pq_written[-1], num_archive_objects=len(csv_gz_objects))

        except Exception as exception:
            error_objects += csv_gz_objects
            log.add_metadata(print_log=False, num_error_objects=len(csv_gz_objects))
            log.failed(exception)

    return (pq_written, archive_objects, error_objects)
