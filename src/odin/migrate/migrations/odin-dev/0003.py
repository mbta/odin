import os
import tempfile

import pyarrow.parquet as pq

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import upload_file
from odin.utils.aws.s3 import download_object
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import CUBIC_ODS_FACT_DATA
from odin.utils.logger import ProcessLog


def migration() -> None:
    """
    ODIN DEV Migration 0003.

    May 30, 2025

    This migration is to drop unwanted columns from the Cubic ODS Fact parquet files.

    There was an error in how ODS Fact records were being updated that introduced unwanted columns
    do the dataset parquet files on S3.

    This migration will go through every Cubic ODS Fact parquet file on S3 and iterate through the
    row groups to drop the unwanted columns and then replace the parquet file on S3.

    Process Steps:
    1. Check parquet file schema for drop_columns
        1.a If columns not found, continue to next file
    2. Alter schema of parquet file to remove drop_columns
    3. Download parquet file to local disk (for faster updating)
    4. Iterate through row groups of parquet file, removing drop_columns and writing to new file
    5. Upload newly created parquet file with drop_columns removed
    """
    fact_prefix = os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_FACT_DATA)

    drop_columns = [
        "header__year",
        "header__month",
        "header__change_oper",
        "header__timestamp",
        "header__from_csv",
        "snapshot",
    ]
    for obj in list_objects(fact_prefix, in_filter=".parquet"):
        pq_schema = pq.read_metadata(obj.path).schema.to_arrow_schema()
        file_drop_columns = list(set(pq_schema.names).intersection(set(drop_columns)))
        if len(file_drop_columns) == 0:
            continue

        ProcessLog("migration_003", pq_file=obj.path, file_drop_columns=",".join(file_drop_columns))

        # Create NEW schema for parquet file with columns dropped.
        write_schema = pq_schema
        for col in file_drop_columns:
            write_schema = write_schema.remove(write_schema.get_field_index(col))

        # Download original parquet file
        # Iterate through row groups and drop columns
        # Upload new parquet file
        with tempfile.TemporaryDirectory() as tmpdir:
            dl_path = os.path.join(tmpdir, "original.parquet")
            write_path = os.path.join(tmpdir, "new.parquet")
            download_object(obj.path, dl_path)
            pq_file = pq.ParquetFile(dl_path)
            writer = pq.ParquetWriter(
                write_path,
                schema=write_schema,
                compression="zstd",
                compression_level=3,
            )
            for rg_index in range(pq_file.num_row_groups):
                rg_table = pq_file.read_row_group(rg_index)
                rg_table = rg_table.drop_columns(file_drop_columns)
                writer.write_table(rg_table, row_group_size=rg_table.num_rows)
            writer.close()
            pq_file.close()
            upload_file(write_path, obj.path)
