import os
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import upload_file
from odin.utils.aws.s3 import download_object
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import CUBIC_ODS_FACT_DATA
from odin.utils.locations import CUBIC_QLIK_DATA
from odin.utils.logger import ProcessLog


def migration() -> None:
    """
    ODIN PROD Migration 0006.

    July 1, 2025

    This migration is to change the type of the `purse_type` column of the REASON_DIMENSION table.

    Process Steps:
    1. Iterate through REASON_DIMENSION parquet files
    2. Alter schema of parquet file to change `purse_type` to string
    3. Download parquet file to local disk (for faster updating)
    4. Iterate through row groups of parquet file, casting the new schema
    5. Upload newly created parquet file
    """
    prefixes = (
        os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DATA, "EDW.REASON_DIMENSION", ""),
        os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_FACT_DATA, "EDW.REASON_DIMENSION", ""),
    )

    for prefix in prefixes:
        for obj in list_objects(prefix, in_filter=".parquet"):
            pq_schema = pq.read_metadata(obj.path).schema.to_arrow_schema()

            ProcessLog("migration_006", pq_file=obj.path)

            # Create CAST schema for parquet file with purse_type fix.
            cast_schema = pq_schema.set(
                pq_schema.get_field_index("purse_type"),
                pa.field("purse_type", pa.large_string()),
            )

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
                    schema=cast_schema,
                    compression="zstd",
                    compression_level=3,
                )
                for rg_index in range(pq_file.num_row_groups):
                    rg_table = pq_file.read_row_group(rg_index)
                    rg_table = rg_table.cast(cast_schema)
                    writer.write_table(rg_table, row_group_size=rg_table.num_rows)
                writer.close()
                pq_file.close()
                upload_file(write_path, obj.path)
