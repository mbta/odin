import os
import json
import sched
from dataclasses import dataclass
from dataclasses import asdict
from typing import Generator
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from odin.job import OdinJob
from odin.job import job_proc_schedule
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import ODIN_DATA
from odin.utils.locations import ODIN_DICTIONARY
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import list_partitions
from odin.utils.aws.s3 import upload_file
from odin.generate.data_dictionary.duck_db import create_fares_db

NEXT_RUN_DEFAULT = 60 * 60 * 24  # 24 hours
ODIN_ROOT = f"s3://{os.path.join(DATA_SPRINGBOARD, ODIN_DATA)}"


@dataclass
class DatasetField:
    """Values exepcted for DatasetField"""

    column_name: str
    column_type: str
    is_partition: bool

    def __init__(self, column: pa.Field) -> None:
        """Create DatasetField from pyarrow field."""
        self.column_type = str(column.type)
        if isinstance(column.type, pa.DictionaryType):
            self.column_type = str(column.type.value_type)
        self.column_name = column.name
        # in testing DictionaryType is always partition column, futher testing may change this...
        self.is_partition = isinstance(column.type, pa.DictionaryType)


@dataclass
class DatasetDictionary:
    """Values expected for DatasetDictionary"""

    dataset_group: str
    dataset_name: str
    dataset_path: str
    size_mb: int
    child_files: list[str]
    schema: list[DatasetField]

    def __init__(self, ds_path: str) -> None:
        """Create DatasetDictionary from datset path."""
        data_objects = list_objects(ds_path, in_filter=".parquet")
        self.dataset_group = os.path.dirname(ds_path).replace(ODIN_ROOT, "").strip("/")
        self.dataset_name = os.path.basename(ds_path)
        self.dataset_path = ds_path
        self.size_mb = int(sum([obj.size_bytes for obj in data_objects]) / (1024 * 1024))
        self.child_files = [obj.path for obj in data_objects]
        self.schema = [DatasetField(c) for c in pq.ParquetDataset(ds_path).schema]


def generate_dictionary(path: str) -> Generator[dict[str, Any]]:
    """Recursively generate DatabaseDictionary objects."""
    path_parts = list_partitions(path)
    if len(path_parts) == 0 or "=" in "".join(path_parts):
        if list_objects(path, in_filter=".parquet"):
            yield asdict(DatasetDictionary(ds_path=path))
    else:
        for part in path_parts:
            yield from generate_dictionary(os.path.join(path, part))


class DataDictionary(OdinJob):
    """Generate Data Dictionary for Odin Springboard partition."""

    def run(self) -> int:
        """
        Create ODIN Data Dictionary exports.

        Dictionary format for each dataset entry:
        {
            "child_files": list[S3 objects in dataset]
            "dataset_group": common path stripped of ODIN_ROOT and dataset_name
            "dataset_name": prefix immedieatly before start of dataset
            "datasetpath": ODIN_ROOT + dataset_group + dataset_name
            "schema": List[
                {
                    "column_name": name
                    "column_type": type
                    "is_partition": column derived for S3 partition
                },
            ]
        }
        """
        # Create and upload Data Dictionary json file.
        data_dictionary = [d for d in generate_dictionary(ODIN_ROOT)]

        json_tmp = os.path.join(self.tmpdir, "temp.json")
        with open(json_tmp, "w") as json_writer:
            json.dump(data_dictionary, json_writer)

        upload_path = os.path.join(DATA_SPRINGBOARD, ODIN_DICTIONARY, "data_dictionary.json")
        upload_file(json_tmp, upload_path)

        # Create and upload DuckDB access file.
        db_file = create_fares_db(self.tmpdir)
        upload_file(
            db_file, os.path.join(DATA_SPRINGBOARD, ODIN_DICTIONARY, os.path.basename(db_file))
        )

        return NEXT_RUN_DEFAULT


def schedule_dictionary(schedule: sched.scheduler) -> None:
    """
    Schedule DataDictionary process.

    :param schedule: application scheduler
    """
    job = DataDictionary()
    schedule.enter(0, 1, job_proc_schedule, (job, schedule))
