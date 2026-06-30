import os
import json
import sched
from dataclasses import dataclass
from dataclasses import asdict
from typing import Generator
from typing import Any
import urllib3

import pyarrow as pa
import pyarrow.parquet as pq

from odin.job import OdinJob
from odin.job import job_proc_schedule
from odin.utils.aws.ecs import AWS_ENV
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import ODIN_DATA
from odin.utils.locations import ODIN_DICTIONARY
from odin.utils.logger import ProcessLog
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import list_partitions
from odin.utils.aws.s3 import upload_file
from odin.generate.data_dictionary.duck_db import create_fares_db

NEXT_RUN_DEFAULT = 60 * 60 * 24  # 24 hours
ODIN_ROOT = f"s3://{os.path.join(DATA_SPRINGBOARD, ODIN_DATA)}"
AFC_ROOT = os.getenv("AFC_ROOT", "")


def fetch_AFC_column_descriptions() -> dict[str, dict[str, str | None]]:
    """Fetch column descriptions from AFC tableinfos endpoint."""
    log = ProcessLog("fetch_AFC_column_descriptions")

    if not AFC_ROOT:
        return {}

    headers = {
        "client_id": os.getenv("AFC_API_CLIENT_ID", ""),
        "client_secret": os.getenv("AFC_API_CLIENT_SECRET", ""),
    }

    try:
        pool = urllib3.PoolManager(
            headers=headers,
            timeout=urllib3.Timeout(total=60),
            retries=False,
        )
        response = pool.request("GET", f"{AFC_ROOT}/tableinfos")

        if response.status != 200:
            return {}

        column_descriptions: dict[str, dict[str, str | None]] = {}
        for table_dict in response.json():
            for table_name, table_info in table_dict.items():
                table_columns = table_info.get("table_infos", [])
                column_descriptions[table_name] = {}
                for col in table_columns:
                    column_name = col.get("column_name")
                    if column_name:
                        remarks = col.get("remarks")
                        column_descriptions[table_name][column_name] = remarks

        return column_descriptions

    except Exception as exception:
        log.failed(exception=exception)
        return {}


@dataclass
class DatasetField:
    """Values exepcted for DatasetField"""

    column_name: str
    column_type: str
    is_partition: bool
    column_description: str | None

    def __init__(self, column: pa.Field, description: str | None = None) -> None:
        """Create DatasetField from pyarrow field."""
        self.column_type = str(column.type)
        if isinstance(column.type, pa.DictionaryType):
            self.column_type = str(column.type.value_type)
        self.column_name = column.name
        # in testing DictionaryType is always partition column, further testing may change this...
        self.is_partition = isinstance(column.type, pa.DictionaryType)
        self.column_description = description


@dataclass
class DatasetDictionary:
    """Values expected for DatasetDictionary"""

    dataset_group: str
    dataset_name: str
    dataset_path: str
    size_mb: int
    child_files: list[str]
    schema: list[DatasetField]

    def __init__(
        self,
        ds_path: str,
        column_descriptions: dict[str, str | None] | None = None,
    ) -> None:
        """Create DatasetDictionary from datset path."""
        data_objects = list_objects(ds_path, in_filter=".parquet")
        self.dataset_group = os.path.dirname(ds_path).replace(ODIN_ROOT, "").strip("/")
        self.dataset_name = os.path.basename(ds_path)
        self.dataset_path = ds_path
        self.size_mb = int(sum([obj.size_bytes for obj in data_objects]) / (1024 * 1024))
        self.child_files = [obj.path for obj in data_objects]
        if not column_descriptions:
            column_descriptions = {}
        self.schema = [
            DatasetField(c, column_descriptions.get(c.name))
            for c in pq.ParquetDataset(ds_path).schema
        ]


def generate_dictionary(
    path: str,
    column_descriptions_by_table: dict[str, dict[str, str | None]] | None = None,
) -> Generator[dict[str, Any]]:
    """Recursively generate DatabaseDictionary objects."""
    path_parts = list_partitions(path)
    if len(path_parts) == 0 or "=" in "".join(path_parts):
        if list_objects(path, in_filter=".parquet"):
            dataset_name = os.path.basename(path)
            if not column_descriptions_by_table:
                column_descriptions_by_table = {}
            column_descriptions = column_descriptions_by_table.get(dataset_name)
            yield asdict(DatasetDictionary(ds_path=path, column_descriptions=column_descriptions))
    else:
        for part in path_parts:
            yield from generate_dictionary(os.path.join(path, part), column_descriptions_by_table)


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
        column_descriptions = fetch_AFC_column_descriptions()
        data_dictionary = [
            d
            for d in generate_dictionary(
                ODIN_ROOT,
                column_descriptions_by_table=column_descriptions,
            )
        ]

        json_tmp = os.path.join(self.tmpdir, "temp.json")
        with open(json_tmp, "w") as json_writer:
            json.dump(data_dictionary, json_writer)

        dd_obj_name = "data_dictionary.json"
        if AWS_ENV.lower() != "prod":
            dd_obj_name = f"data_dictionary_{AWS_ENV}.json"
        upload_path = os.path.join(DATA_SPRINGBOARD, ODIN_DICTIONARY, dd_obj_name)
        upload_file(json_tmp, upload_path)

        # Create and upload DuckDB access file.
        db_file = create_fares_db(self.tmpdir)
        db_obj_name = os.path.basename(db_file)
        if AWS_ENV.lower() != "prod":
            db_obj_name = db_obj_name.replace(".db", f"_{AWS_ENV}.db")
        upload_path = os.path.join(DATA_SPRINGBOARD, ODIN_DICTIONARY, db_obj_name)
        upload_file(db_file, upload_path)

        return NEXT_RUN_DEFAULT


def schedule_dictionary(schedule: sched.scheduler) -> None:
    """
    Schedule DataDictionary process.

    :param schedule: application scheduler
    """
    job = DataDictionary()
    schedule.enter(0, 1, job_proc_schedule, (job, schedule))
