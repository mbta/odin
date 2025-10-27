import os
import shutil
from typing import Generator
from unittest.mock import patch

import pytest
import polars as pl

from odin.generate.data_dictionary.dictionary import generate_dictionary
from odin.utils.aws.s3 import S3Object

PQ_NUM_ROWS = 5_000
PQ_MAX_INT = 5_000


def mock_list_objects(path: str, **kwargs):
    """Mock list_objects for local call."""
    objects = []
    for dir, _, fnames in os.walk(path):
        objects += [
            S3Object(path=os.path.join(dir, f), size_bytes=0, last_modified="now") for f in fnames
        ]
    return objects


def mock_list_partitions(path: str):
    """Mock list_partitions for local call."""
    return [d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))]


@pytest.fixture(scope="module")
def pq_file(tmp_path_factory) -> Generator[str]:
    """Create temporary parquet files for testing."""
    tmp_path = tmp_path_factory.mktemp("dictionary_files", numbered=False)
    path = os.path.join(tmp_path, "group", "name", "year=2021", "month=5", "t1.parquet")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    (
        pl.DataFrame()
        .with_columns(
            pl.int_range(PQ_MAX_INT).sample(PQ_NUM_ROWS, with_replacement=True).alias("int_col"),
            pl.lit("strings").alias("string_col"),
            pl.lit(True).alias("bool_col"),
        )
        .write_parquet(path)
    )

    yield str(tmp_path)
    shutil.rmtree(tmp_path)


@patch("odin.generate.data_dictionary.dictionary.list_objects", mock_list_objects)
@patch("odin.generate.data_dictionary.dictionary.list_partitions", mock_list_partitions)
def test_generate_dictionary(pq_file):
    """Test generate_dictionary function"""
    expected_dictionary = [
        {
            "child_files": [
                f"{pq_file}/group/name/year=2021/month=5/t1.parquet",
            ],
            "dataset_group": "group",
            "dataset_name": "name",
            "dataset_path": f"{pq_file}/group/name",
            "schema": [
                {
                    "column_name": "int_col",
                    "column_type": "int64",
                    "is_partition": False,
                    "column_description": None,
                },
                {
                    "column_name": "string_col",
                    "column_type": "large_string",
                    "is_partition": False,
                    "column_description": None,
                },
                {
                    "column_name": "bool_col",
                    "column_type": "bool",
                    "is_partition": False,
                    "column_description": None,
                },
                {
                    "column_name": "year",
                    "column_type": "int32",
                    "is_partition": True,
                    "column_description": None,
                },
                {
                    "column_name": "month",
                    "column_type": "int32",
                    "is_partition": True,
                    "column_description": None,
                },
            ],
            "size_mb": 0,
        },
    ]
    with patch("odin.generate.data_dictionary.dictionary.ODIN_ROOT", pq_file):
        data_dictionary = [d for d in generate_dictionary(pq_file)]
        assert data_dictionary == expected_dictionary


@patch("odin.generate.data_dictionary.dictionary.list_objects", mock_list_objects)
@patch("odin.generate.data_dictionary.dictionary.list_partitions", mock_list_partitions)
def test_generate_dictionary_schema_with_descriptions(pq_file):
    column_descriptions = {
        "name": {
            "int_col": "Column containing ints",
            "string_col": "Column containing strings",
        },
    }
    with patch("odin.generate.data_dictionary.dictionary.ODIN_ROOT", pq_file):
        result = [d for d in generate_dictionary(pq_file, column_descriptions_by_table=column_descriptions)]
    schema = {field["column_name"]: field["column_description"] for field in result[0]["schema"]}
    assert schema["int_col"] == "Column containing ints"
    assert schema["string_col"] == "Column containing strings"
    assert schema["bool_col"] is None
