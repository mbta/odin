import os
import shutil
from typing import Generator
from unittest.mock import MagicMock

import polars as pl
import pyarrow as pa
import pytest
from tableauhyperapi import SqlType

from odin.ingestion.tableau.tableau_upload import (
    convert_parquet_dtype,
    parquet_schema_to_hyper_definition,
    build_hyper_from_parquet,
    resolve_project_id,
    TABLE_CONFIG,
)

# Create fixtures (sample data and mock server)
@pytest.fixture(scope="module")
def sample_parquet_file(tmp_path_factory) -> Generator[str, None, None]:
    """
    Create temporary parquet file for testing
    """
    # Create a temporary directory that persists for all tests in this module
    tmp_dir = tmp_path_factory.mktemp("parquet_test")
    file_path = os.path.join(tmp_dir, "test_data.parquet")

    # Create a sample DataFrame with various column types to test type conversion
    df = pl.DataFrame(
        {
            "stop_id": [1, 2, 3, 4, 5],  # Integer column (will be Int64)
            "line": ["Red", "Orange", "Green", "Blue", "Bus"],  # String column
            "speed": [40.50, 50.75, 10.25, 20.00, 15.99],  # Float column
            "is_active": [True, False, True, True, False],  # Boolean column
            "last_train_date": pl.Series(  # Date column
                ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"]
            ).str.to_date(),
        }
    )

    # Write the DataFrame to a parquet file
    df.write_parquet(file_path)

    # `yield` returns the value to the test, then continues with cleanup after test completes
    yield file_path

    # Remove the temporary directory and all its contents
    # This runs after all tests using this fixture have completed
    shutil.rmtree(tmp_dir)


@pytest.fixture
def mock_tableau_server():
    """
    Create mock Tableau Server client for testing publish operations
    """
    # Create a mock server object that mimics tableauserverclient.Server
    mock_server = MagicMock()

    # Set up the mock to return project data when projects.get() is called
    # This simulates the Tableau server having a project hierarchy
    mock_project_parent = MagicMock()
    mock_project_parent.name = "Technology Innovation"
    mock_project_parent.id = "parent-123"
    mock_project_parent.parent_id = None  # Top-level project

    mock_project_child = MagicMock()
    mock_project_child.name = "Odin"
    mock_project_child.id = "child-456"
    mock_project_child.parent_id = "parent-123"  # Child of Technology Innovation

    mock_project_specific = MagicMock()
    mock_project_specific.name = "specific_project"
    mock_project_specific.id = "child-789"
    mock_project_specific.parent_id = "child-456"  # Child of Technology Innovation

    # Configure projects.get() to return our mock projects
    mock_server.projects.get.return_value = (
        [mock_project_parent, mock_project_child, mock_project_specific],
        None,  # Pagination info (not used in our tests)
    )

    # Create a mock authentication object
    mock_auth = MagicMock()

    return mock_server, mock_auth


# Testing parquet -> Tableau Hyper SQL type mapping
def test_convert_parquet_dtype_all_types():
    """Test all parquet -> Hyper type conversions in a single test."""
    test_cases = [
        (pa.int32(), SqlType.int(), "int32"),
        (pa.int64(), SqlType.big_int(), "int64"),
        (pa.bool_(), SqlType.bool(), "bool"),
        (pa.float64(), SqlType.double(), "float64"),
        (pa.date32(), SqlType.date(), "date32"),
        (pa.timestamp("us"), SqlType.timestamp(), "timestamp"),
        (pa.string(), SqlType.text(), "string"),
    ]

    failures = []
    for input_dtype, expected_sql_type, type_name in test_cases:
        result = convert_parquet_dtype(input_dtype)
        if result != expected_sql_type:
            failures.append(f"{type_name}: expected {expected_sql_type}, got {result}")

    # Report all failures at once
    assert not failures, "Type conversion failures:\n" + "\n".join(failures)


def test_convert_parquet_dtype_unknown_type():
    """
    Test that unknown/unsupported data types fall back to text
    Unsupported types should return SqlType.text() as a default
    """
    unusual_type = pa.decimal128(precision=7, scale=3)

    result = convert_parquet_dtype(unusual_type)
    assert result == SqlType.text()


def test_parquet_schema_to_hyper_definition(sample_parquet_file):
    """
    Test creating a Tableau Hyper table definition from a parquet file schema.

    This test verifies that:
    1. The function reads the parquet schema correctly
    2. Column names are preserved
    3. The table name is formatted correctly
    """
    # Call the function with our test parquet file
    table_name = "TEST.TABLE ONE"
    table_def = parquet_schema_to_hyper_definition(sample_parquet_file, table_name)

    # Verify the table name is formatted correctly
    # The function should prefix with "public." and replace spaces with underscores
    table_name_str = str(table_def.table_name)
    assert "public" in table_name_str
    assert "TEST.TABLE_ONE" in table_name_str

    # Get the column names from the table definition
    # Note: TableDefinition columns have Name objects whose str() includes quotes
    # We use the unescaped property to get the plain name
    column_names = [col.name.unescaped for col in table_def.columns]

    # Verify all expected columns are present
    expected_columns = ["stop_id", "line", "speed", "is_active", "last_train_date"]
    for expected_col in expected_columns:
        assert expected_col in column_names, f"Column '{expected_col}' not found in table definition"


def test_build_hyper_from_parquet(sample_parquet_file, tmp_path):
    """
    Test building a Tableau Hyper extract from a parquet file. This test:

    1. Creates a table definition from the parquet schema
    2. Builds a Hyper file from the parquet data
    3. Verifies the row count matches the source data
    """
    # Create the output path for the Hyper file
    hyper_file = os.path.join(tmp_path, "test_output.hyper")

    # Get the table definition from the parquet schema
    table_def = parquet_schema_to_hyper_definition(sample_parquet_file, "TEST")

    # Build the Hyper file
    row_count = build_hyper_from_parquet(sample_parquet_file, hyper_file, table_def)

    # Verify the row count matches our test data (5 rows)
    assert row_count == 5, f"Expected 5 rows, but got {row_count}"

    # Verify the Hyper file was created
    assert os.path.exists(hyper_file), "Hyper file was not created"


def test_resolve_project_id(mock_tableau_server):
    """
    The resolve_project_id function should be able to take a nested project path 
    like "Technology Innovation/Odin" and return the ID of the "Odin" project
    by iterating through the mock project hierarchy
    """
    mock_server, _ = mock_tableau_server

    # Test resolving a top-level project
    project_id = resolve_project_id(mock_server, "Technology Innovation")
    assert project_id == "parent-123"

    # Test resolving a nested project path
    project_id = resolve_project_id(mock_server, "Technology Innovation/Odin")
    assert project_id == "child-456"

    # Test resolving a multi-level nested project path
    project_id = resolve_project_id(mock_server, "Technology Innovation/Odin/specific_project")
    assert project_id == "child-789"


def test_table_config_structure():
    """
    Test that TABLE_CONFIG entries have the required structure
    """
    required_keys = {"casts", "drops", "index_column"}

    for table_name, config in TABLE_CONFIG.items():
        # Verify all required keys are present
        assert set(config.keys()) == required_keys, (
            f"Table {table_name} is missing required config keys. "
            f"Expected {required_keys}, got {set(config.keys())}"
        )

        # Verify types
        assert isinstance(config["casts"], dict), (
            f"Table {table_name}: 'casts' should be a dict"
        )
        assert isinstance(config["drops"], set), (
            f"Table {table_name}: 'drops' should be a set"
        )
        assert config["index_column"] is None or isinstance(config["index_column"], str), (
            f"Table {table_name}: 'index_column' should be None or str"
        )
