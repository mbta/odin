import os
import re
from typing import Tuple
from typing import Iterable
from typing import Sequence
from typing import Union
from typing import Optional
from typing import List
from typing import Any
from typing import Literal
from typing import TypedDict
from functools import reduce
from operator import gt
from operator import lt
from operator import and_
from operator import attrgetter
from itertools import chain

import polars as pl
from polars._typing import SchemaDict
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pyarrow.dataset as pd
import pyarrow.compute as pc
import pyarrow.acero as ac

from odin.utils.logger import ProcessLog
from odin.utils.aws.s3 import list_objects


def polars_decimal_as_string(df: pl.DataFrame) -> Tuple[SchemaDict, SchemaDict]:
    """
    Create polars dtype mapping for Decimal to String type cast.

    Polars can not currently perform join operations on Decimal datatypes. So when trying to perform
    .update or .join operations on Polars dataframes Decimal types must be cast to string.
    This helper function will produce a type mapping for Decimal types to String and a reverse map.

    :param df: DataFrame with columns that will be mapped from Decimal -> String

    :return: (Decimal->String map, String->Decimal map)
    """
    mod_cast = {}
    orig_cast = {}
    for column in df.columns:
        if isinstance(df.schema[column], pl.Decimal):
            mod_cast[column] = pl.String()
            orig_cast[column] = df.schema[column]
    return (mod_cast, orig_cast)


def pq_rows_and_bytes(path: str, num_rows: Optional[int] = None) -> Tuple[int, int]:
    """
    Retrieve number of rows and size, in bytes, of parquet file.

    :param path: path to parquet file (local or S3), if S3 must start with s3://
    :param num_rows: (Optional) explicit row count, otherwise will read from file metadata

    :return: (num_rows, size_in_bytes)
    """
    log = ProcessLog("pq_rows_and_bytes", path=path)
    if num_rows is None:
        num_rows = pq.read_metadata(path).num_rows

    if path.startswith("s3://"):
        s3_obj = list_objects(path, max_objects=1)
        assert len(s3_obj) == 1
        size_bytes = s3_obj[0].size_bytes
    else:
        size_bytes = os.path.getsize(path)

    log.complete(num_rows=num_rows, size_bytes=size_bytes)
    return (num_rows, size_bytes)


def pq_rows_per_mb(source: Union[str, Sequence[str]], num_rows: Optional[int] = None) -> int:
    """
    Approximate the number of rows that are equivalent to 1MB on disk for parquet file(s).

    Floor is a minimum of 1000 rows.

    :param source: parquet file path(s) on local disk or S3, if S3 must start with s3://
    :param num_rows: (Optional) explicit row count, otherwise will read from file metadata

    :return: approximate number of rows equal to 1MB of parquet disk space
    """
    log = ProcessLog("pq_rows_per_mb")
    paths = []
    if isinstance(source, str):
        if not source.endswith(".parquet"):
            # assume S3 partition
            paths += [obj.path for obj in list_objects(source)]
        else:
            paths.append(source)
        log.add_metadata(source=source)
    elif isinstance(source, Sequence):
        paths += source
        log.add_metadata(num_sources=len(source))

    total_rows = 0
    total_mbs = 0.0
    for path in paths:
        _rows, _bytes = pq_rows_and_bytes(path, num_rows=num_rows)
        total_rows += _rows
        total_mbs += _bytes / (1024 * 1024)

    rows_per_mb = max(1_000, int(total_rows / total_mbs))
    log.complete(rows_per_mb=rows_per_mb)

    return rows_per_mb


class RowGroupStats(TypedDict):
    """Incomplete representation of Stats fields, but only ones currently used."""

    has_min_max: bool
    max: Any
    min: Any
    null_count: int
    num_values: int


def file_column_stats(pq_meta: pq.FileMetaData, column: str) -> list[RowGroupStats]:
    """
    Retrieve 'column' statistics from metadata of parquet file.

    :param pq_meta: Metadata of parquet file.
    :param column: Name of column to retrieve stats for.

    :return: List of RowGroupStats, each List index correspends File RowGroup of same index.
    """
    col_index = pq_meta.schema.to_arrow_schema().get_field_index(column)
    assert col_index >= 0
    file_stats = []
    for rg_index in range(pq_meta.num_row_groups):
        rg_stats: RowGroupStats = (
            pq_meta.row_group(rg_index).column(col_index).to_dict()["statistics"]  # type: ignore[assignment]
        )
        file_stats.append(rg_stats)

    return file_stats


def row_group_column_stats(rg_metadata: pq.RowGroupMetaData, column: str) -> RowGroupStats:
    """
    Retrieve 'column' statistics from metadata of row group.

    :param rg_metadata: Metadata of parquet row group.
    :param column: Name of column to retrieve stats for.

    :return: Dictionary of column stats for row group.
    """
    rg_dict = rg_metadata.to_dict()
    for col in rg_dict["columns"]:
        if col["path_in_schema"] != column:  # type: ignore[index]
            continue
        return col["statistics"]  # type: ignore[index]

    raise IndexError(f"{column} not round in row group metadata")


def pq_path_partitions(path: str) -> List[Tuple[str, str]]:
    """
    Extract partition columns for parquet file path.

    :param path: path of parquet file

    :return: List of (column_name, column_value) tuples extracted from 'path'
    """
    file_partitions = []
    for part in path.split("/"):
        if "=" in part:
            col, value = part.split("=", 1)
            file_partitions.append((col, value))
    return file_partitions


def ds_files(ds: pd.UnionDataset) -> List[str]:
    """
    Produce list of paths found in dataset.

    :param ds: dataset to extract file paths from

    :return: List of all paths in dataset
    """
    log = ProcessLog("ds_files")
    paths = []
    children = ds.children
    for child in children:
        if isinstance(child, pd.FileSystemDataset):
            if isinstance(child.filesystem, pafs.S3FileSystem):
                paths.append([f"s3://{f}" for f in child.files])
            else:
                paths.append(child.files)
    return_paths = list(chain.from_iterable(paths))
    log.complete(num_paths=len(return_paths))
    return return_paths


def ds_from_path(source: Union[str, Sequence[str]]) -> pd.UnionDataset:
    """
    Create pyarrow Dataset from parquet path(s). If multiple paths, schemas must be unionable.

    :param source: parquet file path(s) on local disk or S3, if S3 must start with s3://

    :return: pyarrow Dataset of path(s) with "unionable" schema
    """
    log = ProcessLog("ds_from_path")
    paths = []
    if isinstance(source, str):
        # single parquet file
        if source.endswith(".parquet"):
            paths.append(source)
        # S3 partition path
        elif source.startswith("s3://"):
            paths = [o.path for o in list_objects(source, in_filter=".parquet")]
        # local partition path
        else:
            for dir, _, files in os.walk(source):
                paths += [os.path.join(dir, f) for f in files if f.endswith(".parquet")]
    elif isinstance(source, Sequence):
        paths = [f for f in source if f.endswith(".parquet")]
    log.add_metadata(num_source=len(paths), paths=",".join(paths))
    ds = pd.dataset([pd.dataset(part, partitioning="hive", format="parquet") for part in paths])
    log.complete(num_sources=len(paths))
    return ds


def ds_column_min_max(
    ds: pd.Dataset,
    column: str,
    ds_filter: Optional[pd.Expression] = None,
    filter_cols: Optional[List[str]] = None,
) -> Tuple[Any, Any]:
    """
    Get min & max value of column from pyarrow Dataset.

    :param ds: pyarrow.Dataset to scan
    :param column: column to query
    :param ds_filter: (Optional) Expression filter to apply during scan
    :param filter_cols: (Optional) Any columns, that are not 'column', used in 'ds_filter'
                        Expression. Not including these will nullify 'ds_filter'
    :return: (min, max)
    """
    if filter_cols is None:
        filter_cols = []
    scan_cols = list(set([column] + filter_cols))
    agg_col = "__agg_result"
    scan_node = ac.ScanNodeOptions(ds, columns=scan_cols, batch_readahead=0, fragment_readahead=1)  # type: ignore[attr-defined]
    declarations = [ac.Declaration("scan", scan_node)]
    if ds_filter is not None:
        declarations.append(ac.Declaration("filter", ac.FilterNodeOptions(ds_filter)))
    declarations.append(
        ac.Declaration(
            "aggregate",
            ac.AggregateNodeOptions(
                aggregates=[(column, "min_max", pc.ScalarAggregateOptions(), agg_col)],  # type: ignore[list-item]
                keys=None,
            ),
        )
    )
    log = ProcessLog("ds_column_min_max", column=column)
    try:
        r = ac.Declaration.from_sequence(declarations).to_table().column(agg_col).to_pylist()[0]
        log.complete()
    except Exception as exception:
        log.failed(exception)
        raise exception

    return (r["min"], r["max"])  # type: ignore[assignment,index]


def ds_metadata_min_max(ds: pd.UnionDataset, column: str) -> Tuple[Any, Any]:
    """
    Get min & max value of column from Dataset metadata.

    This is a very fast and efficient way to get min/max from large dataset with accurate
    metadata statistics.

    If the `column` contains all NULL values, return values will be `None`.

    If the `column` is a path partition, return values will be type str.

    :param ds: pyarrow.Dataset to scan
    :param column: column to query

    :return: (min, max)
    """
    log = ProcessLog("ds_metadata_min_max", column=column)
    column_mins = []
    column_maxs = []
    try:
        for child in ds.children:
            # check if column in ds partition
            joined_files = ",".join(child.files)  # type: ignore[attr-defined]
            if f"/{column}=" in joined_files:
                column_re = re.compile(rf"\/{column}=([^\/]*)\/")
                column_parts = column_re.findall(joined_files)
                column_mins += column_parts
                column_maxs += column_parts
                continue
            for frag in child.get_fragments():
                metadata: pq.FileMetaData = frag.metadata
                if column in ds.schema.names and column not in metadata.schema.names:
                    # column in dataset but not fragment file
                    continue
                for col_stats in file_column_stats(metadata, column):
                    if col_stats["min"] is not None:
                        column_mins.append(col_stats["min"])
                    if col_stats["max"] is not None:
                        column_maxs.append(col_stats["max"])
        col_min = None
        if column_mins:
            col_min = min(column_mins)
        col_max = None
        if column_maxs:
            col_max = max(column_maxs)
        log.complete()
    except Exception as exception:
        log.failed(exception)
        raise exception

    return (col_min, col_max)


def fast_last_mod_ds_max(partition: str, column: str) -> Any:
    """
    Find max value of column from the file of parquet partition that was most recently modified.

    This is useful for very large datasets where it is guaranteed that the max value of a column
    will be in the most recently modified file of the partition.

    TODO: make this work locally as well??

    :param partition: S3 partition that will be used to find most recently modified file.
    :param column: Column to pull max from.

    :return: column max value
    """
    log = ProcessLog("fast_last_mod_ds_max", partition=partition, column=column)
    part_objs = list_objects(partition, in_filter=".parquet")
    if len(part_objs) == 0:
        raise IndexError(f"No parquet files found in S3 partition: {partition}")
    last_mod_path = sorted(part_objs, key=attrgetter("last_modified"))[-1].path
    _, max = ds_metadata_min_max(ds_from_path(last_mod_path), column=column)
    log.complete()

    return max


def ds_limit_k_sorted(
    ds: pd.Dataset,
    sort_column: str,
    batch_size: int,
    sort_direction: Literal["ascending", "descending"] = "ascending",
    max_nbytes: int = 256 * 1024 * 1024,
) -> pl.DataFrame:
    """
    Produce limited number of sorted results from large parquet dataset.

    This function scans very large parquet datsets and produces sorted results in a memory
    constrained environment. Currently only supports one sort column.

    :param ds: Dataset to scan, pre-filtered if possible.
    :param sort_column: Column to sort results on.
    :param sort_direction: "ascending" or "descending".
    :param max_nbytes: Max bytes (reported by RecordBatch.nbytes) to keep in results.

    :return: polars Dataframe of sorted results
    """
    log = ProcessLog(
        "ds_limit_k_sorted",
        sort_column=sort_column,
        batch_size=batch_size,
        sort_direction=sort_direction,
        max_nbytes=max_nbytes,
    )
    result_compare_op = pc.min
    batch_compare_op = pc.max
    check_batch_op = gt
    if sort_direction == "ascending":
        result_compare_op = pc.max
        batch_compare_op = pc.min
        check_batch_op = lt

    bytes_read = 0
    result_compare_value = None
    result_batch = pa.RecordBatch.from_pylist([], schema=ds.schema)
    max_result_rows = 0
    for batch in ds.to_batches(batch_size=batch_size, batch_readahead=0, fragment_readahead=0):
        if batch.num_rows == 0:
            continue
        if result_compare_value is None:
            result_compare_value = result_compare_op(batch.column(sort_column)).as_py()
        batch_compare_value = batch_compare_op(batch.column(sort_column)).as_py()
        if bytes_read < max_nbytes or check_batch_op(batch_compare_value, result_compare_value):
            bytes_read += batch.nbytes
            result_batch: pa.RecordBatch = pa.concat_batches([result_batch, batch]).sort_by(  # type: ignore[attr-defined, no-redef]
                [(sort_column, sort_direction)]
            )
            if bytes_read > max_nbytes:
                if max_result_rows == 0:
                    max_result_rows = result_batch.num_rows
                result_batch = result_batch.slice(length=max_result_rows)
            result_compare_value = result_compare_op(result_batch.column(sort_column)).as_py()

    return_df = pl.from_arrow(result_batch)
    if isinstance(return_df, pl.Series):
        raise TypeError("Always dataframe.")
    log.complete(num_results=result_batch.num_rows)
    return return_df


def ds_metadata_limit_k_sorted(
    ds: pd.UnionDataset,
    sort_column: str,
    min_sort_value: Any | None = None,
    ds_filter: pc.Expression | None = None,
    ds_filter_columns: List[str] | None = None,
    max_nbytes: int = 256 * 1024 * 1024,
    max_rows: int = 0,
) -> pl.DataFrame:
    """
    Produce limited number of sorted (ascending) results from large parquet dataset.

    This function attempts to increase dataset scan performance by using parquet metadata to skip
    the reading of entire row groups. Performance is best if parquet files are loosely sorted by
    sort_column across dataset row groups.

    Any records with a NULL value in 'sort_column' will be ignored.

    :param ds: Dataset to scan, can not be filtered.
    :param sort_column: Column to sort results on.
    :param min_sort_value: (Optional) minimum value to include in results (exclusive).
    :param ds_filter: (Optional) Expression filter to apply during scan
    :param ds_filter_columns: (Optional) Any columns, that are not 'sort_column', used in
                              'ds_filter' Expression.
                              In the future, maybe, these columns can be extracted from 'ds_filter'
    :param max_nbytes: Max bytes (reported by RecordBatch.nbytes) to keep in results.
    :param max_rows: (Optional) Maximum number of rows to keep in results.

    :return: polars Dataframe of sorted results
    """
    log = ProcessLog(
        "ds_metadata_limit_k_sorted",
        sort_column=sort_column,
        max_nbytes=max_nbytes,
        min_sort_value=min_sort_value,
    )
    bytes_read = 0
    if ds_filter_columns is None:
        ds_filter_columns = []
    scan_cols = list(set([sort_column] + ds_filter_columns))
    result_table = pa.Table.from_pylist([], schema=ds.schema)
    result_max = None
    for child in ds.children:
        for file in child.files:  # type: ignore[attr-defined]
            pq_file = pq.ParquetFile(file, filesystem=child.filesystem)  # type: ignore[attr-defined]
            # if any scan_cols in dataset but not fragment file, skip fragment
            if set(scan_cols) <= set(ds.schema.names) and bool(
                set(scan_cols) - set(pq_file.schema_arrow.names)
            ):
                continue
            pq_metadata = pq_file.metadata
            rg_stats = file_column_stats(pq_metadata, sort_column)
            for rg_index in range(pq_metadata.num_row_groups):
                # filter row group by metadata (min/max sort value only)
                col_stats = rg_stats[rg_index]
                if col_stats["min"] is None or col_stats["max"] is None:
                    continue
                if min_sort_value is not None and col_stats["max"] <= min_sort_value:
                    continue
                if result_max is not None and col_stats["min"] > result_max:
                    continue
                # filter row group by min_sort_value and/or ds_filter
                # to limit data transfer initial filter limited to 'scan_cols'
                rg_table = pq_file.read_row_group(rg_index, columns=scan_cols)
                try:
                    if ds_filter is not None:
                        rg_table = rg_table.filter(ds_filter)
                    if min_sort_value is not None:
                        rg_table = rg_table.filter((pc.field(sort_column) > min_sort_value))
                except IndexError:
                    # IndexError is raised if .filter() produces no results???
                    continue
                if rg_table.num_rows == 0:
                    continue
                # read/filter entire row group
                rg_table = pq_file.read_row_group(rg_index)
                if ds_filter is not None:
                    rg_table = rg_table.filter(ds_filter)
                if min_sort_value is not None:
                    rg_table = rg_table.filter((pc.field(sort_column) > min_sort_value))
                # add parition columns to row group table, if they exist
                # ParquetFile does not include parition columns when reading row groups
                # all partition columns added as strings, but cast back to dataset types later
                for part_col, part_val in pq_path_partitions(file):
                    if part_col in ds.schema.names and part_col not in pq_file.schema_arrow.names:
                        rg_table = rg_table.append_column(
                            part_col,
                            pa.array(
                                [part_val] * rg_table.num_rows,
                                pa.string(),
                            ).cast(ds.schema.field(part_col).type),
                        )
                # add dataset columns that may not be in file as all Null
                for ds_col in ds.schema.names:
                    if ds_col not in rg_table.column_names:
                        rg_table = rg_table.append_column(
                            ds_col,
                            pa.array([None] * rg_table.num_rows, ds.schema.field(ds_col).type),
                        )
                bytes_read += rg_table.nbytes
                result_table = pa.concat_tables(
                    [result_table, rg_table],
                    promote_options="default",
                ).sort_by([(sort_column, "ascending")])
                # limit result_table to max_nbytes or max_row_count
                if bytes_read > max_nbytes or max_rows > 0:
                    if max_rows == 0:
                        max_rows = int(result_table.num_rows * (max_nbytes / bytes_read))
                    result_table = result_table.slice(length=max_rows)
                result_max = pc.max(result_table.column(sort_column)).as_py()
            pq_file.close()
    return_df = pl.from_arrow(result_table)
    if isinstance(return_df, pl.Series):
        raise TypeError("Always dataframe.")
    log.complete(num_results=result_table.num_rows)
    return return_df


def ds_batched_join(
    ds: pd.Dataset, match_frame: pl.DataFrame, keys: List[str], batch_size: int
) -> pl.DataFrame:
    """
    Join polars dataframe to very large parquet dataset.

    Join a limited polars dataframe against a very large parquet dataset in memory constrained
    environment.

    :param ds: Dataset to join against match_frame
    :param match_frame: Dataframe to join to ds
    :param keys: List of columns to match between ds and match_frame

    :return: ds records that joined to match_frame as polars Dataframe
    """
    log = ProcessLog("ds_batched_join", keys="|".join(keys), batch_size=batch_size)
    match_cols = match_frame.select(keys).unique()
    log.add_metadata(match_rows=match_cols.height)
    mod_cast, orig_cast = polars_decimal_as_string(match_cols)
    for key in keys:
        if match_cols.get_column(key).null_count() == 0:
            key_min = match_cols.get_column(key).min()
            key_max = match_cols.get_column(key).max()
            ds = ds.filter((pc.field(key) >= key_min) & (pc.field(key) <= key_max))
            break

    return_frame = pl.from_arrow(pa.Table.from_pylist([], schema=ds.schema))  # type: ignore[assignment]
    if match_cols.null_count().sum_horizontal().item() == 0:
        join_tables = []
        # perform join operation in pyarrow to constrain memmory
        # this should work if match_cols conatins no NULL values
        match_table = match_cols.to_arrow()
        for batch in ds.to_batches(batch_size=batch_size, batch_readahead=0, fragment_readahead=0):
            if batch.num_rows == 0:
                continue
            _table = pa.Table.from_batches([batch]).join(match_table, keys=keys, join_type="inner")
            if _table.num_rows > 0:
                join_tables.append(_table)
            log.add_metadata(arrow_mem=int(pa.total_allocated_bytes() / (1024 * 1024)))
        if join_tables:
            return_frame = pl.from_arrow(pa.concat_tables(join_tables, promote_options="default"))

    else:
        # pyarrow can not perform join operations on columns containing NULL values
        # if keys columns contain NULL values, use polars for JOIN operation
        # converting the dataset RecordBatch to polars results in a memory leak because the memory
        # associated with every polars DataFrame saved to join_frames is retained until the entire
        # dataset has been iterated through
        # this is a problem if the join operations ends up taking a very small number of records
        # from a loarge number of paritions in a very large dataset
        join_frames = []
        match_cols = match_cols.cast(dtypes=mod_cast)  # type: ignore[arg-type]
        for batch in ds.to_batches(batch_size=batch_size, batch_readahead=0, fragment_readahead=0):
            if batch.num_rows == 0:
                continue
            _df = pl.from_arrow(batch)
            if isinstance(_df, pl.Series):
                raise TypeError("Always dataframe.")
            _df = _df.cast(mod_cast).join(match_cols, on=keys, how="inner", nulls_equal=True)  # type: ignore[arg-type]
            if _df.height > 0:
                join_frames.append(_df)

        if join_frames:
            return_frame = pl.concat(join_frames, how="diagonal")

    if isinstance(return_frame, pl.Series):
        raise TypeError("Always dataframe.")

    log.complete(num_results=return_frame.height)
    return return_frame.cast(orig_cast)  # type: ignore[arg-type]


def ds_unique_values(ds: pd.Dataset, columns: List[str]) -> pa.Table:
    """
    Find Unique values on 'columns' of dataset.

    :param ds: pyarrow.Dataset to scan
    :param columns: columns to unique

    :return: pyarrow table with unique results
    """
    log = ProcessLog("ds_unique_values", columns="|".join(columns))
    scan_node = ac.ScanNodeOptions(ds, columns=columns, batch_readahead=0, fragment_readahead=1)  # type: ignore[attr-defined]
    declarations: List[ac.Declaration] = [
        ac.Declaration("scan", scan_node),
        ac.Declaration("aggregate", ac.AggregateNodeOptions(aggregates=[], keys=columns)),  # type: ignore[arg-type]
    ]
    table = ac.Declaration.from_sequence(declarations).to_table()
    log.complete(num_unique_rows=table.num_rows)
    return table


def _pq_find_part_offset(folder: str) -> int:
    """
    Determine existing partition offset from files in folder.

    :param folder: folder to seach for part files

    :return: file partition offset
    """
    part_offset = 1
    r = re.compile(r"(\d{3,}).parquet")
    pq_files = sorted(filter(r.search, os.listdir(folder)))
    if pq_files:
        part_offset = int(r.findall(pq_files[-1])[0])

    return part_offset


def _pq_write_batches(
    batches: Iterable[pa.RecordBatch],
    schema: pa.Schema,
    target_rows_per_mb: int,
    export_folder: str = "",
    export_file_prefix: str = "export",
    mb_per_file: int = 1024,
    mb_per_group_disk: int = 16,
    mb_per_group_mem: int = 1024,
    compression: Literal["gzip", "bz2", "brotli", "lz4", "zstd", "snappy", "none"] = "zstd",
    compression_level: int = 3,
) -> List[str]:
    """
    Write RecordBatches to local file(s).

    :return: list of parquet files created
    """
    log = ProcessLog("_pq_write_batches")
    files_created: List[str] = []

    os.makedirs(export_folder, exist_ok=True)
    part_offset = _pq_find_part_offset(export_folder)
    files_created.append(
        os.path.join(
            export_folder,
            f"{export_file_prefix}_{len(files_created) + part_offset:03}.parquet",
        )
    )
    log.add_metadata(file_created=files_created[-1])

    writer = pq.ParquetWriter(
        where=files_created[-1],
        schema=schema,
        compression=compression,
        compression_level=compression_level,
    )
    write_batches = []
    write_rows = [0]
    write_mbs = [0.0]
    for batch in batches:
        if batch.num_rows == 0:
            continue
        write_batches.append(batch)
        write_rows[-1] += batch.num_rows
        write_mbs[-1] += batch.nbytes / (1024 * 1024)
        # write batch if target row-group size reached
        if (
            write_rows[-1] >= mb_per_group_disk * target_rows_per_mb
            or write_mbs[-1] > mb_per_group_mem
        ):
            writer.write_batch(
                batch=pa.concat_batches(write_batches),  # type: ignore[attr-defined]
                row_group_size=write_rows[-1],
            )
            write_batches.clear()
            write_rows.append(0)
            write_mbs.append(0.0)

            # re-calculate target_rows_per_mb based on export_path writing
            target_rows_per_mb = pq_rows_per_mb(files_created[-1], num_rows=sum(write_rows))

            # create new parquet file if mb_per_file reached
            if int(os.path.getsize(files_created[-1]) / (1024 * 1024)) > mb_per_file:
                writer.close()
                files_created.append(
                    os.path.join(
                        export_folder,
                        f"{export_file_prefix}_{len(files_created) + part_offset:03}.parquet",
                    )
                )
                log.add_metadata(file_created=files_created[-1])
                writer = pq.ParquetWriter(
                    where=files_created[-1],
                    schema=schema,
                    compression=compression,
                    compression_level=compression_level,
                )

    # write any remaining batches and close writer
    if write_batches:
        writer.write_batch(
            batch=pa.concat_batches(write_batches),  # type: ignore[attr-defined]
            row_group_size=write_rows[-1],
        )
    writer.close()
    log.complete(num_files_created=len(files_created), file_created="|".join(files_created))
    return files_created


def pq_dataset_writer(
    source: pd.UnionDataset,
    partition_columns: Optional[List[str]] = None,
    export_folder: str = "",
    export_file_prefix: str = "export",
    mb_per_file: int = 1024,
    mb_per_group_disk: int = 16,
    mb_per_group_mem: int = 1024,
    compression: Literal["gzip", "bz2", "brotli", "lz4", "zstd", "snappy", "none"] = "zstd",
    compression_level: int = 3,
) -> List[str]:
    """
    Write pyarrow dataset to local file(s).

    :param source: parquet file path(s) on local disk
    :param export_path: path for new re-parted parquet file

    :return: list of parquet files created
    """
    log = ProcessLog("pq_dataset_writer")
    target_rows_per_mb = pq_rows_per_mb(ds_files(source))
    files_created = []
    if partition_columns is None:
        batches = source.to_batches(
            batch_size=target_rows_per_mb,
            batch_readahead=1,
            fragment_readahead=0,
        )
        files_created += _pq_write_batches(
            batches=batches,
            schema=source.schema,
            target_rows_per_mb=target_rows_per_mb,
            export_folder=export_folder,
            export_file_prefix=export_file_prefix,
            mb_per_file=mb_per_file,
            mb_per_group_disk=mb_per_group_disk,
            mb_per_group_mem=mb_per_group_mem,
            compression=compression,
            compression_level=compression_level,
        )
    else:
        # creat list of columns and schema with partition columns removed
        batch_columns = [col for col in source.schema.names if col not in partition_columns]
        batch_schema = source.schema
        for column in partition_columns:
            batch_schema = batch_schema.remove(batch_schema.get_field_index(column))

        for part in ds_unique_values(source, partition_columns).to_pylist():
            # create folder partition prefix
            # {"year":"2024", "month":"01"}
            # part_prefix = "year=2024/month=01"
            part_prefix = "/".join([f"{k}={v}" for k, v in part.items()])

            # create pyarrow compute experssion to filter dataset
            # {"year":"2024", "month":"01"}
            # filter = compute.Expession( (year==2024) and (month==01) )
            filter = reduce(and_, [pc.field(k) == v for k, v in part.items()])

            batches = source.to_batches(
                columns=batch_columns,
                batch_size=target_rows_per_mb,
                filter=filter,
                batch_readahead=1,
                fragment_readahead=0,
            )
            files_created += _pq_write_batches(
                batches=batches,
                schema=batch_schema,
                target_rows_per_mb=target_rows_per_mb,
                export_folder=os.path.join(export_folder, part_prefix),
                export_file_prefix=export_file_prefix,
                mb_per_file=mb_per_file,
                mb_per_group_disk=mb_per_group_disk,
                mb_per_group_mem=mb_per_group_mem,
                compression=compression,
                compression_level=compression_level,
            )

    # return unique list of files
    files_created = sorted(set(files_created))
    log.complete(num_files_created=len(files_created))
    return files_created
