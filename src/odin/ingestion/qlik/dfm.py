import json
from typing import List
from typing import TypedDict
from typing import Optional
from typing import Dict
from datetime import datetime

import polars as pl

from odin.utils.aws.s3 import stream_object


class DFMtaskInfo(TypedDict):
    """DFM fields of taskInfo"""

    name: str
    sourceEndpoint: str
    sourceEndpointType: str
    sourceEndpointUser: str
    replicationServer: str
    operation: str


class DFMfileInfo(TypedDict):
    """DFM fields of fileInfo"""

    name: str
    extension: str
    location: str
    startWriteTimestamp: str
    endWriteTimestamp: str
    firstTransactionTimestamp: str
    lastTransactionTimestamp: str
    content: str
    recordCount: int
    errorCount: int


class DFMformatInfooptions(TypedDict):
    """DFM fields of formatInfo.options"""

    fieldDelimiter: str
    recordDelimiter: str
    nullValue: str
    quoteChar: str
    escapeChar: str


class DFMformatInfo(TypedDict):
    """DFM fields of formatInfo"""

    format: str
    options: DFMformatInfooptions


class DFMdataInfocolumns(TypedDict):
    """DFM fields of dataInfo.columns"""

    ordinal: int
    name: str
    type: str
    length: int
    precision: int
    scale: int
    primaryKeyPos: int


class DFMprecisionException(TypedDict):
    """Column matcher that pins a drifted DFM precision back to its archived value."""

    name: str
    type: str
    scale: int
    drift_precision: int
    archive_precision: int


# Qlik has been observed changing the declared precision of a column without any change to the
# values it holds. Because the declared precision determines the polars (and therefore parquet)
# type, that drift re-types the column and newly ingested files can no longer be unioned with the
# files already in the archive:
#
#   ArrowTypeError: Unable to merge: Field <column> has incompatible types:
#                   int64 vs decimal128(19, 0)
#
# Each entry below matches a column whose precision drifted and pins it back to the precision the
# existing archive was written with, so the ingested type stays stable.
#
# This is a work-around. An entry should only be removed alongside a re-write of the archived
# files for that column.
DFM_PRECISION_EXCEPTIONS: List[DFMprecisionException] = [
    # PAL_CONFIRMATION_KEY went from NUMERIC(10, 0) -> NUMERIC(19, 0), which moves it from
    # Int64 to Decimal(19, 0). Values are keys nowhere near the Int64 limit.
    {
        "name": "PAL_CONFIRMATION_KEY",
        "type": "NUMERIC",
        "scale": 0,
        "drift_precision": 19,
        "archive_precision": 10,
    },
    {
        "name": "PAL_CONFIRMATION_ID",
        "type": "NUMERIC",
        "scale": 0,
        "drift_precision": 19,
        "archive_precision": 10,
    },
]


class DFMdataInfo(TypedDict):
    """DFM fields of dataInfo"""

    sourceSchema: str
    sourceTable: str
    targetSchema: str
    targetTable: str
    tableVersion: int
    columns: List[DFMdataInfocolumns]


class QlikDFM(TypedDict):
    """DFM root fields"""

    dfmVersion: str
    taskInfo: DFMtaskInfo
    fileInfo: DFMfileInfo
    formatInfo: DFMformatInfo
    dataInfo: DFMdataInfo


def dfm_from_s3(dfm_path: str) -> QlikDFM:
    """
    Read Qlik .dfm file from S3.

    :param dfm_path: path to .csv or .csv.gz or .dfm file

    :return: dfm contents as TypedDict
    """
    dfm_path = dfm_path.replace(".csv.gz", ".dfm").replace(".csv", ".dfm")
    return json.load(stream_object(dfm_path))


def dfm_snapshot_dt(dfm: QlikDFM) -> datetime:
    """
    Convert Qlik DFM fileInfo.startWriteTimestamp to datetime object.

    :param dfm: DFM object to pull snapshot datetime from

    :return: fileInfo.startWriteTimestamp
    """
    return datetime.fromisoformat(dfm["fileInfo"]["startWriteTimestamp"])


def dfm_field_precision(field: DFMdataInfocolumns) -> int:
    """
    Get the precision to use for a DFM column, applying any DFM_PRECISION_EXCEPTIONS match.

    :param field: Qlik DFM column

    :return: archived precision if 'field' matches an exception, otherwise declared precision
    """
    for exception in DFM_PRECISION_EXCEPTIONS:
        if (
            field["name"].upper() == exception["name"]
            and field["type"] == exception["type"]
            and field["scale"] == exception["scale"]
            and field["precision"] == exception["drift_precision"]
        ):
            return exception["archive_precision"]

    return field["precision"]


def dfm_type_to_polars(field: DFMdataInfocolumns) -> pl.DataType:
    """
    Convert Qlik types to polars types.

    :param field: Qlik DFM column to be converted to Polars type

    :return: qlik type converted to polars
    """
    qlik_type = field["type"]
    field["precision"] = dfm_field_precision(field)

    if field["name"] == "header__change_seq":
        return pl.String()

    exact_type_matches = {
        "REAL4": pl.Float32(),
        "REAL8": pl.Float64(),
        "BOOLEAN": pl.Boolean(),
        "DATE": pl.Date(),
        "TIME": pl.Time(),
        "DATETIME": pl.Datetime(),
    }
    # check for exacty type matching
    return_type = exact_type_matches.get(qlik_type, None)
    if return_type is not None:
        return return_type

    # continue with alternate type matching
    if "INT" in qlik_type:
        return_type = pl.Int64()
    elif "NUMERIC" in qlik_type and field["scale"] == 0 and field["precision"] < 19:
        return_type = pl.Int64()
    elif "NUMERIC" in qlik_type:
        return_type = pl.Decimal(precision=field["precision"], scale=field["scale"])
    else:
        return_type = pl.String()

    return return_type


def dfm_to_polars_schema(
    dfm: QlikDFM, prefix: Optional[Dict[str, pl.DataType]] = None
) -> pl.Schema:
    """
    Create polars schema based from .dfm dataInfo.columns field.

    :param data_info: DFM dataInfo for table
    :param prefix: Extra schema definitions to be added to the front of the polars schema.

    :return: polars schema of DFM
    """
    columns = dfm["dataInfo"]["columns"]
    if prefix is None:
        prefix = {}
    return pl.Schema(prefix | {col["name"].lower(): dfm_type_to_polars(col) for col in columns})
