import os
from dataclasses import dataclass
from string import Template

import duckdb

from odin.utils.logger import ProcessLog
from odin.utils.aws.s3 import list_partitions
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import CUBIC_ODS_FACT_DATA
from odin.utils.locations import CUBIC_QLIK_DATA
from odin.utils.locations import AFC_DATA
import odin.generate.data_dictionary.cubic_reports_sql as cubic_sql


DB_FILE = "fares_data_repository.db"


@dataclass
class ViewBuilder:
    """Fields needed to cretae dataset views from parquet files."""

    s3_prefix: str
    schema: str
    template: Template


DROP_VIEW = "DROP VIEW IF EXISTS $schema.$table;"
READ_PQ = "read_parquet('$s3_path/**/*.parquet')"

dataset_views = [
    ViewBuilder(
        s3_prefix=os.path.join(DATA_SPRINGBOARD, AFC_DATA),
        schema="sb",
        template=Template(f"{DROP_VIEW} CREATE VIEW $schema.$table AS SELECT * FROM {READ_PQ};"),
    ),
    ViewBuilder(
        s3_prefix=os.path.join(DATA_SPRINGBOARD, CUBIC_ODS_FACT_DATA),
        schema="cubic_ods",
        template=Template(f"{DROP_VIEW} CREATE VIEW $schema.$table AS SELECT * FROM {READ_PQ};"),
    ),
    ViewBuilder(
        s3_prefix=os.path.join(DATA_SPRINGBOARD, CUBIC_QLIK_DATA),
        schema="cubic_ods_history",
        template=Template(
            (
                f"{DROP_VIEW} CREATE VIEW $schema.$table AS SELECT * FROM {READ_PQ} "
                f"WHERE snapshot=(SELECT max(snapshot) FROM {READ_PQ});"
            )
        ),
    ),
]

cubic_report_views = [
    cubic_sql.COMP_A_VIEW,
    cubic_sql.COMP_B_VIEW,
    cubic_sql.COMP_D_VIEW,
    cubic_sql.AD_HOC_VIEW,
]


def create_fares_db(folder: str) -> str:
    """
    Create fares data DUCK DB file.

    :param folder: DB FILE export folder.

    :return: full path of created DB file.
    """
    write_path = os.path.join(folder, DB_FILE)
    with duckdb.connect(write_path) as con:
        # Hopefully this works on ECS...
        con.execute("CREATE OR REPLACE SECRET secret (TYPE s3, PROVIDER credential_chain);")

        for view in dataset_views:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {view.schema};")
            for view_table in list_partitions(view.s3_prefix):
                try:
                    view_log = ProcessLog(schema=view.schema, s3_prefix=view.s3_prefix)
                    view_query = view.template.substitute(
                        schema=view.schema,
                        table=view_table.replace(".", "_").lower(),
                        s3_path=f"s3://{os.path.join(view.s3_prefix, view_table)}",
                    )
                    con.execute(view_query)
                except Exception as exception:
                    view_log.failed(exception=exception)

        con.execute("CREATE SCHEMA IF NOT EXISTS cubic_reports;")
        for view in cubic_report_views:
            try:
                view_log = ProcessLog(view_type="cubic_report")
                con.execute(view)
            except Exception as exception:
                view_log.failed(exception=exception)

    return write_path
