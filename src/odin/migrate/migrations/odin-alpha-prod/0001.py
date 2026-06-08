import os

from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import delete_objects
from odin.utils.locations import DATA_SPRINGBOARD
from odin.utils.locations import MASABI_DATA
from odin.utils.logger import ProcessLog

TABLES_TO_DELETE: list[str] = [
    "retail.account_actions",
    "retail.activations",
    "retail.ticket_purchases",
    "retail.tickets",
    "validation.scans",
]


def migration() -> None:
    """
    Delete all files under
    s3://<springboard>/odin/data/masabi/api/<table>/
    for each table in TABLES_TO_DELETE
    """
    log = ProcessLog("odin_migration", migration="alpha_prod_0001")
    failures: dict[str, int] = {}

    for table in TABLES_TO_DELETE:
        prefix = os.path.join(DATA_SPRINGBOARD, MASABI_DATA, table, "")
        remaining = delete_objects([obj.path for obj in list_objects(prefix)])
        if remaining:
            failures[prefix] = len(remaining)

    log.add_metadata(
        tables_attempted=len(TABLES_TO_DELETE),
        tables_failed=len(failures),
        failure_details=str(failures) if failures else "none",
    )
    log.complete()

    assert not failures, f"Failed to delete objects for prefixes: {failures}"
