import os
import re
import tempfile
from importlib.util import find_spec

from odin.utils.logger import ProcessLog
from odin.utils.runtime import infinite_wait
from odin.utils.locations import DATA_ARCHIVE
from odin.utils.locations import ODIN_MIGRATIONS
from odin.utils.aws.s3 import list_objects
from odin.utils.aws.s3 import upload_file


def get_last_run_migration(status_path: str) -> str | None:
    """
    Retrieve last run migration number from S3 object.

    :param status_path: S3 path of migration status file

    :return: None or stem of s3 object representing last run migration
    """
    status_objs = list_objects(status_path)
    if len(status_objs) == 0:
        return None
    if len(status_objs) > 1:
        raise AssertionError("More than one ODIN migration status file found.")
    status_obj = status_objs[0].path
    return status_obj.split("/")[-1]


def upload_migration_file(status_path: str, stem: str) -> None:
    """
    Upload migration file to S3.

    :param status_path: S3 path of migration status file
    :param stem: 4 numbers of migration stem. (0001)
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = os.path.join(tmpdir, stem)
        with open(file_path, "w") as f:
            f.write(stem)
        upload_file(file_path, os.path.join(status_path, stem))


def run_migrations(modules_path: str, task_name: str) -> None:
    """Run migration."""
    status_path = os.path.join(DATA_ARCHIVE, ODIN_MIGRATIONS, task_name)
    odin_root = find_spec("odin").submodule_search_locations[0]  # type: ignore[index, union-attr]
    format_check = re.compile(r"^\d{4}\.py$")
    last_run_migration = get_last_run_migration(status_path)
    for migration in sorted(os.listdir(modules_path)):
        migration_file = os.path.join(modules_path, migration)
        # verify migration is properly formatted migration file e.g. 0001.py
        if not os.path.isfile(migration_file):
            continue
        elif len(format_check.findall(migration)) != 1:
            ProcessLog("migration_skipped", reason=f"'bad migration file format: {migration_file}'")
            # Do something other than normal log here??
            continue
        migration_stem = migration.replace(".py", "")
        # skip migrations that have already been run
        if last_run_migration is not None and last_run_migration >= migration_stem:
            ProcessLog("migration_skipped", reason=f"'migration {migration_stem} already run'")
            continue

        import_path = migration_file.replace(odin_root, "odin").replace("/", ".").replace(".py", "")
        log = ProcessLog("run_migration", migration=migration_stem)
        try:
            migrate_module = __import__(import_path, fromlist=[""])
            migrate_module.migration()
            upload_migration_file(status_path, migration_stem)
            log.complete()
        except Exception as exception:
            log.failed(exception)
            raise exception


def start_migrations():
    """Start migration."""
    task_name = os.getenv("ECS_TASK_GROUP")
    if task_name is None:
        # Only run in AWS
        return
    task_name = task_name.replace("family:", "")
    log = ProcessLog("start_migrations", task_name=task_name)
    try:
        here = os.path.dirname(os.path.abspath(__file__))
        modules_path = os.path.join(here, "migrations", task_name)
        run_migrations(modules_path, task_name)
        log.complete()
    except FileNotFoundError as fnfe:
        # migration folder does not exist for task
        if modules_path == fnfe.filename:
            log.complete(no_migrations_found=True)
        else:
            raise fnfe
    except Exception as exception:
        log.failed(exception)
        infinite_wait(f"Migration failed for {task_name=}.")
