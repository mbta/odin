import os

# bucket constants
DATA_ARCHIVE = os.getenv("DATA_ARCHIVE", "unset_DATA_ARCHIVE")
DATA_ERROR = os.getenv("DATA_ERROR", "unset_DATA_ERROR")
DATA_INCOMING = os.getenv("DATA_INCOMING", "unset_DATA_INCOMING")
DATA_SPRINGBOARD = os.getenv("DATA_SPRINGBOARD", "unset_DATA_SPRINGBOARD")

# ODIN
ODIN_DATA = "odin/data"
ODIN_ARCHIVE = "odin/archive"
ODIN_ERROR = "odin/error"
ODIN_LOGS = "odin/logs"
ODIN_MIGRATIONS = "odin/migrations"
ODIN_DICTIONARY = f"{ODIN_DATA}/dictionary"

# CUBIC QLIK
IN_QLIK_PREFIX = "cubic/ods_qlik"
CUBIC_QLIK_DATA = f"{ODIN_DATA}/cubic/ods_history"
CUBIC_QLIK_ERROR = f"{ODIN_ERROR}/cubic_qlik"
CUBIC_QLIK_PROCESSED = f"{ODIN_ARCHIVE}/cubic_qlik/processed"
CUBIC_QLIK_IGNORED = f"{ODIN_ARCHIVE}/cubic_qlik/ignored"

# CUBIC ODS FACT
CUBIC_ODS_FACT_DATA = f"{ODIN_DATA}/cubic/ods"
CUBIC_ODS_REPORTS = f"{ODIN_DATA}/cubic/reports"

# Per-table fact status JSON, published for anyone with read access to the bucket
CUBIC_ODS_FACT_STATUS = f"{ODIN_LOGS}/ods"

CUBIC_ODS_DELTA_DATA = f"{ODIN_DATA}/cubic/ods_delta"
CUBIC_ODS_DELTA_STATUS = f"{ODIN_LOGS}/ods_delta"

# AFC
AFC_DATA = f"{ODIN_DATA}/sb/api"
AFC_RESTRICTED = f"{ODIN_DATA}/sb/restricted"
AFC_STATUS = f"{ODIN_LOGS}/afc"

# Masabi
MASABI_DATA = f"{ODIN_DATA}/masabi/api"
MASABI_RESTRICTED = f"{ODIN_DATA}/masabi/restricted"
MASABI_TEMP = f"{ODIN_DATA}/masabi/temporary"
# Destination for the historical backfill job. Kept separate from MASABI_DATA
# so the live pipeline is undisturbed until the backfill is swapped in.
MASABI_BACKFILL = f"{ODIN_DATA}/masabi/backfill"
MASABI_STATUS = f"{ODIN_LOGS}/masabi"
