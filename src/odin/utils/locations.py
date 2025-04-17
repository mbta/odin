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
ODIN_MIGRATIONS = "odin/migrations"

# CUBIC QLIK
IN_QLIK_PREFIX = "cubic/ods_qlik"
CUBIC_QLIK_DATA = f"{ODIN_DATA}/cubic_qlik"
CUBIC_QLIK_ERROR = f"{ODIN_ERROR}/cubic_qlik"
CUBIC_QLIK_PROCESSED = f"{ODIN_ARCHIVE}/cubic_qlik/processed"
CUBIC_QLIK_IGNORED = f"{ODIN_ARCHIVE}/cubic_qlik/ignored"

# CUBIC ODS FACT
CUBIC_ODS_FACT_DATA = f"{ODIN_DATA}/cubic_ods"
