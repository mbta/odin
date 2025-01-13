import os

# bucket constants
DATA_ARCHIVE = os.getenv("DATA_ARCHIVE", "unset_DATA_ARCHIVE")
DATA_ERROR = os.getenv("DATA_ERROR", "unset_DATA_ERROR")
DATA_INCOMING = os.getenv("DATA_INCOMING", "unset_DATA_INCOMING")
DATA_SPRINGBOARD = os.getenv("DATA_SPRINGBOARD", "unset_DATA_SPRINGBOARD")

# prefix constants
QLIK_PREFIX = "cubic/ods_qlik"
