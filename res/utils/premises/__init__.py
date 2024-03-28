import os
import res.utils.premises.premtools as premtools
import sys

# If app is internet sensitive, check here on future iteration
if not os.getenv("RES_TEAM"):
    os.environ["RES_TEAM"] = "res-premises"

CALLING_APP_SCRIPT = f"{sys.path[0]}/main.py"
LOCAL_REPO_DIRECTORY = os.getenv("LOCAL_REPO_DIRECTORY")
LOCAL_LOGS_DIRECTORY = f"{LOCAL_REPO_DIRECTORY}/logs"
CALLING_APP_DIRECTORY_IN_REPO = CALLING_APP_SCRIPT.split(f"{LOCAL_REPO_DIRECTORY}/")[1]
DEVICE_NAME = os.getenv("DEVICE_NAME")
CONFIG_FILE_PATH = f"{LOCAL_REPO_DIRECTORY}/res-data-platform/apps/res-premises/machine-manager/configs.yaml"


APP_NAME = premtools.get_appname_from_directory(
    DEVICE_NAME, CONFIG_FILE_PATH, CALLING_APP_DIRECTORY_IN_REPO
)
if APP_NAME == None:
    print("Found no App with that Name")
    exit()
premtools.app_status_notifier(DEVICE_NAME, APP_NAME, "started")
if premtools.is_app_runing(CALLING_APP_SCRIPT):
    exit()
premtools.log_to_local(LOCAL_LOGS_DIRECTORY, DEVICE_NAME, APP_NAME)
