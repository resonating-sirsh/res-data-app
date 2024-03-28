from signal import signal
import subprocess
import os

from black import main
from res.utils.logging.ResLogger import ResLogger
import res.utils.premises.premtools as premtools
from functools import partial
import signal

logger = ResLogger()
device_app_status_topic = "res_premises.machine_manager.device_app_status"
process_name = None
RES_APP_NAME = os.environ["RES_APP_NAME"] = "machine_manager"
LOCAL_REPO_DIRECTORY = __file__.split("apps")[0]
DEVICE_NAME = os.getenv("DEVICE_NAME")
username = subprocess.run("whoami", capture_output=True, text=True).stdout.strip()
branch = os.getenv("GITHUB_BRANCH", "main")
config_file_name = "configs.yaml"
config_filepath = f"{os.path.dirname(__file__)}/{config_file_name}"
running_processes = []
activate_file_path = None
FIRST_RUN_FLAG = os.getenv("MACHINE_MANAGER_FIRST_RUN", 0)

# RES_ENV=development
# DEVICE_NAME=bernardo_mbp
# HOME=/Users/bsosa
# GITHUB_BRANCH=sosa-morrobel-premises
# LOCAL_REPO_DIRECTORY=/Users/bsosa/repos/
# RES_NAMESPACE=res_premises
# AWS_SECRET=53yy/9hgpVdIalGhZqey0jmXy1RPA5ay5Mlnaqah
# AIRTABLE_API_KEY=keyIjvhB1dfsd4DEh
# LOG_TO_S3=1
# _=/usr/bin/printenv


def start():

    try:
        device_data = premtools.get_device_data(DEVICE_NAME, config_filepath)

    except Exception as e:
        logger.error(f"Couldn't find {DEVICE_NAME} in config file {e!r}")
        raise e

    # Checks for changes on Config File
    if premtools.changes_on_file(config_filepath) or FIRST_RUN_FLAG:
        logger.info("file has changed.")

        # Changes on Configs File -> Update device data
        device_data = premtools.get_device_data(DEVICE_NAME, config_filepath)

        activate_file_path = device_data["activate_file_path"]
        environment_variables = device_data["env_vars"]

        # Looks confirms if environment variables have changed.
        if premtools.environment_variables_changed(
            environment_variables
        ):  # Environment Variables Changed
            logger.info("Env changed.")

            premtools.delete_environment_variables(activate_file_path)
            logger.info("Done deleting env")

            premtools.set_environment_variables(
                activate_file_path, environment_variables
            )
            logger.info("Done re-writing env")

        premtools.create_crons(
            username,
            device_data["apps"],
            activate_file_path,
            LOCAL_REPO_DIRECTORY.split("res-data-platform")[0],
        )
        logger.info("Done Creating Crons")

    else:
        logger.info("No changes on config file")

    # Create Copy of Yaml File before pulling from Github
    premtools.create_file_copy(config_filepath)
    logger.info("Done Creating File Copy")


if __name__ == "__main__":
    logger.info("Machine Manager Started")
    try:
        start()
    except Exception as e:
        logger.error(f"An unknown error has occured: {RES_APP_NAME}\n {e!r}")
        raise e
