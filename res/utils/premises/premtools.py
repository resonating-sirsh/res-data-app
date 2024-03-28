from asyncio.log import logger
import psutil
import os
import yaml
import subprocess
from crontab import CronTab
from git import Repo
import filecmp
import ntpath
import shutil
import requests
from pathlib import Path
import re
import time
import sys
from datetime import datetime
from smart_open import open
from functools import partial
import signal
from tenacity import retry, stop_after_attempt, wait_fixed

start_word = "#START OF VARIABLES#"
end_word = "#END OF VARIABLES#"
path_to_copies_folder = f"{str(Path.home())}/machine_manager_copies_folder"


def get_appname_from_directory(
    DEVICE_NAME, CONFIG_FILE_PATH, CALLING_APP_DIRECTORY_IN_REPO
):
    device_data = get_device_data(DEVICE_NAME, CONFIG_FILE_PATH)
    for app in device_data["apps"]:
        if (
            CALLING_APP_DIRECTORY_IN_REPO
            in device_data["apps"][app]["script_directory"]
        ):
            return app


def log_to_local(LOCAL_LOGS_DIRECTORY, DEVICE_NAME, APP_NAME):
    logging_directory = f"{LOCAL_LOGS_DIRECTORY}/{DEVICE_NAME}/{APP_NAME}"
    print(logging_directory)
    TIMESTAMP = datetime.now()

    if not os.path.exists(logging_directory):
        os.makedirs(logging_directory)

    sys.stdout = open(
        f"{LOCAL_LOGS_DIRECTORY}/{DEVICE_NAME}/{APP_NAME}/{TIMESTAMP}.log", "w"
    )


def internet_is_up():
    n = 0
    urls = [
        "https://www.google.com",
        "https://www.amazon.com",
    ]
    for url in urls:
        n += 1
        try:
            response = requests.get(url=url, timeout=1)
            if response.status_code == 200:
                return True

        except:
            if n == len(urls):
                return False


def app_status_notifier(DEVICE_NAME, APP_NAME, status):
    defined_statuses = [
        "started",
        "ended",
        "manual override",
        "ended with error",
        "running",
    ]
    if status not in defined_statuses:
        print("Not a valid Status")
        print(f"Select one of the following:\n {defined_statuses}")

        logger.error(
            f"You've set a non defined status for {DEVICE_NAME}-{APP_NAME}: {status}.\n Select one of the following:\n {defined_statuses}"
        )

    p = psutil.Process(os.getpid())
    topic = "res_premises.machine_manager.device_app_status"

    content = {
        "device": DEVICE_NAME,  # env_variable or a way to identify where is this app running
        "app_name": APP_NAME,
        "app_config": {
            "status": status,
            "pid": f"{p.pid}",
            "run_time": f"{time.time() - p.create_time()}",
            "time_last_run": f"{p.create_time()}",
        },
    }

    send_to_kgateway(topic, APP_NAME, content)


def initialize_signal_captures():
    signal.signal(signal.SIGINT, partial(signal_handler, "device", "app"))
    signal.signal(signal.SIGTERM, partial(signal_handler, "device", "app"))


def signal_handler(DEVICE_NAME, APP_NAME, sig, frame):
    if sig == 2:
        app_status_notifier(DEVICE_NAME, APP_NAME, "manual override")
        sys.exit(0)

    elif sig == 15:
        app_status_notifier(DEVICE_NAME, APP_NAME, "Killed")
        sys.exit(0)

    else:
        app_status_notifier(DEVICE_NAME, APP_NAME, "ended with error")
        sys.exit(1)


def create_crons(username, device_apps, activate_file_path, local_repo_directory):
    cron = CronTab(user=username)
    cron.remove_all()

    # Always create cron to pull from git
    job = cron.new(f"cd {local_repo_directory}res-data-platform && git pull")
    job.setall("*/5 * * * *")

    for app_name, app_config in device_apps.items():
        full_app_script_directory = (
            local_repo_directory + app_config["script_directory"]
        )

        if app_config["action"] != "kill":
            command = ". {} && {} {}".format(
                activate_file_path,
                app_config["cron"]["command"],
                full_app_script_directory,
            )
            job = cron.new(command)
            job.setall(app_config["cron"]["schedule"])

    cron.write()


def is_app_runing(python_script_path):
    running_instances = []
    python_scripts = []

    for process in psutil.process_iter():
        if "python" in process.name() and (
            process.status() == psutil.STATUS_RUNNING
            or process.status() == psutil.STATUS_SLEEPING
        ):
            # Maybe we should get the python version i.e: "Python 3.8.5"(?)
            try:
                python_scripts.append(process.cmdline()[1])
            except psutil.ZombieProcess:
                continue

    for script in python_scripts:
        if script == python_script_path:
            running_instances.append(script)

    if len(running_instances) <= 1:
        return False
    logger.info("App is already running")
    return True


def get_device_data(DEVICE_NAME, path_to_config_yaml_file):
    with open(f"{path_to_config_yaml_file}") as f:
        data = yaml.load(f, Loader=yaml.FullLoader)

    return data["devices"][f"{DEVICE_NAME}"]


def environment_variables_changed(environment_variables):
    for env_keys, env_values in environment_variables.items():
        if os.getenv(f"{env_keys}") == env_values:
            continue
        else:
            return True
    return False


def set_environment_variables(activate_file_path, environment_variables):
    new_lines = []
    new_lines.append(start_word)

    for env_keys, env_values in environment_variables.items():
        new_lines.append(f"export {env_keys}={env_values}")

    new_lines.append(end_word)

    with open(activate_file_path, "a+") as activate_file:
        for line in new_lines:
            activate_file.write(f"{line}\n")
        # activate_file.writelines(f"{new_lines}")
        activate_file.close()


def delete_environment_variables(activate_file_path):
    with open(activate_file_path, "r+") as activate_file:
        lines = activate_file.readlines()
        activate_file.close()

    lines_to_keep = []
    Delete_Mode = False

    for line in lines:
        if not Delete_Mode:
            if line.startswith(start_word):
                Delete_Mode = True
                continue
        elif line.startswith(end_word):
            Delete_Mode = False
            continue
        else:
            continue
        lines_to_keep.append(line)

    with open(activate_file_path, "w") as activate_file:
        activate_file.writelines(lines_to_keep)
        activate_file.close()


def actions(device_apps, local_repo_directory):
    for APP_NAME, app_config in device_apps.items():
        if app_config["action"] == "run":
            for process in psutil.process_iter():
                try:
                    if (
                        f"{local_repo_directory}{app_config['script_directory']}"
                        in process.cmdline()
                    ):
                        print("App already running... initiation aborted.")

                except psutil.ZombieProcess:
                    continue

                subprocess.Popen(
                    [
                        "python3",
                        f"{local_repo_directory}{app_config['script_directory']}",
                    ],
                    shell=False,
                )

        elif app_config["action"] == "kill":
            subprocess.run(
                [
                    "pkill",
                    "-f",
                    f"{local_repo_directory}{app_config['script_directory']}",
                ],
                shell=False,
            )

        elif app_config["action"] == "restart":
            subprocess.run(
                [
                    "pkill",
                    "-f",
                    f"{local_repo_directory}{app_config['script_directory']}",
                ],
                shell=False,
            )
            subprocess.Popen(
                ["python3", f"{local_repo_directory}{app_config['script_directory']}"],
                shell=False,
            )

        else:
            print("Error: A valid action has not been especified")


def get_last_commit_to_branch(repo_url, branch):
    process = subprocess.Popen(
        ["git", "ls-remote", repo_url, branch], stdout=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    sha = re.split(r"\t+", stdout.decode("ascii"))[0]
    return str(sha)


def repo_is_behind(local_repo_directory, branch):
    repo = Repo(local_repo_directory)
    repo_url = repo.remotes.origin.url
    local_current_commit = str(repo.head.commit)
    remote_current_commit = get_last_commit_to_branch(repo_url, branch)
    print(f"{local_current_commit} - {remote_current_commit}")

    if local_current_commit == remote_current_commit:
        return False

    return True


def pull_from_github(local_repo_directory, branch):
    repo = Repo(local_repo_directory)
    origin = repo.remotes.origin
    origin.pull(branch)
    return True


def create_file_copy(path_to_file):
    filename = ntpath.basename(path_to_file)
    path_to_file_copy = f"{path_to_copies_folder}/{filename}"

    if not os.path.exists(path_to_copies_folder):
        print(f"creating folder: {path_to_file_copy}")
        os.mkdir(path_to_copies_folder)

    shutil.copyfile(path_to_file, path_to_file_copy)
    print(f"Done Creating file at: {path_to_file_copy}")


def changes_on_file(path_to_file):
    # Compares two files and returns True if file has changed..
    filename = ntpath.basename(path_to_file)
    path_to_file_copy = f"{path_to_copies_folder}/{filename}"

    try:
        if filecmp.cmp(path_to_file, path_to_file_copy):
            return False
        else:
            return True
    except:
        return True


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3))
def send_to_kgateway(topic, process_name, content):
    if os.getenv("RES_ENV", "development") == "production":
        url = "https://data.resmagic.io/kgateway/submitevent"
    else:
        url = "https://datadev.resmagic.io/kgateway/submitevent"

    data = {
        "version": "1.0.0",
        "process_name": process_name,
        "topic": topic,
        "data": content,
    }
    if internet_is_up:
        response = requests.post(url, json=data)
        logger.info(response)
    return data


def event_to_local(data, LOCAL_EVENTS_DIRECTORY, APP_NAME):
    event_directory = f"{LOCAL_EVENTS_DIRECTORY}/{APP_NAME}"

    TIMESTAMP = datetime.now()
    path_to_event = f"{event_directory}/{TIMESTAMP}.txt"

    if not os.path.exists(event_directory):
        os.makedirs(event_directory)

    with open(path_to_event, "w") as f:
        f.write(str(data))
        f.close()


def check_version(path):
    f = open(path, "r")

    for line in f.readlines():
        if "__VERSION__" in line:
            latest_version = (
                line.split("__VERSION__ = ")[1].replace('"', "").replace("\n", "")
            )
            f.close()
            return latest_version
    f.close()
