import os
import subprocess
import time
import res

PLUGIN_HOME = os.environ.get("VSTITCHER_PLUGIN_DIR")
RES_DATA_HOME = res.utils.get_res_root()


def main():
    """
    experimental convenience button to reload the code - can you fetch code that you are updating?
    we set it up as a daemon because when we restart it in supervisor it will just refresh the code and wait around
     - and actually it needs to be restarted to be reloaded

    #would add to stats d with this
     [program:git_pull_wait_daemon]
        command=./git-pull-wait-daemon
        autorestart=true

    """
    if PLUGIN_HOME:
        res.utils.logger.info("pulling changes for plugin")
        byteOutput = subprocess.check_output(
            ["git", "reset", "--hard origin/main"], cwd=PLUGIN_HOME
        )
        out = byteOutput.decode("UTF-8").rstrip()
        byteOutput = subprocess.check_output(
            ["git", "pull", "origin", "main"], cwd=PLUGIN_HOME
        )
        out = byteOutput.decode("UTF-8").rstrip()

        res.utils.logger.info(out)

    if RES_DATA_HOME:
        byteOutput = subprocess.check_output(
            ["git", "reset", "--hard origin/main"], cwd=RES_DATA_HOME
        )
        out = byteOutput.decode("UTF-8").rstrip()
        byteOutput = subprocess.check_output(
            ["git", "pull", "origin", "main"], cwd=RES_DATA_HOME
        )
        out = byteOutput.decode("UTF-8").rstrip()

        res.utils.logger.info(out)


if __name__ == "__main__":
    main()

    while True:
        time.sleep(1)
