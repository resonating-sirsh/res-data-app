from xmlrpc.client import ServerProxy
import res


RUNNER_PROCESS_NAME = "vstitcher_runner"

# http://supervisord.org/api.html
class Bertha:
    def __init__(self, host="192.30.132.155", port=9001):
        self._server = ServerProxy(f"http://{host}:{port}/RPC2")
        # http://supervisord.org/api.html
        res.utils.logger.info(self._server.supervisor.getState())
        # vstitcher_file_sync_daemon

    def get_runner_state(self):
        return self._server.supervisor.getProcessInfo(RUNNER_PROCESS_NAME)

    def start_process(self, name, **kwargs):
        return self._server.supervisor.startProcess(name)

    def stop_process(self, name, **kwargs):
        return self._server.supervisor.stopProcess(name)

    def restart_process(self, name, **kwargs):
        s = self.stop_process(name)
        return self.start_process(name)

    def upgrade_process(self, name):
        # queue update -> check for jobs to finish, stop process, pull git, start process
        # probably better to create a monitor script that checks for updates outside of this

        # is there a job running? if so wait some time
        # if not, stop the processes [job runner] locally on Bertha machine using the RPC client
        # pull all the git changes
        # VSTITCHER_PLUGIN_DIR <- move to these dirs and git pull
        # RES_JOB_RUNNER_DIR <- move to these dirs and git pull
        # restart the processes [job runner] locally on Bertha machine using the RPC client
        pass
