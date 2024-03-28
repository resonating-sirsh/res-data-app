import os
from pprint import pprint
import getpass

from . import _utils


def get_res_data_docker(docker_subpath="res-data"):
    return _utils.get_res_root() / "res/docker/"


def build_docker(tag=None, hash=False, **kwargs):
    import subprocess

    # process to dir
    if tag is None:
        tag = getpass.getuser()
        # todo clean name
        if hash:
            tag = _utils.res_hash(tag)

    docker_dir = get_res_data_docker()
    file = docker_dir / "push_docker.sh"
    try:
        byteOutput = subprocess.check_output([file, tag], cwd=docker_dir)
        out = byteOutput.decode("UTF-8").rstrip()
        pprint(out)
        return tag
    except subprocess.CalledProcessError as exc:
        print("Error in ls -a:\n", exc.output)
        return None


def update_repos(paths=None, **kwargs):
    """
    experimental script for remote machines checking git repos e.g bertha
    this will run in a deamon on a sleep
    we should pass in some check to see if the repo *should* be updated or skip
    for example we check that there are no jobs running when we update a job runner
    """
    import subprocess

    if paths is None:
        paths = os.environ.get("RES_UPDATE_REPOS", "").split(";")

    if not paths:
        print("no updateable repos")
        return

    if not isinstance(paths, list):
        paths = [paths]

    for path in paths:
        if "checks" in kwargs:
            check_ok = kwargs["checks"].get("path")
            if check_ok and not check_ok():
                print("cannot update now")
                continue
        print(path, "will be updated using git")
        byteOutput = subprocess.check_output(
            [" git reset --hard origin/main & git pull origin main"], cwd=path
        )
        out = byteOutput.decode("UTF-8").rstrip()
        pprint(out)


def deploy_trainer(
    image,
    tag="latest",
    login=True,
    docker_dir="",
    platform="--platform linux/amd64",
    docker_context_path="../../../",
):
    """
    - This script could be turned into something more useful
      we hard code some things such as paths for simplicity
      the script is a useful way to document the moving parts
      in reality this would be used to point to a trainer and deploy it
      main trainers could be on the same docker/lib

    - set all the environment variables that are given internally
      the WORKFLOW_PATH is the path to the
      ARGO workflow manifest on your system as per article
      for example a repo of trainers could share workflows like this in a common area

    - Args:
      image: the docker image
      tag: the tag for this build. could be a hash or user
      login: for ECR we can login
      docker_dir: point to where the docker file is
      platform: on M1 (ARM) we should build for the target
      docker_context_path: the docker context if it is not in same location as docker file

    """

    import os
    import shlex
    import subprocess
    import time

    def run_command(command):
        ps = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=docker_dir,
        )
        r = ps.communicate()[0]
        print(r.decode())
        return r.decode()

    region = os.environ["AWS_DEFAULT_REGION"]
    acc_id = os.environ.get("AWS_ACCOUNT_ID", "286292902993")

    if login:
        command = f"""aws ecr get-login-password --region {region} | docker login --username AWS --password-stdin {acc_id}.dkr.ecr.{region}.amazonaws.com"""
        print(command)
        run_command(command)

    command = f"""docker build --pull --rm -f "Dockerfile" -t {image}:{tag} {platform or ""} {docker_context_path or ""}"""

    print(" <<<< BUILDING >>>>>>")
    print(command)
    run_command(command)

    ecr = f"""{acc_id}.dkr.ecr.{region}.amazonaws.com/{image}:{tag}"""
    command = f"""docker tag {image}:{tag} {ecr} & docker push {ecr}"""

    print(" <<<< PUSHING >>>>>>")
    print(command)
    run_command(command)

    print("INVOKE WORKFLOW")

    default_path = _utils.get_res_root() / ".workflows.old/training"
    workflow_path = os.environ.get("WORKFLOW_PATH", default=default_path)
    print(f"using workflow at {workflow_path}")
    # note the workflow params. we could also point to an S3 output path but for now we hard code in the workflow
    root = "/"  # this could be for different trainers on the docker image but simple case for now
    command = f"""argo submit {workflow_path}/trainer_workflow.yaml -n argo -p image={image} -p tag={tag} -p train_root={root}"""
    print(command)
    run_command(command)

    # sleep and open tensort port
    print(
        "sleep and wait for pods for 15 seconds or whatever the node provisioning time is"
    )
    time.sleep(15)
    # simle option to look for the trainer pod in argo
    command = """kubectl get pods --no-headers -o custom-columns=":metadata.name" -n argo | grep ^trainer """
    pod = run_command(command)
    print(f"We have the pod {pod} - we will listen to it")

    command = f"""kubectl port-forward pod/{pod} 6006:6006 -n argo"""
    p = subprocess.Popen(
        shlex.split(command),
        start_new_session=True,
    )

    # we opened tensor board on this port
    print(f"Visit http://localhost:6006")


# deploy with deploy_trainer
