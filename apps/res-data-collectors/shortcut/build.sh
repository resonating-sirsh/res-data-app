#!/bin/bash

set -e

if ! command -v virtualenv &> /dev/null
then
    echo "virtualenv could not be found"
    pip install pipx;
else
  venv_path=$(which virtualenv);
  echo "using venv located at $venv_path"
fi

venv="venv"
echo "making virtualenv: $venv"

echo "linking res library"
ln -sf ../../../res .

virtualenv $venv

$venv/bin/pip install --upgrade pip pytest
$venv/bin/pip install pip-tools

cp ../../../requirements.in requirements.txt

# this might lead to a better approach but i gave up before i got it working
#echo "compiling requirements file"
#$venv/bin/pip-compile ../../../res/docker/res-data-lite/requirements.in\
#                      ../../../mac-requirements.in\
#                      -o requirements.txt -v\
#                      --use-feature=2020-resolver

$venv/bin/pip install -r requirements.txt

path=`pwd`

echo "running pytest"
PYTHONPATH=res:$PYTHONPATH $venv/bin/pytest

echo "useful aliases"
echo "alias sc_py=\"PYTHONPATH=$path/res:$PYTHONPATH $path/$venv/bin/python\""
echo "alias sc_pytest=\"PYTHONPATH=$path/res:$PYTHONPATH $path/$venv/bin/pytest\""

echo "then run"
echo "sc_py src/main.py -d"
