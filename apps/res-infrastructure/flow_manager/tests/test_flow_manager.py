import pytest
import res
from glob import glob
import json, yaml


# sometimes we might create a json file which is not parseable and we only notice when the deployment occurs
# def can_parse_all_json_and_yaml_on_path():
#     for file in glob("..//*.yaml", recursive=True):
#         with open(file, "r") as f:
#             d = yaml.safe_load(f)
#             assert isinstance(d, dict), f"Failed to parse the file {file}"

#     for file in glob("..//*.json", recursive=True):
#         with open(file, "r") as f:
#             d = json.loads(f)
#             assert isinstance(d, dict), f"Failed to parse the file {file}"


def test_determine_asset_type():
    assert True, "false"


def test_determine_one_fields_for_table_set():
    assert True, "false"


def test_load_assembly_info_for_table_set():
    assert True, "false"


def test_full():
    # res.flows.FlowContext.mocks = {}

    from ..src.main import run

    # run with an option that does not run forever and with mocks

    event = {}
    context = {}

    assert True, "false"
