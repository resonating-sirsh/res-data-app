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


def test_shell():
    assert True, "false"
