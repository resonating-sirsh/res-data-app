from pathlib import Path

from ..src.main import read_config_file


class TestConfigValidation:
    def test_valid_config_validation(self):
        config = list(read_config_file(Path("tests/test_config_valid.yaml")))
        assert config

    def test_invalid_config_validation(self):
        config = list(read_config_file(Path("tests/test_config_invalid.yaml")))
        # Invalid configs shouldn't be added to "config"
        assert not list(config)
