from res.utils.secrets import ResSecretsClient
import json
import pytest


@pytest.mark.service
class TestResSecrets:
    def test_missing_secret(self):
        rs = ResSecretsClient("development")
        with pytest.raises(Exception):
            rs.get_secret("TEST_MISSING_SECRET")

    def test_development_get_secret(self):
        rs = ResSecretsClient("development")
        secret_value = rs.get_secret("TEST_SECRET")
        assert secret_value == "ABCD"

    def test_development_get_secret_prefix(self):
        rs = ResSecretsClient("development")
        secret_value = rs.get_secrets_by_prefix("TEST_S")
        assert len(secret_value) == 1
        assert secret_value[0]["TEST_SECRET"] == "ABCD"

    def test_production_get_secret(self):
        rs = ResSecretsClient("production")
        secret_value = rs.get_secret("TEST_SECRET")
        assert secret_value == "DEFG"

    def test_production_get_secret_prefix(self):
        rs = ResSecretsClient("production")
        secret_value = rs.get_secrets_by_prefix("TEST_S")
        assert len(secret_value) == 1
        assert secret_value[0]["TEST_SECRET"] == "DEFG"
