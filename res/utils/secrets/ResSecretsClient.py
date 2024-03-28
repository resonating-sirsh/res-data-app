"""
This is a Resonance Secrets Client, which retrieves secrets from Secrets Manager. Each
secret has a development and production value.
"""

from __future__ import annotations
import base64
import json
import os
from typing import Optional, Type
from boto3.session import Session

from .. import logging

environments = ["development", "production"]

__all__ = [
    "ResEnvironmentException",
    "ResSecretsClient",
]


def __getattr__(name):
    """Provide the secrets client lazily."""
    if name in ["secrets", "secrets_client"]:
        return ResSecretsClient.get_default_secrets_client()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class ResEnvironmentException(Exception):
    pass


class ResSecretsClient:
    _default_secrets_client: Optional[ResSecretsClient] = None

    @classmethod
    def get_default_secrets_client(cls: Type[ResSecretsClient]) -> ResSecretsClient:
        """Return the default logger."""
        if cls._default_secrets_client is None:
            cls._default_secrets_client = cls()
        return cls._default_secrets_client

    @classmethod
    def set_default_secrets_client(
        cls: Type[ResSecretsClient], new_client: ResSecretsClient
    ):
        """Set the default logger impl returned by the logger module attr."""
        if not isinstance(new_client, ResSecretsClient):
            raise TypeError(
                f"Refusing to set '{type(new_client).__qualname__}' object as the "
                f"default secrets client! (got '{new_client!r})"
            )
        cls._default_secrets_client = new_client

    def __init__(self, environment=None):
        # sa temp: changed this because self._env needs to be set + this is more concise
        self._environment = environment or os.getenv("RES_ENV", "development")
        # Create a Secrets Manager client
        self.region_name = "us-east-1"
        self.session = Session()
        self.client = self.session.client(
            service_name="secretsmanager", region_name=self.region_name
        )

    def get_secret(
        self,
        secret_name,
        load_in_environment=True,
        force=True,
        return_entire_dict=False,
        return_secret_string=False,
    ):
        if not force:
            if os.environ.get(secret_name):
                logging.logger.debug(f"secret {secret_name} already loaded")
                return os.environ.get(secret_name)

        logging.logger.debug("requesting secret: {}".format(secret_name))
        try:
            get_secret_value_response = self.client.get_secret_value(
                SecretId=secret_name
            )

            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields
            # will be populated.
            if "SecretString" in get_secret_value_response:
                secret = get_secret_value_response["SecretString"]

                if return_secret_string:

                    return secret
            else:
                secret = base64.b64decode(get_secret_value_response["SecretBinary"])

            # Check for development or production secret value
            secret_dict = json.loads(secret)
            # Return the entire dict if indicated. Otherwise return env value
            if return_entire_dict:
                return secret_dict
            elif self._environment not in secret_dict:
                logging.logger.debug(
                    f"Secret {secret_name} not set up with `development` and/or `production` values, "
                    "returning the entire dict"
                )
                return secret_dict
            else:
                if load_in_environment:
                    logging.logger.debug("adding secret key to environment...")
                    os.environ[secret_name] = secret_dict[self._environment]
                return secret_dict[self._environment]
        except Exception as ex:
            logging.logger.error(f"Unable to get the secret - {repr(ex)}")
            raise ex

    def get_secrets_by_prefix(
        self, secret_prefix, load_in_environment=True, force=True
    ):
        all_secrets = self._get_all_secret_names()
        all_secret_values = []
        for secret in all_secrets:
            if (
                len(secret) > len(secret_prefix)
                and secret_prefix.lower() == secret[: len(secret_prefix)].lower()
            ):
                all_secret_values.append(
                    {secret: self.get_secret(secret, load_in_environment, force=force)}
                )

        return all_secret_values

    def _get_all_secret_names(self):
        try:
            secrets_list = []
            next_token = "(init)"
            while next_token is not None:
                if next_token == "(init)":
                    secrets_response = self.client.list_secrets()
                else:
                    secrets_response = self.client.list_secrets(NextToken=next_token)
                secrets_list += [
                    item["Name"] for item in secrets_response["SecretList"]
                ]
                next_token = secrets_response.get("NextToken", None)
            return secrets_list
        except Exception as ex:
            logging.logger.error("Unable to get list of secrets, check permissions")
            raise ex
