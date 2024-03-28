import json
import os

import boto3
from intuitlib.client import AuthClient
from intuitlib.exceptions import AuthClientError
from quickbooks import QuickBooks

from res.utils import logger
from res.utils.secrets.ResSecretsClient import ResSecretsClient

RESONANCE_COMPANIES_COMPANY_ID = "9130354489877656"
RESONANCE_COMPANIES_DEV_COMPANY_ID = "9130357992227056"
RESONANCE_MANUFACTURING_COMPANY_ID = "123146117316034"
RESONANCE_MANUFACTURING_DEV_COMPANY_ID = "9130357992222806"
# Partner brands use the same dev/sandbox company
PARTNER_BRAND_COMPANY_DEV_COMPANY_ID = "9341452007820480"
ALLCAPS_STUDIO_COMPANY_ID = "9130357764775106"
BRUCE_GLENN_COMPANY_ID = "9130357764659326"
JCRT_COMPANY_ID = "9130354499069116"
THE_KIT_COMPANY_ID = "9130354490158626"
TUCKER_COMPANY_ID = "9130354490045406"


class QuickBooksClient:
    def __init__(self, account_name=None, client=None, res_env=None):

        # Determine environment. Quickbooks has their own environment
        # nomenclature and refers to development environments as sandboxes
        self.res_env = res_env if res_env else os.getenv("RES_ENV", "development")
        self.qb_env = self.res_env if self.res_env == "production" else "sandbox"
        self.is_partner_brand = False

        # Create boto3 client used for updating secret values
        self.boto = boto3.client("secretsmanager")
        self.secrets_client = ResSecretsClient(environment=self.res_env)

        # Determine account name. Resonance is made up of Resonance Companies
        # and Resonance Manufacturing. Partner brands have their own company
        # accounts as well
        if account_name.lower() in (
            "resonance_manufacturing",
            "resonance manufacturing",
            "manufacturing",
        ):

            self.account_name = "RESONANCE_MANUFACTURING"
            self.company_id = (
                RESONANCE_MANUFACTURING_COMPANY_ID
                if self.res_env == "production"
                else RESONANCE_MANUFACTURING_DEV_COMPANY_ID
            )

        elif account_name.lower() in (
            "resonance_companies",
            "resonance companies",
            "companies",
        ):

            self.account_name = "RESONANCE_COMPANIES"
            self.company_id = (
                RESONANCE_COMPANIES_COMPANY_ID
                if self.res_env == "production"
                else RESONANCE_COMPANIES_DEV_COMPANY_ID
            )

        elif account_name.lower() in (
            "allcaps",
            "allcaps studio",
            "all_caps",
        ):

            self.account_name = (
                "ALLCAPS" if self.res_env == "production" else "PARTNER_BRAND_SANDBOX"
            )
            self.company_id = (
                ALLCAPS_STUDIO_COMPANY_ID
                if self.res_env == "production"
                else PARTNER_BRAND_COMPANY_DEV_COMPANY_ID
            )
            self.is_partner_brand = True

        elif account_name.lower() in (
            "bruceglenn",
            "bruceglen",
            "bruce_glenn",
            "bruce_glen",
        ):

            self.account_name = (
                "BRUCE_GLENN"
                if self.res_env == "production"
                else "PARTNER_BRAND_SANDBOX"
            )
            self.company_id = (
                BRUCE_GLENN_COMPANY_ID
                if self.res_env == "production"
                else PARTNER_BRAND_COMPANY_DEV_COMPANY_ID
            )
            self.is_partner_brand = True

        elif account_name.lower() in (
            "jcrt",
            "atdm",
        ):

            self.account_name = (
                "JCRT" if self.res_env == "production" else "PARTNER_BRAND_SANDBOX"
            )
            self.company_id = (
                JCRT_COMPANY_ID
                if self.res_env == "production"
                else PARTNER_BRAND_COMPANY_DEV_COMPANY_ID
            )
            self.is_partner_brand = True

        elif account_name.lower() in (
            "thekit",
            "the_kit",
            "the kit",
            "the kit nyc",
            "the kit.",
        ):

            self.account_name = (
                "THE_KIT" if self.res_env == "production" else "PARTNER_BRAND_SANDBOX"
            )
            self.company_id = (
                THE_KIT_COMPANY_ID
                if self.res_env == "production"
                else PARTNER_BRAND_COMPANY_DEV_COMPANY_ID
            )
            self.is_partner_brand = True

        elif "tucker" in account_name.lower():

            self.account_name = (
                "TUCKER" if self.res_env == "production" else "PARTNER_BRAND_SANDBOX"
            )
            self.company_id = (
                TUCKER_COMPANY_ID
                if self.res_env == "production"
                else PARTNER_BRAND_COMPANY_DEV_COMPANY_ID
            )
            self.is_partner_brand = True

        else:

            raise Exception("Invalid account name provided to class")

        # Retrieve relevant secrets values
        self.client_id = self.secrets_client.get_secret(
            "QUICKBOOKS_DATA_INTEGRATION_CLIENT_ID"
        )
        self.client_secret = self.secrets_client.get_secret(
            "QUICKBOOKS_DATA_INTEGRATION_CLIENT_SECRET"
        )

        if self.is_partner_brand:

            self.access_token = self.secrets_client.get_secret(
                f"QUICKBOOKS_{self.account_name}_ACCESS_TOKEN",
                return_secret_string=True,
            )
            self.refresh_token = self.secrets_client.get_secret(
                f"QUICKBOOKS_{self.account_name}_REFRESH_TOKEN",
                return_secret_string=True,
            )

        else:

            self.access_token = self.secrets_client.get_secret(
                f"QUICKBOOKS_{self.account_name}_ACCESS_TOKEN"
            )
            self.refresh_token = self.secrets_client.get_secret(
                f"QUICKBOOKS_{self.account_name}_REFRESH_TOKEN"
            )

        # Set up an AuthClient passing in client_id and client_secret
        self.auth_client = AuthClient(
            client_id=self.client_id,
            client_secret=self.client_secret,
            access_token=self.access_token,
            environment=self.qb_env,
            redirect_uri="https://developer.intuit.com/v2/OAuth2Playground/RedirectUrl",
        )

        # Create a QuickBooks client object passing in the AuthClient, refresh
        # token, and company id
        if client:

            self.client = client

        else:

            self.client = QuickBooks(
                auth_client=self.auth_client,
                refresh_token=self.refresh_token,
                company_id=self.company_id,
            )

    def refresh(self):
        """
        Refreshes current access and refresh tokens for the corresponding
        Quickbooks account then updates their values in AWS. There are a
        few sets of tokens that are refreshed here:

        - Resonance Companies dev and prod tokens
        - Resonance Manufacturing dev and prod tokens
        - Partner brand prod tokens and a single partner brand dev token

        The tokens that will be refreshed depend on the environment in which
        the app is run, i.e. refreshing in a dev environment only refreshes
        the dev tokens and vice versa. An app called Quickbooks Credentials
        Refresher runs once per hour in both dev and prod and refreshes
        the respective tokens

        The entire secrets dict is retrieved because both production and dev
        values are needed to update it. This only happens for Resonance
        Companies and Resonance Manufacturing. For partner brands, the secrets
        retrieved are for the specific partner brand and then the general
        partner brand sandbox account

        If you run into authentication errors even after refresh that means
        the access or refresh tokens have expired. You can use the intuit
        OAuth 2.0 playground to quickly create new tokens without having to
        mimic the OAuth flow from within an app. Make sure to select all
        scopes.

        https://developer.intuit.com/app/developer/playground
        """

        if self.is_partner_brand:

            # Refresh
            self.auth_client.refresh(self.refresh_token)
            logger.info(
                f"Refreshed {self.res_env} tokens for {self.account_name.title()}"
            )

            # Update class tokens with refreshed values
            self.access_token = self.auth_client.access_token
            self.refresh_token = self.auth_client.refresh_token

            # Update the secrets
            self.boto.update_secret(
                SecretId=f"QUICKBOOKS_{self.account_name}_ACCESS_TOKEN",
                SecretString=self.access_token,
            )
            logger.info("Updated access token value in AWS")
            self.boto.update_secret(
                SecretId=f"QUICKBOOKS_{self.account_name}_REFRESH_TOKEN",
                SecretString=self.refresh_token,
            )
            logger.info("Updated refresh token value in AWS")

        else:

            # Retrieve current access and refresh token secrets
            access_token_secret_dict = self.secrets_client.get_secret(
                secret_name=f"QUICKBOOKS_{self.account_name}_ACCESS_TOKEN",
                return_entire_dict=True,
            )
            refresh_token_secret_dict = self.secrets_client.get_secret(
                secret_name=f"QUICKBOOKS_{self.account_name}_REFRESH_TOKEN",
                return_entire_dict=True,
            )

            # Refresh
            self.auth_client.refresh(self.refresh_token)
            logger.info(
                f"Refreshed {self.res_env} tokens for {self.account_name.title()}"
            )

            # Update class tokens with refreshed values
            self.access_token = self.auth_client.access_token
            self.refresh_token = self.auth_client.refresh_token

            # Update the secret dicts, turn to strings, then update secrets

            # Access token
            access_token_secret_dict[self.res_env] = self.access_token
            access_token_secret_dict_str = json.dumps(access_token_secret_dict)
            self.boto.update_secret(
                SecretId=f"QUICKBOOKS_{self.account_name}_ACCESS_TOKEN",
                SecretString=access_token_secret_dict_str,
            )
            logger.info("Updated access token value in AWS")

            # Refresh token
            refresh_token_secret_dict[self.res_env] = self.refresh_token
            refresh_token_secret_dict_str = json.dumps(refresh_token_secret_dict)
            self.boto.update_secret(
                SecretId=f"QUICKBOOKS_{self.account_name}_REFRESH_TOKEN",
                SecretString=refresh_token_secret_dict_str,
            )
            logger.info("Updated refresh token value in AWS")

        # Test credentials with a user info API call. User info is returned with
        # this method only if the credentials have the OpenID scope, which they
        # should
        logger.info("Testing updated credentials")

        try:

            response = self.auth_client.get_user_info()
            logger.info("Credentials are valid with OpenID scope")

        except AuthClientError as e:

            logger.error(
                f"Auth Client Error; check access token API scopes to ensure"
                " the OpenID scope is included alongside other scopes"
            )
            logger.error(f"{e.status_code} for intuit_tid {e.intuit_tid}: {e.content}")

            raise

    def save_invoice(self, invoice):
        invoice.save(qb=self.client)

    def get_report(self, type):
        report = self.client.get_report(report_type=type)
        return report
