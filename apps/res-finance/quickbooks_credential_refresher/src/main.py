import os
from res.connectors.quickbooks.QuickbooksClient import QuickBooksClient
from res.utils import logger

# Environment variables and arguments
RES_ENV = os.getenv("RES_ENV", "development").lower()
DEV_OVERRIDE = os.getenv("DEV_OVERRIDE", "development").lower() == "true"

if __name__ == "__main__" and (RES_ENV == "production" or DEV_OVERRIDE):

    # Refreshes both dev and prod Quickbooks tokens (only dev if run manually in
    # dev) for Resonance Companies and Manufacturing and updates their values in
    # AWS secrets. Also refreshes values for the production token of each
    # partner brand QBO account and the single partner brand sandbox account
    envs_to_refresh = ["development"]

    if RES_ENV == "production":

        envs_to_refresh.append("production")

    for env in envs_to_refresh:

        if env == "production":

            target_companies = [
                "Companies",
                "Manufacturing",
                "allcaps",
                "bruceglenn",
                "jcrt",
                "thekit",
                "tucker",
            ]

        else:

            # In dev only the all-encompassing partner brand sandbox account
            # needs to be refreshed. It will be refreshed if you pass any
            # partner brand company name to the class
            target_companies = [
                "Companies",
                "Manufacturing",
                "thekit",
            ]

        for company in target_companies:

            try:

                logger.info(f"Refreshing credentials for {company} Quickbooks")
                QBO = QuickBooksClient(account_name=company, res_env=env)
                QBO.refresh()

            except Exception as e:

                logger.error(
                    f"Error refreshing quickbooks credentials for Resonance"
                    f" {company} {env} environment"
                )

                raise

    if RES_ENV == "production":

        logger.info(
            "Resonance Companies, Resonance Manufacturing, and partner brand"
            " Quickbooks tokens refreshed for"
            " development and production environments"
        )

    else:

        logger.info(
            "Resonance Companies, Resonance Manufacturing, and partner brand"
            " Quickbooks tokens refreshed for"
            " development environment"
        )
