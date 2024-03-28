import json
import os
import res

from res.flows.make.nest.clear_nest_roll_assignments import clear_nest_roll_assignments
from res.utils import secrets, logger

RES_ENV = os.getenv("RES_ENV", "development")

MATERIALS = os.environ.get("MATERIALS", "all")
ROLLS = os.environ.get("ROLLS", "all")


def initialize():
    if "AIRTABLE_API_KEY" not in os.environ:
        secrets.secrets_client.get_secret("AIRTABLE_API_KEY")


if __name__ == "__main__":
    materials = [x.upper() for x in MATERIALS.split(",") if x]
    rolls = [x.upper() for x in ROLLS.split(",") if x]

    dry_run = RES_ENV != "production"
    if dry_run:
        logger.info("running in dry run mode")
    else:
        logger.info("running for reals")

    initialize()

    clear_nest_roll_assignments(materials, rolls, dry_run, deallocate_ppu=False)
