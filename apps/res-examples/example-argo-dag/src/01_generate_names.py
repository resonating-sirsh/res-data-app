import json, os, datetime, sys
from res.utils import logger

# Generates a list of names, which will create pods/steps for each name in the next Argo step
# Developers can manually trigger this using the following event payload options:
# {
#     "name": "appABCDE"
# }
# Which will launch this job for a single name

RES_ENV = os.getenv("RES_ENV", "development")
NAMES = ["chris", "jattenberg", "sirsh", "ben", "alex", "marianne", "josh"]

if __name__ == "__main__":
    try:
        # Get any user-overrides from the payload
        event = json.loads(sys.argv[1])
        names_to_run = event["name"] if "name" in event else NAMES

        # Dump this to dump.txt, which will be parsed by argo in the yaml file to generate the next steps
        with open("/app/dump.txt", "w") as f:
            json.dump(names_to_run, f)
    except Exception as e:
        # Catch the error and log, so that it will be sent to PagerDuty
        logger.error("Error generating names! {}".format(str(e)))
        raise
