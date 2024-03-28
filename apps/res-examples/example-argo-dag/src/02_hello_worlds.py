import os, sys, json
from res.utils import logger

if __name__ == "__main__":
    try:
        event = sys.argv[1]
        print(f"Hello {event}!")
        # Write an empty file
        with open("/app/dump.txt", "w") as f:
            json.dump({}, f)
    except Exception as e:
        # Try to get the event to print in the error message
        event = event if "event" in locals() else '["bad event payload"]'
        logger.error("Error running step!{} {}".format(json.dumps(event), str(e)))
        raise
