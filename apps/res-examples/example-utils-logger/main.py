import os
from res.utils import logger
import random

log_levels = [
    "debug",
    "info",
    "warn",
    "warning",
    "error",
    "critical",
    "incr",
    "set_gauge",
]
log_message = "send_test_logs_amount"

# Pulls env vars from Argo UI and creates a log
if __name__ == "__main__":
    log_level = os.getenv("LOG_LEVEL")
    msg = os.getenv("MESSAGE")
    env = os.getenv("RES_ENV")

    logger.info(
        "Logging a message at level: {} in environment: {}".format(log_level, env)
    )

    ll = log_level.lower()
    if ll not in log_levels:
        logger.error("Error! Log level must be one of: {}".format(str(log_levels)))
    elif ll == "debug":
        logger.debug(msg)
    elif ll == "info":
        logger.info(msg)
    elif ll == "warn" or ll == "warning":
        logger.warn(msg)
    elif ll == "error":
        logger.error(msg)
    elif ll == "incr":
        logger.incr(log_message, 10)
    elif ll == "set_gauge":
        logger.set_gauge(stat="res_gauge_test", value=random.randint(0, 10))
    else:
        logger.critical(msg)
