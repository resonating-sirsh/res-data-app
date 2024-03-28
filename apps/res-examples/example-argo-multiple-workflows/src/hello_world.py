import os, sys, json
from res.utils import logger

if __name__ == "__main__":
    logger.info(f"Hello World!")
    with open("/app/dump.txt", "w") as f:
        json.dump([], f)
