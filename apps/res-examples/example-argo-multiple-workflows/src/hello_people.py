import os, sys, json
from res.utils import logger

people = ["Marianne", "Ben"]

if __name__ == "__main__":
    for person in people:
        logger.info(f"Hello {person}!")
    with open("/app/dump.txt", "w") as f:
        json.dump([], f)
