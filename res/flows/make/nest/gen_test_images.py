# this is just a flow so i can run it on a giant box in aws.
import numpy as np
import cv2
import res
from res.flows import flow_node_attributes
from res.utils import logger


@flow_node_attributes(
    "gen_test_images",
    memory="250G",
    disk="100G",
    mapped=False,
    allow24xlg=True,
)
def handler(event, context):
    s3 = res.connectors.load("s3")
    width = 15000

    heights = [10000, 50000, 100000, 200000, 300000, 400000]

    # generate some all white images
    for height in heights:
        filename = f"white_{height}.png"
        logger.info(filename)
        color = np.full((height, width, 3), 255)
        cv2.imwrite(filename, color)
        s3.upload(filename, f"s3://res-data-development/rip_experiment/{filename}")

    # generate some random nose in RGB -- this should be basically impossible to compress.
    for height in heights:
        filename = f"color_{height}.png"
        logger.info(filename)
        color = np.random.randint(0, high=256, size=(height, width, 3))
        cv2.imwrite(filename, color)
        s3.upload(filename, f"s3://res-data-development/rip_experiment/{filename}")

    # generate a black and white checkerboard -- this should compress easily
    gray_10k = np.fromfunction(
        np.vectorize(lambda x, y: 255 if (x // 1000) % 2 == (y // 1000) % 2 else 0),
        shape=(10000, width),
    )
    for height in heights:
        filename = f"checker_{height}.png"
        logger.info(filename)
        gray = np.tile(gray_10k, (height // 10000, 1))
        cv2.imwrite(filename, gray)
        s3.upload(filename, f"s3://res-data-development/rip_experiment/{filename}")
