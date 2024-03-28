import pytest


@pytest.mark.media
def test_nest_basic():
    from res.learn.optimization import nest

    # TODO:
    shapes = []

    assert 1 > 0, "nope"


# TODO get a test event for something pieces that have already been split - we really only need a job id
# import res

# %matplotlib inline
# from matplotlib import pyplot as plt
# import numpy as np
# from skimage import data, io, filters
# from PIL import Image
# from res.connectors.s3 import S3Connector
# s3 = S3Connector()
# Image.MAX_IMAGE_PIXELS = None

# flow_event = """
# apiVersion: v0
# kind: Flow
# metadata:
#  name: dxa.printfile
#  version: expv0
# args:
#  images:
#  - s3://resmagic/uploads/c85be3b7-8f4f-4fe5-9e5d-6834baf191a3.png
#  - s3://resmagic/uploads/fe34a7f4-2245-49af-bd57-9829ebaa30b3.png
#  - s3://resmagic/uploads/28b63f32-8e8f-4982-86e4-ab204cf34e28.png
#  - s3://resmagic/uploads/ef0a662c-75e9-452f-9c1d-ebe0df2af8ab.png
#  - s3://resmagic/uploads/f75ce9ad-dd64-4565-8dc5-e8cfcd963c4b.png
#  jobKey: dewyn-job
#  material: LTCSL
#  buffer: 10
# nodes:
# """
# import yaml
# import json
# event = yaml.safe_load(flow_event)
# e = json.dumps(event)
# print(e)
# event

# from res.flows.dxa import printfile

# printfile.perform_nesting(event,context= {})
