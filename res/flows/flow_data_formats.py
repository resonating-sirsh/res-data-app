"""
We are trying to standardize on how flows save data to S3 
data can be images,json,yaml or dataframes with columnar data
The s3 connector will manage the types of each of these based on paths
The flow writer will decide to write elements or collections

When we save data there needs to be consistency in the IO. For example
We save arrays as numpy arrays which can be stacked or flattened as required

When we load data we will want to convert them easily into the most
convenient formats and this file contains the different formatters.

The formatters are tested against our current IO protocols
"""

import pandas as pd
import numpy as np
from shapely.geometry import Polygon, MultiPolygon


def extract_polygons(df, column, return_shapely=True):
    """
    We store results in dataframes (feather) and this is a convenient way to get polygons out of the data
    """

    def extract_polygon_from_nparrays(ar):
        return np.stack(ar).tolist()

    pols = [extract_polygon_from_nparrays(row[column]) for _, row in df.iterrows()]
    if not return_shapely:
        return pols

    return MultiPolygon([Polygon(p) for p in pols])
