"""
 
###

"""

import os
import torch
from datasets import load_dataset
from torch import cuda
import pandas as pd
import json
from res.connectors import load
import pickle

PROCESS_NAME = "movie-trainer-a"

from tqdm import tqdm
from glob import glob
import numpy as np
from datasets import Dataset, DatasetDict, Image

from Movie import Movie
from SAM import get_model, show_anns


def load_dataset(
    #
    # uri="/Users/sirsh/Documents/datasets/body-pieces/body_pieces_seam_labelled",
    uri="s3://res-data-platform/samples/movies/print/XNHF4639.MP4",
):
    """
    if its s3 assumes its got data.tar and fetch
    this is just for the segmentation case for now
    """

    if "s3://" in uri:

        s3 = load("s3")

        print("downloading datasets...")
        # fetch the same model that we need
        s3._download(
            "s3://res-data-platform/models/sam/sam_vit_h_4b8939.pth", target_path="/tmp"
        )

        # and get the dataset
        return s3._download(uri, target_path="/tmp")

    return None


# def upload_file(source, bucket="res-data-platform", key=None):
#     print(f"Uploading {source}")
#     import boto3
#     import os

#     client = boto3.client(
#         "s3",
#         aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
#         aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
#     )
#     key = key or source.split("/")[-1]
#     path = f"test-models-out/{key}"
#     print(f"Uploading {source} to s3://{bucket}/{path}")
#     client.upload_file(source, bucket, path)


# def tb(log_dir):
#     import subprocess

#     print("launch tensor board sub process")
#     p = subprocess.Popen(
#         [
#             "tensorboard",
#             "--samples_per_plugin",
#             "images=100",
#             "--logdir",
#             log_dir,
#             "--bind_all",
#         ],
#         start_new_session=True,
#     )

#     print("spawned...")


if __name__ == "__main__":

    """
    takes args
    """
    from glob import glob
    from pathlib import Path

    p = Path("/tmp/data")
    p.mkdir()

    print(list(glob("/app/*")))

    # for now until we figure out a nice pattern for loading segmentation datasets from s3. should be easy
    prepared_ds = load_dataset()  # load_dataset(dataset_loc, image_dataset)

    m = Movie(prepared_ds)
    mask_generator = get_model()
    s3 = load("s3")

    print("processing frames")
    frames = list(range(0, m._num_frames, 10))
    for frame_index in tqdm(frames):
        try:
            frame = np.asarray(m[frame_index])
            masks = mask_generator.generate(frame)

            with open(f"/tmp/data/frame_masks{frame_index}.pkl", "wb") as f:
                pickle.dump(masks, f)
        except Exception as ex:
            print(f"failed from {frame_index} {ex}")

        # break
    print("uploading data...")

    s3.upload_folder_to_archive(
        folder="/tmp/data",
        target_folder=f"s3://res-data-platform/samples/datasets/movie-masks{prepared_ds.split('.')[0]}",
    )
