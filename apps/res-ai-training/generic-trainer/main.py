"""
The Trainer code can run locally or be deployed via Dockerfile to an Argoworkflow. 
An example dockerfile contents is shown here - package as required


MIGHT BE AN IDEA TO KEEP THEN IN RES DATA AND THE COPY THE BUILD CONTEXT
- COPY MODELS
- COPY DATASETS
- main.py imports and executes


##./Dockerfile 
##build and push this to ECR for use in the argo workflow running on EKS
##this captures the requirements but any dockerfile that copies main.py makes sense for this simple example
#
    FROM python:3.10
    RUN apt-get update && apt-get install -y bash gcc python3-dev musl-dev 
    RUN  pip install torch transformers[torch] accelerate datasets numpy Pillow scikit-learn tensorboard boto3
    ADD ./apps/res-ai-training/vit-trainer-a/ /app/
    CMD ["python", "/app/main.py"]
#
###

"""

import os
import torch
from datasets import load_dataset
from torch import cuda
from SegFormerTrainer import get_trainer
import pandas as pd

PROCESS_NAME = "trainer-a"

from glob import glob
import numpy as np
from datasets import Dataset, DatasetDict, Image


def load_dataset(
    #
    # uri="/Users/sirsh/Documents/datasets/body-pieces/body_pieces_seam_labelled",
    uri="s3://res-data-platform/samples/datasets/body_pieces_seam_labelled.tar.gz",
):
    """
    if its s3 assumes its got data.tar and fetch
    this is just for the segmentation case for now
    """

    if "s3://" in uri:
        from res.connectors import load

        s3 = load("s3")
        name = uri.split("/")[-1].split(".")[0]
        destination = f"/tmp/{name}"

        # the uri becomes the destination e.g. an archive s3://../dataset.tar.gz -> /tmp/dataset ... and this contains images or whatever
        output_root = s3.unpack_folder_from_archive(
            uri,
            "/tmp",
        )
        uri = destination

    def create_dataset(image_paths, label_paths):
        dataset = Dataset.from_dict(
            {"pixel_values": sorted(image_paths), "label": sorted(label_paths)}
        )
        dataset = dataset.cast_column("pixel_values", Image())
        dataset = dataset.cast_column("label", Image())
        return dataset

    # note the convention here - images folder is assumed while the base folder is the folder holder
    df = pd.DataFrame(glob(f"{uri}/images/*.png"), columns=["image"])
    print(f"{len(df)} images at location {uri}/images/*.png")
    df["label"] = df["image"].map(lambda x: x.replace("/images/", "/labels/"))
    df["train"] = df["image"].map(lambda x: np.random.rand() > 0.2)

    train_dataset = create_dataset(df[df["train"]]["image"], df[df["train"]]["label"])
    validation_dataset = create_dataset(
        df[~df["train"]]["image"], df[~df["train"]]["label"]
    )

    dataset = DatasetDict(
        {
            "train": train_dataset,
            "validation": validation_dataset,
        }
    )

    return dataset


def upload_file(source, bucket="res-data-platform", key=None):
    print(f"Uploading {source}")
    import boto3
    import os

    client = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
    )
    key = key or source.split("/")[-1]
    path = f"test-models-out/{key}"
    print(f"Uploading {source} to s3://{bucket}/{path}")
    client.upload_file(source, bucket, path)


def tb(log_dir):
    import subprocess

    print("launch tensor board sub process")
    p = subprocess.Popen(
        [
            "tensorboard",
            "--samples_per_plugin",
            "images=100",
            "--logdir",
            log_dir,
            "--bind_all",
        ],
        start_new_session=True,
    )

    print("spawned...")


if __name__ == "__main__":
    out_dir = "./segformer"

    tb(log_dir=out_dir)

    """
    takes args
    """
    from glob import glob

    print(list(glob("/app/*")))
    print(list(glob("/app/S3ImageDataset/*")))

    image_dataset = "sim_body_pieces"
    image_dataset = "sim_body_pieces_sketched"
    # image_dataset = "fisheye"
    # image_dataset = "buttongate"
    uri = f"s3://res-data-platform/samples/datasets/{image_dataset}.tar.gz"
    print("Dataset", image_dataset)

    dataset_loc = os.environ.get("DATASET_ROOT", "/app/S3ImageDataset/")
    # for now until we figure out a nice pattern for loading segmentation datasets from s3. should be easy
    prepared_ds = load_dataset(uri)  # load_dataset(dataset_loc, image_dataset)

    device = "cuda" if cuda.is_available() else "cpu"
    cuda.empty_cache()
    print(f"<<<<<<DEVICE: {device}>>>>>>")
    if device == "cuda":
        print(f"<<<<<<CUDA VERSION: {torch.version.cuda}>>>>>>")

    fp16 = os.environ.get("NO_FP16", False) != "true"

    trainer = get_trainer(prepared_ds, epochs=1000, fp16=fp16)
    print("<<<<<<TRAIN NOW>>>>>>")
    train_results = trainer.train()
    print("<<<<<<SAVE NOW>>>>>>")
    trainer.save_model()
    trainer.log_metrics("train", train_results.metrics)
    trainer.save_metrics("train", train_results.metrics)
    trainer.save_state()
    print("<<<<<<UPLOADING TO S3>>>>>>")
    upload_file(
        f"{out_dir}/{PROCESS_NAME}/{image_dataset}/pytorch_model.bin", key=image_dataset
    )
    upload_file(
        f"{out_dir}/{PROCESS_NAME}/{image_dataset}/config.json", key=image_dataset
    )
    print("<<<<<DONE>>>>>>")

    print("<<<<<UPLOADING...>>>>>>")
    from res.connectors import load

    s3 = load("s3")
    for folder in glob("/segformer/*"):
        s3.upload_folder_to_archive(
            folder=folder,
            target_folder=f"s3://res-data-platform/models/runs/{image_dataset}/checkpoints/",
        )
