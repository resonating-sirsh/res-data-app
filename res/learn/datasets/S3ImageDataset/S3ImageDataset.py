# https://huggingface.co/datasets/food101/raw/main/food101.py
# https://huggingface.co/docs/datasets/main/en/image_dataset#loading-script

from res.connectors import load as load_connector

S3_ROOT = "s3://res-data-platform/samples/datasets"

# GENERIC
import datasets
from datasets.tasks import ImageClassification
import tarfile
from pathlib import Path
from tqdm import tqdm
from numpy import random
import pandas as pd
from glob import glob

MY_DATA_SET_NAME = "ResMNISTFiles"
TILE_CHECK_DATA_SET = "tileable_data"
COLOR_PIECES_DATA_SET = "color_piece_parts"
BODY_PIECES_MASKS_DATA_SET = "body_pieces_masks"

SEGMENT_SEAMS = "body_pieces_seam_labelled"

s3 = load_connector("s3")


def make_s3_dataset(
    image_dir,
    test_split_below=0.2,
    target_s3_root=S3_ROOT,
):
    """
    A folder containing a named collection of classes with images e.g.
    This is to conform to a huggingface dataset example image dataset
    for example see https://huggingface.co/datasets/food101/raw/main/food101.py
    the one thing we add is to use the class names as a metadata file rather than
    a collection in the class which makes it more generic

    MY_COLLECTION/MY_CLASS/my_file

    Example usage:> pointing to a folder with classes and images

        from res.learn.datasets.s3_image_dataset import S3ImageDataset, make_s3_dataset
        make_s3_dataset("/Users/sirsh/Documents/datasets/gray-pieces/ResMNISTFiles")
    """

    collection_name = Path(image_dir).name

    tar_file_name = f"/tmp/{collection_name}.tar.gz"
    print(f"packing {image_dir} into {tar_file_name}")
    with tarfile.open(tar_file_name, "w:gz") as tar:
        items = []
        for name in tqdm(glob(f"{image_dir}/**/*.png", recursive=True)):
            p = Path(name)
            class_item = p.relative_to(image_dir)
            items.append(
                {
                    "name": f"{str(class_item)}",
                    "class": str(class_item.parent),
                    "train": random.rand() > test_split_below,
                }
            )
            tar.add(name, arcname=f"{collection_name}/images/{class_item}")

    upload_uri = f"{target_s3_root}/{Path(tar_file_name).name}"
    print(f"uploading to {upload_uri}")

    """
    NOTE!! you need to create an s3 utility to to the uploading - not shown here
    otherwise this code is self-contained
    """
    s3.upload(tar_file_name, upload_uri)

    metadata_uri = f"{target_s3_root}-metadata/"
    print(f"publishing train test metadata to  {metadata_uri}")

    df = pd.DataFrame(items)
    # use s3fs to upload csv to S3
    # we could save these to metadata in the gz file if required which masses what the foot101 dataset looks like
    # in that case we would create meta/and save these contents there too
    df[df["train"]][["name"]].to_csv(
        f"{metadata_uri}{collection_name}/train.txt", index=None, header=False
    )
    df[~df["train"]][["name"]].to_csv(
        f"{metadata_uri}{collection_name}/test.txt", index=None, header=False
    )
    df[["class"]].drop_duplicates().to_csv(
        f"{metadata_uri}{collection_name}/classes.txt", index=None, header=False
    )

    print(list(s3.ls(metadata_uri)))

    return df


class S3ImageDatasetConfig(datasets.BuilderConfig):
    """Builder Config for Food-101"""

    def __init__(self, data_url, metadata_urls, **kwargs):
        """BuilderConfig for Food-101.
        Args:
          data_url: `string`, url to download the zip file from.
          metadata_urls: dictionary with keys 'train' and 'validation' containing the archive metadata URLs
          **kwargs: keyword arguments forwarded to super.
        """
        super(S3ImageDatasetConfig, self).__init__(
            version=datasets.Version("1.0.0"), **kwargs
        )
        self.data_url = data_url
        self.metadata_urls = metadata_urls


class S3ImageDataset(datasets.GeneratorBasedBuilder):
    """Generic S3 Images dataset.

    #load with this - you can also pass a sub config e.g.    MY_DATA_SET_NAME can be something other than the default
    from datasets import load_dataset
    ds = load_dataset("/Users/sirsh/code/res/res-data-platform/res/learn/datasets/S3ImageDataset" )
    ds
    """

    _HOMEPAGE = "http://todo.com"
    _CITATION = """\
    NA
    """
    _LICENSE = """\
    NA
    """

    BUILDER_CONFIGS = [
        S3ImageDatasetConfig(
            name=MY_DATA_SET_NAME,
            description="stuff",
            data_url=f"{S3_ROOT}/{MY_DATA_SET_NAME}.tar.gz",
            data_dir=f"{MY_DATA_SET_NAME}/images",
            metadata_urls={
                "train": f"{S3_ROOT}-metadata/{MY_DATA_SET_NAME}/train.txt",
                "test": f"{S3_ROOT}-metadata/{MY_DATA_SET_NAME}/test.txt",
            },
        ),
        S3ImageDatasetConfig(
            name=TILE_CHECK_DATA_SET,
            description="stuff",
            data_url=f"{S3_ROOT}/{TILE_CHECK_DATA_SET}.tar.gz",
            data_dir=f"{TILE_CHECK_DATA_SET}/images",
            metadata_urls={
                "train": f"{S3_ROOT}-metadata/{TILE_CHECK_DATA_SET}/train.txt",
                "test": f"{S3_ROOT}-metadata/{TILE_CHECK_DATA_SET}/test.txt",
            },
        ),
        S3ImageDatasetConfig(
            name=COLOR_PIECES_DATA_SET,
            description="stuff",
            data_url=f"{S3_ROOT}/{COLOR_PIECES_DATA_SET}.tar.gz",
            data_dir=f"{COLOR_PIECES_DATA_SET}/images",
            metadata_urls={
                "train": f"{S3_ROOT}-metadata/{COLOR_PIECES_DATA_SET}/train.txt",
                "test": f"{S3_ROOT}-metadata/{COLOR_PIECES_DATA_SET}/test.txt",
            },
        ),
        S3ImageDatasetConfig(
            name=BODY_PIECES_MASKS_DATA_SET,
            description="stuff",
            data_url=f"{S3_ROOT}/{BODY_PIECES_MASKS_DATA_SET}.tar.gz",
            data_dir=f"{BODY_PIECES_MASKS_DATA_SET}/images",
            metadata_urls={
                "train": f"{S3_ROOT}-metadata/{BODY_PIECES_MASKS_DATA_SET}/train.txt",
                "test": f"{S3_ROOT}-metadata/{BODY_PIECES_MASKS_DATA_SET}/test.txt",
            },
        ),
        S3ImageDatasetConfig(
            name=SEGMENT_SEAMS,
            description="segmentation",
            data_url=f"{S3_ROOT}/{SEGMENT_SEAMS}.tar.gz",
            data_dir=f"{SEGMENT_SEAMS}/images",
            metadata_urls={
                "train": f"{S3_ROOT}-metadata/{SEGMENT_SEAMS}/train.txt",
                "test": f"{S3_ROOT}-metadata/{SEGMENT_SEAMS}/test.txt",
            },
        ),
    ]

    DEFAULT_CONFIG_NAME = MY_DATA_SET_NAME

    def _info(self):
        name = self.config.name

        _NAMES = s3.read_text_lines(f"{S3_ROOT}-metadata/{name}/classes.txt")

        _NAMES = [i.strip("\n") for i in _NAMES]
        return datasets.DatasetInfo(
            description=self.config.description,
            features=datasets.Features(
                {
                    "image": datasets.Image(),
                    "labels": datasets.ClassLabel(names=_NAMES),
                }
            ),
            supervised_keys=("image", "labels"),
            homepage=S3ImageDataset._HOMEPAGE,
            citation=S3ImageDataset._CITATION,
            license=S3ImageDataset._LICENSE,
            task_templates=[
                ImageClassification(image_column="image", label_column="labels")
            ],
        )

    def _split_generators(self, dl_manager):
        print(self.config)
        print(self.config.data_url)
        print(self.config.metadata_urls)

        sign_url = lambda url: s3.generate_presigned_url(url)

        archive_path = dl_manager.download(sign_url(self.config.data_url))
        urls = {k: sign_url(url) for k, url in self.config.metadata_urls.items()}
        split_metadata_paths = dl_manager.download(urls)

        print(split_metadata_paths)

        return [
            datasets.SplitGenerator(
                name=datasets.Split.TRAIN,
                gen_kwargs={
                    "images": dl_manager.iter_archive(archive_path),
                    "metadata_path": split_metadata_paths["train"],
                },
            ),
            datasets.SplitGenerator(
                name=datasets.Split.VALIDATION,
                gen_kwargs={
                    "images": dl_manager.iter_archive(archive_path),
                    "metadata_path": split_metadata_paths["test"],
                },
            ),
        ]

    def _generate_examples(self, images, metadata_path):
        name = self.config.name
        # THIS IS IMPORTANT BECAUSE IT SAYS WHERE IN THE ARCHIVE THE FILES ARE
        # IF WE MISS THIS, WE CAN HAVE 0 ROWS IN OUR SPLITS
        images_dir = f"{name}/images"
        """Generate images and labels for splits."""
        with open(metadata_path, encoding="utf-8") as f:
            files_to_keep = set(f.read().split("\n"))
        for file_path, file_obj in images:
            if file_path.startswith(images_dir):
                if Path(file_path).suffix in [".png", ".jpg"]:
                    label = file_path.split("/")[2]

                    yield file_path, {
                        "image": {"path": file_path, "bytes": file_obj.read()},
                        "labels": label,
                    }
