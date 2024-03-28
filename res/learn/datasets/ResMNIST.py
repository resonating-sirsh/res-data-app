"""
dummy code for sketch
"""

from torchvision import datasets
import res


class ResMNIST(datasets.MNIST):
    mirrors = [
        res.connectors.load("s3").generate_presigned_url(
            "s3://res-data-platform/samples/training-data/ResMNISTGray10Class"
        )
    ]
    resources = [
        ("train-images-idx3-ubyte", "8d4fb7e6c68d591d4c3dfef9ec88bf0d"),
        ("train-labels-idx1-ubyte", "25c81989df183df01b3e8a0aad5dffbe"),
        ("t10k-images-idx3-ubyte", "bef4ecab320f06d8554ea6380940ec79"),
        ("t10k-labels-idx1-ubyte", "bb300cfdad3c16e7a12a480ee83cd310"),
    ]
    classes = [
        "BKPNL",
        "BKYKE",
        "FTPKB",
        "FTPKT",
        "FTPNL",
        "NKCLR",
        "NKCLS",
        "SDPKB",
        "SLCUF",
        "SLPNL",
    ]
