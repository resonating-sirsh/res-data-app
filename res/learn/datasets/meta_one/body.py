import numpy as np
from PIL import Image
from pathlib import Path
import res
import pandas as pd
import json


def _dir(f):
    return f"/".join(f.split("/")[:-1])


def classes_from_files(uri='"s3://meta-one-assets-prod/bodies/3d_body_files/"'):
    """
    Go through the json files and collect all the classes
    factorize over the set mapping non mesh codes to a number
    later we will convert the images by mapping the older classes to the new ones for just the 5 character part codes like BKYKE, BKPNL etc
    """
    s3 = res.connectors.load("s3")
    files = [a for a in s3.ls(uri) if "mask_data" in a]

    # start a mapping for each file/piece
    class_mapping = []
    for file in files:
        if ".json" in file:

            dj = s3.read(file)
            for k, v in dj.items():
                class_mapping.append(
                    {
                        "uri": file,
                        "piece": k,
                        "id": v,
                    }
                )

    # get all the file mappings and factorize the part codes we care about
    class_mapping = pd.DataFrame(class_mapping)
    class_mapping["part"] = class_mapping["piece"].map(lambda x: x[3:8])
    class_mapping = class_mapping[
        class_mapping["piece"].map(lambda x: "mesh_" not in x)
    ]

    # map each 5char code to a number with 1 indexing
    class_mapping["pid"] = pd.factorize(class_mapping["part"])[0] + 1
    label2id = dict(class_mapping[["part", "pid"]].drop_duplicates().values)
    # segformer will treat this background
    label2id["Background"] = 0

    # this is to keep a key to map json mappings per mask/image
    def _dir(f):
        return f"/".join(f.split("/")[:-1])

    # we can lookup the converted per file
    class_mapping["dir"] = class_mapping["uri"].map(_dir)
    convert_map = {}
    for k, v in class_mapping.groupby("dir"):
        convert_map[k] = dict(v[["id", "pid"]].astype(np.uint8).values)

    return class_mapping, label2id


def convert_and_save_images_and_masks(
    files,
    class_mapping,
    id_mapping,
    target_folder=f"/Users/sirsh/Documents/datasets/body-pieces/sim_body_pieces/",
):
    """
    Convert the image masks to the class ids we want and convert background to 0
    do not save anything if the mask contains only the background which can happen for example if the labels were for meshes and things we dont care about
    write the image and the mask to the /images and /labels folder under the target with matching file names
    upload the tar file to s3 for use in training
    """

    s3 = res.connectors.load("s3")

    target = target_folder
    Path(f"{target}/labels").mkdir(exist_ok=True)
    Path(f"{target}/images").mkdir(exist_ok=True)

    dirs = []
    for uri in files:
        # parse out some naming stuff
        b = uri.split("/")[5]
        v = uri.split("/")[6]
        f = uri.split("/")[-1]
        f = f"{b}-{v}-{f}"
        dr = _dir(uri)
        mask_map = class_mapping.get(dr)
        if not mask_map:
            res.utils.logger.info(f"there is no mask map for {dr}")
            continue

        def map_classes(x):
            if x == 255:
                return 255
            return mask_map.get(x, 0)

        def ensure_background(x):
            """ """
            if x == 255:
                return 0
            return x

        map_classes = np.vectorize(map_classes)
        ensure_background = np.vectorize(ensure_background)

        if "mask.png" in uri:
            try:
                # the image is not a mask but an RGBA so convert e.g. as below
                im = s3.read(uri)
                mask = im[..., 3] == 0
                # turn it to a white background RGB
                im[mask] = [255, 255, 255, 255]
                rgb = Image.fromarray(im).convert("RGB")
                # then gray it to create the mask we really need
                mask = np.asarray(rgb.convert("L"))

                """
                apply the class mappings
                """
                was = np.unique(mask)
                mask = map_classes(mask)
                mask = ensure_background(mask)
                now = np.unique(mask)
                print(f"{uri=} {was=}, {now=}, number of classes is {len(now)}")

                # some are degenerate so we can ignore in the training data
                if set(now) == {0}:
                    print("Nothing interesting here - background")
                    continue

                # save the images and labels to the expected paths
                Image.fromarray(mask.astype(np.uint8)).save(f"{target}/labels/{f}")
                s3.read_image(uri.replace("_mask", "")).convert("RGB").save(
                    f"{target}/images/{f}"
                )
                dirs.append(dr)
            except Exception as ex:
                raise
                res.utils.logger.info(f"problem with {uri} - {ex}")
    # upload the stuff
    res.utils.logger.info("uploading dataset")
    s3.upload_folder_to_archive(
        folder=target_folder,
        target_folder=f"s3://res-data-platform/samples/datasets",
    )
