import os
import numpy as np
import pandas as pd
from PIL import Image
import datetime
from res.flows.dxa.inspectors.dxa_gpt4v_utils import (
    save_run_as_markdown,
    # pyvips_to_base64,
    pil_to_base64,
    image_path_to_base64,
    extract_json,
    Stats,
    unmap,
    get_thumbnail_from_s3,
    get_thumbnail_from_file,
    pyvips_to_pil,
)

import res
from res.utils.logging import logger

# from res.utils.secrets import secrets
# os.environ["OPENAI_API_KEY"] = secrets.get_secret("ASK_ONE_OPENAI_API_KEY")
# from openai import OpenAI
# client = OpenAI()


# roll an image so you can centre the edges
def roll(image, x=0.5, y=0.5):
    x_pixels = x if isinstance(x, int) else int(image.size[1] * x)
    y_pixels = y if isinstance(y, int) else int(image.size[0] * y)

    arr = np.array(image)
    arr = np.roll(arr, x_pixels, axis=0)
    arr = np.roll(arr, y_pixels, axis=1)

    return Image.fromarray(arr)


def distance_scan(
    file,
    distance_threshold=100,
    continuous_threshold=30,
    needs_rolling=False,
    check_type="center",
    debug=False,
):
    s3 = res.connectors.load("s3")
    """
    Scan the image line by line and see which line has the biggest jump in distance between pixels.

    if check_type == "center", then we check the center line of the image
    otherwise we see if one line is significantly different to the others
    """
    logger.info(f"distance_scan: {file}, threshold: {distance_threshold}")
    img = file

    # if isinstance(file, str):
    #     if file.startswith("s3://"):
    #         img = pyvips_to_pil(get_thumbnail_from_s3(file))
    #     else:
    #         img = get_thumbnail_from_file(file)
    # don't get a thumbnail, the blurring might mask the tiling error
    if isinstance(file, str):
        if file.startswith("s3://"):
            img = s3.read_image(file)
        else:
            img = Image.open(file)

    # convert the pyvips image to a numpy array
    arr = np.array(img)

    color = [0, 0, 255] if arr.shape[2] == 3 else [0, 0, 255, 255]

    if needs_rolling:
        arr = np.array(roll(img, 0.5, 0.5))
    arr_copy = arr.copy()

    logger.info(f"arr shape: {arr.shape}")

    # do a horizontal and vertical scan, checking for the continuous maximum distance between pixels
    scan_params = {
        "horizontal": {
            "axis": 1,
            "axis_size": arr.shape[0],
            "other_axis_size": arr.shape[1],
        },
        "vertical": {
            "axis": 0,
            "axis_size": arr.shape[1],
            "other_axis_size": arr.shape[0],
        },
    }

    result = {}
    for scan, params in scan_params.items():
        max_continuous_distance = 0
        max_continuous_distance_row = 0
        # avg_continuous_distance = 0
        continuous_distance_dict = {}
        for y in range(1, params["axis_size"]):
            total_continuous_distance = 0
            continuous_distance_count = 0
            prev_dist = 0
            for x in range(params["other_axis_size"]):
                prev = arr[y - 1, x] if scan == "horizontal" else arr[x, y - 1]
                curr = arr[y, x] if scan == "horizontal" else arr[x, y]

                # if either are transparent forget it
                if arr.shape[2] == 4 and (prev[3] < 255 or curr[3] < 255):
                    continuous_distance_count = 0
                    continue

                # work out the distance between the two pixels
                dist = np.linalg.norm(prev - curr)
                if dist > distance_threshold and prev_dist > distance_threshold:
                    continuous_distance_count += 1
                else:
                    continuous_distance_count = 0

                if continuous_distance_count >= continuous_threshold:
                    # if scan == "horizontal":
                    #     arr_copy[y, x - continuous_distance_count : x] = color
                    # else:
                    #     arr_copy[x - continuous_distance_count : x, y] = color

                    total_continuous_distance += (
                        continuous_threshold
                        if continuous_distance_count == continuous_threshold
                        else 1
                    )

                prev_dist = dist

            continuous_distance_dict[y] = continuous_distance_count
            if total_continuous_distance > max_continuous_distance:
                max_continuous_distance = total_continuous_distance
                max_continuous_distance_row = y

            # avg_continuous_distance = (
            #     avg_continuous_distance * (y - 1) + continuous_distance_count
            # ) / y
            continuous_distance_dict[y] = total_continuous_distance

        logger.info(
            f"{scan} scan: max: {max_continuous_distance}, max_row: {max_continuous_distance_row}"
        )

        if check_type == "center":
            # if it's the centre row, it's probably a bad tile
            bad_tiling = (
                max_continuous_distance_row == params["axis_size"] // 2
                or max_continuous_distance_row == params["axis_size"] // 2 - 1
            )
            logger.info(
                f"{scan} scan: {'does NOT tile well' if bad_tiling else 'tiles well'}"
            )
            result[f"{scan}_tiles_poorly"] = bad_tiling
            result[f"{scan}_breaks_at"] = max_continuous_distance_row
        else:
            # is there any one that is significantly different to the others?
            bad_index = None
            df = pd.DataFrame.from_dict(continuous_distance_dict, orient="index")
            df = df.sort_values(by=0, ascending=False)

            if debug:
                print(df.head(10))

            # bad_index = df.index[0]

            def find_outliers(arr):
                z_scores = np.abs((arr - np.mean(arr)) / np.std(arr))
                # Adjust as needed, typical value is 2.0 or 3.0 for significance
                threshold_z_score = 2
                outliers = np.where(z_scores > threshold_z_score)[0]
                return outliers

            # if there is one that is significantly different to the others flag it
            outliers = find_outliers(df[0][:10])
            print(f"outliers: {outliers}")
            if len(outliers) > 0:
                bad_index = df.index[outliers[0]]
                logger.info(f"tiling interrupted {scan}ly at {bad_index}")
                logger.info(f"{df.iloc[outliers[0]]} is an outlier")
            else:
                logger.info(f"{scan} scan: tiles well")

            # # if the worst row is much worse that the previous two flag it
            # worst_to_second_worst = (df.iloc[0] - df.iloc[1]).iloc[0]
            # second_worst_to_third_worst = (df.iloc[1] - df.iloc[2]).iloc[0]
            # if worst_to_second_worst > second_worst_to_third_worst * 2:
            #     bad_index = df.index[0]
            #     logger.info(f"tiling interrupted {scan}ly at {bad_index}")
            #     logger.info(f"{worst_to_second_worst} > {second_worst_to_third_worst}")
            # else:
            #     logger.info(f"{scan} scan: tiles well")

            result[f"{scan}_breaks_at"] = bad_index

    if debug:
        color = [255, 0, 0] if arr.shape[2] == 3 else [255, 0, 255, 255]

        for scan, params in scan_params.items():
            index = result.get(f"{scan}_breaks_at")
            if index is not None:
                if scan == "horizontal":
                    arr_copy[index, :: arr.shape[0] // 30] = color
                else:
                    arr_copy[:: arr.shape[1] // 30, index] = color

        img = Image.fromarray(arr_copy)
        return img

    logger.info(f"result: {result}")

    return result


def distance_scan_np(
    file,
    distance_threshold=100,
    continuous_threshold=30,
    needs_rolling=False,
    check_type="center",
    debug=False,
    return_image=False,
):
    """
    Scan the image line by line and see which line has the biggest jump in distance between pixels.

    if check_type == "center", then we check the center line of the image
    otherwise we see if one line is significantly different to the others
    """
    s3 = res.connectors.load("s3")
    logger.info(f"distance_scan: {file}, threshold: {distance_threshold}")
    img = file

    # if isinstance(file, str):
    #     if file.startswith("s3://"):
    #         img = pyvips_to_pil(get_thumbnail_from_s3(file))
    #     else:
    #         img = get_thumbnail_from_file(file)
    if isinstance(file, str):
        if file.startswith("s3://"):
            img = s3.read_image(file)
        else:
            img = Image.open(file)

    # convert the pyvips image to a numpy array
    arr = np.array(img)

    color = [0, 0, 255] if arr.shape[2] == 3 else [0, 0, 255, 255]

    if needs_rolling:
        arr = np.array(roll(img, 0.5, 0.5))
    # to make it easier to debug, I'm going to get a centre square
    f = 0
    if f:
        arr = arr[
            arr.shape[0] // 2 - f : arr.shape[0] // 2 + f,
            arr.shape[1] // 2 - f : arr.shape[1] // 2 + f,
        ]

    arr_copy = arr.copy()

    # do a horizontal and vertical scan, checking for the continuous maximum distance between pixels
    scan_params = {
        "horizontal": {
            "diff_axis": 0,
            "axis_size": arr.shape[0],
            "other_axis_size": arr.shape[1],
        },
        "vertical": {
            "diff_axis": 1,
            "axis_size": arr.shape[1],
            "other_axis_size": arr.shape[0],
        },
    }

    logger.info(f"shape: {arr.shape}")

    # make any transparent bits black
    if arr.shape[2] == 4:
        arr[arr[:, :, 3] == 0] = [0, 0, 0, 0]

    # make sure it's int8 as uint8 will overflow
    arr = arr.astype(np.int16)

    result = {}
    for scan, params in scan_params.items():
        arr = np.swapaxes(arr, 0, params["diff_axis"])
        # get the distance between each pixel and the one in the previous row
        diffs = np.diff(arr, axis=0)
        dists = np.linalg.norm(diffs, axis=2)
        # dists = np.vstack([np.zeros((1, dists.shape[1])), dists])

        if f:
            return arr, diffs, dists

        # set them to True if they are above the threshold
        dists = dists > distance_threshold
        result[f"{scan}_dists"] = dists

        def count_continuous_trues(arr):
            # Find the indices where the transitions occur
            transitions = np.where(np.diff(np.concatenate(([False], arr, [False]))))[0]

            # Calculate the lengths of continuous True sequences
            lengths = np.diff(transitions)[::2]

            return lengths.tolist()

        continuous_dists = [count_continuous_trues(row) for row in dists]
        # only include a continuous dist if it's above the continuous threshold
        continuous_dists = [
            [x for x in row if x >= continuous_threshold] for row in continuous_dists
        ]

        # sum up all the continuous distances and normalise them
        summed = [sum(row) for row in continuous_dists]
        normed = (summed - np.min(summed)) / (np.max(summed) - np.min(summed))

        with_index = {i: round(normed[i], 3) for i in range(len(normed))}

        mid = params["axis_size"] // 2
        logger.info(f"{scan} mid values={[round(x, 3) for x in normed[mid-1:mid+2]]}")

        # sort it by value
        sorted_dists = sorted(with_index.items(), key=lambda x: x[1], reverse=True)

        # only pick the top n
        n = 50
        sorted_dists = sorted_dists[:n]
        result[scan] = sorted_dists

        logger.info(f"{scan} scan (index, continuous change): {sorted_dists}")
        logger.info(f"standard deviation: {np.std(normed)}")

        # if the worst one is in the centre, it's probably a bad tile
        # worst_index = sorted_dists[0][0]
        # if mid - 1 <= worst_index <= mid + 1:
        #     result[f"{scan}_tiles_poorly"] = True
        #     result[f"{scan}_breaks_at"] = sorted_dists[0][0]

        # how many values are >= the mid_value?
        low_mid = mid - 2
        high_mid = mid + 2
        mid_value = max(normed[low_mid:high_mid])
        peers = len([x for x in normed[:low_mid] if x >= mid_value])
        peers += len([x for x in normed[high_mid + 1 :] if x >= mid_value])
        if mid_value > 0.98:
            if peers == 0:  # just the mid ones have the worst change
                # if there's only a diff of .1 with fifth highest it's prob ok
                # if sorted_dists[0][1] - sorted_dists[4][1] > 0.1:
                result[f"{scan}_tiles_poorly"] = True
                result[f"{scan}_breaks_at"] = sorted_dists[0][0]
            else:
                # TODO: if there's some sort of striping, is it regular?
                # might be a repeating pattern, if it's the same either side
                # of mid but not across mid it probably doesn't tile well
                indexes = [x[0] for x in sorted_dists if x[1] >= mid_value]
                indexes = sorted(indexes)

                below = indexes[: len(indexes) // 2]
                above = indexes[len(indexes) // 2 :]

                logger.info(f"  below: {below}")
                logger.info(f"  above: {above}")

    if result.get("horizontal_tiles_poorly") or result.get("vertical_tiles_poorly"):
        logger.info("tiles poorly")

    if debug or return_image:
        color = [255, 0, 255] if arr.shape[2] == 3 else [255, 0, 255, 255]

        if debug:
            for scan, params in scan_params.items():
                dists = result[f"{scan}_dists"]

                def draw_conts(index):
                    for i, v in enumerate(dists[index]):
                        if v:  # draw over the bits that are ok so bad bits highlighted
                            continue
                        if scan == "horizontal":
                            arr_copy[index, i] = color
                        else:
                            arr_copy[i, index] = color

                def draw_dots(index, gap=8):
                    if scan == "horizontal":
                        arr_copy[index, ::gap] = color
                    else:
                        arr_copy[::gap, index] = color

                color[0] = 255
                index = result.get(f"{scan}_breaks_at")
                if index is not None:
                    draw_conts(index)

                draw_conts(params["axis_size"] // 2 - 1)

                top = result[scan][:5]
                color[0] = 0
                for index, _ in top:
                    if index == result.get(f"{scan}_breaks_at"):
                        continue

                    draw_conts(index)

        img = Image.fromarray(arr_copy)
        return img

    # logger.info(f"result: {result}")
    del result["horizontal_dists"]
    del result["vertical_dists"]
    del result["horizontal"]
    del result["vertical"]

    return result


# these values seem to work well on test data, pretty harsh though
DIST_THRESH = 5
CONT_THRESH = 5


def inspect_tileability(
    s3_path=None, batch=None, return_result_as_image=False, debug=False
):
    if batch and not s3_path:
        hasura = res.connectors.load("hasura")

        # find a bunch that don't have the "tileable" metadata prop
        Q = """
            query get_untested_tileability($batch: Int!) {
                meta_artworks(
                    where: {
                        _not: {metadata: {_has_key: "tileable" } }
                    },
                    limit: $batch
                ) {
                    id
                    dpi_300_uri
                    metadata
                }
            }
        """
        rows = hasura.execute_with_kwargs(Q, batch=batch)["meta_artworks"]

        U = """
            mutation set_tileable($id: uuid!, $metadata: jsonb!) {
                update_meta_artworks_by_pk(
                    pk_columns: {id: $id},
                    _set: {metadata: $metadata}
                ) {
                    id
                }
            }
        """

        results = {}
        for row in rows:
            uri = row["dpi_300_uri"]
            metadata = row["metadata"]
            logger.info(f"checking {uri}")
            tiling_problems = distance_scan_np(
                uri,
                needs_rolling=True,
                debug=False,
                return_image=False,
                distance_threshold=DIST_THRESH,
                continuous_threshold=CONT_THRESH,
            )

            if tiling_problems:
                logger.info(f"tiling problems: {tiling_problems}")

            results[row["id"]] = "tiles poorly" if tiling_problems else "tiles well"
            metadata["tileable"] = not tiling_problems
            metadata["tileable_checked_at"] = datetime.datetime.now().isoformat()

            hasura.execute_with_kwargs(U, id=row["id"], metadata=metadata)

        return results
    elif s3_path:
        return distance_scan_np(
            s3_path,
            needs_rolling=True,
            debug=debug,
            return_image=return_result_as_image,
            distance_threshold=DIST_THRESH,
            continuous_threshold=CONT_THRESH,
        )
    else:
        raise ValueError("Either s3_path or batch must be provided")
