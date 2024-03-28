from warnings import filterwarnings

filterwarnings("ignore")

import os
import matplotlib
import pyvips
import base64
import json
from io import BytesIO
from PIL import Image, ImageDraw
import shapely.geometry
import matplotlib.pyplot as plt
from res.flows.dxa.inspectors.dxa_gpt4v_utils import (
    save_run_as_markdown,
    # pyvips_to_base64,
    pil_to_base64,
    image_path_to_base64,
    extract_json,
    Stats,
    unmap,
)

import res
from res.utils.secrets import secrets
from res.utils.logging import logger

os.environ["OPENAI_API_KEY"] = secrets.get_secret("ASK_ONE_OPENAI_API_KEY")

from openai import OpenAI

client = OpenAI()
from res.flows.meta.body.unpack_asset_bundle.body_db_sync import sync_body_to_db

current_dir = os.path.dirname(os.path.realpath(__file__))
path_to_b64 = lambda x: image_path_to_base64(os.path.join(current_dir, x))


def get_geometries(body_code: str, body_version: int, piece: str = "%"):
    hasura = res.connectors.load("hasura")

    GET_GEOMS = """ 
        query get_geometries($body_code: String!, $body_version: numeric!, $piece: String) {
            meta_body_pieces(
                where: {
                    body: { 
                        body_code: {_ilike: $body_code}
                        version: {_eq: $body_version}
                    }
                    deleted_at: {_is_null: true}
                    key: {_ilike: $piece}
                }
            ) {
                key
                inner_geojson
                outer_geojson
            }
        }
    """

    results = hasura.execute_with_kwargs(
        GET_GEOMS, body_code=body_code, body_version=body_version, piece=piece
    )
    return hasura_to_name_size_geometries(results["meta_body_pieces"])


def hasura_to_name_size_geometries(hasura_results, ignore_stampers=True):
    name_size_geometries = {}
    for result in hasura_results:
        key = result["key"]

        parts = key.split("_")
        if len(parts) != 2:
            raise Exception(f"Unexpected key format: {key}")
        [name, size] = parts

        if ignore_stampers and name.endswith("-X"):
            continue

        if name not in name_size_geometries:
            name_size_geometries[name] = {}

        name_size_geometries[name][size] = {
            "inner": result["inner_geojson"],
            "outer": result["outer_geojson"],
        }
    return name_size_geometries


def geojson_to_centred_shape(geojson):
    shape = shapely.geometry.shape(geojson.get("geometry"))
    centre = shape.centroid
    return shapely.affinity.translate(shape, xoff=-centre.x, yoff=-centre.y)


def geometries_to_flattened_images(
    name_size_geometries, thumb_size=512, rescale=False, debug_save_images=False
):
    name_size_shape = {}
    for name in name_size_geometries:
        name_size_shape[name] = {}
        size_geometries = name_size_geometries[name]
        for size in size_geometries:
            geometries = size_geometries[size]
            # shape = geojson_to_centred_shape(geometries["outer"])
            shape = shapely.geometry.shape(geometries["outer"].get("geometry"))
            # inner = geojson_to_centred_shape(geometries['inner'])
            # #union
            # shape = shape.union(inner)
            name_size_shape[name][size] = shape

    the_b64s = {}
    for key, value in name_size_shape.items():
        geometries = list(value.values())
        if rescale:
            first = geometries[0]
            width = first.bounds[2] - first.bounds[0]
            height = first.bounds[3] - first.bounds[1]
            rescaled = []
            for geometry in geometries:
                old_width = geometry.bounds[2] - geometry.bounds[0]
                old_height = geometry.bounds[3] - geometry.bounds[1]
                rescaled.append(
                    shapely.affinity.scale(
                        geometry,
                        xfact=width / old_width,
                        yfact=height / old_height,
                        origin=(0, 0),
                    )
                )
            geometries = rescaled

        # draw the geoms using PIL, let's draw it on the biggest size then scale down
        max_width = int(max([x.bounds[2] - x.bounds[0] for x in geometries]))
        max_height = int(max([x.bounds[3] - x.bounds[1] for x in geometries]))

        # make the buffer and line width proportional to the size of the image
        buffer = int(max(max_width, max_height) / 20)
        line_width = int(max(max(max_width, max_height) / 750, 1))

        max_width += buffer * 2
        max_height += buffer * 2

        img = Image.new("RGB", (max_width, max_height), color="white")
        draw = ImageDraw.Draw(img)
        for geometry in geometries:
            this_width = int(geometry.bounds[2] - geometry.bounds[0])
            this_height = int(geometry.bounds[3] - geometry.bounds[1])
            geometry = shapely.affinity.translate(
                geometry,
                xoff=(max_width - this_width) / 2,
                yoff=(max_height - this_height) / 2,
            )
            draw.polygon(geometry.coords, outline="black", width=line_width)

        # scale it down
        if max_width > max_height:
            scale = thumb_size / max_width
            scale = (int(max_width * scale), int(max_height * scale))
        else:
            scale = thumb_size / max_height
            scale = (int(max_width * scale), int(max_height * scale))
        thumb = img.resize(scale, resample=Image.BICUBIC)
        img.close()
        if debug_save_images:
            thumb.save(f"{key}.png")
        base64_encoded = pil_to_base64(thumb)
        the_b64s[key] = base64_encoded

    return the_b64s


def image_folder_to_messages(relative_folder_path):
    files = os.listdir(os.path.join(current_dir, relative_folder_path))
    files = [x for x in files if x.endswith(".png")]

    return [
        {
            "type": "image_url",
            "image_url": {"url": path_to_b64(os.path.join(relative_folder_path, file))},
        }
        for file in files
    ]


def get_prompt_all_as_user_role(image_messages, use_rescaled=False):
    subdir = "rescaled" if use_rescaled else "unscaled"
    bad_examples = image_folder_to_messages(os.path.join(subdir, "bad"))
    good_examples = image_folder_to_messages(os.path.join(subdir, "good"))
    return [
        {
            "role": "user",  # system?
            "content": [
                {
                    "type": "text",
                    # "text": "You are an expert fashion designer specialising in grading. You will review individual pieces across several sizes to see how they grade. You are looking for the piece to grade smoothy through the sizes, if there are any inconsistencies, please raise them as errors.\n\nEach example you are given will be the outlines of a single piece for each size overlaid on top of each other, and each size is drawn with a different color.\n\nHere are examples of grading errors, you can see they don't grade smoothly:",
                    "text": "You are an expert fashion designer specialising in grading which is how the individual pieces of a garment change across different sizes. You will be given an image that depicts all the outlines of a single piece for each size overlaid on top of each other. For each image you are being asked if there are any grading issues. You are looking for a smooth gradient across the sizes. It's ok if some lines cross so long as the grading is smooth.\n\nHere are examples of grading errors, you can see they don't grade smoothly and look messy:",
                },
                # rescaled bad
                *bad_examples,
                # rescaled good
                {
                    "type": "text",
                    "text": "Here are examples of good grading, the gradients are smooth, nothing out of place:",
                },
                *good_examples,
                # let's go
                {
                    "type": "text",
                    #                    "text": """Inspect each image if you see major grading problems mark it as true. Otherwise mark it as false if you fail to detect any issues or they are minor.\n\nPlease format the output as json like this (no need for comments):
                    # "text": """Carefully inspect each image if you see any grading problems no matter how small, such as the grading not being smooth or is not consistent through the sizes mark it as true. Otherwise mark it as false if you fail to detect any issues.\n\nPlease format the output as json like this (no need for comments):
                    "text": """Carefully inspect each image and if you see any grading problems mark it as true. Mark it as false if you fail to detect any issues or they are very minor. If you are unsure, mark it as null.\n\nPlease format the output as json like this (no need for comments):
                
    {
        "1": true,
        "2": false,
        "3": true,
        "4": null
    }


    Now let's get started! For each image, please answer the question: Does this image have a grading issue?
    """,
                },
                # *image_messages[34:36], # a vertical rectangle is too provocative apparently
                *image_messages,
            ],
        },
    ]


def get_prompt_system_user_assistant_roles(image_messages):
    return [
        {
            "role": "system",
            "content": [
                {
                    "type": "text",
                    # "text": "You are an expert fashion designer specialising in grading. You will review individual pieces across several sizes to see how they grade. You are looking for the piece to grade smoothy through the sizes, if there are any inconsistencies, please raise them as errors.\n\nEach example you are given will be the outlines of a single piece for each size overlaid on top of each other, and each size is drawn with a different color.\n\nHere are examples of grading errors, you can see they don't grade smoothly:",
                    "text": """You are an expert fashion designer specialising in grading which is how the individual pieces of a garment change across different sizes. You will be given an image that depicts all the outlines of a single piece for each size overlaid on top of each other. For each image you are being asked if there are any grading issues. You are looking for a smooth gradient across the sizes. It's ok if some lines cross so long as the grading is smooth.\n\n Carefully inspect each image if you see any grading problems no matter how small, such as the grading not being smooth or is not consistent through the sizes mark it as true. Otherwise mark it as false if you fail to detect any issues.\n\nPlease format the output as json like this (no need for comments):
                
    {
        "1": true,
        "2": false,
        "3": true,
        "4": false
    }
""",
                },
            ],
        },
        {
            "role": "user",  # system?
            "content": [
                {
                    "type": "text",
                    "text": "Are there any grading issues in these images?",
                },
                {"type": "text", "text": "A"},
                # bad good good
                {
                    "type": "image_url",
                    "image_url": {"url": path_to_b64("eg_rescaled_spike.png")},
                },
                {"type": "text", "text": "B"},
                {
                    "type": "image_url",
                    "image_url": {"url": path_to_b64("eg_pants_good.png")},
                },
                {"type": "text", "text": "C"},
                {
                    "type": "image_url",
                    "image_url": {"url": path_to_b64("eg_rescaled_good_collar.png")},
                },
            ],
        },
        {
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": """{\n"A": true,\n"B":\nfalse,\n"C": false\n}""",
                }
            ],
        },
        {
            "role": "user",  # system?
            "content": [
                {
                    "type": "text",
                    "text": "That's perfect thanks. How about these, are there any grading issues here?",
                },
                {"type": "text", "text": "D"},
                {
                    "type": "image_url",
                    "image_url": {"url": path_to_b64("eg_rescaled_collar_good.png")},
                },
                {"type": "text", "text": "E"},
                {
                    "type": "image_url",
                    "image_url": {"url": path_to_b64("eg_rescaled_mess.png")},
                },
                {"type": "text", "text": "F"},
                {
                    "type": "image_url",
                    "image_url": {"url": path_to_b64("eg_rescaled_bat_yoke.png")},
                },
                {"type": "text", "text": "G"},
                {
                    "type": "image_url",
                    "image_url": {"url": path_to_b64("eg_rescaled_good_w.png")},
                },
            ],
        },
        {
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": """{\n"D": false,\n"E": true,\n"F": true,\n"G": false\n}""",
                }
            ],
        },
        {
            "role": "user",  # system?
            "content": [
                {
                    "type": "text",
                    "text": "Well done, perfect again! How about these, are there any grading issues here?",
                },
                # all good
                {"type": "text", "text": "H"},
                {
                    "type": "image_url",
                    "image_url": {"url": path_to_b64("eg_panel_good.png")},
                },
                {"type": "text", "text": "I"},
                {
                    "type": "image_url",
                    "image_url": {"url": path_to_b64("eg_good_panel_2.png")},
                },
                {"type": "text", "text": "J"},
                {
                    "type": "image_url",
                    "image_url": {"url": path_to_b64("eg_sleeve_good.png")},
                },
            ],
        },
        {
            "role": "assistant",
            "content": [
                {
                    "type": "text",
                    "text": """{\n"H": false,\n"I": false,\n"J": false\n}""",
                }
            ],
        },
        {
            "role": "user",  # system?
            "content": [
                {
                    "type": "text",
                    "text": "Wow, that's impressive, perfect again! How about these, are there any grading issues here?",
                },
                # let's go
                *image_messages,
            ],
        },
    ]


def get_prompt_to_describe(image_messages):
    return [
        {
            "role": "user",  # system?
            "content": [
                {
                    "type": "text",
                    # "text": "You are an expert fashion designer specialising in grading. You will review individual pieces across several sizes to see how they grade. You are looking for the piece to grade smoothy through the sizes, if there are any inconsistencies, please raise them as errors.\n\nEach example you are given will be the outlines of a single piece for each size overlaid on top of each other, and each size is drawn with a different color.\n\nHere are examples of grading errors, you can see they don't grade smoothly:",
                    "text": "You are an expert fashion designer specialising in grading which is how the individual pieces of a garment change across different sizes. You will be given an image that depicts all the outlines of a single piece for each size overlaid on top of each other. For each image you are being asked to describe what you see in terms of grading and how well the piece grades in detail.",
                },
                {
                    "type": "text",
                    "text": """You will recieve a text label then an image. Please give me your description of what you see and your evaluation of how well it grades. Please format your answer as JSON like this (no need for comments):
                    
                    {
                        "A": "<put your decription and grading evaluation for image A here>",
                        "B": "<put your decription and grading evaluation for image B here>",
                        "1": "<put your decription and grading evaluation for image 1 here>",
                        "2": "<put your decription and grading evaluation for image 2 here>",
                    }
                    
                    """,
                },
                # rescaled bad
                {"type": "text", "text": "A"},
                {
                    "type": "image_url",
                    "image_url": {"url": image_path_to_base64("eg_rescaled_spike.png")},
                },
                {"type": "text", "text": "B"},
                {
                    "type": "image_url",
                    "image_url": {"url": image_path_to_base64("eg_rescaled_mess.png")},
                },
                {"type": "text", "text": "C"},
                {
                    "type": "image_url",
                    "image_url": {
                        "url": image_path_to_base64("eg_rescaled_bat_yoke.png")
                    },
                },
                # rescaled good
                {"type": "text", "text": "D"},
                {"type": "text", "text": "E"},
                {
                    "type": "image_url",
                    "image_url": {"url": image_path_to_base64("eg_rescaled_pant.png")},
                },
                {"type": "text", "text": "F"},
                {
                    "type": "image_url",
                    "image_url": {
                        "url": image_path_to_base64("eg_rescaled_good_collar.png")
                    },
                },
                {"type": "text", "text": "G"},
                {
                    "type": "image_url",
                    "image_url": {
                        "url": image_path_to_base64("eg_rescaled_collar_good.png")
                    },
                },
                {"type": "text", "text": "H"},
                {
                    "type": "image_url",
                    "image_url": {
                        "url": image_path_to_base64("eg_rescaled_good_w.png")
                    },
                },
                *image_messages,
            ],
        },
    ]


def assemble_messages(the_b64s={}):
    logger.info("assembling messages...")
    image_messages = []
    mapping = {}
    i = 0
    for key, b64 in the_b64s.items():
        i += 1
        label = f"{i}"
        mapping[label] = key
        # save it for debug
        image_messages.append({"type": "text", "text": label})
        image_messages.append({"type": "image_url", "image_url": {"url": b64}})

    messages = get_prompt_all_as_user_role(image_messages)
    # messages = get_prompt_system_user_assistant_roles(image_messages)
    # messages = get_prompt_to_describe(image_messages)

    return messages, mapping


def analyze_pieces(
    the_b64s={}, expected={}, multiple_runs=3, save_as_markdown=False, use_plant=True
):
    if not the_b64s:
        logger.warning("no images to analyze")
        return {}

    # gpt4v seems to want to "succeed" so when there aren't a lot of grading issues it will pick out
    # a lot of very minor ones. I'm going to plan an obviously bad one so it feels better about itself
    plant = "__plant__"
    if use_plant:
        logger.info("planting a bad one...")
        the_b64s = {
            plant: path_to_b64("rescaled/bad_plant.png"),
            **the_b64s,
        }

    final_results = {}
    a_toast = None
    runs = []
    multiple_runs = max(1, multiple_runs)  # at least 1 run
    messages, mapping = assemble_messages(the_b64s)
    try:
        for i in range(multiple_runs):
            logger.info(f"sending {len(the_b64s)} images, run {i+1}...")
            response = client.chat.completions.create(
                model="gpt-4-vision-preview",
                messages=messages,
                max_tokens=len(the_b64s) * 50,
            )
            results = json.loads(extract_json(response.choices[0].message.content))
            results = unmap(results, mapping)
            logger.info(
                f"run {i+1} found {len([x for x in results.values() if x])} issues"
            )
            runs.append(results)

        # now we have a list of results, we need to aggregate them
        # we'll do this by taking the most common answer
        aggregated_results = {}
        for run in runs:
            for key, value in run.items():
                if key not in aggregated_results:
                    aggregated_results[key] = []

                aggregated_results[key].append(value)

        logger.info(json.dumps(aggregated_results, indent=4))

        for key, values in aggregated_results.items():
            non_null = [x for x in values if x is not None]
            ## majority vote
            # final_results[key] = max(set(non_null), key=values.count)
            final_results[key] = all(non_null)  # only if they ALL agree
    except Exception as e:
        logger.error(f"Calling GPT4V didn't work: {e}")
        a_toast = e

    if save_as_markdown:
        save_run_as_markdown(
            messages,
            expected,
            response=None,
            results=final_results,
            mapping=mapping,
            positive="has grading issue",
            negative="don't see any grading issues",
        )

    if a_toast:
        raise a_toast

    if use_plant:
        del final_results[plant]

    if expected:
        stats = Stats(final_results, expected)
        print(stats.markdown())

    return final_results


def sort_dict_by_size(d, sizes_df=None):
    if sizes_df is None:
        return dict(sorted(d.items()))

    try:
        # we'll look up the size against _accountingsku and use __sortorder to sort
        keys = list(d.keys())
        keys = sorted(
            keys,
            key=lambda x: sizes_df[sizes_df["_accountingsku"] == x]["__sortorder"].iloc[
                0
            ],
        )

        return {k: d[k] for k in keys}
    except:
        return dict(sorted(d.items()))


def manual_check(name_size_geometries, rescale=True, debug_save_images=False):
    import math

    # go and get the sizes from airtable so we can sort properly, can't depend on the name
    sizes_df = None
    try:
        airtable = res.connectors.load("airtable")
        sizes_table = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
        sizes_df = sizes_table.to_dataframe()
    except:
        pass

    name_size_shape = {}
    for name in name_size_geometries:
        name_size_shape[name] = {}
        size_geometries = name_size_geometries[name]
        for size in size_geometries:
            geometries = size_geometries[size]
            shape = geojson_to_centred_shape(geometries["outer"])
            # inner = geojson_to_centred_shape(geometries['inner'])
            # #union
            # shape = shape.union(inner)
            name_size_shape[name][size] = shape

    results = {}
    for key, shape_by_size in list(name_size_shape.items()):
        # sort by size
        shape_by_size = sort_dict_by_size(shape_by_size, sizes_df=sizes_df)
        geometries = list(shape_by_size.values())

        if rescale:
            first = geometries[0]
            width = first.bounds[2] - first.bounds[0]
            height = first.bounds[3] - first.bounds[1]
            rescaled = []
            for geometry in geometries:
                old_width = geometry.bounds[2] - geometry.bounds[0]
                old_height = geometry.bounds[3] - geometry.bounds[1]
                rescaled.append(
                    shapely.affinity.scale(
                        geometry,
                        xfact=width / old_width,
                        yfact=height / old_height,
                        origin=(0, 0),
                    )
                )
            geometries = rescaled

        parts = 15
        labels = []
        lines = []
        for i in range(parts):
            line_points = []
            for geometry in geometries:
                point = geometry.interpolate(i / parts, normalized=True)
                line_points.append(point)
            lines.append(shapely.geometry.LineString(line_points))
            labels.append(f"part_{i}")

        def line_is_continuous(line):
            cm = 300 / 2.54
            if len(line.coords) == 1 or line.length < cm:
                return True

            # check that the angle between each point is around 180 degrees
            for i in range(len(line.coords) - 2):
                p1 = line.coords[i]
                p2 = line.coords[i + 1]
                p3 = line.coords[i + 2]
                angle = math.degrees(
                    math.atan2(p3[1] - p2[1], p3[0] - p2[0])
                    - math.atan2(p1[1] - p2[1], p1[0] - p2[0])
                )
                if angle < 0:
                    angle += 360
                # print(f"{key} {angle}")
                if not math.isclose(angle, 180, abs_tol=30):
                    return False

            return True

        results[key] = not all([line_is_continuous(line) for line in lines])

        if debug_save_images:
            import geopandas as gpd

            bad_lines = [line for line in lines if not line_is_continuous(line)]
            gdf = gpd.GeoDataFrame(
                {
                    # "size": list(shape_by_size.keys()) + labels,
                    # "geometry": geometries + lines,
                    "size": [key] * len(shape_by_size.keys())
                    + ["bad"] * len(bad_lines),
                    "geometry": geometries + bad_lines,
                }
            )
            colors = ["b"] * len(shape_by_size.keys()) + ["r"] * len(bad_lines)
            ax = gdf.plot(column="size", figsize=(10, 10), color=colors, legend=True)
            ax.set_title(f'{key} {"BAD" if results[key] else "ok"}')
            buf = BytesIO()
            plt.axis("off")
            # this removes the ticks and numbers
            ax.get_xaxis().set_visible(False)
            ax.get_yaxis().set_visible(False)
            ax.figure.tight_layout()
            ax.figure.savefig(
                buf, bbox_inches="tight", pad_inches=0, dpi=200, format="png"
            )
            # close it so we don't display it
            plt.close(ax.figure)
            buf.seek(0)
            im = pyvips.Image.new_from_buffer(buf.read(), "")
            #     # aspect = im.width / im.height
            #     # thumb = im.thumbnail_image(thumb_size, height=int(thumb_size / aspect))
            thumb = im.thumbnail_image(512)
            thumb.write_to_file(f"{key}_a.png")

    return results


def inspect_grading(
    body_code="CC-2065",
    body_version=1,
    expected={},
    multiple_runs=3,
    save_as_markdown=False,
    slack_channel=None,  # "U03Q8PPFARG",  # to John
    debug_save_images=False,
):
    matplotlib.pyplot.switch_backend("Agg")  # don't need to display the images

    logger.info(f"*********** INSPECT GRADING {body_code} {body_version} ***********")
    logger.info(f"getting geometries...")
    geometries = get_geometries(body_code, body_version)
    the_b64s = geometries_to_flattened_images(
        geometries, debug_save_images=debug_save_images
    )

    # not going gpt4v route any more
    # result = analyze_pieces(
    #     the_b64s,
    #     expected=expected,
    #     multiple_runs=multiple_runs,
    #     save_as_markdown=save_as_markdown,
    # )

    # manual check performs better and isn't too prudish about rectangles
    result = manual_check(geometries, debug_save_images=debug_save_images)

    if not result:
        logger.warning("no results")
        return result

    # sort the result by key
    result = dict(sorted(result.items()))

    # pretend the first one has a grading issue for testing
    # result = {list(result.keys())[0]: True}

    logger.info("which pieces have grading issues results:")
    logger.info(json.dumps(result, indent=4))

    if slack_channel:
        slack = res.connectors.load("slack")

        files = [
            {
                "filename": f"{key}.png",
                "file": base64.b64decode(the_b64s[key].split(",")[1]),
            }
            for key, has_grading_issues in result.items()
            if has_grading_issues
        ]

        if files:
            slack.post_files(
                files,
                channel=slack_channel,
                initial_comment=f"There might be some grading issues for {body_code}-V{body_version}",
            )

    return result


def get_body_geometry_changes(
    body_code="JR-3060",
    body_version=9,
    prev_body_version=8,
    abs_tol=0.02,
    include_images=False,
    debug_save_images=False,
):
    from shapely.ops import polygonize
    from shapely.affinity import translate
    from shapely.geometry import LineString
    import math

    logger.info(
        f"*********** GET GEOMETRY CHANGES {body_code} {body_version} vs {prev_body_version} ***********"
    )

    logger.info(f"getting geometries...")
    curr_geometries = get_geometries(body_code, body_version)
    prev_geometries = get_geometries(body_code, prev_body_version)

    # looks like:
    # name_size_geometries[name][size] = { e.g.
    # name_size_geometries["TK-3001-V1-SHTFTPNL-S"]["3ZZMD"] = {
    #         "inner": result["inner_geojson"],
    #         "outer": result["outer_geojson"],
    #     }
    curr_names = set("-".join(k.split("-")[3:]) for k in curr_geometries.keys())
    prev_names = set("-".join(k.split("-")[3:]) for k in prev_geometries.keys())

    curr_sizes = set([x for y in curr_geometries.values() for x in y.keys()])
    prev_sizes = set([x for y in prev_geometries.values() for x in y.keys()])

    result = {}

    if curr_names - prev_names:
        result["added_names"] = list(curr_names - prev_names)
    if prev_names - curr_names:
        result["removed_names"] = list(prev_names - curr_names)

    if curr_sizes - prev_sizes:
        result["added_sizes"] = list(curr_sizes - prev_sizes)
    if prev_sizes - curr_sizes:
        result["removed_sizes"] = list(prev_sizes - curr_sizes)

    # for the names and sizes that are both there, let's compare the geometries
    changes = {}
    images = {}
    for name in curr_names & prev_names:
        # if name != "SHTSDGSSLF-S":
        #     continue
        first = True
        for size in curr_sizes & prev_sizes:
            curr_geometry = curr_geometries[f"{body_code}-V{body_version}-{name}"][size]
            prev_geometry = prev_geometries[f"{body_code}-V{prev_body_version}-{name}"][
                size
            ]

            to_poly = lambda g: list(polygonize(geojson_to_centred_shape(g)))[0]

            curr_shape = to_poly(curr_geometry["outer"])
            prev_shape = to_poly(prev_geometry["outer"])
            intersection = curr_shape.intersection(prev_shape)

            top = min(curr_shape.bounds[1], prev_shape.bounds[1])
            left = min(curr_shape.bounds[0], prev_shape.bounds[0])
            right = max(curr_shape.bounds[2], prev_shape.bounds[2])
            bottom = max(curr_shape.bounds[3], prev_shape.bounds[3])

            # box = LineString(
            #     [
            #         (left, top),
            #         (right, top),
            #         (right, bottom),
            #         (left, bottom),
            #         (left, top),
            #     ]
            # )

            width = int(right - left) + 1
            height = int(bottom - top) + 1

            curr_shape = translate(curr_shape, xoff=-left, yoff=-top)
            prev_shape = translate(prev_shape, xoff=-left, yoff=-top)
            # box = translate(box, xoff=-left, yoff=-top)

            curr_overlap = intersection.area / curr_shape.area  # <= 1
            prev_overlap = intersection.area / prev_shape.area  # <= 1

            overall_overlap = curr_overlap * prev_overlap
            if not math.isclose(overall_overlap, 1, abs_tol=abs_tol):
                # there's been a change
                if name not in changes:
                    changes[name] = {}
                if size not in changes[name]:
                    # round to 3 decimal places
                    changes[name][size] = round(overall_overlap, 3)

                if include_images and first:
                    first = False
                    im = Image.new("RGB", (width, height), color="white")
                    draw = ImageDraw.Draw(im)

                    line = int(max(max(width, height) / 750, 1))
                    draw.polygon(curr_shape.boundary.coords, outline="blue", width=line)
                    draw.polygon(prev_shape.boundary.coords, outline="red", width=line)

                    thumb = 512
                    scale = thumb / max(width, height)
                    im = im.resize(
                        (int(width * scale), int(height * scale)),
                        resample=Image.BICUBIC,
                    )

                    images[f"{name}-{size}"] = im

                    # im.save(f"{name}_{size}.png")
                    if debug_save_images:
                        im.save(f"{name}-{size}.png")
                    # import geopandas as gpd
                    # return gpd.GeoDataFrame(
                    #     {"geometry": [curr_shape, prev_shape, box]}
                    # ).plot()

    if changes:
        result["changes"] = changes

    return (result, images) if include_images else result
