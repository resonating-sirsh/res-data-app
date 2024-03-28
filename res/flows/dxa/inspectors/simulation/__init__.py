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
    pil_to_base64,
    image_path_to_base64,
    extract_json,
    Stats,
    unmap,
    get_thumbnail_from_s3,
    pyvips_to_base64,
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
    bad_examples = image_folder_to_messages("bad")
    good_examples = image_folder_to_messages("good")
    return [
        {
            "role": "user",  # system?
            "content": [
                {
                    "type": "text",
                    "text": "You are an expert fashion designer. Part of your job is inspecting 3D simulations of garments and flagging if it looks bad or if there is anything odd or out of place. You will be given a few images that depict different views of the 3D simulation to judge if there are any problems.\n\nTo give you an idea of what to expect, here are some bad examples:",
                },
                # rescaled bad
                *bad_examples,
                # rescaled good
                {
                    "type": "text",
                    "text": "And here are some good examples:",
                },
                *good_examples,
                # let's go
                {
                    "type": "text",
                    "text": """Carefully inspect these images of a 3D simulation, marking it as "good" or "bad" and giving a brief reason.\n\nPlease format the output as json like this:
                
    {
        "bad": "there are some unexpected spikes possibly due to a bug in the simulation, also it appears a piece is missing on the left",
    }

    Here you go, is this a good or bad simulation? Carefully check it for any problems but it's ok if you can't find any.
    """,
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

    return messages, mapping


def analyze_images(the_b64s={}, expected={}, save_as_markdown=False):
    if not the_b64s:
        logger.warning("no images to analyze")
        return {}

    # I want to catch any GPT errors so I can save the run as markdown either way
    a_toast = None
    results = {}
    try:
        messages, mapping = assemble_messages(the_b64s)
        logger.info(f"sending {len(the_b64s)} images...")
        response = client.chat.completions.create(
            model="gpt-4-vision-preview",
            messages=messages,
            max_tokens=100 + len(the_b64s) * 50,
        )
        results = json.loads(extract_json(response.choices[0].message.content))
    except Exception as e:
        logger.error(f"Calling GPT4V didn't work: {e}")
        a_toast = e

    if save_as_markdown:
        save_run_as_markdown(
            messages,
            expected,
            response=None,
            results=results,
            mapping=mapping,
            positive="found some simulation issue",
            negative="no simulation issues",
        )

    if a_toast:
        raise a_toast

    if expected:
        stats = Stats(results, expected)
        print(stats.markdown())

    return results


def inspect_simulation(
    body_code="CC-2065",
    body_version=1,
    color_code=None,
    expected={},
    save_as_markdown=False,
    slack_channel=None,  # "U03Q8PPFARG",  # to John
    debug_save_images=False,
):
    logger.info(
        f"*********** INSPECT SIMULATION {body_code} {body_version} {color_code} ***********"
    )
    logger.info(f"getting turntable images...")

    body_lower = body_code.lower().replace("-", "_")
    the_b64s = {}
    if color_code is None:
        url = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_lower}/v{body_version}/extracted/front.png"
        the_b64s[url] = pyvips_to_base64(get_thumbnail_from_s3(url))
    else:
        color_lower = color_code.lower()  # dashes are kept for color codes
        folder = f"s3://meta-one-assets-prod/color_on_shape/{body_lower}/v{body_version}/{color_lower}/turntable/snapshots"
        for i in range(1, 24, 3):
            url = f"{folder}/turntable_image_{i}.png"
            the_b64s[url] = pyvips_to_base64(get_thumbnail_from_s3(url))

    result = analyze_images(
        the_b64s,
        expected=expected,
        save_as_markdown=save_as_markdown,
    )

    if not result:
        logger.warning("no results")
        return result

    # pretend the first one has a grading issue for testing
    # result = {list(result.keys())[0]: True}

    logger.info("results:")
    logger.info(json.dumps(result, indent=4))

    if slack_channel and len(result) > 0:
        slack = res.connectors.load("slack")

        # should just be the one result
        key = list(result.keys())[0]
        explanation = result[key]

        if key == "bad":
            pre = f"{body_code}-V{body_version}-{color_code}"
            files = [
                {
                    "filename": f"{pre}-{k.split('/')[-1]}",
                    "file": base64.b64decode(v.split(",")[1]),
                }
                for k, v in the_b64s.items()
            ]

            if files:
                slack.post_files(
                    files,
                    channel=slack_channel,
                    initial_comment=f"There might be some simulation issues for {body_code}-V{body_version} {color_code}: {explanation}",
                )

    return result
