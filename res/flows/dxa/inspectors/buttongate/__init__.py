import os
from openai import OpenAI
import json

import res
from res.utils.logging import logger
from res.utils.secrets import secrets
from res.flows.dxa.inspectors.dxa_gpt4v_utils import (
    sku_glob,
    extract_json,
    pyvips_to_base64,
    image_path_to_base64,
    get_thumbnail_from_file,
    get_thumbnail_from_s3,
    save_run_as_markdown,
)

os.environ["OPENAI_API_KEY"] = secrets.get_secret("ASK_ONE_OPENAI_API_KEY")
client = OpenAI()


def get_prompt():
    current_dir = os.path.dirname(os.path.realpath(__file__))
    p = lambda x: os.path.join(current_dir, x)
    return [
        # intro
        # {
        #     "type": "text",
        #     "text": "We print patterns on fabric and want to avoid printing things like buttons or buttonholes on the fabric. If there are buttons or buttonholes on the fabric we have to throw it out which is very expensive. You are an expert at detecting buttons and buttonholes, we want you to look at some images of fabric and tell us if there are any buttons or buttonholes on the fabric.",
        # },
        # # what buttons and buttonholes look like
        # {"type": "text", "text": "Here is an example of what a button looks like:"},
        # {"type": "image_url", "image_url": {"url": image_path_to_base64(p("button.png"))}},
        # {"type": "text", "text": "And here is an example of what a buttonhole looks like:"},
        # {"type": "image_url", "image_url": {"url": image_path_to_base64(p("buttonhole.png"))}},
        # # extra button
        # {"type": "text", "text": "Here is another example of what a button looks like:"},
        # {"type": "image_url", "image_url": {"url": image_path_to_base64(p("otherbutton.png"))}},
        # # extra buttonhole
        # {"type": "text", "text": "Here is another example of what a buttonhole looks like:"},
        # {"type": "image_url", "image_url": {"url": image_path_to_base64(p("keyhole.png"))}},
        # all-in-one
        {"type": "text", "text": "Here are examples of buttons and buttonholes:"},
        {
            "type": "image_url",
            "image_url": {"url": image_path_to_base64(p("buttons.png"))},
        },
        # reinforce buttonholes
        {
            "type": "text",
            "text": "Buttonholes are very difficult to detect so be extra vigilant looking for these. Here are some examples of buttonholes:",
        },
        {
            "type": "image_url",
            "image_url": {"url": image_path_to_base64(p("buttonhole.png"))},
        },
        {
            "type": "image_url",
            "image_url": {"url": image_path_to_base64(p("keyhole.png"))},
        },
        # some examples that have buttons and buttonholes
        {
            "type": "text",
            "text": "In this example we can see a buttonhole on the left and a button on the right:",
        },
        {
            "type": "image_url",
            "image_url": {"url": image_path_to_base64(p("eg_cuff_no_circle.png"))},
        },
        {
            "type": "text",
            "text": "In this example we can see a buttonhole on the bottom left:",
        },
        {
            "type": "image_url",
            "image_url": {"url": image_path_to_base64(p("eg_hood_no_circle.png"))},
        },
        # more, hard to spot
        {
            "type": "text",
            "text": "and in this example we can see a very small buttonhole on the top left:",
        },
        {
            "type": "image_url",
            "image_url": {"url": image_path_to_base64(p("eg_large_pants.png"))},
        },
        {
            "type": "text",
            "text": "and in this example we can see a buttonholes down the right edge:",
        },
        {
            "type": "image_url",
            "image_url": {"url": image_path_to_base64(p("eg_right_2.png"))},
        },
        ############## this didn't help
        # # some examples that have buttons and buttonholes circled
        # {"type": "text", "text": "In this example we can see a buttonhole on the left and a button on the right, both circled in red:"},
        # {"type": "image_url", "image_url": {"url": image_path_to_base64(p("eg_cuff.png"))}},
        # {"type": "text", "text": "and in this example we can see a buttonhole on the bottom left circled in red:"},
        # {"type": "image_url", "image_url": {"url": image_path_to_base64(p("eg_hood.png"))}},
        # an example that has no buttons at all
        {
            "type": "text",
            "text": "and in this example there are no buttons or buttonholes:",
        },
        {
            "type": "image_url",
            "image_url": {"url": image_path_to_base64(p("eg_none.png"))},
        },
        # commenting out so we err on the side of caution
        # # more, very misleading jcrt with circles
        # { "type": "text", "text": "and here's another example where there are no buttons or buttonholes:" },
        # { "type": "image_url", "image_url": {"url": image_path_to_base64(p("eg_none_2_misleading.png"))} },
        # output format prompt
        {
            "type": "text",
            "text": """You will receive a number followed by an image for several images. For each individual image, carefully inspect it and check if there are any buttons or buttonholes present. Err on the side of caution: if something looks like it might possibly be a button or buttonhole mark it as true. Otherwise, if you are completely sure there are definitely no buttons or buttonholes at all mark it as false.\n\nPlease format the output as json like this:

    {
        "1": true,
        "2": false,
        "3": false,
        "4": true
    }
    """,
        },
    ]


def analyze_pieces(urls_or_files=[], retries=3, save_as_markdown=False):
    logger.info("assembling messages for GPT4V...")
    image_messages = []
    mapping = {}
    for i, url in enumerate(urls_or_files):
        thumb = (
            get_thumbnail_from_s3(url)
            if url.startswith("s3://")
            else get_thumbnail_from_file(url)
        )

        label = f"{i+1}"
        mapping[f"{label}"] = url
        # save it for debug
        # thumb.write_to_file("/Users/john/temp/thumb.png")
        image_messages.append({"type": "text", "text": label})
        image_messages.append(
            {"type": "image_url", "image_url": {"url": pyvips_to_base64(thumb)}}
        )

    # logger.info(image_messages)
    # return

    messages = [
        # {
        #     "role": "system",
        #     "content": [
        #         {
        #             "type": "text",
        #             # "text": "You are an expert at detecting buttons and buttonholes, we want you to look at some images of fabric and tell us if there are any buttons or buttonholes on the fabric.",
        #             "text": "You are an expert at detecting buttons and buttonholes, we want you to look at some images of patterns and tell us if there are any buttons or buttonholes present. If there are buttons or buttonholes on the pattern we have to throw it out which is very expensive.",
        #         }
        #     ],
        # },
        {
            "role": "user",
            "content": [
                *get_prompt(),
                # images to analyze
                *image_messages,
            ],
        },
    ]

    for _ in range(retries):
        try:
            logger.info("sending messages to GPT4V...")
            response = client.chat.completions.create(
                model="gpt-4-vision-preview",
                messages=messages,
                # response_format={"type": "json_object"},
                max_tokens=len(image_messages) * 10,
            )

            logger.info("received response from GPT4V...")
            ans = response.choices[0].message.content
            logger.info(ans)

            result = json.loads(extract_json(ans))
            # unmapping
            result = {mapping[k]: v for k, v in result.items() if k in mapping}
            logger.info(result)

            if save_as_markdown:
                save_run_as_markdown(messages, response=response, mapping=mapping)
            else:
                logger.info(response.usage)
                logger.info("result:")
                logger.info(ans)

            return result
        except Exception as e:
            logger.warn(f"Failed executing gpt4v, retrying... {e}")

    logger.error("Failed executing gpt4v")
    return {}


# write a function which takes a sku or files and returns which might have buttons or buttonholes
def check_for_buttons(
    sku: str = None,
    files=None,
    body_version: int = None,
    piece: str = None,
    max_pieces=50,
    top_pieces=0,
    retries=3,
    save_as_markdown=False,
    slack_channel=None,  # U03Q8PPFARG @John, C06DVEE12JV #inspector_gpt4vet
):
    logger.info(f"*** Checking for buttons {sku if sku else 'no sku'} ***")

    if not sku and not files:
        raise ValueError("Must provide either a sku or files to check")

    logger.info("getting files...")
    files = sku_glob(sku, body_version=body_version, piece=piece) if sku else files
    files = files[:top_pieces] if top_pieces else files
    logger.info(f"{len(files)} files to check")

    # logger.info(json.dumps(files, indent=2))

    if max_pieces > 0 and len(files) > max_pieces:
        raise ValueError(
            "Too many files to process, set to 0 to disable max_pieces check if you meant to process this many"
        )

    if len(files) == 0:
        return {}

    result = analyze_pieces(files, retries=retries, save_as_markdown=save_as_markdown)

    if slack_channel:
        slack = res.connectors.load("slack")

        files = [
            {
                "filename": s3_path.split("/")[-1],
                "file": get_thumbnail_from_s3(s3_path).write_to_buffer(".png"),
            }
            for s3_path, has_buttons in result.items()
            if has_buttons
        ]

        if files:
            slack.post_files(
                files,
                channel=slack_channel,
                initial_comment=f"There might be buttons or button holes {sku}",
            )

    return result


# result = automatic_piece_inspection(
#     # "CC-3001 XXXXX *", piece="HODHDHODLF", # test all colors for a piece
#     "CC-3001 XXXXX marbly", # test one color for all its pieces
#     body_version=9,
#     #top_pieces=3,
#     max_pieces=15,
#     expected=expected)
