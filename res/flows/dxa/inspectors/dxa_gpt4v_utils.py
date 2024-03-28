from warnings import filterwarnings

filterwarnings("ignore")

import os
import base64
import json
from io import BytesIO
from PIL import Image
import pyvips


from res.utils.secrets import secrets

os.environ["OPENAI_API_KEY"] = secrets.get_secret("ASK_ONE_OPENAI_API_KEY")

import res
from res.utils.logging import logger

from openai import OpenAI

client = OpenAI()


def get_open_ai_client():
    return client


def sku_glob(sku: str, body_version: int = None, piece: str = None):
    s3 = res.connectors.load("s3")

    sku_parts = sku.split(" ")

    body_code = sku_parts[0]
    if "-" not in body_code:
        body_code = body_code[:2] + "-" + body_code[2:]
    body_code = body_code.lower().replace("-", "_")
    color = sku_parts[2].lower() if len(sku_parts) > 2 else None
    # material = sku_parts[1] # doesn't really matter
    size = sku_parts[3].lower() if len(sku_parts) > 3 else None

    base_url = f"s3://meta-one-assets-prod/color_on_shape/{body_code}"

    files = list(s3.ls(base_url, suffixes=[".png"]))
    files = list(filter(lambda x: "pieces" in x, files))

    # if there's no body version, get the latest
    if not body_version:
        body_version = max(map(lambda x: int(x.split("/")[5].split("v")[1]), files))

    # if there's no size, get the smallest
    if not size or size == "min":
        sizes = list(set(map(lambda x: x.split("/")[7], files)))
        size = sorted(sizes)[0]
    elif size == "*" or size == "all":
        size = None

    if color == "*" or color == "all":
        color = None

    # logger.info(f"{body_code}/v{body_version}/{color}/pieces/{size}/{piece}")

    # e.g. s3://meta-one-assets-prod/color_on_shape/cc_3001/v10/bluevs/1zzxs/pieces/300/CC-3001-V10-HODFEPNLLF-S.png
    files = list(filter(lambda x: color == x.split("/")[6], files)) if color else files
    # logger.info(len(files))
    files = list(filter(lambda x: size == x.split("/")[7], files)) if size else files
    # logger.info(len(files))
    files = (
        list(
            filter(lambda x: body_version == int(x.split("/")[5].split("v")[1]), files)
        )
        if body_version
        else files
    )
    # logger.info(len(files))
    files = (
        list(filter(lambda x: piece.lower() in x.split("/")[-1].lower(), files))
        if piece
        else files
    )
    # logger.info(len(files))

    return list(files)


def pyvips_to_base64(image):
    image_bytes = image.write_to_buffer(".png")
    base64_encoded = base64.b64encode(image_bytes).decode("utf-8")

    return "data:image/png;base64," + base64_encoded


def image_path_to_base64(img_path):
    with open(img_path, "rb") as img_file:
        return "data:image/png;base64," + base64.b64encode(img_file.read()).decode(
            "utf-8"
        )


def pil_to_base64(image):
    buffered = BytesIO()
    image.save(buffered, format="PNG")
    img_str = base64.b64encode(buffered.getvalue())

    return "data:image/png;base64," + img_str.decode("utf-8")


def extract_json(result):
    # find the first instance of a {
    start = result.find("{")
    # find the last instance of a }
    end = result.rfind("}")
    # extract the json
    return result[start : end + 1]


def unmap(d, mapping):
    return {(mapping[k] if k in mapping else k): v for k, v in d.items()}


def confusion_matrix(results, expected):
    true_positives = 0
    false_positives = 0
    true_negatives = 0
    false_negatives = 0

    for key, value in results.items():
        if key in expected:
            expected_value = expected[key]
            correct = value == expected_value

            if correct:
                true_positives += 1 if value else 0
                true_negatives += 1 if not value else 0
            else:
                false_positives += 1 if value else 0
                false_negatives += 1 if not value else 0

    return true_positives, false_positives, true_negatives, false_negatives


class Stats:
    def __init__(self, results, expected):
        self.true_positives = 0
        self.false_positives = 0
        self.true_negatives = 0
        self.false_negatives = 0

        for key, value in results.items():
            if key in expected:
                expected_value = expected[key]
                correct = value == expected_value

                if correct:
                    self.true_positives += 1 if value else 0
                    self.true_negatives += 1 if not value else 0
                else:
                    self.false_positives += 1 if value else 0
                    self.false_negatives += 1 if not value else 0

    def accuracy(self):
        if (
            self.true_positives
            + self.true_negatives
            + self.false_positives
            + self.false_negatives
        ) == 0:
            return 0

        return (self.true_positives + self.true_negatives) / (
            self.true_positives
            + self.true_negatives
            + self.false_positives
            + self.false_negatives
        )

    def precision(self):
        if self.true_positives + self.false_positives == 0:
            return 0
        return self.true_positives / (self.true_positives + self.false_positives)

    def recall(self):
        if self.true_positives + self.false_negatives == 0:
            return 0
        return self.true_positives / (self.true_positives + self.false_negatives)

    def markdown(self):
        markdown = ""
        markdown += "|  actual/predicted | negative | positive |\n"
        markdown += "| --- | --- | --- |\n"
        markdown += f"| negative | {self.true_negatives} | {self.false_positives} |\n"
        markdown += f"| positive | {self.false_negatives} | {self.true_positives} |\n\n"

        accuracy = self.accuracy()
        precision = self.precision()
        recall = self.recall()

        markdown += f"Accuracy: {accuracy:.2f}\n\n"
        markdown += f"Precision: {precision:.2f}\n\n"
        markdown += f"Recall: {recall:.2f}\n\n"
        if precision + recall == 0:
            markdown += f"F1: 0\n\n"
        else:
            markdown += f"F1: {(2 * precision * recall) / (precision + recall):.2f}\n\n"

        return markdown

    def __add__(self, other):
        result = Stats({}, {})
        result.true_positives = self.true_positives + other.true_positives
        result.false_positives = self.false_positives + other.false_positives
        result.true_negatives = self.true_negatives + other.true_negatives
        result.false_negatives = self.false_negatives + other.false_negatives

        return result

    def __str__(self):
        return f"tp: {self.true_positives} fp: {self.false_positives} tn: {self.true_negatives} fn: {self.false_negatives}"


def save_run_as_markdown(
    messages,
    expected={},
    response=None,
    markdown_folder="trials",
    positive="ok",
    negative="not ok",
    mapping={},
    results=None,
):
    logger.info("Saving run as markdown...")

    # if it's not there create it
    if not os.path.exists(markdown_folder):
        os.mkdir(markdown_folder)

    ans = None
    if results is None:
        # logger.info(response.usage)
        ans = response.choices[0].message.content
        # logger.info(ans)

        try:
            results = json.loads(extract_json(ans))
        except:
            logger.info("Could not parse json")
            results = {}
    results = unmap(results, mapping)

    markdown = ""

    files = os.listdir(markdown_folder)
    files = list(filter(lambda x: x.startswith("trial_"), files))
    if len(files) == 0:
        trial_number = 1
    else:
        trial_number = max(map(lambda x: int(x.split("_")[1]), files)) + 1
    output_dir = f"{markdown_folder}/trial_{trial_number}"
    os.mkdir(output_dir)

    images_folder = f"{output_dir}/images"
    os.mkdir(images_folder)

    for message in messages:
        role = message["role"]
        content = message["content"]

        markdown += f"## Role: {role}\n\n"
        for c in content:
            if c["type"] == "text":
                text = c["text"].strip()
                text = mapping[text] if text in mapping else text
                ok = ""
                if text in results:
                    ok = results[text]
                    color = "red" if not ok else "green"
                    ok = positive if ok else negative
                    ok = f"<span style='color:{color}'>{ok}</span>\n\n"

                if text in expected and text in results:
                    correct = expected[text] == results[text]
                    # tick emoji if correct, x if not
                    tick = "✅" if correct else "❌"
                    markdown += f"{text} {ok} {tick}\n\n"
                else:
                    # if results["text"] is a string not a bool print it
                    if type(results.get(text)) == str:
                        markdown += f"{text} {results.get(text)}\n\n"
                    else:
                        markdown += f"{text} {ok} \n\n"
            elif c["type"] == "image_url":
                # assumes all the images are base64 encoded
                base64_data = c["image_url"]["url"]
                base64_data = base64_data.split(",")[1]
                image_data = base64.b64decode(base64_data)
                image = Image.open(BytesIO(image_data))
                image.resize((512, 512))
                image_number = len(os.listdir(images_folder)) + 1
                dest = f"{images_folder}/{image_number}.png"
                image.save(dest)

                markdown += f"![{image_number}](images/{image_number}.png)\n\n"
                # aspect = image.width / image.height
                # width = height = 512
                # if image.width > image.height:
                #     width = 512
                #     height = int(512 / aspect)
                # else:
                #     height = 512
                #     width = int(512 * aspect)

                # markdown += f"<img src='{image_number}.png' width='{width}' height='{height}'>\n\n"

    if len(results) > 0:
        markdown += f"## Results\n\n```\n{json.dumps(results, indent=4)}\n```\n"

        markdown += f"## Performance\n\n"

        # table with headers url, expected, predicted, correct
        markdown += "| url | expected | predicted | correct |\n"
        markdown += "| --- | --- | --- | --- |\n"
        for key, value in results.items():
            if key in expected:
                correct = value == expected[key]

                markdown += f"| {key} | {expected[key]} | {value} | "
                if correct:
                    markdown += f"""<span style="color:green">Correct</span> |\n"""
                else:
                    markdown += f"""<span style="color:red">Incorrect</span> |\n"""
            else:
                markdown += f"| {key} | | {value} | |\n"

        stats = Stats(results, expected)
        markdown += f"\n### Stats\n\n"
        markdown += stats.markdown()
    else:
        markdown += f"## Results\n\n{ans}\n\n"

    if response:
        usage = response.usage
        markdown += f"\n## Usage\n\n```\n{usage}\n```\n"
        markdown += "| item | tokens |\n"
        markdown += "| --- | --- |\n"
        # completion_tokens=149, prompt_tokens=6741, total_tokens=6890
        markdown += f"| completion tokens | {usage.completion_tokens} |\n"
        markdown += f"| prompt tokens | {usage.prompt_tokens} |\n"
        markdown += f"| total tokens | {usage.total_tokens} |\n"

    with open(f"{output_dir}/results.md", "w") as f:
        f.write(markdown)
        logger.info(f"Saved results to {output_dir}/results.md")

    return results


def s3_url_to_base64(url):
    s3 = res.connectors.load("s3")

    image = s3.open(url)
    buffered = BytesIO()
    image.save(buffered, format="PNG")
    img_str = base64.b64encode(buffered.getvalue())

    return img_str.decode("utf-8")


def get_thumbnail_from_file(file, size=2048):
    image = pyvips.Image.new_from_file(file)

    # save a black and white thumbnail, if it's bigger than size
    if image.width > size or image.height > size:
        image = image.thumbnail_image(size)
    # image = image.colourspace("b-w")
    return image


def get_thumbnail_from_s3(url, version_id=None, size=2048):
    s3 = res.connectors.load("s3")
    s3client = s3.get_client()

    parts = url.split("/")
    bucket = parts[2]
    prefix = "/".join(parts[3:])

    if version_id is None:
        response = s3client.get_object(Bucket=bucket, Key=prefix)
    else:
        response = s3client.get_object(Bucket=bucket, Key=prefix, VersionId=version_id)

    body = response["Body"].read()
    image = pyvips.Image.new_from_buffer(body, "")

    # save a black and white thumbnail
    image = image.thumbnail_image(size)
    # image = image.colourspace("b-w")
    return image


def pyvips_to_pil(pyvips_img):
    img_bytes = pyvips_img.write_to_buffer(".png")
    img = Image.open(BytesIO(img_bytes))
    return img
