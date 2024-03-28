import os
import base64
import io
from PIL import Image

os.environ["RES_ENV"] = "production"
import res
from res.utils.secrets import secrets

os.environ["OPENAI_API_KEY"] = secrets.get_secret("ASK_ONE_OPENAI_API_KEY")
from openai import OpenAI

client = OpenAI()

# directions = """
# Each of these images depict some artwork (image, pattern or text) placed within a shape outline.

# The base image is the source of truth, and I need to verify that the overall look of the other images
# is relatively similar to the base image. The outline is not important, but how the artwork is placed
# within the outline is important. Ignore color and resolution differences.

# Things that would fail:
#   an artwork is truncated or cut off compared to the base image
#   an artwork is in a slightly different position compared to the base image
#   two or more artworks drift apart or closer together compared to the base image
#   an artwork looks too big or small compared to the base image
#   an artwork is distorted or stretched compared to the base image

# With that in mind, what differences do you see? Give a percentage rating too."""
directions = """
these images have artwork within a shape outline. the first image is the base image and it is the source of truth: the other images should look relatively similar to the base image. Essentially, your job is to play spot the difference (ignoring the outline size and shape), which you are an expert at. Here are some things to look out for:

- are there any extra or missing artworks compared to the base image?
For each artwork:
- is the artwork in the same place relative to the base image?
- is the artwork the same size relative to the base image?
- is the artwork cut off or truncated compared to the base image?

be brief and be very critical, any small difference is important.

Here is an example base image, with two other images to compare against. The artworks here are letters for easy comparison, but artworks can be anything.
"""
# directions = """
# these images have one or more artworks within a shape outline. the first image is the source of truth,
# the other images should look relatively similar to the first image.
# ignoring the outlines, rank the following similarities in percentage terms:
# - size of the artwork
# - position of the artwork
# - any artwork truncation

# be brief and be brutal, any small difference is important.
# """
# directions = """
# These images have one or more artworks within a shape outline. The first image is the "base" image and it is the source of truth: the other images should look relatively similar to the base image. However, due to the shape outline being different, the artworks may be in a different position.

# Your job is to highlight any differences between the base image and the other images.

# ignoring the outlines, is the artwork in the same place relative to the base image?
# is the artwork the same size relative to the base image?
# is the artwork cut off or truncated compared to the base image?
# be brief and be very critical, any small difference is important.
# """

# eg_answer = """
# For the example image above following the directions, I would say:

#     example A:
#         - the letter A artwork has the same relative position and size as the base image
#         - the letter B artwork is too big and gets cut off at the top
#         - the letters C and D were side by side in the base image, but are now overlapping
#         - the letter F artwork was on the left and is only slightly cut off compared to the base image

#     example B:
#         - the letter A artwork has the same relative position and size as the base image
#         - the letter B artwork is much smaller and to the left compared to the base image
#         - the letters C and D were side by side in the base image, but are now spaced apart
#         - the letter F artwork was on the left but has moved to the right and there is a gap between its left edge and side where there was none in the base image
# """

eg_answer_a = """
For the example image above following the directions, I would say:

    example A:
        - the letter A artwork has the same relative position and size as the base image
        - the letter B artwork is too big and gets cut off at the top
        - the letters C and D were side by side in the base image, but are now overlapping
        - the letter F artwork was on the left and is only slightly cut off compared to the base image
"""

eg_answer_b = """
For the example image above following the directions, I would say:

    example B:
        - the letter A artwork has the same relative position and size as the base image
        - the letter B artwork is much smaller and to the left compared to the base image
        - the letters C and D were side by side in the base image, but are now spaced apart
        - the letter F artwork was on the left but has moved to the right and there is a gap between its left edge and side where there was none in the base image
"""


def image_to_base64(img_path):
    with open(img_path, "rb") as img_file:
        return "data:image/png;base64," + base64.b64encode(img_file.read()).decode(
            "utf-8"
        )


eg_base_size_image = image_to_base64("./_base_size_image.png")
eg_small_size_image = image_to_base64("./_small_size_image.png")
eg_large_size_image = image_to_base64("./_large_size_image.png")


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


class Intent(BaseModel):
    base_size_image: str
    small_size_image: str
    large_size_image: str


def save_image(b64, filename):
    img_data = base64.b64decode(b64[22:])
    img = Image.open(io.BytesIO(img_data))
    img.save(filename)


class B64(BaseModel):
    b64: str
    name: str


@app.post("/save_image")
def save(b64: B64) -> str:
    # save each base64 string as an image locally for reference
    save_image(b64.b64, b64.name)
    return "success"


@app.post("/check_intent")
async def check_intent(intent: Intent) -> str:
    # save each base64 string as an image locally for reference
    save_image(intent.base_size_image, "_base_size_image.png")
    save_image(intent.small_size_image, "_small_size_image.png")
    save_image(intent.large_size_image, "_large_size_image.png")

    res.utils.logger.info(f"Checking intent.... {intent.base_size_image[:50]}")
    # return "testing"
    response = client.chat.completions.create(
        model="gpt-4-vision-preview",
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": directions},
                    # give some example images
                    {"type": "text", "text": "example base image"},
                    {
                        "type": "image_url",
                        "image_url": {"url": eg_base_size_image},
                    },
                    {"type": "text", "text": "example A"},
                    {
                        "type": "image_url",
                        "image_url": {"url": eg_small_size_image},
                    },
                    {"type": "text", "text": eg_answer_b},
                    {"type": "text", "text": "example B"},
                    {
                        "type": "image_url",
                        "image_url": {"url": eg_large_size_image},
                    },
                    {"type": "text", "text": eg_answer_b},
                    {
                        "type": "text",
                        "text": "here are the images I want you to compare. First is the base image, then image A, then image B.",
                    },
                    {"type": "text", "text": "base image"},
                    {
                        "type": "image_url",
                        "image_url": {"url": intent.base_size_image},
                    },
                    {"type": "text", "text": "image A"},
                    {
                        "type": "image_url",
                        "image_url": {"url": intent.small_size_image},
                    },
                    {"type": "text", "text": "image B"},
                    {
                        "type": "image_url",
                        "image_url": {"url": intent.large_size_image},
                    },
                ],
            }
        ],
        max_tokens=300,
    )
    res.utils.logger.info(response.usage)
    ans = response.choices[0].message.content
    res.utils.logger.info(ans)

    return ans


origins = [
    "http://localhost:3000",
    "http://localhost:3001",
    "http://localhost:5000",
    "https://strictly-mutual-newt.ngrok-free.app",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    # Use this for debugging purposes only
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=5005,
        log_level="debug",
        reload=True,
    )  # try reload=False if CPU is high after a while
