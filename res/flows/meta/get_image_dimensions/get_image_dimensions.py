import boto3
from PIL import Image
from pathlib import Path
Image.MAX_IMAGE_PIXELS = None


def get_image_dimensions(s3_uri):
    s3_client = boto3.client("s3")
    bucket, input_file_key = s3_uri.split('/',2)[-1].split('/',1)

    # Download file
    print(f"Downloading {input_file_key}...")
    input_key_path = Path(input_file_key)
    input_file_path = f"/tmp/{input_key_path.name}"

    s3_client.download_file(bucket, input_file_key, input_file_path)

    img = Image.open(input_file_path)
    width, height = img.size

    print(f'Width: {width}')
    print(f'Height: {height}')

    return width, height


# if __name__ == '__main__':
#     get_image_dimensions('s3://resmagic/uploads/3744ef09-4a7f-4771-bfcf-784b4ac42b52.png')



