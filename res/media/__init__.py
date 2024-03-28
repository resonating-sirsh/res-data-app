from ..utils import logger
from . import text, images
import io

parts = [logger, text, images]


def content_reader(url, bytes):
    """
    for now just to read pdf and doc attachments from somewhere else
    the bytes are fetched from the url but the url can add context

    return a tuple of a name and best effort provider

    here is an example

    url = 'https://files.slack.com/files-pri/T076U7DHR-F068J1F3AD9/download/one_board_may23.pdf'

    data = slack.get_file_uploaded_to_slack(url)

    name, provider = content_reader(url, data.content)

    provider <- pdf viewer
    """
    filename = url.split("/")[-1]

    ext = filename.split(".")[-1].lower() if "." in filename else None

    bs = io.BytesIO(bytes)

    if ext == "pdf":
        import pikepdf

        return filename, pikepdf.open(bs)

    # by default do this and other things can open
    return filename, bs
