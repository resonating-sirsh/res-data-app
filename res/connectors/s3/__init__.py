from .S3Connector import S3Connector
import re
import res


def clean_filename(s):
    """ """

    def remove_dots_keep_last(input_string):
        parts = input_string.split(".")
        if len(parts) > 0:
            prefix = f"_".join(parts[:-1])
            return f"{prefix}.{parts[-1]}"
        return input_string

    s = re.sub(r"[^a-zA-Z0-9.]+", "-", s).lower()
    s = remove_dots_keep_last(s)
    return s


def fetch_to_s3(url, target, token=None):
    """
    helper to move a http dataset to s3
    """
    import requests

    s3 = res.connectors.load("s3")
    response = (
        requests.get(url)
        if not token
        else requests.get(url, headers={"Authorization": "Bearer %s" % token})
    )

    with s3.file_object(target, "wb") as f:

        f.write(response.content)

    res.utils.logger.info(target)
    return target
