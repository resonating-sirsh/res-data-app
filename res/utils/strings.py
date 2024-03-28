from re import sub
import re
from base64 import b64encode, b64decode
from bs4 import BeautifulSoup
import zlib


# legacy
def to_snake_case(s):
    return "_".join(
        sub(
            "([A-Z][a-z]+)", r" \1", sub("([A-Z]+)", r" \1", s.replace("-", " "))
        ).split()
    ).lower()


def strip_emoji(text):
    if text is not None:
        RE_EMOJI = re.compile(
            "([\U00002600-\U000027BF])|([\U0001f300-\U0001f64F])|([\U0001f680-\U0001f6FF])"
        )
        return RE_EMOJI.sub(r"", text).strip()
    return None


def compress_string(regular_string):
    """
    this is used as a pair of function to compress a string but to base 64 bytes that we can move around

    example Json -> dumps -> compression -> base 64 encoded -> send
    """
    return b64encode(zlib.compress(regular_string.encode())).decode("utf-8")


def decompress_string(base_64_encoded_compressed):
    """
    other part of a pair; base 64 over the wire and then decompress it

    example receive base 64 encoded string that has been compressed, unpack it e.g. something we can load into json
    """
    return zlib.decompress(b64decode(base_64_encoded_compressed)).decode()


def is_html(s: str) -> bool:
    return bool(BeautifulSoup(s, "html.parser").find())
