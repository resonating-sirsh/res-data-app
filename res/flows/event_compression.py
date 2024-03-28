import codecs
import base64
import json


def compress_event(event):
    return json.dumps(
        {
            **event,
            "assets": base64.b64encode(
                codecs.encode(json.dumps(event["assets"]).encode(), "zlib")
            ).decode(),
        }
    )


def decompress_event(compressed_str):
    event = json.loads(compressed_str)
    event["assets"] = json.loads(
        codecs.decode(base64.b64decode(event["assets"]), "zlib").decode()
    )
    return event
