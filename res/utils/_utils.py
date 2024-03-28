"""Res library utils catchall."""

from ast import literal_eval
from collections.abc import MutableMapping
from importlib import import_module
from pathlib import Path
from structlog.contextvars import get_contextvars, bound_contextvars
from threading import Thread
import hashlib
import json
import os
import sys
import traceback
import uuid
import yaml
import requests


from datetime import datetime, date, time
from decimal import Decimal
import base64
from enum import Enum

from tenacity import retry, wait_exponential, wait_fixed, stop_after_attempt

__all__ = [
    "get_res_root",
    "flatten_dict",
    "ex_repr",
    "res_hash",
    "hash_of",
    "hash_dict",
    "uuid_str_from_dict",
    "hash_dict_short",
    "post_to_lambda",
    "read",
    "intersection",
    "disjunction",
    "rflatten",
    "delete_from_dict",
    "group_by",
    "slack_exceptions",
    "ping_slack",
    "post_with_token",
    "get_with_token",
    "validate_record_for_topic",
    "run_job_async_with_context",
    "safe_json_serialiser",
    "safe_json_dumps",
]


def get_res_root():
    path = os.environ.get("RES_HOME")
    if not path:
        res = import_module("res")
        if res.__file__ is not None:
            path = Path(res.__file__).parent.parent
        else:
            path = Path(__file__).parent.parent.parent
    return Path(path)


def _flatten_dict_gen(d, parent_key, sep):
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, MutableMapping):
            yield from flatten_dict(v, new_key, sep=sep).items()
        else:
            yield new_key, v


def flatten_dict(d: MutableMapping, parent_key: str = "", sep: str = "_"):
    return dict(_flatten_dict_gen(d, parent_key, sep))


def ex_repr(ex, full=True):
    """
    most basic formatting for capturing exception info
    """
    exc_type, exc_obj, exc_tb = sys.exc_info()
    if exc_tb and not full:
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

        return f"{exc_type} in {fname} line {exc_tb.tb_lineno}: {repr(ex)}"
    traceback.print_exception(*sys.exc_info())


def res_hash(s=None, m=5, prefix="res"):
    s = s or str(uuid.uuid1()).encode()

    h = hashlib.shake_256(s).hexdigest(m).upper()

    return f"{prefix}{h}"


def hash_of(s):
    return hashlib.md5(f"{s}".encode("utf-8")).hexdigest()


def hash_dict(d):
    if isinstance(d, str):
        d = literal_eval(d)
    return hashlib.sha1(json.dumps(d, sort_keys=True).encode("utf-8")).hexdigest()


def uuid_str_from_dict(d):
    """
    generate a uuid string from a seed that is a sorted dict
    """
    d = json.dumps(d, sort_keys=True).encode("utf-8")
    m = hashlib.md5()
    m.update(d)
    return str(uuid.UUID(m.hexdigest()))


def hash_dict_short(d, m=5):
    """
    short hash of length 2*m of the dictionary d
    """
    h = json.dumps(d, sort_keys=True).encode("utf-8")

    h = hashlib.shake_256(h).hexdigest(m).upper()

    return h


def slack_exceptions(func, channel, msg):
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as ex:
            ping_slack(f"{msg}: {repr(ex)}", channel)
            raise ex

    return wrapped


def ping_slack(msg, channel):
    if os.environ.get("RES_ENV") != "production":
        from res.utils import logger

        logger.info(f"Would slack: {msg} to {channel} if this were prod")
        return
    try:
        from res.connectors.slack.SlackConnector import SlackConnector

        SlackConnector()(
            {
                "slack_channels": [channel],
                "message": msg,
                "attachments": [],
            }
        )
    except Exception as ex:
        from res.utils import logger

        logger.warn(f"Failing to slack - {ex}")


def safe_json_dumps(dict_to_dump: dict):
    return json.dumps(
        dict_to_dump,
        default=safe_json_serialiser,
        indent=4,
    )


def safe_json_serialiser(obj):
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)  # or str(obj)
    elif isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, bytes):
        return base64.b64encode(obj).decode()
    elif isinstance(obj, uuid.UUID):
        return str(obj)
    elif isinstance(obj, Enum):
        return obj.value  # or obj.name
    # Add custom handling for more types if necessary
    raise TypeError("Type not serializable")


def validate_record_for_topic(record, topic, schema=None):
    """
    friendlier messages for kafka schema validation
    read a schema by convention on the res data path or pass one (e.g. from schema registry)
    """
    from fastavro.validation import validate
    from res.utils import get_res_root
    import json

    if not schema:
        parts = topic.split(".")
        path = str(
            get_res_root()
            / f"res-schemas/avro/{parts[0].replace('_','-')}/{parts[1].replace('_','-')}/{parts[2]}.avsc"
        )
        print(path)
        with open(path, "r") as f:
            schema = json.loads(f.read())

    return validate(record, schema)


# going to add a lambda call temp here for now as do not want to create a new module for
# it yet may promote to botox or botyo extensions just for some convenient wrappers
@retry(wait=wait_exponential(multiplier=1, min=5, max=60), stop=stop_after_attempt(5))
def post_to_lambda(lambda_fn, data):
    import boto3
    import json
    from botocore.config import Config

    client = boto3.client(
        "lambda",
        region_name="us-east-1",
        config=Config(
            read_timeout=900,  # sometimes lambdas can seem to get slammed into oblivion
        ),
    )
    if "arn:aws:lambda:us-east-1:286292902993:function:" not in lambda_fn:
        lambda_fn = f"arn:aws:lambda:us-east-1:286292902993:function:{lambda_fn}"
    data = json.dumps(data)

    print(f"Posting to {lambda_fn} - {data}")

    return client.invoke(
        FunctionName=lambda_fn,
        InvocationType="RequestResponse",
        Payload=str.encode(data),
    )


def read(f, **kwargs):
    """
    Simple utility to read any local file WIP
    May move this to its own readers file or module if it becomes more interesting
    """

    if f[:5] == "s3://":
        from res.connectors import load as load_connector

        s3 = load_connector("s3")

        return s3.read(f)

    p = Path(f)
    if p.suffix == ".json":
        with open(f, "r") as f:
            return json.load(f, **kwargs)
    if p.suffix == ".yaml":
        with open(f, "r") as f:
            try:
                return yaml.safe_load(f, **kwargs)
            except:
                return list(yaml.safe_load_all(f, **kwargs))
    raise NotImplementedError(
        f"The suffix {p.suffix} is not yet supported by the generic reader"
    )


def intersection(a, b):
    return list(set(a) & set(b))


def disjunction(a, b):
    return list(set(a) | set(b))


def rflatten(x):
    atom = lambda x: not isinstance(x, list)
    nil = lambda x: not x
    car = lambda x: x[0]
    cdr = lambda x: x[1:]
    cons = lambda x, y: x + y
    return [x] if atom(x) else x if nil(x) else cons(*map(rflatten, [car(x), cdr(x)]))


def delete_from_dict(a_dict, key):
    """
    delete a key from a dict without mutating the original dict
    if thats ever a thing thats important to you
    """
    return {k: v for k, v in a_dict.items() if k != key}


def group_by(get_key, lists):
    """
    Groub by element on a list into a dict where a key is going to be the result of the
    get_key function.
    :param get_key function return string and it's the key used to group by the element
           on the lists.
    :param lists all the elements to group by

    # implementation
    lists1 = ['A', 'B', 'C', 'A']
    get_key = lambda x: x
    grouped = group_by(get_key, lists1)

    #----------------------------------

    response:
    {
        'A': ['A', 'A'],
        'B': ['B'],
        'C': ['C'],
    }

    """
    response = {}
    for item in lists:
        key = get_key(item)
        if key in response:
            response[key].append(item)
            continue
        response[key] = [item]
    return response


@retry(wait=wait_fixed(10), stop=stop_after_attempt(3))
def post_with_token(url, secret_name, **kwargs):
    from .secrets import secrets_client

    return requests.post(
        url,
        headers={"Authorization": "Bearer " + secrets_client.get_secret(secret_name)},
        **kwargs,
    )


@retry(wait=wait_fixed(10), stop=stop_after_attempt(3))
def get_with_token(url, secret_name, **kwargs):
    from .secrets import secrets_client

    apiKey = secrets_client.get_secret(secret_name)
    if type(apiKey) == type(dict()):
        apiKey = apiKey.get(secret_name)

    return requests.get(
        url,
        headers={"Authorization": "Bearer " + apiKey},
        **kwargs,
    )


def run_job_async_with_context(target_function, *args, **kwargs):
    context_vars = get_contextvars()

    def wrapped():
        with bound_contextvars(**context_vars):
            target_function(*args, **kwargs)

    Thread(target=wrapped).start()
