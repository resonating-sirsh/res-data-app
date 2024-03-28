import threading
import traceback
import requests
from tenacity import retry, wait_chain, wait_fixed, wait_random, stop_after_attempt
from . import logging
from res.utils.secrets import secrets

# TODO: WIP
# just doing this to keep a common behavior on all http
# it could become annoying


def relay_to_res_replica(request, async_request=True):
    """
    Make an async request to the "replica" if whatever (flask) app we are calling
    For example if we post to  datadev.resmagic.io/process/path/
    the replica could be  data.resmagic.io/process/path/

    This is a way of treating any flask endpoint not as environment seperated but as
    replicas of each other we want to keep up to date
    An external client could send data to one if the replicas and it would be processed
    by the other too
    """


def meta_one_token_as_headers():
    token = secrets.get_secret("RES_META_ONE_API_KEY")
    headers = {
        "authorization": f"Bearer {token}",
    }
    return headers


# @retry(wait=wait_fixed(3), stop=stop_after_attempt(4))
def request_get_meta_one_endpoint(
    endpoint, headers=None, auth=None, timeout=None, **params
):
    try:
        url = "https://data.resmagic.io/" + endpoint.lstrip("/")

        headers = headers = {}
        headers.update(meta_one_token_as_headers())
        res = requests.get(
            url, headers=headers, auth=auth, timeout=timeout, params=params
        )
        res.raise_for_status()
        return res
    except Exception as ex:
        logging.logger.info(traceback.format_exc())
        print(f"failed to get url {url}")
        raise ex


@retry(wait=wait_fixed(3), stop=stop_after_attempt(4))
def request_put(url, json=None, headers=None, auth=None, timeout=5):
    res = None
    try:
        res = requests.put(url, json=json, headers=headers, auth=auth, timeout=timeout)
        res.raise_for_status()
        return res
    except Exception as ex:
        logging.logger.info(traceback.format_exc())
        print(f"failed to get url {url} {res.content if res else None}")
        raise ex


@retry(wait=wait_fixed(3), stop=stop_after_attempt(4))
def request_patch(url, json=None, headers=None, auth=None, timeout=5):
    res = None
    try:
        res = requests.patch(
            url, json=json, headers=headers, auth=auth, timeout=timeout
        )
        res.raise_for_status()
        return res
    except Exception as ex:
        logging.logger.info(traceback.format_exc())
        print(f"failed to get url {url} {res.content if res else None}: {ex}")
        raise ex


@retry(
    wait=wait_chain(
        *[wait_fixed(3) + wait_random(0, 2) for i in range(2)]
        + [wait_fixed(7) + wait_random(0, 2) for i in range(1)]
        + [wait_fixed(9) + wait_random(0, 2)]
    ),
    stop=stop_after_attempt(4),
)
def request_get(url, headers=None, auth=None, timeout=5, params: dict = None):
    """
    Performs a get request with backoff + jitter to prevent thundering herd
    issues. Waits 3 seconds + up to 2 seconds of random delay for the first
    two attempts, 7 seconds + up to 2 seconds of random delay for the third
    attempt, then 9 seconds + up to 2 seconds of random delay for the fourth
    """

    try:
        res = requests.get(
            url, headers=headers, auth=auth, timeout=timeout, params=params
        )
        res.raise_for_status()
        return res
    except Exception as ex:
        logging.logger.info(traceback.format_exc())
        print(f"failed to get url {url}")
        raise ex


@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def request_post(url, json=None, data=None, headers=None, timeout=5):
    text = ""
    try:
        res = requests.post(url, headers=headers, json=json, data=data, timeout=timeout)
        text = res.content
        res.raise_for_status()
        return res
    except Exception as exc:
        logging.logger.warn(f"failed to post to url {url} - {text}, {repr(exc)}....")
        logging.logger.info(traceback.format_exc())
        raise exc


def request_post_fire_and_forget(url, json=None, data=None, headers=None, timeout=5):
    try:

        def invoke_post():
            return requests.post(
                url, headers=headers, json=json, data=data, timeout=timeout
            )

        threading.Thread(target=invoke_post).start()
    except Exception as exc:
        logging.logger.info(traceback.format_exc())
        print(f"Failed when doing the fire and forget {repr(exc)}")


@retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
def request_delete(url, headers, timeout=5):
    try:
        res = requests.delete(url, headers=headers, timeout=timeout)
        res.raise_for_status()
        return res
    except Exception as exc:
        print(f"failed to delete at url {url}")
        raise exc


def graph_request_post():
    pass
