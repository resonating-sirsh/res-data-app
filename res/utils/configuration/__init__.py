from distutils.util import strtobool
import os


def bool_env(key, default=False):
    v = os.environ.get(key, default)
    return bool(strtobool(str(v)))
