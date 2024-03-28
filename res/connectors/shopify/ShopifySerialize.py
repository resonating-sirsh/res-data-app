import abc
import json


# class Normalizer
#
# abstract class
# convert a raw input or dictionary to an object. and also
class Normalizer(abc.ABC):
    raw = dict()

    def __init__(self, raw):
        self.raw = raw

    def to_json(self):
        json.dumps(self)

    @abc.abstractmethod
    def _from_raw(self):
        pass
