from abc import ABC, abstractmethod


class Loader(ABC):
    @abstractmethod
    def load_asset_info(self, materials=[], record_ids=[], material_swaps={}):
        ...

    @abstractmethod
    def get_packing_nodes(self, material_props, **kwargs):
        ...
