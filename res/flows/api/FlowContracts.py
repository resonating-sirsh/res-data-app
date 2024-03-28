"""
The idea of the FlowAPI is there should be a single way to do anything
In this case there is only one way to add gates events

contracts are config based operations run from the flow against assets
we can configure all the tests we want to run and run handlers based on a namespace

a handler can have a Contract(key=) so we know how to find it from a factory
factories can resolve lookups so they are fast to run on many items without worrying about state
"""
from schemas.pydantic.common import List


class FlowContracts:
    def __init__(self, node, asset_type, **kwargs):
        # load config from db or cache
        pass

    def run(self, asset, **kwargs):
        """
        modify flags and tags on the asset which is assumed to be relayed elsewhere with some status rule
        for Example this can be used with the FlowAPI which is already sending things to airtable
        """
        return {}

    def __call__(self, asset, **kwargs):
        return self.run(asset, **kwargs)


class ValidationResult(dict):
    def __init__(*args, **kwargs):
        pass

    def __getitem__(self, key):
        pass

    def __repr__(self) -> str:
        pass

    def tags(self):
        return list(self.keys())


class FlowContractValidator:
    def validate(self, asset) -> List[str]:
        return self.validation_result(asset).tags

    def validation_result(self, asset) -> ValidationResult:
        return None
