import typing
from res.flows import flow_node_attributes, flow_node
from res.flows.FlowNodeManager import NodeQueue, dataclass, StatusType
from enum import Enum
import res
from dataclasses import field as dfield


class SampleValidationType(Enum):
    BADA = "BADA"
    BADB = "BADB"
    BADC = "BADC"


@dataclass
class SampleRequest(NodeQueue):
    "Sample Request"
    uri: str = ""
    body_code: str = ""
    # string lists - are different to enums in airtable for us??
    # TODO list of enums for the multi select
    # validation_tags: types.Enum = types.Enum(["BAD A", "BAD B", "BAD C", "BAD D"])
    status: StatusType = StatusType.TODO
    tags: typing.List[SampleValidationType] = dfield(default_factory=list)
    subnode: str = None
    # want to find a way to handle and parse dicts everywhere - needs some testing
    # metadata: typing.Dict[str, typing.Union[str, float, None]]

    class Meta:
        namespace = "res_flows.node"
        aliases = ["abstract"]
        airtable_mapping = {}

    def validate(self):
        """
        this is a dummy validatin - sometimes it complains and sometimes it does not
        """
        import numpy as np

        if np.random.random() > 0.6:
            l = [item.value for item in list(SampleValidationType)]
            return list(np.random.choice(l, np.random.randint(1, len(l)), False))

        # randomly generate errors from the tags
        return []


def random_error(p=0.1):
    import numpy as np

    if np.random.random() < 0.1:
        res.utils.logger.debug("generating a random error!!")
        raise Exception("This is a test random error")


@dataclass
class SampleResponse(NodeQueue):
    "Sample Response"
    uri: str = ""
    body_code: str = ""
    status: StatusType = StatusType.TODO
    tags: typing.List[SampleValidationType] = dfield(default_factory=list)

    class Meta:
        namespace = "res_flows.node"
        aliases = ["response"]
        airtable_mapping = {}


@flow_node_attributes(memory="50Gi", mapped=True)
@flow_node
def handler(unit: SampleRequest) -> SampleResponse:
    # when we receive the work in flow context, set the status to WORKING in airtable

    # pause for effect
    random_error(p=0.3)

    return SampleResponse(
        id=unit.id,
        unit_key="test",
        uri="test",
        body_code="test",
        status="TODO",
    )
