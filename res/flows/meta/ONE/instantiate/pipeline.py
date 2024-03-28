import res
import typing
from pydantic import BaseModel


class IngestIntent(BaseModel):
    """
    this is used to kick off an argo process to interpret the data
    """

    body_code: str
    body_version: int = 0
    brand_code: str = None
    selected_parent_body: str = None
    category: str = None
    # size information
    dxf_uri: typing.Optional[str] = None
    techpack_uri: typing.Optional[str] = None
    worker_cpus: int = 4


class TechpackIngestion(IngestIntent):
    techpack_uri: str


class DxfIngestion(IngestIntent):
    dxf_uri: str


def ingest_techpack(event, context={}):
    with res.flows.FlowContext(event, context) as fc:
        for asset in fc.assets:
            asset = IngestIntent(**asset)
            handle_payload(asset)

    return {}


@res.flows.flow_node_attributes(memory="10Gi", disk="5G", cpu=6)
def handle_payload(payload: IngestIntent):
    """
    ingest a tech pack and possible a dxf file at the same time
    """
    from res.media.parsers.TechpackParser import TechpackParser

    key = f"{payload.body_code.upper()}-V{payload.body_version}"
    tp = TechpackParser(payload.techpack_uri, key=key, dxf_file=payload.dxf_uri)
    _ = tp.par_read(from_cache=False)

    return {}


"""
A type for a piece input in the pipeline
- piece shape
- piece name (distribution)
- piece symmetries
- piece fusing/lining
- piece buttons
- piece stitch lines and types and other piece placements
- piece partial poms
- has_double (require comment or known rules)
- piece major axis measurements and other energy keypoints 
- piece materials??
- seam allowances
- grain/rotation
"""

"""
ambiguous or shapeless pieces - maybe needs length matching e.g. bindings 
choose avatar
labels
"""

"""
sketch POMs across pieces
- match the poms to the piece based on some energy principle
- learn from examples?
- seams are special poms and we should identify them across pieces; armholes, hems, sleeves seams, side seams, back and shoulder seams, etc 
"""

"""
GPT what pieces connect to what other pieces
- sleeves and necks alway attach to font and backs
- and back and front panels connect elsewhere
- sleeve cuffs always attach to sleeves
"""

"""
classify
- name
- size chart ()
- body category (enum)
- body attributes selection (xis labels)
"""


"""
Avatar intelligence
- point clouds and surface meshes and energy models / landmarks
- pose

refs:
https://standards.ieee.org/wp-content/uploads/2022/07/Landmarking-for-Product-Development.pdf
"""

"""
moving between spaces -> sketch -> avatar -> 2d pieces -> ... -> regional standard sewing sketches like the construction of collars and collar stand standards
- sketch draping
- pom matching and grading
- avatar region segmentation of both poms (points and lines) and pieces (regions)
"""


"""
some rules:
- cuffs, collars and their stands and (sleeve) plackets require top and under
- assume block fusing for now for sew dev
- need to determine material types in slow dev and the corresponding weight of the fusing (AI might know from sewing standards)
- block fuses are just tagged and dont generate pieces but if someone calls for fusing we can duplicate a shape and name it
"""
