from pydantic import typing, BaseModel
from res.connectors.neo4j.Neo4jConnector import NeoEdge, NeoNode
import res
import json


class GarmentType(NeoNode):
    pass


class GarmentRegion(NeoNode):
    pass


class GarmentPart(NeoNode):
    region: str


class GarmentPiece(NeoNode):
    code: str
    name: str


class SeamOperationSiteCategory(NeoNode):
    code: str
    name: str = None


class SeamOperationSite(NeoNode):
    code: str
    name: str = None
    operation_site_code: str = None
    # will link to sew ops


class SewOperation(NeoNode):
    code: str
    name: str
    record_id: str
    spanish_name: typing.Optional[str] = None

    machines: typing.Optional[typing.List[str]] = []
    machine_accessories: typing.Optional[typing.List[str]] = []
    parts_applied: typing.Optional[typing.List[str]] = []
    body_operation_type: typing.Optional[str] = None
    visible_stitch_attributes: typing.Optional[str] = None

    operation_intention: typing.Optional[str] = None
    industry_name: typing.Optional[str] = None
    operation_specification: typing.Optional[str] = None
    operation_time_attributes: typing.Optional[str] = None
    operation_image_s3: typing.Optional[str] = None
    ai_description: typing.Optional[str] = None


class SewOperationRef(NeoNode):
    """it can be convenient to just add a ref by code for neo4j to use for lookup template"""

    code: str

    """
     RELATIONS
    """


class GarmentSeamOpRel(NeoEdge):
    pass


class GarmentSewOpRel(NeoEdge):
    pass


class GarmentPartConfigRel(NeoEdge):
    pass


class GarmentRegionConfigRel(NeoEdge):
    pass


class GarmentPartSeamRel(NeoEdge):
    comment: str = None


"""
Example adding data

n.add_records([
    GarmentRegion(code='Front Bodice',name='Front Bodice'),
    GarmentRegion(code='Back Bodice',name='Back Bodice'),
    GarmentRegion(code='Sleeve',name='Sleeve'),
    GarmentRegion(code='Neck',name='Neck'),
    GarmentRegion(code='Front Bottom',name='Front Bottom'),
    GarmentRegion(code='Back Bottom',name='Back Bottom'),
])

n.add_records([
    GarmentPart(code='BKYKE', name='Back Yoke', region='Back Bodice'),
    GarmentPart(code='FTYKE', name='Front Yoke', region='Front Bodice'),
    GarmentPart(code='BKPNL', name='Back Panel', region='Back Bodice'),
    GarmentPart(code='FTPNL', name='Front Panel', region='Front Bodice'),
])


n.add_records([
    GarmentPiece(code='BKYKE', name='Back Yoke'),
    GarmentPiece(code='BKYKETP', name='Back Yoke Top'),
    GarmentPiece(code='BKYKELF', name='Back Yoke Left'),
    GarmentPiece(code='BKYKEUN', name='Back Yoke Under'),
    GarmentPiece(code='BKYKERT', name='Back Yoke Right'),
    GarmentPiece(code='BKYKETPLF', name='Back Yoke Top Left'),
    GarmentPiece(code='BKYKETPRT', name='Back Yoke Top Right'),
    GarmentPiece(code='BKYKEUNLF', name='Back Yoke Under Left'),
    GarmentPiece(code='BKYKEUNRT', name='Back Yoke Right Right'),
])

#add a part and a bunch of break downs into pieces
a = GarmentPart(code='BKYKE', name='Back Yoke', region='Back Bodice')
b1 = GarmentPiece(code='BKYKE', name='Back Yoke')
n.add_relationship(a=a, b=b1, rel=GarmentPartConfigRel(configuraton='Self') )

b1 = GarmentPiece(code='BKYKETP', name='Back Yoke Top')
b2 = GarmentPiece(code='BKYKEUN', name='Back Yoke Under')
n.add_relationship(a=a, b=b1, rel=GarmentPartConfigRel(configuraton='TP/UN') )
n.add_relationship(a=a, b=b2, rel=GarmentPartConfigRel(configuraton='TP/UN') )

b1 = GarmentPiece(code='BKYKELF', name='Back Yoke Left')
b2 = GarmentPiece(code='BKYKERT', name='Back Yoke Right')
n.add_relationship(a=a, b=b1, rel=GarmentPartConfigRel(configuraton='LF/RT') )
n.add_relationship(a=a, b=b2, rel=GarmentPartConfigRel(configuraton='LF/RT') )

b1=GarmentPiece(code='BKYKETPLF', name='Back Yoke Top Left')
b2=GarmentPiece(code='BKYKETPRT', name='Back Yoke Top Right')
b3=GarmentPiece(code='BKYKEUNLF', name='Back Yoke Under Left')
b4=GarmentPiece(code='BKYKEUNRT', name='Back Yoke Right Right')
n.add_relationship(a=a, b=b1, rel=GarmentPartConfigRel(configuraton='TPLF/TPRT/UNLF/UNRT') )
n.add_relationship(a=a, b=b2, rel=GarmentPartConfigRel(configuraton='TPLF/TPRT/UNLF/UNRT') )
n.add_relationship(a=a, b=b3, rel=GarmentPartConfigRel(configuraton='TPLF/TPRT/UNLF/UNRT') ) 
n.add_relationship(a=a, b=b4, rel=GarmentPartConfigRel(configuraton='TPLF/TPRT/UNLF/UNRT') )



a = GarmentRegion(code='Back Bodice',name='Back Bodice')
 
n.add_relationship(a=a, b=GarmentPart(code='BKYKE', name='Back Yoke', region='Back Bodice'), rel=GarmentRegionConfigRel(configuraton='BKYKE/BKPNL') )
n.add_relationship(a=a, b=GarmentPart(code='BKPNL', name='Back Panel', region='Back Bodice'), rel=GarmentRegionConfigRel(configuraton='BKYKE/BKPNL') )
n.add_relationship(a=a, b=GarmentPart(code='BKPNL', name='Back Panel', region='Back Bodice'), rel=GarmentRegionConfigRel(configuraton='BKPNL') )

"""
