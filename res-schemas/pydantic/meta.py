"""Pydantic models for the event hooks system."""

from __future__ import annotations
import datetime
from typing import List, Optional, Union
from enum import Enum


from schemas.pydantic.common import (
    Field,
    FlowApiModel,
    CommonConfig,
    root_validator,
    uuid_str_from_dict,
    id_mapper_factory,
    FlowStatus,
    CommonBaseModel,
    GLine,
    GMultiLine,
    GMultiPoint,
    GPoint,
)
import numpy as np


def linked_node_id_mapper(**kwargs):
    """
    if we have a node which maps to node.name
    """
    return id_mapper_factory(["node"], alias={"node": "name"})(**kwargs)


class MetaOneStatus(FlowApiModel):
    """Pydantic model for Meta one status. will probably deprecate this one"""

    class Config(CommonConfig):
        # specify in the name that will become
        airtable_attachment_fields = {"Printable Pieces Previews"}

    sku: str = Field(primary_key=True)
    style_sku: str
    body_code: str
    material_code: str
    body_version: int
    print_type: Optional[str]
    is_unstable_material: Optional[bool]
    flow: Optional[str]
    flags: Optional[List[str]]
    flag_owners: Optional[List[str]]
    # PENDING BODY, PENDING COLOR, ACTIVE
    status: str
    printable_pieces: Optional[
        List[str]
    ]  # = Field(airtable_fk_table='tblf7JWphUqnoJaHW')
    printable_pieces_previews: Optional[List[str]]
    cut_pieces: Optional[List[str]]  # = Field(airtable_fk_table='tblf7JWphUqnoJaHW')


class BodyMetaOnePiecesRequest(FlowApiModel):
    # the id may be known but our parent validator can set it as a function of the body and size
    id: Optional[str] = Field(primary_key=True)
    key: str


class MetaOnePiecesRequest(FlowApiModel):
    # the id may be known but our parent validator can set it as a function of the body and size
    id: Optional[str] = Field(primary_key=True)
    body_piece_id: Optional[str]
    key: str
    material_code: str
    # TODO: do we want this to be optional
    artwork_uri: Optional[str]
    color_code: str
    base_image_uri: Optional[str]
    # is_color_placement
    # customization: Optional[str] = Field(default_factory=dict)
    # cadinality: Optional[int] = Field(default=1)
    offset_size_inches: Optional[float] = Field(db_write=False)
    metadata: Optional[dict]

    @root_validator
    def _metadata(cls, values):
        """
        The id for res flow api is generated as a hash of the key map.
        we use ID_FIELD as _id so that the users id can always be aliased to something else
        """

        metadata = values.get("metadata", {}) or {}

        offset_size_inches = values.get("offset_size_inches")
        if offset_size_inches is not None:
            metadata["offset_size_inches"] = offset_size_inches
        else:
            # try the other way
            values["offset_size_inches"] = metadata.get("offset_size_inches")

        if "offset_size_inches" not in metadata:
            raise Exception(
                f"`offset_size_inches` must be added as a metadata attribute OR a value including 0 should be supplied for `offset_size_inches` - see values {values}"
            )

        values["metadata"] = metadata

        return values


class MetaOneRequest(FlowApiModel):
    class Config(CommonConfig):
        airtable_primary_key = "Sku"

    style_name: str
    # for back compat we allow optional on some of these but the validation catches it
    # skus are required but maybe called different things for now- we should deprecate the relay from generate-meta-one
    style_sku: Optional[str] = Field(primary_key=True)
    sku: Optional[str]
    # body can be inferred from sku
    body_code: Optional[str]
    contracts_failing: Optional[List[str]] = Field(
        airtable_fk_table="Contract Variables", default_factory=list
    )
    status: Optional[str]
    body_version: int
    sample_size: str = Field(db_write=False)
    size_code: str
    normed_size: Optional[str]
    mode: Optional[str] = Field(db_write=False)
    # printable_pieces: List[str] = Field(default_factory=list, db_write=False)
    sizes: Optional[List[str]] = Field(default_factory=list, db_write=False)
    pieces: Optional[List[MetaOnePiecesRequest]] = Field(default_factory=list)
    metadata: Optional[dict] = Field(default_factory=dict)
    print_type: str

    @property
    def is_sample_size(cls):
        return cls.sample_size == cls.size_code

    def get_inferred_piece_image_uri(cls, key):
        """
        the image path follows convention
        if there is a material offset we add that in the path
        """
        body_code = cls.body_code.lower().replace("-", "_")
        color = cls.sku.split(" ")[2].lower()
        size_code = cls.size_code.lower()
        pc = [p for p in cls.pieces if p.key == key][0]
        offset_size = pc.metadata.get("offset_size_inches", 0) * 300
        offset_size_qual = f"{int(offset_size)}/" if offset_size else ""
        uri = f"s3://meta-one-assets-prod/color_on_shape/{body_code}/v{cls.body_version}/{color}/{size_code}/pieces/{offset_size_qual}{key}.png"
        return uri

    def hash_pieces(self):
        import pandas as pd

        return uuid_str_from_dict(
            {
                "pieces": pd.DataFrame(self.dict()["pieces"])[
                    [
                        "key",
                        "material_code",
                        "artwork_uri",  # this is really a color code of sorts but not a clean model of color however if artwork changes its significant
                        "color_code",
                        # "base_image_uri",  #these are always generated in the same place and not really a proof
                        "offset_size_inches",
                    ]
                ].to_dict("records")
            }
        )

    @root_validator
    def _ids(cls, values):
        """"""
        # most of this is for safety and back compatibility on coercing the contract
        if values.get("style_sku"):
            # norm
            sku = values["style_sku"]
            # there part safety
            sku = f" ".join(sku.split(" ")[:3])
            if sku[2] != "-":
                sku = f"{sku[:2]}-{sku[2:]}"
            values["style_sku"] = sku
        if values.get("sku"):
            sku = values["sku"]
            if sku[2] != "-":
                sku = f"{sku[:2]}-{sku[2:]}"
            # circular but confirm we have the style sku
            values["style_sku"] = f" ".join(
                [f.lstrip().rstrip() for f in sku.split(" ")[:3]]
            )
        else:
            values["sku"] = f"{values['style_sku']} {values['size_code']}"

        if not values.get("body_code"):
            values["body_code"] = values["style_sku"].split(" ")[0].lstrip().rstrip()
        if values.get("body_code") and values.get("style_name"):
            values["body_code"] = values["body_code"].upper().replace("_", "-")
            # use the same as the style id but its a status object really
            sid = uuid_str_from_dict(
                {
                    "name": values["style_name"].lstrip().rstrip(),
                    "body_code": values["body_code"],
                }
            )
            # style id for the request - contains many sizes
            values["id"] = sid
            ##########################################################

        values["metadata"]["sku"] = values["sku"]
        return values


class StyleMetaOneRequest(MetaOneRequest):
    """
    Just use some different aliases but otherwise the same
    """

    name: str = Field(alias="style_name")
    pieces: Optional[List[MetaOnePiecesRequest]] = Field(
        default_factory=list, alias="piece_name_mapping"
    )
    body_code: Optional[str]


class BodyMetaOneRequest(FlowApiModel):
    """Pydantic model for Requests for Meta Ones

        d = {
        'style_name' : 'test',
        'sku' : 'test',
        'color_code' : 'test',
        'body_code' : 'test',
        'body_version' : 1,
        'size_code' : 'test',
        'pieces': [
            {
              'key' : 'test',
              'material_code' : 'test',
              'artwork_uri' : 'test',
              'color_code' : 'test',
              'offset_size_inches' : 0,
              'uri'  : 'test'
            }
        ],
        'body_one_ready_request': 'rec...'
    }

    should be used with MetaOneRequestIds to generate ids as used in the database by convention
    """

    class Config(CommonConfig):
        # specify in the name that will become
        airtable_attachment_fields = {}
        airtable_primary_key = "Name"

    id: Optional[str]
    body_code: str
    name: Optional[str] = Field(primary_key=True)
    body_version: int
    size_code: Optional[str]
    # trim_costs: Optional[float]
    # estimated_sewing_time: Optional[float]
    sample_size: str
    pieces: Optional[List[BodyMetaOnePiecesRequest]] = Field(default_factory=list)
    metadata: Optional[dict]
    contracts_failed: Optional[List[str]] = Field(default_factory=list)
    contract_failure_context: Optional[dict] = Field(db_write=False)
    body_one_ready_request: Optional[str] = Field(db_write=False)
    previous_differences: Optional[dict] = Field(db_write=False)

    @property
    def is_sample_size(cls):
        return cls.sample_size == cls.size_code

    @root_validator
    def _ids(cls, values):
        """
        The id for res flow api is generated as a hash of the key map.
        we use ID_FIELD as _id so that the users id can always be aliased to something else
        """
        # we cannot really valid business logic in ids but it would be nice to do something
        values["name"] = f"{values['body_code']}-V{values['body_version']}"

        return values


class MetaOneStatus(FlowApiModel):
    class Config(CommonConfig):
        airtable_table_name = "Meta One Response"

    id: str
    contracts_failed: List[str] = Field(airtable_fk_table="Contract Variables")
    status: Optional[str]
    stage: Optional[str]
    sizes: Optional[List[str]] = Field(default_factory=list)


class BodyMetaOneStatus(CommonBaseModel):
    id: str
    body_code: str
    body_version: int
    sample_size: str
    request_id: Optional[str]
    contracts_failed: List[str] = Field(default_factory=list)
    status: Optional[str]
    sizes: Optional[List[str]] = Field(default_factory=list)
    body_file_uploaded_at: Optional[str]
    copied_from_path: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]


# class BodyMetaOnePiece(CommonBaseModel):
#     key: str
#     piece_type: str
#     identity_symbol: Optional[str] = None


# class BodyMetaOne(BodyMetaOneStatus):
#     pieces: List[BodyMetaOnePiece]


class MetaOneResponse(FlowApiModel):
    class Config(CommonConfig):
        airtable_attachment_fields = {
            "Printable Pieces",
        }

    sku: str
    style_name: str
    body_code: str
    body_version: int
    print_type: Optional[str]
    size_code: Optional[str]
    style_sku: Optional[str] = Field(primary_key=True)
    contracts_failed: List[str] = Field(airtable_fk_table="Contract Variables")
    status: Optional[str]
    stage: Optional[str]
    printable_pieces: List[str] = Field(default_factory=list)
    sizes: List[str] = Field(default_factory=list)
    available_sizes: List[str] = Field(default_factory=list)
    metadata: Optional[dict]
    trace_log: Optional[str]
    # this joins to the style record - the styles pieces are hashed to generate it..
    piece_material_hash: Optional[str]
    # generate for convenience, can write to kafka - this joins to the BMS record
    sized_piece_material_hash: Optional[str] = Field(db_write=False)
    # trace log

    @root_validator
    def _ids(cls, values):
        if not values.get("style_sku"):
            sku = values["sku"]
            values["style_sku"] = f" ".join(
                [f.lstrip().rstrip() for f in sku.split(" ")[:3]]
            )

        # generate from the size code
        if not values.get("sized_piece_material_hash") and values.get(
            "piece_material_hash"
        ):
            values["sized_piece_material_hash"] = uuid_str_from_dict(
                {
                    "style_hash": values.get("piece_material_hash"),
                    "size_code": values["size_code"],
                }
            )

        return values


class BodyMetaOneResponseStage(Enum):
    ALL_CHECKED_02 = "02 All Contracts Checked"
    AUTO_CHECKED_01 = "01 Automated Contracts Checked"
    NOT_CHECKED = "00 Check Contracts"


class UpsertBodyResponse(CommonBaseModel):
    body_code: str
    body_version: int
    process_id: Optional[str] = Field(db_write=False)
    user_email: Optional[str] = Field(db_write=False)
    body_file_uploaded_at: Optional[str]
    metadata: Optional[dict] = Field(default_factory=dict)

    status: Optional[Union[str, FlowStatus]]


class BodyPieceMaterialRequest(FlowApiModel):
    id: str
    size_code: str
    number_of_materials: int
    body_code: str
    body_version: int
    ##for now this block is for the response object
    number_of_printable_pieces: int = Field(db_write=False)
    number_of_body_pieces: int = Field(db_write=False)
    total_raw_pieces_area: Optional[int] = Field(db_write=False)
    style_pieces_hash: Optional[str] = Field(db_write=False)
    ###########
    metadata: Optional[dict] = Field(default_factory=dict)
    sewing_time: float
    trim_costs: float
    piece_map: Optional[dict] = Field(default_factory=dict)
    nesting_statistics: List[dict] = Field(default_factory=list)
    created_at: Optional[str] = Field(exclude=True)
    updated_at: Optional[str] = Field(exclude=True)
    checksum: Optional[int]

    @staticmethod
    def _get_check_sum(values):
        """
        we return the integer check sum value
        """
        # we choose an invariant that does not depend on random nesting - if the combined physical piece areas changes something important has changed
        area_pieces_yds_physical = sum(
            [a["area_pieces_yds_physical"] for a in values["nesting_statistics"]]
        )
        return int(
            sum([values["trim_costs"], values["sewing_time"], area_pieces_yds_physical])
        )

    @root_validator
    def _ids(cls, values):
        """"""
        area_pieces_yds_raw = sum(
            [a["area_pieces_yds_raw"] for a in values["nesting_statistics"]]
        )

        values["checksum"] = BodyPieceMaterialRequest._get_check_sum(values)
        values["total_raw_pieces_area"] = area_pieces_yds_raw
        return values


class BodyPieceMaterialResponse(BodyPieceMaterialRequest):
    checksum: Optional[int] = Field(exclude=True)


class BodyPieceInfo(CommonBaseModel):
    key: str
    piece_type: str
    piece_type_name: str
    piece_code: str


class BodyMetaOneResponse(BodyMetaOneRequest):
    class Config(CommonConfig):
        airtable_attachment_fields = {
            "Printable Pieces",
            "Complementary Pieces",
            "Cut Files",
            "Poms",
        }

        # it seems for inheritance it would be better to choose one key here
        airtable_primary_key = "Name"
        # lesson: one of two things today we need to map db interfaces -> we are replacing a node with a node_id and we have factories that compute the ids from attributes on the object to prevent round trippin
        db_rename = {"node": "node_id"}

    sample_size: Optional[str]

    stage: Optional[Union[str, BodyMetaOneResponseStage]]
    contracts_failed: List[str] = Field(airtable_fk_table="Contract Variables")
    contract_failure_context: Optional[dict] = Field(db_write=False)
    previous_differences: Optional[dict] = Field(db_write=False)
    status: Optional[Union[str, FlowStatus]]
    node: Optional[str] = Field(id_mapper=linked_node_id_mapper)
    max_body_version: Optional[int]
    # many of these can be empty when the body is actually bad
    printable_pieces: List[str] = Field(default_factory=list)
    complementary_pieces: Optional[List[str]] = Field(default_factory=list)
    cut_files: Optional[List[str]] = Field(default_factory=list)
    pieces: Optional[List[BodyPieceInfo]] = Field(default_factory=list)
    poms: Optional[List[str]] = Field(default_factory=list)
    sizes: List[str] = Field(default_factory=list)
    trace_log: Optional[str]
    process_id: Optional[str] = Field(db_write=False)
    user_email: Optional[str] = Field(db_write=False)
    body_file_uploaded_at: Optional[str] = Field(db_write=False)
    # this will be an id that links to another record
    body_one_ready_request: Optional[str] = Field(db_write=False)
    # enum trim_type

    @root_validator
    def _ids(cls, values):
        """"""
        if values.get("body_code") and values.get("body_version"):
            values["body_code"] = values["body_code"].upper().replace("_", "-")
            values["name"] = f"{values['body_code']}-V{values['body_version']}"
            # use the same as the body id but its a status object really
            values["id"] = uuid_str_from_dict(
                {
                    "code": values["body_code"],
                    "version": int(values["body_version"]),
                    "profile": "default",
                }
            )

        metadata = values.get("metadata", {}) or {}
        metadata["sizes"] = values.get("sizes")
        values["metadata"] = metadata

        return values


from stringcase import titlecase, snakecase


class ContractVariable(FlowApiModel):
    name: str
    variable_name: str
    variable_name_spanish: str
    code: Optional[str]
    dxa_node: Optional[str]
    ordinal: Optional[int]
    record_id: str

    @root_validator
    def _ids(cls, values):
        return values


class MetaOneAssets(FlowApiModel):
    uri: str
    asset_trim_type: str
    metadata: Optional[dict]


class MetaOnePrintablePieceAnnotation(CommonBaseModel):
    position: GPoint
    size: GPoint
    angle: float = Field(default=0)
    name: str
    resource_uri: Optional[str]
    curvature: Optional[float] = Field(default=0)


class MetaOnePrintablePiece(FlowApiModel):
    id: str
    key: str
    piece_code: Optional[str]
    piece_type: str
    image_uri: str
    image_s3_version: Optional[str]
    annotated_image_s3_version: Optional[str]
    annotated_image_uri: Optional[str]
    # We should probably not allow this
    sew_identity_symbol: Optional[str] = "neutral"
    grouping_symbol: Optional[str]
    material_code: str
    color_code: str
    is_color_placement: bool
    artwork_uri: Optional[str]
    fusing_type: Optional[str]
    # this is not yet implemented - useful in contract
    commercial_acceptability_zone: Optional[str]
    normed_size: str
    size_code: str
    piece_ordinal: int
    piece_set_size: int
    outline: GLine = Field(default_factory=list)
    corners: List[GPoint] = Field(default_factory=list)
    seam_guides: GMultiLine = Field(default_factory=list)
    # notches: GMultiPoint = Field(default_factory=list)
    notches: GMultiLine = Field(default_factory=list)
    offset_size_px: int = Field(default=0)
    annotations: List[MetaOnePrintablePieceAnnotation] = Field(default_factory=list)
    deleted_at: Optional[str]
    metadata: Optional[dict] = Field(default_factory=dict)

    @root_validator
    def _ids(cls, values):
        """"""
        if not values.get("piece_code"):
            values["piece_code"] = f"-".join(values["key"].split("-")[-2:])

        return values


class MetaOnePieceCollection(FlowApiModel):
    class Config(CommonConfig):
        pass

    printable_pieces: List[MetaOnePrintablePiece] = Field(default_factory=list)
    metadata: Optional[dict] = Field(default_factory=dict)

    @property
    def piece_keys(cls):
        return [p.key for p in cls.printable_pieces]

    @property
    def material_codes(cls):
        return list(set([p.material_code for p in cls.printable_pieces]))

    def __getitem__(cls, key):
        for p in cls.printable_pieces:
            if p.key == key:
                return p
        raise Exception(
            f"the piece with key {key} is not in the list of known pieces {cls.piece_keys}"
        )

    def __iter__(self):
        for p in self.printable_pieces:
            yield p


class MetaOnePrintAsset(MetaOnePieceCollection):
    name: str
    sku: str
    body_code: str
    body_version: int
    style_code: Optional[str]
    style_color_code: Optional[str]
    style_material_code: Optional[str]
    size_code: str
    normed_size: Optional[str]
    assets: List[MetaOneAssets] = Field(default_factory=list)
    contracts_failing: List[str] = Field(default_factory=list)
    created_at: str
    updated_at: str

    @property
    def placed_images_uri(cls):
        body_code_lower = cls.body_code.lower().replace("-", "_")
        return f"s3://meta-one-assets-prod/color_on_shape/{body_code_lower}/v{cls.body_version}/{cls.style_color_code.lower()}/{cls.size_code.lower()}/pieces/"

    @property
    def annotated_images_uri(cls):
        body_code_lower = cls.body_code.lower().replace("-", "_")
        return f"{MetaOnePrintAsset.get_versioned_sku_uri()}/annotated-pieces/{body_code_lower}/v{cls.body_version}/{cls.style_code.replace(' ', '_')}_{cls.size_code}/"

    @property
    def annotated_images_metadata_uri(cls):
        body_code_lower = cls.body_code.lower().replace("-", "_")
        return f"{MetaOnePrintAsset.get_versioned_sku_uri()}/metadata/{body_code_lower}/v{cls.body_version}/{cls.style_code.replace(' ', '_')}_{cls.size_code}/"

    @staticmethod
    def get_annotated_images_metadata_uri_for_sku(sku, body_version):
        parts = sku.split(" ")
        assert (
            len(parts) == 4
        ), "You must pass a four part sku to get the uri for the cache"

        style_code = "_".join(parts[:3])
        size_code = parts[-1]
        body_code = parts[0]
        body_code_lower = body_code.lower().replace("-", "_")
        return f"{MetaOnePrintAsset.get_versioned_sku_uri()}/metadata/{body_code_lower}/v{body_version}/{style_code.replace(' ', '_')}_{size_code}/"

    @staticmethod
    def get_versioned_sku_uri():
        return f"s3://meta-one-assets-prod/versioned-skus/api-v9"

    @root_validator
    def _ids(cls, values):
        """"""
        if not values.get("style_code"):
            values["style_code"] = f" ".join(values["sku"].split(" ")[:3])

        values["style_color_code"] = values["style_code"].split(" ")[2].strip()
        values["style_material_code"] = values["style_code"].split(" ")[1].strip()

        return values


class MetaOneMaterialCategory(FlowApiModel):
    id: str
    name: str
    code: str
    use: Optional[str]
    materials: Optional[str]
    created_at: datetime.datetime
    updated_at: datetime.datetime
    metadata: dict


class MetaOneTrim(FlowApiModel):
    id: str
    name: str
    hash: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    metadata: dict
