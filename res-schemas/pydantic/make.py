"""Pydantic models for the event hooks system."""
from __future__ import annotations
from datetime import datetime
from typing import List, Optional, Set, Union, Dict
from uuid import UUID
from pydantic import Field, root_validator, BaseModel
import os
from schemas.pydantic.common import (
    FlowApiModel,
    HasuraModel,
    SKIP_PROPAGATION,
    CommonConfig,
    uuid_str_from_dict,
    id_mapper_factory,
    CommonBaseModel,
)
import re
import res
import pandas as pd
import json


def linked_node_id_mapper(**kwargs):
    """
    if we have a node which maps to node.name
    """
    return id_mapper_factory(["node"], alias={"node": "name"})(**kwargs)


class MakeOrder(HasuraModel):
    """Pydantic model for Make Order representations."""

    one_number: str
    sku: Optional[str]
    order_number: Optional[str]
    metadata: Optional[MakeOrder.Metadata] = Field(alias="metadata")


class PieceInstance(HasuraModel):
    """Pydantic model for Piece Instance representations."""

    class Config(HasuraModel.Config):
        """Extended config for PieceSet."""

        autogen_field_names: Set[str] = {"piece_set_id"}

    id: str = Field(alias="piece_id")
    one_number: str
    piece_set_id: Optional[Union[UUID, str]] = None
    piece_set_key: str
    metadata: Optional[PieceInstance.Metadata] = Field(alias="metadata")
    print_node_observation: Optional[Union[str, None]] = "PENDING"
    roll_inspection_node_observation: Optional[Union[str, None]] = "PENDING"


class MakeProductionRequest(HasuraModel):
    """Pydantic model for Make Production Request representation."""

    class Config(HasuraModel.Config):
        """Extended config for PieceSet."""

        autogen_field_names: Set[str] = {"id"}

    id: Optional[Union[UUID, str, None]] = None
    type: str = Field(alias="type")
    style_id: str = Field(alias="style_id")
    style_version: str = Field(alias="style_version")
    body_code: str = Field(alias="body_code")
    body_version: int = Field(alias="body_version")
    material_code: str = Field(alias="material_code")
    color_code: str = Field(alias="color_code")
    size_code: str = Field(alias="size_code")
    brand_code: str = Field(alias="brand_code")
    sales_channel: str = Field(alias="sales_channel")
    order_line_item_id: str = Field(alias="order_line_item_id")
    sku: str = Field(alias="sku")
    order_number: str = Field(alias="order_number")
    request_name: str = Field(alias="request_name")
    channel_line_item_id: str = Field(alias="channel_line_item_id")
    channel_order_id: str = Field(alias="channel_order_id")
    line_item_creation_date: datetime = Field(alias="line_item_creation_date")
    metadata: MakeProductionRequest.Metadata = Field(alias="metadata")


class PieceSet(HasuraModel):
    """Pydantic model for Piece Set representations."""

    class Config(HasuraModel.Config):
        """Extended config for PieceSet."""

        autogen_field_names: Set[str] = {"id", "one_number"}

    id: Optional[Union[UUID, str]] = None
    external_id: Optional[str] = Field(alias="id", exclude=True)
    key: Union[UUID, str] = Field(alias="piece_set_key")
    one_number: str
    metadata: PieceSet.Metadata = Field(alias="metadata")
    make_order: MakeOrder = Field(
        default_factory=dict,
        skip_propagation={"piece_instances"},
    )
    piece_instances: List[PieceInstance] = Field(
        alias="make_pieces",
        propagate_rename=[("id", "piece_set_id", SKIP_PROPAGATION)],
        skip_propagation={"make_order"},
    )


class Printer(HasuraModel):
    """Pydantic model for Printers."""

    class Config(HasuraModel.Config):
        """Extended config for PieceSet."""

        autogen_field_names: Set[str] = {"id"}

    id: Optional[Union[UUID, str]] = None
    name: str = Field(alias="Printer")
    metadata: Printer.Metadata = Field(alias="metadata")


class PrintJob(HasuraModel):
    """Pydantic model for Print Jobs."""

    class Config(HasuraModel.Config):
        """Extended config for PrintJob."""

        autogen_field_names: Set[str] = {"id", "printer_id"}

    id: Optional[Union[UUID, str]] = None
    name: str = Field(alias="Name")
    job_id: str = Field(alias="Jobid")
    status: str = Field(alias="Status")
    metadata: PrintJob.Metadata = Field(alias="metadata")
    start_time: datetime = Field(alias="StartTime")
    end_time: Optional[datetime] = Field(alias="EndTime")
    printer_name: str = Field(alias="Printer")
    printer_id: Optional[Union[UUID, str]] = None
    printer: Printer = Field(default_factory=dict)

    @staticmethod
    def get_status_name(code):
        return {
            "0": "PROCESSING",
            "1": "PRINTED",
        }.get(code, "TERMINATED")

    @staticmethod
    def get_ink_consumption(ink_consumption_data):
        ink_consumption = {"consumption_data": []}
        per_ink_data = {}
        for ink in ink_consumption_data["Ink"]:
            per_ink_data["Color"] = ink["Color"]
            for k, v in ink["Usage"].items():
                per_ink_data[k] = float(v)
            ink_consumption["consumption_data"].append(per_ink_data.copy())

        return ink_consumption

    @root_validator
    def _status_name(cls, values):
        values["status_name"] = PrintJob.get_status_name(values["status"])
        if values["status"] != "0":
            values["ink_consumption"] = PrintJob.get_ink_consumption(
                values["metadata"].dict()["MSinkusage"]
            )
            values["material_consumption"] = {
                "printed_length_mm": float(values["metadata"].dict()["PrintedLength"]),
                "printed_width_mm": float(values["metadata"].dict()["Width"]),
            }
        return values


class MSJobinfo(PrintJob):
    """Wrapper class for parsing messages with MSJobinfo."""

    def __init__(self, *, MSjobinfo: dict):
        """Pass the contents of MSjobinfo to the parent constructor."""
        super().__init__(**MSjobinfo)


class Nest(HasuraModel):
    class Config(HasuraModel.Config):
        autogen_field_names: Set[str] = {"id"}

    id: Optional[UUID] = None
    nest_key: str
    nest_file_path: str
    nest_length_px: int
    nest_length_yd: float
    asset_count: int
    utilization: float
    # todo: assets connected to piece sets etc.
    metadata: Nest.Metadata = Field(alias="metadata")


class RollSolutionNest(HasuraModel):
    class Config(HasuraModel.Config):
        autogen_field_names: Set[str] = {
            "id",
            "roll_solution_id",
            "nest_id",
            "metadata",
        }

    id: Optional[UUID] = None
    roll_solution_id: Optional[UUID] = None
    nest_id: Optional[UUID] = None
    nest: Nest = Field(alias="nest_info")
    metadata: RollSolutionNest.Metadata = Field(alias="metadata", exclude=True)


class RollSolution(HasuraModel):
    class Config(HasuraModel.Config):
        autogen_field_names: Set[str] = {"id"}

    id: Optional[UUID] = None
    roll_name: str
    roll_length_px: int
    roll_length_yd: float
    reserved_length_px: int
    reserved_length_yd: float
    asset_count: int
    utilization: float
    length_utilization: float
    roll_solutions_nests: List[RollSolutionNest] = Field(
        alias="nest_info", relationship_rhs="nest"
    )
    metadata: RollSolution.Metadata = Field(alias="metadata")


class MaterialSolution(HasuraModel):
    class Config(HasuraModel.Config):
        autogen_field_names: Set[str] = {"id"}

    id: Optional[UUID] = None
    solution_key: str
    solution_status: str
    material_code: str
    pending_asset_count: int
    total_roll_length_px: int
    total_roll_length_yd: float
    total_reserved_length_px: int
    total_reserved_length_yd: float
    total_asset_count: int
    total_utilization: float
    total_length_utilization: float
    roll_solutions: List[RollSolution] = Field(
        alias="roll_info",
        propagate_rename=[("id", "material_solution_id", SKIP_PROPAGATION)],
    )
    metadata: MaterialSolution.Metadata = Field(alias="metadata")


class NestedOne(HasuraModel):
    class Config(HasuraModel.Config):
        autogen_field_names: Set[str] = {"id"}

    id: Optional[UUID] = None
    piece_set_key: str
    piece_count: int
    height_nest_px: int
    width_nest_px: int
    utilization: float
    material_code: str
    nest_file_path: str
    one_number: int
    piece_type: str
    metadata: NestedOne.Metadata = Field(alias="metadata")


class PrintfilePiece(HasuraModel):
    class Config(HasuraModel.Config):
        autogen_field_names: Set[str] = {"id"}

    id: Optional[UUID] = None
    piece_id: str
    min_y_in: float = Field(alias="min_y_inches")
    max_y_in: float = Field(alias="max_y_inches")
    min_x_in: float = Field(alias="min_x_inches")
    metadata: PrintfilePiece.Metadata = Field(alias="metadata")


class Printfile(HasuraModel):
    class Config(HasuraModel.Config):
        autogen_field_names: Set[str] = {"id"}

    id: Optional[UUID] = None
    name: str = Field(alias="printfile_name")
    roll_name: str
    airtable_record_id: Optional[str]
    s3_path: str = Field(alias="printfile_s3_path")
    width_px: int = Field(alias="printfile_width_px")
    height_px: int = Field(alias="printfile_height_px")
    width_in: float = Field(alias="printfile_width_inches")
    height_in: float = Field(alias="printfile_height_inches")
    printfile_pieces: List[PrintfilePiece] = Field(
        alias="piece_info",
        propagate_rename=[("id", "printfile_id", SKIP_PROPAGATION)],
    )
    metadata: Printfile.Metadata = Field(alias="metadata")

    @root_validator
    def _printjob_name(cls, values):
        values["printjob_name"] = re.sub("[^A-Za-z0-9_]+", "_", values["name"])
        return values


class OnePiece(FlowApiModel):
    """Pydantic model for Piece Set representations."""

    # it seems like hasura should know how to populate the FK for us - we could do this in a validation or just before upsert too
    # make_one_order_id: Optional[Union[UUID, str]] = None
    # code: str = Field(key_field=True)
    meta_piece_id: Optional[str]
    # TODO a primary key piece id based on the one-code
    make_instance: int = 1
    uri: Optional[str]
    oid: Optional[str]
    contracts_failed: Optional[List[str]]
    code: str
    node_id: Optional[str]  # default the make node
    metadata: Optional[Dict]
    # style_piece_key: str = Field(exclude=True)
    # make_piece_key: str = Field(exclude=True)

    @staticmethod
    def make_pieces_for_one_request(
        request_id,
        pid,
        piece,
        healings=0,
        piece_observation_date=None,
        contracts_failed=[],
        node_id=None,
        uri=None,
        one_number=None,
        legacy_print_asset_id=None,
    ):
        """
        the initial data just creates the piece reference.
        later when we inspect pieces, we may fail and generate a new piece instance

        piece / piece_code is a suffix of the piece name i.e the part without body and version
        """
        piece = f"-".join(piece.split("-")[3:])
        # this is a hack for now - we should not have these petite suffixes
        if piece[-2:] == "_P":
            piece = piece[:-2]

        # here is another code we could use to denote the uniqueness but it can be generated
        # piece = f"{piece}-H{healings}"

        return {
            # TODO we need to heavily unit test id generation - for example here when validating it would be wrong to set the id for the user
            # so having any key fields on one piece violates this as the system will try to generate the id
            # REQUEST ID IS current parent ID in practice -it comes from f(order_item, rank)
            "id": uuid_str_from_dict({"request_id": request_id, "piece_code": piece}),
            "oid": uuid_str_from_dict(
                {"one_number": int(float(one_number)), "piece_code": piece}
            )
            if one_number
            else None,
            "meta_piece_id": pid,
            # this will be a lookup field to the pieces table - its globally unique
            # "make_piece_key": f"{one_code}:{piece}",
            # this is a lookup to the style piece e.g. unique on body version and piece key
            # "style_piece_key": body_piece_key,
            # this is local to the style/body and the order but unique within the body - the healing increment is added
            "code": piece,
            "metadata": {"one_code": None, "version": 2, "one_number": one_number},
            "make_instance": 1 + healings,
            "observed_at": piece_observation_date,
            "node_id": node_id,
            "uri": uri,
            "contracts_failed": contracts_failed,
            # we can phase this out and start pointing the printfile and observations to the make piece ->moving to one.codes
            # "piece_instance_id": None,
        }

    @root_validator
    def _id(cls, values):
        key_fields = [
            name
            for name, meta in cls.__fields__.items()
            if meta.field_info.extra.get("key_field")
        ]

        assert (
            len(key_fields) == 0
        ), "The one piece id cannot be generated from keys; Do not set any key attributes. The runtime id will be f(OneCode, PieceId)"
        return values


class OneOrder(FlowApiModel):
    """
    Pydantic model for Piece Set representations.
    Example at time of commenting:
       MakeOneOrder(**{'one_code' : "ONE", "metadatax": {'ok':'fine'},'id': "mine" ,  'pieces' : [{"code": "test", 'meta_piece_id':'bcda8cab-9228-2a23-f3a6-1b9b78308c0c'}] })

    There are some bogus values in here but we are demonstrating the following;
    - the ids are not included as they are generated from the key fields on the model
    - the uuids for other things like the meta piece have been resolved elsewhere and attached as strings in the model
    - there is no need to resolve the parent id i.e. Make_OnePieces->MakeOneOrder since Hasura can resolve that
    """

    class Config(CommonConfig):
        # specify in the name that will become
        airtable_attachment_fields = {"One Pieces"}

    one_code: str = Field(primary_key=True)
    request_id: str = Field(exclude=True)
    oid: Optional[str]
    order_item_id: str
    order_item_rank: int = Field(default=0)
    sku: str
    metadata: Dict = Field(default_factory=dict)
    # how we generate the ids for this on the way in -> will need a factor from the parent or ??
    one_pieces: List[OnePiece] = Field(alias="one_pieces")
    one_code: Optional[str]
    status: str = "PENDING"
    flags: Optional[List[str]]
    make_instance: Optional[int]
    one_code_uri: Optional[str] = Field(alias="uri")
    one_number: Optional[int] = Field(default=0)
    order_number: Optional[str]
    # there are some props used to cook up one-codes but we dont need to see them as as they are inferred in the relations
    style_size_id: Optional[str]
    customization: Optional[dict] = Field(default_factory=dict)
    # TODO a primary key piece id based on the one-code
    size_code: Optional[str] = Field(exclude=True)
    style_rank: Optional[int] = Field(exclude=True)
    style_group: Optional[str] = Field(exclude=True)
    style_size_id: str

    @root_validator
    def _code(cls, values):
        """
        The id for res flow api is generated as a hash of the key map.
        we use ID_FIELD as _id so that the users id can always be aliased to something else
        """
        # use gets but they are required on the model
        sku = values.get("sku")
        size_code = sku.split("-")[-1]
        size_scale = values.get("size_scale", size_code[-2:])
        # required in metadata if notprovided by object
        style_group = values.get("style_group") or values["metadata"].get("style_group")
        style_rank = values.get("style_rank") or values["metadata"].get("style_rank")
        # for idempotence
        values["metadata"]["style_group"] = values["style_group"] = style_group
        values["metadata"]["style_rank"] = values["style_rank"] = style_rank
        values["metadata"]["version"] = 2

        size_code = values.get("size_code", size_code)
        make_instance = values["make_instance"]

        values["one_code"] = (
            values.get("one_code")
            or f"{style_group}-G{style_rank}-{size_scale}-{make_instance}"
        )

        # new strategy
        # a request id is needed - this could be any unique request id like a fulfillment id but not necessarily - used to build sets
        # dont set key fields on the one code - control here as it is a runtime key
        values["id"] = values["request_id"]
        # for one_number==0 we do not save an oid at all
        values["oid"] = (
            uuid_str_from_dict({"id": values["one_number"]})
            if values.get("one_number")
            else None
        )

        return values


class OneOrderResponse(FlowApiModel):
    class Config(CommonConfig):
        # all airtable stuff is Title Case
        airtable_primary_key = "One Number"
        airtable_attachment_fields = {"Piece Images"}

        overridden_airtable_baseid = "appKwfiVbkdJGEqTm"
        RES_ENV = os.environ.get("RES_ENV")
        if RES_ENV == "production":
            overridden_airtable_baseid = "apps71RVe4YgfOO8q"

    one_number: str = Field(primary_key=True)
    sku: str
    pieces: List[str] = Field(airtable_fk_table="Piece Names")
    piece_images: List[str]
    order_number: str


class MachineConfigs(HasuraModel):
    """Pydantic model for machines and their configurations."""

    class Config(HasuraModel.Config):
        autogen_field_names: Set[str] = {"id"}

    id: Optional[Union[UUID, str]] = None
    name: str
    node: Optional[str]
    description: Optional[str]
    configs: dict
    serial_number: str
    is_active: bool = False
    metadata: Optional[dict] = {}


class OneOrderPiecesStatus(FlowApiModel):
    id: str
    created_at: str
    updated_at: str
    """  the base image of the uri - url encoded optionally """
    uri: str
    """the number of pieces that need to be completed"""
    piece_set_size: int
    """ the unique piece index in the set that needs to be completed """
    piece_ordinal: int
    code: str
    """  contracts that ever failed """
    contracts_failed: List[str] = Field(default_factory=list)
    """  contracts failing right now if the piece just failed inspection """
    contracts_failing: List[str] = Field(default_factory=list)
    """  the process node id - has values like RollPacking and RollInspection - should be mapped to make nodes  """
    node: Optional[str]
    """   the piece observation date  """
    observed_at: Optional[str]
    make_instance: int
    """
    we store the history of inspections and healings - the current inspection might not have happened yet
    if it has we should see all defects and the pass/fail for both inspection points
    """
    piece_healing_history: Optional[list] = Field(default_factory=list)
    """
    We store the last inspection details which may be blank if the piece is healing
    example records are like [
     {
         print_asset_piece_id': 'res0AE1994156',
        'defect_name': {'Horizontal Lines | Lineas Horizontales'},
        'fail': {False, True},
        'inspected_at': '2023-11-10 11:25:07.805424+00:00',
        'healing_print_asset_at': '2023-11-08 00:08:33.460573+00:00'}],
    ]
    """
    last_inspection_details: Optional[dict] = Field(default_factory=dict)
    """  for convenience we store the latest inspection result set {pass/fail} """
    current_inspection_status: Optional[set]


class OneOrderStatus(FlowApiModel):
    created_at: str
    updated_at: str
    order_number: str
    one_code: str
    one_number: Optional[str]
    sku: str
    one_pieces: List[OneOrderPiecesStatus] = Field(default_factory=list)


class OneOrderStatusNodeAndExpDate(CommonBaseModel):
    order_number: str
    one_number: Optional[str]
    sku: str
    make_node: Optional[str]
    expected_delivery_date: Optional[datetime]
    fulfilled_date: Optional[datetime]
    digital_assets_ready_date: Optional[datetime]


class RollsRequest(FlowApiModel):
    class Config(CommonConfig):
        # all airtable stuff is Title Case
        airtable_primary_key = "Roll Primary Key"
        airtable_attachment_fields = []

    id: Optional[str]
    roll_type: str
    roll_length: Optional[float]
    roll_key: str
    #    To Do: Change kafka topic to be material code, and make material_name_match nullable
    material_code: str = Field(alias="material_name_match")
    roll_id: int
    metadata: Optional[dict] = Field(default_factory=dict)
    warehouse_rack_location: Optional[str] = Field(alias="_warehouse_rack_location")
    po_number: str
    standard_cost_per_unit: Optional[float]
    roll_primary_key: Optional[str] = Field(primary_key=True)

    #    "material": "recufP4asmM7SkMMg",
    #    "purchasing_record_id": "recG94k8u14CuRiGx",
    #    "location_id": ""

    @root_validator
    def _metadata(cls, values):
        # """
        # The id for res flow api is generated as a hash of the key map.
        # we use ID_FIELD as _id so that the users id can always be aliased to something else
        # """

        metadata = values.get("metadata", {}) or {}
        keys = ["material", "purchasing_record_id", "location_id"]
        for key in keys:
            if key in values:
                metadata[key] = values[key]

        if values["id"] == "null":
            values["id"] = None

        values[
            "roll_primary_key"
        ] = f"R{values.get('roll_id')}: {values.get('material_code')}"

        return values


class OnePieceObservation(FlowApiModel):
    class Config(CommonConfig):
        airtable_primary_key = "One Number"

    # # the id may be known but our parent validator can set it as a function of the body and size
    # for now we are going to remove this guy because we want to use this with one code in future
    id: Optional[str] = Field(exclude=True)
    # instead of id, we will use oid which is f(one_number,code:=piece-code-suffix)
    oid: Optional[str] = Field(primary_key=True)
    code: str
    # taken from the parent set
    node_id: Optional[str]
    node: Optional[str] = Field(exclude=True)
    status: Optional[str] = Field()
    set_key: Optional[str] = Field()
    metadata: Optional[dict] = Field(default_factory=dict)
    printer_file_id: Optional[str] = Field()
    roll_name: Optional[str] = Field()
    nest_key: Optional[str] = Field()
    roll_key: Optional[str] = Field()
    print_job_name: Optional[str] = Field()
    material_code: Optional[str] = Field()

    healing: Optional[bool] = Field(exclude=True)
    make_instance: Optional[int] = Field(default=1)
    defects: Optional[List[str]] = Field(default_factory=list)
    contracts_failed: Optional[List[str]] = Field(default_factory=list)
    observed_at: Optional[str]
    # TODO i am not sure about the validation on the upsert when not affecting cols. adding this for now
    # This is a HACK because i dont know how to upsert and say that we are not updating this one_order_id anyway but it gives a null violation
    # in future we can make this a function of a one code or one number that will ne known and supply it (and still its not updated)
    # recall this is the parent id but we are not looking that up here and just using the oid to update children directly
    # so we add a dummy uuid here but this would be dangerous if the mutation updated it (which it should never do)
    one_order_id: Optional[str] = Field(default="de55574a-eac0-a126-881f-6f0efb34e61f")
    created_at: Optional[str] = Field(exclude=True)
    updated_at: Optional[str] = Field(exclude=True)
    # TODO ensure piece codes is only a suffix

    @root_validator
    def _metadata(cls, values):
        values["node_id"] = linked_node_id_mapper(**values)

        return values


class OnePieceLateAtNodeResponse(FlowApiModel):
    """
    This class is used to update airtable OneOrderRespones table with late pieces. Not setup or needed to do hasura updates.
    This is used only after a read of late pieces from hasura and then a write to airtable.
    """

    class Config(CommonConfig):
        airtable_primary_key = "One Number"
        # override the default pydantic table naming convention
        overridden_airtable_tablename = "One Order Responses"
        overridden_airtable_baseid = "appKwfiVbkdJGEqTm"
        RES_ENV = os.environ.get("RES_ENV")
        if RES_ENV == "production":
            overridden_airtable_baseid = "apps71RVe4YgfOO8q"

    # db_rename = {"node": "node_id", "status": "node_status"}
    delayed_printer_7days: Optional[List[str]] = Field(airtable_fk_table="Piece Names")
    delayed_printer_1days: Optional[List[str]] = Field(airtable_fk_table="Piece Names")
    delayed_rollinspection_7days: Optional[List[str]] = Field(
        airtable_fk_table="Piece Names"
    )
    delayed_rollinpsection_1days: Optional[List[str]] = Field(
        airtable_fk_table="Piece Names"
    )
    delayed_rollpacking_7days: Optional[List[str]] = Field(
        airtable_fk_table="Piece Names"
    )
    delayed_rollpacking_1days: Optional[List[str]] = Field(
        airtable_fk_table="Piece Names"
    )

    one_code: str = Field(primary_key=True, db_write=False)
    one_number: str = Field(primary_key=True)


class OnePieceSetUpdateRequest(FlowApiModel):
    """
    This object maps to kafka observation events
    it is saved to the database by a mutations that take in piece observations for one or more pieces and returns all the ONE effected and all their pieces
    the response is denormed for relay to queues but our primary objective is to capture the piece states in
    the usage is normally to have one or more pieces up to the full number of pieces in the one but grouped by common node and contracts/defects failed
    then we can retrieve the one level data after making the batch updates
    so it looks like these are both one level but really one is a partial set and one is necessary a full set
    """

    class Config(CommonConfig):
        airtable_primary_key = "One Number"
        db_rename = {"node": "node_id"}

    id: Optional[str] = Field(exclude=True)
    # instead of id, we will use oid which is f(one_number,code:=piece-code-suffix)
    oid: Optional[str] = Field(primary_key=True)
    one_code: str = Field(primary_key=True, db_write=False)
    one_number: int = Field(db_write=False)

    metadata: Optional[dict] = Field(default_factory=dict, db_write=False)
    created_at: Optional[str] = Field(exclude=True)
    updated_at: Optional[str] = Field(exclude=True)
    pieces: Optional[List[OnePieceObservation]] = Field(default_factory=list)
    # piece level

    contracts_failed: Optional[List[str]] = Field(
        airtable_fk_table="Contracts", default_factory=list, db_write=False
    )
    # pattern: child elements are projected onto the pieces for the group to be more compact and because they are excluded they are not exported from the mode in e..g db_dict
    node: Optional[str] = Field(id_mapper=linked_node_id_mapper, db_write=False)
    status: Optional[str] = Field(exclude=False, db_write=False)
    defects: Optional[List[str]] = Field(default_factory=list, db_write=False)
    observed_at: Optional[str] = Field(db_write=False)

    @root_validator
    def _metadata(cls, values):
        if "id" not in values:
            values["id"] = uuid_str_from_dict({"id": values["one_code"]})
        values["oid"] = (
            uuid_str_from_dict({"id": values["one_number"]})
            if values.get("one_number")
            else None
        )
        return values

    def project_pieces(cls, return_dict=False):
        """
        convenience to take the compact form and generate the dense form (adding attributes onto each child piece)
        properties that are piece level are projected onto each piece for the upsert

        should be idempotent cls.project_pieces() == cls.project_pieces().project_pieces()
        """
        d = cls.dict()
        pieces = d.pop("pieces")

        def _update(p):
            for c in [
                "node",
                "status",
                "metadata",
                "defects",
                "observed_at",
                "contracts_failed",
                "one_number",
                "metadata",
            ]:
                p[c] = d.get(c)

            return p

        def _update_from_metadata_ifset(p):
            d.get("metadata")["historical_one_number"] = d.get("one_number")

            for c in [
                "print_file_id",
                "roll_name",
                "nest_key",
                "roll_key",
                "printjob_name",
                "material_code",
            ]:
                if d["metadata"].get(c):
                    p[c] = d["metadata"].get(c)

            if p.get("print_file_id"):
                p["printer_file_id"] = p.pop("print_file_id")
            if p.get("printjob_name"):
                p["print_job_name"] = p.pop("printjob_name")

            return p

        project_pieces = [_update(dict(p)) for p in pieces]

        project_pieces = [_update_from_metadata_ifset(dict(p)) for p in project_pieces]

        for p in project_pieces:
            p["oid"] = res.utils.uuid_str_from_dict(
                {"one_number": int(float(d["one_number"])), "piece_code": p["code"]}
            )
            p["set_key"] = (
                p["metadata"].get("nest_key")
                or p["metadata"].get("print_file_id")
                or p["metadata"].get("roll_key")
            )

            # this was being handled upstream - no longer seems to be - setting to 1 if None
            if p.get("make_instance") == None:
                p["make_instance"] = 1

            # p.pop("metadata") want to leave on the metadata - log that too

        d["pieces"] = project_pieces

        return OnePieceSetUpdateRequest(**d) if not return_dict else d

    def partition_payloads(assets, re_repartition=False):
        """
        experimentally consider buffering and re-partitioning for efficiency or just relay as the client sent it
        """
        import pandas as pd

        if isinstance(assets, dict):
            assets = [assets]
        if isinstance(assets, list):
            assets = pd.DataFrame(assets)

        if re_repartition:
            assets["C"] = assets["contracts_failed"].map(set).map(str)
            assets["D"] = assets["defects"].map(set).map(str)
            # based on the headers we can group and re-partition a batch
            for _, data in assets.groupby(
                "one_number", "C", "D", "status", "observed_at"
            ):
                pieces = data["pieces"].set()
                data = data.to_dict("records")[0]
                data["pieces"] = pieces
                data = OnePieceSetUpdateRequest(**data).project_pieces()
                yield data
        else:
            # for asset in assets: # if assets is a df, this just gives the cols in teh DF
            for index, asset in assets.iterrows():
                if asset.get("one_code") is None:
                    asset["one_code"] = f"FakeOneCode-{asset['one_number']}"

                for piece in asset["pieces"]:
                    if piece.get("code") is not None:
                        piece["code"] = "-".join(piece["code"].split("-")[-2:])
                if not isinstance(asset["pieces"], list):
                    # in some kafka messages - the pieces are on one_pieces and vica versa. Right now, these we just wont process = # todo dead letter them
                    res.utils.logger.warn(
                        f"{asset['id']} does not seem to be a valid kakfa messsage for make_piece_observation_request.  pieces is null. ignoring"
                    )

                else:
                    ur = OnePieceSetUpdateRequest(**asset).project_pieces()
                    yield ur


class OnePieceSetUpdateResponse(OnePieceSetUpdateRequest):
    """
    the one level responds should always compile and report the fill state of the ONE
    """

    class Config(CommonConfig):
        airtable_primary_key = "One Number"
        # override the default pydantic table naming convention
        overridden_airtable_tablename = "One Order Responses"
        overridden_airtable_baseid = "appKwfiVbkdJGEqTm"
        RES_ENV = os.environ.get("RES_ENV")
        if RES_ENV == "production":
            overridden_airtable_baseid = "apps71RVe4YgfOO8q"

    # this is in the base class so not sure why its needed here?
    one_number: str = Field(primary_key=True)
    pieces: Optional[List[str]] = Field(airtable_fk_table="Piece Names")
    healing: Optional[List[str]] = Field(airtable_fk_table="Piece Names")
    # TBD node names, shows list of pieces (which are linked fields) at each node
    ###the following are mutually exclusive
    ##

    make_inbox: Optional[List[str]] = Field(airtable_fk_table="Piece Names")
    make_print_rollpacking: Optional[List[str]] = Field(airtable_fk_table="Piece Names")
    make_print_printer: Optional[List[str]] = Field(airtable_fk_table="Piece Names")
    make_print_rollinspection: Optional[List[str]] = Field(
        airtable_fk_table="Piece Names"
    )
    make_cut: Optional[List[str]] = Field(airtable_fk_table="Piece Names")
    make_setbuilding_trims: Optional[List[str]] = Field(airtable_fk_table="Piece Names")
    make_setbuilding: Optional[List[str]] = Field(airtable_fk_table="Piece Names")

    delayed_printer_1days: Optional[List[str]] = Field(airtable_fk_table="Piece Names")
    delayed_printer_7days: Optional[List[str]] = Field(airtable_fk_table="Piece Names")
    delayed_rollpacking_1days: Optional[List[str]] = Field(
        airtable_fk_table="Piece Names"
    )
    delayed_rollpacking_7days: Optional[List[str]] = Field(
        airtable_fk_table="Piece Names"
    )
    delayed_rollinpsection_1days: Optional[List[str]] = Field(
        airtable_fk_table="Piece Names"
    )
    delayed_rollinspection_7days: Optional[List[str]] = Field(
        airtable_fk_table="Piece Names"
    )


class OneInInventory(FlowApiModel):
    """Pydantic model for Inventory Service representations."""

    one_number: str
    reservation_status: Optional[str]
    bin_location: Optional[str]
    sku_bar_code_scanned: str
    warehouse_checkin_location: Optional[str]
    operator_name: Optional[str]
    work_station: Optional[str]
    checkin_type: Optional[str]
    one_order_id: Optional[str]
    created_at: Optional[str] = Field(exclude=True)
    updated_at: Optional[str] = Field(exclude=True)
    metadata: Optional[Dict]


class InventoryCheckout(FlowApiModel):
    """Pydantic model for Inventory checkout representations."""

    checkout_at: Optional[str]
    fulfillment_item_id: Optional[Union[UUID, str]] = None


class ConsumptionAtNode(BaseModel):
    consumption_label: str
    consumption_name: str
    consumption_category: str
    node: str
    consumption_value: float
    consumption_units: str


class PieceAssetRequest(FlowApiModel):
    key: str
    make_sequence_number: int = Field(default=0)
    multiplicity: int = Field(default=1)
    request_group_id: Optional[str]


class PrepPrintAssetRequest(FlowApiModel):
    # print asset id
    id: str
    sku: str
    one_number: str
    sales_channel: str
    sku_make_sequence_number: Optional[int]
    customer_order_number: Optional[str]
    customer_order_date: Optional[str]
    # this should probably be required
    upstream_request_id: Optional[str]

    piece_sequence_numbers: List[PieceAssetRequest] = Field(default_factory=list)

    meta_one_cache_path: Optional[str]

    body_code: Optional[str]
    body_version: Union[int, str]
    size_code: Optional[str]
    color_code: Optional[str]
    material_code: Optional[str]
    piece_type: Optional[str]
    created_at: Optional[str]
    flow: Optional[str] = Field(default="default")

    # contains make order id and print request id, custom email
    metadata: Optional[dict] = Field(default_factory=dict)

    @root_validator
    def _code(cls, values):
        # back compat - ppp should not have been string but it was
        values["body_version"] = str(values["body_version"])

        for i, part in enumerate(
            ["body_code", "material_code", "color_code", "size_code"]
        ):
            if not values.get(part):
                values[part] = values["sku"].split(" ")[i].strip()

        if not values.get("created_at"):
            values["created_at"] = datetime.utcnow().isoformat()

        return values


class MakeAssetRequest(FlowApiModel):
    """
    Anything we want to specify in a make order request
    """

    sku: str
    style_sku: Optional[str]
    body_version: int
    brand_code: str
    brand_name: Optional[str]
    style_id: str
    style_version: int
    request_type: str
    request_name: str
    sales_channel: str
    allocation_status: Optional[str]
    ordered_at: Union[str, pd.Timestamp]

    #  info about the order - if this thing corresponds to an actual order.
    order_name: str
    order_line_item_id: Optional[str]
    priority_type: Optional[str]

    # god knows
    channel_order_id: Optional[str]
    channel_line_item_id: Optional[str]
    flag_for_review_tags: Optional[List[str]]

    metadata: Optional[dict] = Field(default_factory=dict)

    @root_validator
    def _code(cls, values):
        if not values.get("style_sku"):
            sku = values["sku"]
            if sku[2] != "-":
                values["sku"] = sku = f"{sku[:2]}-{sku[2:]}"
            values["style_sku"] = " ".join([f for f in sku.split(" ")[:3]])
        # experimental consider coercing pandas types
        for f in ["ordered_at"]:
            if values.get(f) and not isinstance(values[f], str):
                values[f] = values[f].isoformat()
        return values


class MaterialInfo(CommonBaseModel):
    """
    The status about the material
    """

    material_code: str
    material_name: str
    unscoured_or_printable_yards: float
    ready_to_print_yards: float
    flagged_yards_otherwise_ready_to_print: float
    flagged_yards: float
    calculated_at: Union[str, pd.Timestamp]


class OrderMaterialInfo(CommonBaseModel):
    material_code: str
    scouring_or_printable_yards: float
    ready_to_print_yards: float
    pending_additional_yards: float
    expected_remaining_yards: float
    order_required_yards: float
    safe_to_order: bool
    reason: str


class SkuAndQuantity(CommonBaseModel):
    sku: str
    quantity: int


class OrderInfoRequest(CommonBaseModel):
    skus: List[SkuAndQuantity]


class OrderMaterialInfoResponse(CommonBaseModel):
    """
    The status about the material for a sku
    """

    sku: str
    quantity: int
    material_info: List[OrderMaterialInfo]
    safe_to_order: bool
    reason: str


class MakeAssetStatus(FlowApiModel):
    """
    The full expanded status of the make request
    """

    id: str
    one_number: str
    status: Optional[str]
    sku: str
    created_at: Union[str, pd.Timestamp]
    updated_at: Union[str, pd.Timestamp]
    cancelled_at: Optional[Union[str, pd.Timestamp]]
    airtable_rid: Optional[str]
    print_request_airtable_id: Optional[str]
    order_line_item_id: Optional[str]
    make_assets_checked_at: Optional[Union[str, pd.Timestamp]]
    dxa_assets_ready_at: Optional[Union[str, pd.Timestamp]]
    entered_assembly_at: Optional[Union[str, pd.Timestamp]]

    @root_validator
    def _code(cls, values):
        for f in [
            "created_at",
            "updated_at",
            "cancelled_at",
            "ordered_at",
            "dxa_assets_ready_at",
            "make_assets_checked_at",
        ]:
            if values.get(f) and not isinstance(values[f], str):
                values[f] = values[f].isoformat()
        return values


class UnassignRollsRequest(BaseModel):
    roll_names: List[str]
    notes: Optional[str]
    contract_variables: Optional[List[str]]


class UnassignRollResponse(BaseModel):
    roll_name: str
    unassigned_assets: int
    unassigned_nests: int
    unassigned_printfiles: int


class UnassignRollsResponse(BaseModel):
    rolls_info: List[UnassignRollResponse]


"""
PREP PRINT PIECES
"""

#####
##
#####


class PrepPrintPiece(FlowApiModel):
    """
    keys
    """

    piece_id: str
    asset_id: str
    asset_key: str
    piece_name: str
    piece_code: str
    meta_piece_key: str
    make_sequence: int = Field(default=0)
    """
    uris
    """
    filename: str
    outline_uri: str
    cutline_uri: str
    artwork_uri: str
    meta_one_cache_path: Optional[str]
    """
    added piece info
    """
    multiplicity: str = Field(default=1)
    requires_block_buffer: bool = Field(default=False)
    piece_area_yds: Optional[float]
    fusing_type: Optional[str]
    annotation_json: Optional[Union[str, dict]]
    piece_ordinal: Optional[int]
    piece_set_size: Optional[int]
    piece_type: Optional[str]

    # The BMC+Size
    sku_make_sequence_number: Optional[int]
    # THE BMC
    style_make_sequence_number: Optional[int]

    @root_validator
    def _vals(cls, values):
        if "annotation_json" in values and isinstance(values["annotation_json"], dict):
            values["annotation_json"] = res.utils.numpy_json_dumps(
                values["annotation_json"]
            )

        return values

    @property
    def annotation(cls):
        return json.loads(cls.annotation_json)


class PPPStats(CommonBaseModel):
    height_nest_yds: Optional[float]
    width_nest_yds: Optional[float]
    area_nest_yds: Optional[float]
    area_nest_utilized_pct: Optional[float]
    width_nest_yds_physical: Optional[float]
    height_nest_yds_physical: Optional[float]
    area_nest_yds_physical: Optional[float]
    area_nest_utilized_pct_physical: Optional[float]
    area_pieces_yds_physical: Optional[float]
    height_nest_yds_raw: Optional[float]
    width_nest_yds_raw: Optional[float]
    area_nest_yds_raw: Optional[float]
    area_nest_utilized_pct_raw: Optional[float]
    area_pieces_yds_raw: Optional[float]
    height_nest_yds_compensated: Optional[float]
    width_nest_yds_compensated: Optional[float]
    area_nest_yds_compensated: Optional[float]
    area_nest_utilized_pct_compensated: Optional[float]
    area_pieces_yds_compensated: Optional[float]
    piece_count: float


class PrepPieceResponse(FlowApiModel, PPPStats):
    id: str
    one_number: str
    sku: str
    body_version: Union[int, str]
    sales_channel: Optional[str]
    upstream_request_id: str

    body_code: Optional[str]
    color_code: Optional[str]
    size_code: Optional[str]
    material_code: Optional[str]
    piece_type: Optional[str]

    uri: Optional[str]
    piece_set_key: Optional[str]

    flow: Optional[str] = Field(default="v2")

    pieces: Optional[dict] = Field(default_factory=dict)
    make_pieces: Optional[List[PrepPrintPiece]] = Field(default_factory=list)
    metadata: Optional[dict] = Field(default_factory=dict)

    @root_validator
    def _code(cls, values):
        # back compat - ppp should not have been string but it was
        values["body_version"] = str(values["body_version"])

        for i, part in enumerate(
            ["body_code", "material_code", "color_code", "size_code"]
        ):
            if not values.get(part):
                values[part] = values["sku"].split(" ")[i].strip()

        if not values.get("created_at"):
            values["created_at"] = datetime.utcnow().isoformat()

        return values
