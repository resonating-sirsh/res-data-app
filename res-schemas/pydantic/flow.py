from __future__ import annotations
from typing import Dict, Optional, List
from schemas.pydantic.common import (
    CommonConfig,
    Field,
    FlowApiModel,
    id_mapper_factory,
    root_validator,
)


def node_id_mapper(**kwargs):
    return id_mapper_factory(["name"])(**kwargs)


def linked_node_id_mapper(**kwargs):
    """
    if we have a node which maps to node.name
    """
    return id_mapper_factory(["node"], alias={"node": "name"})(**kwargs)


def contract_id_mapper(**kwargs):
    return id_mapper_factory(["key", "version"])(**kwargs)


def owner_id_mapper(**kwargs):
    """
    if we have an owner that maps to user.email
    """
    return id_mapper_factory(["owner"], alias={"owner": "email"})(**kwargs)


def user_id_mapper(**kwargs):
    return id_mapper_factory(["email"])(**kwargs)


class User(FlowApiModel):
    email: str
    slack_uid: Optional[str]
    alias: Optional[str]
    metadata: Optional[Dict] = Field(default_factory=dict)

    @root_validator
    def _ids_and_defaults(cls, values):
        # todo regexify this one
        assert "@" in values["email"] and "." in values["email"], "not a valid email"
        values["alias"] = values.get("alias") or values["email"].split("@")[0]
        values["id"] = user_id_mapper(**values)
        return values


class Node(FlowApiModel):
    name: str
    metadata: Optional[Dict] = Field(default_factory=dict)
    type: str = Field(default="Flow Node")

    @root_validator
    def _ids_and_defaults(cls, values):
        values["id"] = node_id_mapper(**values)
        return values


class Contract(FlowApiModel):
    """The flow.contracts type for defining contracts anywhere in the ONE.platform"""

    class Config(CommonConfig):
        # specify in the name that will become
        airtable_attachment_fields = {}
        # todo move these to field spec as options too
        airtable_custom_types = {"Owner": "singleCollaborator", "Uri": "url"}
        db_rename = {"owner": "owner_id", "node": "node_id"}

    key: str = Field(primary_key=True)
    # the name is defaulting below to key using the root validator
    name: Optional[str]
    description: str
    status: str = Field(default="Active")
    version: int = Field(default=1.0)
    owner: str = Field(id_mapper=owner_id_mapper)
    # a reminder that the upsert wrapper always resolves these ids for the database but our model shows the name
    node: str = Field(id_mapper=linked_node_id_mapper)
    uri: Optional[str]
    asset_type: Optional[str]
    required_exiting_flows: List[str] = Field(default_factory=list)
    metadata: Optional[Dict] = Field(default_factory=dict)

    @root_validator
    def _ids_and_defaults(cls, values):
        """
        this is run after populating the data
        we can validate (or replace values)
        always must return values
        """
        values["name"] = values.get("name") or values["key"]
        values["id"] = contract_id_mapper(**values)
        return values


class ONEMakeEnterStatus(FlowApiModel):
    one_number: str
    sku: str
    dxa_exited_at: Optional[str]
    customer_order_number: str
    customer_order_date: str
    print_assets_cached_at: Optional[str]
    piece_make_sequences: dict = Field(default_factory=dict)
