from pydantic import typing, BaseModel
from res.connectors.neo4j.Neo4jConnector import NeoEdge, NeoNode
import res
import json


class EntityType(NeoNode):
    description: str


class FunctionType(NeoNode):
    description: str


class KnowledgeType(NeoNode):
    description: str


class Function(NeoNode):
    description: str


class HasFunctionType(NeoEdge):
    pass


class HasKnowledgeType(NeoEdge):
    pass


class HasEntityRelation(NeoEdge):
    """
    primary entity
    downstream of
    consists of
    """

    description: str
