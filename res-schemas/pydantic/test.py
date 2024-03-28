"""
temporary namespace for testing types

- we can reference airtable tables in the same base with id or name for look up fields
- we can cache id one by one or batch; batch is useful for large sets like pieces on rolls. The key-lookup is [our_key:airtable_record_id]
- Types should be singular tense and by convention we pluralize for airtable table

"""

from typing import List, Optional
from schemas.pydantic.common import FlowApiModel, Field


class TestMakeOne(FlowApiModel):
    one_number: str = Field(key_field=True, primary_key=True)
    body_code: str
    color_code: str
    size_code: str
    material_code: str
    sku: str
    rolls: List[str] = Field(default_factory=list)
    order_item_id_source: Optional[str]
    order: Optional[str]


class TestPiece(FlowApiModel):
    Key: str = Field(key_field=True, primary_key=True)
    RollXInches: float
    RollYInches: float
    IsHealing: bool
    OneNumber: str
    PieceName: str


class TestRoll(FlowApiModel):
    key: str = Field(key_field=True, primary_key=True)
    material_code: str
    name: str
    height_inches: float
    width_inches: float
    s3_path: str
    pieces: List[str] = Field(airtable_fk_table="tblf7JWphUqnoJaHW")
    one_numbers: List[str] = Field(airtable_fk_table="viwkkIsNDzL1bFV5k")
    healing_pieces: List[str] = Field(airtable_fk_table="tblf7JWphUqnoJaHW")
