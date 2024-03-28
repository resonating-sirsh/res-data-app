from res.observability.entity import AbstractEntity

import typing


class ResonanceSizesModel(AbstractEntity):
    size_category: str
    size_scale: str
    size_description: str
    size_code: str
    aliases: typing.Optional[typing.List[str]] = []
    airtable_record_id: str


class PieceComponentsModel(AbstractEntity):
    piece_component_code: str
    piece_component_type: str
