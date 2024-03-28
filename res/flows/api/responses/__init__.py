from schemas.pydantic.common import FlowApiModel
from schemas.pydantic.meta import (
    BodyMetaOneResponse,
    MetaOneResponse,
    BodyPieceMaterialResponse,
)


class FlowApiResponse(FlowApiModel):
    pass

    def flatten(
        cls,
        expand_excludes=["metadata"],
        aliases={},
        rename_and_whitelist={},
        operators={},
    ):
        """
        Go through types - if list then explode the dataframe and expand its fields
        For maps, expand unless excluded
        Finally rename and white list and apply operators such as converting geometries
        """
        return None


# for now a lame way to link the objects to kafka messages just like we link things to mutations in this api
KAFKA_RESPONSE_TOPICS = {
    BodyMetaOneResponse: "res_meta.dxa.body_update_responses",
    MetaOneResponse: "res_meta.dxa.style_pieces_update_responses",
    BodyPieceMaterialResponse: "res_meta.dxa.body_piece_material_updates",
    # ADD RFID response type to kafka message mapping here
}
