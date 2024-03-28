from tqdm import tqdm
from res.media.images.outlines import get_piece_outline
from res.media.images.geometry import to_geojson
import res
from res.flows.meta.ONE.queries import save_piece_image_outline, get_latest_style_pieces
import pandas as pd


def generator(event, context={}):
    """
    Pass in either a body or a list of skus in the apply color queue
    TODO - for now just do sku set mode and we can figure out the list on client
    handler will process latest pieces for each
    """
    with res.flows.FlowContext(event, context) as fc:
        if len(fc.assets):
            res.utils.logger.info(
                f"Assets supplied by payload - using instead of kafka"
            )
            assets = fc.assets_dataframe

        # if we implement a job queue
        # else:
        #     res.utils.logger.info(f"Consuming from topic {REQUEST_TOPIC}")
        #     kafka = fc.connectors["kafka"]
        #     assets = kafka[REQUEST_TOPIC].consume()

        if len(assets):
            assets = assets.to_dict("records")

        if len(assets) > 0:
            assets = fc.asset_list_to_payload(assets)

        res.utils.logger.info(f"returning the work from the generator - {assets} ")
        return assets.to_dict("records") if isinstance(assets, pd.DataFrame) else assets


@res.flows.flow_node_attributes(memory="50Gi", disk="5G")
def handler(event, context={}):
    s3 = res.connectors.load("s3")
    with res.flows.FlowContext(event, context) as fc:
        for payload in fc.decompressed_assets:
            style_code = payload["style_code"]
            res.utils.logger.info(f"Style {style_code}")
            for record in tqdm(get_latest_style_pieces(style_code).to_dict("records")):
                im = s3.read(record["base_image_uri"])
                outline = get_piece_outline(im)
                id = record["id"]
                id = save_piece_image_outline(
                    id=id, base_image_outline=to_geojson(outline)
                )
                res.utils.logger.debug(
                    f"Updated piece outline for image {record['base_image_uri']}"
                )

    return {}


def reducer(event, context={}):
    return {}
