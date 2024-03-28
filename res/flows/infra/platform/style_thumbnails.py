import res
import pandas as pd
from res.flows.meta.pieces import PieceName
from res.media.images import make_square_thumbnail


def generator(event, context={}):
    with res.flows.FlowContext(event, context) as fc:
        assets = {"body_code" for b in fc.assets}
    return fc.asset_list_to_payload(assets)


def _label_paths(dd):
    ddf = dd[dd[0].map(lambda x: "/pieces/" in x)].reset_index()
    ddf["uri"] = ddf[0]
    ddf["piece"] = ddf[0].map(lambda x: x.split("/")[-1])
    ddf["v"] = ddf[0].map(lambda x: x.split("/")[-5].replace("v", "")).map(int)
    ddf["color"] = ddf[0].map(lambda x: x.split("/")[-4])
    ddf["body"] = ddf[0].map(lambda x: x.split("/")[-6])
    ddf["size"] = ddf[0].map(lambda x: x.split("/")[-3])
    ddf = (
        ddf.sort_values("v")
        .drop_duplicates(subset=["body", "color", "size"])
        .reset_index()
    )
    ddf["rank"] = ddf.groupby(["body", "color"])["index"].transform("rank", pct=False)

    ddf["piece_key"] = ddf["uri"].map(lambda x: PieceName(x.split("/")[-1])._key)
    ddf["piece_name"] = ddf["uri"].map(lambda x: PieceName(x.split("/")[-1]).name)
    ddf["piece_component"] = ddf["uri"].map(
        lambda x: PieceName(x.split("/")[-1]).component
    )
    ddf["piece_part"] = ddf["uri"].map(lambda x: PieceName(x.split("/")[-1]).part)
    ddf["piece_type"] = ddf["uri"].map(
        lambda x: PieceName(x.split("/")[-1]).product_taxonomy
    )
    ddf["target_part_comp"] = ddf.apply(
        lambda f: f"{f['piece_part']}{f['piece_component']}", axis=1
    )

    def thumb_path(row):
        return f"s3://res-data-platform/images/pieces/part-thumbnails/v1/{row['body']}/{row['v']}/{row['color']}/{row['size']}/{row['piece_name']}"

    # s3://res-data-platform/images/pieces/part-thumbnails/kt_2011/9/duskgd/PNTBKPKTLF-S.png
    # paths like these are body color paths showing exactly the piece in that color - we keep the version to study drift
    ddf["path"] = ddf.apply(thumb_path, axis=1)
    ddf = ddf.drop_duplicates(subset=["path"])
    return ddf


@res.flows.flow_node_attributes(
    memory="16Gi",
)
def handler(event, context={}):
    s3 = res.connectors.load("s3")
    with res.flows.FlowContext(event, context) as fc:
        for b in fc.assets:
            body_code = b.lower().replace("-", "_")
            res.utils.logger.info(
                f"Loading source images for the body for all colors for the last [TIME PERIOD]"
            )
            dd = pd.DataFrame(
                s3.ls(
                    f"s3://meta-one-assets-prod/color_on_shape/{body_code}",
                    modified_after=res.utils.dates.relative_to_now(180),
                )
            )
            dd = _label_paths(dd)

            res.utils.logger.info(f"Will generate thumbnails for all the images")
            for record in dd.to_dict("records"):
                uri = record["uri"]
                out_uri = record["path"]

                im = make_square_thumbnail(uri)
                res.utils.logger.info(out_uri)
                s3.write(out_uri, im)
    return {}


def reducer(event, context={}):
    return {}
