import res
import json
import time
from res.airtable.print import ASSETS
from res.utils import secrets, logger
from res.flows import flow_node_attributes, FlowContext


def retry_assets(assets, flow):
    job_key = f"pack-one-retry-{int(time.time())}"

    res.connectors.load("argo").handle_event(
        {
            "apiVersion": "resmagic.io/v1",
            "args": {},
            "assets": assets,
            "kind": "resFlow",
            "metadata": {
                "name": flow,
                "version": "primary",
            },
            "task": {"key": job_key},
        },
        context={},
        template="res-flow-node",
        unique_job_name=job_key,
        compressed=True,
    )


@flow_node_attributes(
    "retry_pack_one.handler",
    mapped=False,
)
def handler(event, context):
    with FlowContext(event, context) as fc:
        assets_to_nest = ASSETS.all(
            formula="""
                AND(
                    {Nesting ONE Ready}=1,
                    {Has Nests}=0,
                    {Print Queue}='TO DO',
                    {Prepared Pieces Count}!='',
                    {Rank}!='Healing'
                )
            """,
            fields=["Prepared Pieces Key", "Key"],
        )

        s3 = res.connectors.load("s3")
        snow = res.connectors.load("snowflake")
        for r in assets_to_nest:
            if not s3.exists(
                f"s3://res-data-production/flows/v1/make-nest-progressive-pack_one/primary/nest/{r['id']}/output.feather"
            ):
                logger.info(f"Retrying asset {r}")
                try:
                    ppp_response = snow.execute(
                        f"""select RECORD_CONTENT from IAMCURIOUS_PRODUCTION.PREP_PIECES_RESPONSES_QUEUE where "id" = '{r['id']}' order by "created_at" desc limit 1"""
                    )
                    if ppp_response.shape[0] > 0:
                        logger.info(f"Retrying pack-one")
                        retry_assets(
                            [json.loads(ppp_response.at[0, "RECORD_CONTENT"])],
                            "make.nest.progressive.pack_one",
                        )
                        continue
                    ppp_request = snow.execute(
                        f"""select RECORD_CONTENT from IAMCURIOUS_PRODUCTION.PREP_PIECES_REQUESTS_QUEUE where "id" = '{r['id']}' order by "created_at" desc limit 1"""
                    )
                    if ppp_request.shape[0] > 0:
                        logger.info(f"Retrying ppp")
                        retry_assets(
                            [json.loads(ppp_request.at[0, "RECORD_CONTENT"])],
                            "dxa.prep_ordered_pieces",
                        )
                        continue
                except Exception as e:
                    logger.info(f"failed to retry one {r['id']}")
