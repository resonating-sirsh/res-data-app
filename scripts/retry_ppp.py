import res
import json
import time
import math
from res.utils import secrets
from pyairtable import Table

# The intent of this script is to resubmit PPP requests in the case where somethings got hosed
# which typically means we failed to write the result into airtable..

assets_to_ppp = Table(
    secrets.secrets_client.get_secret("AIRTABLE_API_KEY"),
    "apprcULXTWu33KFsh",
    "tblwDQtDckvHKXO4w",
).all(
    formula="""
        AND(
            {Prepared Pieces Count}='',
            {Print Queue}='TO DO',
            {Rolls Assignment v3}='',
            {Healing Piece S3 Piece URI}='',
            OR({S3 File URI}!='', {Intelligent ONE Marker File ID}!='')
        )
    """,
    fields=["Key"],
)

record_ids = [r["id"] for r in assets_to_ppp]
print(len(record_ids), "assets needing PPP")

assets = res.connectors.load("snowflake").execute(
    f"""
select RECORD_CONTENT from IAMCURIOUS_PRODUCTION.PREP_PIECES_REQUESTS_QUEUE where "id" in ('{"','".join(record_ids)}')
"""
)

assets = [json.loads(p) for p in assets.RECORD_CONTENT.values]
print(len(assets), "PPP payloads")

for i in range(math.ceil(len(assets) / 10)):
    job_key = f"prep-pieces-retry-rob-{i}-{int(time.time())}"

    payload = {
        "apiVersion": "resmagic.io/v1",
        "args": {},
        "assets": assets[(i * 10) : min(10 * (i + 1), len(assets))],
        "kind": "resFlow",
        "metadata": {
            "name": "dxa.prep_ordered_pieces",
            "version": "primary",
        },
        "task": {"key": job_key},
    }

    res.connectors.load("argo").handle_event(
        payload,
        context={},
        template="res-flow-node",
        unique_job_name=job_key,
    )
