# utility functions for dealing with print assets.

from res.airtable.print import REQUESTS, ASSETS
from res.utils.dates import utc_now_iso_string
from res.utils import logger


def cancel_print_request(one_number):
    request = REQUESTS.first(formula=f"{{KEY_}}='{one_number}'")
    if request is not None:
        REQUESTS.update(
            request["id"],
            {
                "Cancelled Request": True,
                "Cancelled At": utc_now_iso_string(),
            },
        )
        logger.info(f"Cancelled print request for {one_number} ({request['id']})")
        asset_ids = request["fields"].get("Print File Queue", [])
        assets = ASSETS.all(formula=f"FIND(RECORD_ID(), '{','.join(asset_ids)}')")
        assets_to_cancel = [
            r for r in assets if r["fields"].get("Print Queue") == "TO DO"
        ]
        if len(assets_to_cancel) > 0:
            ASSETS.batch_update(
                [
                    {"id": r["id"], "fields": {"Print Queue": "CANCELED"}}
                    for r in assets_to_cancel
                ]
            )
            logger.info(
                f"Cancelled print assets {[r['fields']['Key'] for r in assets_to_cancel]}"
            )
