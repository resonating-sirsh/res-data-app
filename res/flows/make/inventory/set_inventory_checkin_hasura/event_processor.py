import res
from schemas.pydantic.make import *
from res.flows.api import FlowAPI
from ast import literal_eval
from res.flows.make.inventory.process_one_in_warehouse.process_one import handler


def handle_event(event, **kwargs):
    API = FlowAPI(OneInInventory)
    try:
        res.utils.logger.info("handle...")
        barcode_scanned = literal_eval(event.get("bar_code_scanned", ""))
        inventory_payload = {
            "id": res.utils.uuid_str_from_dict({"id": event.get("one_number")})
            if event.get("one_number")
            else None,
            "one_number": event.get("one_number", ""),
            "reservation_status": event.get("reservation_status", ""),
            "bin_location": event.get("bin_location", ""),
            "sku_bar_code_scanned": barcode_scanned["text"] if barcode_scanned else "",
            "warehouse_checkin_location": event.get("warehouse_checkin_location", ""),
            "operator_name": event.get("operator_name", ""),
            "work_station": event.get("work_station", ""),
            "one_order_id": None,
            "checkin_type": event.get("checkin_type", ""),
            "metadata": event.get("metadata", {}),
        }
        inventory = OneInInventory(**inventory_payload)
        res.utils.logger.info({"toUpdateInventoryPayload...": inventory})
        response = API.update_hasura(inventory)
        res.utils.logger.info({"Process record": response})
        handler(response)
    except Exception as ex:
        res.utils.logger.warn(f"Failing {ex}")
