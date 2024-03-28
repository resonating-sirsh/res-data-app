"""
Flatteners retrieve flatter data that can be mapped to flatter contract types
"""
import res
import pandas as pd


def load_flattener(key):
    return eval(f"flatten_{key}")


def insert_meta_body_pieces_with_material_one(response, as_dataframe=False):
    return response["insert_meta_body_pieces_with_material_one"]


def flatten_insert_flow_contracts(response, as_dataframe=False):
    # checks

    df = pd.DataFrame(response["insert_flow_contracts"]["returning"])
    if len(df):
        df = res.utils.dataframes.expand_column_drop(df, "node")
        df = res.utils.dataframes.expand_column_drop(df, "user")
        df["owner"] = df["user_email"]
        df["node"] = df["node_name"]

    return df if as_dataframe else df.to_dict("records")


def flatten_insert_make_one_orders_one(response, as_dataframe=False):
    r = response["insert_make_one_orders_one"]
    one_pieces = r["one_pieces"]
    d = {
        "sku": r["sku"],
        "one_number": r["one_number"],
        "pieces": [p["code"] for p in one_pieces],
        "piece_images": [p["piece"]["base_image_uri"] for p in one_pieces],
    }
    # order number now required when make the response
    # you can insert the object without constructing a response object though
    # this is allowed to create a delayed association
    d["order_number"] = r.get("order_number")
    # for now convention is to return a list - easy to change
    return [d]


def update_delayed_pieces(pieceUpdateSetDict):
    from datetime import datetime, timedelta
    import pytz

    now = datetime.now(pytz.utc)
    day_ago = (now - timedelta(days=1)).replace(tzinfo=pytz.utc)
    week_ago = (now - timedelta(days=7)).replace(tzinfo=pytz.utc)

    pieceUpdateSetDict["delayed_printer_1days"] = []
    pieceUpdateSetDict["delayed_printer_7days"] = []
    pieceUpdateSetDict["delayed_rollpacking_1days"] = []
    pieceUpdateSetDict["delayed_rollpacking_7days"] = []
    pieceUpdateSetDict["delayed_rollinpsection_1days"] = []
    pieceUpdateSetDict["delayed_rollinspection_7days"] = []

    for piece in pieceUpdateSetDict["pieces"]:
        if piece.get("observed_at") != None:
            piece["observed_at"] = pd.to_datetime(piece["observed_at"], utc=True)

            if (
                piece.get("node").get("name") == "Make.Print.Printer"
                and piece.get("status") != None
            ):
                if piece["observed_at"] < week_ago:
                    pieceUpdateSetDict["delayed_printer_7days"].append(piece["code"])
                elif piece["observed_at"] < day_ago:
                    pieceUpdateSetDict["delayed_printer_1days"].append(piece["code"])

            if (
                piece.get("node").get("name") == "Make.Print.RollPacking"
                and piece.get("status") != None
            ):
                if piece["observed_at"] < week_ago:
                    pieceUpdateSetDict["delayed_rollpacking_7days"].append(
                        piece["code"]
                    )
                elif piece["observed_at"] < day_ago:
                    pieceUpdateSetDict["delayed_rollpacking_1days"].append(
                        piece["code"]
                    )

            if (
                piece.get("node").get("name") == "Make.Print.RollInspection"
                and piece.get("status") != None
                and piece.get("status") != "Exit"
            ):
                if piece["observed_at"] < week_ago:
                    pieceUpdateSetDict["delayed_rollinspection_7days"].append(
                        piece["code"]
                    )
                elif piece["observed_at"] < day_ago:
                    pieceUpdateSetDict["delayed_rollinspection_1days"].append(
                        piece["code"]
                    )

            piece["observed_at"] = piece["observed_at"].strftime(
                "%Y-%m-%dT%H:%M:%S.%f%z"
            )


def flatten_update_make_one_orders(response, as_dataframe=False):
    returning_list = response["update_make_one_orders"]["returning"]
    if returning_list:
        r = returning_list[0]
        one_pieces = r["one_pieces"]
        d = {
            "sku": r["sku"],
            "one_number": r["one_number"],
            "order_number": r["order_number"],
            "one_code": r["one_code"],
            "pieces": one_pieces,
            # extract the possible nodes that exist under one_pieces
            "node_names": [
                p["node"]["name"] for p in one_pieces if p["node"] is not None
            ]
            # "observed_at": [
            #    p["observed_at"] for p in one_pieces if p["observed_at"] is not None
            # ],
        }

        nodeAlias = {
            "make_inbox": "Make.Inbox",
            "make_print_rollpacking": "Make.Print.RollPacking",
            "make_print_printer": "Make.Print.Printer",
            "make_print_rollinspection": "Make.Print.RollInspection",
            "make_cut": "Make.Cut",
            "make_setbuilding_trims": "Make.SetBuilding.Trims",
            "make_setbuilding": "Make.SetBuilding",
            "make_cut": "Make.Cut.LaserCutter",
        }

        # return the piece code # if its current node matches the node name requested by that property from the alias dict. gives us a list of pieces at a given node
        for respPropName, nodeNameInDb in nodeAlias.items():
            d[respPropName] = [
                p["code"]
                for p in one_pieces
                if p["node"] is not None and p["node"]["name"] == nodeNameInDb
            ]

        update_delayed_pieces(d)

        # reach into which instance this is from one_pieces, save on healing list if we are making more than first instance
        d["healing"] = [
            p["code"]
            for p in one_pieces
            if p["make_instance"] is not None and p["make_instance"] > 1
        ]

        d["pieces"] = [p["code"] for p in one_pieces]

        d["contracts_failed"] = [
            item for p in one_pieces for item in (p["contracts_failed"] or [])
        ]

        d["defects"] = [item for p in one_pieces for item in (p["defects"] or [])]

        return [d]
    # no entries were returned from the query/mutation
    return []


def flatten_insert_make_assemble_rolls(response, as_dataframe=False):
    r = response["insert_make_assemble_rolls"]
    r = r["returning"][0]

    d = {
        "roll_type": r["roll_type"],
        "roll_key": r["roll_key"],
        "material_name_match": r["material_name_match"],
        "roll_id": r["roll_id"],
        "po_number": r["po_number"],
        "roll_length": r["roll_length"],
        "_warehouse_rack_location": r["_warehouse_rack_location"],
        "standard_cost_per_unit": r["standard_cost_per_unit"],
        "roll_primary_key": r["roll_primary_key"],
    }
    # for now convention is to return a list - easy to change
    return [d]


def flatten_insert_flow_queue_bodies(response, as_dataframe=False):
    r = response["insert_flow_queue_bodies"]
    r = r["returning"]
    # TODO validate
    r = r[0]
    r["body_code"] = r["body"]["body_code"]
    r["body_version"] = int(r["body"]["version"])
    r["node"] = r["node"]["name"]
    r["metadata"] = r["body"]["metadata"]
    return [r]


# experimental - temps could be read back but we may not need them
# the metadata
# see the res.flows.sell.orders FulfillmentNode
def flatten_insert_sell_orders_one(result):
    order = result["insert_sell_orders_one"]

    order["order_items"] = result["insert_sell_order_line_items"]["returning"]
    # brand and ecom added

    # get the fulfillmen stuff and use it to process
    for o in order["order_items"]:
        o["order_id"] = order["id"]
        # the product should be resolved for any valid order but we support saving invalid orders for tracking
        product = o.get("product")
        if product:
            o["product_id"] = product["id"]
            o["product_style_id"] = product["style_id"]
    return order
