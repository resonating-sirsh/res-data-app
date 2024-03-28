import res
from res.connectors.airtable.AirtableConnector import Airtable

CUT_APP_ID = "appyIrUOJf8KiXD1D"


def get_cut_one_request_table() -> Airtable:
    airtable = res.connectors.load("airtable")
    return airtable[CUT_APP_ID]["tblwIFbHo4PsZbDgz"]


def get_cut_pieces_request_table() -> Airtable:
    airtable = res.connectors.load("airtable")
    return airtable[CUT_APP_ID]["tblAqWxhkV30UFTUW"]


def get_cut_request_for_one(production_request, factor_order_pdf):
    """
    Fetching and merge existing or create new record for cut app - assert 1 or 0 since duplicates are not allowed

    The header information that we add to cut is a copy of records that we have in make one production. Airtable.

    """
    table = get_cut_one_request_table()
    one_number = production_request["one_number"]

    res.utils.logger.debug(f"check cut assets for {one_number}")
    data = table.to_dataframe(filters=f"{{ONE Code}}='{one_number}'")
    res.utils.logger.debug(f"found {len(data)} cut assets for {one_number}")

    sku = production_request["sku"]
    sku_body, sku_material, sku_color, sku_size = sku.split(" ")
    assert (
        len(data) <= 1
    ), f"There is more the one request for cut - one number {one_number}, ids {list(data['record_id'])}"

    record_id = None
    if len(data):
        record_id = data.iloc[0]["record_id"]
        res.utils.logger.debug(dict(data.iloc[0]))

    return {
        "record_id": record_id,
        # "ONE Code": one_number,
        "__onecode": one_number,
        "make_one_production_request_id": production_request["airtable_rid"],
        "Factory Order PDF": factor_order_pdf,
        "Request Name": production_request["order_number"],
        "Brand Code": production_request["brand_code"],
        "SKU": sku,
        "Body Code": sku_body,
        "Material Code": sku_material,
        "Color Code": sku_color,
        # not sure if ths is the val we want
        "Original ONE Placed At": production_request["ordered_at"]
        .date()
        .strftime("%Y-%m-%d"),
    }


def get_cut_pieces_for_one(production_request, current_pieces, cut_one_record_id=None):
    """
    get any existing pieces that we have and merge the latest data - its generally assumed when this is called that there are none existing

    the pieces that we pull from the meta one is always truth. the question just if we need to resolve existing ids

    **Args**
        current_pieces: these are the piece dictionaries raw from the meta one
        cut_one_record_id: after creating the cut record this is the key link in airtable to that header linked to all pieces
                           if its not passed in we resolve it from the one number

    TODO: if there is a piece that was on the meta one but is not now we could delete the piece from the cut app
    """

    # look up the header row if we need to
    if not cut_one_record_id:
        res.utils.logger.debug(f"No cut record passed in so resolving it for the one")
        table = get_cut_one_request_table()
        one_number = production_request["one_number"]
        data = table.to_dataframe(filters=f"{{ONE Code}}='{one_number}'")
        if len(data):
            cut_one_record_id = data.iloc[0]["record_id"]

    # now that we know
    res.utils.logger.debug(
        f"Checking for any existing pieces for cut record {cut_one_record_id}"
    )
    one_number = production_request["one_number"]
    table = get_cut_pieces_request_table()
    data = table.to_dataframe(filters=f"{{ONE Number}}='{one_number}'")
    res.utils.logger.debug(f"Found {len(data)}")

    def resolve_record_key(row):
        """
        if we already saved these pieces, we can lookup their record ids for the upsert - normally we are adding them new
        """
        d = {
            f"{one_number}_{item['Piece Number']}": item["record_id"]
            for item in data.to_dict("records")
        }
        # if data ... we can map a key with a row.one, row.piece to a record id
        return d.get(f"{one_number}_{row['Piece Number']}")

    # update the references of ids for pieces which we get from the meta one
    for piece in current_pieces:
        # we need to check if we saved ONE_NUMBER/Piece_number before and set the record id if so
        piece["record_id"] = resolve_record_key(piece)
        # For Geovanny question... - i think this links to the ONE lookup??
        piece["ONE"] = [cut_one_record_id]

    return current_pieces
