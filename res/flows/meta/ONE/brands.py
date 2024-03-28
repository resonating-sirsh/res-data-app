import res


def _populate_brands():
    redis = res.connectors.load("redis")
    cache = redis["LOOKUPS"]["META.BRANDS"]
    airtable = res.connectors.load("airtable")
    res.utils.logger.info("refreshing brand lookup based on missing brands observed")
    data = airtable.get_table_data("appa7Sw0ML47cA8D1/tblqPtMB7IxGmzM5H")
    lu = dict(data[["Code", "Name"]].values)

    for k, v in lu.items():
        cache[k] = v

    return lu


def get_brand(code):
    redis = res.connectors.load("redis")
    cache = redis["LOOKUPS"]["META.BRANDS"]

    value = cache[code]

    if not value:
        lu = _populate_brands()
        return lu.get(code, "Unknown")

    return value
