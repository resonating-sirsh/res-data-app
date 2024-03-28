import res


def _get_brand_token_by_code(brand_code, postgres=None):
    """ """

    postgres = postgres or res.connectors.load("postgres")

    Q = """SELECT brand_code, api_key FROM sell.brands where brand_code = %s """
    result = postgres.run_query(Q, data=(brand_code,))
    if len(result):
        return dict(result.iloc[0])["api_key"]


def _get_brand_by_token(token, postgres=None):
    """ """

    postgres = postgres or res.connectors.load("postgres")

    Q = """SELECT brand_code, api_key FROM sell.brands where api_key = %s """
    result = postgres.run_query(Q, data=(token,))
    if len(result):
        return dict(result.iloc[0])["brand_code"]


def generate_token(brand_code, save=False):
    """
    generates a token and saves in the database in future (manual for now)
    """
    h = res.utils.res_hash().lower()
    token = {"brand_code": brand_code, "hash": h}
    return f"ONE-{brand_code}-{h}-{res.utils.uuid_str_from_dict(token)}".lower()
