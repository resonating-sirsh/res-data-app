import res
from res.flows import FlowContext, flow_node_attributes
import pandas as pd
from res.flows.dxa.styles import helpers
from res.flows.dxa.styles.exceptions import InvalidStyleException

REQUEST_TOPIC = "res_meta.dxa.style_pieces_update_requests"
RESPONSE_TOPIC = "res_meta.dxa.style_pieces_update_responses"


def get_validators():
    from res.flows.meta.pieces import PieceName

    res.utils.logger.debug(f"Loading the piece comp lookup for validation")
    pc = None  # get_piece_components_cache().read()

    def has_missing_body_asset(row):
        return pd.isnull(row["body_asset"]) or (
            pd.isnull(row["body_asset_combo"]) and row["is_combo"]
        )

    def has_mismatched_asset_versions(row):
        return row["body_asset_version"] != row["color_asset_version"]

    def has_duplicated_piece_names(row):
        unique = True
        if isinstance(row["piece_names_combo"], list) and row["is_combo"]:
            unique = len(list(row["piece_names_combo"])) == len(
                set(row["piece_names_combo"])
            )
        if isinstance(row["piece_names"], list):
            return unique & (
                len(list(row["piece_names"])) == len(set(row["piece_names"]))
            )
        return unique

    def has_invalid_piece_names(row):
        if isinstance(row["piece_names"], list):
            for pn in row["piece_names"]:
                if len(PieceName(pn).validate(known_components=pc)):
                    return True
        if isinstance(row["piece_names_combo"], list) and row["is_combo"]:
            for pn in row["piece_names_combo"]:
                if len(PieceName(pn).validate(known_components=pc)):
                    return True
        return False

    def has_invalidated_marker(row):
        return not row["is_marker_valid"] or (
            not row["is_marker_valid_combo"] and row["is_combo"]
        )

    def has_missing_color_files(row):
        if isinstance(row["color_pieces"], list):
            return len(row["color_pieces"]) == 0
        if isinstance(row["color_pieces_combo"], list) and row["is_combo"]:
            return len(row["color_pieces_combo"]) == 0
        return True

    def has_missing_piece_names(row):
        return False

    validators = {
        "has_missing_body_asset": has_missing_body_asset,
        "has_mismatched_asset_versions": has_mismatched_asset_versions,
        "has_duplicated_piece_names": has_duplicated_piece_names,
        # "has_invalid_piece_names": has_invalid_piece_names,
        "has_invalidated_marker": has_invalidated_marker,
        "has_missing_color_files": has_missing_color_files,
        "has_missing_piece_names": has_missing_piece_names,
    }

    return validators


def path_from_sku_and_version(sku, body_version):
    def _part(i):
        return sku.split(" ")[i].lstrip().rstrip()

    body_code, material_code, color_code, size_code = (
        _part(0),
        _part(1),
        _part(2),
        _part(3),
    )
    if "-" not in body_code:
        body_code = f"{body_code[:2]}-{body_code[2:]}"
    return f"s3://res-data-platform/2d-meta-ones/{body_code.replace('-', '_')}/v{body_version}/{material_code}/{color_code}/{size_code}/pieces.feather".lower()


def save_to_database(record, data):
    """
    linking the asset creation to how we save styles in the new database
    depends on bodies having been created and then we link the style pieces

    having updated a body we can link the style
        update_2d_body_from_net_dxf('KT-3038', 4, '1ZZXS')

    this payload is for a specific size so we collapse the sku and add the sizes to the list to make compatible
    we also parse out the other parts of the sju
    the piece mapping is generated from the stuff compute on the 2d meta one as passed in in data
    that does some funky piece matching etc so better to abstract that away there

    another way to save styles to the database is to look up that file or regenerate it and attach piece mappings to a style record
    in future we will generate payloads in other ways

    """
    try:
        from res.flows.dxa.styles.helpers import (
            update_style_sizes_pieces_from_record,
            piece_info_from_2dm1_file,
        )

        record = dict(record)
        record["sizes"] = [record["sku"].split(" ")[-1]]
        # the sku should be the three part one
        record["sku"] = " ".join([s.strip() for s in record["sku"].split(" ")[:3]])
        record["body_code"] = record["sku"].split(" ")[0]
        record["color_code"] = record["sku"].split(" ")[2]
        pi = piece_info_from_2dm1_file(data)
        record["piece_name_mapping"] = pi

        return update_style_sizes_pieces_from_record(record)
    except Exception as ex:
        res.utils.logger.warn(
            f"Failed to update the style in the database {res.utils.ex_repr(ex)} {record}"
        )


# for testing set a sizable bit of memory but we can set this on demand in the metadata per asset...
@flow_node_attributes(memory="50Gi", allow24xlg=True)
def old_handler(event, context={}, plan=False, raise_failure=False):
    """
    run the code that processes the asset and dump it to a feather
    """
    validators = get_validators()

    def try_validate_asset(asset):
        """
        Add validation flags to the metadata on the response
        """
        try:
            for k, v in validators.items():
                asset["metadata"][k] = v(asset)
        except:
            pass
        return asset

    with FlowContext(event, context) as fc:
        kafka = fc.connectors["kafka"]
        s3 = fc.connectors["s3"]

        for asset in fc.assets:
            try:
                path = path_from_sku_and_version(
                    asset["sku"], asset["color_asset_version"]
                )
                asset = try_validate_asset(asset)
                ######
                data = helpers.process_dxa_asset(asset)
                data = pd.DataFrame(data)
                for c in ["outline", "notches"]:
                    data[c] = data[c].map(str)
                # check that we named all the pieces with confidence
                if len(data[data["confidence"] == -1]) > 0:
                    asset["metadata"]["has_missing_piece_names"] = True
                #######
                asset["uri"] = path
                asset["created_at"] = res.utils.dates.utc_now_iso_string(None)
                res.utils.logger.debug(f"Preparing asset {path}")
                if not plan:
                    save_to_database(asset, data)
                    s3.write(path, data)
                    kafka[RESPONSE_TOPIC].publish(asset, use_kgateway=True, coerce=True)
                else:
                    return data
            except Exception as ex:
                res.utils.logger.warn(
                    f"FAILED TO UPDATE {asset['sku'] } {res.utils.ex_repr(ex)}"
                )
                if raise_failure:
                    raise
                # res.utils.logger.debug(asset)

    return {}


def generator(event, context={}):
    with FlowContext(event, context) as fc:

        if len(fc.assets):
            res.utils.logger.info(
                f"Assets supplied by payload - using instead of kafka"
            )
            assets = fc.assets_dataframe
        else:
            res.utils.logger.info(f"Consuming from topic {REQUEST_TOPIC}")
            kafka = fc.connectors["kafka"]
            assets = kafka[REQUEST_TOPIC].consume(give_up_after_records=50)

        if len(assets):
            assets = assets.to_dict("records")

        if len(assets) > 0:
            # compressing the payload - also good to encode so that we dont fuck up the yaml.json
            assets = fc.asset_list_to_payload(assets, compress=True)

        return assets.to_dict("records") if isinstance(assets, pd.DataFrame) else assets


def make_style_from_payload(
    payload,
    force_upsert_style_header=False,
    update_kafka=False,
    body_version=None,
    lookup_body_piece_names=False,
):
    """
    A local testing wrapper as we would post to kafka
    """
    e = res.flows.FlowEventProcessor().make_sample_flow_payload_for_function(
        "dxa.styles.asset_builder.handler"
    )
    e["assets"] = [payload]
    e["args"]["body_version"] = body_version
    return handler(
        e,
        force_upsert_style_header=force_upsert_style_header,
        update_kafka=update_kafka,
        lookup_body_piece_names=lookup_body_piece_names,
    )


# for testing set a sizable bit of memory but we can set this on demand in the metadata per asset...
@flow_node_attributes(memory="50Gi", allow24xlg=True)
def handler(
    event,
    context={},
    force_upsert_style_header=False,
    update_kafka=True,
    lookup_body_piece_names=False,
):
    """

    lookup_body_piece_names:> this is because the ids are messed up on 3d body pieces so we need to resolve the body first

    process handler for receiving any update to [style pieces] and generating the meta one assets in the database for 2d or 2d cases
    payload: res_meta.dxa.update_style_pieces_requests :> caller responsible for exploding the sizes

    ASSET PAYLOAD:
     id: hash of sku+size_code requested
     sku: this is a four part sku BMCS.
     style_sku: this is a three part sku BMC
     size_code: this is a single size code such as 2ZZSM in the accounting format. See below for the comma separated list cheat

     color files can be a directory but the caller must be sure there is no redundant files OR have named them - for example in 3d we provide a path to lookup but they are already named (test this use case
     body assets can also be inlined as body_asset_map: test this use case which just side steps resolving from cache

     NOTE if the user passes a sku in the format BMC the size is treated as a list that could be expanded. For example
      for example:
        sku         = KT-2045 CT406 ABSTSH
        size_code   = ZZZ02,ZZZ22
     This is an "unofficial cheat" to do bundles but formally we expect kafka messages to be requested at style level

    We augment URIs from placed color. In 3d this could be resolved from color files take from the meta one for the size or from the DXA asset in d

    -- add validation on the response : bad piece names, unable to name placements,
    -- specify the class of the 4 classes

    NOTE : This should converge with the meta one request payload actually. We can effectively deprecate the older handler but we can see

    """
    from res.flows.dxa.styles.helpers import (
        add_placed_piece_uris_to_pieces,
        update_style_sizes_pieces_from_record,
    )

    def _missing_uris(x):
        """
        if we already have all the uris we may not want to match them again - leave for now
        """
        for a in x["piece_name_mapping"]:
            if a.get("base_image_uri") == None:
                return True
        return False

    def _as_multi_size(a):
        """
        TODO:
        suppose we have a string or list of sizes
        in stead of size_code=2ZZSM if could be 2ZZSM,1ZZXS
        """
        if "," in a["size_code"]:
            raise Exception("MULTI SIZES NOT YET SUPPORTED")

            def _sized_asset(sa, s):
                s = s.strip()
                d = {k: v for k, v in sa.items() if k not in ["sku", "size_code"]}
                d["sku"] = f"{d['style_sku']} {s}"
                d["size_code"] = s
                return d

            return [_sized_asset(a, s) for s in a["size_code"].split(",")]

        return [a]

    def _add_validations(a):
        """
        0. Ensure the SKU is a 4 part SKU: this can happen either because we passed in SKU properly with one size or the cheat with a three part and a size list
        1. when we saved the style there was no body - probably because there is nothing for that size
        2. when we tried to save the pieces there was no complete match with the body pieces
        3. the piece names are not conforming to piece names leading to 1 or 2
        4. placement style for which we could not name all the pieces
        6. DB other such as missing a style header or something silly like that
        7. A null piece name in the style i.e. not resolved properly to a proper piece
        """
        a["validation_flags"] = []
        return a

    def _augment_pieces(asset, require_names=True):
        """
        add placed color uris to named pieces if we can
        """
        inferred_body_code = asset["sku"].split(" ")[0].strip()
        color_files = asset.pop("color_pieces")
        offset_size_inches = asset.get("offset_size_inches")
        offset_size_inches_combo = asset.get("offset_size_inches_combo")
        asset = add_placed_piece_uris_to_pieces(
            asset,
            color_file_names=color_files,
            offset_size_inches=offset_size_inches,
            offset_size_inches_combo=offset_size_inches_combo,
        )
        return asset

    with FlowContext(event, context) as fc:
        kafka = fc.connectors["kafka"]
        body_version = fc.args.get("body_version", None)
        for asset in fc.decompressed_assets:  # we packed them in the generator
            # with fc.asset_failure_capture(asset):
            for asset in _as_multi_size(asset):
                try:
                    if asset.get("print_type") == "Placement" and _missing_uris(asset):
                        res.utils.logger.debug(f"adding a placement naming")
                        asset = _augment_pieces(asset)
                        # if the pieces are not all named then we fail
                    # allow a cheat where we explode a list of sizes in special cases -> we generate a new asset with a sized sku and size_code for each of the string separated list of sizes
                    asset = _add_validations(asset)
                    # generally we do not force style headers in a batch mode or background mode but for some paths we can check
                    _ = update_style_sizes_pieces_from_record(
                        asset,
                        force_upsert_style_header=force_upsert_style_header,
                        body_version=body_version,
                        lookup_body_piece_names=lookup_body_piece_names,
                    )
                except InvalidStyleException as sx:
                    asset["validation_flags"] = asset.get("validation_flags", [])
                    asset["validation_flags"].append(sx.flag)
                    # when we write the response which will go to snowflake - we must be able to use this to know our full status on styles that can be used in production as 2dm1s or 3dm1s
                if update_kafka:
                    kafka[RESPONSE_TOPIC].publish(asset, use_kgateway=True, coerce=True)

    return {}


def reducer(event, context={}):
    return {}


# in the reducer scrape DXA since some date for which there are no meta ones and retry
