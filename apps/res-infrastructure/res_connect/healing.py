### TODO: move this out of res-connect into its own app.
import res
import time
import json
import os
import pandas as pd
import matplotlib
from collections import OrderedDict
from functools import lru_cache
from matplotlib import pyplot as plt
from shapely.geometry import box
from shapely.wkt import loads
from shapely.affinity import translate, scale
from res.airtable.healing import (
    REQUEST_PIECES,
    ZONE_X_DEFECT,
    HEALING_ROLLS,
)
from res.airtable.print import NESTS, PRINTFILES, ROLLS
from res.flows.make.nest.progressive.utils import ping_rob
from res.flows.make.healing.healing_app_nests import (
    handler as sync_nests_to_healing_handler,
)
from res.utils import res_hash, ping_slack
from res.utils.dates import utc_now_iso_string
from res.utils.logging import logger
from res.utils.env import RES_DATA_BUCKET
from res.learn.optimization.packing.annealed.progressive import (
    ProgressiveNode,
)
from res.flows.make.production.queries import get_one_piece_healing_instance_resolver

matplotlib.pyplot.switch_backend("Agg")

PIECE_OBSERVATION_TOPIC = "res_make.piece_tracking.make_piece_observation_request"

CACHE_TIMEOUT_S = 15 * 60


def cache_timestamp():
    return int(time.time() / CACHE_TIMEOUT_S)


@lru_cache(maxsize=2)
# kind of jury rigged ttl
def _get_healing_info(timestamp):
    logger.info("Loading defect data from airtable")
    recs = ZONE_X_DEFECT.all(
        fields=[
            "Defect Name",
            "Zone",
            "Image 1",
            "Image 2",
            "Image 3",
            "Image 6",
            "PASS Images",
        ],
        formula="""
        AND(
            {Defect Name} != '',
            {Zone} != '',
            {PASS Images} != '',
            {Image 1} != '',
            {Image 2} != '',
            {Image 3} != ''
        )
        """,
    )
    logger.info(f"Loaded {len(recs)} defect records")
    return {
        r["id"]: {
            "zone": r["fields"]["Zone"],
            "defect": r["fields"]["Defect Name"],
            "image1": {
                "url": r["fields"]["Image 1"][0]["thumbnails"]["large"]["url"],
                "pass": "1" in r["fields"]["PASS Images"],
            }
            if "Image 1" in r["fields"]
            else None,
            "image2": {
                "url": r["fields"]["Image 2"][0]["thumbnails"]["large"]["url"],
                "pass": "2" in r["fields"]["PASS Images"],
            }
            if "Image 2" in r["fields"]
            else None,
            "image3": {
                "url": r["fields"]["Image 3"][0]["thumbnails"]["large"]["url"],
                "pass": "3" in r["fields"]["PASS Images"],
            }
            if "Image 3" in r["fields"]
            else None,
            # special "repair" image for inspector 1.
            "image4": {
                "url": r["fields"]["Image 6"][0]["thumbnails"]["large"]["url"],
                "pass": True,
            }
            if "Image 6" in r["fields"]
            else None,
        }
        for r in recs
    }


@lru_cache(maxsize=2)
def inspectable_roll_info(inspector_id, timestamp):
    rolls_nests = NESTS.all(
        formula=f"""AND({{Roll Name}}!='', {{Current Roll Substaiton}}='Roll Inspection {inspector_id}')""",
        fields=[
            "Roll Name",
            "Rank",
            "Argo Jobkey",
            "Dataframe Path",
            "Nested Length in Yards",
            "Current Roll Substaiton",
            "Roll Assigned At",
        ],
        sort=["Current Roll Substaiton", "Roll Name"],
    )
    rolls_info = OrderedDict()
    for r in rolls_nests:
        if r["fields"]["Roll Name"] not in rolls_info:
            rolls_info[r["fields"]["Roll Name"]] = {
                # sa -try for missing substation - shrug
                "substation": r["fields"].get("Current Roll Substaiton", ["NONE"])[0],
                "nested_length": r["fields"]["Nested Length in Yards"],
                "nests": 1,
                "assigned_at": r["fields"]["Roll Assigned At"],
                "nest_keys": [r["fields"]["Argo Jobkey"]],
                "roll_key": r["fields"]["Roll Name"]
                .replace("_0 ", " ")
                .replace(": ", "-"),
            }
        else:
            rolls_info[r["fields"]["Roll Name"]]["nests"] += 1
            rolls_info[r["fields"]["Roll Name"]]["nest_keys"].append(
                r["fields"]["Argo Jobkey"]
            )
            rolls_info[r["fields"]["Roll Name"]]["nested_length"] += r["fields"][
                "Nested Length in Yards"
            ]
    return rolls_info


def get_roll_info(roll_key, inspector_id):
    logger.info(f"Loading roll info {roll_key}")
    s3 = res.connectors.load("s3")
    nest_info = [
        {
            "rank": r["fields"]["Rank"],
            "key": r["fields"]["Argo Jobkey"],
            "url": s3.generate_presigned_url(
                f'{r["fields"]["Asset Path"]}/roll_inspection.png'
            ),
        }
        for r in NESTS.all(
            formula=f"AND({{Rank}}!='', FIND('{roll_key}', {{Argo Jobkey}}), {{Rolls}}!='')",
            fields=["Asset Path", "Argo Jobkey", "Rank"],
            sort=["Rank"],
        )
    ]
    started_field = f"Roll Inspection {inspector_id} Start"
    finished_field = f"Roll Inspection {inspector_id} Finish"
    healing_roll = HEALING_ROLLS.all(
        formula=f"{{Name}}='{roll_key.replace('-', ': ')}'",
        fields=[started_field, finished_field],
    )
    if len(healing_roll) == 0:
        raise ValueError(
            f"Roll {roll_key.replace('-', ': ')} not generated in healing app"
        )
    return {
        "roll_key": roll_key,
        "nest_info": nest_info[::-1] if inspector_id == "1" else nest_info,
        "scale_y": -1 if inspector_id == "1" else 1,
        "inspector_id": inspector_id,
        "roll_id": healing_roll[0]["id"],
        "started": healing_roll[0]["fields"].get(started_field, False),
        "finished": healing_roll[0]["fields"].get(finished_field, False),
    }


@lru_cache(maxsize=300)
def get_static_nest_info(nest_key, timestamp):
    logger.info(f"Loading nest info {nest_key}")
    nest_info = NESTS.all(
        formula=f"{{Argo Jobkey}}='{nest_key}'", fields=["Asset Path", "Dataframe Path"]
    )
    if len(nest_info) != 1:
        raise Exception(f"Failed to find nest {nest_key} in airtable")
    s3 = res.connectors.load("s3")
    nest_df_path = nest_info[0]["fields"]["Dataframe Path"]
    logger.debug(f"Loading nest df {nest_df_path}")
    nest_composited_path = f'{nest_info[0]["fields"]["Asset Path"]}/roll_inspection.png'
    nest_df = s3.read(nest_df_path)
    nest_height = nest_df.composition_y.max() + 10
    nest_width = nest_df.output_bounds_width.iloc[0]
    logger.debug(f"Nest dimensions {nest_height}, {nest_width}")
    if nest_height > nest_width:
        img_height = 4000
        img_width = int(img_height * nest_width / nest_height)
    else:
        img_width = 4000
        img_height = int(img_width * nest_height / nest_width)
    img_href = s3.generate_presigned_url(nest_composited_path)
    scale_y = img_height / nest_height
    scale_x = img_width / nest_width
    logger.debug(f"Assumed image dimensions {img_height}, {img_width}")
    logger.debug(f"Scale {scale_x} {scale_y}")
    nest_df["asset_key"] = nest_df["asset_key"].apply(
        lambda s: "" if s is None else s.split("_")[0]
    )  # healing pieces get a name like 123455_piece_2
    nest_df["nested_geometry"] = nest_df["nested_geometry"].apply(loads)
    nest_df["simplified_geom"] = nest_df.nested_geometry
    nest_df["y_coord"] = (nest_df.max_nested_y + nest_df.min_nested_y) / 2
    nest_df["simplified_geom"] = nest_df.apply(
        lambda r: translate(r.simplified_geom, 0, nest_height - 2 * r.y_coord),
        axis=1,
    )
    nest_df["simplified_geom"] = nest_df["simplified_geom"].apply(
        lambda p: scale(p, 1, -1)
    )

    # go to the healing app for manually tagged zone info.
    # for now just making use of the exiting print-healing linkage which we already did.
    healing_app_info = {
        (r["fields"]["Print Asset ID (Print App)"], r["fields"]["Piece Name"]): (
            r["id"],
            r["fields"].get("final_commercial_acceptability_zone", ""),
        )
        for r in REQUEST_PIECES.all(
            formula=f"{{Nest Job Key}}='{nest_key}'",
            fields=[
                "Piece Name",
                "Print Asset ID (Print App)",
                "final_commercial_acceptability_zone",
            ],
        )
    }

    logger.debug(
        f"Loaded {len(healing_app_info)} records from healing app request pieces"
    )

    nest_df["healing_app_record_id"] = nest_df.apply(
        lambda r: healing_app_info.get((r.asset_id, r.piece_name), ("", ""))[0],
        axis=1,
    )
    nest_df["healing_app_zone"] = nest_df.apply(
        lambda r: healing_app_info.get((r.asset_id, r.piece_name), ("", ""))[1],
        axis=1,
    )

    nest_df["zone"] = nest_df.zone.where(
        nest_df.zone.apply(lambda z: z != None and z != ""),
        nest_df.healing_app_zone,
        axis=0,
    )

    def _geom_to_coords(g):
        try:
            coords = g.boundary.coords
        except:
            coords = box(*g.bounds).boundary.coords
        return ",".join(
            [f"{int(x*scale_x)},{max(0, int(y*scale_y))}" for x, y in coords][:-1]
        )

    map_pieces = [
        f"""
        <area
            href='#'
            piece_id='{r.piece_id}'
            shape='poly'
            coords='{_geom_to_coords(r.simplified_geom)}'
        />
        """
        for _, r in nest_df.iterrows()
    ]

    piece_info = ",".join(
        f"""'{r.piece_id}': {r[["asset_key", "asset_id", "piece_name", "piece_code", "piece_description", "zone", "min_nested_x", "min_nested_y", "healing_app_record_id"]].to_json()}"""
        for _, r in nest_df.iterrows()
    )

    # info to draw on better markers over the image.
    num_markers = int(nest_df.max_nested_y.max()) // 3000
    marker_percent = 300000.0 / int(nest_df.max_nested_y.max())

    return {
        "nest_key": nest_key,
        "nest_img_path": img_href,
        "piece_info": piece_info,
        "nest_map": "\n".join(map_pieces),
        "defect_info": json.dumps(_get_healing_info(timestamp)),
        "num_markers": num_markers,
        "marker_pct": marker_percent,
    }


def get_nest_info(nest_key, inspector_id, timestamp):
    existing_inspection = res.connectors.load("hasura").execute(
        """
        query ($inspector: Int, $nest_key: String) {
            make_roll_inspection(where: {inspector: {_eq: $inspector}, nest_key: {_eq: $nest_key}}, distinct_on: piece_id) {
                piece_id
            }
        }
        """,
        {"inspector": inspector_id, "nest_key": nest_key},
    )

    return {
        **get_static_nest_info(nest_key, timestamp),
        "inspector_id": inspector_id,
        "scale_y": -1 if inspector_id == "1" else 1,
        "tagged_piece_ids": ",".join(
            r["piece_id"] for r in existing_inspection["make_roll_inspection"]
        ),
    }


def _map_piece_ids_to_healing_app_record_ids(roll_key):
    # load all teh nests on the roll
    nest_info = NESTS.all(
        formula=f"AND({{Roll Name}}!='', FIND('{roll_key}', {{Argo Jobkey}}))",
        fields=["Dataframe Path", "Argo Jobkey"],
    )
    logger.info(f"Syncing to nests {nest_info}")
    s3 = res.connectors.load("s3")
    combined_df = pd.concat([s3.read(r["fields"]["Dataframe Path"]) for r in nest_info])
    logger.info(f"Build combined df for {roll_key} with shape {combined_df.shape}")
    logger.info(
        f"Going to healing app for {roll_key} for ids {combined_df.asset_id.unique()}"
    )
    healing_app_info = {
        (
            r["fields"]["Print Asset ID (Print App)"],
            r["fields"]["Piece Name"],
        ): r["id"]
        for r in REQUEST_PIECES.all(
            formula="OR("
            + ",".join(
                [
                    f"{{Print Asset ID (Print App)}}='{aid}'"
                    for aid in combined_df.asset_id.unique()
                    if aid is not None  # wtf
                ]
            )
            + ")",
            fields=["Piece Name", "Print Asset ID (Print App)"],
        )
    }
    logger.info(f"Got healing app info for {roll_key}")
    if "piece_name" not in combined_df.columns:
        combined_df["piece_name"] = None
    combined_df["piece_name"] = combined_df.piece_name.where(
        combined_df.piece_name.notnull(),
        combined_df.s3_image_path.apply(lambda p: p.split("/")[-1].replace(".png", "")),
        axis=0,
    )
    combined_df["one_number"] = combined_df["asset_key"].apply(
        lambda s: None if s is None else str(s).split("_")[0]
    )
    combined_df["healing_app_record_id"] = combined_df.apply(
        lambda r: healing_app_info.get((r.asset_id, r.piece_name), ""),
        axis=1,
    )
    piece_id_to_airtable_id_map = combined_df.set_index("piece_id").to_dict()[
        "healing_app_record_id"
    ]
    # handle block buffers.
    # for now these pieces wont possibly have defects against them.
    # we can always change the interface.
    for _, r in combined_df.iterrows():
        if r["wrapped_piece_codes"] is not None:
            piece_names = r["wrapped_piece_codes"].split(",")
            piece_ids = r["wrapped_piece_ids"].split(",")
            for pn, pid in zip(piece_names, piece_ids):
                if (r["asset_id"], pn) in healing_app_info:
                    piece_id_to_airtable_id_map[pid] = healing_app_info.get(
                        (r["asset_id"], pn)
                    )
    piece_id_to_airtable_id_map = {
        k: v for k, v in piece_id_to_airtable_id_map.items() if v != ""
    }
    unknown_pieces = combined_df[
        (combined_df["healing_app_record_id"] == "")
        & pd.notna(combined_df["asset_id"])
        & (combined_df["piece_name"].apply(lambda n: not n.startswith("block_")))
    ]
    logger.info(
        f"Pieces for {roll_key}: {len(piece_id_to_airtable_id_map)}, unknown {unknown_pieces.shape[0]}"
    )
    return combined_df, piece_id_to_airtable_id_map, unknown_pieces.shape[0]


def sync_roll_to_healing_app(roll_key, inspector_id):
    try:
        logger.info(
            f"Syncing roll {roll_key} to healing app for inspector {inspector_id}"
        )
        lock = res.connectors.load("redis").try_get_lock(
            f"LOCK:HEALING_SYNC:{roll_key}:{inspector_id}", timeout=30 * 60
        )
        if not lock or not lock.owned():
            logger.info(f"Failed to get lock {roll_key}:{inspector_id}")
            return
        logger.info(
            f"Aquired lock to sync roll {roll_key}, to healing app for inspector {inspector_id}"
        )
        ping_slack(
            f"[HEALING] Aquired lock to sync roll {roll_key}, to healing app for inspector {inspector_id}.",
            "autobots",
        )
        if int(inspector_id) != 1 and int(inspector_id) != 2:
            raise Exception(f"Invalid inspector {inspector_id}")

        (
            combined_df,
            piece_id_to_airtable_id_map,
            unknown_piece_count,
        ) = _map_piece_ids_to_healing_app_record_ids(roll_key)
        if unknown_piece_count > 0:
            logger.info(f"Re syncing to healing app {roll_key}")
            sync_nests_to_healing_handler({"roll": roll_key.replace("-", ": ")}, {})
            (
                combined_df,
                piece_id_to_airtable_id_map,
                unknown_piece_count,
            ) = _map_piece_ids_to_healing_app_record_ids(roll_key)
            if unknown_piece_count > 0:
                ping_rob(
                    f"[HEALING] Pieces missing in healing app {unknown_piece_count} on roll {roll_key}"
                )

        defects = res.connectors.load("hasura").execute(
            """
            query ($roll_key: String, $inspector: Int) {
                make_roll_inspection(where: {inspector: {_eq: $inspector}, nest_key: {_like: $roll_key}}) {
                    piece_id
                    challenge
                    defect_name
                    defect_idx
                    image_idx
                    defect_id
                    airtable_defect_id
                }
            }
            """,
            {
                "roll_key": f"%{roll_key}%",
                "inspector": inspector_id,
            },
        )["make_roll_inspection"]
        defect_dict = {}
        for d in defects:
            if d["piece_id"] not in defect_dict:
                defect_dict[d["piece_id"]] = []
            defect_dict[d["piece_id"]].append(d)
        # handle duplicate printed pieces.
        duplicate_piece_ids = [
            i for i in piece_id_to_airtable_id_map.keys() if "::duplicate" in i
        ]
        originals_with_dupes = [i.split("::")[0] for i in duplicate_piece_ids]
        for dupe_id in duplicate_piece_ids:
            original_id = dupe_id.split("::")[0]
            if dupe_id not in defect_dict and original_id in defect_dict:
                logger.info(f"Passing {original_id} due to no defects on duplicate")
                del defect_dict[original_id]
            del piece_id_to_airtable_id_map[dupe_id]

        airtable_updates = []
        zone_free_defect_map = {
            r["id"]: r["fields"]["Defect"][0]
            for r in ZONE_X_DEFECT.all()
            if "Defect" in r["fields"]
        }
        for piece_id, airtable_id in piece_id_to_airtable_id_map.items():
            if (
                piece_id is not None
                and piece_id != ""
                and airtable_id is not None
                and airtable_id != ""
            ):
                defects = defect_dict.get(piece_id, [])
                payload = {
                    f"v3-{inspector_id}: All Defects": [
                        zone_free_defect_map[defect["airtable_defect_id"]]
                        for defect in defects
                        if defect["airtable_defect_id"] in zone_free_defect_map
                    ],
                    f"v3-{inspector_id}: Synced At": utc_now_iso_string(),
                    "Roll Name": roll_key.replace("-", ": "),
                    "Duplicated In Nesting": (piece_id in originals_with_dupes),
                }
                for i, defect in enumerate(defects):
                    if i > 3:
                        break
                    payload[f"v3-{inspector_id}: Defect {i+1}"] = [
                        defect["airtable_defect_id"]
                    ]
                    payload[f"v3-{inspector_id}: Defect {i+1} Input"] = defect[
                        "image_idx"
                    ]
                    payload[
                        f"v3-{inspector_id}: Defect {i+1} Challenge Contract"
                    ] = defect["challenge"]
                for i in range(len(defects), 4):
                    payload[f"v3-{inspector_id}: Defect {i+1}"] = []
                    payload[f"v3-{inspector_id}: Defect {i+1} Input"] = ""
                    payload[f"v3-{inspector_id}: Defect {i+1} Challenge Contract"] = ""
                airtable_updates.append(
                    {
                        "id": airtable_id,
                        "fields": payload,
                    }
                )
                if len(airtable_updates) > 50:
                    _update_airtable_pieces(airtable_updates, roll_key)
                    airtable_updates = []
        if len(airtable_updates) > 0:
            _update_airtable_pieces(airtable_updates, roll_key)
        res.connectors.load("hasura").execute(
            """
                mutation ($inspector: Int = 10, $roll_key: String = "") {
                    update_make_roll_inspection_progress(where: {inspector: {_eq: $inspector}, roll_key: {_eq: $roll_key}}, _set: {synced_ts: "now()"}) {
                        returning {
                            id
                        }
                    }
                }
                """,
            {
                "inspector": inspector_id,
                "roll_key": roll_key,
            },
        )
        combined_df["defect_list"] = combined_df.piece_id.apply(
            lambda i: defect_dict.get(i, [])
        )
        ping_slack(
            f"[HEALING] Synced {len(set(piece_id_to_airtable_id_map.values()))} pieces ({len(defect_dict.keys())} with defects) to healing app for roll {roll_key} for inspector {inspector_id}.",
            "autobots",
        )
        try:
            if lock.owned():
                lock.release()
        except:
            pass
        _post_observations_to_hasura(combined_df, inspector_id, roll_key)
    except Exception as ex:
        res.utils.logger.error(f"Failed to sync {roll_key} to healing app {repr(ex)}")
        ping_rob(
            f"[HEALING] Failed to sync {roll_key} for inspector {inspector_id} to healing app {repr(ex)}"
        )


def _update_airtable_pieces(airtable_updates, roll_key):
    if os.environ["RES_ENV"] == "production":
        logger.info(
            f"About to update {len(airtable_updates)} request pieces in airtable for {roll_key}"
        )
        try:
            results = REQUEST_PIECES.batch_update(airtable_updates, typecast=True)
            logger.info(f"Updated {len(results)} request pieces in airtable")
        except Exception as ex:
            logger.warn(f"Failed to sync to airtable {repr(ex)}")
            ping_slack(
                f"[HEALING] Failed to sync to airtable for {roll_key}: {repr(ex)}"
            )


def update_roll_inspection_progress(roll_key, roll_id, inspector, state):
    if os.environ["RES_ENV"] == "production":
        HEALING_ROLLS.update(
            roll_id,
            {f"Roll Inspection {inspector} {state}": "true"},
            typecast=True,
        )

        try:
            if state == "Finish":
                roll_record = ROLLS.first(
                    formula=f"{{Name}}='{roll_key.replace('-', ': ')}'"
                )

                ROLLS.update(
                    roll_record["id"],
                    {f"Roll Inspection {inspector} {state}": "true"},
                    typecast=True,
                )
        except Exception as e:
            res.utils.logger.warn(f"Failed to find the Roll in the Print App {e}")

    if state == "Start":
        res.connectors.load("hasura").execute(
            """
            mutation ($inspector: Int = 10, $roll_key: String = "") {
                insert_make_roll_inspection_progress_one(object: {inspector: $inspector, roll_key: $roll_key, started_ts: "now()"}, on_conflict: {constraint: roll_inspection_progress_pkey, update_columns: started_ts}) {
                    id
                }
            }
            """,
            {
                "inspector": inspector,
                "roll_key": roll_key,
            },
        )
    elif state == "Finish":
        res.connectors.load("hasura").execute(
            """
            mutation ($inspector: Int = 10, $roll_key: String = "") {
                update_make_roll_inspection_progress(where: {inspector: {_eq: $inspector}, roll_key: {_eq: $roll_key}}, _set: {finished_ts: "now()"}) {
                    returning {
                        id
                    }
                }
            }
            """,
            {
                "inspector": inspector,
                "roll_key": roll_key,
            },
        )
    else:
        logger.error(f"Unknown roll state: {state}")


def pre_cache_nest_info():
    logger.info("Pre caching nest info")
    for inspector in [1, 2]:
        roll_info = inspectable_roll_info(inspector, cache_timestamp())
        for _, v in roll_info.items():
            for nest_key in v["nest_keys"]:
                get_static_nest_info(nest_key, cache_timestamp())


def _post_observations_to_hasura(df, inspector_id, roll_key):
    kafka_connector = res.connectors.load("kafka")[PIECE_OBSERVATION_TOPIC]
    df = df[~df.asset_key.isnull()]

    df["one_number"] = df["asset_key"].apply(lambda k: int(k.split("_")[0]))
    df = df[~df.piece_code.isnull()]
    try:
        ones = df["one_number"].unique()

        non_zero_piece_instances = get_one_piece_healing_instance_resolver(ones)
    except Exception as ex:
        res.utils.logger.warn(f"Failed to load the healing pieces resolver {ex}")
        non_zero_piece_instances = lambda a, b: None

    for _, r in df.iterrows():
        kafka_msg = {
            "pieces": [
                {
                    "code": r.piece_code,
                    "make_instance": non_zero_piece_instances(
                        r.one_number, r.piece_code
                    ),
                    # "sequence_number": null...
                }
            ],
            "id": f"roll_inspect_{roll_key}_{inspector_id}_{r.one_number}_{r.piece_code}",
            "one_number": r.one_number,
            "node": "Make.Print.RollInspection",
            "status": "Exit",
            "defects": [d["defect_name"] for d in r.defect_list],
            "observed_at": res.utils.dates.utc_now_iso_string(),
            "metadata": {
                "inspector": inspector_id,
                "roll_key": roll_key,
                "raw_defect_json": json.dumps(r.defect_list),
            },
        }
        kafka_connector.publish(kafka_msg, use_kgateway=True)


# manual healing during print
@lru_cache(maxsize=10)
def get_manual_healing_data(printfile_key):
    asset_path = get_printfile_record(printfile_key)["fields"]["Asset Path"]
    s3 = res.connectors.load("s3")
    manifest = s3.read(f"{asset_path}/manifest.feather")
    pf_height = manifest.printfile_height_px.max()
    pf_width = manifest.printfile_width_px.max()
    thumb_dim = min(int(pf_height / 5), 15000)
    logger.debug(f"Printfile dimensions {pf_height}, {pf_width}")
    thumb_height = pf_height * 0.05
    thumb_width = pf_width * 0.05
    logger.debug(f"Assumed thumbnail dimensions {thumb_height}, {thumb_width}")
    scale_x = 300.0 * thumb_width / pf_width
    scale_y = 300.0 * thumb_height / pf_height
    stitching_payload = s3.read(f"{asset_path.replace('concat', '.meta')}/payload.json")
    combined_nest_df = pd.concat(
        [s3.read(n["nest_df_path"]) for n in stitching_payload["assets"]],
        ignore_index=True,
    )
    merged_manifest = manifest.drop(
        columns=["asset_id", "asset_key", "piece_name"]
    ).merge(combined_nest_df, on=["piece_id"], how="left")
    merged_manifest["poly"] = merged_manifest.nested_geometry.apply(
        lambda w: scale(loads(w), 1.0 / 300, -1.0 / 300)
    )
    merged_manifest["translated_poly"] = merged_manifest.apply(
        lambda r: translate(
            r.poly, r.min_x_inches - r.poly.bounds[0], r.min_y_inches - r.poly.bounds[1]
        ),
        axis=1,
    )
    merged_manifest["piece_name"] = merged_manifest.apply(
        lambda r: r.piece_name
        if r.piece_name is not None
        else r.s3_image_path.split("/")[-1].replace(".png", ""),
        axis=1,
    )
    map_pieces = "\n".join(
        [
            f"""
        <area
            href='#'
            piece_id='{r.piece_id}'
            shape='poly'
            coords='{",".join([f"{int(x*scale_x)},{max(0, int(y*scale_y))}" for x, y in r.translated_poly.boundary.coords][:-1])}'
        />\n
        """
            for _, r in merged_manifest.iterrows()
        ]
    )
    return {
        "printfile_img_path": s3.generate_presigned_url(
            f"{asset_path}/printfile_composite_thumbnail.png"
        ),
        "printfile_piece_map": map_pieces,
        "merged_manifest": merged_manifest,
        "printfile_key": printfile_key,
    }


@lru_cache(maxsize=100)
def get_printfile_record(printfile_key):
    pfs = PRINTFILES.all(formula=f"{{Rip File Name}}='{printfile_key}'")
    if len(pfs) == 1:
        return pfs[0]
    else:
        raise ValueError(f"There are {len(pfs)} printfile for RIP file {printfile_key}")


@lru_cache(maxsize=10)
def renest_pieces(printfile_key, piece_id_str):
    piece_ids = piece_id_str.split(",")
    logger.info(f"Renesting pieces for printfile {printfile_key}: {piece_ids}")
    merged_manifest = get_manual_healing_data(printfile_key)["merged_manifest"]
    # nest the pieces
    nestable_df = merged_manifest[
        merged_manifest.piece_id.apply(lambda i: i in piece_ids)
    ].reset_index(drop=True)
    packing = ProgressiveNode.from_pieces(
        nestable_df[
            [
                "piece_id",
                "asset_id",
                "asset_key",
                "s3_image_path",
                "piece_name",
                "piece_code",
                "piece_description",
                "zone",
                "fusing_type",
                "nestable_wkt",
                "piece_type",
                "cutline_uri",
                "buffer",
                "material",
                "stretch_x",
                "stretch_y",
                "paper_marker_stretch_x",
                "paper_marker_stretch_y",
                "output_bounds_width",
            ]
        ],
        nestable_df.output_bounds_width.max(),
    ).packing
    packing.compress(threads=1)
    s3 = res.connectors.load("s3")
    nest_key = f"{printfile_key}-{res_hash(str(piece_ids).encode())}"
    nested_df = packing.nested_df(None)
    s3_path = f"s3://{RES_DATA_BUCKET}/manual_healing/nests/{nest_key}"
    s3.write(f"{s3_path}/nest_df.feather", nested_df)
    plt.figure(figsize=(5, 5 * packing.height() / float(packing.max_width)))
    packing.packing.plot(show_waste=False)
    plt.savefig("nest.png", bbox_inches="tight")
    plt.close()
    s3.upload("nest.png", f"{s3_path}/nest.png")
    nest_preview_path = s3.generate_presigned_url(f"{s3_path}/nest.png")
    nest_height_yds = packing.height() / (300.0 * 36)
    nest_response = NESTS.create(
        {
            "Assets": list(nested_df.asset_id.unique()),
            "Material": nested_df.material[0],
            "Argo Jobkey": "manual_healing_" + nest_key,
            "Nested Length in Yards": nest_height_yds,
            "Nesting Utilization Score": packing.utilization(),
            "Dataframe Path": s3_path + "/nest_df.feather",
            "Nested Pieces": nestable_df.shape[0],
            "Print Queue": "TO DO",
            "Asset Path": "TODO",
            "Eligible for Assignment": "true",
            "Nesting Approach": "progressive",
            "Preview": [{"url": nest_preview_path}],
        },
        typecast=True,
    )
    return {
        "nest_preview": nest_preview_path,
        "nest_length": f"{nest_height_yds:.2f} yd",
        "nest_key": nest_response["id"],
    }


def composite_printfile(printfile_key, nest_key):
    printfile_record = get_printfile_record(printfile_key)
    # create printfile record
    airtable_response = PRINTFILES.create(
        {
            "task_key": "MANUAL_HEALING",
            "Rolls": printfile_record["fields"]["Rolls"],
            "Material": printfile_record["fields"]["Material"],
            "Nests": [nest_key],
            "Print Queue": "TO DO",
            **(
                {
                    "Assigned Printer": printfile_record["fields"]["Assigned Printer"],
                }
                if "Assigned Printer" in printfile_record["fields"]
                else {}
            ),
        },
        typecast=True,
    )
    nest_record = NESTS.get(nest_key)
    job_name = f"composite-manual-healing-{int(time.time())}"
    # start argo job
    payload = {
        "apiVersion": "resmagic.io/v1",
        "kind": "resFlow",
        "metadata": {"name": "make.nest.stitch_printfiles", "version": "primary"},
        "assets": [
            {
                "nest_record_id": nest_key,
                "nest_job_key": nest_record["fields"]["Argo Jobkey"],
                "nest_df_path": nest_record["fields"]["Dataframe Path"],
            }
        ],
        "args": {
            "print_file_record_id": airtable_response["id"],
            "num_headers": 0,
            "roll_name": "",
            "optimus_task_key": "",
            "roll_offset_px": 0,
            "header_offset_idx": 0,
        },
        "task": {"key": job_name},
    }
    res.connectors.load("argo").handle_event(
        payload,
        unique_job_name=job_name,
    )
    return {
        "printfile_id": airtable_response["id"],
        "rip_file_name": airtable_response["fields"]["RIP File Name"],
        "argo_job_name": "job_name",
    }


@lru_cache(maxsize=1)
def healable_printfiles(timestamp):
    printfiles = PRINTFILES.all(
        formula="""AND({Stitching Status}='DONE', OR({Print Queue}='TO DO', {Print Queue}='PROCESSING', {Hours Since Printed}<2))""",
        fields=[
            "Stitching Job Payload",
            "RIP File Name",
            "Assigned Printer",
        ],
    )
    pf_info = {}
    for p in printfiles:
        printer = p["fields"].get("Assigned Printer", "UNASSIGNED")
        if printer not in pf_info:
            pf_info[printer] = []
        pf_info[printer].append(p["fields"]["RIP File Name"])
    od = OrderedDict()
    for k in sorted(pf_info.keys()):
        od[k] = sorted(pf_info[k])
    return od


def retry_healing_sync():
    try:
        lock = res.connectors.load("redis").get_lock(
            f"LOCK:HEALING_SYNC_RETRY", timeout=30 * 60
        )
        lock.acquire()
        if not lock.owned():
            raise Exception("Failed to get lock")
        rolls_to_sync = res.connectors.load("hasura").execute(
            """
            query {
                make_roll_inspection_progress(where: {synced_ts: {_is_null: true}, finished_ts: {_is_null: false}, created_at: {_gt: "2024-03-20"}}) {
                    roll_key
                    inspector
                    started_ts
                    synced_ts
                    finished_ts
                }
            }
            """,
            {},
        )["make_roll_inspection_progress"]
        for r in rolls_to_sync:
            ping_rob(
                f"[HEALING] Attempting to re-sync: {r['roll_key']} for inspector {r['inspector']} finished inspection at {r['finished_ts']}"
            )
            sync_roll_to_healing_app(r["roll_key"], r["inspector"])
        try:
            if lock.owned():
                lock.release()
        except:
            pass
    except Exception as ex:
        res.utils.logger.error(f"Failed to retry healing sync {repr(ex)}")
