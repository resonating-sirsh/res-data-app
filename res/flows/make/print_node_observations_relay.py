import res
from itertools import groupby
from operator import itemgetter
from datetime import datetime
from res.flows.meta.pieces import PieceName
from res.utils import logger
from res.flows.meta.pieces import PieceName
from tenacity import retry, wait_fixed, stop_after_attempt
import pandas as pd
import traceback
from res.flows.make.production.queries import get_one_piece_healing_instance_resolver


FAIL_TO_PRINT_CONTRACT = "PIECE_WITHIN_PRINTED_ROLL_REGION"
CARRIAGE_OFFSET_IN = 0.0  # Will define the length of the carriage to adjust the observation confidence. For now it will be 0.0
USE_KGATEWAY = True
NODE_OBSERVATIONS_TOPIC = "res_make.piece_tracking.make_piece_observation_request"
NODE_CONSUMPTION_TOPIC = "res_make.costs.piece_set_consumption_at_node"
NODE_NAME = "Make.Print.Printer"
GET_PIECES_BY_PRINTJOB_NAME_QUERY = """
query get_pieces_by_printjob_name($printjob_name: String) {
  make_printfile_pieces(where: {printfile: {printjob_name: {_eq: $printjob_name}}}) {
    piece_id
    min_y_in
    printfile_id
    asset_key: metadata(path: "asset_key")
    piece_name: metadata(path: "piece_name")
    printfile{
    height_px
    }
  }
}
"""


def get_consumption_payload(label, category, value, units):
    return {
        "units": units,
        "value": value,
        "category": category,
        "label": label,
        "name": f"{label} {category}",
    }


@retry(wait=wait_fixed(3), stop=stop_after_attempt(1))
def produce_kafka_messages(
    kafka_client, data, ink_consumption=None, material_consumption=None, job_id=None
):
    kafka_client[NODE_OBSERVATIONS_TOPIC].publish(data, use_kgateway=USE_KGATEWAY)
    cost_client = kafka_client[NODE_CONSUMPTION_TOPIC]
    m = dict(data[0]["metadata"])
    # prepare one grain costs
    data = (
        res.utils.dataframes.expand_column_drop(
            pd.DataFrame(data).explode("pieces").reset_index(), "pieces"
        )[["one_number", "pieces_code"]]
        .drop_duplicates()
        .groupby("one_number")
        .agg({"pieces_code": list})
        .reset_index()
    )
    data["count"] = data["pieces_code"].map(len)

    res.utils.logger.info(
        f"publishing consumption data for pieces in {len(data)} ONEs with {data['count'].sum()} pieces"
    )
    # Also generate consumption contributions for pieces
    payloads = []

    for record in data.to_dict("records"):
        one_number = record["one_number"]
        pieces = record["pieces_code"]
        count = record["count"]
        d = {
            "id": res.utils.uuid_str_from_dict(
                {"one_number": int(one_number), "id": job_id, "node": "printer-exit"}
            ),
            "node": NODE_NAME,
            "group_id": str(job_id),
            "one_number": str(one_number),
            "created_at": res.utils.dates.utc_now_iso_string(),
            "piece_codes": pieces,
            "piece_count": count,
            "metadata": m,
        }
        consumption = []
        try:
            if ink_consumption:
                for k, v in ink_consumption.items():
                    consumption.append(
                        get_consumption_payload(
                            label=k,
                            # the value is normed by the number of pieces and then we multiple
                            value=v * count,
                            category="ink",
                            units="ml",
                        )
                    )

            # material is a good test - there is one material and we should hve contributions of total material in payload / piece count * pieces in ONE set
            if material_consumption:
                for k, v in material_consumption.items():
                    consumption.append(
                        get_consumption_payload(
                            label=k,
                            value=v * count,
                            category="material",
                            units="mm",
                        )
                    )
            d["consumption"] = consumption
            payloads.append(d)
        except:
            res.utils.logger.warn(f"Failing to publish consumption data")
            res.utils.logger.warn(traceback.format_exc())
    # publish what we have
    res.utils.logger.info(f"publishing payloads {len(payloads)} records ")
    cost_client.publish(payloads, use_kgateway=USE_KGATEWAY)
    res.utils.logger.info(f"done.")


def millimeters_to_inches(mm):
    if mm:
        return int(mm) * 0.0393701


def pixels_to_inches(pixels):
    printfile_dpi = 300
    return pixels / printfile_dpi


def get_ink_usage_contribution(p, factor=1):
    idf = pd.DataFrame([k["Usage"] for k in p["MSinkusage"]["Ink"]])
    idf["ToClean"] = idf["ToClean"].map(float)
    idf["ToPrint"] = idf["ToPrint"].map(float)
    idf["total"] = idf.sum(axis=1)
    ndf = pd.DataFrame([k["Color"] for k in p["MSinkusage"]["Ink"]])
    idf = idf.join(ndf)
    idf["total"] /= factor
    return dict(idf[[0, "total"]].values)


def get_piece_code(piece_name):
    if PieceName(piece_name).name != "":
        return PieceName(piece_name).name

    return piece_name


def produce_print_node_observations(event):
    """
    TODO
    1. Takes print-job data and get the pieces contained within that printfile
    2. Base on the min_y_in of the pieces, we will determine which ones we are almost certain that where printed.
    3. We will send an observation request to the topic set as NODE_OBSERVATIONS_TOPIC.

    to test see  /notebooks/docs/notebooks/flow-api/OnePiecesCosts.ipynb
    """
    output = None
    try:
        logger.info("Starting: produce_print_node_observations")
        hasura = res.connectors.load("hasura")
        kafka = res.connectors.load("kafka")
        MSjobinfo = event.get("MSjobinfo")
        print_job_name = MSjobinfo["Name"]
        printed_length_mm = float(MSjobinfo["PrintedLength"])
        ink_consumption = {}
        job_id = MSjobinfo.get("Jobid", MSjobinfo.get("StartTime"))
        printed_length_in = millimeters_to_inches(MSjobinfo["PrintedLength"])
        print_job_requested_length_in = millimeters_to_inches(
            MSjobinfo["RequestedLength"]
        )
        lowest_printed_y_in = (
            print_job_requested_length_in - printed_length_in + CARRIAGE_OFFSET_IN
        )

        if MSjobinfo["Status"] != "0":
            logger.info(f"Querying Hasura for pieces contained within {print_job_name}")
            # Getting all pieces on printfile.
            response = hasura.execute_with_kwargs(
                GET_PIECES_BY_PRINTJOB_NAME_QUERY, **{"printjob_name": print_job_name}
            )

            if response["make_printfile_pieces"]:
                """
                Determine if this was a "cropped" printfile.
                By cropped we mean that operations are trying to fix a mistake they've made
                i.e: re-printing damaged pieces, re-printing stuff for the sake of experimenting.
                since we can't know where stuff starts/ends we will just ignore it for now and not
                produce observations based on them.
                For now let's say that teh requested length should be at least 95% of the print-file's
                original height:
                request_length/pf_height>=0.95
                """
                print_file_height_in = pixels_to_inches(
                    response["make_printfile_pieces"][0]["printfile"]["height_px"]
                )

                if print_job_requested_length_in / print_file_height_in >= 0.95:
                    pieces_all = response["make_printfile_pieces"]

                    num_pieces = len(pieces_all)
                    res.utils.logger.info(f"found {num_pieces} pieces for the job")
                    try:
                        ink_consumption = get_ink_usage_contribution(
                            MSjobinfo, factor=num_pieces
                        )
                    except:
                        res.utils.logger.info(f"Problem getting ink consumption")
                        res.utils.logger.warn(traceback.format_exc())

                    material_consumption = {
                        MSjobinfo["Media"]: printed_length_mm / num_pieces
                    }
                    res.utils.logger.info(
                        f"Material Consumption Contributions {material_consumption}"
                    )
                    res.utils.logger.info(
                        f"Ink Consumption Contributions {ink_consumption}"
                    )

                    print_file_id = pieces_all[0]["printfile_id"]

                    # Get one_number and piece code from pulled info.
                    for piece in pieces_all:
                        piece["one_number"] = int(piece["asset_key"].split("_")[0])
                        piece["piece_code"] = get_piece_code(piece["piece_name"])

                    # Here we sort -> group pieces by one.
                    sorted_pieces = sorted(pieces_all, key=itemgetter("one_number"))
                    grouped_pieces = []
                    one_numbers = []

                    for one_number, pieces in groupby(
                        sorted_pieces, itemgetter("one_number")
                    ):
                        grouped_pieces.append(list(pieces))
                        one_numbers.append(one_number)

                    try:
                        non_zero_piece_instances = (
                            get_one_piece_healing_instance_resolver(
                                one_numbers=one_numbers
                            )
                        )
                        res.utils.logger.info(
                            "Loaded the resolver for the healing instances"
                        )
                    except Exception as ex:
                        res.utils.logger.warn(
                            f"Failed to load the healing pieces resolver {ex}"
                        )
                        non_zero_piece_instances = lambda a, b: None

                    # Create an array of observations. check below
                    ones_observations = []
                    pieces_fail = []
                    pieces_exit = []
                    for i in range(len(one_numbers)):
                        pieces_fail = []
                        pieces_exit = []
                        pieces_exit = [
                            piece
                            for piece in grouped_pieces[i]
                            if float(piece["min_y_in"]) > lowest_printed_y_in
                        ]
                        pieces_fail = [
                            piece
                            for piece in grouped_pieces[i]
                            if float(piece["min_y_in"]) < lowest_printed_y_in
                        ]
                        ones_observations.append(
                            {
                                "one_number": one_numbers[i],
                                "pieces_exit": pieces_exit,
                                "pieces_fail": pieces_fail,
                            }
                        )

                    NODE_OBSERVING_AT = "Make.Print.Printer"

                    observation_data = {
                        "id": "",
                        "pieces": [],
                        "one_number": "",
                        "one_code": None,
                        "node": NODE_OBSERVING_AT,
                        "status": "",
                        "contracts_failed": [],
                        "defects": [],
                        "observed_at": "",
                        "observation_confidence": None,
                        "metadata": {
                            "printjob_name": print_job_name,
                            "print_file_id": print_file_id,
                        },
                    }

                    # We create messages batching by the grouped pieces (by one_number).
                    messages_to_send = []
                    observation_timestamp = (
                        datetime.utcnow().isoformat()
                    )  # datetime.fromtimestamp(int(MSjobinfo["EndTime"])).isoformat()#
                    res.utils.logger.info("compiling observations")
                    for observation in ones_observations:
                        one_number = observation["one_number"]
                        # Pieces Exiting
                        if observation["pieces_exit"]:
                            observation_data["pieces"] = []
                            OBSERVATION_STATUS = "Exit"
                            observation_data["id"] = res.utils.uuid_str_from_dict(
                                {
                                    "print_file_id": print_file_id,
                                    "one_number": one_number,
                                    "status": OBSERVATION_STATUS,
                                }
                            )
                            observation_data["one_number"] = one_number
                            observation_data["status"] = OBSERVATION_STATUS
                            observation_data["observed_at"] = observation_timestamp
                            observation_data["contracts_failed"] = []
                            observation_data["pieces"] = []
                            for piece in observation["pieces_exit"]:
                                observation_data["pieces"].append(
                                    {
                                        "id": None,
                                        "oid": res.utils.uuid_str_from_dict(
                                            {
                                                "one_number": int(float(one_number)),
                                                "piece_code": piece["piece_code"],
                                            }
                                        ),
                                        "code": piece["piece_code"],
                                        "make_instance": non_zero_piece_instances(
                                            int(float(one_number)),
                                            piece["piece_code"],
                                        ),
                                    }
                                )
                            messages_to_send.append(observation_data.copy())
                        # Pieces Failing
                        if observation["pieces_fail"]:
                            observation_data["pieces"] = []
                            OBSERVATION_STATUS = "Fail"
                            observation_data["id"] = res.utils.uuid_str_from_dict(
                                {
                                    "print_file_id": print_file_id,
                                    "one_number": one_number,
                                    "status": OBSERVATION_STATUS,
                                }
                            )
                            observation_data["one_number"] = one_number
                            observation_data["status"] = OBSERVATION_STATUS
                            observation_data["observed_at"] = observation_timestamp
                            observation_data["contracts_failed"] = [
                                FAIL_TO_PRINT_CONTRACT
                            ]

                            for piece in observation["pieces_fail"]:
                                observation_data["pieces"].append(
                                    {
                                        "id": None,
                                        "oid": res.utils.uuid_str_from_dict(
                                            {
                                                "one_number": int(float(one_number)),
                                                "piece_code": piece["piece_code"],
                                            }
                                        ),
                                        "code": piece["piece_code"],
                                    }
                                )
                        messages_to_send.append(observation_data.copy())
                    output = messages_to_send
                    produce_kafka_messages(
                        kafka,
                        messages_to_send,
                        ink_consumption=ink_consumption,
                        material_consumption=material_consumption,
                        job_id=job_id,
                    )
                    logger.info("Done Creating Observations")
                else:
                    logger.info(
                        "This printfile does not match its original size. Skipping to not cause errors.\n Done running."
                    )

            else:
                logger.info(
                    "No pieces were found in the database for printjob:\n Name: {}\n This could be due to dev test or due to off-framework print".format(
                        MSjobinfo["Name"]
                    )
                )

        elif MSjobinfo["Status"] == "0":
            res.utils.logger.warn("Print-job still in progress...")
        else:
            res.utils.logger.warn("No Print-job Data.")

        return output
    except Exception as ex:
        res.utils.logger.warn(f"error handling : {res.utils.ex_repr(ex)}")
        raise ex


EXAMPLE_INPUT = {
    "MSjobinfo": {
        "Printer": "flash",
        "Jobid": "92181",
        "Name": "CTW70_R39084_0_001_CTW70_1678908109",
        "Status": "3",
        "PrintMode": "D4: 8passes 9levels StdSpeed HQ",
        "Direction": "Bidi",
        "Media": "CTW70",
        "HeadGap": "3.000",
        "Width": "1488",
        "RequestedLength": "26801",
        "StartTime": "1678912336",
        "EndTime": "1678915745",
        "PrintedLength": "23858",
        "FoldDetected": "1",
        "MSinkusage": {
            "Ink": [
                {
                    "Color": "Cyan",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "39.938", "ToClean": "0.000"},
                },
                {
                    "Color": "Magenta",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "17.678", "ToClean": "0.000"},
                },
                {
                    "Color": "Yellow",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "30.037", "ToClean": "0.000"},
                },
                {
                    "Color": "Black",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "36.936", "ToClean": "0.000"},
                },
                {
                    "Color": "Red",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "3.849", "ToClean": "0.000"},
                },
                {
                    "Color": "Orange",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "2.681", "ToClean": "0.000"},
                },
                {
                    "Color": "Blue",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "1.289", "ToClean": "0.000"},
                },
                {
                    "Color": "Transparent",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "86.445", "ToClean": "0.000"},
                },
            ]
        },
    }
}

EXAMPLE_OUTPUT = [
    {
        "id": "fa9aee7f-7a9f-664b-42b0-d10ee42047af",
        "pieces": [
            {
                "id": None,
                "oid": "2932a18f-ee79-9d0c-3537-0ad2074dcf4f",
                "code": "DRSSLPNLTPRT-S",
            }
        ],
        "one_number": 10296790,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "2cc29589-9612-eb67-c177-de1e0fd02dc2",
        "pieces": [
            {
                "id": None,
                "oid": "04a60fa3-84e3-b92b-016b-e771c18290a1",
                "code": "DRSBKPNLRT-S",
            }
        ],
        "one_number": 10296936,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "69b30d6b-141d-3bb5-0ca9-aa21cd3d7db8",
        "pieces": [
            {
                "id": None,
                "oid": "5021418c-e354-47a3-d0f8-4a8083d0e076",
                "code": "SKTBKRFLTP-S",
            }
        ],
        "one_number": 10297008,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "bb8755fa-2905-1ed6-3a91-fd543cb1f420",
        "pieces": [
            {
                "id": None,
                "oid": "2c3318a1-36bd-e7d1-66b5-d2597d95f393",
                "code": "SHTSLPNLLF-S",
            }
        ],
        "one_number": 10297133,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "2824d475-b3fe-b7d8-144d-1bbfc27ed631",
        "pieces": [
            {
                "id": None,
                "oid": "93e7334e-b91d-1296-0670-f7aa853fcb61",
                "code": "DRSSLPNLUNRT-S",
            }
        ],
        "one_number": 10297174,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "b297d45f-5db7-666f-6f49-406d89ddc3c5",
        "pieces": [
            {
                "id": None,
                "oid": "93e7334e-b91d-1296-0670-f7aa853fcb61",
                "code": "DRSSLPNLUNRT-S",
            }
        ],
        "one_number": 10297174,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Fail",
        "contracts_failed": "PIECE_WITHIN_PRINTED_ROLL_REGION",
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "2c4b40ca-7529-f8bf-17e3-c751ee62ea1d",
        "pieces": [
            {
                "id": None,
                "oid": "afe65d14-077f-f480-586c-c2e2b0119575",
                "code": "DRSSLPNLTPRT-S",
            }
        ],
        "one_number": 10297183,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "ad3ad75a-9595-a6f0-f320-b28dc8ab9096",
        "pieces": [
            {
                "id": None,
                "oid": "afe65d14-077f-f480-586c-c2e2b0119575",
                "code": "DRSSLPNLTPRT-S",
            }
        ],
        "one_number": 10297183,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Fail",
        "contracts_failed": "PIECE_WITHIN_PRINTED_ROLL_REGION",
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "4569fb03-4746-e8ba-0ec7-0a1d8bd16a59",
        "pieces": [
            {
                "id": None,
                "oid": "c9fe4657-79f6-2b74-639a-bb25a529b65a",
                "code": "DRSFTPNLLF-S",
            }
        ],
        "one_number": 10297186,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "05a4e9c8-0b93-9702-8019-0a2a04a71f8a",
        "pieces": [
            {
                "id": None,
                "oid": "6cd16a2d-dcd9-9c73-6c58-f8f65fd4b158",
                "code": "DRSFTPNLLF-S",
            }
        ],
        "one_number": 10297243,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "8c563125-60c1-8813-0990-1508ecacdd64",
        "pieces": [
            {
                "id": None,
                "oid": "47bd0705-908f-5025-37fa-e0d201b5a67a",
                "code": "SHTSLPNLRT-S",
            },
            {
                "id": None,
                "oid": "39ab292c-aaf1-143c-bbae-a1c7ff5b2424",
                "code": "SHTSLPNLLF-S",
            },
            {
                "id": None,
                "oid": "e5a1d9b4-eeab-4523-1736-ba14efe7f900",
                "code": "SHTBKYKETP-S",
            },
            {
                "id": None,
                "oid": "2c8ad1f7-b4e1-fa15-a7c7-880b64cb630f",
                "code": "SHTNKCLRUN-S",
            },
            {
                "id": None,
                "oid": "63bf61ef-2521-e729-b6d6-f131c03f5d7c",
                "code": "SHTBKYKEUN-S",
            },
            {
                "id": None,
                "oid": "eb080eef-7e01-d931-3384-7e602ca91279",
                "code": "SHTBKPNL-S",
            },
            {
                "id": None,
                "oid": "ae392841-d75d-d303-b707-b0f9254bff0a",
                "code": "SHTFTPNLRT-S",
            },
        ],
        "one_number": 10297327,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "beea57aa-72c8-4031-c320-ed419620b191",
        "pieces": [
            {
                "id": None,
                "oid": "47bd0705-908f-5025-37fa-e0d201b5a67a",
                "code": "SHTSLPNLRT-S",
            },
            {
                "id": None,
                "oid": "39ab292c-aaf1-143c-bbae-a1c7ff5b2424",
                "code": "SHTSLPNLLF-S",
            },
            {
                "id": None,
                "oid": "e5a1d9b4-eeab-4523-1736-ba14efe7f900",
                "code": "SHTBKYKETP-S",
            },
            {
                "id": None,
                "oid": "2c8ad1f7-b4e1-fa15-a7c7-880b64cb630f",
                "code": "SHTNKCLRUN-S",
            },
            {
                "id": None,
                "oid": "63bf61ef-2521-e729-b6d6-f131c03f5d7c",
                "code": "SHTBKYKEUN-S",
            },
            {
                "id": None,
                "oid": "eb080eef-7e01-d931-3384-7e602ca91279",
                "code": "SHTBKPNL-S",
            },
            {
                "id": None,
                "oid": "ae392841-d75d-d303-b707-b0f9254bff0a",
                "code": "SHTFTPNLRT-S",
            },
        ],
        "one_number": 10297327,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Fail",
        "contracts_failed": "PIECE_WITHIN_PRINTED_ROLL_REGION",
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "3e8bc638-32de-fb0f-1c17-16f38998d6e0",
        "pieces": [
            {
                "id": None,
                "oid": "11890264-1430-0c5a-5ebf-b01aa95ca2fc",
                "code": "SHTSLPNLLF-S",
            },
            {
                "id": None,
                "oid": "b8863fef-e61e-e557-2cdf-cb5499270747",
                "code": "SHTFTPNLRT-S",
            },
        ],
        "one_number": 10297338,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "03651d0d-1e20-4b5d-9b2a-5fa08f9c7d6c",
        "pieces": [
            {
                "id": None,
                "oid": "11890264-1430-0c5a-5ebf-b01aa95ca2fc",
                "code": "SHTSLPNLLF-S",
            },
            {
                "id": None,
                "oid": "b8863fef-e61e-e557-2cdf-cb5499270747",
                "code": "SHTFTPNLRT-S",
            },
        ],
        "one_number": 10297338,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Fail",
        "contracts_failed": "PIECE_WITHIN_PRINTED_ROLL_REGION",
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "50f0e806-0ab2-4a7c-30a9-0ac6a086f6ba",
        "pieces": [
            {
                "id": None,
                "oid": "67195da0-3a44-aadb-b525-2e3ddf3ec92b",
                "code": "SKTFTWBN-S",
            },
            {
                "id": None,
                "oid": "452030d4-e705-ea25-24e5-05fb4e457a6b",
                "code": "SKTFTRFLTP-S",
            },
        ],
        "one_number": 10297657,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "34cb5529-fe97-4d5f-00f5-81b4bbd84b05",
        "pieces": [
            {
                "id": None,
                "oid": "f3f551fd-ab73-640b-4c68-25f24446fcfb",
                "code": "SKTFTRFLTP-S",
            }
        ],
        "one_number": 10297704,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "e0216ba7-f343-34c5-89c9-70b5f12cf6dd",
        "pieces": [
            {
                "id": None,
                "oid": "18e12f9f-d3c5-f768-663b-4518f320601a",
                "code": "SKTFTRFLTP-S",
            }
        ],
        "one_number": 10297716,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "b9cf8d2a-590c-3d47-3874-fcf4960fa8c6",
        "pieces": [
            {
                "id": None,
                "oid": "1cf690dc-cbeb-001c-7693-81286db9e8b8",
                "code": "BLSBNTIELF-S",
            },
            {
                "id": None,
                "oid": "f36dfb31-e087-2937-9fd2-12e322ef457e",
                "code": "BLSBKPNL-S",
            },
        ],
        "one_number": 10297732,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "c9134e50-2d62-ecb3-2430-5454df1b74fa",
        "pieces": [
            {
                "id": None,
                "oid": "ff23632f-09f4-41d3-5548-61b75f51d11d",
                "code": "BLSBNTIELF-S",
            }
        ],
        "one_number": 10297738,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "0a66e02e-92ca-b719-a914-37c48d50a69f",
        "pieces": [
            {
                "id": None,
                "oid": "e4e91b90-a2a2-eec7-62ab-1582f5e27142",
                "code": "SKTBKWBN-S",
            },
            {
                "id": None,
                "oid": "fc49b7ea-61d7-b883-b449-fd2455d71288",
                "code": "SKTFTRFLBT-S",
            },
        ],
        "one_number": 10297776,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "80b64683-b051-fd70-3c84-5ec45f029547",
        "pieces": [
            {
                "id": None,
                "oid": "5ed660b4-b11f-9bd3-1243-f1a642388f47",
                "code": "SKTFTRFLTP-S",
            },
            {
                "id": None,
                "oid": "e1d67868-b763-8dcc-3088-223df91981f1",
                "code": "SKTFTWBN-S",
            },
        ],
        "one_number": 10297784,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "f7ce4a57-ff50-b842-1580-fea3b95bc5cf",
        "pieces": [
            {
                "id": None,
                "oid": "705bdda4-0ace-66f0-5b52-f64a7ed8b83f",
                "code": "SKTBKWBN-S",
            },
            {
                "id": None,
                "oid": "c8cc860d-743f-84c4-c5d5-4ea06f6c652b",
                "code": "SKTBKRFLTP-S",
            },
            {
                "id": None,
                "oid": "58688430-65cf-749e-21c4-b3010176d5af",
                "code": "SKTBKRFLBT-S",
            },
        ],
        "one_number": 10297802,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "88647dc1-1fe5-156a-cce2-7863001f9c38",
        "pieces": [
            {
                "id": None,
                "oid": "22de2545-6968-e3f4-4367-7392c7de0cfc",
                "code": "PNTFTPKBLF-C",
            },
            {
                "id": None,
                "oid": "08fa6674-456a-3d21-f16b-2a77e98bd22b",
                "code": "PNTFTPKBRT-C",
            },
        ],
        "one_number": 10297932,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "419b4517-2efd-7815-c02f-c94971d374de",
        "pieces": [
            {
                "id": None,
                "oid": "424c850b-9ba5-2c02-cd9a-1394e0d28b17",
                "code": "SKTFTRFLTP-S",
            }
        ],
        "one_number": 10297977,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "a0728dbb-da41-a95e-905e-52230190abb6",
        "pieces": [
            {
                "id": None,
                "oid": "424c850b-9ba5-2c02-cd9a-1394e0d28b17",
                "code": "SKTFTRFLTP-S",
            }
        ],
        "one_number": 10297977,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Fail",
        "contracts_failed": "PIECE_WITHIN_PRINTED_ROLL_REGION",
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "67ec9e42-4caf-8391-2bbf-dec5f9ceaa2f",
        "pieces": [
            {
                "id": None,
                "oid": "78fef691-066c-84d1-8391-fb1a52bd5786",
                "code": "SKTBKWBN-S",
            }
        ],
        "one_number": 10297982,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "7f9cc067-5e57-de61-2082-a72bd14ceb7e",
        "pieces": [
            {
                "id": None,
                "oid": "78fef691-066c-84d1-8391-fb1a52bd5786",
                "code": "SKTBKWBN-S",
            }
        ],
        "one_number": 10297982,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Fail",
        "contracts_failed": "PIECE_WITHIN_PRINTED_ROLL_REGION",
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "9467ab85-0c24-3acd-0f0c-b7c946bd1208",
        "pieces": [
            {
                "id": None,
                "oid": "e68897e3-7501-70d2-f4ae-8b529780485b",
                "code": "DRSSLPNLTPRT-S",
            }
        ],
        "one_number": 10298161,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "0fb83037-c811-8803-4574-7411d59841ab",
        "pieces": [
            {
                "id": None,
                "oid": "6c70d8e4-c960-7c9b-de26-11687ec4de9f",
                "code": "DRSSLPNLTPRT-S",
            }
        ],
        "one_number": 10298237,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "4c908f7b-f4dc-ba80-96cc-1e8d0eeea008",
        "pieces": [
            {
                "id": None,
                "oid": "5c9460eb-a626-ac17-5710-3b49197664c3",
                "code": "DRSFTPNLRT-S",
            },
            {
                "id": None,
                "oid": "bf267643-c734-f891-4d0b-7a3071b8d9f7",
                "code": "DRSBKYKETP-S",
            },
            {
                "id": None,
                "oid": "169b53cc-9111-1322-58be-ed9321668d14",
                "code": "DRSSDPKBRTTP-S",
            },
            {
                "id": None,
                "oid": "782e497d-557d-00b8-408a-591850e0bde1",
                "code": "DRSBKPNLRT-S",
            },
            {
                "id": None,
                "oid": "44ecde73-4a22-745a-56e5-4e7fe5c33086",
                "code": "DRSWTBLT-S",
            },
            {
                "id": None,
                "oid": "ea016790-b071-ce3a-f10d-1c64136e1cd1",
                "code": "DRSSLPNLRT-S",
            },
            {
                "id": None,
                "oid": "df08220d-9829-6f8d-4cb2-78ec476fcd8c",
                "code": "DRSSLPNLLF-S",
            },
            {
                "id": None,
                "oid": "3657995e-6891-d09b-9466-1824b73f7221",
                "code": "DRSFTPNLLF-S",
            },
            {
                "id": None,
                "oid": "4ff8a976-20e6-cb9d-57f3-090a470042c1",
                "code": "DRSBKPNLLF-S",
            },
            {
                "id": None,
                "oid": "198fbbf9-a807-950a-0d3c-3e896ceb072f",
                "code": "DRSSDPKBLFTP-S",
            },
            {
                "id": None,
                "oid": "5f99971f-f28e-2f26-f1be-004428b70df2",
                "code": "DRSSDPKBRTUN-S",
            },
            {
                "id": None,
                "oid": "ded44108-2480-bbbe-6220-130310a44fee",
                "code": "DRSBKYKEUN-S",
            },
            {
                "id": None,
                "oid": "6f3578ee-79d5-312d-2da5-fadd4a08a123",
                "code": "DRSSDPKBLFUN-S",
            },
        ],
        "one_number": 10298539,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "c3315069-1c4f-10e0-cd30-c723ad122b7f",
        "pieces": [
            {
                "id": None,
                "oid": "78ce4380-cd8a-9ff6-5f72-234f9b33339d",
                "code": "SHTSLUNPLF-S",
            },
            {
                "id": None,
                "oid": "a45950a2-e80f-4529-9c0a-476ab9690c4e",
                "code": "SHTFTPNLLF-S",
            },
            {
                "id": None,
                "oid": "0663b397-939e-892a-46fa-0fe7632edd8d",
                "code": "SHTSLPNLLF-S",
            },
            {
                "id": None,
                "oid": "68fe1521-db57-a395-0e93-b1bb6913a4ac",
                "code": "SHTFTPNLRT-S",
            },
            {
                "id": None,
                "oid": "5f8a0cda-6d4d-0de6-06b6-824cee841cc7",
                "code": "SHTBKPNL-S",
            },
            {
                "id": None,
                "oid": "95aa420d-cba6-9175-bd2f-fb26d8d1402d",
                "code": "SHTSLPNLRT-S",
            },
            {
                "id": None,
                "oid": "6d67f60d-d38e-f438-f805-c8f22de1fd9f",
                "code": "SHTFTPKTLF-S",
            },
            {
                "id": None,
                "oid": "ef935b2b-6970-ef95-252b-ceec7fce9ac6",
                "code": "SHTSLUNPRT-S",
            },
        ],
        "one_number": 10298563,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "92f7a3e5-a4b6-2f6c-f709-0824b35df173",
        "pieces": [
            {
                "id": None,
                "oid": "1f74e36e-63a2-0c43-94a8-37306f83f9e5",
                "code": "DRSSLPNLUNRT-S",
            },
            {
                "id": None,
                "oid": "42ab6915-e5ef-23ca-3c50-b0b2b0280ee3",
                "code": "DRSSLPNLUNLF-S",
            },
            {
                "id": None,
                "oid": "345b5491-9bd3-ff1f-c597-6b20679dbd0f",
                "code": "DRSFTPKBLFTP-S",
            },
            {
                "id": None,
                "oid": "805da018-2320-eac7-1ee3-20b9862c703c",
                "code": "DRSFTPKBRTUN-S",
            },
            {
                "id": None,
                "oid": "b8f87953-6036-cf6d-7b10-ee0a471bdb11",
                "code": "DRSFTCHPRT-S",
            },
            {
                "id": None,
                "oid": "7e0a4ab6-0322-4a72-48b8-1843d49b8806",
                "code": "DRSBKYKETP-S",
            },
            {
                "id": None,
                "oid": "781332e5-c310-1dc8-ff73-a33d204cbd12",
                "code": "DRSSLPNLTPLF-S",
            },
            {
                "id": None,
                "oid": "06878e29-5334-1693-70cb-3a7d5f7703f6",
                "code": "DRSFTPNLLF-S",
            },
            {
                "id": None,
                "oid": "8bebe1dc-03a7-62b4-490e-2015aa4642a2",
                "code": "DRSBSPNLLF-S",
            },
            {
                "id": None,
                "oid": "3004ce15-07f8-0c18-3582-b38fab484f79",
                "code": "DRSSLPNLTPRT-S",
            },
            {
                "id": None,
                "oid": "15fafe51-7984-221f-8a8e-c54d5cc769d9",
                "code": "DRSFTPKBLFUN-S",
            },
            {
                "id": None,
                "oid": "7f793977-8bf9-c53d-d20c-24b714443d82",
                "code": "DRSBKYKEUN-S",
            },
            {
                "id": None,
                "oid": "405f5cf9-f1c8-34a2-bf2a-596a6f9794f4",
                "code": "DRSFTPNLRT-S",
            },
            {
                "id": None,
                "oid": "d98f2203-24ef-9626-9de2-6cc156eebfad",
                "code": "DRSFTPKBRTTP-S",
            },
            {
                "id": None,
                "oid": "3874b7c4-bbdb-0027-0259-7da4f3a359a6",
                "code": "DRSBKPNL-S",
            },
            {
                "id": None,
                "oid": "c4bc92dc-1e70-e27f-4810-a1f043b9e842",
                "code": "DRSBSPNLRT-S",
            },
            {
                "id": None,
                "oid": "a953e138-0bff-c750-0e84-175858b10e11",
                "code": "DRSFTCHPLF-S",
            },
        ],
        "one_number": 10298584,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "4ad7acf9-8f8f-27c8-dcab-dd1225d4c748",
        "pieces": [
            {
                "id": None,
                "oid": "067359b7-6253-7af5-a658-273adf561664",
                "code": "SKTBKWBN-S",
            },
            {
                "id": None,
                "oid": "6ae22cb2-b4b9-87b2-df40-9aa394acdba4",
                "code": "SKTFTWBN-S",
            },
            {
                "id": None,
                "oid": "015de78a-c8d1-4635-fd45-d1b33f460ea2",
                "code": "SKTBKRFLTP-S",
            },
            {
                "id": None,
                "oid": "3ef082a6-91ca-a6ed-c524-28677d0afc49",
                "code": "SKTBKRFLBT-S",
            },
            {
                "id": None,
                "oid": "ad24de04-0ff3-3ccb-eacd-e7f67147200a",
                "code": "SKTFTRFLTP-S",
            },
        ],
        "one_number": 10298595,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "7f166f5d-50b3-64f4-efa4-16453980d37b",
        "pieces": [
            {
                "id": None,
                "oid": "067359b7-6253-7af5-a658-273adf561664",
                "code": "SKTBKWBN-S",
            },
            {
                "id": None,
                "oid": "6ae22cb2-b4b9-87b2-df40-9aa394acdba4",
                "code": "SKTFTWBN-S",
            },
            {
                "id": None,
                "oid": "015de78a-c8d1-4635-fd45-d1b33f460ea2",
                "code": "SKTBKRFLTP-S",
            },
            {
                "id": None,
                "oid": "3ef082a6-91ca-a6ed-c524-28677d0afc49",
                "code": "SKTBKRFLBT-S",
            },
            {
                "id": None,
                "oid": "ad24de04-0ff3-3ccb-eacd-e7f67147200a",
                "code": "SKTFTRFLTP-S",
            },
        ],
        "one_number": 10298595,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Fail",
        "contracts_failed": "PIECE_WITHIN_PRINTED_ROLL_REGION",
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "e9fa124d-8e36-45d0-b9ec-1f8011308cc3",
        "pieces": [
            {
                "id": None,
                "oid": "fb1fefa4-e182-a47a-43c7-1fd83c316d2e",
                "code": "SKTFTRFLTP-S",
            },
            {
                "id": None,
                "oid": "e94c6089-7047-835c-d343-630f2b2a9b47",
                "code": "SKTFTRFLBT-S",
            },
            {
                "id": None,
                "oid": "34c992e7-c2c6-a404-f50c-8f972ac6c3e1",
                "code": "SKTBKRFLTP-S",
            },
            {
                "id": None,
                "oid": "1cdd2202-06fb-5696-6506-5fb75202e31a",
                "code": "SKTWTWBN-S",
            },
            {
                "id": None,
                "oid": "9a728265-b788-d27d-6ecb-d124fd27961f",
                "code": "SKTBKRFLBT-S",
            },
        ],
        "one_number": 10298596,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "303216af-87fe-fca3-f8a4-de1fc0468343",
        "pieces": [
            {
                "id": None,
                "oid": "c511dd48-b269-fe9f-e7b4-1789897d7e43",
                "code": "SKTFTRFLBT-S",
            },
            {
                "id": None,
                "oid": "2bfe67cf-ff2c-98ed-8d07-8badeb5afa65",
                "code": "SKTFTWBN-S",
            },
            {
                "id": None,
                "oid": "d0582e76-affb-12bd-c27b-3bf571205652",
                "code": "SKTBKRFLTP-S",
            },
            {
                "id": None,
                "oid": "fc79eed5-2277-f279-02e1-3824044baaa5",
                "code": "SKTBKRFLBT-S",
            },
            {
                "id": None,
                "oid": "4f839f3b-de3b-8ac8-e895-2b87915f6c67",
                "code": "SKTFTRFLTP-S",
            },
        ],
        "one_number": 10298597,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "aaae4d8a-d73d-a2d8-fbbe-345424e1e7cd",
        "pieces": [
            {
                "id": None,
                "oid": "c511dd48-b269-fe9f-e7b4-1789897d7e43",
                "code": "SKTFTRFLBT-S",
            },
            {
                "id": None,
                "oid": "2bfe67cf-ff2c-98ed-8d07-8badeb5afa65",
                "code": "SKTFTWBN-S",
            },
            {
                "id": None,
                "oid": "d0582e76-affb-12bd-c27b-3bf571205652",
                "code": "SKTBKRFLTP-S",
            },
            {
                "id": None,
                "oid": "fc79eed5-2277-f279-02e1-3824044baaa5",
                "code": "SKTBKRFLBT-S",
            },
            {
                "id": None,
                "oid": "4f839f3b-de3b-8ac8-e895-2b87915f6c67",
                "code": "SKTFTRFLTP-S",
            },
        ],
        "one_number": 10298597,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Fail",
        "contracts_failed": "PIECE_WITHIN_PRINTED_ROLL_REGION",
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "af0847ec-fb65-6890-01e9-37e0dc59eb7c",
        "pieces": [
            {
                "id": None,
                "oid": "5bcb99a0-d319-332a-fd57-611f7f576274",
                "code": "SKTFTRFLTP-S",
            },
            {
                "id": None,
                "oid": "0805b963-4dfa-91ac-22d8-8fb041de65a6",
                "code": "SKTFTWBN-S",
            },
            {
                "id": None,
                "oid": "d6c8ebd0-8dc9-fd11-bdca-e2d1ee3bf987",
                "code": "SKTBKWBN-S",
            },
            {
                "id": None,
                "oid": "d0386c10-a8de-ed5c-a29e-5e40756711b2",
                "code": "SKTFTRFLBT-S",
            },
        ],
        "one_number": 10298598,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "0c7b1fdf-c5ea-9dc5-3c52-7ee1d4066f14",
        "pieces": [
            {
                "id": None,
                "oid": "5bcb99a0-d319-332a-fd57-611f7f576274",
                "code": "SKTFTRFLTP-S",
            },
            {
                "id": None,
                "oid": "0805b963-4dfa-91ac-22d8-8fb041de65a6",
                "code": "SKTFTWBN-S",
            },
            {
                "id": None,
                "oid": "d6c8ebd0-8dc9-fd11-bdca-e2d1ee3bf987",
                "code": "SKTBKWBN-S",
            },
            {
                "id": None,
                "oid": "d0386c10-a8de-ed5c-a29e-5e40756711b2",
                "code": "SKTFTRFLBT-S",
            },
        ],
        "one_number": 10298598,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Fail",
        "contracts_failed": "PIECE_WITHIN_PRINTED_ROLL_REGION",
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "5579da65-77dd-a452-6355-c880ebfdf859",
        "pieces": [
            {
                "id": None,
                "oid": "684faad1-eda7-f9b5-1f80-5efc02f4d8ba",
                "code": "PNTFTPKBLF-C",
            },
            {
                "id": None,
                "oid": "30a20b22-6cd3-38b8-7dc1-e1027ca1382e",
                "code": "PNTFTPKBRT-C",
            },
        ],
        "one_number": 10298622,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "6330b24d-1e3a-c82f-21a4-66f3d2505371",
        "pieces": [
            {
                "id": None,
                "oid": "037b819b-414f-4107-a5b7-5e7710fcb05a",
                "code": "DRSSLPNLRT-S",
            },
            {
                "id": None,
                "oid": "17fa8f20-dd87-bfc9-672e-41d7729ceb80",
                "code": "DRSBKYKETP-S",
            },
            {
                "id": None,
                "oid": "d8ee4cda-591f-ca8e-295a-81bc0c28c6f1",
                "code": "DRSSDPKBLFTP-S",
            },
            {
                "id": None,
                "oid": "a6bd2ecb-3aaa-f86c-acb0-cc89025ae6f8",
                "code": "DRSSLPNLLF-S",
            },
            {
                "id": None,
                "oid": "e279b6b1-127e-f2f5-a4c5-dc9c12401a09",
                "code": "DRSSDPKBRTUN-S",
            },
            {
                "id": None,
                "oid": "876ac3e5-5ed6-040c-ebc9-6c06198e8b8e",
                "code": "DRSWTBLT-S",
            },
            {
                "id": None,
                "oid": "9577c43c-0273-4612-8205-95b32f928148",
                "code": "DRSSDPKBRTTP-S",
            },
            {
                "id": None,
                "oid": "d6606d8a-d4cd-3a3a-ecf3-cf8149c8ac60",
                "code": "DRSFTPNLRT-S",
            },
            {
                "id": None,
                "oid": "e5941a4c-7d89-48d3-73bc-8888203bfb97",
                "code": "DRSBKPNLRT-S",
            },
            {
                "id": None,
                "oid": "f0ef9015-0881-0c01-4026-6563647c323a",
                "code": "DRSSDPKBLFUN-S",
            },
            {
                "id": None,
                "oid": "0a824528-107b-3c75-7836-342a5f2a9900",
                "code": "DRSBKYKEUN-S",
            },
            {
                "id": None,
                "oid": "0a240a13-8847-3559-3718-54ec180b3d4a",
                "code": "DRSFTPNLLF-S",
            },
            {
                "id": None,
                "oid": "06e7041c-bcfa-ec60-a629-7cef8664a8f5",
                "code": "DRSBKPNLLF-S",
            },
        ],
        "one_number": 10298630,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "224527d2-6f6d-88cb-ea43-0b217f04c63d",
        "pieces": [],
        "one_number": 10298638,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Fail",
        "contracts_failed": "PIECE_WITHIN_PRINTED_ROLL_REGION",
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "ca3e15f7-886b-31ba-8013-a4d97d3961c1",
        "pieces": [
            {
                "id": None,
                "oid": "25ebc648-4564-0b31-6a8b-00e683497447",
                "code": "DRSBKYKEUN-S",
            },
            {
                "id": None,
                "oid": "459720d2-67f3-3bb2-3190-8ce08bfee66e",
                "code": "DRSSLPNLRT-S",
            },
            {
                "id": None,
                "oid": "ac9c998d-c24b-129b-ea59-67d85f77f390",
                "code": "DRSWTBLT-S",
            },
            {
                "id": None,
                "oid": "4975a622-34c1-4322-0456-de31c77b2a99",
                "code": "DRSSDPKBRTTP-S",
            },
            {
                "id": None,
                "oid": "b1180a27-97f3-a19d-cd22-2c8816632d4e",
                "code": "DRSFTPNLLF-S",
            },
            {
                "id": None,
                "oid": "6ca9b7c9-42c8-264c-80cc-0eed9ae5a949",
                "code": "DRSSDPKBRTUN-S",
            },
            {
                "id": None,
                "oid": "3b07482d-deba-d983-4546-7a8dcec1f63d",
                "code": "DRSFTPNLRT-S",
            },
            {
                "id": None,
                "oid": "5d0361be-c67b-b491-f643-d720c36e815b",
                "code": "DRSBKYKETP-S",
            },
            {
                "id": None,
                "oid": "8047aafb-30d6-b40e-a919-2549f62584a2",
                "code": "DRSSLPNLLF-S",
            },
            {
                "id": None,
                "oid": "b3a184d1-32be-4e88-5bde-1aac12cb3788",
                "code": "DRSBKPNLRT-S",
            },
            {
                "id": None,
                "oid": "d8fabad6-19e0-347d-e62d-3ae013334631",
                "code": "DRSBKPNLLF-S",
            },
            {
                "id": None,
                "oid": "7fe4ec9f-adc1-d593-dc73-66b4fb36933b",
                "code": "DRSSDPKBLFUN-S",
            },
            {
                "id": None,
                "oid": "3cccc7d9-b776-c92e-1485-02d08f797517",
                "code": "DRSSDPKBLFTP-S",
            },
        ],
        "one_number": 10298639,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "1d596d3b-d431-cd2d-c70d-d6e7a5ee6d86",
        "pieces": [
            {
                "id": None,
                "oid": "eddb90d8-0d87-cdee-6cbc-0c3f129e5fc6",
                "code": "PNTFTPKBRT-C",
            }
        ],
        "one_number": 10298643,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "283e8429-4b60-d9c1-69c3-36029d6121a7",
        "pieces": [
            {
                "id": None,
                "oid": "eddb90d8-0d87-cdee-6cbc-0c3f129e5fc6",
                "code": "PNTFTPKBRT-C",
            }
        ],
        "one_number": 10298643,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Fail",
        "contracts_failed": "PIECE_WITHIN_PRINTED_ROLL_REGION",
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "fd8c5507-13ca-e940-60c3-c1bc46684b05",
        "pieces": [
            {
                "id": None,
                "oid": "ce42bda5-c13a-09f0-bbbe-02e3f2730df5",
                "code": "PNTFTPKBLF-C",
            },
            {
                "id": None,
                "oid": "f91bf805-6485-00f0-d99c-2262466c6bff",
                "code": "PNTFTPKBRT-C",
            },
        ],
        "one_number": 10298655,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "a9b88d80-a010-4ed0-2c5e-b078186ac060",
        "pieces": [
            {
                "id": None,
                "oid": "30468e11-cc5b-b4af-3355-14488c145e11",
                "code": "TOPBYPNL-S",
            }
        ],
        "one_number": 10298663,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "a01ee88b-84f8-d73c-bfc1-93ea45335989",
        "pieces": [
            {
                "id": None,
                "oid": "30468e11-cc5b-b4af-3355-14488c145e11",
                "code": "TOPBYPNL-S",
            }
        ],
        "one_number": 10298663,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Fail",
        "contracts_failed": "PIECE_WITHIN_PRINTED_ROLL_REGION",
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
    {
        "id": "2ada6b37-5890-274d-3816-f76a207896b8",
        "pieces": [
            {
                "id": None,
                "oid": "afb79467-ffd5-f932-919f-e0635dca9107",
                "code": "DRSFTPNLLF-S",
            },
            {
                "id": None,
                "oid": "9c494ea8-cb21-7b48-1d0f-912f37424168",
                "code": "DRSFTPKBLFTP-S",
            },
            {
                "id": None,
                "oid": "b6b9556c-eb30-5a94-f273-f25586419fb1",
                "code": "DRSBKYKEUN-S",
            },
            {
                "id": None,
                "oid": "f74808f9-e0bb-b58d-cb61-fda4989135f4",
                "code": "DRSSLPNLUNLF-S",
            },
            {
                "id": None,
                "oid": "0e8c495d-c735-3fff-532a-584adcbb2506",
                "code": "DRSFTPKBRTUN-S",
            },
            {
                "id": None,
                "oid": "dbcd3007-77a9-8654-1e67-3578b0d869b4",
                "code": "DRSFTCHPRT-S",
            },
            {
                "id": None,
                "oid": "ccaed531-ff93-5f65-fcf1-c639e129409d",
                "code": "DRSBKYKETP-S",
            },
            {
                "id": None,
                "oid": "6407e920-bce2-c971-e749-84395ab6fc31",
                "code": "DRSFTCHPLF-S",
            },
            {
                "id": None,
                "oid": "75eec095-2901-6870-d025-4593ed379618",
                "code": "DRSBSPNLLF-S",
            },
            {
                "id": None,
                "oid": "63b8247e-4eb3-db6a-750e-219aff798840",
                "code": "DRSFTPKBRTTP-S",
            },
            {
                "id": None,
                "oid": "4ba6787a-45ad-63fd-84b5-108d38791c68",
                "code": "DRSBSPNLRT-S",
            },
            {
                "id": None,
                "oid": "e644c618-9a06-e6a5-0a2f-cd106d15aced",
                "code": "DRSSLPNLUNRT-S",
            },
            {
                "id": None,
                "oid": "1ee0ed12-64a3-9fd9-5712-a29de447e49f",
                "code": "DRSBKPNL-S",
            },
            {
                "id": None,
                "oid": "459e8551-375b-56d6-5e57-0fa91422329d",
                "code": "DRSFTPNLRT-S",
            },
            {
                "id": None,
                "oid": "92e72847-8ef6-764a-2b57-193ffe706ed4",
                "code": "DRSSLPNLTPLF-S",
            },
            {
                "id": None,
                "oid": "667d4daa-2bab-01e5-5c7c-bfe121367b09",
                "code": "DRSSLPNLTPRT-S",
            },
            {
                "id": None,
                "oid": "8256320c-de5b-0807-3bd9-2ba5d1372295",
                "code": "DRSFTPKBLFUN-S",
            },
        ],
        "one_number": 10298665,
        "one_code": None,
        "node": "Make.Print.Printer",
        "status": "Exit",
        "contracts_failed": [],
        "defects": [],
        "observed_at": "2023-03-15T17:29:05",
        "observation_confidence": None,
        "metadata": {
            "printjob_name": "CTW70_R39084_0_001_CTW70_1678908109",
            "print_file_id": "df163d60-6681-4839-8f0f-0d12e1c516d2",
        },
    },
]
