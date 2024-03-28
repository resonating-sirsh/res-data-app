from schemas.pydantic.premises import RfidEventResponse
from datetime import datetime
from pyairtable import Api
from res.utils import logger, secrets
import json
import arrow
from tenacity import retry, wait_fixed, stop_after_attempt

AIRTABLE_API_KEY = secrets.secrets_client.get_secret("AIRTABLE_API_KEY")
airtable = Api(AIRTABLE_API_KEY)
cnx_rfid_channels = airtable.table("appyxEVba0zT38PP7", "tbljyK1ngi4dQ479v")
cnx_rfid_tracking = airtable.table("appyxEVba0zT38PP7", "tblcLo3gkFZlfDR3J")
cnx_print_rolls = airtable.table("apprcULXTWu33KFsh", "tblSYU8a71UgbQve4")
cnx_ppu_scheduling = airtable.table("appHqaUQOkRD8VbzX", "tblNG5tFbNppt2fFL")


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3))
def call_airtable(cnx, *args, method="all", **kwargs):
    return getattr(cnx, method)(*args, **kwargs)


def handle_roll_rfid_event(event: dict) -> dict:
    channel_data = cnx_rfid_channels.all(
        formula=f"AND(key='{event['reader']} - Channel {event['channel']}')"
    )[0]

    item_data = get_item_data(event["tag_epc"])

    if item_data:
        other_tag = next(
            tag
            for tag in item_data["fields"]["rfid_tags"].split(", ")
            if tag != event["tag_epc"]
        )

        item_state = json.loads(item_data["fields"]["item_state"])
        roll_to_process = get_record(item_data["fields"]["tagged_item"])
        channel_substation = get_channel_substaiton(channel_data, roll_to_process)

        for substation in item_state:
            if substation.get("Substation") == channel_substation:
                if len(substation) == 1:
                    substation.update(
                        {
                            f"{event['tag_epc']} - Entry": "",
                            f"{event['tag_epc']} - Exit": "",
                            f"{other_tag} - Entry": "",
                            f"{other_tag} - Exit": "",
                        }
                    )

                    substation[
                        f"{event['tag_epc']} - {channel_data['fields']['location'].split(' ')[-1]}"
                    ] = "True"

                    call_airtable(
                        cnx_rfid_tracking,
                        item_data["id"],
                        {"item_state": item_state},
                        typecast=True,
                        method="update",
                    )

                    rifd_rolls_logic(
                        substation,
                        roll_to_process,
                        channel_substation,
                        event["observed_at"],
                    )

                    break

                elif not (
                    substation[
                        f"{event['tag_epc']} - {channel_data['fields']['location'].split(' ')[-1]}"
                    ]
                ):
                    substation[
                        f"{event['tag_epc']} - {channel_data['fields']['location'].split(' ')[-1]}"
                    ] = "True"

                    call_airtable(
                        cnx_rfid_tracking,
                        item_data["id"],
                        {"item_state": item_state},
                        typecast=True,
                        method="update",
                    )

                    rifd_rolls_logic(
                        substation,
                        roll_to_process,
                        channel_substation,
                        event["observed_at"],
                    )
                    break

                else:
                    logger.info("State already registered")
                    return False

        response = create_response(event, channel_data, item_data)
        return response


def get_item_data(epc):
    records = cnx_rfid_tracking.all(formula=f"IF(SEARCH('{epc}', rfid_tags), 1)")
    # There's a chance that the tag was used in the past and not removed on platform, so we take the latest created
    try:
        latest_record = max(
            records,
            key=lambda x: datetime.fromisoformat(
                x["fields"]["created_at"].removesuffix("Z")
            ),
        )
        return latest_record
    except:
        return False


def get_record(roll_id):
    record = cnx_print_rolls.all(formula=f"AND({'_roll_id'}='{roll_id}')")
    return record[0]


def get_channel_substaiton(channel_data, roll_to_process):
    if len(channel_data["fields"]["Substation"]) == 1:
        return channel_data["fields"]["Substation"][0]

    else:
        # Doing this because roll can be on one substation on platform but physically on another
        if roll_to_process["fields"]["Current Substation"] in [
            "Material Prep",
            "Pretreatment",
            "Print",
        ]:
            return "Pretreatment"
        elif roll_to_process["fields"]["Current Substation"] in [
            "Wash",
            "Dry",
        ]:
            return "Dry"


def rifd_rolls_logic(
    substation_state, roll_to_process, channel_substation, observed_at
):
    tag_states = []
    for state, value in substation_state.items():
        if state == "Substation":
            continue

        if value == "True":
            tag_states.append(True)
        else:
            tag_states.append(False)

    t1_entry, t1_exit, t2_entry, t2_exit = tag_states

    if tag_states.count(True) == 1:
        if channel_substation != "Print" and channel_substation != "Roll Inspection 1":
            call_airtable(
                cnx_print_rolls,
                roll_to_process["id"],
                {
                    f"{channel_substation} Queue": "Processing",
                    f"{channel_substation} Enter Date At": observed_at,
                },
                typecast=True,
                method="update",
            )

        else:
            call_airtable(
                cnx_print_rolls,
                roll_to_process["id"],
                {f"{channel_substation} Enter Date At": observed_at},
                typecast=True,
                method="update",
            )

    # This was a formula we got using logic tables
    elif t2_exit and (t1_entry and not t1_exit or t1_exit):
        if channel_substation != "Print" and channel_substation != "Roll Inspection 1":
            call_airtable(
                cnx_print_rolls,
                roll_to_process["id"],
                {
                    f"{channel_substation} Queue": "Done",
                    f"{channel_substation} Exit Date At": observed_at,
                },
                typecast=True,
                method="update",
            )
            ppu_schedule_validator(roll_to_process, channel_substation)

        else:
            call_airtable(
                cnx_print_rolls,
                roll_to_process["id"],
                {f"{channel_substation} Exit Date At": observed_at},
                typecast=True,
                method="update",
            )
            ppu_schedule_validator(roll_to_process, channel_substation)

        logger.info(
            f"{roll_to_process['fields']['_roll_id']} finished {roll_to_process['fields']['Current Substation']}"
        )


def ppu_schedule_validator(roll_to_process, channel_substation):
    ppu_rolls = cnx_print_rolls.all(
        formula=f"AND({{Assigned PPU Name}}='{roll_to_process['fields']['Assigned PPU Name'][0]}')",
    )

    earliest_enter_date, latest_exit_date = rolls_completed_substation(
        ppu_rolls, channel_substation
    )

    if earliest_enter_date and latest_exit_date:
        assigned_ppu_name = roll_to_process["fields"]["Assigned PPU Name"][0]

        if "Roll Inspection" in channel_substation:
            channel_substation = "Roll Inspection"

        if channel_substation != "Print":
            ppu_to_process = cnx_ppu_scheduling.all(
                formula=f"AND(SEARCH('{channel_substation}', __scheduling), FIND('{assigned_ppu_name}', Name), LEN(Name)= FIND('{assigned_ppu_name}', Name) + LEN('{assigned_ppu_name}') - 1)"
            )[0]

            call_airtable(
                cnx_ppu_scheduling,
                ppu_to_process["id"],
                {
                    "Actual Start Time": earliest_enter_date,
                    "Actual Complete Time": latest_exit_date,
                },
                typecast=True,
                method="update",
            )
            logger.info(
                f"{roll_to_process['fields']['Assigned PPU Name'][0]} actual times have been updated"
            )

        else:
            ppu_to_process = cnx_ppu_scheduling.all(
                formula=f"AND(SEARCH('{channel_substation}', {{Asset Type}}), FIND('{assigned_ppu_name}', Name), LEN(Name)= FIND('{assigned_ppu_name}', Name) + LEN('{assigned_ppu_name}') - 1)"
            )[0]

            call_airtable(
                cnx_ppu_scheduling,
                ppu_to_process["id"],
                {
                    "Actual Start Time": earliest_enter_date,
                    "Actual Complete Time": latest_exit_date,
                },
                typecast=True,
                method="update",
            )
            logger.info(
                f"{roll_to_process['fields']['Assigned PPU Name'][0]} actual times have been updated"
            )

    else:
        print("PPU has not finished substation")


def rolls_completed_substation(ppu_rolls, channel_substation):
    if all(
        f"{channel_substation} Enter Date At" in roll["fields"]
        and f"{channel_substation} Exit Date At" in roll["fields"]
        for roll in ppu_rolls
    ):
        earliest_enter_date = None
        latest_exit_date = None

        for roll in ppu_rolls:
            enter_date = arrow.get(roll["fields"][f"{channel_substation} Enter Date At"])  # fmt: skip
            exit_date = arrow.get(roll["fields"][f"{channel_substation} Exit Date At"])

            if earliest_enter_date is None or enter_date < earliest_enter_date:
                earliest_enter_date = enter_date

            if latest_exit_date is None or exit_date > latest_exit_date:
                latest_exit_date = exit_date

        return earliest_enter_date.isoformat(), latest_exit_date.isoformat()

    else:
        return None, None


def create_response(event, channel_data, item_data):
    response = RfidEventResponse(
        **{
            "id": event["id"],
            "event": channel_data["fields"]["location"],
            "observed_at": event["observed_at"],
            "observed_item": {
                item_data["fields"]["item_type"]: item_data["fields"]["tagged_item"]
            },
            "metadata": {
                "rfid_tags": item_data["fields"]["rfid_tags"],
                "flows": channel_data["fields"]["associated_flows"],
                "item_state": item_data["fields"]["item_state"],
            },
        }
    )

    return response
