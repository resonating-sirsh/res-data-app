from datetime import datetime
from res.flows import flow_node_attributes, FlowContext
from res.learn.optimization.schedule.cut_timer import ScheduleManager
import res

PING_CHANNEL = "assemble_node"
ANDREW_ERSKINE_PING = "<@U06714XME3Y>"


@flow_node_attributes(
    "schedule_cuts.handler",
    cpu="7500m",
    memory="4G",
    slack_channel="assemble_node",
    slack_message=f"<@U06714XME3Y> Cut scheduling failed",
    mapped=False,
)
def handler(event, context):
    with FlowContext(event, context) as fc:
        slack = res.connectors.load("slack")
        try:
            # schedule for this day
            today = datetime.now()
            # an instance of this class pulls data and creates the best schedule
            schedule_manager = ScheduleManager(
                today, person_to_ping=ANDREW_ERSKINE_PING
            )
            loaded_to_airtable = schedule_manager.load_to_airtable()
            if loaded_to_airtable:
                schedule_manager.post_to_slack(PING_CHANNEL)
            else:
                raise Exception(
                    f"Failed to load to airtable for {today.strftime('%Y-%m-%d')}"
                )
            loaded_pieces_to_airtable = schedule_manager.load_piece_timing_to_airtable()
            if not loaded_pieces_to_airtable:
                raise Exception(
                    f"Failed to load piece timing to airtable for {today.strftime('%Y-%m-%d')}"
                )
        except Exception as ex:
            slack.post_message(
                f"Error in scheduling cuts: {repr(ex)} - {ANDREW_ERSKINE_PING}",
                PING_CHANNEL,
            )
            raise
