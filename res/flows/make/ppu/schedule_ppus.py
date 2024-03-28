from res.flows import flow_node_attributes, FlowContext
from res.learn.optimization.schedule.ppu import schedule_ppus, assign_ppus_to_printers
from slack_sdk import WebClient
from res.utils.secrets import secrets

PING_CHANNEL = "autobots"
SCHED_CHANNEL = "material_node"


@flow_node_attributes(
    "schedule_ppus.handler",
    cpu="7500m",
    memory="4G",
    slack_channel="autobots",
    slack_message="<@U0361U4B84X> PPU scheduling failed",
    mapped=False,
)
def handler(event, context):
    with FlowContext(event, context) as fc:
        client = WebClient(token=secrets.get_secret("SLACK_OPTIMUS_BOT_TOKEN"))
        try:
            client.chat_postMessage(
                channel=PING_CHANNEL,
                text="Started constructing PPU Schedule",
            )
            scheduled, summary = schedule_ppus(
                plot_filename="ppu_sched.png",
                # active_printers=["luna", "flash"],
            )
            if scheduled:
                client.files_upload(
                    channels=SCHED_CHANNEL,
                    title="PPU Schedule",
                    file="ppu_sched.png",
                )
                client.chat_postMessage(
                    channel=SCHED_CHANNEL,
                    text=summary,
                )
            client.chat_postMessage(
                channel=PING_CHANNEL,
                text=summary,
            )
        except Exception as ex:
            client.chat_postMessage(
                channel=PING_CHANNEL,
                text=f"Failed to schedule PPUs: {repr(ex)}",
            )
            raise


def assign_ppus(event, context):
    assign_ppus_to_printers()
