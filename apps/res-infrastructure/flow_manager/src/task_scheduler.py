import os

import pandas as pd

# https://apscheduler.readthedocs.io/en/3.x/userguide.html#basic-concepts
from apscheduler.schedulers.blocking import BlockingScheduler

# AND example https://stackoverflow.com/questions/27110586/apscheduler-at-90-second-intervals
from apscheduler.triggers.cron import CronTrigger

import res
from res.flows import FlowEventProcessor
from res.utils import post_with_token

REDIS = os.environ.get("REDIS_HOST", "localhost")
RES_CONNECT = os.environ.get(
    "RES_CONNECT_URL", "https://datadev.resmagic.io/res-connect"
)
ENV = os.environ.get("RES_ENV", "development")
EVERY_FIVE_MINUTES = CronTrigger(
    start_date="2022-1-1", minute="*/5", timezone="US/Eastern"
)

# .... ... ....


def run_task(event):
    try:
        argo = res.connectors.load("argo")
        event = dict(event)
        # we trust the name generated in the template
        # we add a qualifier for this session and we replace directly in the event which will be used in keys
        name = event["task"]["key"]
        # idempotent ensure that we have a token of three parts with our own qualifier
        name = "-".join(name.split("-")[:2])
        event["task"]["key"] = name + f"-{res.utils.res_hash()}".lower()
        job_name = event.get("task", {}).get("key")
        res.utils.logger.info(f"SCHEDULED PAYLOAD {event}")
        # argo on this image needs to know about the module - so need to think about dynamic tasks -or it could be an image tag thing
        argo.handle_event(
            event,
            context={},
            template="res-flow-node",
            unique_job_name=job_name,
            compressed="pack-one" in name,
        )
    except Exception as ex:
        res.utils.logger.error(f"Failed to schedule flow {name}: {repr(ex)}")


scheduler = BlockingScheduler()
# - rh: stop using redis because it doesnt actually empty out when we call remove_all_jobs
# scheduler.add_jobstore("redis", host=REDIS, port=6379)
# for testing we want to purge - then we need to develop a system for management
res.utils.logger.info(
    f"Will clear existing jobs as we start the process. Adding back from source and config."
)
scheduler.remove_all_jobs()


def _sync_tasks(df):
    existing = scheduler.get_jobs()
    # do some checks ^
    for _, row in df.iterrows():
        row = dict(row)
        my_env = row.get("envs", [])
        if ENV not in my_env:
            res.utils.logger.warn(f"Skipping scheduled of {row} in env {ENV}")
            continue
        hour = row.get("hour")
        day = row.get("day")
        minute = row.get("minute")
        day_of_week = row.get("day_of_week")

        if pd.isnull(hour):
            hour = None
        if pd.isnull(minute):
            minute = None
        if pd.isnull(day):
            day = None
        if pd.isnull(day_of_week):
            day_of_week = None

        # check are we in envs otherwise skip...

        res.utils.logger.info(f"adding scheduled tasks{row}")

        scheduler.add_job(
            run_task,
            CronTrigger(
                start_date="2022-1-1",
                hour=hour,
                minute=minute,
                day=day,
                day_of_week=day_of_week,
            ),
            id=row["id"],
            replace_existing=True,
            args=[row["payload"]],
        )


def sync_static_tasks_on_startup():
    """
    Source managed tasks can be mixed with dynamic ones

    This would allow is to configure some core ones here in source but also allow anyone to schedule more tasks
    and those other tasks could be manged in some tool
    """

    def payload(name, data_bind_memory=False, disk=None, task_key=None):
        e = FlowEventProcessor().make_sample_flow_payload_for_function(name)
        e["metadata"]["data_bind_memory"] = data_bind_memory

        if disk:
            e["metadata"]["disk_override"] = {"handler": disk}

        if task_key:
            e["task"]["key"] = task_key

        return e

    # some of these only change when people are working so we can restrict to times of day
    # nudge
    df = pd.DataFrame(
        [
            {
                "payload": payload(
                    "dxa.prep_ordered_pieces", data_bind_memory=True, disk="5G"
                ),
                "minute": "*/5",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # {
            #     "payload": payload("make.healing.healing_app_nests"),
            #     "minute": "*/30",
            #     "timezone": "US/Eastern",
            #     "envs": ["production"],
            # },
            {
                "payload": payload("infra.notifications.register_notifications"),
                "minute": "*/5",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # {
            #     "payload": payload("meta.marker"),
            #     "minute": "*/5",
            #     "timezone": "US/Eastern",
            #     "envs": ["development", "production"],
            # },
            {
                "payload": payload("meta.marker.handle_status_pings"),
                "hour": "*/1",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # we will handle the flush of previews and the flush of stuck GMOs...
            # {
            #     "payload": payload("meta.marker.retry_missing"),
            #     "hour": "*/1",
            #     "timezone": "US/Eastern",
            #     "envs": ["production"],
            # },
            # {
            #     "payload": payload("infra.pulse.KGraph"),
            #     "minute": "*/20",
            #     "timezone": "US/Eastern",
            #     "envs": ["development", "production"],
            # },
            {
                "payload": payload("etl.admin.contract_violation_report"),
                # 4utc
                "hour": 4,
                "timezone": "US/Eastern",
                "envs": ["development"],
            },
            {
                "payload": payload("infra.platform.status"),
                "hour": "*/23",
                "timezone": "US/Eastern",
                "envs": ["platform"],
            },
            {
                "payload": payload("make.nest.progressive.pack_one"),
                "minute": "*/5",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload("make.nest.progressive.retry_pack_one"),
                "minute": "30",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload("make.production.healing"),
                "hour": "*/1",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload("make.nest.progressive.prod_fabric_order"),
                # the timezone is a lie - if we specify hour X then the thing actually runs at hour X-4.
                "hour": "0",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                # Optimus run to actually allocate PPUs.
                "payload": {
                    **payload("make.nest.progressive.construct_rolls"),
                    "args": {
                        "time_limit_s": 3600 * 5,
                        "slack_channel": "make_one_team",
                        "supress_messages": True,
                        "max_solutions": 25,
                        "fill_allocations": False,  # this run is to create the allocations.
                    },
                },
                "hour": "4",  # 4 = midnight in the amazing python clock.
                "day_of_week": "mon-fri",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                # Optimus run to assign assets to allocated ppus.
                "payload": {
                    **payload("make.nest.progressive.construct_rolls"),
                    "args": {
                        "time_limit_s": 3600 * 5,
                        "slack_channel": "make_one_team",
                        "supress_messages": True,
                        "max_solutions": 25,
                        "fill_allocations": True,  # this run is to fill the allocations.
                    },
                },
                "hour": "16",  # this is noon on the day when they make the ppus -- we can push this later but need to remember the printer assignment cron or replace.
                "day_of_week": "mon-fri",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # {
            #     # Intermittent optimus runs so we can inspect the current solution.
            #     "payload": {
            #         **payload(
            #             "make.nest.progressive.construct_rolls",
            #             task_key="optimus-noassign",
            #         ),
            #         "args": {
            #             "time_limit_s": 3600 * 2,
            #             "slack_channel": "autobots",
            #             "supress_messages": True,
            #             "max_solutions": 10,
            #             "fill_allocations": True,
            #             "no_assign": True,
            #         },
            #     },
            #     "hour": "*/2",
            #     "timezone": "US/Eastern",
            #     "envs": ["production"],
            # },
            {
                "payload": payload("make.ppu.schedule_ppus"),
                "hour": "10",
                "day_of_week": "mon-fri",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload("make.cut.schedule_cuts"),
                "hour": "10",
                "minute": "30",
                "day_of_week": "mon-fri",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload("infra.platform.support_view_monitor"),
                "hour": "*/12",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # deprecate
            {
                "payload": payload("meta.construction.sew.handle_sew_requests"),
                "hour": "*/1",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # some of these tasks should be deprecated when he migrate 3d fully or hook up some events on s2 style changes
            # wait for a request to update a 2d asset - usually if the DXA queue or net file is modified/added
            # {
            #     "payload": payload("dxa.styles.asset_builder"),
            #     "minute": "*/5",
            #     "timezone": "US/Eastern",
            #     "envs": ["development"],
            # },
            {
                "payload": payload("dxa.setup_files.sync_changes"),
                "day": "*/1",
                "timezone": "US/Eastern",
                "envs": ["development"],
            },
            ###### general maint. task / bundles
            {
                "payload": payload("infra.platform.main_jobs.hourly"),
                "hour": "*/1",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload("infra.platform.main_jobs.daily"),
                "day": "*/1",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload("infra.platform.main_jobs.six_hourly"),
                "hour": "*/6",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # trivial change ... ... ... ...
            {
                "payload": payload("infra.platform.airtablemetadatacache_jobs"),
                "minute": "*/10",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload("infra.platform.dead_letter_retry"),
                "minute": "*/5",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload("make.production.piece_observations"),
                "minute": "*/5",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # print assets that were never requested @ PPP
            {
                "payload": {
                    **payload("dxa.prep_ordered_pieces"),
                    "args": {"ppp_from_pa": True},
                },
                "hour": "*/6",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # this only runs every 6 hours but its easy to force at any time if waiting
            {
                "payload": payload("make.one_journey.handler"),
                "hour": "*/1",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload(
                    "meta.ONE.style_node", data_bind_memory=True, disk="5G"
                ),
                "minute": "*/5",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload(
                    "meta.ONE.style_node.retry_generate_meta_one", data_bind_memory=True
                ),
                "hour": "*/12",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload("meta.ONE.body_node", data_bind_memory=True),
                "minute": "*/5",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                # after the body node kicks of the style regression test we can also back fill the ones we never did slowly
                "payload": payload("meta.ONE.style_piece_deltas.do_one"),
                "minute": "*/30",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload(
                    "meta.ONE.body_material_node", data_bind_memory=True
                ),
                "minute": "*/5",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload(
                    "meta.ONE.queries.stamper_handler", data_bind_memory=True
                ),
                "hour": "*/4",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # this is to backfill orders into hasura from warehouse - every 2 hours latest first ..
            {
                "payload": payload("res.flows.sell.orders.process.get_next_1500"),
                "hour": "*/6",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # {
            #     "payload": payload("make.production.utils.fill_make_orders_onenum"),
            #     "hour": "*/6",
            #     "timezone": "US/Eastern",
            #     "envs": ["production"],
            # },
            # switching to repair these properly ^
            {
                "payload": payload(
                    "res.flows.dxa.bertha.bertha_retry_non_user_failed_jobs"
                ),
                "hour": "*/2",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # {
            #     "payload": payload("make.production.utils.reconcile_ones_map"),
            #     "day": "*/6",
            #     "timezone": "US/Eastern",
            #     "envs": ["production"],
            # },
            # cache the mone state for use in other tools e.g. availability states on one
            # {
            #     "payload": payload("make.production.utils.cache_mone_state"),
            #     "hour": "*/1",
            #     "timezone": "US/Eastern",
            #     "envs": ["production"],
            # },
            # this is temp required to fill up our s3 request payloads for migration testing
            # here we take orders that we dont have request payloads for and store them
            # they do some useful things like look up ids etc to turn a fulfillment item into a make order
            # {
            #     "payload": payload(
            #         "res.flows.sell.orders.process.bulk_make_requests_backup"
            #     ),
            #     "hour": "*/6",
            #     "timezone": "US/Eastern",
            #     "envs": ["production"],
            # },
            # {
            #     "payload": payload("infra.platform.main_jobs.save_thumbnails"),
            #     "hour": "*/6",
            #     "timezone": "US/Eastern",
            #     "envs": ["production"],
            # },
            ########################
            ##the experimental thing to create set building queue by just looking at what is coming off the cut inspection queue and populating the data
            # this thing could do more work based on other things that can change e..g consume other kafka topic(s) data
            # {
            #     "payload": payload("make.set_building.cut_receipt"),
            #     "minute": "*/60",
            #     "timezone": "US/Eastern",
            #     "envs": ["development"],
            # },
            {
                "payload": payload(
                    "res.flows.infra.platform.support_view_monitor.get_and_aggregate"
                ),
                "hour": "*/12",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload("res.flows.infra.platform.observability.handler"),
                "hour": "*/12",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            {
                "payload": payload(
                    "infra.platform.observability.send_healing_status_update"
                ),
                "hour": "11",
                "timezone": "US/Eastern",
                "envs": ["production"],
            },
            # move into the daily after slack channel indexing
            # {
            #     "payload": payload(
            #         "res.flows.infra.platform.observability.the_slack_news"
            #     ),
            #     "day": "*/1",
            #     "timezone": "US/Eastern",
            #     "envs": ["production"], ##
            # },
        ]
    )
    df["id"] = df["payload"].map(
        lambda d: f"""{d["metadata"]["name"]}={hash(str(d))}"""
    )

    return _sync_tasks(df)


# some 5 minute decorator + run it on startup -> should ensure we have the
def sync_dynamic_tasks():
    """
    This is a future thing we could do if we want to configure some
    We can override static ones too if we want to - especially if we want to store interesting payloads
    """
    res.utils.logger.debug("Syncing dynamic tasks")
    return
    # df = FlowEventProcessor.load_dynamic_tasks()
    # return _sync_tasks(df)


sync_static_tasks_on_startup()

scheduler.add_job(
    sync_dynamic_tasks,
    EVERY_FIVE_MINUTES,
    id="core.sync_dynamic_tasks",
    replace_existing=True,
)
# start here ...

scheduler.add_job(
    lambda: post_with_token(
        "https://data.resmagic.io/make-one/retry_assembly_add",
        secret_name="RES_META_ONE_API_KEY",
    ),
    CronTrigger(start_date="2022-1-1", hour="*/1"),
    id="retry assembly add",
    replace_existing=True,
)
