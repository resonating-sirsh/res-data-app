from res.connectors.dgraph.utils import *
from res.flows import FlowContext
import os
from pathlib import Path

# we dont have to sync airtable every time, we could check elapsed time but update dgraph more often
# this strategy means in future we could have a deployment of continuous processes writing into dgraph
# and we could use a redis layer on the record id lookups which means we know when we are in create mode
# it may be we know when inserting into dgraph if the record is new or not > when we search airtable by key we can remove duplicates if they exist

STANDARD_AIRTABLE_FIELDS = ["Status", "Notes", "Tags"]
RES_ENV = os.environ.get("RES_ENV", "development")


def determine_record_id_map_and_add_dups_to_purge_list(data, purge_list):
    """ """
    purge_list = purge_list or []
    map_records = {}
    return map_records, purge_list


@res.flows.flow_node_attributes(
    memory="12Gi",
)
def handler(event, context={}):
    """
    Using
    """

    with FlowContext(event, context) as fc:
        WATERMARK = fc.args.get("watermark_minutes", 20)
        # go back to the last run time approx 10 minutes - does not need to be exact - pick up any recent purges
        watermark = res.utils.dates.minutes_ago(WATERMARK).isoformat()

        res.utils.logger.info(f"sync with change watermark {watermark}")

        graph_config = str(
            Path(res.utils.get_res_root()) / "res-schemas" / "res" / "topic_graph.yaml"
        )
        res.utils.logger.info(f"Loading config from {graph_config}")
        graph_config = res.utils.read(graph_config)
        graph_config = GraphModel(graph_config)
        name = fc.args["name"]
        # we can set this to -1 to iterate continuously
        consume_batch_size = fc.args.get("consume_batch_size", 1000)
        # need a unit test or config test to make sure keys are respected
        schema_details = dict(graph_config.schema_table.loc[name])
        # what is the consumer name
        kafka = fc.connectors["kafka"]
        kafka_topic = fc.args["topic_name"]
        key = graph_config[name]["key"]
        alias = graph_config[name]["alias"]

        res.utils.logger.info(f"Checking airtable schema for {name}")

        t, fields = graph_config.sync_airtable_schema_from_graph_config(
            name, plan=False
        )

        # queues.get_changes() -> this just manages the hasura logic
        assets = get_changes(alias, modified_after=watermark, expanded=True)

        res.utils.logger.info(
            f"fetched back {len(assets)} relevant changes for airtable update"
        )

        # queues.sync_airtable_data
        stats = sync_airtable_data(graph_config, name, assets)

        if len(stats):
            node_path = f"{fc.get_node_path_for_key('airtable_sync', key=fc.key)}/{name}/{fc.key}/sync_stats.feather"
            res.utils.logger.info(f"Writing stats to {node_path}")
            fc.connectors["s3"].write(node_path, stats)

        # in this mode we do actually not have a continuous iterator

        fc.metric_incr(
            verb="AIR_SYNCED",
            group=name,
            status="OK",
        )

    return {}


@res.flows.flow_node_attributes(
    memory="12Gi",
)
def handler_dgraph(event, context={}):
    """
    Pulls events from kafka and syncs them with airtable via dgraph

    simple  test payload stuff / partial print asset

    - demonstrate flat without uids
    - demonstrate with uids
    - isolate test of airtable sync

    args {
        "name" : "Avro Type" ,
        "invert_parent_child": None,
        "topic_name" : "kafka_topic",
        "skip_airtable_sync" : True
    }

    example

        'args': {
            'name': 'create_response',
            'topic_name': 'res_make.make_one_request.create_response',
            'referenced_by_type_count': 0,
            'invert_parent_child': False
        }


    """
    # todo configure kafka topics

    with FlowContext(event, context) as fc:
        WATERMARK = fc.args.get("watermark_minutes", 20)
        # go back to the last run time approx 10 minutes - does not need to be exact - pick up any recent purges
        watermark = res.utils.dates.minutes_ago(WATERMARK).isoformat()

        res.utils.logger.info(f"sync with change watermark {watermark}")

        graph_config = str(
            Path(res.utils.get_res_root()) / "res-schemas" / "res" / "topic_graph.yaml"
        )
        res.utils.logger.info(f"Loading config from {graph_config}")
        graph_config = res.utils.read(graph_config)
        graph_config = GraphModel(graph_config)
        name = fc.args["name"]
        # we can set this to -1 to iterate continuously
        consume_batch_size = fc.args.get("consume_batch_size", 1000)
        # need a unit test or config test to make sure keys are respected
        schema_details = dict(graph_config.schema_table.loc[name])
        # what is the consumer name
        kafka = fc.connectors["kafka"]
        kafka_topic = fc.args["topic_name"]
        key = graph_config[name]["key"]
        alias = graph_config[name]["alias"]

        res.utils.logger.info(f"Checking airtable schema for {name}")

        t, fields = graph_config.sync_airtable_schema_from_graph_config(
            name, plan=False
        )

        # TODO if we have a parent we should also update that schema

        res.utils.logger.debug(
            f"For name: {name} Consuming from topic if exists: {kafka_topic} - or payload will be used if supplied"
        )

        # we could stream as an iterator if we say consume(batch_size=-1)
        assets = (
            fc.assets
            if event.get("assets")
            else kafka[kafka_topic]
            .consume(give_up_after_records=consume_batch_size, errors="ignore")
            .to_dict("records")
        )

        responses = []
        # this rule for now -> if we are referenced by some type we are a child entry in the current scheme
        invert_parent_child = schema_details.get(
            "invert_parent_child", schema_details["referenced_by_type_count"] != 0
        )

        res.utils.logger.debug(
            f"child inversion is: {invert_parent_child} - asset count is {len(assets)}"
        )

        # 1. First insert what we have into dgraph
        # 2 later thing about metrics and exception handling for this - bug or connectivity failure or timeout
        for asset in assets:
            res.utils.logger.debug(
                f"Upserting for {key} on kafka object {name} -> {alias}"
            )
            gc = select_subgraph(graph_config, kafka_topic)
            if not invert_parent_child:
                # we look up the avro type in our schema and find what this is in draph to upsert
                response = upsert(asset, alias, graph_config=gc)
                # dont collect this if we are in batch
                if consume_batch_size != -1:
                    responses.append(response)
            else:
                # we inspect the config to find for this topic what we should map
                # we are looking for dgraph aliases and also the field for inversion eg child_list from parent.child_list
                inv_kwargs = inversion_kwargs_from_avro_type(name, graph_config)
                _responses = upsert_with_parent_child_inversion(
                    asset,
                    **inv_kwargs,
                    graph_config=gc,
                )
                if consume_batch_size != -1:
                    responses += _responses

        # 2 now make the batch airtable sync with all our changes -
        # there is an argument for doing this in the reducer which we can do separately but we would need to know whats new in dgraph
        # turn OFF for now and do in the reducer once per table
        if not fc.args.get("skip_airtable_sync", True):
            try:

                # whatever we inserted may be inserts or updates and may have purged dates added to the set - get back with all the meta data - we could be more efficient here
                assets = get_changes(alias, modified_after=watermark, expanded=True)

                res.utils.logger.info(
                    f"fetched back {len(assets)} relevant changes for airtable update"
                )

                stats = sync_airtable_data(graph_config, name, assets)
                if len(stats):
                    node_path = f"{fc.get_node_path_for_key('airtable_sync', key=fc.key)}/{name}/{fc.key}/sync_stats.feather"
                    res.utils.logger.info(f"Writing stats to {node_path}")
                    fc.connectors["s3"].write(node_path, stats)
                # in this mode we do actually not have a continuous iterator
                fc.metric_incr(
                    verb="AIR_SYNCED",
                    group=name,
                    status="OK",
                )
            except Exception as ex:
                res.utils.logger.warn(
                    f"failed when updating airtable {res.utils.ex_repr(ex)}"
                )
                fc.metric_incr(
                    verb="AIR_SYNCED",
                    group=name,
                    status="FAILED",
                )
        else:
            res.utils.logger.info(
                f"skipped the airtable update based on config: skip_airtable_sync=True"
            )
        return responses


def generator(event, context={}):
    # map over the graph for our configured topics and kick off pulse
    # we choose distinct topics and look for conflicts
    # by default we use the child types for saving when there is a parent child - warn if its ambiguous
    # if we use the default, the parent child relationship is inverted in the upserts
    # in this version
    with FlowContext(event, context) as fc:
        config = str(
            Path(res.utils.get_res_root()) / "res-schemas" / "res" / "topic_graph.yaml"
        )
        res.utils.logger.info(f"Loading config from {config}")
        g = GraphModel(res.utils.read(config))
        assets = (
            g.schema_table.drop_duplicates(subset=["name"], keep="last")
            .reset_index()
            .rename(columns={"name": "topic_name", "index": "name"})
        )

        assets["invert_parent_child"] = assets["referenced_by_type_count"].map(
            lambda x: x != 0
        )
        assets = assets[
            ["name", "topic_name", "referenced_by_type_count", "invert_parent_child"]
        ].to_dict("records")

        assets = fc.asset_list_to_configured_payload(assets)

        return assets


def reducer(event, context={}):
    """
    There is the option do a sequential update of airtable in the reducer.... ...
    """

    with FlowContext(event, context) as fc:
        WATERMARK = fc.args.get("watermark_minutes", 20)
        # go back to the last run time approx 10 minutes - does not need to be exact - pick up any recent purges
        watermark = res.utils.dates.minutes_ago(WATERMARK).isoformat()

        graph_config = str(
            Path(res.utils.get_res_root()) / "res-schemas" / "res" / "topic_graph.yaml"
        )
        res.utils.logger.info(f"Loading config from {graph_config}")
        graph_config = res.utils.read(graph_config)
        graph_config = GraphModel(graph_config)

        # do we want to determine changes by node since we started this session

        attempted_aliases = []

        for name in graph_config._loaded_topics.keys():
            key = graph_config[name]["key"]
            alias = graph_config[name]["alias"]

            try:
                res.utils.logger.info(
                    f"syncing schema for airtable using config {name}"
                )
                t, fields = graph_config.sync_airtable_schema_from_graph_config(
                    name, plan=False
                )
            except Exception as ex:
                res.utils.logger.warn(f"Unable to sync schema {res.utils.ex_repr(ex)}")

            # we only need to visit the alias once
            if alias in attempted_aliases:
                continue

            assets = get_changes(alias, modified_after=watermark, expanded=True)

            res.utils.logger.info(
                f"Alias {alias} has {len(assets)} changes. Syncing with airtable"
            )

            try:
                attempted_aliases.append(alias)
                # here we are not checking the remote first because its cheaper
                # we would check the remote if the user could change something in a real queue and these tables should be smaller and more manageable
                # we might also check if we felt the tables were out of sync e.g. we have the incorrect record ids that could create duplicates or failure to update because the record is not remote
                # for the former case we need the true user updated list which would be small
                stats = sync_airtable_data(
                    graph_config,
                    name,
                    assets,
                    check_remote_first=False,
                    write_back_changes=False,
                )
                if len(stats):
                    node_path = f"{fc.get_node_path_for_key('airtable_sync', key=fc.key)}/{name}/sync_stats.feather"
                    res.utils.logger.info(f"Writing stats to {node_path}")
                    fc.connectors["s3"].write(node_path, stats)
                # in this mode we do actually not have a continuous iterator
                fc.metric_incr(
                    verb="AIR_SYNCED",
                    group=name,
                    status="OK",
                )
            except Exception as ex:
                res.utils.logger.warn(
                    f"failed when updating airtable {res.utils.ex_repr(ex)}"
                )
                fc.metric_incr(
                    verb="AIR_SYNCED",
                    group=name,
                    status="FAILED",
                )

        return {}


EXAMPLE_ASSET = {}
