"""
FlowAPI : simple interface to data pipelines driven by typed models
Data consumed from kafka is saved to database and airtable queues - thats it
We provide patterns to
- define mutations and data models
- define event schema at different stages
- allow understanding the node and traces etc
"""

from schemas.pydantic.common import FlowApiModel, UTCDatetime, Union
from pathlib import Path
from typing import TypeVar
import stringcase
from typing import Dict, TypeVar
import res
from res.connectors.airtable.AirtableUpdateQueue import AirtableQueue
from res.flows.api.upserts.flatteners import load_flattener
from res.flows.api.responses import KAFKA_RESPONSE_TOPICS

T = TypeVar("T")
F = TypeVar("F", bound=FlowApiModel)

USE_KGATEWAY = True


class FlowAPI:
    def __init__(self, atype: type, response_type=None, on_init_fail=None, **kwargs):
        """
        Dodo - easy version with conventions. configure later......

        Desired Usage:

            from schemas.pydantic.make import PieceSet
            from res.flows.api import FlowAPI
            api = FlowAPI(PieceSet)
            api.update(some_payload, **kwargs)

        Args:
            on_init_fail: set to raise if we want to complain of the airtable objects down exist
        """

        self._type = atype
        self._response_type = response_type or atype
        self._type_name = atype.__name__
        self._response_type_name = self._response_type.__name__
        self._type_namespace = stringcase.titlecase(atype.__module__.split(".")[-1])
        self._mutation = None
        self._upsert_mutation_uri = (
            Path(__file__).parent
            / "upserts"
            / f"{self._type_namespace}{self._type_name}.gql".replace(" ", "")
        )
        self._queue = None
        self._queue_query_evict_sql = None
        self._queue_uri = (
            Path(__file__).parent
            / "queues"
            / f"{self._type_namespace}{self._type_name}Evictions.gql"
        )
        self._queue_evicition_sql_uri = (
            Path(__file__).parent
            / "queues"
            / f"{self._type_namespace}{self._type_name}Evictions.sql"
        )
        self._hasura = kwargs.get("hasura") or res.connectors.load("hasura")
        self._postgres = kwargs.get("postgres") or res.connectors.load("postgres")
        # for now the name is determine like this
        table_name = f"{stringcase.titlecase(self._response_type_name)}s"

        overridden_airtable_table_name = getattr(
            getattr(response_type, "Config", None),
            "overridden_airtable_tablename",
            None,
        )

        if overridden_airtable_table_name:
            table_name = overridden_airtable_table_name

        overridden_airtable_baseid = getattr(
            getattr(response_type, "Config", None), "overridden_airtable_baseid", None
        )

        # flowAPI constructor might have supplied base_id in kwargs - if its there, we dont want to override it with the config from the overridden_airtable_baseid from the pydantic config
        if "base_id" not in kwargs and overridden_airtable_baseid is not None:
            kwargs["base_id"] = overridden_airtable_baseid

        try:
            self._airtable_queue = AirtableQueue(
                table_name,
                on_init_fail=on_init_fail,
                **kwargs,
            )
        except Exception as ex:
            import traceback

            raise Exception(
                f"Tried and failed to load the airtable queue {table_name} - {traceback.format_exc()}"
            )

    def _try_coerce(self, d):
        """
        experimental - there are smarter ways to think about mutation parsing if we can even do it in general
        this would allow us to pass dictionaries around more freely but coerce them to the mutation.
        it masks problems however so it should be used with care.
        if we have tested that we want to use the same pydantic object with multiple contexts e.g. airtable, kafka, hasura
        just as we coerce to kafka schema we can coerce to write a subset of fields from the type based on what the mutation accepts
        like the kafka coerce we should try to coerce types but that is not so useful when talking to relational databases from pydantic types
        """
        from res.flows.api import simple_mutation_arg_inspector

        args = simple_mutation_arg_inspector(self._upsert_mutation_uri)
        if args:
            # print(args, d.keys())
            d = {k: v for k, v in d.items() if k in args}
            res.utils.logger.debug(
                f"Coercing the keys to {list(d.keys())} to match the mutation args"
            )
        return d

    def __repr__(self):
        type_name = str(self._type.__name__)
        return f"FlowApi: {type_name}"

    def validate(self):
        """
        checks:
        the fields should have problems like multiple primary keys
        we can check services too
        """

    def ensure_loaded_mutation(self):
        if not self._mutation:
            # TODO: some validation here
            self._mutation = open(self._upsert_mutation_uri).read()
        return self._mutation

    def ensure_load_queue_query(self):
        if not self._queue:
            # TODO: some validation here
            self._queue = open(self._queue_uri).read()
        return self._queue

    def ensure_load_queue_sql_query_evictions(self):
        if not self._queue_query_evict_sql:
            # TODO: some validation here
            self._queue_query_evict_sql = open(self._queue_evicition_sql_uri).read()
        return self._queue_query_evict_sql

    def update(self, payload, **kwargs):
        """ """
        resp = self.update_hasura(payload, **kwargs)
        # convention is the last key is the one we care about for now but we can load multiple flatteners - graph ql returns results like that
        key = list(resp.keys())[-1]

        # flatteners must map back to the contract type - the response type could be different to the request type in future
        flattener = load_flattener(key)
        if flattener is None:
            raise Exception(f"Failed to load a flattener for {key}")
        # flatteners always returns sets - here we know we are upserting one with the mutation
        resp = flattener(resp)[0]

        if kwargs.get("plan_response"):
            return resp

        resp = self._response_type(
            **resp,
        )

        return self.relay(resp, **kwargs)

    def relay(self, payload, **kwargs):
        """
        relay is part of an update that does not change state in hasura but just sends data to other systems
        """
        # if we have configured and end point we will post to kafka
        self.try_update_kafka(payload)
        # TODO need to update this so we can switch types / tables - if the response is different the queue is always talking to to other type so maybe flow api should init the response type
        if kwargs.get("update_airtable", True):
            self.update_airtable_queue(payload, **kwargs)

        return payload

    def try_update_kafka(self, payload, **kwargs):
        # try resolve the kafka topic name from the type
        T = type(payload)
        topic = KAFKA_RESPONSE_TOPICS.get(T)

        if topic:
            topic = res.connectors.load("kafka")[topic]
            kpayload = payload.dict()
            topic.publish(kpayload, use_kgateway=USE_KGATEWAY, coerce=True)
        else:
            res.utils.logger.debug(
                f"Could not resolve any kafka response topic for type {T}"
            )
        return None

    def update_hasura(self, payload, **kwargs):
        """
        Use the mutation to write the object to the database - we only care about whats in the schema construction
        Note for flow api these are usually quiable objects. for example suppose we save a body
        there is also something call a body queue
        in hasura it may be the same table is both the body itself and the queue details e.g. status on the body
        this api should not be concerned so much with the body but rather the status of the body.
        if they are not the same object, queue apu should only update the queue status
        in principle flow api does not know anything other than to write pydantic types but this is an important conceptual distinction
        """
        mutation = self.ensure_loaded_mutation()
        some_mutation_options = {}  # from_kwargs
        data = payload.db_dict()
        if kwargs.get("coerce_hasura_mutation"):
            data = self._try_coerce(data)
        return self._hasura.execute_with_kwargs(
            mutation, **data, **some_mutation_options
        )

    # TODO: def update_hasura_async(self, payload: Dict, **kwargs):

    def update_airtable(self, payload: Union[Dict, FlowApiModel], **kwargs):
        """
        alias for `update_airtable_queue`
        """
        return self.update_airtable_queue(payload=payload, **kwargs)

    def update_airtable_queue(self, payload: Union[Dict, FlowApiModel], **kwargs):
        """
        Upsert a record to hasura
        """
        res.utils.logger.info(f"airtable payload: {payload}")
        if isinstance(payload, dict):
            # type should be passed in but we will try to coerce it
            payload = self._type(**payload)
        return self._airtable_queue.try_update_airtable_record(payload, **kwargs)

    def process_queue_evictions_sql(
        self, window_from: UTCDatetime, window_to: UTCDatetime, plan=False
    ):
        """
        this will be called by a cron job:
        each model has queries that define the queue eviction logic - we loop over the models and run them
        """
        query = self.ensure_load_queue_sql_query_evictions()

        res.utils.logger.info(
            f"querying postgres with window_from {window_from}, window_to {window_to}"
        )
        res.utils.logger.info(query)
        df = self._postgres.run_query(
            query, (window_from, window_to), keep_conn_open=True
        )

        # planning can show what is evicted from the queue
        if not plan:
            sets = list(set(df.iloc[:, 0]))
            self._airtable_queue.try_purge_airtable_records(sets, self._postgres)

        return sets

    def process_queue_evictions(
        self, window_from: UTCDatetime, window_to: UTCDatetime, plan=False
    ):
        """
        this will be called by a cron job:
        each model has queries that define the queue eviction logic - we loop over the models and run them
        """
        query = self.ensure_load_queue_query()
        data = self._hasura.execute_with_kwargs(
            query, window_from=window_from, window_to=window_to
        )

        # planning can show what is evicted from the queue
        if not plan:
            sets = list(data.keys())
            assert len(sets) < 2, "A queue query should only return a single set"
            # the conventions is the queue query must return a business key or id which is also used to update views like airtable
            keys = [d[self._type.primary_key] for d in data[sets[0]]]
            self._airtable_queue.remove_records(keys)

        return data

    @property
    def schema(self):
        return self._type.schema()

    def _check_contracts(response_includes_exceptions):
        """
        when we make a request, we get a response or an exception
        either of these can be evaluated against contracts ny known keys
        we can lookup the owner of the contract violation tag
        """

        # check if there are contact violations that need to be augmented + alerts e.g. if body-asset-failed and flow is 2d then send to julia queue
        # we can read the queue and send updates to users daily too
        # problem to solve; know the list of contracts and if the asset or asset-update violates the contract

        return None

    def _queue_status_event(response_includes_exceptions):
        """
        when we make a request, we get a response or an exception
        either of these can be evaluated against contracts ny known keys
        we can lookup the owner of the contract violation tag

        <id>
        <node>
        <status> ENTERED  | QUEUED | ACTIVE | DONE | CANCELLED | FAILED | EXITED
        <contract-flags>
        <metadata>
        <event-time>
        <resources>
          <resource id='' units='' qty=''/>
        </resources>
        <notes>

        when we insert a request the state change response can be constructed with
        <duration>
        this leaves us with the current state in the database and all previous states as events
        """

        # check if there are contact violations that need to be augmented + alerts e.g. if body-asset-failed and flow is 2d then send to julia queue
        # we can read the queue and send updates to users daily too
        # problem to solve; know the list of contracts and if the asset or asset-update violates the contract

        return None
