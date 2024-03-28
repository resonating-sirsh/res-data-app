from res.observability.entity import AbstractEntity, typing
from . import AbstractStore, COLUMNAR_STORE_ROOT_URI, get_agent
from res.utils import logger
import res
import numpy as np
import pyarrow as pa
import pandas as pd
import datetime
import json


class ColumnarDataStore(AbstractStore):
    """
    Load a datastore or use it as a function

    """

    def __init__(
        self, entity: AbstractEntity, description=None, create_if_not_found=False
    ):
        super().__init__(entity=entity, description=description)
        self._entity = entity
        self._db = res.connectors.load("duckdb")
        self._s3 = res.connectors.load("s3")
        self._table_path = f"{COLUMNAR_STORE_ROOT_URI}/{self._entity_namespace}/{self._entity_name}/parts/0/data.parquet"
        self._exists_at_load = self._s3.exists(self._table_path)
        if not self._exists_at_load:
            if not create_if_not_found:
                raise Exception(
                    f"The path {self._table_path} does not yet exist and we are not auto creating"
                )
        self._metadata_loaded = False
        # self._load_table_metadata(exists=self._exists_at_load)
        if not self._exists_at_load:
            res.utils.logger.info(
                f"{self._table_path} does not exist yet - creating the index"
            )
            self.update_index()
        self._agent = None

    def _load_table_metadata(cls, exists=True):
        res.utils.logger.debug(
            f"Inspecting table metadata for {cls._table_path} at load"
        )
        cls._enums = cls._db.inspect_enums(cls._table_path) if exists else {}
        cls._fields = cls._db.probe_field_names(cls._table_path) if exists else []
        cls._metadata_loaded = True

    def load(self, limit: int = None):
        limiter = "" if not limit else f"LIMIT {limit}"
        return self.query(f"SELECT * FROM {self._entity_name} {limiter}")
        # return self._s3.read(self._table_path)

    def __call__(self, question):
        return self.run_search(question)

    def __repr__(self) -> str:
        return f"ColumnarDataStore({self._table_path})"

    def get_data_model(cls):
        return AbstractEntity.create_model_from_pyarrow(
            # names
            name=cls._entity_name,
            namespace=cls._entity_namespace,
            # sample the schema
            py_arrow_schema=cls.load(limit=1).to_arrow().schema,
        )

    def __getitem__(self, key: str) -> AbstractEntity:
        """
        get accessor fetching by name
        """

        key_field = "name"
        data = self.query(
            f"SELECT * FROM {self._entity_name} WHERE {key_field} = '{key}'"
        ).to_dicts()

        if len(data):
            return self._entity(**data[0])

    @staticmethod
    def open(name, namespace, reload_model=True):
        """
        Open a model using schema inference and path conventions
        """
        Model = AbstractEntity.create_model(name, namespace=namespace)
        store = ColumnarDataStore(Model)
        if reload_model:
            # we can infer the schema and create a dynamic model
            # if we actually knew the model we would just open the store with the constructor
            # sometimes just the name and namespace is enough but this is stronger as it loads the schema for supported types
            Model = store.get_data_model()
            return ColumnarDataStore(Model)
        return store

    @property
    def query_context(self):
        def get_query_context(uri, name):
            """
            get the polar query context from polars
            """
            # polars functionality in observability
            from res.observability.io import read, pl

            ctx = pl.SQLContext()
            ctx.register(name, read(uri, lazy=True))
            return ctx

        return get_query_context(self._table_path, name=self._entity_name)

    def query(self, query):
        ctx = self.query_context
        return ctx.execute(query).collect()

    def fetch_entities(self, limit=10) -> typing.List[AbstractEntity]:
        data = self.query(f"SELECT * FROM {self._entity_name} LIMIT {limit}").to_dicts()
        return [self._entity(**d) for d in data]

    def infer_entities(cls, limit=None):
        """
        If the entity model is dynamic we would not have the schema
        we can determine the schema from the parquet data and convert to entities
        """
        # the fillna does not really make sense but for this we will do it
        data = cls.load()
        schema = pa.Table.from_pandas(data).schema
        data = data.to_dict("records")

        def treat(row):
            for k, v in row.items():
                # ndarray to lists for lance
                if isinstance(v, np.ndarray):
                    row[k] = list(v)
                # cast dicts to str for lance
                elif isinstance(v, dict):
                    row[k] = str(v)

            return row

        Model = AbstractEntity.create_model_from_pyarrow(
            name=cls._entity_name,
            namespace=cls._entity_namespace,
            py_arrow_schema=schema,
        )

        def _try_model(row):
            return Model(**treat(row))

        # best effort for testing - it should not fail if the pydantic objects are properly defined or dynamic defaults are implemented
        records = [_try_model(row) for row in data]
        return [r for r in records if r is not None]

    def add(self, records: typing.List[AbstractEntity], mode="merge", key_field=None):
        """
        Add the fields configured on the Pydantic type that are columnar - defaults all
        These are merged into parquet files on some path in the case of this tool
        """

        def unpack_object(d):
            if hasattr(d, "dict"):
                d = d.dict()

            def _cast(v):
                if isinstance(v, dict):
                    return json.dumps(v, default=str)
                # im not sure if i need to do this when writing pyarrow types

                # these things have a to-arrow ??
                elif isinstance(v, datetime.datetime):
                    return pd.to_datetime(v, utc=True)

                return v

            d = {k: _cast(v) for k, v in d.items()}

            return d

        from . import merge, write

        if records and not isinstance(records, list):
            records = [records]

        records = [unpack_object(r) for r in records]

        merge_key = key_field or self._key_field

        if len(records):
            logger.info(f"Writing {self._table_path}. {len(records)} records.")
            if mode == "merge":
                logger.info(f" Merge will be on key[{merge_key}]")
            return (
                merge(
                    self._table_path,
                    records,
                    key=merge_key,
                    schema=self._entity.pyarrow_schema(),
                )
                if mode != "overwrite"
                else write(
                    self._table_path, records, schema=self._entity.pyarrow_schema()
                )
            )

        self._load_table_metadata()

        return records

    def run_search(
        self,
        queries: str | typing.List[str],
        limit: int = 200,
        **kwargs,
    ):
        """
        Perform the columnar data search for the queries directly on the store.
        This store is used for answering questions of a statistical nature about the entity and may provide some line item comments.
        Be very careful not to assume particular attributes denote the entities you care about. You should know the difference between bodies, materials, ONES (Production Request), orders(custom orders) and styles.

        **Args**
            queries: supply one or more questions about data in the store
            limit: limit the number of data rows returned - this is to stay with context window but defaults can be trusted in most cases
        """
        #
        # NOTE kwargs allows previously indexes stuff to take the limit

        # may make these class property of the store. the search method should be something an LLM can use
        return_type = "dict"
        build_enums = True
        # force this despite what the agent does - it should be big if we ask for specific things but generally wasteful to provide large result sets
        limit = 50  # disable for now

        if self._agent is None:
            self._agent = get_agent()

        if not self._metadata_loaded:
            # lazy load on query one time
            self._load_table_metadata()

        # todo allow multiples
        res.utils.logger.debug(f"[Columnar Store {self._entity_name}] {queries=}")
        question = str(queries)

        def parse_out_sql_and_try_clean(s):
            if "```" in s:
                s = s.split("```")[1].replace("sql", "").strip("\n")
            return s.replace("CURRENT_DATE ", "CURRENT_DATE()")

        enums = {} if not build_enums else self._enums

        # DO WE WANT LIMITS in ROWS?? :> Be efficient! Try to be specific with your request by selecting only the columns you think you will need and if you are asked for 1 or examples LIMIT the results to only a few rows.

        prompt = f"""For a table called literally `TABLE` that describes the entities of type {self._entity_name} with the {self._fields}, and the following column enum types {enums} ignore any columns asked that are not in this schema and give
            me a DuckDB dialect sql query without any explanation that answers the question below. 
            Any generic attributes like `name` or `description` should be assumed to refer to the entity of type {self._entity_name}.  
            Questions: {question} """

        logger.debug(prompt)
        query = self._agent.ask(prompt)
        logger.debug(f"Agent   provided {query=}")
        # a safety in case table os quoted for some reason
        query = query.replace(f"'TABLE'", "TABLE").replace('"TABLE"', "TABLE")
        logger.debug(f"Cleaner provided {query=}")
        # in case it tries to be smart
        query = query.replace("TABLE", f"'{self._table_path}'")

        NO_DATA_WARNING = """"Error - Try the following hint to find data: because there are zero results you are probably constructing a query that does not make sense. 
        For example you are adding too many predicates, misusing the type of entity or the type of data or looking for values that are not in the column.
        If you are trying to filter columns by specific values, try first getting distinct values for those columns to see what values you can actually filter by"""

        try:
            query = parse_out_sql_and_try_clean(query)
            logger.info(f"{query=}")
            data = self._db.execute(query)
            if limit:
                data = data[:limit]
            if return_type == "dict":
                data = data.to_dict("records")
            if len(data) == 0:
                return [{"bad query or question": NO_DATA_WARNING}]
            return data
        # TODO better LLM and Duck exception handling
        except Exception as ex:
            print(ex, "failing in the store")
            return [{"bad query or question": NO_DATA_WARNING}]

    def as_function(self, question: str):
        """
        The full columnar data tool provides statistical and quantitative results but also key attributes. Usually can be used to answer questions such as how much, rank, count etc. and random facts about the entity.
        this particular function should be used to answer questions about {self._entity_name}

        :param question: the question being asked
        """

        results = self.run_search(question)
        # audit
        logger.debug(results)
        return results
