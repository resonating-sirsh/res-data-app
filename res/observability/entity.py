"""
notes we have use by_alias false in pydantic schema resolution which may not be as expected
"""

from typing import List, Optional
from pydantic import Field, BaseModel, create_model, root_validator
import math
import json
import numpy as np
import typing
import res
import pyarrow as pa
import datetime
import re

# When we are mature we would store configuration like this somewhere more central
INSTRUCT_EMBEDDING_VECTOR_LENGTH = 768
OPEN_AI_EMBEDDING_VECTOR_LENGTH = 1536
CLIP_EMBEDDING_VECTOR_LENGTH = 512

from enum import Enum


class ResonanceContextCategory(Enum):
    Glossary: str = "Glossary"
    Mission: str = "Mission"
    Processes: str = "Processes"
    People: str = "People"
    About: str = "About"


class FunctionTypes(Enum):
    Api: str = "Api"
    Utility: str = "Utility"
    VectorSearch: str = "VectorSearch"
    ColumnarQuantitativeSearch: str = "ColumnarQuantitativeSearch"


def load_type(namespace, entity_name):
    """ """

    raise NotImplemented("TODO")


def map_pyarrow_type_info(field_type, name=None):
    """
    Load not only the type but extra type metadata from pyarrow that we use in Pydantic type
    """
    t = map_pyarrow_type(field_type, name=name)
    d = {"type": t}
    if pa.types.is_fixed_size_list(field_type):
        d["fixed_size_length"] = field_type.list_size
    return d


def map_pyarrow_type(field_type, name=None):
    """
    basic mapping between pyarrow types and typing info for some basic types we use in stores
    """

    if pa.types.is_fixed_size_list(field_type):
        return typing.List[map_pyarrow_type(field_type.value_type)]
    if pa.types.is_map(field_type):
        return dict
    if pa.types.is_list(field_type):
        return list
    else:
        if field_type in [pa.string(), pa.utf8(), pa.large_string()]:
            return str
        if field_type == pa.string():
            return str
        if field_type in [pa.int16(), pa.int32(), pa.int8(), pa.int64()]:
            return int
        if field_type in [pa.float16(), pa.float32(), pa.float64()]:
            return float
        if field_type in [pa.date32(), pa.date64()]:
            return datetime.datetime
        if field_type in [pa.bool_()]:
            return bool
        if field_type in [pa.binary()]:
            return bytes
        if pa.types.is_timestamp(field_type):
            return datetime.datetime

        if "timestamp" in str(field_type):
            return typing.Optional[str]
        return typing.Optional[str]

        raise NotImplementedError(f"We dont handle {field_type} ({name})")


def map_field_types_from_pa_schema(schema):
    """
    given a pyarrow schema return type info so we can create the Pydantic Model from the pyarrow table
    """

    return {f.name: map_pyarrow_type_info(f.type, f.name) for f in schema}


class NpEncoder(json.JSONEncoder):
    """
    A Json encoder that is a little bit more understanding of  numpy types
    """

    def default(self, obj):
        from pandas.api.types import is_datetime64_any_dtype as is_datetime
        from pandas._libs.tslibs.timestamps import Timestamp
        from pandas import isna

        dtypes = (np.datetime64, np.complexfloating)
        if isinstance(obj, dtypes):
            return str(obj)
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            if any([np.issubdtype(obj.dtype, i) for i in dtypes]):
                return obj.astype(str).tolist()
            return obj.tolist()
        elif not isinstance(obj, list) and isna(obj):
            return None
        # this is a choice for serialization
        elif isinstance(obj, Timestamp) or is_datetime(obj):
            return str(obj)
        return super(NpEncoder, self).default(obj)


class AbstractEntity(BaseModel):
    """
    The base type with some store helper attributes
    """

    name: str

    @classmethod
    @property
    def namespace(cls):
        """
        Takes the namespace from config or module - returns default if nothing else e.g. dynamic modules
        """
        if hasattr(cls, "Config"):
            if hasattr(cls.Config, "namespace"):
                return cls.Config.namespace

        # TODO: simple convention for now - we can introduce other stuff including config
        namespace = cls.__module__.split(".")[-1]
        # for now we use the default namespace for anything in entities
        return namespace if namespace != "entities" else "default"

    @classmethod
    @property
    def entity_name(cls):
        # TODO: we will want to fully qualify these names
        s = cls.schema()
        return s["title"]

    @classmethod
    @property
    def full_entity_name(cls, sep="_"):
        return f"{cls.namespace}{sep}{cls.entity_name}"

    @classmethod
    @property
    def key_field(cls):
        s = cls.schema()
        key_props = [k for k, v in s["properties"].items() if v.get("is_key")]
        # TODO: assume one key for now
        if len(key_props):
            return key_props[0]

    @classmethod
    @property
    def fields(cls):
        s = cls.schema()
        key_props = [k for k, v in s["properties"].items()]
        return key_props

    @classmethod
    @property
    def about_text(cls):
        if hasattr(cls, "config"):
            c = cls.config
            return getattr(c, "about", "")

    @classmethod
    @property
    def embeddings_provider(cls):
        if hasattr(cls, "Config"):
            if hasattr(cls.Config, "embeddings_provider"):
                return cls.Config.embeddings_provider

    def large_text_dict(cls):
        return cls.dict()

    def __str__(cls):
        """
        For the purposes of testing some logging with types
        the idea of using markdown and fenced objects is explored
        """

        d = cls.dict()
        d["__type__"] = cls.entity_name
        d["__key__"] = cls.key_field
        d["__namespace__"] = cls.namespace
        d = json.dumps(d, cls=NpEncoder, default=str)
        return f"""```json{d}```"""

    @classmethod
    def create_model_from_data(cls, name, data, namespace=None, **kwargs):
        schema = pa.Table.from_pandas(data).schema
        data = data.to_dict("records")

        # def treat(row):
        #     for k, v in row.items():
        #         if isinstance(v, np.ndarray):
        #             row[k] = list(v)
        #     return row

        Model = cls.create_model_from_pyarrow(
            name=name,
            namespace=namespace,
            py_arrow_schema=schema,
        )

        return Model

    @classmethod
    def create_model_from_pyarrow(
        cls, name, py_arrow_schema, namespace=None, key_field=None, **kwargs
    ):
        def _Field(name, fi):
            d = dict(fi)
            t = d.pop("type")
            # field = Field (t, None, is_key=True) if name == key_field else Field
            return (t, None)

        namespace = namespace or cls.namespace
        fields = map_field_types_from_pa_schema(py_arrow_schema)
        fields = {k: _Field(k, v) for k, v in fields.items()}
        return create_model(name, **fields, __module__=namespace, __base__=cls)

    @classmethod
    def create_model(cls, name, namespace=None, **fields):
        """
        For dynamic creation of models for the type systems
        create something that inherits from the class and add any extra fields

        when we create models we should audit them somewhere in the system because we can later come back and make them
        """
        namespace = namespace or cls.namespace
        return create_model(name, **fields, __module__=namespace, __base__=cls)

    @staticmethod
    def get_type(namespace, entity_name):
        """
        accessor for type loading by convention
        """
        return load_type(namespace=namespace, entity_name=entity_name)

    @staticmethod
    def get_type_from_the_ossified(json_type):
        """
        accessor for type loading by convention
        """
        namespace = json_type["__namespace__"]
        entity_name = json_type["__type__"]

        return load_type(namespace=namespace, entity_name=entity_name)

    @staticmethod
    def hydrate_type(json_type):
        """
        determine type and reload the json string by conventional embed of type info
        """
        ptype = AbstractEntity.get_type_from_the_ossified(json_type)
        return ptype(**json_type)

    @classmethod
    def pyarrow_schema(cls):
        """
        Convert a Pydantic model into a PyArrow schema in a very simplistic sort of way

        Args:
            model_class: A Pydantic model class.

        Returns:
            pyarrow.Schema: The corresponding PyArrow schema.
        """
        import pyarrow as pa

        def iter_field_annotations(obj):
            for key, info in obj.__fields__.items():
                yield key, info.annotation

        """
        We want to basically map to any types we care about
        there is a special consideration for vectors
        we take the pydantic field attributes in this case fixed_size_length to determine this
        Each pydantic type is designed for a particular embedding so for example we want an 
        OPEN_AI or Instruct embedding fixed vector length
        """

        def mapping(k, fixed_size_length=None):
            mapping = {
                int: pa.int64(),
                float: pa.float64(),
                str: pa.string(),
                bool: pa.bool_(),
                bytes: pa.binary(),
                # assuming a string list is not really safe
                list: pa.list_(pa.utf8()),
                typing.List[str]: pa.list_(pa.utf8()),
                datetime.datetime: pa.timestamp("us", tz="UTC"),
                # maps not supported in lance
                dict: pa.string(),  # pa.map_(pa.string(), pa.null()),
                # TODO: this is temporary: there is a difference between these embedding vectors and other stuff
                typing.List[int]: pa.list_(
                    pa.int64(), list_size=fixed_size_length or -1
                ),
                typing.List[float]: pa.list_(
                    pa.float32(), list_size=fixed_size_length or -1
                ),
                # should the default be string?
                None: pa.string(),
            }

            return mapping[k]

        fields = []

        props = cls.schema()["properties"]
        for field_name, field_info in iter_field_annotations(cls):
            if hasattr(field_info, "__annotations__"):
                # TODO need to test this more
                field_type = AbstractEntity.pyarrow_schema(field_info)
            else:
                if getattr(field_info, "__origin__", None) is not None:
                    field_info = field_info.__args__[0]
                """
                take the fixed size from the pydantic type attribute if it exists turning the 
                list into a vector
                """
                field_type = mapping(
                    field_info, props[field_name].get("fixed_size_length")
                )

            field = pa.field(field_name, field_type)
            fields.append(field)

        return pa.schema(fields)


class KeyedAbstractEntity(AbstractEntity):
    """
    this entity is convenient because subclasses just need to create a name or id field to have a primary key by convention
    """

    id: str = Field(is_key=True)


class AbstractVectorStoreEntry(AbstractEntity):
    """
    The Base of Vector Store types that manages vector embedding lengths

    We can store vectors and other attributes
    At a minimum the and text are needed as we can generate other ids from those
    Name must be unique if the id is omitted of course - but its the primary key so it makes sense
    """

    name: str = Field(is_key=True)
    text: str = Field(long_text=True)
    doc_id: Optional[str]
    vector: Optional[List[float]] = Field(
        fixed_size_length=OPEN_AI_EMBEDDING_VECTOR_LENGTH,
        embedding_provider="open-ai",
        default_factory=list,
    )  # lambda: [0.0 for i in range(OPEN_AI_EMBEDDING_VECTOR_LENGTH)]
    id: Optional[str]
    depth: Optional[int] = 0
    # node types eg. summaries at depth -1
    text_node_type: Optional[str] = "FULL"
    username: typing.Optional[str] = ""
    timestamp: typing.Optional[str] = ""
    unix_ts: typing.Optional[int] = 0
    image_content_uri: typing.Optional[str] = ""
    uri: typing.Optional[str] = ""
    page: typing.Optional[int] = -1

    @root_validator()
    def default_ids(cls, values):
        # create timestamps and unix timestamps for filtering
        if not values.get("id"):
            values["id"] = values["name"]
        if not values.get("doc_id"):
            values["doc_id"] = values["name"]
        return values

    def split_text(cls, max_length=int(1 * 1e4), **options):
        """
        simple text splitter - working on document model
        """

        def part(s, i):
            return s[i * max_length : (i * max_length) + max_length]

        # we keep the model N times with partial items
        # we assume for now that we have univariate vector schema which in general will not be the case
        def model_copy(**kwargs):
            d = cls.dict()
            d.update(kwargs)
            return cls.__class__(**d)

        return [
            model_copy(text=part)
            for part in [
                part(cls.text, i) for i in range(math.ceil(len(cls.text) / max_length))
            ]
        ]

    # @classmethod
    # def as_store(cls):
    #     """
    #     because stores are determined by types alone, we can construct stores this way
    #     """

    #     return VectorDataStore(cls)


class InstructAbstractVectorStoreEntry(AbstractVectorStoreEntry):
    class Config:
        embeddings_provider = "instruct"

    vector: Optional[List[float]] = Field(
        fixed_size_length=INSTRUCT_EMBEDDING_VECTOR_LENGTH,
        embedding_provider="instruct",
        default_factory=list,
    )


class FunctionDescriptionVectorStoreEntry(AbstractVectorStoreEntry):
    # class Config:
    #     embeddings_provider = "instruct"

    # vector: Optional[List[float]] = Field(
    #     fixed_size_length=INSTRUCT_EMBEDDING_VECTOR_LENGTH,
    #     default_factory=list,
    # )

    function_namespace: str
    function_class: str  # typing.Optional[FunctionTypes, str]
    # the text will describe the function but this will prompt the LLM
    function_description: typing.Optional[str] = None


class ReturnReasonsVectorStoreEntry(AbstractVectorStoreEntry):
    source_return_line_item_id: str
    return_reason_notes: str
    brand_code: str
    brand_name: str
    body_code: str
    style_code: str
    size_code: str
    fulfillment_completed_at: datetime.datetime
    ordered_at: datetime.datetime
    synced_at: datetime.datetime


class ResonanceContextVectorStoreEntry(AbstractVectorStoreEntry):
    # class Config:
    #     embeddings_provider = "instruct"

    # vector: Optional[List[float]] = Field(
    #     fixed_size_length=INSTRUCT_EMBEDDING_VECTOR_LENGTH,
    #     default_factory=list,
    # )

    category: ResonanceContextCategory = ResonanceContextCategory.About
    timestamp: str
    username: Optional[str] = None

    @root_validator()
    def defaults(cls, values):
        if not values.get("timestamp"):
            values["timestamp"] = res.utils.dates.utc_now_iso_string()


class SlackMessageVectorStoreEntry(AbstractVectorStoreEntry):
    # class Config:
    #     embeddings_provider = "instruct"

    # vector: Optional[List[float]] = Field(
    #     fixed_size_length=INSTRUCT_EMBEDDING_VECTOR_LENGTH,
    #     default_factory=list,
    # )

    username: typing.Optional[str] = ""
    timestamp: typing.Optional[str] = None

    @root_validator()
    def defaults(cls, values):
        if not values.get("timestamp"):
            values["timestamp"] = res.utils.dates.utc_now_iso_string()

        return values

    @classmethod
    def collection_batch_from_reader(
        cls, channel_id, since_date=None, parse_referenced_files=False
    ):
        res.utils.logger.debug(f"{channel_id=}, {since_date=}")
        slack = res.connectors.load("slack")

        batch = slack.get_processed_channel_messages(channel_id, since_date=since_date)

        batch["name"] = batch.apply(
            lambda row: f"{row['channel_name']}-{row['username']}-{row['timestamp']}",
            axis=1,
        )
        batch["id"] = batch["name"]
        batch["text"] = batch["thread"]
        batch["timestamp"] = batch["timestamp"].map(lambda x: str(x).split(".")[0])

        batch = batch.to_dict("records")

        return [SlackMessageVectorStoreEntry(**s) for s in batch]


class AbstractImageStoreEntry(AbstractEntity):
    """
    Create stores using S3 Uri to source images for embedding
        Piece = AbstractImageStoreEntry.create_model(name='images2', namespace='test')
        p = Piece(name='my_image',  image_uri='s3://res-data-platform/samples/images/test_image.png')
        store = VectorDataStore(Piece)
        store.add(p)
    """

    class Config:
        embeddings_provider = "clip"

    id: typing.Optional[str]
    name: str
    # not used - just a workaround
    text: str = ""
    vector: typing.Optional[typing.List[float]] = Field(
        embedding_provider="clip",
        fixed_size_length=CLIP_EMBEDDING_VECTOR_LENGTH,
        default_factory=list,
    )
    image_uri: str

    @property
    def image(cls):
        return res.connectors.load("s3").read_image(cls.image_uri)

    @root_validator
    def _image_vector(cls, values):
        if not values.get("id"):
            values["id"] = values["name"]
        # this is just a hack because the vector store currently assumes text mostly
        values["text"] = values["image_uri"]
        return values
