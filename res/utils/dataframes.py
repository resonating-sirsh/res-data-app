# TODO: add features
# https://pyjanitor.readthedocs.io/reference/janitor.functions/janitor.clean_names.html

# import pandera
# https://pandera.readthedocs.io/en/stable/index.html
#
# from typing import Mapping
from ast import literal_eval
from base64 import b64encode
from io import BytesIO
import json
import re
import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from collections.abc import Iterable
from collections import defaultdict
import res
from typing import Any, Dict, List, Union
import math


def hash_dataframe_short(df):
    return res.utils.res_hash(pd.util.hash_pandas_object(df).values.sum())


def as_agent(df, wide_columns=None, **kwargs):
    """
    you can run the agent with a query
    as_agent(df).run('ask a question')

    """
    from langchain.agents import create_pandas_dataframe_agent
    from langchain.llms import OpenAI
    from res.utils.secrets import secrets

    secrets.get_secret("OPENAI_API_KEY")

    # not sure if this thing gets html of the table in which case we need a wide option
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.expand_frame_repr', False)
    # pd.set_option('max_colwidth', -1)

    # temp
    df.style.set_properties(subset=wide_columns or [], **{"width": "1200px"})

    return create_pandas_dataframe_agent(OpenAI(temperature=0), df, verbose=True)


def replace_nan_with_none(df):
    df = df.astype(object).replace(np.nan, None)
    df = df.where(pd.notnull(df), None)
    return df


def get_prop(d, prop, default):
    v = d.get(prop, default)
    if not isinstance(v, Iterable) and pd.isnull(v):
        return default
    return v


def not_null_or_enumerable(x):
    if isinstance(x, list) or isinstance(x, np.ndarray):
        return True
    return pd.notnull(x)


def dataframe_with_shape_images(df, columns):
    import io
    from IPython.display import HTML
    from cairosvg import svg2png
    from PIL import Image
    from res.utils.dataframes import image_formatter

    formatters = {}

    object_types = []
    for k, v in df.dtypes.items():
        if str(v) == "object":
            object_types.append(k)

    for c in columns:
        df[f"__{c}"] = df[c].map(
            lambda x: Image.open(io.BytesIO(svg2png(x._repr_svg_())))
        )
        formatters[f"__{c}"] = image_formatter

    columns = [c for c in df.columns if c not in columns]

    return HTML(df[columns].to_html(formatters=formatters, escape=False))


def image_formatter(im):
    """
    In a dataframe we can display images if we format them and then

    #in jupyter
      from IPython.display import HTML -> for some image data called 'icon'
      HTML(df.to_html(formatters={'icon': image_formatter}, escape=False))

    """

    def image_base64(im):
        with BytesIO() as buffer:
            im.save(buffer, "png")
            return b64encode(buffer.getvalue()).decode()

    return f'<img src="data:image/png;base64,{image_base64(im)}">'


def column_as_group_rank(df, group, column):
    """
    take any group and rank rows, sorting on column in that group
    replaces the column with the rank of the values

    subtract by 1 to 0-index
    """
    df[column] = df.groupby(by=group)[column].transform(lambda x: x.rank()) - 1
    return df


def diff_seconds(data, start_time_column="old_timestamp", end_time_column="timestamp"):
    for col in ["timestamp", "old_timestamp"]:
        if not is_datetime(data[col]):
            data[col] = pd.to_datetime(data[col])

    return (data[end_time_column] - data[start_time_column]).dt.total_seconds()


def as_hours(row, start_time_column="old_timestamp", end_time_column="timestamp"):
    dts = []
    diff = row[end_time_column] - row[start_time_column]
    hours_in_set = np.ceil(diff.total_seconds() / 3600.0) + 1
    for d in pd.date_range(row[start_time_column], periods=hours_in_set, freq="H"):
        dts.append(d.to_pydatetime().replace(minute=0, second=0, microsecond=0))
    return dts


def seconds_in_hour(
    row,
    start_time_column="old_timestamp",
    end_time_column="timestamp",
    hour_column="segment_hour",
):
    def same_hour(a, b):
        return a.replace(minute=0, second=0, microsecond=0) == b.replace(
            minute=0, second=0, microsecond=0
        )

    if same_hour(row[hour_column], row[start_time_column]):
        # if the end time is also in the same hour we just get the difference
        if same_hour(row[hour_column], row[end_time_column]):
            return (row[end_time_column] - row[start_time_column]).total_seconds()
        return (row[start_time_column] - row[hour_column]).total_seconds()
    if same_hour(row[hour_column], row[end_time_column]):
        # if the start time column is also in the same hour
        if same_hour(row[hour_column], row[start_time_column]):
            return (row[end_time_column] - row[start_time_column]).total_seconds()
        return (row[end_time_column] - row[hour_column]).total_seconds()
    if (
        row[hour_column] < row[end_time_column]
        and row[hour_column] > row[start_time_column]
    ):
        return 3600
    return 0


def make_hour_segment_durations(data):
    """
    Time helper utility to aportion time accross distinct hours for reporting
    A dataframe with a start and end time can be expanded into rows per hour with
    portion of hour covered

    Sample data to test / example

        import numpy as np
        data = pd.DataFrame([f"Key{i}" for i in range(100)],columns=['key'])
        from datetime import datetime,timedelta
        data['timestamp_start'] = data['key'].map(
                lambda x : datetime.now() + timedelta(seconds=np.random.randint(1000))
        )
        data['timestamp'] = data['timestamp'].map(
            lambda x : x + timedelta(seconds=np.random.randint(10000))
        )
        data['duration_minutes'] = (
            data['timestamp_end'] - data['timestamp']
        ).dt.seconds / 60.
        data = data.sort_values('duration_minutes')
        make_hour_segment_durations(data)
    """
    if data is None or len(data) == 0:
        return data
    # ensure datetimes - hours assumes these names for this operation
    for col in ["timestamp", "old_timestamp"]:
        if not is_datetime(data[col]):
            data[col] = pd.to_datetime(data[col])

    data["segment_hour"] = data.apply(as_hours, axis=1)
    data = data.explode("segment_hour").reset_index().drop("index", 1)
    data["segment_duration_seconds"] = data.apply(seconds_in_hour, axis=1).round(2)
    data = data[data["segment_duration_seconds"] > 0].reset_index().drop("index", 1)
    data["segment_duration_minutes"] = (data["segment_duration_seconds"] / 60.0).round(
        2
    )
    return data


def _try_eval(s):
    try:
        return literal_eval(str(s))
    except:
        return s


def default_clean_field_name(name):
    """
    i have tried to match how it seems we map to Snowflake but im not sure about this -
    logic seemed weird
    """
    name = name.replace('"', "")
    if re.match(r"^[\w ]+$", name) is None:
        name = re.sub(r"[^\w]+", "_", name)

    return name.rstrip("_")


def _to_snake_case(name):
    if pd.isnull(name):
        return name
    # this is a really lazy version of this - need to do better
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    name = re.sub("__([A-Z])", r"_\1", name)
    name = re.sub("([a-z0-9])([A-Z])", r"\1_\2", name)
    return name.lower().replace(" ", "_").replace("__", "_")


def snake_case_columns(df, clean_fn=default_clean_field_name):
    def cleaner(s):
        s = clean_fn(s)
        return _to_snake_case(s)

    df.columns = [cleaner(s) for s in df.columns]

    return df


# def snake_case_columns(df):
#     return df.rename(columns={c: _to_snake_case(c) for c in df.columns})


def snake_case_names(names):
    if isinstance(names, str):
        return _to_snake_case(names)
    # or its a list of strings
    return [_to_snake_case(n) for n in list(names)]


def to_python_types(dts, default="string", object_as_string=False):
    mapping = {
        "object": "object" if not object_as_string else "str",
        "int64": "int",
        "float64": "float",
        "bool": "bool",
        "datetime64": "datetime",
        "datetime64[ns]": "datetime",
        "datetime64[ns, UTC]": "datetime",
        "string": "str",
    }

    if not isinstance(dts, dict):
        dts = dict(dts)
        dts = {k: v.name for k, v in dts.items()}

    return {k: mapping.get(v, v) for k, v in dts.items()}


def res_schema_from_dataframe(df, key, name, object_types=None, meta_types=None):
    python_types = to_python_types(df.dtypes, object_as_string=True)

    def make_field(k, v):
        return {"key": k, "name": k, "type": v}

    return {
        "key": key,
        "name": name or key,
        "fields": [make_field(k, v) for k, v in python_types.items()],
    }


def coerce_nan(x, treat_as_none_lower=None):
    return coerce_null(x, treat_as_none_lower)


def coerce_null(x, treat_as_none_lower=None):
    """
    eg df.applymap(coerce_nan, ['{Missing}'])
    """
    if isinstance(x, list):
        return x
    if pd.isna(x):
        return None
    if str(x).lower() in ["nan", "none", "null", "nill", "{}", "[]"]:
        return None
    if treat_as_none_lower is not None and str(x).lower() in treat_as_none_lower:
        return None
    return x


def coerce_int(x, return_null_for_fail=False):
    try:
        return int(float(x))
    except:
        # return -1 is not really safe but None can cause problems in sets where we
        # cannot treat it as an int
        return -1 if not return_null_for_fail else None


def coerce_bool(x):
    try:
        if pd.isnull(x):
            return False
        return True if str(x).lower in ["yes", "true", "checked"] else False
    except:
        return False


def coerce_float(x):
    try:
        return float(x)
    except:
        return None


def coerce_str(x):
    # sometimes we are told something is singleinetext and its in a [list] - we could
    # try to coerce this but not for now
    return str(coerce_nan(x))


def coerce_object(x):
    """
    first try json parsing otherwise try eval a list else just return as string
    """
    try:
        x = json.load(x)
    except:
        try:
            x = literal_eval(str(x))
        except:
            pass
    return str(coerce_nan(x))


def push_schema_to_glue(df, path, metadata):
    pass


def rename_and_whitelist(
    df, columns, conflict_resolution="rename", replace_bad_chars=True
):
    """Renames columns as per pandas.DataFrame.rename but also drops columns not in the
    target columns
    While renaming multiple columns to the same target is not allowed, sometimes we
    allow this in the column spec if it is known that the same dataframe only has one of
    the source columns (TODO error_on_many_one_target) A case that can be problematic is
    if we are renaming to a column name already on the dataframe at runtime
    In this case we can either drop the offending column, rename it to something else in
    the background for tracking
    (it is presumed not to be needed otherwise it would be in the column targets)

    """

    if columns == None:
        return df

    # unless you want to terminate when there are items missing on the source
    # this can be used to rename something [if exists] for example we decide to keep
    # something like city that is different to your city
    # we can also drop to from the rename silently

    # add a special temp tracker for things on the source we are going to drop
    # context.tracker._log_dropped_source_columns(columns)

    if replace_bad_chars:
        df.columns = [c.replace("\n", " ").rstrip().lstrip() for c in df.columns]

    if conflict_resolution != "raise":
        for c in columns.keys():
            if c not in df.columns:
                if conflict_resolution == "rename":
                    df[c] = None
                if conflict_resolution == "drop":
                    columns.pop()

    # try to drop anything that we are not going to need in the rename
    df = df.drop(
        [c for c in df.columns if c not in list(columns.keys())],
        axis=1,
        errors="ignore",
    )
    df = df.rename(columns=columns)

    l = list(df.columns[df.columns.duplicated()])
    if len(l) > 0:
        # TODO: add context logger + think about how this plays with validation system
        raise Exception(
            f"these cols are duplicated: {l}. This just wont do on a dataframe!"
        )

    return df


def flatten(df, explode_column, lsuffix=None, rsuffix=None):
    """
    Exploded nested items and add a suffix to disambiguate column names
    """
    items = df[[explode_column]].explode(explode_column)
    items = pd.DataFrame(
        [d for d in items[explode_column]], index=items.index
    ).sort_index()
    # drop the exploded column and add its flattened form via a join
    df = df.drop(explode_column, 1).join(items, lsuffix=lsuffix, rsuffix=rsuffix)
    return df.reset_index().drop("index", 1)


def expand_levels(df, column_depths, **kwargs):
    """
    If there are nested dicts (even if they are strings) unnest them to some depth
    """
    for k, v in column_depths.items():
        # support deep un-nesting in future but just testing for one level
        df = expand_column(df, k, levels=v)
    return df


def expand_column_drop(df, column, alias=None, levels=1):
    return expand_column(df, column=column, alias=alias, drop=True, levels=levels)


def expand_column(df, column, drop=False, alias=None, levels=1):
    """
    Expand a dict column and join back onto dataframe with field name prefix on each
    field
    """

    def check_nan(d):
        try:
            if pd.isnull(d):
                return {}
        except:
            pass
        return d

    if column not in df.columns:
        # do nothing but could warn - the user should only expand a column that exist or
        # we return for no op
        return df

    alias = alias or column
    _df = pd.DataFrame([check_nan(d) for d in df[column]])
    _df.columns = [f"{alias}_{c}" for c in _df.columns]
    df = df.join(_df)
    if drop:
        df = df.drop(columns=[column], axis=1)
    return df


def unnest(df, column, rejoin_parent=False):
    """
    Expand a child field into a dataframe and optionally add back parent header fields
    """

    def check_nan(d):
        if pd.isnull(d):
            return {}
        return d

    f = df.explode(column)[[column]].join(df.drop(column, 1)).reset_index()
    f = pd.DataFrame([check_nan(d) for d in f[column]], index=f["index"])

    if rejoin_parent:
        # return the expansion and keep everything on the parent except for the old now
        # expanded col
        return df.join(f, lsuffix="_parent").drop(column, 1)
    return f


def filter_by_dict_field_keys(x, filters, type_maps=None):
    """
    Filter a dict field by keys for nested field filtration
    """

    def map_type(k, v):
        """
        If we have a function that can be used to cast safely, we can use it here to
        cast
        This is used for basic nested casting
        """
        if type_maps is not None:
            if k in type_maps:
                return type_maps[k](v) if v is not None else v
        return v

    def _filter_dict(x):
        return {k: map_type(k, v) for k, v in dict(x).items() if k in filters}

    return [_filter_dict(d) for d in x]


def array_as_comma_seperated_list(x):
    return (
        _try_eval(x)
        if not isinstance(x, list)
        else ",".join([(str(item) for item in x)])
    )


def remove_object_types(data):
    for c in data.columns:
        if str(data[c].dtype) == "object":
            data[c] = data[c].astype("string")

    return data


def coerce_to_avro_schema(df, avro_schema):
    ops = casting_functions_for_avro_types(avro_schema)

    for k, v in ops.items():
        if k in df.columns:
            try:
                df[k] = df[k].map(v)
            except Exception as ex:
                print(
                    f"Failed to apply map {v} to column {k} which may lead to problems "
                    "down the line"
                )
    return df


def casting_functions_for_avro_types(avro_schema):
    """
    python or pandas types from avro type - use for coerce to schema
    """

    schema = {}
    for item in avro_schema["fields"]:
        schema[item["name"]] = item["type"]

    def is_type(i, t):
        if isinstance(i, list):
            return t in i
        else:
            return i == t

    def _op_for_avro_type(t):
        if is_type(t, "boolean"):
            return bool
        if is_type(t, "float"):
            return float
        if is_type(t, "int") or is_type(t, "long"):
            return int
        if isinstance(t, dict):
            return lambda x: x
        return str

    return {k: _op_for_avro_type(v) for k, v in schema.items()}


def decompose(df, flatten_fields, explode_fields, schema_map=None):
    # as we look into each dataframe by name, we look up the schema by map and coerce
    # for each field in explode, pop and explode
    # yield, name, df
    # for each field in flatten fields, flatten
    # yield "parent", df

    pass


def dataframe_from_airtable(at, fields=None):
    fields = fields or []

    if not at:
        return pd.DataFrame(columns=["id", "createdTime"] + list(fields))

    df = pd.DataFrame(
        [
            {**rec["fields"], **{"id": rec["id"], "createdTime": rec["createdTime"]}}
            for rec in at
        ]
    )

    for missing_field in set(fields) - set(df.columns):
        df[missing_field] = np.nan

    return df


def query_airtable(connection, filter_formula, fields, sort=None):
    if sort is not None:
        records = connection.all(formula=filter_formula, fields=fields, sort=sort)
    else:
        records = connection.all(formula=filter_formula, fields=fields)

    return dataframe_from_airtable(records, fields)


def df_to_dict_of_lists(df, key, values_as_df=False):
    odf = (
        pd.DataFrame(
            [
                {key: group, "records": dfm.to_dict("records")}
                for group, dfm in df.groupby(key)
            ]
        )
        .set_index(key)
        .to_dict("index")
    )

    return {
        k: (pd.DataFrame(v["records"]) if values_as_df else v["records"])
        for k, v in odf.items()
    }


def title_map_keys(input, mapper=False):
    title_map = {k: k.replace("_", " ").title() for k, v in input.items()}

    return title_map if mapper else {title_map[k]: v for k, v in input.items()}


def separate_header_columns(df, parent_fk_name, child_field, key_col="key", **kwargs):
    """
    Given a dataframe with nesting, we can separate
    - the header fields are grouped and but into a separate dataframe
    - the child types are given a foreign key pointing to the parents let with the supplied name
    - the meta type is not really needed here but would be used to define the type

    the key col is assumed as a convention as the primary row key on all types
    """
    # we are exploding one column and everything else is assumed a header
    header_fields = [c for c in df.columns if c != child_field]
    # add a foreign key
    df[parent_fk_name] = df[key_col]
    # return both, the child should have a FK based on logic above
    return (
        df[header_fields],
        unnest(df[[child_field, parent_fk_name]], child_field, True),
    )


def _isnan(x: Any) -> bool:
    """Check if a value is NaN."""
    return isinstance(x, float) and math.isnan(x)


def remap_df_to_dicts(
    df: pd.DataFrame, mapping: Dict[str, Union[str, Dict[str, callable]]]
) -> List[Dict]:
    """
    Remaps a dataframe to a list of dicts based on a mapping. NaNs are converted to None.
    Supports one level of nesting via the `__` separator.

    Example:
        df = pd.DataFrame({ 'ONE': np.zeros(7), 'TWO': np.zeros(7), 'THREE': np.zeros(7) })
        mapping = {
            'ONE': 'one',
            'TWO': { 'two': lambda x: x + 1 },
            'THREE': 'metadata__three',
        }
        remap_df_to_dicts(df, mapping)
        # [{'one': 0.0, 'two': 1.0, 'metadata': {'three': 0.0}},
        # ...
        # {'one': 0.0, 'two': 1.0, 'metadata': {'three': 0.0}}]
    """
    DICT_SPLIT = "__"
    df2 = df.copy()
    for k, v in mapping.items():
        if isinstance(v, dict):
            func = list(v.values())[0]
            df2[k] = df2[k].apply(func)

    transformed = []
    for _, row in df2.iterrows():
        result = defaultdict(dict)
        for k, v in mapping.items():
            if isinstance(v, dict):
                v = list(v.keys())[0]

            if DICT_SPLIT in v:
                split = v.split(DICT_SPLIT)
                result[split[0]][split[1]] = None if _isnan(row[k]) else row[k]
            else:
                result[v] = None if _isnan(row[k]) else row[k]
        transformed.append(dict(result))
    return transformed
