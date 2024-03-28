from ast import literal_eval
import re
import pandas as pd
from . import dataframes
from . import logger


def clean_field_name(name):
    """
    i have tried to match how it seems we map to Snowflake but im not sure about this - logic seemed weird
    """
    name = name.replace('"', "")
    if re.match(r"^[\w ]+$", name) is None:
        name = re.sub(r"[^\w]+", "_", name)

    return name.rstrip("_")


def get_field_names(df, resolve_name_conflicts=True):
    """
    A conventional format creates key value cell values with metadata of airtable cells
    Cells can have field ids and names and there can unfortunately be a many-many relationship between then
    This function preserves copies of the same names giving precedence to common values

    We may change this behaviour but one way or another we need a strategy about how to format cell names.
    """

    field_to_name_mapping = (
        df[["field_id", "column_name", "timestamp"]]
        .groupby("column_name")
        .max()
        .sort_values("timestamp")
    )

    field_to_name_mapping = (
        field_to_name_mapping.drop_duplicates(subset=["field_id"], keep="last")
        .drop("timestamp", 1)
        .reset_index()[["field_id", "column_name"]]
    )

    # TODO: if there is a conflict in the image of the mapping then we need to apply a conflict resolution
    # when field ids conflict we can only choose one name for the same field but if two fields map to the same name we need to make a choice
    # and this choice need to be respected on all partitions.

    return dict(field_to_name_mapping.values)

    # for now ignoring conflicts as their is no clearcut resolution strategy
    # we just use the latest name for a field for old and new fields
    return field_to_name_mapping

    if not resolve_name_conflicts:
        return field_to_name_mapping

    # determine how often each field and column combo appear
    f = (
        df[["field_id", "column_name", "cell_value"]]
        .groupby(["field_id", "column_name"])
        .count()
    )

    f = f.reset_index().set_index("column_name").sort_values("field_id")

    # determine how often columns appear per field (inheriting from previous grouping)
    name_counter = (
        f.reset_index().groupby(["column_name"]).count().sort_values("cell_value")
    )
    name_counter = f.join(name_counter, rsuffix="counts").sort_values(
        "cell_valuecounts"
    )

    # get conflicts: fields ids mapped to different names
    conflicts = (
        name_counter[name_counter["field_idcounts"] > 1]
        .reset_index()
        .sort_values(["column_name", "cell_value"], ascending=[False, False])
    )
    # for each group giving prec to the least populated, fill prev into next and drop list
    conflicts["rank"] = conflicts.groupby("column_name")["cell_value"].rank() - 1

    # bump any column names mapped to the same field (we could do other things)
    for _, grp in conflicts[conflicts["rank"] > 0].groupby("column_name"):
        for i, rec in enumerate(grp.to_dict("records")):
            key = rec["field_id"]
            field_to_name_mapping[key] = field_to_name_mapping[key] + f".{i+1}"

    return field_to_name_mapping


def _handle_dates_in_place(df, field_id, names):
    try:
        # be careful here we are making a bunch of assumptions
        df[field_id] = pd.to_datetime(
            df[field_id],
            errors="coerce",
            utc=True,
            infer_datetime_format=True,
        )
        # TODO: do this in a smarter way
        # try:
        #     df[field_id] = df[field_id].dt.tz_localize(None)
        # except:
        #     # when already tz aware
        #     df[field_id] = df[field_id].dt.tz_convert(None)
        #     pass
    except:
        logger.debug(f"failed to parse column {names[field_id]} as date.")


def remove_pseudo_lists(s):
    if not s or "[" not in str(s):
        return s
    if not isinstance(s, list) and pd.isnull(s):
        return s

    if "[" in str(s) and "]" in str(s):
        try:
            if isinstance(s, list) and len(s) == 1:
                return s[0]
            # try parse the list
            l = literal_eval(s)
            # if there is only one item return it
            if len(l) == 1:
                return l[0]
        except Exception as ex:

            pass
    # otherwise return whatever it is
    return s


def map_field_types(airtable_table, data):
    def pandas_type_from_airtable_type(type_string):
        type_string = str(type_string).lower() if type_string else ""
        # we could be more specific
        if "time" in type_string or "date" in type_string:
            return "datetime"
        if "multi" in type_string:
            return dataframes.coerce_object
        if "singleLineText" in type_string:
            return "string"
        if "count" in type_string:
            return dataframes.coerce_int
        if "checkbox" in type_string:
            return dataframes.coerce_bool
        if type_string in ["percent", "currency"]:
            return dataframes.coerce_float
        if "percent" in type_string:
            return dataframes.coerce_float
        # we would need more info for these
        if "number" in type_string:
            return dataframes.coerce_float
        # this is the pandas string type and its different to using str which can be represented as objects
        # todo we may need other types of object/variant here
        return "string"

    field_metadata = airtable_table.get_extended_field_metadata()
    # use the return type from airtable to lookup a coerce function - we could use other metadata too
    # generally we get an actual func that can be evaluated but the string case is just a string and we use astype(s) instead of map(f)
    field_metadata["func"] = field_metadata["return_type"].map(
        pandas_type_from_airtable_type
    )

    # we use specific functions to apply formatting logic - not sure if we can get away with this
    mapping_functions = dict(field_metadata[["id", "func"]].values)
    names = dict(field_metadata[["id", "name"]].values)

    # manage types/values
    data = data.applymap(dataframes.coerce_nan)

    data = data.applymap(remove_pseudo_lists)

    for field_id in data.columns:
        if field_id in mapping_functions:
            # logger.debug(f'converting field {names[field_id]}')
            func = mapping_functions[field_id]
            if str(func) == "datetime":
                _handle_dates_in_place(data, field_id, names)
            elif str(func) == "string":
                data[field_id] = data[field_id].astype("string")
            else:
                data[field_id] = data[field_id].map(func)
        else:
            logger.debug(
                f"missing {field_id} from extended table metadata"
            )  # <- look into why this happens. its not wrong but make sure we know what happened
            # what can we do, we can try to guess types like _ts but then we should check what they are in snowflake too

    # dont allow object types - metadata should be used to coerce desired types and this is a safety

    data = dataframes.remove_object_types(data)

    return data


def strip_emoji(text):
    if text is not None:
        RE_EMOJI = re.compile(
            "([\U00002600-\U000027BF])|([\U0001f300-\U0001f64F])|([\U0001f680-\U0001f6FF])"
        )
        return RE_EMOJI.sub(r"", text).lstrip().rstrip().lstrip()
    return None


# known issues:
# File "/app/res/connectors/airtable/AirtableConnector.py", line 423, in _update_partition_file
# {"table_id":"tblSYU8a71UgbQve4","base_id":"apprcULXTWu33KFsh"}}
# pyarrow.lib.ArrowTypeError: ("Expected bytes, got a 'float' object", 'Conversion failed for column MS_Steamer_Steam with type object')
