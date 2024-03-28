# these common types have assumed semantics and types
# we raise a warning if defining these types with different semantics or if we try to qualify them
common_fields = [
    "id",  # int
    "key",  # sting
    "status",  # string, enum
    "name",  # string
    "description",  # string,text
    "user",  # string
    "created",  # date
    "modified",  # date
    "count",  # int
    "sku",  # string
    "code",  # string
    "category",  # string
    "sub_Category",  # string
]


"""
The res meta cerberus schema
"""

# add fk on attributes for overriding the default in res.meta e.g. bodies.key could be bodies.KEY
# we try to avoid this by ingesting data using our schema and conventions
# if a child type is true, then there must be a meta type because its a complex type
# a type MUST have a key and a name is optional
# fields must have either a key or a name - we resolve to both when saving
RES_META_SCHEMA_V0 = """

"""


def _validate_single(res_schema):
    pass


def validate(res_schema_set):
    if not isinstance(res_schema_set, list):
        res_schema_set = [res_schema_set]

    validations = []
    for s in res_schema_set:
        validations.append(_validate_single(s))

    return validations
