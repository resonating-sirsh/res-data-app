AIRTABLE_TO_LOOKER = {
    "singleLineText": "string",
    "multipleSelects": "string",
    "singleSelect": "string",
    "url": "string",
    "multilineText": "string",
    "button": "string",
    "singleCollaborator": "string",
    "phoneNumber": "string",
    "lastModifiedBy": "string",
    "richText": "string",
    "createdBy": "string",
    "email": "string",
    "multipleAttachments": "string",
    "checkbox": "yesno",
    # Dates and datetimes need to be converted to dimension groups
    "lastModifiedTime": "datetime",
    "createdTime": "datetime",
    "dateTime": "datetime",
    "date": "datetime",
    "autoNumber": "number",
    "number": "number",
    "currency": "number",
    "count": "number",
    "rating": "number",
    "percent": "number",
    "duration": "number",
    # For formulas, need to check result type
    "formula": "formula",
    # These are linkage records, don't add to any views
    "multipleRecordLinks": "linkage",
    "rollup": "linkage",
    "lookup": "linkage",
}

DIMENSION_TYPES = ["string", "yesno", "datetime"]
MEASURE_TYPES = ["number"]
JOIN_TYPES = ["linkage"]
