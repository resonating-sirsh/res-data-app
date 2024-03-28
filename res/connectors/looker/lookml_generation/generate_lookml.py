import os, datetime, boto3, json, random, string, re
from res.utils import logger, secrets_client
from res.connectors.looker.lookml_generation.s3_helpers import *
from res.connectors.looker.lookml_generation.airtable_mappings import AIRTABLE_TO_LOOKER
from res.connectors.airtable.AirtableClient import ResAirtableClient
from res.connectors.looker.lookml_generation.lookml_templates import (
    VIEW_TEMPLATE,
    DIMENSION_TEMPLATE,
    TIME_DIMENSION_TEMPLATE,
    MEASURE_COUNT_TEMPLATE,
    MEASURE_TEMPLATE,
    MODEL_TEMPLATE,
    EXPLORE_TEMPLATE,
    EXPLORE_JOIN_TEMPLATE,
)
import lookml

proj = lookml.Project(
    repo="resonance/iamcurious-looker-v2",
    access_token=secrets_client.get_secret("LOOKER_GIT_ACCESS_TOKEN"),
)

RES_ENV = os.getenv("RES_ENV", "development")
RES_ENV = "production"


if __name__ == "__main__":
    bases = get_bases()
    latest_date = get_s3_folders(S3_PREFIX)[-1].split("/")[-2]
    for base in bases:
        # Open base metadata
        base_key = f"data_sources/Airtable/RS_{RES_ENV}/{latest_date}/{base}/base.json"
        print(base)
        base_metadata = get_records(base_key)
        clean_base_name = ResAirtableClient.clean_name(base_metadata["name"])
        linkages = {}
        for table in bases[base]:
            for metadata_table in base_metadata["tables"]:
                if metadata_table["id"] == table:
                    clean_table_name = ResAirtableClient.clean_name(
                        metadata_table["name"]
                    )
                    columnar_table_name = (
                        f"AIRTABLE__{clean_base_name}__{clean_table_name}"
                    )
                    dimensions = []
                    measures = [MEASURE_COUNT_TEMPLATE]
                    unique_fields = []
                    for field in metadata_table["fields"]:
                        looker_type = AIRTABLE_TO_LOOKER[field["config"]["type"]]
                        clean_field = ResAirtableClient.clean_name(field["name"])
                        if clean_field in unique_fields:
                            original_field = field["name"]
                            print(
                                f"Found duplicate field! {clean_field} (Original : {original_field})"
                            )
                        elif clean_field == "":
                            print("Found empty field!")
                        elif looker_type in ["string", "yesno"]:
                            # Standard dimension
                            dimensions.append(
                                DIMENSION_TEMPLATE.format(
                                    dimension_name=clean_field,
                                    dimension_type=looker_type,
                                    column_name=clean_field,
                                )
                            )
                        elif looker_type in ["datetime"]:
                            # Date Dimension
                            dimensions.append(
                                TIME_DIMENSION_TEMPLATE.format(
                                    dimension_name=clean_field, column_name=clean_field
                                )
                            )
                        elif looker_type in ["number"]:
                            # Measure, add multiple types
                            for measure_type in ["average", "sum", "min", "max"]:
                                measures.append(
                                    MEASURE_TEMPLATE.format(
                                        measure_name=f"{clean_field}_{measure_type}",
                                        measure_type=measure_type,
                                        column_name=clean_field,
                                    )
                                )
                        elif looker_type in ["formula"]:
                            dimensions.append(
                                DIMENSION_TEMPLATE.format(
                                    dimension_name=clean_field,
                                    dimension_type="string",
                                    column_name=clean_field,
                                )
                            )
                        elif looker_type == "linkage":
                            if field["config"]["type"] != "multipleRecordLinks":
                                print(field)
                        unique_fields.append(clean_field)
                    table_sql = f"SELECT * FROM {columnar_table_name}"
                    view = VIEW_TEMPLATE.format(
                        view_name=columnar_table_name,
                        table_sql=table_sql,
                        dimensions="\n".join(dimensions),
                        measures="\n".join(measures),
                        custom_code="",
                    )
                    # print(view)
                    folder = f"views__airtable__{clean_base_name}"
                    file = f"{folder}/{columnar_table_name}.view.lkml"
                    print(file)
                    try:
                        newFile = proj.new_file(file)
                    except Exception as e:
                        if "already exists" in str(e):
                            newFile = proj.file(file)
                        else:
                            raise
                    try:
                        newFile + view
                        # proj.put(newFile)
                    except Exception as e:
                        print(view)
                        raise
        # multipleRecordLinks
        #     linkedTableId
        #     prefersSingleRecordLink
        #     inverseLinkFieldId

        # lookup
        #     recordLinkFieldId
        #     fieldIdInLinkedTable
        #     result
        #         'type': 'multipleRecordLinks'
        #         options:
        #             prefersSingleRecordLink

        # rollup
        #     recordLinkFieldId
        #     fieldIdInLinkedTable
        #     referencedFieldIds
        #     result
        #         'type': 'singleLineText'
