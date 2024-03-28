import json, os, datetime, sys, boto3
from res.connectors.airtable.AirtableClient import ResAirtableClient
from res.utils import logger, secrets_client

# Generates a list of bases/tables that we want to archive and load into Snowflake
# Developers can manually trigger this using the following event payload options:
# {
#     "base": "appABCDE",
#     "tables": ["tblWXYZ"],
#     "start_date": "2021-01-01",
#     "end_date": "2021-10-01"
# }
# If base and tables are specified, it will only load the specified tables.
# If base is specified, it will load all tables in that base.
# If start and end dates are specified, it will load all the data for dates in
# that range

RES_ENV = os.getenv("RES_ENV", "development")
BASE_IDS = [
    "appjmzNPXOuynj6xP",  # Res.magic
    "appfaTObyfrmPHvHc",  # Fulfillment
    "appoaCebaYWsdqB30",  # Purchasing
    "appa7Sw0ML47cA8D1",  # Res.Meta.One
    "appc7VJXjoGsSOjmw",  # Res.meta
    "apprcULXTWu33KFsh",  # Print
    "appqtN4USHTmyC6Dv",  # DxA One Markers
    "app35ALADpo8gEqTy",  # Healing
    "appoftTcaySVg5DXz",  # Infrastructure
    "applaK5MLcPfcgCla",  # People and culture
    "appSvWBhBi3W4hl2B",  # Acquisition
    "appyIrUOJf8KiXD1D",  # Cut
    "appKGWiGPT52oCPC4",  # Material Swap
    "appX3oQCt0fMbjejA",  # Color Specs
    "app70YvSyxW4d0vJ3",  # ReFramework
    "appC4z45QWwe7ZWdc",  # Material Cost (Rolls)
    "app5H5jVK1CPZjxYL",  # Sell
    "appWxYpvDIs8JGzzr",  # Healing App
    "appVhiIf26Ju8o1ht",  # Prep
    "appSiJQaixEciJGyY",  # res.Labor
    "appHppdkO4f7GlXuh",  # Inventory
    "appH5S4hIuz99tAjm",  # Production
    "appKhgje0Ui2vZ15A",  # Managed services
    "appQ8kWyq5LbYBXFo",  # Order Tracker
    "app1FBXxTRoicCW8k",  # Color Chips Inspection
    "appHqaUQOkRD8VbzX",  # Scheduling Matrix
    "app6tdm0kPbc24p1c",  # Contracts
    "appziWrNVKw5gpcnW",  # res.Finance
    "appwW1RF7xUYRRYhq",  # Sew.ONE
    "appxdguqC15Sl9UPE",  # res.Lexicon
    "app8NAtlIIJHOLLJT",  # Supervisory Control & Data Acquisition (SCADA)
    "appd9qqPELi5zOBrx",  # res.Trims
    "appbl3XRRi5DPx5Rh",  # Recursos Humanos (Confidencial)
]

if __name__ == "__main__":
    try:
        event = json.loads(sys.argv[1])

        # Set up airtable client. Replace default personal access token with data
        # team specific one. This helps to prevent rate limiting
        DATA_TEAM_AIRTABLE_READER_PAT = secrets_client.get_secret(
            "DATA_TEAM_AIRTABLE_READER_PAT"
        )
        client = ResAirtableClient()
        client.api_key = DATA_TEAM_AIRTABLE_READER_PAT
        client.bearer_header = {
            "Authorization": f"Bearer {DATA_TEAM_AIRTABLE_READER_PAT}"
        }
        boto_client = boto3.client("s3")

        # Establish which dates this run is for (historical or latest)
        if "start_date" in event and "end_date" in event:
            result = boto_client.list_objects(
                Bucket=f"res-data-{RES_ENV}", Prefix="airtable_archives/", Delimiter="/"
            )
            possible_dates = [
                o.get("Prefix").split("/")[-2] for o in result.get("CommonPrefixes")
            ]
            run_dates = []
            for date in possible_dates:
                if date >= event["start_date"] and date < event["end_date"]:
                    run_dates.append(date)
            is_latest = False
        else:
            # Just run for latest
            run_dates = [
                datetime.datetime.strftime(
                    datetime.datetime.now(), "%Y_%m_%d__%H_%M_%S"
                )
            ]
            is_latest = True

        # Set up base info to send to downstream pods
        if "base" in event:
            bases = [
                {
                    "is_latest": is_latest,
                    "dates": run_dates,
                    "base": event["base"],
                    "tables": event.get("tables", []),
                }
            ]
        else:
            bases = []
            for base_id in BASE_IDS:
                bases.append(
                    {
                        "is_latest": is_latest,
                        "dates": run_dates,
                        "base": base_id,
                        "tables": [],
                    }
                )

        # Fill in tables for each base if the tables have not been specified
        for base in bases:
            if not base["tables"]:
                logger.info(f"Pulling schema for base: {base}")
                base_schema = client.get_base_schema(base["base"])
                temp_tables = []
                for table in base_schema["tables"]:
                    if "table" not in event or event["table"] == table["id"]:
                        temp_tables.append(table["id"])
                # If no tables were specified, use all tables found in the base's schema
                base["tables"] = temp_tables

        with open("/app/dump.txt", "w") as f:
            logger.info(f"Schema recorded: {bases}")
            json.dump(bases, f)
    except Exception as e:
        logger.error(
            "Error generating tables for Airtable to Snowflake ELT! {}".format(str(e))
        )
        raise
