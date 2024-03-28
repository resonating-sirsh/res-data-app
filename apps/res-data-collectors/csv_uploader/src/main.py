from res.connectors.box.BoxConnector import BoxConnector
from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient
from res.utils.logging import logger
import pandas
import os

CSV_folder = "146677979801"
boxcon = BoxConnector()

if os.getenv("RES_ENV") == "production":
    locale = "IAMCURIOUS_PRODUCTION"
else:
    locale = "IAMCURIOUS_DEVELOPMENT"
# File tree can probably be replaced by a snowflake table in the future
filetree = [
    {
        "Name": "Pinterest Campaigns",
        "Snowflake": "IAMCURIOUS_DB."
        + locale
        + ".RES_DATA_COLLECTORS_CSV_UPLOADS_PINTEREST_CAMPAIGNS",
        "Box_ID": "146678775716",
        "Snowflake_Table": "RES_DATA_COLLECTORS_CSV_UPLOADS_PINTEREST_CAMPAIGNS",
    }
]


def main():
    client = boxcon._get_client()
    snow_client = ResSnowflakeClient()

    for each in filetree:

        root_folder = client.folder(folder_id=each["Box_ID"]).get_items(
            direction="desc", sort="date"
        )

        if os.getenv("BACKFILL") == "true":
            filenames = []
        else:
            out = snow_client.execute(
                f'select distinct DATA:"Box_File_Name" from {each["Snowflake"]}'
            )
            filenames = list(next(zip(*out)))

        for index, name in enumerate(filenames):
            filenames[index] = name.replace('"', "")

        for brand in root_folder:

            folder = client.folder(folder_id=brand.id).get_items(
                direction="desc", sort="date"
            )

            for item in folder:

                if item.name in filenames:
                    logger.debug("skipping " + item.name)
                    continue
                else:
                    logger.debug("Processing " + item.name)
                boxcon.download_file(str(item.id), "temp.csv")

                DF = pandas.read_csv("temp.csv").fillna("None")

                DF.columns = [
                    col.replace(" ", "_")
                    .replace("%", "")
                    .replace("-", "_")
                    .replace("(", "")
                    .replace(")", "")
                    .replace(".", "")
                    .replace("/", "_")
                    for col in DF.columns
                ]

                DF["Box_File_Name"] = str(item.name)
                DF["Brand"] = str(brand.name)
                json = DF.to_dict(orient="records")
                # logger.info()
                data = []
                for message in json:
                    if "Clicks" in message:
                        if not isinstance(message["Clicks"], int):

                            message["Clicks"] = int(message["Clicks"].replace(",", ""))

                    message["primary_key"] = "{}-{}-{}-{}".format(
                        message["Campaign_ID"],
                        message["Brand"],
                        message["Date"],
                        message["Box_File_Name"],
                    )
                    data.append(message)
                print("now trying to push data")
                snow_client.load_json_data(data, each["Snowflake_Table"], "primary_key")


if __name__ == "__main__":
    main()
