import boto3, json, os

BUCKET = "iamcurious"
RES_ENV = os.getenv("RES_ENV", "production")
RES_ENV = "production"
S3_PREFIX = f"data_sources/Airtable/RS/"

client = boto3.client("s3")


def get_s3_folders(prefix):
    result = client.list_objects(Bucket=BUCKET, Prefix=prefix, Delimiter="/")
    folders = [o.get("Prefix") for o in result.get("CommonPrefixes")]
    return folders


def get_records(key):
    # Returns all records from a file in S3
    data = client.get_object(Bucket=BUCKET, Key=key)
    contents = data["Body"].read()
    return json.loads(contents)


def get_bases():
    # Returns all bases from latest S3 date
    latest_date = get_s3_folders(S3_PREFIX)[-1]
    print(latest_date)
    latest_date = "data_sources/Airtable/RS/2021_10_15__10_19_83/"
    bases = get_s3_folders(latest_date + "bases/")
    return_data = {}
    for base in bases:
        return_data[base.split("/")[-2]] = [
            table.split("/")[-2] for table in get_s3_folders(f"{base}tables/")
        ]
    # return return_data
    # For now just do print app
    return {
        "appjmzNPXOuynj6xP": return_data["appjmzNPXOuynj6xP"],  # Res.magic
        "appHppdkO4f7GlXuh": return_data["appHppdkO4f7GlXuh"],  # Inventory
        "appfaTObyfrmPHvHc": return_data["appfaTObyfrmPHvHc"],  # Fulfillment
        "appoaCebaYWsdqB30": return_data["appoaCebaYWsdqB30"],  # Purchasing
        "appa7Sw0ML47cA8D1": return_data["appa7Sw0ML47cA8D1"],  # Res.Meta.One
        "appc7VJXjoGsSOjmw": return_data["appc7VJXjoGsSOjmw"],  # Res.meta
        "apprcULXTWu33KFsh": return_data["apprcULXTWu33KFsh"],  # Print
        "appH5S4hIuz99tAjm": return_data["appH5S4hIuz99tAjm"],  # Production
    }


def get_tables(base):
    # Returns all tables for a given base
    latest_date = get_s3_folders(S3_PREFIX)[-1]
    tables = get_s3_folders(f"{latest_date}{base}/tables/")
    return [table.split("/")[-2] for table in tables]
