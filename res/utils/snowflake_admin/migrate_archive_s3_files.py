import boto3
from s3_helpers import *

new_bucket = "res-data-development"
s3 = boto3.resource("s3")
bucket = s3.Bucket(new_bucket)
bases = get_bases()
from_prefix = "data_sources/Airtable/RS/"
dates = [date.split("/")[-2] for date in get_s3_folders(from_prefix)]

# Get list of existing files
result = client.list_objects(
    Bucket=new_bucket, Prefix="airtable_archives/", Delimiter="/"
)
existing_dates = [o.get("Prefix") for o in result.get("CommonPrefixes")]
existing_files = {}
for existing_date in existing_dates:
    result = client.list_objects(Bucket=new_bucket, Prefix=existing_date, Delimiter="/")
    existing_bases = [o.get("Prefix") for o in result.get("CommonPrefixes")]
    for existing_base in existing_bases:
        base = existing_base.split("/")[-2]
        t_date = existing_base.split("/")[-3]
        if base not in existing_files:
            existing_files[base] = [t_date]
        else:
            existing_files[base].append(t_date)

for base in bases:
    total_tables = 0
    for table in bases[base]:
        total_tables += 1
    table_count = 0
    print(total_tables)
    for table in bases[base]:
        table_count += 1
        percent = table_count / total_tables
        print(f"{base}.{table} {percent}")
        for date in dates:
            if base not in existing_files or date not in existing_files[base]:
                for file in [
                    f"{from_prefix}{date}/bases/{base}/tables/{table}/records.json",
                    f"{from_prefix}{date}/bases/{base}/tables/{table}/table.json",
                    f"{from_prefix}{date}/bases/{base}/base.json",
                ]:
                    copy_source = {"Bucket": BUCKET, "Key": file}
                    new_file = file.replace(
                        "data_sources/Airtable/RS/", "airtable_archives/"
                    ).replace("bases/", "")
                    try:
                        bucket.copy(copy_source, new_file)
                    except Exception as e:
                        if "Not Found" in str(e):
                            # print(f"Missing file: {file}")
                            pass
                        else:
                            raise
