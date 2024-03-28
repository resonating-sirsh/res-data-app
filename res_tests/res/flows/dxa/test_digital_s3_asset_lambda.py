"""
this is added more for in source documentation
for the meta one system, we listen to certain s3 paths and then relay
this function is a lambda 
https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/DigitalAssetsCreated?tab=code

that is invoked when we added assets to s3

below is a way to check the logic

copy files to a test location to trigger- you should see a record in kafka:> res_infrastructure.flows.digital_assets

 s3.copy('s3://meta-one-assets-prod/bodies/3d_body_files/tk_6121/v3/extracted/dxf/TK-6121-V3-3D_BODY.dxf',
         's3://meta-one-assets-prod/bodies/3d_body_files/test_123/v3/extracted/dxf/TK-6121-V3-3D_BODY.dxf'
       )

"""

import json

event = {
    "Records": [
        {
            "eventVersion": "2.2",
            "eventSource": "aws:s3",
            "awsRegion": "us-west-2",
            "eventTime": "The time, in ISO-8601 format, for example, 1970-01-01T00:00:00.000Z, when Amazon S3 finished processing the request",
            "eventName": "event-type",
            "userIdentity": {
                "principalId": "Amazon-customer-ID-of-the-user-who-caused-the-event"
            },
            "requestParameters": {
                "sourceIPAddress": "ip-address-where-request-came-from"
            },
            "responseElements": {
                "x-amz-request-id": "Amazon S3 generated request ID",
                "x-amz-id-2": "Amazon S3 host that processed the request",
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "ID found in the bucket notification configuration",
                "bucket": {
                    "name": "bucket-name",
                    "ownerIdentity": {
                        "principalId": "Amazon-customer-ID-of-the-bucket-owner"
                    },
                    "arn": "bucket-ARN",
                },
                "object": {
                    "key": "bodies/kt_2011/pattern_files/body_kt_2011_v9_pattern.dxf",
                    "size": "object-size in bytes",
                    "eTag": "object eTag",
                    "versionId": "object version if bucket is versioning-enabled, otherwise null",
                    "sequencer": "a string representation of a hexadecimal value used to determine event sequence, only used with PUTs and DELETEs",
                },
            },
            "glacierEventData": {
                "restoreEventData": {
                    "lifecycleRestorationExpiryTime": "The time, in ISO-8601 format, for example, 1970-01-01T00:00:00.000Z, of Restore Expiry",
                    "lifecycleRestoreStorageClass": "Source storage class for restore",
                }
            },
        }
    ]
}


def lambda_handler(event, context={}):
    url = "https://datadev.resmagic.io/kgateway/submitevent"
    records = event.get("Records")
    if records:
        for r in records:
            s3 = r.get("s3")
            path = "s3://" + s3["bucket"]["name"] + "/" + s3["object"]["key"]
            parts = path.split("/")[-1].split("_")
            key = ""
            type = ""
            if parts[-1].split(".")[-1] == "dxf":

                type = "body_astm_file"
                flow = "2d"

                if "3d_body_files" in path:
                    # example s3://meta-one-assets-prod/bodies/3d_body_files/tk_6121/v3/extracted/dxf/TK-6121-V3-3D_BODY.dxf
                    flow = "3d"
                    key = path.split("/")[5] + "-" + path.split("/")[6]
                    body = path.split("/")[5]
                else:
                    # example s3://meta-one-assets-prod/bodies/kt_2011/pattern_files/body_kt_2011_v9_pattern.dxf
                    key = parts[1] + "-" + parts[2] + "-" + parts[3]
                    body = path.split("/")[4]

                data = {
                    "body_code": body.upper().replace("_", "-"),
                    "flow": flow,
                    "path": path,
                }
                print("relay to res-connect", data)

                try:
                    murl = "https://datadev.resmagic.io/res-connect/meta-one/body"
                    print(murl)
                    # r = urllib3.PoolManager().request("POST", murl, body=json.dumps(data),  headers={'Content-Type': 'application/json'} )

                except Exception as ex:
                    print("failure in post")
                    pass

            elif "rasterized" in path:
                # example "s3://meta-one-assets-prod/color_on_shape/cc_6001/v10/elecqd/2zzsm/rasterized_files/file.png"
                key = "-".join(path.split("/")[4:6]).upper().replace("_", "-")
                type = "rasterized_marker_file"
            else:
                continue

            data = {
                "version": "1.0.0",
                "process_name": "default",
                "topic": "res_infrastructure.flows.digital_assets",
                "data": {
                    "key": key,
                    "path": path,
                    "type": type,
                    "status": "OK",
                    "created_at": "",
                },
            }

            print(url, data)
            # r = urllib3.PoolManager().request("POST", url, body=json.dumps(data),  headers={'Content-Type': 'application/json'} )

    return {"statusCode": 200, "body": json.dumps("sent payload")}


lambda_handler(event)
