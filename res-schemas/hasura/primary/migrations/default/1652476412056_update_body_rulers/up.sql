UPDATE platform.job_preset set details ='{
    "instructions": [
        {
            "type": "EXPORT",
            "export_type": "DXA_ASSET_BUNDLE",
            "techpack_name": "body-asset-bundle",
            "output_file_name": "asset_bundle.zip"
        }
    ],
    "remote_destination": "s3://res-temp-public-bucket/style_assets_dev/",
    "remote_destination_subfolder_type": "JOB_ID"
}'
where key = 'body_bundle_v0';
