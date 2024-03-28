UPDATE platform.job_preset SET details=
'{
    "instructions": [
        {
            "type": "EXPORT",
            "export_type": "DXA_ASSET_BUNDLE",
            "techpack_name": "tp-p2f-renders",
            "output_file_name": "asset_bundle.zip"
        }
    ],
    "remote_destination": "s3://res-temp-public-bucket/style_assets_dev/",
    "remote_destination_subfolder_type": "JOB_ID"
}'
where key='body_bundle_v0';
