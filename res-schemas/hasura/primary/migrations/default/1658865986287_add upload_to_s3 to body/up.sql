UPDATE dxa.flow_node_config set details ='{ 
    "instructions": [ 
        { 
            "type": "EXPORT", 
            "bundle_type": "body", 
            "export_type": "DXA_ASSET_BUNDLE", 
            "output_file_name": "asset_bundle.zip", 
            "set_seam_allowance_symmetrically": false,
            "upload_to_s3": false
        } 
    ], 
    "remote_destination": "s3://res-temp-public-bucket/style_assets_dev/", 
    "remote_destination_subfolder_type": "JOB_ID" 
}'
where key = 'body_bundle_v0';
