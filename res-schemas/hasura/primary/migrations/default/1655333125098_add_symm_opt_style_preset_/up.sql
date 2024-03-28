UPDATE dxa.flow_node_config set details ='{ 
    "instructions": [ 
        { 
            "type": "EXPORT", 
            "export_type": "TURNTABLE", 
            "output_file_name": "turntable.zip", 
            "number_of_snapshots": 24 
        }, 
        { 
            "type": "MUTATION", 
            "mutation_type": "DELETE_TRIMS", 
            "output_file_name": "placeholder2" 
        }, 
        { 
            "type": "EXPORT", 
            "bundle_type": "style", 
            "export_type": "DXA_ASSET_BUNDLE", 
            "output_file_name": "asset_bundle.zip",
            "set_seam_allowance_symmetrically": false
        } 
    ], 
    "remote_destination": "s3://res-temp-public-bucket/style_assets_dev/", 
    "remote_destination_subfolder_type": "JOB_ID" 
}'
where key = 'techpack_v0';
