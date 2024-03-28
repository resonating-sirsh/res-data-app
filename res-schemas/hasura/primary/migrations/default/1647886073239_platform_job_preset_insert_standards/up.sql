insert into platform.job_type (type) VALUES ('DXA_EXPORT_BODY_BUNDLE')
ON CONFLICT DO NOTHING;

insert into platform.job_preset (key, name, version, job_type, details) VALUES
('techpack_v0', 'techpack', 0, 'DXA_EXPORT_ASSET_BUNDLE', '{
    "instructions": [
        {
            "type": "EXPORT",
            "export_type": "TURNTABLE",
            "output_file_name": "turntable.zip",
            "number_of_snapshots": 24
        },
        {
            "type": "EXPORT",
            "export_type": "DXA_ASSET_BUNDLE",
            "techpack_name": "tp-p2f-renders",
            "output_file_name": "asset_bundle.zip"
        }
    ],
    "remote_destination": "s3://res-temp-public-bucket/style_assets_dev/",
    "remote_destination_subfolder_type": "JOB_ID"
}'),

('ruler_v0', 'ruler', 0, 'DXA_EXPORT_BODY_BUNDLE', '{
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
}'),

('onboarding_v0', 'onboarding', 0, 'BRAND_ONBOARDING', '{
    "instructions": [
        {
            "dpi": 80,
            "type": "EXPORT",
            "export_type": "MEDIA_RENDER",
            "include_avatar": false,
            "camera_position": {
                "x": -250,
                "y": -250,
                "z": 100
            },
            "output_file_name": "left_front.png"
        },
        {
            "dpi": 80,
            "type": "EXPORT",
            "export_type": "MEDIA_RENDER",
            "include_avatar": false,
            "camera_position": {
                "x": 0,
                "y": -400,
                "z": 100
            },
            "output_file_name": "front.png"
        },
        {
            "dpi": 80,
            "type": "EXPORT",
            "export_type": "MEDIA_RENDER",
            "include_avatar": false,
            "camera_position": {
                "x": 250,
                "y": -250,
                "z": 100
            },
            "output_file_name": "right_front.png"
        },
        {
            "dpi": 300,
            "type": "EXPORT",
            "export_type": "MEDIA_3D_SCENE",
            "include_avatar": false,
            "output_file_name": "3d.glb"
        },
        {
            "dpi": 800,
            "type": "EXPORT",
            "export_type": "MEDIA_RENDER_RAY_TRACE",
            "include_avatar": false,
            "output_file_name": "render.png"
        }
    ],
    "remote_destination": "s3://res-temp-public-bucket/style_assets_dev/",
    "remote_destination_subfolder_type": "JOB_ID",
    "should_hide_previous_style_assets": true
}'),

('dxf_v0', 'dxf', 0, 'DXA_EXPORT_ASSET_BUNDLE', '{
    "instructions": [
        {
            "type": "MUTATION",
            "mutation_type": "REMOVE_SYMMETRY",
            "output_file_name": "blablabla"
        },
        {
            "type": "EXPORT",
            "export_type": "SCAN",
            "output_file_name": "scan.json"
        },
        {
            "type": "EXPORT",
            "export_type": "DXA_ASSET_BUNDLE",
            "techpack_name": "dxf",
            "output_file_name": "dxf_bundle.zip"
        }
    ],
    "remote_destination": "s3://res-temp-public-bucket/style_assets_dev/",
    "remote_destination_subfolder_type": "JOB_ID"
}')
ON CONFLICT DO NOTHING;
