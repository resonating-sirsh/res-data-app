INSERT INTO "create"."asset_type" ("name", "description")
VALUES
    ('MEDIA_RENDER', 'Image render'),
    ('MEDIA_RENDER_RAY_TRACE', 'Ray traced image render'),
    ('MEDIA_3D_SCENE', '3d glb, ie. a rotatable object. Like a turntable, but actually 3d'),
    ('DXA_ASSET_BUNDLE', 'Asset bundle for automations in the DxA flow'),
    ('TURNTABLE', 'A collection of [n] zipped images of a body or style');
