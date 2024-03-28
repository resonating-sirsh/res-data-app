__FIELDS__ = """
        id,
        asset_id,
        name,
        annotation_type,
        value,
        value2,
        label_style_id,
        artwork_uri,
        main_label_uri,
        size_label_uri,
        coordinate_x,
        coordinate_y,
        coordinate_z,
        bill_of_material_id,
        supplier_type,
        created_at,
        updated_at
"""

UPSERT_BODY_REQUEST_ANNOTATION = (
    """
    INSERT INTO meta.body_request_asset_annotations (
    """
    + __FIELDS__
    + """
    ) VALUES (
        %(id)s,
        %(asset_id)s,
        %(name)s,
        %(annotation_type)s,
        %(value)s,
        %(value2)s,
        %(label_style_id)s,
        %(artwork_uri)s,
        %(main_label_uri)s,
        %(size_label_uri)s,
        %(coordinate_x)s,
        %(coordinate_y)s,
        %(coordinate_z)s,
        %(bill_of_material_id)s,
        %(supplier_type)s,
        %(created_at)s,
        %(updated_at)s
    ) ON CONFLICT (id) DO UPDATE SET
        asset_id = %(asset_id)s,
        name = %(name)s,
        annotation_type = %(annotation_type)s,
        value = %(value)s,
        value2= %(value2)s,
        label_style_id = %(label_style_id)s,
        artwork_uri = %(artwork_uri)s,
        main_label_uri = %(main_label_uri)s,
        size_label_uri = %(size_label_uri)s,
        coordinate_x = %(coordinate_x)s,
        coordinate_y = %(coordinate_y)s,
        coordinate_z = %(coordinate_z)s,
        updated_at = %(updated_at)s,
        bill_of_material_id = %(bill_of_material_id)s,
        supplier_type = %(supplier_type)s
"""
)

FIND_BY_ID_BODY_REQUEST_ANNOTATION = (
    """
    SELECT
"""
    + __FIELDS__
    + """
    FROM meta.body_request_asset_annotations
    WHERE id = %(id)s AND deleted_at IS NULL
"""
)

FIND_BODY_REQUEST_ANNOTATION = (
    """SELECT \n"""
    + __FIELDS__
    + """
    FROM meta.body_request_asset_annotations
    WHERE
        asset_id = %(asset_id)s
        AND (%(ids)s IS NULL OR id IN (%(ids)s))
        AND (%(annotation_type)s IS NULL OR annotation_type = %(annotation_type)s)
        AND deleted_at IS NULL
    ORDER BY created_at DESC
    LIMIT %(limit)s
"""
)

DELETE_BODY_REQUEST_ANNOTATION_HARD = """
    DELETE FROM meta.body_request_asset_annotations
    WHERE id in %(ids)s
"""

DELETE_BODY_REQUEST_ANNOTATION_SOFT = """
    UPDATE meta.body_request_asset_annotations
    SET deleted_at = NOW()
    WHERE id in %(ids)s
"""
