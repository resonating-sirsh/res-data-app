"""
This file contains all the sql queries used in the application for BrandBodyRequest
"""

INSERT_BRAND_BODY_REQUESTS = """
INSERT INTO "meta"."body_requests" (
    id,
    brand_code,
    name,
    status,
    request_type,
    body_category_id,
    body_onboarding_material_ids,
    fit_avatar_id,
    body_code,
    meta_body_id,
    combo_material_id,
    lining_material_id,
    size_scale_id,
    base_size_code,
    created_by
) VALUES (
    %(id)s,
    %(brand_code)s,
    %(name)s,
    %(status)s,
    %(request_type)s,
    %(body_category_id)s,
    %(body_onboarding_material_ids)s,
    %(fit_avatar_id)s,
    %(body_code)s,
    %(meta_body_id)s,
    %(combo_material_id)s,
    %(lining_material_id)s,
    %(size_scale_id)s,
    %(base_size_code)s,
    %(created_by)s
);
"""

UPDATE_BRAND_BODY_REQUESTS = """
UPDATE 
    "meta"."body_requests"
SET
    name = %(name)s,
    status = %(status)s,
    body_category_id = %(body_category_id)s,
    body_onboarding_material_ids = %(body_onboarding_material_ids)s,
    fit_avatar_id = %(fit_avatar_id)s,
    combo_material_id = %(combo_material_id)s,
    lining_material_id = %(lining_material_id)s,
    size_scale_id = %(size_scale_id)s,
    base_size_code = %(base_size_code)s
WHERE
    id = %(id)s
"""

GET_BRAND_BODY_REQUEST_BY_ID_AND_BRAND_CODE = """
SELECT
    id,
    brand_code,
    name,
    status,
    request_type,
    fit_avatar_id,
    body_category_id,
    body_onboarding_material_ids,
    body_code,
    meta_body_id,
    combo_material_id,
    lining_material_id,
    size_scale_id,
    base_size_code,
    created_by,
    created_at,
    updated_at,
    deleted_at
FROM
    "meta"."body_requests"
WHERE
    id = %(id)s
AND
    deleted_at IS NULL
"""

DELETE_BRAND_BODY_REQUEST_PERMANENTLY = """
DELETE FROM
    "meta"."body_requests"
WHERE
    id = %(id)s
;
"""

DELETE_BRAND_BODY_REQUEST = """
UPDATE "meta"."body_requests" SET
    deleted_at = NOW()
WHERE
    id = %(id)s
"""
