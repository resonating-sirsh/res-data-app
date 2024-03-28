"""
This module hold all the SQL queries for the body request assets table.
"""

SELECT_BODY_REQUEST_ASSETS = """
SELECT 
    id,
    name,
    type,
    uri,
    brand_body_request_id,
    created_at,
    updated_at,
    deleted_at
FROM
    "meta"."body_request_assets"
WHERE
    brand_body_request_id = %(body_request_id)s
AND
    deleted_at is NULL
;
"""

SELECT_BODY_REQUEST_ASSET_BY_IDS = """
SELECT 
    id,
    name,
    type,
    uri,
    brand_body_request_id,
    created_at,
    updated_at,
    deleted_at
FROM
    "meta"."body_request_assets"
WHERE
    id IN %(ids)s
AND
    deleted_at is NULL
;
"""


SELECT_BODY_REQUEST_ASSET_BY_ID = """
SELECT 
    id,
    name,
    type,
    uri,
    brand_body_request_id,
    created_at,
    updated_at,
    deleted_at
FROM
    "meta"."body_request_assets"
WHERE
    id = %(id)s
AND
    deleted_at is NULL
;
"""

UPDATE_BODY_REQUEST_ASSETS = """
UPDATE
    "meta"."body_request_assets"
SET
    name = %(name)s,
    uri = %(uri)s
WHERE
    id = %(id)s
;
"""


DELETE_BODY_REQUEST_ASSETS_BY_IDS = """
UPDATE
    "meta"."body_request_assets"
SET
    deleted_at = NOW()
WHERE
    id IN %(ids)s
;
"""

DELETE_BODY_REQUEST_ASSETS_BY_BBR = """
UPDATE
    "meta"."body_request_assets"
SET
    deleted_at = NOW()
WHERE
    brand_body_request_id = %(body_request_id)s
;
"""

DELETE_BODY_REQUEST_ASSETS_PERMANENTLY_IDS = """
DELETE FROM
    "meta"."body_request_assets"
WHERE
    id IN %(ids)s
;
"""

DELETE_BODY_REQUEST_ASSETS_PERMANENTLY_BBR = """
DELETE FROM
    "meta"."body_request_assets"
WHERE
    brand_body_request_id = %(body_request_id)s
;
"""
