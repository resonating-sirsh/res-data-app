
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_request_asset_nodes" add column "comment" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_request_asset_nodes" add column "mesaurment" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- DROP table "meta"."body_request_asset_trims";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- DROP table "meta"."body_request_asset_annotations";

alter table "meta"."body_requests" rename to "brand_body_requests";

alter table "meta"."body_request_asset_trims" rename to "brand_body_request_asset_trims";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- DROP TABLE IF EXISTS meta.brand_body_request_asset_annotations CASCADE;
