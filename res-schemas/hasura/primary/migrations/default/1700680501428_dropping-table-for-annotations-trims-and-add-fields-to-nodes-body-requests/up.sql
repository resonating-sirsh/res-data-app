alter table "meta"."brand_body_requests" rename to "body_requests";
alter table "meta"."brand_body_request_asset_trims" rename to "body_request_asset_trims";

DROP TABLE IF EXISTS meta.brand_body_request_asset_annotations CASCADE;

DROP TABLE IF EXISTS  "meta"."body_request_asset_annotations";

DROP TABLE IF EXISTS "meta"."body_request_asset_trims";

alter table "meta"."body_request_asset_nodes" add column "value" text
 null;
