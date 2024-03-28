
alter table "meta"."body_request_asset_nodes" add column "deleted_at" timestamptz
 null;

alter table "meta"."body_request_asset_nodes" drop column "icon" cascade;

alter table "meta"."body_request_asset_nodes" rename column "label" to "name";
