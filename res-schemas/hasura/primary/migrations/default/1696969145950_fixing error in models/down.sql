
alter table "meta"."body_request_asset_nodes" rename column "name" to "label";

comment on column "meta"."body_request_asset_nodes"."icon" is E'Nodes that can be use to ping positions of Assets in the 3D or 2D assets';
alter table "meta"."body_request_asset_nodes" alter column "icon" drop not null;
alter table "meta"."body_request_asset_nodes" add column "icon" text;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_request_asset_nodes" add column "deleted_at" timestamptz
--  null;
