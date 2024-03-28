
alter table "meta"."brand_body_request_asset_trims" drop constraint "brand_body_request_asset_trims_node_id_fkey";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."brand_body_request_asset_trims" add column "node_id" uuid
--  null;

DROP TABLE "meta"."brand_body_request_asset_trims";

DROP TABLE "meta"."body_request_asset_annotations";

DROP TABLE "meta"."body_request_asset_nodes";

DROP TABLE "meta"."body_request_assets";

alter table "meta"."brand_body_requests" drop constraint "brand_body_requests_meta_body_id_fkey",
  add constraint "brand_body_requests_meta_body_id_fkey"
  foreign key ("meta_body_id")
  references "meta"."bodies"
  ("id") on update restrict on delete set null;

alter table "meta"."brand_body_requests" drop constraint "brand_body_requests_meta_body_id_fkey";

alter table "meta"."brand_body_requests" rename column "meta_body_id" to "body_version_id";

comment on column "meta"."brand_body_requests"."cover_image_ids" is E'Brand intake for create/modify a body from existing or from scratch';
alter table "meta"."brand_body_requests" alter column "cover_image_ids" drop not null;
alter table "meta"."brand_body_requests" add column "cover_image_ids" text;

comment on column "meta"."brand_body_requests"."annotations" is E'Brand intake for create/modify a body from existing or from scratch';
alter table "meta"."brand_body_requests" alter column "annotations" drop not null;
alter table "meta"."brand_body_requests" add column "annotations" jsonb;

comment on column "meta"."brand_body_requests"."sketch_id" is E'Brand intake for create/modify a body from existing or from scratch';
alter table "meta"."brand_body_requests" alter column "sketch_id" drop not null;
alter table "meta"."brand_body_requests" add column "sketch_id" text;
