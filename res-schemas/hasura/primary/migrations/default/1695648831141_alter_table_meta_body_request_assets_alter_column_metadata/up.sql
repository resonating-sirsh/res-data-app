alter table "meta"."body_request_assets" alter column "metadata" drop not null;
alter table "meta"."body_request_assets" drop constraint "body_request_assets_metadata_key";
