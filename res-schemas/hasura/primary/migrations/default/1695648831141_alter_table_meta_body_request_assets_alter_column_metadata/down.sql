alter table "meta"."body_request_assets" add constraint "body_request_assets_metadata_key" unique ("metadata");
alter table "meta"."body_request_assets" alter column "metadata" set not null;
