
comment on column "meta"."body_request_assets"."metadata" is E'Assets use on the Body Onboarding process';
alter table "meta"."body_request_assets" alter column "metadata" drop not null;
alter table "meta"."body_request_assets" add column "metadata" jsonb;

DROP TABLE "meta"."label_styles";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_request_asset_annotations" add column "label_style_id" uuid
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_request_asset_annotations" add column "size_label_uri" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_request_asset_annotations" add column "main_label_uri" text
--  null;

alter table "meta"."body_request_asset_annotations" rename column "annotation_type" to "type";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_request_asset_annotations" add column "artwork_uri" text
--  null;

alter table "meta"."body_request_asset_annotations" rename to "body_request_asset_nodes";
