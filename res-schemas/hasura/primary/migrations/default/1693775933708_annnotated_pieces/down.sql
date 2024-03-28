
comment on column "meta"."pieces"."annotated_image_uri" is NULL;

comment on column "meta"."pieces"."annotated_image_outline" is E'these are specific pieces that can be made and linked to styles';
alter table "meta"."pieces" alter column "annotated_image_outline" set default jsonb_build_object();
alter table "meta"."pieces" alter column "annotated_image_outline" drop not null;
alter table "meta"."pieces" add column "annotated_image_outline" jsonb;

alter table "meta"."pieces" rename column "annotated_image_outline" to "base_image_outline";

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."pieces" add column "annotated_image_s3_file_version_id" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."pieces" add column "annotated_image_uri" text
--  null;
