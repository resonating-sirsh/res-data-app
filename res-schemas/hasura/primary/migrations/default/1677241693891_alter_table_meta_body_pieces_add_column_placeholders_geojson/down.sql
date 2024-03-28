-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_pieces" add column "placeholders_geojson" jsonb
--  not null default '{}'::jsonb;
-- JL why can't it?
alter table "meta"."body_pieces" drop column "placeholders_geojson"