-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."body_pieces" add column "pins_geojson" jsonb
--  not null default '{}'::jsonb;
alter table "meta"."body_pieces" drop column "pins_geojson"