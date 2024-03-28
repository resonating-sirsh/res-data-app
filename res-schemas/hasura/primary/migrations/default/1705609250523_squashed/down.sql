
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "airtable_color_id" text
--  null;

alter table "meta"."trims" alter column "color_id" drop not null;
alter table "meta"."trims" add column "color_id" uuid;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trims" add column "airtable_size_id" text
--  null;

alter table "meta"."trims" alter column "size_id" drop not null;
alter table "meta"."trims" add column "size_id" uuid;

alter table "meta"."trims"
  add constraint "trims_color_id_fkey"
  foreign key ("color_id")
  references "meta"."colors"
  ("id") on update restrict on delete restrict;
