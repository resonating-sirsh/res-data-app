

ALTER TABLE "meta"."trim_taxonomy" ALTER COLUMN "id" drop default;

alter table "meta"."trim_taxonomy" alter column "parent_meta_trim_taxonomy_pkid" set not null;

alter table "meta"."trim_taxonomy" alter column "friendly_name" set not null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trim_taxonomy" add column "airtable_trim_size" text
--  null;

comment on column "meta"."trim_taxonomy"."size_id" is E'This represent the taxonomy of Trims';
alter table "meta"."trim_taxonomy"
  add constraint "trim_taxonomy_size_id_fkey"
  foreign key (size_id)
  references "meta"."sizes"
  (id) on update restrict on delete restrict;
alter table "meta"."trim_taxonomy" alter column "size_id" drop not null;
alter table "meta"."trim_taxonomy" add column "size_id" uuid;

alter table "meta"."trim_taxonomy" rename column "parent_meta_trim_taxonomy_pkid" to "parent_trim_taxonomy_id";


-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "meta"."trim_taxonomy" add column "airtable_record_id" text
--  null;
