

alter table "meta"."trim_taxonomy" add column "airtable_record_id" text
 null;


alter table "meta"."trim_taxonomy" rename column "parent_trim_taxonomy_id" to "parent_meta_trim_taxonomy_pkid";

alter table "meta"."trim_taxonomy" drop column "size_id" cascade;

alter table "meta"."trim_taxonomy" add column "airtable_trim_size" text
 null;

alter table "meta"."trim_taxonomy" alter column "friendly_name" drop not null;

alter table "meta"."trim_taxonomy" alter column "parent_meta_trim_taxonomy_pkid" drop not null;

alter table "meta"."trim_taxonomy" alter column "id" set default gen_random_uuid();
