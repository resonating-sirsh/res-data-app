
alter table "meta"."trims" drop constraint "trims_color_id_fkey";

alter table "meta"."trims" drop column "size_id" cascade;

alter table "meta"."trims" add column "airtable_size_id" text
 null;

alter table "meta"."trims" drop column "color_id" cascade;

alter table "meta"."trims" add column "airtable_color_id" text
 null;
