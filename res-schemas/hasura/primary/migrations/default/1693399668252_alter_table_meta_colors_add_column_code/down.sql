alter table "meta"."colors" drop constraint "colors_code_key";

alter table "meta"."colors" drop column if exists "code";
alter table "meta"."colors" drop column if exists "description";
alter table "meta"."colors" drop column if exists "metadata";
