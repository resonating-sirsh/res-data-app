
alter table "meta"."bodies" add column if not exists "front_image_uri" text
 null;

alter table "meta"."styles" add column if not exists "front_image_uri" text
 null;
