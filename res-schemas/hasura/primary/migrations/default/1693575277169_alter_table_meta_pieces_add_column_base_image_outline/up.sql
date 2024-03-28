alter table "meta"."pieces" add column "base_image_outline" jsonb
 null default jsonb_build_object();
