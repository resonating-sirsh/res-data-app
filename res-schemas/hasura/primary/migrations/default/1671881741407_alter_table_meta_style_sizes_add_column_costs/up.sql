alter table "meta"."style_sizes" add column "costs" jsonb
 null default jsonb_build_object();
