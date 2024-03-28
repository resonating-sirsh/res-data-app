alter table "meta"."bodies" add column "bom" jsonb
 null default jsonb_build_object();
