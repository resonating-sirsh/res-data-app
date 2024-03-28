alter table "meta"."bodies" add column "costs" jsonb
 null default jsonb_build_object();
