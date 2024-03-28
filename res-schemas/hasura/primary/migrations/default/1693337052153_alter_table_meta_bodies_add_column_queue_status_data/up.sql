alter table "meta"."bodies" add column "queue_status_data" jsonb
 null default jsonb_build_object();
