alter table "sell"."customers" add column "metadata" jsonb
 null default jsonb_build_object();
