alter table "make"."make_production_request" add column "metadata" jsonb
 null default jsonb_build_object();
