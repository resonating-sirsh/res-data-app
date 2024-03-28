alter table "make"."make_order_in_inventory" add column "metadata" jsonb
 null default jsonb_build_object();
