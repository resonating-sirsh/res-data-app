
alter table "sell"."orders" add column "line_item_json" jsonb
 null default jsonb_build_array();
