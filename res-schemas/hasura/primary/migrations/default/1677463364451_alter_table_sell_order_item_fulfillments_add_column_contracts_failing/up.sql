alter table "sell"."order_item_fulfillments" add column "contracts_failing" jsonb
 null default jsonb_build_object();
