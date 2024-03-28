
alter table "sell"."order_item_fulfillments" alter column "make_order_id" drop not null;
alter table "sell"."order_item_fulfillments" add column "stage" text
 not null default 'Not Send';
alter table "sell"."order_item_fulfillments" add column "node" text
 null;
alter table "sell"."order_item_fulfillments" add column "sku" text
 null;

alter table "sell"."order_item_fulfillments" add column "quantity" integer
 null;

alter table "sell"."order_item_fulfillments" add column "order_id" text
 null;

alter table "sell"."order_item_fulfillments" add column "contracts_failed" jsonb
 null;
