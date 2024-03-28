alter table "sell"."order_item_fulfillments" alter column "make_order_id" set not null;
alter table "sell"."order_item_fulfillments" drop column "stage";
alter table "sell"."order_item_fulfillments" drop column "node";
alter table "sell"."order_item_fulfillments" drop column "sku";
alter table "sell"."order_item_fulfillments" drop column "quantity";
alter table "sell"."order_item_fulfillments" drop column "order_id";
alter table "sell"."order_item_fulfillments" drop column "contracts_failed";
