
alter table "sell"."order_line_items" add column "ecommerce_quantity" integer
 null;

alter table "sell"."order_line_items" add column "ecommerce_order_number" text
 null;

alter table "sell"."order_line_items" add column "ecommerce_line_item_id" text
 null;

alter table "sell"."order_line_items" add column "ecommerce_fulfillment_status" text
 null;

alter table "sell"."order_line_items" add column "product_variant_title" text
 null;

alter table "sell"."order_line_items" add column "product_name" text
 null;

alter table "sell"."order_line_items" add column "inventory_airtable_id" text
 null;

alter table "sell"."order_line_items" add column "warehouse_location" text
 null;

alter table "sell"."order_line_items" add column "stock_state_day_of_order" text
 null;

alter table "sell"."order_line_items" add column "inventory_checkin_type" text
 null;

alter table "sell"."order_line_items" add column "unit_ready_in_warehouse" text
 null;

alter table "sell"."order_line_items" drop column "unit_ready_in_warehouse" cascade;

alter table "sell"."order_line_items" add column "unit_ready_in_warehouse" boolean
 null;

alter table "sell"."order_line_items" add column "one_number" text
 null;
