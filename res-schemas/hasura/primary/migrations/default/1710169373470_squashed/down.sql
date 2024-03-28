
-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "one_number" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "unit_ready_in_warehouse" boolean
--  null;

comment on column "sell"."order_line_items"."unit_ready_in_warehouse" is E'Individual Line items from orders';
alter table "sell"."order_line_items" alter column "unit_ready_in_warehouse" drop not null;
alter table "sell"."order_line_items" add column "unit_ready_in_warehouse" text;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "unit_ready_in_warehouse" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "inventory_checkin_type" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "stock_state_day_of_order" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "warehouse_location" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "inventory_airtable_id" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "product_name" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "product_variant_title" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "ecommerce_fulfillment_status" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "ecommerce_line_item_id" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "ecommerce_order_number" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "ecommerce_quantity" integer
--  null;
