
alter table "sell"."customers" add constraint "customers_email_key" unique ("email");

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "refunded_quantity" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_line_items" add column "fulfilled_quantity" integer
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."customers" add column "province_code" text
--  null;

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."orders" add column "contracts_failing" jsonb
--  null default jsonb_build_object();

-- Could not auto-generate a down migration.
-- Please write an appropriate down migration for the SQL below:
-- alter table "sell"."order_item_fulfillments" add column "sent_to_assembly_at" timestamptz
--  null;
