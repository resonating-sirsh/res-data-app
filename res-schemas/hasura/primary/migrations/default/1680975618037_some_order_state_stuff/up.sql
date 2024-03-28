
alter table "sell"."order_item_fulfillments" add column "sent_to_assembly_at" timestamptz
 null;

alter table "sell"."orders" add column "contracts_failing" jsonb
 null default jsonb_build_object();

alter table "sell"."customers" add column "province_code" text
 null;
alter table "sell"."customers" add column "country_code" text
 null;

alter table "sell"."order_line_items" add column "fulfilled_quantity" integer
 null;

alter table "sell"."order_line_items" add column "refunded_quantity" integer
 null;

alter table "sell"."customers" drop constraint "customers_email_key";
