alter table "make"."make_order_in_inventory" alter column "order_name" drop not null;
alter table "make"."make_order_in_inventory" add column "order_name" text;
