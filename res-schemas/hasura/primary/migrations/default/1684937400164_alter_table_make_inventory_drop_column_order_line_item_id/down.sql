alter table "make"."inventory" alter column "order_line_item_id" drop not null;
alter table "make"."inventory" add column "order_line_item_id" text;
