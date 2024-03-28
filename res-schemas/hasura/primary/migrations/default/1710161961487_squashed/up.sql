
alter table "sell"."orders" add column "request_type" text
 null;

alter table "sell"."orders" add column "request_name" text
 null;

alter table "sell"."orders" add column "channel_order_id" text
 null;
