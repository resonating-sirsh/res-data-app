
alter table "sell"."transaction_details" add column "order_number" text
 not null;

alter table "sell"."transaction_details" add column "order_date" date
 not null;

alter table "sell"."transaction_details" add column "order_type" text
 not null;

alter table "sell"."transaction_details" add column "revenue_share" float4
 not null;

alter table "sell"."transaction_details" add column "total_amount" float4
 not null;
