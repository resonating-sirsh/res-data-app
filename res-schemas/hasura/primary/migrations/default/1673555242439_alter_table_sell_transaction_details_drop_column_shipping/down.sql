comment on column "sell"."transaction_details"."shipping" is E'This table holds Finance state for each item at the moment of payment. ';
alter table "sell"."transaction_details" alter column "shipping" drop not null;
alter table "sell"."transaction_details" add column "shipping" float4;
