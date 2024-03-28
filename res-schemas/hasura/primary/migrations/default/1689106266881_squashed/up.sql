
alter table "sell"."transactions" add column "transaction_status" text
 null;

alter table "sell"."transactions" add column "transaction_error_message" text
 null;
