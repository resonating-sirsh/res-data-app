alter table "sell"."orders" add column "was_payment_successful" boolean
 not null default 'False';
